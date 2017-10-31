package extender

import (
	"encoding/json"
	"net/http"
	"strings"

	log "github.com/Sirupsen/logrus"
	"github.com/libopenstorage/stork/drivers/volume"
	storklog "github.com/libopenstorage/stork/pkg/log"
	"k8s.io/api/core/v1"
	schedulerapi "k8s.io/kubernetes/plugin/pkg/scheduler/api"
)

const (
	filter     = "filter"
	prioritize = "prioritize"
	// priorityScore Score by which each node is bumped if it has data for a volume
	priorityScore = 100
	// defaultScore Score assigned to a node which doesn't have data for any volume
	defaultScore = 10
)

// Extender Scheduler extender
type Extender struct {
	Driver volume.Driver
}

// Init Initializes the extender
func (e *Extender) Init() error {
	// TODO: Make the listen port configurable
	go func() {
		if err := http.ListenAndServe(":8099", http.HandlerFunc(e.serveHTTP)); err != nil {
			log.Panicf("Error starting extender server: %v", err)
		}
	}()
	return nil
}

func (e *Extender) serveHTTP(w http.ResponseWriter, req *http.Request) {
	if strings.Contains(req.URL.Path, filter) {
		e.processFilterRequest(w, req)
	} else if strings.Contains(req.URL.Path, prioritize) {
		e.processPrioritizeRequest(w, req)
	} else {
		http.Error(w, "Unsupported request", http.StatusNotFound)
	}
}

func (e *Extender) processFilterRequest(w http.ResponseWriter, req *http.Request) {
	decoder := json.NewDecoder(req.Body)
	defer func() {
		if err := req.Body.Close(); err != nil {
			log.Warnf("Error closing decoder")
		}
	}()
	encoder := json.NewEncoder(w)

	var args schedulerapi.ExtenderArgs
	if err := decoder.Decode(&args); err != nil {
		log.Errorf("Error decoding filter request: %v", err)
		http.Error(w, "Decode error", http.StatusBadRequest)
		return
	}

	pod := &args.Pod
	storklog.PodLog(pod).Debugf("Nodes in filter request:")
	for _, node := range args.Nodes.Items {
		storklog.PodLog(pod).Debugf("%+v", node.Status.Addresses)
	}

	filteredNodes := []v1.Node{}
	driverVolumes, err := e.Driver.GetPodVolumes(pod)

	if err != nil {
		storklog.PodLog(pod).Warnf("Error getting volumes for Pod for driver: %v", err)
		if _, ok := err.(*volume.ErrPVCPending); ok {
			http.Error(w, "Waiting for PVC to be bound", http.StatusBadRequest)
			return
		}
	} else if len(driverVolumes) > 0 {
		driverNodes, err := e.Driver.GetNodes()
		if err != nil {
			storklog.PodLog(pod).Errorf("Error getting list of driver nodes, returning all nodes")
		} else {
			for _, node := range args.Nodes.Items {
				for _, address := range node.Status.Addresses {
					if address.Type != v1.NodeHostName {
						continue
					}

					for _, driverNode := range driverNodes {
						if driverNode.Hostname == address.Address &&
							driverNode.Status == volume.NodeOnline {
							filteredNodes = append(filteredNodes, node)
						}
					}
				}
			}
		}
	}

	// If we filtered out all the nodes, or didn't find a PVC that
	// interested us, return all the nodes from the request
	if len(filteredNodes) == 0 {
		filteredNodes = args.Nodes.Items
	}

	storklog.PodLog(pod).Debugf("Nodes in filter response:")
	for _, node := range filteredNodes {
		log.Debugf("%+v", node.Status.Addresses)
	}
	response := &schedulerapi.ExtenderFilterResult{
		Nodes: &v1.NodeList{
			Items: filteredNodes,
		},
	}
	if err := encoder.Encode(response); err != nil {
		storklog.PodLog(pod).Fatalf("Error encoding filter response: %+v : %v", response, err)
	}

}
func (e *Extender) processPrioritizeRequest(w http.ResponseWriter, req *http.Request) {
	decoder := json.NewDecoder(req.Body)
	defer func() {
		if err := req.Body.Close(); err != nil {
			log.Warnf("Error closing decoder")
		}
	}()
	encoder := json.NewEncoder(w)

	var args schedulerapi.ExtenderArgs
	if err := decoder.Decode(&args); err != nil {
		log.Errorf("Error decoding prioritize request: %v", err)
		http.Error(w, "Decode error", http.StatusBadRequest)
		return
	}

	pod := &args.Pod
	storklog.PodLog(pod).Debugf("Nodes in prioritize request:")
	for _, node := range args.Nodes.Items {
		storklog.PodLog(pod).Debugf("%+v", node.Status.Addresses)
	}
	respList := schedulerapi.HostPriorityList{}
	driverVolumes, err := e.Driver.GetPodVolumes(pod)

	priorityMap := make(map[string]int)
	if err != nil {
		storklog.PodLog(pod).Warnf("Error getting volumes for Pod for driver: %v", err)
		if _, ok := err.(*volume.ErrPVCPending); ok {
			http.Error(w, "Waiting for PVC to be bound", http.StatusBadRequest)
			return
		}
	} else if len(driverVolumes) > 0 {
		for _, volume := range driverVolumes {
			storklog.PodLog(pod).Debugf("Volume allocated on nodes:")
			for _, node := range volume.DataNodes {
				log.Debugf("%+v", node)
			}

			for _, datanode := range volume.DataNodes {
				for _, node := range args.Nodes.Items {
					for _, address := range node.Status.Addresses {
						if datanode == address.Address {
							// Increment score for every volume that is required by
							// the pod and is present on the node
							_, ok := priorityMap[node.Name]
							if !ok {
								priorityMap[node.Name] = priorityScore
							} else {
								priorityMap[node.Name] += priorityScore
							}
						}
					}
				}
			}
		}
	}

	// For any nodes that didn't have any volumes, assign it a
	// default score so that it doesn't get completelt ignored
	// by the scheduler
	for _, node := range args.Nodes.Items {
		score, ok := priorityMap[node.Name]
		if !ok {
			score = defaultScore
		}
		hostPriority := schedulerapi.HostPriority{Host: node.Name, Score: score}
		respList = append(respList, hostPriority)
	}

	storklog.PodLog(pod).Debugf("Nodes in response:")
	for _, node := range respList {
		storklog.PodLog(pod).Debugf("%+v", node)
	}

	if err := encoder.Encode(respList); err != nil {
		storklog.PodLog(pod).Fatalf("Failed to encode response: %v", err)
	}

	return

}
