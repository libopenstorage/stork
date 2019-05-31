package extender

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/libopenstorage/stork/drivers/volume"
	storklog "github.com/libopenstorage/stork/pkg/log"
	restore "github.com/libopenstorage/stork/pkg/snapshot/controllers"
	"github.com/portworx/sched-ops/k8s"
	log "github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/record"
	schedulerapi "k8s.io/kubernetes/pkg/scheduler/api"
)

const (
	filter     = "filter"
	prioritize = "prioritize"
	// nodePriorityScore Score by which each node is bumped if it has data for a volume
	nodePriorityScore = 100
	// rackPriorityScore Score by which each node is bumped if it is in the same
	// rack as a node which has data for the volume
	rackPriorityScore = 50
	// zonePriorityScore Score by which each node is bumped if it lies in the
	// same zone as a node which has data for the volume
	zonePriorityScore = 25
	// regionPriorityScore Score by which each node is bumped if it lies in the
	// same region as a node which has data for the volume
	regionPriorityScore = 10
	// defaultScore Score assigned to a node which doesn't have data for any volume
	defaultScore = 5

	schedulingFailureEventReason = "FailedScheduling"
)

// Extender Scheduler extender
type Extender struct {
	Recorder record.EventRecorder
	Driver   volume.Driver
	server   *http.Server
	lock     sync.Mutex
	started  bool
}

// Start Starts the extender
func (e *Extender) Start() error {
	e.lock.Lock()
	defer e.lock.Unlock()

	if e.started {
		return fmt.Errorf("Extender has already been started")
	}

	// TODO: Make the listen port configurable
	e.server = &http.Server{Addr: ":8099"}

	http.HandleFunc("/", e.serveHTTP)
	go func() {
		if err := e.server.ListenAndServe(); err != http.ErrServerClosed {
			log.Panicf("Error starting extender server: %v", err)
		}
	}()
	e.started = true
	return nil
}

// Stop Stops the extender
func (e *Extender) Stop() error {
	e.lock.Lock()
	defer e.lock.Unlock()

	if !e.started {
		return fmt.Errorf("Extender has not been started")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := e.server.Shutdown(ctx); err != nil {
		return err
	}
	e.started = false
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

func (e *Extender) getHostname(node *v1.Node) string {
	for _, address := range node.Status.Addresses {
		if address.Type == v1.NodeHostName {
			return address.Address
		}
	}
	return ""
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

	pod := args.Pod
	for _, vol := range pod.Spec.Volumes {
		// if any of pvc has restore annotation skip scheduling pod
		if vol.PersistentVolumeClaim == nil {
			continue
		}
		pvc, err := k8s.Instance().GetPersistentVolumeClaim(vol.PersistentVolumeClaim.ClaimName, pod.Namespace)
		if err != nil {
			msg := "Unable to find PVC %s" + vol.Name
			storklog.PodLog(pod).Warnf(msg)
			http.Error(w, msg, http.StatusBadRequest)
			return
		} else if pvc.Annotations != nil && pvc.Annotations[restore.RestoreAnnotation] == "true" {
			msg := "Volume restore is in progress for pvc: " + pvc.Name
			storklog.PodLog(pod).Warnf(msg)
			e.Recorder.Event(pod, v1.EventTypeWarning, schedulingFailureEventReason, msg)
			http.Error(w, msg, http.StatusBadRequest)
			return
		}
	}

	storklog.PodLog(pod).Debugf("Nodes in filter request:")
	for _, node := range args.Nodes.Items {
		storklog.PodLog(pod).Debugf("%v %+v", node.Name, node.Status.Addresses)
	}

	filteredNodes := []v1.Node{}
	driverVolumes, err := e.Driver.GetPodVolumes(&pod.Spec, pod.Namespace)
	if err != nil {
		msg := fmt.Sprintf("Error getting volumes for Pod for driver: %v", err)
		storklog.PodLog(pod).Warnf(msg)
		e.Recorder.Event(pod, v1.EventTypeWarning, schedulingFailureEventReason, msg)
		if _, ok := err.(*volume.ErrPVCPending); ok {
			http.Error(w, "Waiting for PVC to be bound", http.StatusBadRequest)
			return
		}
	} else if len(driverVolumes) > 0 {
		driverNodes, err := e.Driver.GetNodes()
		if err != nil {
			storklog.PodLog(pod).Errorf("Error getting list of driver nodes, returning all nodes")
		} else {
			for _, volumeInfo := range driverVolumes {
				onlineNodeFound := false
				for _, volumeNode := range volumeInfo.DataNodes {
					for _, driverNode := range driverNodes {
						if volumeNode == driverNode.StorageID && driverNode.Status == volume.NodeOnline {
							onlineNodeFound = true
						}
					}
				}
				if !onlineNodeFound {
					storklog.PodLog(pod).Errorf("No nodes in filter request have replica for volume, returning error")
					msg := "No online node found with volume replica"
					e.Recorder.Event(pod, v1.EventTypeWarning, schedulingFailureEventReason, msg)
					http.Error(w, msg, http.StatusBadRequest)
					return
				}
			}

			for _, node := range args.Nodes.Items {
				for _, driverNode := range driverNodes {
					storklog.PodLog(pod).Debugf("nodeInfo: %v", driverNode)
					if driverNode.Status == volume.NodeOnline &&
						volume.IsNodeMatch(&node, driverNode) {
						filteredNodes = append(filteredNodes, node)
						break
					}
				}
			}
			// If we filtered out all the nodes, the driver isn't running on any
			// of them, so return an error to avoid scheduling a pod on a
			// non-driver node
			if len(filteredNodes) == 0 {
				storklog.PodLog(pod).Errorf("No nodes in filter request have driver, returning error")
				msg := "No node found with storage driver"
				e.Recorder.Event(pod, v1.EventTypeWarning, schedulingFailureEventReason, msg)
				http.Error(w, msg, http.StatusBadRequest)
				return
			}
		}
	}

	// If we didn't find a PVC that interested us, return all the nodes from the request
	if len(filteredNodes) == 0 {
		filteredNodes = args.Nodes.Items
	}

	storklog.PodLog(pod).Debugf("Nodes in filter response:")
	for _, node := range filteredNodes {
		log.Debugf("%v %+v", node.Name, node.Status.Addresses)
	}
	response := &schedulerapi.ExtenderFilterResult{
		Nodes: &v1.NodeList{
			Items: filteredNodes,
		},
	}
	if err := encoder.Encode(response); err != nil {
		storklog.PodLog(pod).Errorf("Error encoding filter response: %+v : %v", response, err)
	}
}

func (e *Extender) getNodeScore(
	node v1.Node,
	volumeInfo *volume.Info,
	rackInfo *localityInfo,
	zoneInfo *localityInfo,
	regionInfo *localityInfo,
	idMap map[string]*volume.NodeInfo,
) int {
	for _, address := range node.Status.Addresses {
		if address.Type != v1.NodeHostName {
			continue
		}
		nodeRack := rackInfo.HostnameMap[address.Address]
		nodeZone := zoneInfo.HostnameMap[address.Address]
		nodeRegion := regionInfo.HostnameMap[address.Address]

		for _, region := range regionInfo.PreferredLocality {
			if region == nodeRegion || nodeRegion == "" {
				for _, zone := range zoneInfo.PreferredLocality {
					if zone == nodeZone || nodeZone == "" {
						for _, rack := range rackInfo.PreferredLocality {
							if rack == nodeRack || nodeRack == "" {
								for _, datanode := range volumeInfo.DataNodes {
									if volume.IsNodeMatch(&node, idMap[datanode]) {
										return nodePriorityScore
									}
								}
								if nodeRack != "" {
									return rackPriorityScore
								}
							}
						}
						if nodeZone != "" {
							return zonePriorityScore
						}
					}
				}
				if nodeRegion != "" {
					return regionPriorityScore
				}
			}
		}
	}
	return 0
}

type localityInfo struct {
	HostnameMap       map[string]string
	PreferredLocality []string
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

	pod := args.Pod
	storklog.PodLog(pod).Debugf("Nodes in prioritize request:")
	for _, node := range args.Nodes.Items {
		storklog.PodLog(pod).Debugf("%+v", node.Status.Addresses)
	}
	respList := schedulerapi.HostPriorityList{}

	// Intialize scores to 0
	priorityMap := make(map[string]int)
	for _, node := range args.Nodes.Items {
		for _, address := range node.Status.Addresses {
			if address.Type == v1.NodeHostName {
				priorityMap[address.Address] = 0
			}
		}
	}

	driverVolumes, err := e.Driver.GetPodVolumes(&pod.Spec, pod.Namespace)
	if err != nil {
		msg := fmt.Sprintf("Error getting volumes for Pod for driver: %v", err)
		storklog.PodLog(pod).Warnf(msg)
		e.Recorder.Event(pod, v1.EventTypeWarning, schedulingFailureEventReason, msg)
		if _, ok := err.(*volume.ErrPVCPending); ok {
			http.Error(w, "Waiting for PVC to be bound", http.StatusBadRequest)
			return
		}
		goto sendResponse
	} else if len(driverVolumes) > 0 {
		driverNodes, err := e.Driver.GetNodes()
		if err != nil {
			storklog.PodLog(pod).Errorf("Error getting nodes for driver: %v", err)
			goto sendResponse
		}

		// Create a map for ID->Node and Hostname->Rack/Zone/Region
		idMap := make(map[string]*volume.NodeInfo)
		var rackInfo, zoneInfo, regionInfo localityInfo
		rackInfo.HostnameMap = make(map[string]string)
		zoneInfo.HostnameMap = make(map[string]string)
		regionInfo.HostnameMap = make(map[string]string)
		for _, dnode := range driverNodes {
			// Replace driver's hostname with the kubernetes hostname to make it
			// easier to match nodes when calculating scores
			for _, knode := range args.Nodes.Items {
				if volume.IsNodeMatch(&knode, dnode) {
					dnode.Hostname = e.getHostname(&knode)
					break
				}
			}
			idMap[dnode.StorageID] = dnode
			storklog.PodLog(pod).Debugf("nodeInfo: %v", dnode)
			// For any node that is offline remove the locality info so that we
			// don't prioritize nodes close to it
			if dnode.Status == volume.NodeOnline {
				// Add region info into zone and zone info into rack so that we can
				// differentiate same names in different localities
				regionInfo.HostnameMap[dnode.Hostname] = dnode.Region
				if regionInfo.HostnameMap[dnode.Hostname] != "" {
					zoneInfo.HostnameMap[dnode.Hostname] = regionInfo.HostnameMap[dnode.Hostname] + "-" + dnode.Zone
				} else {
					zoneInfo.HostnameMap[dnode.Hostname] = dnode.Zone
				}
				if zoneInfo.HostnameMap[dnode.Hostname] != "" {
					rackInfo.HostnameMap[dnode.Hostname] = zoneInfo.HostnameMap[dnode.Hostname] + "-" + dnode.Rack
				} else {
					rackInfo.HostnameMap[dnode.Hostname] = dnode.Rack
				}
			} else {
				rackInfo.HostnameMap[dnode.Hostname] = ""
				zoneInfo.HostnameMap[dnode.Hostname] = ""
				regionInfo.HostnameMap[dnode.Hostname] = ""
			}
		}

		storklog.PodLog(pod).Debugf("rackMap: %v", rackInfo.HostnameMap)
		storklog.PodLog(pod).Debugf("zoneMap: %v", zoneInfo.HostnameMap)
		storklog.PodLog(pod).Debugf("regionMap: %v", regionInfo.HostnameMap)

		for _, volume := range driverVolumes {
			storklog.PodLog(pod).Debugf("Volume %v allocated on nodes:", volume.VolumeName)
			// Get the racks, zones and regions where the volume is located
			rackInfo.PreferredLocality = rackInfo.PreferredLocality[:0]
			zoneInfo.PreferredLocality = zoneInfo.PreferredLocality[:0]
			regionInfo.PreferredLocality = regionInfo.PreferredLocality[:0]
			for _, node := range volume.DataNodes {
				if _, ok := idMap[node]; ok {
					log.Debugf("ID: %v Hostname: %v", node, idMap[node].Hostname)
					regionInfo.PreferredLocality = append(regionInfo.PreferredLocality, regionInfo.HostnameMap[idMap[node].Hostname])
					zoneInfo.PreferredLocality = append(zoneInfo.PreferredLocality, zoneInfo.HostnameMap[idMap[node].Hostname])
					rackInfo.PreferredLocality = append(rackInfo.PreferredLocality, rackInfo.HostnameMap[idMap[node].Hostname])
				} else {
					log.Warnf("Node %v not found in list of nodes, skipping", node)
				}
			}
			storklog.PodLog(pod).Debugf("Volume %v allocated on racks: %v", volume.VolumeName, rackInfo.PreferredLocality)
			storklog.PodLog(pod).Debugf("Volume %v allocated in zones: %v", volume.VolumeName, zoneInfo.PreferredLocality)
			storklog.PodLog(pod).Debugf("Volume %v allocated in regions: %v", volume.VolumeName, regionInfo.PreferredLocality)

			for _, node := range args.Nodes.Items {
				priorityMap[node.Name] += e.getNodeScore(node, volume, &rackInfo, &zoneInfo, &regionInfo, idMap)
			}
		}
	}

sendResponse:
	// For any nodes that didn't have any volumes, assign it a
	// default score so that it doesn't get completely ignored
	// by the scheduler
	for _, node := range args.Nodes.Items {
		score, ok := priorityMap[node.Name]
		if !ok || score == 0 {
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
		storklog.PodLog(pod).Errorf("Failed to encode response: %v", err)
	}
}
