package extender

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/libopenstorage/stork/drivers/volume"
	storklog "github.com/libopenstorage/stork/pkg/log"
	restore "github.com/libopenstorage/stork/pkg/snapshot/controllers"
	"github.com/portworx/sched-ops/k8s/core"
	kvd "github.com/portworx/sched-ops/k8s/kubevirt-dynamic"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	schedulerapi "k8s.io/kube-scheduler/extender/v1"
)

//go:generate go run github.com/golang/mock/mockgen -destination=../mock/kubevirt.ops.mock.go -package=mock github.com/portworx/sched-ops/k8s/kubevirt-dynamic Ops
//go:generate go run github.com/golang/mock/mockgen -destination=../mock/kubevirt.vmiops.mock.go -package=mock github.com/portworx/sched-ops/k8s/kubevirt-dynamic VirtualMachineInstanceOps

const (
	filter     = "filter"
	prioritize = "prioritize"
	// nodePriorityScore Score by which each node is bumped if it has data for a volume
	nodePriorityScore float64 = 100
	// rackPriorityScore Score by which each node is bumped if it is in the same
	// rack as a node which has data for the volume
	rackPriorityScore float64 = 50
	// zonePriorityScore Score by which each node is bumped if it lies in the
	// same zone as a node which has data for the volume
	zonePriorityScore float64 = 25
	// regionPriorityScore Score by which each node is bumped if it lies in the
	// same region as a node which has data for the volume
	regionPriorityScore float64 = 10
	// defaultScore Score assigned to a node which doesn't have data for any volume
	defaultScore float64 = 5
	// storageDownNodeScorePenaltyPercentage is the percentage by which a node's score
	// will take a hit if the node's status is StorageDown
	storageDownNodeScorePenaltyPercentage float64 = 50
	schedulingFailureEventReason                  = "FailedScheduling"
	// Pod annotation to check if only local nodes should be used to schedule a pod
	preferLocalNodeOnlyAnnotation = "stork.libopenstorage.org/preferLocalNodeOnly"
	// StorageClass parameter to check if only remote nodes should be used to schedule a pod
	preferRemoteNodeOnlyParameter = "stork.libopenstorage.org/preferRemoteNodeOnly"
	// StorageClass parameter to disable anti-hyperconvergence for pods using shared v4 service volumes
	preferRemoteNodeParameter = "stork.libopenstorage.org/preferRemoteNode"
	// annotation to skip a volume and its local node replicas for scoring while
	// scheduling a pod
	skipScoringLabel = "stork.libopenstorage.org/skipSchedulerScoring"
	// annotation to disable hyperconvergence for a pod
	disableHyperconvergenceAnnotation = "stork.libopenstorage.org/disableHyperconvergence"
)

var (
	// HyperConvergedPodsCounter for pods hyper-converged by stork scheduler i.e scheduled on
	// node where replicas for all pod volumes exists
	HyperConvergedPodsCounter = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "stork_hyperconverged_pods_total",
		Help: "The total number of pods hyper-converged by stork scheduler",
	}, []string{"pod", "namespace"})
	// NonHyperConvergePodsCounter for pods which are placed on driver node but not on node
	// where pod volume replicas exists
	NonHyperConvergePodsCounter = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "stork_non_hyperconverged_pods_total",
		Help: "The total number of pods that are not hyper-converged by stork scheduler",
	}, []string{"pod", "namespace"})
	// SemiHyperConvergePodsCounter for pods which scheduled on node where replicas for all
	// volumes does not exists
	SemiHyperConvergePodsCounter = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "stork_semi_hyperconverged_pods_total",
		Help: "The total number of pods that are partially hyper-converged by stork scheduler",
	}, []string{"pod", "namespace"})
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

	prometheus.MustRegister(HyperConvergedPodsCounter)
	prometheus.MustRegister(NonHyperConvergePodsCounter)
	prometheus.MustRegister(SemiHyperConvergePodsCounter)
	if err := e.collectExtenderMetrics(); err != nil {
		return err
	}

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
	if pod == nil {
		msg := "Empty pod received in filter request"
		storklog.PodLog(pod).Errorf(msg)
		http.Error(w, msg, http.StatusBadRequest)
		return
	}

	// Filter csi pods on nodes where PX is online
	csiPodPrefix, err := e.Driver.GetCSIPodPrefix()
	if err == nil && strings.HasPrefix(pod.Name, csiPodPrefix) {
		e.processCSIExtPodFilterRequest(encoder, args)
		return
	}

	for _, vol := range pod.Spec.Volumes {
		// if any of pvc has restore annotation skip scheduling pod
		if vol.PersistentVolumeClaim == nil {
			continue
		}
		pvc, err := core.Instance().GetPersistentVolumeClaim(vol.PersistentVolumeClaim.ClaimName, pod.Namespace)
		if err != nil {
			msg := fmt.Sprintf("Unable to find PVC %s, err: %v", vol.Name, err)
			storklog.PodLog(pod).Warnf(msg)
			e.Recorder.Event(pod, v1.EventTypeWarning, schedulingFailureEventReason, msg)
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

	// preferRemoteOnlyExists is a flag to track if there is a single volume that exists with label
	preferRemoteOnlyExists := false
	// Node -> Bool to track if a Pod is not allowed to be scheduled on the node
	nodeNoAntiHyperconvergedPodAllowed := make(map[string]bool)
	filteredNodes := []v1.Node{}
	driverVolumes, WFFCVolumes, err := e.Driver.GetPodVolumes(&pod.Spec, pod.Namespace, true)
	if err != nil {
		msg := fmt.Sprintf("Error getting volumes for Pod for driver: %v", err)
		storklog.PodLog(pod).Warnf(msg)
		e.Recorder.Event(pod, v1.EventTypeWarning, schedulingFailureEventReason, msg)
		if _, ok := err.(*volume.ErrPVCPending); ok {
			http.Error(w, "Waiting for PVC to be bound", http.StatusBadRequest)
			return
		}
		// Do driver check even if we only have pending WaitForFirstConsumer volumes
	} else if len(driverVolumes) > 0 || len(WFFCVolumes) > 0 {
		driverNodes, err := e.Driver.GetNodes()
		if err != nil {
			storklog.PodLog(pod).Errorf("Error getting list of driver nodes, returning all nodes, err: %v", err)
		} else {
			for _, volumeInfo := range driverVolumes {
				// Pod is using a volume that is labeled for Windows
				// This Pod needs to run only on Windows node
				// Stork will return all nodes in the filter request
				if volumeInfo.WindowsVolume {
					e.encodeFilterResponse(encoder,
						pod,
						args.Nodes.Items)
					return
				}
				onlineNodeFound := false
				for _, volumeNode := range volumeInfo.DataNodes {
					for _, driverNode := range driverNodes {
						if volumeNode == driverNode.StorageID {
							// prefersRemoteNode and preferRemoteNodeOnly apply only to volumes with NeedsAntiHyperconvergence
							if volumeInfo.NeedsAntiHyperconvergence &&
								e.volumePrefersRemoteNode(volumeInfo) &&
								e.volumePrefersRemoteNodeOnly(volumeInfo) {
								preferRemoteOnlyExists = true
								nodeNoAntiHyperconvergedPodAllowed[driverNode.StorageID] = true
							}
							if driverNode.Status == volume.NodeOnline {
								onlineNodeFound = true
							}
						}
					}
				}
				if !onlineNodeFound && len(volumeInfo.DataNodes) > 0 {
					// Volume has a list of DataNodes where it has a replica present and none of
					// those nodes are online
					storklog.PodLog(pod).Errorf("No online storage nodes have replica for volume, returning error")
					msg := "No online node found with volume replica"
					e.Recorder.Event(pod, v1.EventTypeWarning, schedulingFailureEventReason, msg)
					http.Error(w, msg, http.StatusBadRequest)
					return
				}
			}

			preferLocalOnly := false
			if pod.Annotations != nil {
				if value, ok := pod.Annotations[preferLocalNodeOnlyAnnotation]; ok {
					if preferLocalOnly, err = strconv.ParseBool(value); err != nil {
						preferLocalOnly = false
					}
				}
			}

			hyperconvergenceVolumeCount := 0
			nodeHyperconvergenceVolumeCount := make(map[string]int)
			for _, volumeInfo := range driverVolumes {
				if !volumeInfo.NeedsAntiHyperconvergence {
					hyperconvergenceVolumeCount++
				}
				for _, volumeNode := range volumeInfo.DataNodes {
					//This loop is calculating the total number of volumes that are available on one node.
					//This is used to decide at a later step if the node can be used to strictly enforce hyperconvergence based on preferLocalNodeOnly.
					//preferLocalNodeOnly doesn't apply to volumes with NeedsAntiHyperconvergence set to true.
					//So an explicit volumePrefersRemoteNode or volumePrefersRemoteNodeOnly check is not necessary.
					if preferLocalOnly && !volumeInfo.NeedsAntiHyperconvergence {
						nodeHyperconvergenceVolumeCount[volumeNode]++
					}
				}
			}

			for _, node := range args.Nodes.Items {
				for _, driverNode := range driverNodes {
					storklog.PodLog(pod).Debugf("nodeInfo: %v", driverNode)
					if (driverNode.Status == volume.NodeOnline || driverNode.Status == volume.NodeStorageDown) &&
						volume.IsNodeMatch(&node, driverNode) {
						// If only nodes with replicas are to be preferred,
						// filter out all nodes that don't have a replica
						// for all the volumes
						if preferLocalOnly && nodeHyperconvergenceVolumeCount[driverNode.StorageID] != hyperconvergenceVolumeCount {
							continue
						}
						if val, ok := nodeNoAntiHyperconvergedPodAllowed[driverNode.StorageID]; ok && val {
							continue
						}
						filteredNodes = append(filteredNodes, node)
						break
					}
				}
			}

			// If we filtered out all the nodes, the driver isn't running on any
			// of them, so return an error to avoid scheduling a pod on a
			// non-driver node
			if len(filteredNodes) == 0 {
				var msg string
				if preferLocalOnly && preferRemoteOnlyExists {
					msg = "No nodes exist that can enforce Pod annotation preferLocalNodeOnly and StorageClass parameter preferRemoteNodeOnly together"
				} else if preferLocalOnly {
					msg = "No nodes with volume replica available"
				} else if preferRemoteOnlyExists {
					msg = "No nodes exist that can enforce StorageClass parameter preferRemoteNodeOnly"
				} else {
					msg = "No node found with storage driver"
				}
				storklog.PodLog(pod).Error(msg)
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

	e.encodeFilterResponse(encoder, pod, filteredNodes)
}

func (e *Extender) encodeFilterResponse(encoder *json.Encoder,
	pod *v1.Pod,
	filteredNodes []v1.Node) {

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

// volumePrefersRemoteNodeOnly checks if preferRemoteNodeOnly label is applied to the volume
func (e *Extender) volumePrefersRemoteNodeOnly(volumeInfo *volume.Info) bool {
	if volumeInfo.Labels != nil {
		if value, ok := volumeInfo.Labels[preferRemoteNodeOnlyParameter]; ok {
			if preferRemoteOnlyExists, err := strconv.ParseBool(value); err == nil {
				return preferRemoteOnlyExists
			}
		}
	}
	return false
}

// volumePrefersRemoteNode checks if preferRemoteNode label is applied to the volume, else returns default value True.
func (e *Extender) volumePrefersRemoteNode(volumeInfo *volume.Info) bool {
	if volumeInfo.Labels != nil {
		if value, ok := volumeInfo.Labels[preferRemoteNodeParameter]; ok {
			if preferRemoteExists, err := strconv.ParseBool(value); err == nil {
				return preferRemoteExists
			}
		}
	}
	return true
}

func (e *Extender) collectExtenderMetrics() error {
	fn := func(object runtime.Object) error {
		pod, ok := object.(*v1.Pod)
		if !ok {
			err := fmt.Errorf("invalid object type on pod watch: %v", object)
			return err
		}
		labels := make(prometheus.Labels)
		labels["pod"] = pod.GetName()
		labels["namespace"] = pod.GetNamespace()

		if pod.DeletionTimestamp != nil {
			HyperConvergedPodsCounter.Delete(labels)
			SemiHyperConvergePodsCounter.Delete(labels)
			NonHyperConvergePodsCounter.Delete(labels)
			return nil
		}
		// check only if pods in ready state
		isPodReady := false
		for _, cond := range pod.Status.Conditions {
			if cond.Type == v1.PodReady && cond.Status == v1.ConditionTrue {
				isPodReady = true
			}
		}
		if !isPodReady {
			return nil
		}

		driverVolumes, _, err := e.Driver.GetPodVolumes(&pod.Spec, pod.Namespace, false)
		if err != nil {
			msg := fmt.Sprintf("Metric: Error getting volumes for Pod for driver: %v", err)
			storklog.PodLog(pod).Warnf(msg)
			return err
		}
		if len(driverVolumes) == 0 {
			// pods not using any stork supported driver volumes
			return nil
		}
		driverNodes, err := e.Driver.GetNodes()
		if err != nil {
			return err
		}
		// find driver name id
		var storageID string
		for _, dnode := range driverNodes {
			if dnode.SchedulerID == pod.Spec.NodeName {
				storageID = dnode.StorageID
				break
			}
		}
		// find ideal hyperconverge node candidate
		nodeMap := make(map[string]int)
		for _, dvol := range driverVolumes {
			for _, dataIP := range dvol.DataNodes {
				// assign score to node
				nodeMap[dataIP]++
			}
		}
		// find driver node with highest core
		large := 0
		for _, v := range nodeMap {
			if v > large {
				large = v
			}
		}
		if val, ok := nodeMap[storageID]; ok {
			if large == val {
				HyperConvergedPodsCounter.With(labels).Set(1)
			} else {
				SemiHyperConvergePodsCounter.With(labels).Set(1)
			}
		} else {
			NonHyperConvergePodsCounter.With(labels).Set(1)
		}

		return nil
	}

	if err := core.Instance().WatchPods("", fn, metav1.ListOptions{}); err != nil {
		log.Errorf("failed to watch pods due to: %v", err)
		return err
	}
	return nil
}

func (e *Extender) getNodeScore(
	node v1.Node,
	volumeInfo *volume.Info,
	rackInfo *localityInfo,
	zoneInfo *localityInfo,
	regionInfo *localityInfo,
	storageNode *volume.NodeInfo,
) float64 {
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
								for _, datanodeID := range volumeInfo.DataNodes {
									if storageNode.StorageID == datanodeID {
										if storageNode.Status == volume.NodeStorageDown {
											// Even if the volume data is local to the node
											// the node is in degraded state. So the app won't benefit
											// from hyperconvergence on this node. So we will not use
											// the nodePriorityScore but instead rackPriorityScore and
											// penalize based on that.
											return rackPriorityScore * (storageDownNodeScorePenaltyPercentage / 100)
										}
										return nodePriorityScore
									}
								}
								if nodeRack != "" {
									if storageNode.Status == volume.NodeStorageDown {
										return rackPriorityScore * (storageDownNodeScorePenaltyPercentage / 100)
									}
									return rackPriorityScore
								}
							}
						}
						if nodeZone != "" {
							if storageNode.Status == volume.NodeStorageDown {
								return zonePriorityScore * (storageDownNodeScorePenaltyPercentage / 100)
							}
							return zonePriorityScore
						}
					}
				}
				if nodeRegion != "" {
					if storageNode.Status == volume.NodeStorageDown {
						return regionPriorityScore * (storageDownNodeScorePenaltyPercentage / 100)
					}
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

	// Intialize scores to 0
	priorityMap := make(map[string]int)
	for _, node := range args.Nodes.Items {
		for _, address := range node.Status.Addresses {
			if address.Type == v1.NodeHostName {
				priorityMap[address.Address] = 0
			}
		}
	}

	respList := schedulerapi.HostPriorityList{}

	// Score all nodes the same if hyperconvergence is disabled
	disableHyperconvergence := false
	var err error

	// Prioritize virt launcher pods on nodes to facilitate local volume attachment
	isVirtLauncherPod := false
	if e.isVirtLauncherPod(pod) {
		storklog.PodLog(pod).Infof("Processing a virt launcher pod")
		isVirtLauncherPod = true
		err := e.processVirtLauncherPodPrioritizeRequest(encoder, args)
		if err != nil {
			// Let the default scoring logic decide the prioritize score
			storklog.PodLog(pod).Errorf("Failed to process special prioritization for virt launcher pod; will prioritize normally: %v", err)
		} else {
			// Done populating encoded priority score output
			return
		}
	}

	// Prioritize csi pods on nodes where PX is online
	csiPodPrefix, err := e.Driver.GetCSIPodPrefix()
	if err == nil && strings.HasPrefix(pod.Name, csiPodPrefix) {
		e.processCSIExtPodPrioritizeRequest(encoder, args)
		return
	}

	if pod.Annotations != nil {
		if value, ok := pod.Annotations[disableHyperconvergenceAnnotation]; ok {
			if disableHyperconvergence, err = strconv.ParseBool(value); err != nil {
				disableHyperconvergence = false
			}
		}
	}
	if disableHyperconvergence {
		goto sendResponse
	}

	{ // Put these variables in their own scope so we can use the goto above
		driverVolumes, _, err := e.Driver.GetPodVolumes(&pod.Spec, pod.Namespace, true)
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
			// Create a map for k8s node index to StorageNode
			k8sNodeIndexStorageNodeMap := make(map[int]*volume.NodeInfo)
			for _, dnode := range driverNodes {
				// Replace driver's hostname with the kubernetes hostname to make it
				// easier to match nodes when calculating scores
				for k8sNodeIndex, knode := range args.Nodes.Items {
					if volume.IsNodeMatch(&knode, dnode) {
						dnode.Hostname = e.getHostname(&knode)
						k8sNodeIndexStorageNodeMap[k8sNodeIndex] = dnode
						break
					}
				}
				idMap[dnode.StorageID] = dnode
				storklog.PodLog(pod).Debugf("nodeInfo: %v", dnode)
				// For any node that is offline remove the locality info so that we
				// don't prioritize nodes close to it
				if dnode.Status == volume.NodeOnline || dnode.Status == volume.NodeStorageDown {
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

			isAntihyperconvergenceRequired := false
			storklog.PodLog(pod).Debugf("rackMap: %v", rackInfo.HostnameMap)
			storklog.PodLog(pod).Debugf("zoneMap: %v", zoneInfo.HostnameMap)
			storklog.PodLog(pod).Debugf("regionMap: %v", regionInfo.HostnameMap)
			for _, volume := range driverVolumes {
				skipVolumeScoring := false
				if value, exists := volume.Labels[skipScoringLabel]; exists {
					if skipVolumeScoring, err = strconv.ParseBool(value); err != nil {
						skipVolumeScoring = false
					}
				}

				if skipVolumeScoring || volume.WindowsVolume {
					storklog.PodLog(pod).Debugf("Skipping volume %v from scoring", volume.VolumeName)
					continue
				}

				// If we reached this point for a virt launcher pod means processVirtLauncherPodPrioritizeRequest returned
				// an error and we are relying on the default scoring logic. Hyperconvergence is the default behavior.
				if !isVirtLauncherPod &&
					volume.NeedsAntiHyperconvergence &&
					e.volumePrefersRemoteNode(volume) {
					isAntihyperconvergenceRequired = true
					storklog.PodLog(pod).Debugf("Volume %v configured with a requirement of anti hyperconvergence, skipping scoring", volume.VolumeName)
					continue
				}
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

				for k8sNodeIndex, node := range args.Nodes.Items {
					storageNode := k8sNodeIndexStorageNodeMap[k8sNodeIndex]
					priorityMap[node.Name] += int(e.getNodeScore(node, volume, &rackInfo, &zoneInfo, &regionInfo, storageNode))
				}
			}

			if isAntihyperconvergenceRequired {
				e.updateForAntiHyperconvergence(args, driverVolumes, k8sNodeIndexStorageNodeMap, priorityMap)
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
			score = int(defaultScore)
		}
		hostPriority := schedulerapi.HostPriority{Host: node.Name, Score: int64(score)}
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

// isPodRunningOnVolAttachedNode returns true if the pod is running node where volume is attached
func (e *Extender) isPodRunningOnVolAttachedNode(pod *v1.Pod, vol *volume.Info) bool {
	log.Debugf("Pod Name: %v, Pod Node IP: %v,  Volume Node IP: %v", pod.Name, pod.Status.HostIP, vol.AttachedOn)
	return pod.Status.HostIP == vol.AttachedOn
}

// isVirtLauncherPod returns true if it is a virt launcher pod
func (e *Extender) isVirtLauncherPod(pod *v1.Pod) bool {
	return pod.Labels["kubevirt.io"] == "virt-launcher"
}

// getLiveMigrationInfo returns status of LiveMigration
func (e *Extender) getLiveMigrationInfo(refPod *v1.Pod, refVMIUID types.UID) (podBeingLiveMigrated *v1.Pod, err error) {
	pods, err := core.Instance().GetPods(refPod.Namespace, nil)
	if err != nil {
		msg := "unable to get Pod list in the same namespace."
		return nil, fmt.Errorf("err: %s", msg)
	}
	// If another virt launcher pod is Running in the same namespace and has the same VMI owner reference Then it can
	// be concluded that this Pod is where the original VM was being hosted and is being LiveMigrated to the refPod
	for _, pod := range pods.Items {
		if pod.Status.Phase != v1.PodRunning ||
			!e.isVirtLauncherPod(&pod) {
			continue
		}
		_, vmiUID := e.getVMIInfo(&pod)
		if vmiUID == "" {
			continue
		}
		if vmiUID == refVMIUID {
			return &pod, nil
		}
	}
	return nil, nil
}

// getVMIInfo returns the name and UID of the VirtualMachineInstance for the given pod
func (e *Extender) getVMIInfo(refPod *v1.Pod) (string, types.UID) {
	for _, owner := range refPod.OwnerReferences {
		if owner.Kind == "VirtualMachineInstance" {
			return owner.Name, owner.UID
		}
	}
	return "", ""
}

// GetPVNameFromPVC returns PV name for a PVC
func (e *Extender) GetPVNameFromPVC(pvcName string, namespace string) (string, error) {
	pvc, err := core.Instance().GetPersistentVolumeClaim(pvcName, namespace)
	if err != nil || pvc == nil {
		return "", fmt.Errorf("error getting PV name for PVC (%v/%v): %w", pvcName, namespace, err)
	}

	return pvc.Spec.VolumeName, err
}

func (e *Extender) updateForAntiHyperconvergence(
	args schedulerapi.ExtenderArgs,
	driverVolumes []*volume.Info,
	k8sNodeIndexStorageNodeMap map[int]*volume.NodeInfo,
	priorityMap map[string]int) {
	pod := args.Pod
	needsAntiHyperconvergenceReplicaNodes := make(map[string]bool)
	for _, volume := range driverVolumes {
		skipVolumeScoring := false
		var err error
		if value, exists := volume.Labels[skipScoringLabel]; exists {
			if skipVolumeScoring, err = strconv.ParseBool(value); err != nil {
				skipVolumeScoring = false
			}
		}
		if skipVolumeScoring {
			storklog.PodLog(pod).Debugf("Skipping volume %v from scoring during antihyperconvergence evaluation due to skipScoringLabel", volume.VolumeName)
			continue
		}
		//We want hyperconvergence-based scoring for NeedsAntiHyperconvergence volumes with preferRemoteNode parameter set to false
		if !volume.NeedsAntiHyperconvergence || !e.volumePrefersRemoteNode(volume) {
			storklog.PodLog(pod).Debugf("Skipping volume %v from scoring based on antihyperconvergence", volume.VolumeName)
			continue
		}
		for _, datanodeID := range volume.DataNodes {
			needsAntiHyperconvergenceReplicaNodes[datanodeID] = true
		}
	}
	for k8sNodeIndex, node := range args.Nodes.Items {
		storageNode := k8sNodeIndexStorageNodeMap[k8sNodeIndex]
		// storageNode.StorageID = datanodeID
		// Give defaultScore to the nodes where NeedsAntiHyperconvergence volume exist
		// to give them a lower score
		if val, ok := needsAntiHyperconvergenceReplicaNodes[storageNode.StorageID]; ok && val {
			priorityMap[node.Name] = int(defaultScore)
		} else if storageNode.Status == volume.NodeOnline {
			// In a scenario where regular volumes do not exist
			// Raise the score of non replica nodes to give them
			// a score higher than the default score
			priorityMap[node.Name] += int(nodePriorityScore)
		}
	}
}

func (e *Extender) processCSIExtPodFilterRequest(
	encoder *json.Encoder,
	args schedulerapi.ExtenderArgs) {
	filteredNodes := []v1.Node{}
	pod := args.Pod
	driverNodes, err := e.Driver.GetNodes()
	if err != nil {
		storklog.PodLog(pod).Errorf("Error getting list of driver nodes, returning all nodes, err: %v", err)
	} else {
		for _, knode := range args.Nodes.Items {
			for _, dnode := range driverNodes {
				storklog.PodLog(pod).Debugf("nodeInfo: %v", dnode)
				// Only nodes which are Online or in StorageDown state should schedule pods.
				// All the nodes in Offline or Degraded state will be skipped.
				if (dnode.Status == volume.NodeOnline || dnode.Status == volume.NodeStorageDown) &&
					volume.IsNodeMatch(&knode, dnode) {
					filteredNodes = append(filteredNodes, knode)
					break
				}
			}
		}
	}

	if len(filteredNodes) == 0 {
		filteredNodes = args.Nodes.Items
	}

	e.encodeFilterResponse(encoder, pod, filteredNodes)
}

func (e *Extender) processCSIExtPodPrioritizeRequest(
	encoder *json.Encoder,
	args schedulerapi.ExtenderArgs) {
	respList := schedulerapi.HostPriorityList{}
	pod := args.Pod
	driverNodes, err := e.Driver.GetNodes()
	if err != nil || len(driverNodes) == 0 {
		storklog.PodLog(pod).Errorf("Error getting nodes for driver: %v", err)
		for _, knode := range args.Nodes.Items {
			hostPriority := schedulerapi.HostPriority{Host: knode.Name, Score: int64(defaultScore)}
			respList = append(respList, hostPriority)
		}
	} else {
		driverNodes = volume.RemoveDuplicateOfflineNodes(driverNodes)

		for _, dnode := range driverNodes {
			var score int64
			for _, knode := range args.Nodes.Items {
				if volume.IsNodeMatch(&knode, dnode) {
					if dnode.Status == volume.NodeOnline {
						score = int64(nodePriorityScore)
					} else if dnode.Status == volume.NodeOffline {
						score = 0
					} else {
						score = int64(nodePriorityScore * (storageDownNodeScorePenaltyPercentage / 100))
					}
					hostPriority := schedulerapi.HostPriority{Host: knode.Name, Score: int64(score)}
					respList = append(respList, hostPriority)
					break
				}
			}
		}
	}

	storklog.PodLog(pod).Debugf("Nodes in prioritize response:")
	for _, node := range respList {
		storklog.PodLog(pod).Debugf("%+v", node)
	}

	if err := encoder.Encode(respList); err != nil {
		storklog.PodLog(pod).Errorf("Failed to encode response: %v", err)
	}
}

func (e *Extender) processVirtLauncherPodPrioritizeRequest(
	encoder *json.Encoder,
	args schedulerapi.ExtenderArgs) error {
	pod := args.Pod

	vmiName, vmiUID := e.getVMIInfo(pod)
	if vmiName == "" {
		return fmt.Errorf("unable to find vmiName for pod: %v", pod.Name)
	}
	podBeingLiveMigrated, err := e.getLiveMigrationInfo(pod, vmiUID)
	if err != nil {
		return fmt.Errorf("unable to find liveMigrationInfo: %w", err)
	}

	if podBeingLiveMigrated != nil {
		storklog.PodLog(pod).Infof("Live VM migration in progress: (vmiName: %v, podBeingLiveMigrated: %v)", vmiName, podBeingLiveMigrated.Name)
	} else {
		storklog.PodLog(pod).Infof("No ongoing live VM migration in progress. vmiName: %v", vmiName)
	}

	vmi, err := kvd.Instance().GetVirtualMachineInstance(context.TODO(), pod.Namespace, vmiName)
	if err != nil {
		return fmt.Errorf("unable to find VMI Info: %w", err)
	}
	storklog.PodLog(pod).Debugf("VMI is using PVC: %v", vmi.RootDiskPVC)

	pvName, err := e.GetPVNameFromPVC(vmi.RootDiskPVC, pod.Namespace)
	if err != nil {
		return fmt.Errorf("unable to inspect PVC: %v err: %w", vmi.RootDiskPVC, err)
	}
	storklog.PodLog(pod).Debugf("PV for scoring: %v", pvName)

	volInfo, err := e.Driver.InspectVolume(pvName)
	if err != nil {
		return fmt.Errorf("unable to inspect PV: %v. err: %w", pvName, err)
	}

	if podBeingLiveMigrated != nil {
		// We give lower score to replica nodes so that if a pod is unable to schedule on the node
		// where the volume is attached we can run it on a non replica node, so that a subsequent
		// LiveMigration can move the pod back to the replica node in a single hop.
		podIsRunningOnVolAttachedNode := e.isPodRunningOnVolAttachedNode(podBeingLiveMigrated, volInfo)

		if podIsRunningOnVolAttachedNode {
			// LiveMigration in progress and existing pod is running on node with volume attached => Antihyperconvergence
			// i.e. Give lower scores to nodes with volume replicas
			storklog.PodLog(pod).Infof("Pod %v is running with same VMI owner reference is using localAttachment.", podBeingLiveMigrated.Name)
			e.updateVirtLauncherPodPrioritizeScores(encoder,
				args,
				volInfo,
				false, /*preferLocalAttachment*/
				true /*antihyperconvergence*/)
		} else {
			// LiveMigration in progress but existing pod is using NFS.
			// Use Antihyperconvergence to give lower score to replica nodes without volume attachment
			// Use preferLocalAttachment to give highest score to node with volume attachment

			e.updateVirtLauncherPodPrioritizeScores(encoder,
				args,
				volInfo,
				true, /*preferLocalAttachment*/
				true /*antihyperconvergence*/)
		}
	} else {
		// Default behavior for LiveMigration not in progress is hyperconvergence
		// We should still give a higher score to node with volume attachment. We might not
		// have attachment info avilable in this scenario and the same score will be
		// applied for all the replica nodes. However, if volume attachment info is available
		// that node should get higher score. All non replica nodes will get lower score.
		e.updateVirtLauncherPodPrioritizeScores(encoder,
			args,
			volInfo,
			true, /*preferLocalAttachment*/
			false /*antihyperconvergence*/)
	}
	return nil
}

func (e *Extender) updateVirtLauncherPodPrioritizeScores(
	encoder *json.Encoder,
	args schedulerapi.ExtenderArgs,
	volInfo *volume.Info,
	preferLocalAttachment bool,
	antihyperconvergence bool) {
	pod := args.Pod

	// Default behavior is no special preference to local attachment
	localNodeScore := int64(nodePriorityScore)
	// Default behavior is hyperconvergence
	replicaNodeScore := int64(nodePriorityScore)
	remoteNodeScore := int64(defaultScore)

	if antihyperconvergence {
		remoteNodeScore = int64(nodePriorityScore)
		replicaNodeScore = int64(defaultScore)
	}

	// preferLocalAttachment = true but AttachedOn = "" can occur if there is just one pod
	// getting scheduled and there is no LiveMigration ongoing
	if preferLocalAttachment && volInfo.AttachedOn != "" {
		storklog.PodLog(pod).Debugf("Prioritize for volume attachedOn: %v", volInfo.AttachedOn)
		localNodeScore = int64(2 * nodePriorityScore)
	}

	respList := schedulerapi.HostPriorityList{}
	driverNodes, err := e.Driver.GetNodes()
	if err != nil || len(driverNodes) == 0 || volInfo == nil {
		storklog.PodLog(pod).Errorf("Error getting nodes for driver: %v", err)
		for _, knode := range args.Nodes.Items {
			hostPriority := schedulerapi.HostPriority{Host: knode.Name, Score: int64(defaultScore)}
			respList = append(respList, hostPriority)
		}
	} else {
		driverNodes = volume.RemoveDuplicateOfflineNodes(driverNodes)
		for _, dnode := range driverNodes {
			for _, knode := range args.Nodes.Items {
				var score int64
				isUpdated := false
				if volume.IsNodeMatch(&knode, dnode) {
					isUpdated = false
					// Score replica nodes
					for _, dataNodes := range volInfo.DataNodes {
						if dataNodes == dnode.StorageID {
							score = replicaNodeScore
							isUpdated = true
							break
						}
					}

					if preferLocalAttachment && volInfo.AttachedOn != "" {
						// Update score of node for volume attachment
						for _, nodeIP := range dnode.IPs {
							if nodeIP == volInfo.AttachedOn {
								// Local attachment
								storklog.PodLog(pod).Debugf("Updating score of node: %v for local attachment", dnode.StorageID)
								score = localNodeScore
								isUpdated = true
							}
						}
					}

					if !isUpdated {
						// Update score of non replica nodes
						score = remoteNodeScore
					}

					if dnode.Status == volume.NodeOffline {
						score = 0
					} else if dnode.Status != volume.NodeOnline {
						score = int64(float64(score) * (storageDownNodeScorePenaltyPercentage / 100))
					}
					hostPriority := schedulerapi.HostPriority{Host: knode.Name, Score: int64(score)}
					respList = append(respList, hostPriority)
					break
				}
			}
		}
	}

	storklog.PodLog(pod).Debugf("Nodes in prioritize response:")
	for _, node := range respList {
		storklog.PodLog(pod).Debugf("%+v", node)
	}

	if err := encoder.Encode(respList); err != nil {
		storklog.PodLog(pod).Errorf("Failed to encode response: %v", err)
	}
}
