package monitor

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/libopenstorage/stork/drivers/volume"
	storkcache "github.com/libopenstorage/stork/pkg/cache"
	storklog "github.com/libopenstorage/stork/pkg/log"
	"github.com/portworx/sched-ops/k8s/core"
	"github.com/portworx/sched-ops/k8s/storage"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/record"
	"k8s.io/kubernetes/pkg/util/node"
)

const (
	defaultIntervalSec = 120
	minimumIntervalSec = 30
	// This will result into a total 2.5 minutes of backoff
	initialNodeWaitDelay      = 10 * time.Second
	nodeWaitFactor            = 2
	nodeWaitSteps             = 5
	podDeleteBatchSize        = 5
	podBatchDeleteIntervalSec = 30

	storageDriverOfflineReason = "StorageDriverOffline"
)

var (
	// HealthCounter for pods which are rescheduled by stork monitor
	HealthCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "stork_pods_rescheduled_total",
		Help: "The total number of pods rescehduled by stork pod monitor",
	})
)

var nodeWaitCallBackoff = wait.Backoff{
	Duration: initialNodeWaitDelay,
	Factor:   nodeWaitFactor,
	Steps:    nodeWaitSteps,
}

// Monitor Storage driver monitor
type Monitor struct {
	Driver      volume.Driver
	IntervalSec int64
	Recorder    record.EventRecorder
	lock        sync.Mutex
	wg          sync.WaitGroup
	started     bool
	stopChannel chan int
	done        chan int
}

// Start Starts the monitor
func (m *Monitor) Start() error {
	m.lock.Lock()
	defer m.lock.Unlock()

	if m.started {
		return fmt.Errorf("Monitor has already been started")
	}

	if m.IntervalSec == 0 {
		m.IntervalSec = defaultIntervalSec
	} else if m.IntervalSec < minimumIntervalSec {
		return fmt.Errorf("minimum interval for health monitor is %v seconds", minimumIntervalSec)
	}

	m.stopChannel = make(chan int)
	m.done = make(chan int)

	prometheus.MustRegister(HealthCounter)
	if err := m.podMonitor(); err != nil {
		return err
	}

	go m.driverMonitor()

	m.started = true

	return nil
}

// Stop Stops the monitor
func (m *Monitor) Stop() error {
	m.lock.Lock()
	defer m.lock.Unlock()

	if !m.started {
		return fmt.Errorf("Monitor has not been started")
	}

	close(m.stopChannel)
	<-m.done

	m.started = false
	return nil
}

func (m *Monitor) isSameK8sNode(k8sNode *v1.Node, driverNode *volume.NodeInfo) bool {
	if k8sNode.Name == driverNode.Hostname {
		return true
	}
	return volume.IsNodeMatch(k8sNode, driverNode)
}

func (m *Monitor) podMonitor() error {
	fn := func(object runtime.Object) error {
		pod, ok := object.(*v1.Pod)
		if !ok {
			err := fmt.Errorf("invalid object type on pod watch: %v", object)
			return err
		}

		podUnknownState := false
		if pod.Status.Reason == node.NodeUnreachablePodReason {
			podUnknownState = true
		} else if pod.ObjectMeta.DeletionTimestamp != nil {
			n, err := core.Instance().GetNodeByName(pod.Spec.NodeName)
			if err != nil {
				return err
			}

			// Check if node has eviction taint
			for _, taint := range n.Spec.Taints {
				if taint.Key == v1.TaintNodeUnreachable &&
					taint.Effect == v1.TaintEffectNoExecute {
					podUnknownState = true
					break
				}
			}
		}

		var msg string
		if podUnknownState {
			csiPodPrefix, err := m.Driver.GetCSIPodPrefix()
			if err == nil && strings.HasPrefix(pod.Name, csiPodPrefix) {
				msg = "Force deleting csi pod as it's in unknown state."
				storklog.PodLog(pod).Infof(msg)
			} else {
				owns, err := m.doesDriverOwnPodVolumes(pod)
				if err != nil || !owns {
					return nil
				}

				msg = "Force deleting pod as it's in unknown state."
				storklog.PodLog(pod).Infof(msg)

				// delete volume attachments if the node is down for this pod
				err = m.cleanupVolumeAttachmentsByPod(pod)
				if err != nil {
					storklog.PodLog(pod).Errorf("Error cleaning up volume attachments: %v", err)
				}
			}
			// force delete the pod
			m.Recorder.Event(pod, v1.EventTypeWarning, node.NodeUnreachablePodReason, msg)
			err = core.Instance().DeletePods([]v1.Pod{*pod}, true)
			if err != nil {
				if errors.IsNotFound(err) {
					return nil
				}

				storklog.PodLog(pod).Errorf("Error deleting pod: %v", err)
				return err
			}
			HealthCounter.Inc()
		}

		return nil
	}

	podHandler := func(object interface{}) {
		pod, ok := object.(*v1.Pod)
		if !ok {
			log.Errorf("invalid object type on pod watch from cache: %v", object)
		} else {
			fn(pod)
		}
	}

	if storkcache.Instance() != nil {
		log.Debugf("Shared informer cache has been initialized, using it for pod monitor.")
		err := storkcache.Instance().WatchPods(podHandler)
		if err != nil {
			log.Errorf("failed to watch pods with informer cache for health monitoring, err: %v", err)
		}
	} else {
		log.Warnf("Shared informer cache has not been initialized, using watch for pod monitor.")
		if err := core.Instance().WatchPods("", fn, metav1.ListOptions{}); err != nil {
			log.Errorf("failed to watch pods for health monitoring due to: %v", err)
			return err
		}
	}

	return nil
}

func (m *Monitor) driverMonitor() {
	defer close(m.done)

	for {
		select {
		default:
			k8sNodeNameToNodeMap := make(map[string]*v1.Node)
			driverNodeToK8sNodeMap := make(map[string]*v1.Node)
			log.Debugf("Monitoring storage nodes")
			nodes, err := m.Driver.GetNodes()
			if err != nil {
				log.Errorf("Error getting nodes: %v", err)
				time.Sleep(2 * time.Second)
			}
			nodes = volume.RemoveDuplicateOfflineNodes(nodes)
			for _, node := range nodes {
				// Check if nodes are reported as offline or degraded by the storage driver
				// If offline or degraded, look at all the pods on that node
				// For any Running pod on that node using volume by the driver, kill the pod
				// Degraded nodes are not considered offline and pods are not deleted from them.
				if node.Status == volume.NodeOffline || node.Status == volume.NodeDegraded {
					// Only initialize the map when it is absolutely necessary
					if len(k8sNodeNameToNodeMap) == 0 {
						k8sNodeNameToNodeMap, err = m.getK8sNodeNameToNodeMap()
						if err != nil {
							log.Errorf("Error getting k8s node name to node map: %v", err)
							continue
						}
					}
					// Only initialize the map when it is absolutely necessary
					if len(driverNodeToK8sNodeMap) == 0 {
						driverNodeToK8sNodeMap = m.getVolumeDriverNodesToK8sNodeMap(nodes)
					}
					if _, ok := driverNodeToK8sNodeMap[node.StorageID]; !ok {
						log.Errorf("Node with scheduler name %v and storage ID %v is not found in k8s cluster", node.SchedulerID, node.StorageID)
						continue
					}
					k8sNode := driverNodeToK8sNodeMap[node.StorageID]
					m.wg.Add(1)
					go m.cleanupDriverNodePods(node, k8sNode, k8sNodeNameToNodeMap)
				}
			}
			// lets all node to finish processing and then start sleep
			m.wg.Wait()

			// With this default sleep of 2 minutes and the backoff of 2.5 minutes
			// stork will delete the pods if a driver is down within 4.5 minutes
			time.Sleep(time.Duration(m.IntervalSec) * time.Second)
		case <-m.stopChannel:
			return
		}
	}
}

func (m *Monitor) cleanupDriverNodePods(driverNode *volume.NodeInfo, k8sNode *v1.Node, k8sNodeNameToNodeMap map[string]*v1.Node) {
	defer m.wg.Done()
	err := wait.ExponentialBackoff(nodeWaitCallBackoff, func() (bool, error) {
		n, err := m.Driver.InspectNode(driverNode.StorageID)
		if err != nil {
			return false, nil
		}
		if m.isNodeOffline(n) {
			log.Infof("Volume driver on node %v (%v) is still offline (%v)", driverNode.Hostname, driverNode.StorageID, n.RawStatus)
			return false, nil
		}
		return true, nil
	})
	if err == nil {
		return
	}

	var pods *v1.PodList
	if storkcache.Instance() != nil {
		pods, err = storkcache.Instance().ListTransformedPods(k8sNode.Name)
	} else {
		log.Warnf("shared informer cache has not been initialized.")
		pods, err = core.Instance().GetPodsByNode(k8sNode.Name, "")
	}
	if err != nil {
		log.Errorf("Error getting pods: %v", err)
		return
	}

	// delete volume attachments if the node is down for this pod
	err = m.cleanupVolumeAttachmentsByNode(driverNode, k8sNodeNameToNodeMap)
	if err != nil {
		log.Errorf("Error cleaning up volume attachments: %v", err)
	}

	podsSelectedForDeletion := make([]v1.Pod, 0)
	csiPodsSelectedForDeletion := make([]v1.Pod, 0)
	for _, pod := range pods.Items {
		csiPodPrefix, err := m.Driver.GetCSIPodPrefix()
		if err == nil && strings.HasPrefix(pod.Name, csiPodPrefix) {
			csiPodsSelectedForDeletion = append(csiPodsSelectedForDeletion, pod)
		} else {
			if len(pod.Spec.Volumes) == 0 {
				// Pod does not have any volumes. So, no need to check further
				continue
			}
			owns, err := m.doesDriverOwnPodVolumes(&pod)
			if err != nil || !owns {
				continue
			}
			podsSelectedForDeletion = append(podsSelectedForDeletion, pod)
		}
	}

	// Delete CSI pods first
	if len(csiPodsSelectedForDeletion) > 0 {
		m.batchDeletePodsFromOfflineNodes(csiPodsSelectedForDeletion, driverNode, true)
	}

	// Do batch deletes of rest of the pods
	if len(podsSelectedForDeletion) > 0 {
		m.batchDeletePodsFromOfflineNodes(podsSelectedForDeletion, driverNode, false)
	}
}

// batchDeletePodsFromOfflineNodes deletes the pods in batches of batchSize
// and increments the HealthCounter by the number of pods deleted
func (m *Monitor) batchDeletePodsFromOfflineNodes(
	pods []v1.Pod,
	node *volume.NodeInfo,
	csiPods bool) {

	log.Infof("Going to delete %d pods from node %v in batch of %d", len(pods), node.Hostname, podDeleteBatchSize)
	for i := 0; i < len(pods); i += podDeleteBatchSize {

		// Do a fresh node status check before deleting pods
		n, err := m.Driver.InspectNode(node.StorageID)
		if err != nil {
			log.Errorf("Error inspecting node %v in batch deletion of pods: %v", node.StorageID, err)
			return
		}
		if !m.isNodeOffline(n) {
			log.Infof("Volume driver on node %v with storage ID (%v) is now %v in batch deletion of pods, hence ignoring pod deletions from this node", node.Hostname, node.StorageID, n.Status)
			return
		}

		end := i + podDeleteBatchSize
		if end > len(pods) {
			end = len(pods)
		}
		counterIncrement := end - i
		for _, pod := range pods[i:end] {
			var msg string
			if csiPods {
				msg = fmt.Sprintf("Deleting CSI Pod from Node %v due to volume driver status: %v (%v)", pod.Spec.NodeName, node.Status, node.RawStatus)
			} else {
				msg = fmt.Sprintf("Deleting Pod from Node %v due to volume driver status: %v (%v)", pod.Spec.NodeName, node.Status, node.RawStatus)
			}
			storklog.PodLog(&pod).Infof(msg)
			m.Recorder.Event(&pod, v1.EventTypeWarning, storageDriverOfflineReason, msg)
		}

		err = core.Instance().DeletePods(pods[i:end], true)
		if err != nil {
			log.Errorf("Error deleting pods: %v", err)
		}
		HealthCounter.Add(float64(counterIncrement))
		// Delaying between batch deletes to avoid overwhelming the extender
		time.Sleep(podBatchDeleteIntervalSec * time.Second)
	}
}

func (m *Monitor) doesDriverOwnPodVolumes(pod *v1.Pod) (bool, error) {
	volumes, _, err := m.Driver.GetPodVolumes(&pod.Spec, pod.Namespace, false)
	if err != nil {
		storklog.PodLog(pod).Errorf("Error getting volumes for pod: %v", err)
		return false, err
	}

	if len(volumes) == 0 {
		storklog.PodLog(pod).Debugf("Pod doesn't have any volumes by driver")
		return false, nil
	}

	return true, nil
}

func (m *Monitor) doesDriverOwnVolumeAttachment(va *storagev1.VolumeAttachment) (bool, error) {
	pv, err := core.Instance().GetPersistentVolume(*va.Spec.Source.PersistentVolumeName)
	if err != nil {
		log.Errorf("Error getting persistent volume from volume attachment: %v", err)
		return false, err
	}

	var pvc *v1.PersistentVolumeClaim
	var msg string
	if storkcache.Instance() != nil {
		pvc, err = storkcache.Instance().GetPersistentVolumeClaim(pv.Spec.ClaimRef.Name, pv.Spec.ClaimRef.Namespace)
		msg = fmt.Sprintf("Error getting persistent volume claim from volume attachment from informer cache: %v", err)
	} else {
		pvc, err = core.Instance().GetPersistentVolumeClaim(pv.Spec.ClaimRef.Name, pv.Spec.ClaimRef.Namespace)
		msg = fmt.Sprintf("Error getting persistent volume claim from volume attachment: %v", err)
	}
	if err != nil {
		log.Errorf(msg)
		return false, err
	}

	return m.Driver.OwnsPVC(core.Instance(), pvc), nil
}

func (m *Monitor) cleanupVolumeAttachmentsByPod(pod *v1.Pod) error {
	log.Infof("Cleaning up volume attachments for pod %s", pod.Name)

	// Get all vol attachments
	vaList, err := storage.Instance().ListVolumeAttachments()
	if err != nil {
		return err
	}

	if len(vaList.Items) > 0 {
		for _, va := range vaList.Items {
			// Delete attachments for this pod
			if pod.Spec.NodeName == va.Spec.NodeName {
				err := storage.Instance().DeleteVolumeAttachment(va.Name)
				if err != nil {
					return err
				}

				log.Infof("Deleted volume attachment: %s", va.Name)
			}
		}
	}

	return nil
}

func (m *Monitor) cleanupVolumeAttachmentsByNode(node *volume.NodeInfo, k8sNodeNameToNodeMap map[string]*v1.Node) error {
	log.Infof("Cleaning up volume attachments for node %s", node.StorageID)

	// Get all vol attachments
	vaList, err := storage.Instance().ListVolumeAttachments()
	if err != nil {
		return err
	}

	if len(vaList.Items) > 0 {
		for _, va := range vaList.Items {
			owns, err := m.doesDriverOwnVolumeAttachment(&va)
			if err != nil || !owns {
				continue
			}

			// Delete attachments for this pod
			k8sNode, ok := k8sNodeNameToNodeMap[va.Spec.NodeName]
			if !ok {
				log.Warnf("Node with name %v not found in k8s node map", va.Spec.NodeName)
				continue
			}
			if m.isSameK8sNode(k8sNode, node) {
				err := storage.Instance().DeleteVolumeAttachment(va.Name)
				if err != nil {
					return err
				}

				log.Infof("Deleted volume attachment: %s", va.Name)
			}
		}
	}

	return nil
}

func (m *Monitor) getVolumeDriverNodesToK8sNodeMap(driverNodes []*volume.NodeInfo) map[string]*v1.Node {
	nodeMap := make(map[string]*v1.Node)

	k8sNodes, err := core.Instance().GetNodes()
	if err != nil {
		log.Errorf("Error getting nodes in monitor: %v", err)
		return nodeMap
	}
	for _, k8sNode := range k8sNodes.Items {
		for _, driverNode := range driverNodes {
			if m.isSameK8sNode(&k8sNode, driverNode) {
				nodeMap[driverNode.StorageID] = k8sNode.DeepCopy()
			}
		}
	}
	return nodeMap
}

func (m *Monitor) isNodeOffline(node *volume.NodeInfo) bool {
	return node.Status == volume.NodeOffline || node.Status == volume.NodeDegraded
}

func (m *Monitor) getK8sNodeNameToNodeMap() (map[string]*v1.Node, error) {
	nodeMap := make(map[string]*v1.Node)

	k8sNodes, err := core.Instance().GetNodes()
	if err != nil {
		log.Errorf("Error getting nodes in monitor: %v", err)
		return nodeMap, fmt.Errorf("error listing k8s nodes: %v", err)
	}
	for _, k8sNode := range k8sNodes.Items {
		nodeMap[k8sNode.Name] = k8sNode.DeepCopy()
	}
	return nodeMap, nil
}
