package monitor

import (
	"fmt"
	"reflect"
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
	initialNodeWaitDelay = 10 * time.Second
	nodeWaitFactor       = 2
	nodeWaitSteps        = 5

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

func (m *Monitor) isSameNode(k8sNodeName string, driverNode *volume.NodeInfo) bool {
	if k8sNodeName == driverNode.Hostname {
		return true
	}
	node, err := core.Instance().GetNodeByName(k8sNodeName)
	if err != nil {
		log.Errorf("Error getting node %v: %v", k8sNodeName, err)
		return false
	}
	return volume.IsNodeMatch(node, driverNode)
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

	if err := core.Instance().WatchPods("", fn, metav1.ListOptions{}); err != nil {
		log.Errorf("failed to watch pods due to: %v", err)
		return err
	}

	return nil
}

func (m *Monitor) driverMonitor() {
	defer close(m.done)

	for {
		select {
		default:
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
					m.wg.Add(1)
					// wait for 1 min if node is upgrading
					go m.cleanupDriverNodePods(node)
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

func (m *Monitor) cleanupDriverNodePods(node *volume.NodeInfo) {
	defer m.wg.Done()
	err := wait.ExponentialBackoff(nodeWaitCallBackoff, func() (bool, error) {
		n, err := m.Driver.InspectNode(node.StorageID)
		if err != nil {
			return false, nil
		}
		if n.Status != volume.NodeOnline {
			log.Infof("Volume driver on node %v (%v) is still offline (%v)", node.Hostname, node.StorageID, n.RawStatus)
			return false, nil
		}
		return true, nil
	})
	if err == nil {
		return
	}

	var pods *v1.PodList
	if !reflect.ValueOf(storkcache.Instance()).IsNil() {
		pods, err = storkcache.Instance().ListTransformedPods()
	} else {
		log.Warnf("shared informer cache has not been initialized.")
		pods, err = core.Instance().GetPods("", nil)
	}
	if err != nil {
		log.Errorf("Error getting pods: %v", err)
		return
	}

	// delete volume attachments if the node is down for this pod
	err = m.cleanupVolumeAttachmentsByNode(node)
	if err != nil {
		log.Errorf("Error cleaning up volume attachments: %v", err)
	}

	for _, pod := range pods.Items {
		var msg string
		csiPodPrefix, err := m.Driver.GetCSIPodPrefix()
		if err == nil && strings.HasPrefix(pod.Name, csiPodPrefix) {
			msg = fmt.Sprintf("Deleting csi pod from Node %v due to volume driver status: %v (%v)", pod.Spec.NodeName, node.Status, node.RawStatus)

		} else {
			msg = fmt.Sprintf("Deleting Pod from Node %v due to volume driver status: %v (%v)", pod.Spec.NodeName, node.Status, node.RawStatus)
			owns, err := m.doesDriverOwnPodVolumes(&pod)
			if err != nil || !owns {
				continue
			}
		}
		if m.isSameNode(pod.Spec.NodeName, node) {
			storklog.PodLog(&pod).Infof(msg)
			m.Recorder.Event(&pod, v1.EventTypeWarning, storageDriverOfflineReason, msg)
			err := core.Instance().DeletePods([]v1.Pod{pod}, true)
			if err != nil {
				storklog.PodLog(&pod).Errorf("Error deleting pod: %v", err)
				continue
			}
			HealthCounter.Inc()
		}
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

	pvc, err := core.Instance().GetPersistentVolumeClaim(pv.Spec.ClaimRef.Name, pv.Spec.ClaimRef.Namespace)
	if err != nil {
		log.Errorf("Error getting persistent volume claim from volume attachment: %v", err)
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

func (m *Monitor) cleanupVolumeAttachmentsByNode(node *volume.NodeInfo) error {
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
			if m.isSameNode(va.Spec.NodeName, node) {
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
