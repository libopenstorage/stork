package monitor

import (
	"fmt"
	"sync"
	"time"

	"github.com/libopenstorage/stork/drivers/volume"
	storklog "github.com/libopenstorage/stork/pkg/log"
	"github.com/portworx/sched-ops/k8s"
	log "github.com/sirupsen/logrus"
	"k8s.io/api/core/v1"
)

const (
	defaultIntervalSec = 120
)

// Monitor Storage driver monitor
type Monitor struct {
	Driver      volume.Driver
	IntervalSec time.Duration
	lock        sync.Mutex
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
	}

	m.stopChannel = make(chan int)
	m.done = make(chan int)

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

func (m *Monitor) driverMonitor() {
	defer close(m.done)
	for {
		select {
		default:
			log.Infof("Getting nodes")
			nodes, err := m.Driver.GetNodes()
			if err != nil {
				log.Errorf("Error getting nodes: %v", err)
				time.Sleep(2 * time.Second)
			}
			for _, node := range nodes {
				// Check if nodes are reported online by the storage driver
				// If not online, look at all the pods on that node
				// For any Running pod on that node using volume by the driver, kill the pod
				if node.Status != volume.NodeOnline {
					pods, err := k8s.Instance().GetPods("")
					if err != nil {
						log.Errorf("Error getting pods: %v", err)
						continue
					}
					for _, pod := range pods.Items {

						volumes, err := m.Driver.GetPodVolumes(&pod)
						if err != nil {
							storklog.PodLog(&pod).Errorf("Error getting volumes for pod: %v", err)
							continue
						}

						if len(volumes) == 0 {
							storklog.PodLog(&pod).Debugf("Pod doesn't have any volumes by driver, skipping")
							continue
						}

						if pod.Spec.NodeName == node.Hostname &&
							(pod.Status.Phase == v1.PodRunning || pod.Status.Phase == v1.PodFailed) {
							storklog.PodLog(&pod).Infof("Deleting Pod from Node: %v", pod.Spec.NodeName)
							err = k8s.Instance().DeletePods([]v1.Pod{pod}, true)
							if err != nil {
								storklog.PodLog(&pod).Errorf("Error deleting pod: %v", err)
								continue
							}
						}
					}
				}
			}
			time.Sleep(m.IntervalSec * time.Second)
		case <-m.stopChannel:
			return
		}
	}
}
