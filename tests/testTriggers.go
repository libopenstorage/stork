package tests

import (
	"fmt"
	"math"
	"os/exec"
	"strings"
	"time"

	. "github.com/onsi/ginkgo"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/portworx/torpedo/drivers/volume"
	"github.com/sirupsen/logrus"
)

type Event struct {
	ID   string
	Type string
}

type EventRecord struct {
	Event   Event
	Start   time.Time
	End     time.Time
	Outcome []error
}

type EventRecords []*EventRecord

var globalEventRec EventRecords
var currHeadForGEventRec int

// GenerateUUID generates unique ID
func GenerateUUID() string {
	uuidbyte, _ := exec.Command("uuidgen").Output()
	return strings.TrimSpace(string(uuidbyte))
}

// UpdateOutcome updates outcome based on error
func UpdateOutcome(event *EventRecord, err error) {
	if err != nil {
		event.Outcome = append(event.Outcome, err)
	}
}

const (
	RestartVolDriver = "restartVolDriver"
	CrashVolDriver   = "crashVolDriver"
	RebootNode       = "rebootNode"
	DeleteApp        = "deleteApp"
	EmailReporter    = "emailReporter"
	HAUpdate         = "haUpdate"
)

// TriggerHAUpdate changes HA level of all volumes of given contexts
func TriggerHAUpdate(contexts []*scheduler.Context, recordChan *chan *EventRecord) {
	defer GinkgoRecover()
	event := EventRecord{
		Event: Event{
			ID:   GenerateUUID(),
			Type: HAUpdate,
		},
		Start:   time.Now(),
		Outcome: []error{},
	}

	defer func() {
		event.End = time.Now()
		logrus.Infof("Sending event [%+v] on channel\n", event)
		*recordChan <- &event
	}()

	expReplMap := make(map[*volume.Volume]int64)
	Step("get volumes for all apps in test and update replication factor", func() {
		for _, ctx := range contexts {
			var appVolumes []*volume.Volume
			var err error
			Step(fmt.Sprintf("get volumes for %s app", ctx.App.Key), func() {
				appVolumes, err = Inst().S.GetVolumes(ctx)
				UpdateOutcome(&event, err)
				expect(appVolumes).NotTo(beEmpty())
			})
			for _, v := range appVolumes {
				MaxRF := Inst().V.GetMaxReplicationFactor()
				MinRF := Inst().V.GetMinReplicationFactor()

				Step(
					fmt.Sprintf("repl decrease volume driver %s on app %s's volume: %v",
						Inst().V.String(), ctx.App.Key, v),
					func() {
						errExpected := false
						currRep, err := Inst().V.GetReplicationFactor(v)
						UpdateOutcome(&event, err)
						expect(err).NotTo(haveOccurred())

						if currRep == MinRF {
							errExpected = true
						}
						expReplMap[v] = int64(math.Max(float64(MinRF), float64(currRep)-1))
						err = Inst().V.SetReplicationFactor(v, currRep-1)
						if !errExpected {
							UpdateOutcome(&event, err)
							expect(err).NotTo(haveOccurred())
						} else {
							if !expect(err).To(haveOccurred()) {
								UpdateOutcome(&event, fmt.Errorf("Expected HA reduce to fail since new repl factor is less than %v but it did not", MinRF))
							}
						}

					})
				Step(
					fmt.Sprintf("validate successful repl decrease on app %s's volume: %v",
						ctx.App.Key, v),
					func() {
						newRepl, err := Inst().V.GetReplicationFactor(v)
						UpdateOutcome(&event, err)
						expect(err).NotTo(haveOccurred())
						expect(newRepl).To(equal(expReplMap[v]))
					})
				Step(
					fmt.Sprintf("repl increase volume driver %s on app %s's volume: %v",
						Inst().V.String(), ctx.App.Key, v),
					func() {
						errExpected := false
						currRep, err := Inst().V.GetReplicationFactor(v)
						UpdateOutcome(&event, err)
						expect(err).NotTo(haveOccurred())
						// GetMaxReplicationFactory is hardcoded to 3
						// if it increases repl 3 to an aggregated 2 volume, it will fail
						// because it would require 6 worker nodes, since
						// number of nodes required = aggregation level * replication factor
						currAggr, err := Inst().V.GetAggregationLevel(v)
						UpdateOutcome(&event, err)
						expect(err).NotTo(haveOccurred())
						if currAggr > 1 {
							MaxRF = int64(len(node.GetWorkerNodes())) / currAggr
						}
						if currRep == MaxRF {
							errExpected = true
						}
						expReplMap[v] = int64(math.Min(float64(MaxRF), float64(currRep)+1))
						err = Inst().V.SetReplicationFactor(v, currRep+1)
						if !errExpected {
							UpdateOutcome(&event, err)
							expect(err).NotTo(haveOccurred())
						} else {
							if !expect(err).To(haveOccurred()) {
								event.Outcome = append(event.Outcome, fmt.Errorf("Expected HA increase to fail since new repl factor is greater than %v but it did not", MaxRF))
							}
						}
					})
				Step(
					fmt.Sprintf("validate successful repl increase on app %s's volume: %v",
						ctx.App.Key, v),
					func() {
						newRepl, err := Inst().V.GetReplicationFactor(v)
						UpdateOutcome(&event, err)
						expect(err).NotTo(haveOccurred())
						if !expect(newRepl).To(equal(expReplMap[v])) {
							UpdateOutcome(&event, fmt.Errorf("Current repl count [%d] does not match with expected repl count [%d]", newRepl, expReplMap[v]))
						}
					})
				Step("validate apps", func() {
					for _, ctx := range contexts {
						ValidateContext(ctx)
					}
				})
			}
		}
	})
}

// TriggerCrashVolDriver crashes vol driver
func TriggerCrashVolDriver(contexts []*scheduler.Context, recordChan *chan *EventRecord) {
	defer GinkgoRecover()
	Step("crash volume driver in all nodes", func() {
		for _, appNode := range node.GetStorageDriverNodes() {
			Step(
				fmt.Sprintf("crash volume driver %s on node: %v",
					Inst().V.String(), appNode.Name),
				func() {
					CrashVolDriverAndWait([]node.Node{appNode})
				})
		}
	})
}

// TriggerRestartVolDriver restarts volume driver and validates app
func TriggerRestartVolDriver(contexts []*scheduler.Context, recordChan *chan *EventRecord) {
	defer GinkgoRecover()
	Step("get nodes bounce volume driver", func() {
		for _, appNode := range node.GetStorageDriverNodes() {
			Step(
				fmt.Sprintf("stop volume driver %s on node: %s",
					Inst().V.String(), appNode.Name),
				func() {
					StopVolDriverAndWait([]node.Node{appNode})
				})

			Step(
				fmt.Sprintf("starting volume %s driver on node %s",
					Inst().V.String(), appNode.Name),
				func() {
					StartVolDriverAndWait([]node.Node{appNode})
				})

			Step("Giving few seconds for volume driver to stabilize", func() {
				time.Sleep(20 * time.Second)
			})

			Step("validate apps", func() {
				for _, ctx := range contexts {
					ValidateContext(ctx)
				}
			})
		}
	})
}

// TriggerDeleteApps deletes app and verifies if those are rescheduled properly
func TriggerDeleteApps(contexts []*scheduler.Context, recordChan *chan *EventRecord) {
	defer GinkgoRecover()
	Step("delete all application tasks", func() {
		for _, ctx := range contexts {
			Step(fmt.Sprintf("delete tasks for app: %s", ctx.App.Key), func() {
				err := Inst().S.DeleteTasks(ctx, nil)
				expect(err).NotTo(haveOccurred())
			})
			ValidateContext(ctx)
		}
	})
}

// TriggerRebootNodes reboots node on which apps are running
func TriggerRebootNodes(contexts []*scheduler.Context, recordChan *chan *EventRecord) {
	defer GinkgoRecover()
	Step("get all nodes and reboot one by one", func() {
		/*nodesToReboot := node.GetWorkerNodes()

		// Reboot node and check driver status
		Step(fmt.Sprintf("reboot node one at a time from the node(s): %v", nodesToReboot), func() {
			// TODO: Below is the same code from existing nodeReboot test
			for _, n := range nodesToReboot {
				if n.IsStorageDriverInstalled {
					Step(fmt.Sprintf("reboot node: %s", n.Name), func() {
						err := Inst().N.RebootNode(n, node.RebootNodeOpts{
							Force: true,
							ConnectionOpts: node.ConnectionOpts{
								Timeout:         1 * time.Minute,
								TimeBeforeRetry: 5 * time.Second,
							},
						})
						expect(err).NotTo(haveOccurred())
						UpdateOutcome(event, err)
					})

					Step(fmt.Sprintf("wait for node: %s to be back up", n.Name), func() {
						err := Inst().N.TestConnection(n, node.ConnectionOpts{
							Timeout:         15 * time.Minute,
							TimeBeforeRetry: 10 * time.Second,
						})
						expect(err).NotTo(haveOccurred())
						UpdateOutcome(event, err)
					})

					Step(fmt.Sprintf("wait for volume driver to stop on node: %v", n.Name), func() {
						err := Inst().V.WaitDriverDownOnNode(n)
						expect(err).NotTo(haveOccurred())
						UpdateOutcome(event, err)
					})

					Step(fmt.Sprintf("wait to scheduler: %s and volume driver: %s to start",
						Inst().S.String(), Inst().V.String()), func() {

						err := Inst().S.IsNodeReady(n)
						expect(err).NotTo(haveOccurred())
						UpdateOutcome(event, err)

						err = Inst().V.WaitDriverUpOnNode(n, Inst().DriverStartTimeout)
						expect(err).NotTo(haveOccurred())
						UpdateOutcome(event, err)
					})

					Step("validate apps", func() {
						for _, ctx := range contexts {
							ValidateContext(ctx)
						}
					})
				}
			}
		})
		*/
	})
}

// TriggerEmailReporter sends email with all reported errors
func TriggerEmailReporter(contexts []*scheduler.Context, recordChan *chan *EventRecord) {
	logrus.Infof("Going to read from chan with len: %d\n", len(*recordChan))
	if eventRec, ok := <-*recordChan; ok {
		logrus.Infof("Got record from chan : [%+v]", eventRec)
	}
	for eventRecord := range *recordChan {
		logrus.Errorf("record: [%+v]\n", eventRecord)
	}
}
