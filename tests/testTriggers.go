package tests

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"reflect"
	"sort"
	"strings"
	"text/template"
	"time"

	"container/ring"

	"github.com/onsi/ginkgo"

	"github.com/pborman/uuid"
	api "github.com/portworx/px-backup-api/pkg/apis/v1"
	"github.com/portworx/sched-ops/k8s/core"
	"github.com/portworx/sched-ops/task"

	storkv1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	storkops "github.com/portworx/sched-ops/k8s/stork"
	"github.com/portworx/torpedo/drivers/backup"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/portworx/torpedo/drivers/scheduler/k8s"
	"github.com/portworx/torpedo/drivers/scheduler/spec"
	"github.com/portworx/torpedo/drivers/volume"
	appsapi "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/portworx/torpedo/pkg/email"
	"github.com/portworx/torpedo/pkg/errors"
	"github.com/sirupsen/logrus"
)

const (
	subject = "Torpedo Longevity Report"
	from    = "wilkins@portworx.com"

	// EmailRecipientsConfigMapField is field in config map whose value is comma
	// seperated list of email IDs which will receive email notifications about longevity
	EmailRecipientsConfigMapField = "emailRecipients"
	// DefaultEmailRecipient is list of email IDs that will recei
	// ve email
	// notifications when no EmailRecipientsConfigMapField field present in configMap
	DefaultEmailRecipient = "test@portworx.com"
	// SendGridEmailAPIKeyField is field in config map which stores the SendGrid Email API key
	SendGridEmailAPIKeyField = "sendGridAPIKey"
)

const (
	validateReplicationUpdateTimeout = 2 * time.Hour
	errorChannelSize                 = 50
)

// EmailRecipients list of email IDs to send email to
var EmailRecipients []string

// RunningTriggers map of events and corresponding interval
var RunningTriggers map[string]time.Duration

// ChaosMap stores mapping between test trigger and its chaos level.
var ChaosMap map[string]int

// SendGridEmailAPIKey holds API key used to interact
// with SendGrid Email APIs
var SendGridEmailAPIKey string

//backupCounter holds the iteration of TriggerBacku
var backupCounter = 0

// restoreCounter holds the iteration of TriggerRestore
var restoreCounter = 0

// newNamespaceCounter holds the count of current namespace
var newNamespaceCounter = 0

// Event describes type of test trigger
type Event struct {
	ID   string
	Type string
}

// EventRecord recodes which event took
// place at what time with what outcome
type EventRecord struct {
	Event   Event
	Start   string
	End     string
	Outcome []error
}

// eventRing is circular buffer to store
// events for sending email notifications
var eventRing *ring.Ring

// emailRecords stores events for rendering
// email template
type emailRecords struct {
	Records []EventRecord
}

type emailData struct {
	MasterIP     []string
	NodeInfo     []nodeInfo
	EmailRecords emailRecords
	TriggersInfo []triggerInfo
}

type nodeInfo struct {
	MgmtIP    string
	NodeName  string
	PxVersion string
	Status    string
}

type triggerInfo struct {
	Name     string
	Duration time.Duration
}

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

// ProcessErrorWithMessage updates outcome and expects no error
func ProcessErrorWithMessage(event *EventRecord, err error, desc string) {
	UpdateOutcome(event, err)
	expect(err).NotTo(haveOccurred(), desc)
}

const (
	// DeployApps installs new apps
	DeployApps = "deployApps"
	// HAIncrease performs repl-add
	HAIncrease = "haIncrease"
	// HADecrease performs repl-reduce
	HADecrease = "haDecrease"
	// AppTaskDown deletes application task for all contexts
	AppTaskDown = "appTaskDown"
	// RestartVolDriver restart volume driver
	RestartVolDriver = "restartVolDriver"
	// CrashVolDriver crashes volume driver
	CrashVolDriver = "crashVolDriver"
	// RebootNode reboots all nodes one by one
	RebootNode = "rebootNode"
	// CrashNode crashes all nodes one by one
	CrashNode = "crashNode"
	//VolumeClone Clones volumes
	VolumeClone = "volumeClone"
	// VolumeResize increases volume size
	VolumeResize = "volumeResize"
	// VolumesDelete deletes the columes of the context
	VolumesDelete = "volumesDelete"
	// CloudSnapShot takes local snap shot of the volumes
	// CloudSnapShot takes cloud snapshot of the volumes
	CloudSnapShot = "cloudSnapShot"
	// LocalSnapShot takes local snapshot of the volumes
	LocalSnapShot = "localSnapShot"
	// DeleteLocalSnapShot deletes local snapshots of the volumes
	DeleteLocalSnapShot = "deleteLocalSnapShot"
	// EmailReporter notifies via email outcome of past events
	EmailReporter = "emailReporter"
	// CoreChecker checks if any cores got generated
	CoreChecker = "coreChecker"
	// PoolResizeDisk resize storage pool using resize-disk
	PoolResizeDisk = "poolResizeDisk"
	// PoolAddDisk resize storage pool using add-disk
	PoolAddDisk = "poolAddDisk"
	// BackupAllApps Perform backups of all deployed apps
	BackupAllApps = "backupAllApps"
	// BackupScheduleAll Creates and deletes namespaces and checks a scheduled backup for inclusion
	BackupScheduleAll = "backupScheduleAll"
	// BackupScheduleScale Scales apps up and down and checks scheduled backups
	BackupScheduleScale = "backupScheduleScale"
	// TestInspectBackup Inspect a backup
	TestInspectBackup = "inspectBackup"
	// TestInspectRestore Inspect a restore
	TestInspectRestore = "inspectRestore"
	// TestDeleteBackup Delete a backup
	TestDeleteBackup = "deleteBackup"
	//BackupSpecificResource backs up a specified resource
	BackupSpecificResource = "backupSpecificResource"
	// RestoreNamespace restores a single namespace from a backup
	RestoreNamespace = "restoreNamespace"
	//BackupSpecificResourceOnCluster backs up all of a resource type on the cluster
	BackupSpecificResourceOnCluster = "backupSpecificResourceOnCluster"
	//BackupUsingLabelOnCluster backs up resources on a cluster using a specific label
	BackupUsingLabelOnCluster = "backupUsingLabelOnCluster"
	//BackupRestartPX restarts Portworx during a backup
	BackupRestartPX = "backupRestartPX"
	//BackupRestartNode restarts a node with PX during a backup
	BackupRestartNode = "backupRestartNode"
	// BackupDeleteBackupPod deletes px-backup pod during a backup
	BackupDeleteBackupPod = "backupDeleteBackupPod"
	// BackupScaleMongo deletes px-backup pod during a backup
	BackupScaleMongo = "backupScaleMongo"
	// UpgradeStork  upgrade stork version based on PX and k8s version
	UpgradeStork = "upgradeStork"
)

// TriggerCoreChecker checks if any cores got generated
func TriggerCoreChecker(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {
	defer ginkgo.GinkgoRecover()
	event := &EventRecord{
		Event: Event{
			ID:   GenerateUUID(),
			Type: CoreChecker,
		},
		Start:   time.Now().Format(time.RFC1123),
		Outcome: []error{},
	}
	defer func() {
		event.End = time.Now().Format(time.RFC1123)
		*recordChan <- event
	}()

	context("checking for core files...", func() {
		Step("verifying if core files are present on each node", func() {
			nodes := node.GetWorkerNodes()
			expect(nodes).NotTo(beEmpty())
			for _, n := range nodes {
				if !n.IsStorageDriverInstalled {
					continue
				}
				logrus.Infof("looking for core files on node %s", n.Name)
				file, err := Inst().N.SystemCheck(n, node.ConnectionOpts{
					Timeout:         2 * time.Minute,
					TimeBeforeRetry: 10 * time.Second,
				})
				UpdateOutcome(event, err)

				if len(file) != 0 {
					UpdateOutcome(event, fmt.Errorf("[%s] found on node [%s]", file, n.Name))
				}
			}
		})
	})
}

// TriggerDeployNewApps deploys applications in separate namespaces
func TriggerDeployNewApps(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {
	defer ginkgo.GinkgoRecover()
	event := &EventRecord{
		Event: Event{
			ID:   GenerateUUID(),
			Type: DeployApps,
		},
		Start:   time.Now().Format(time.RFC1123),
		Outcome: []error{},
	}

	defer func() {
		event.End = time.Now().Format(time.RFC1123)
		*recordChan <- event
	}()
	errorChan := make(chan error, errorChannelSize)

	Step("Deploy applications", func() {
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			newContexts := ScheduleApplications(fmt.Sprintf("longevity-%d", i), &errorChan)
			*contexts = append(*contexts, newContexts...)
		}

		for _, ctx := range *contexts {
			logrus.Infof("Validating context: %v", ctx.App.Key)
			ctx.SkipVolumeValidation = false
			ValidateContext(ctx, &errorChan)
			for err := range errorChan {
				logrus.Infof("Error: %v", err)
				UpdateOutcome(event, err)
			}
		}
	})
}

// TriggerHAIncrease peforms repl-add on all volumes of given contexts
func TriggerHAIncrease(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {
	defer ginkgo.GinkgoRecover()
	event := &EventRecord{
		Event: Event{
			ID:   GenerateUUID(),
			Type: HAIncrease,
		},
		Start:   time.Now().Format(time.RFC1123),
		Outcome: []error{},
	}

	defer func() {
		event.End = time.Now().Format(time.RFC1123)
		*recordChan <- event
	}()

	expReplMap := make(map[*volume.Volume]int64)
	Step("get volumes for all apps in test and increase replication factor", func() {
		time.Sleep(10 * time.Minute)
		for _, ctx := range *contexts {
			var appVolumes []*volume.Volume
			var err error
			Step(fmt.Sprintf("get volumes for %s app", ctx.App.Key), func() {
				appVolumes, err = Inst().S.GetVolumes(ctx)
				UpdateOutcome(event, err)
				if len(appVolumes) == 0 {
					UpdateOutcome(event, fmt.Errorf("found no volumes for app %s", ctx.App.Key))
				}
			})
			opts := volume.Options{
				ValidateReplicationUpdateTimeout: validateReplicationUpdateTimeout,
			}
			for _, v := range appVolumes {
				MaxRF := Inst().V.GetMaxReplicationFactor()

				Step(
					fmt.Sprintf("repl increase volume driver %s on app %s's volume: %v",
						Inst().V.String(), ctx.App.Key, v),
					func() {
						errExpected := false
						currRep, err := Inst().V.GetReplicationFactor(v)
						UpdateOutcome(event, err)

						// GetMaxReplicationFactory is hardcoded to 3
						// if it increases repl 3 to an aggregated 2 volume, it will fail
						// because it would require 6 worker nodes, since
						// number of nodes required = aggregation level * replication factor
						currAggr, err := Inst().V.GetAggregationLevel(v)
						UpdateOutcome(event, err)
						storageNodes, err := GetStorageNodes()
						UpdateOutcome(event, err)

						//Calulating Max Replication Factor allowed
						MaxRF = int64(len(storageNodes)) / currAggr

						expRF := currRep + 1

						if expRF > 3 || expRF > MaxRF {
							errExpected = true
							expRF = currRep
						}
						logrus.Infof("Expected Replication factor %v", expRF)
						logrus.Infof("Max Replication factor %v", MaxRF)
						expReplMap[v] = expRF
						if !errExpected {
							err = Inst().V.SetReplicationFactor(v, expRF, nil, opts)
							UpdateOutcome(event, err)
						} else {
							logrus.Infof("cannot peform HA increase as new repl factor value is greater than max allowed %v", MaxRF)
							UpdateOutcome(event, fmt.Errorf("cannot peform HA increase as new repl factor value is greater than max allowed %v", MaxRF))
						}
					})
				Step(
					fmt.Sprintf("validate successful repl increase on app %s's volume: %v",
						ctx.App.Key, v),
					func() {
						newRepl, err := Inst().V.GetReplicationFactor(v)
						UpdateOutcome(event, err)

						if newRepl != expReplMap[v] {
							err = fmt.Errorf("volume [%s] has invalid repl value for volume. Expected:%d Actual:%d", v.Name, expReplMap[v], newRepl)
							UpdateOutcome(event, err)
						} else {
							logrus.Infof("Successfully validated repl for volume %s", v.Name)
						}
						logrus.Infof("repl increase validation completed on app %s", v.Name)
					})
			}
			Step(fmt.Sprintf("validating context after increasing HA for app: %s",
				ctx.App.Key), func() {
				errorChan := make(chan error, errorChannelSize)
				ctx.SkipVolumeValidation = true
				logrus.Infof("Context Validation after increasing HA started for  %s", ctx.App.Key)
				ValidateContext(ctx, &errorChan)
				logrus.Infof("Context Validation after increasing HA is completed for  %s", ctx.App.Key)
				for err := range errorChan {
					UpdateOutcome(event, err)
					logrus.Infof("Context outcome after increasing HA is updated for  %s", ctx.App.Key)
				}
			})
		}
	})
}

// TriggerHADecrease performs repl-reduce on all volumes of given contexts
func TriggerHADecrease(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {
	defer ginkgo.GinkgoRecover()
	event := &EventRecord{
		Event: Event{
			ID:   GenerateUUID(),
			Type: HADecrease,
		},
		Start:   time.Now().Format(time.RFC1123),
		Outcome: []error{},
	}

	defer func() {
		event.End = time.Now().Format(time.RFC1123)
		*recordChan <- event
	}()

	expReplMap := make(map[*volume.Volume]int64)
	Step("get volumes for all apps in test and decrease replication factor", func() {
		for _, ctx := range *contexts {
			var appVolumes []*volume.Volume
			var err error
			Step(fmt.Sprintf("get volumes for %s app", ctx.App.Key), func() {
				appVolumes, err = Inst().S.GetVolumes(ctx)
				UpdateOutcome(event, err)
				if len(appVolumes) == 0 {
					UpdateOutcome(event, fmt.Errorf("found no volumes for app %s", ctx.App.Key))
				}
			})
			opts := volume.Options{
				ValidateReplicationUpdateTimeout: validateReplicationUpdateTimeout,
			}
			for _, v := range appVolumes {
				MinRF := Inst().V.GetMinReplicationFactor()

				Step(
					fmt.Sprintf("repl decrease volume driver %s on app %s's volume: %v",
						Inst().V.String(), ctx.App.Key, v),
					func() {
						errExpected := false
						currRep, err := Inst().V.GetReplicationFactor(v)
						UpdateOutcome(event, err)
						expRF := currRep - 1

						if expRF < MinRF {
							errExpected = true
							expRF = currRep
						}
						expReplMap[v] = expRF
						logrus.Infof("Expected Replication factor %v", expRF)
						logrus.Infof("Min Replication factor %v", MinRF)
						if !errExpected {
							err = Inst().V.SetReplicationFactor(v, currRep-1, nil, opts)
							UpdateOutcome(event, err)
						} else {
							logrus.Infof("cannot perfomr HA reduce as new repl factor is less than minimum value %v ", MinRF)
							UpdateOutcome(event, fmt.Errorf("cannot perfomr HA reduce as new repl factor is less than minimum value %v ", MinRF))
						}

					})
				Step(
					fmt.Sprintf("validate successful repl decrease on app %s's volume: %v",
						ctx.App.Key, v),
					func() {
						newRepl, err := Inst().V.GetReplicationFactor(v)
						UpdateOutcome(event, err)

						if newRepl != expReplMap[v] {
							UpdateOutcome(event, fmt.Errorf("volume [%s] has invalid repl value . Expected:%d Actual:%d", v.Name, expReplMap[v], newRepl))
						}
						logrus.Infof("repl decrease validation completed on app %s", v.Name)

					})
			}
			Step(fmt.Sprintf("validating context after reducing HA for app: %s",
				ctx.App.Key), func() {
				errorChan := make(chan error, errorChannelSize)
				ctx.SkipVolumeValidation = true
				logrus.Infof("Context Validation after reducing HA started for  %s", ctx.App.Key)
				ValidateContext(ctx, &errorChan)
				logrus.Infof("Context Validation after reducing HA is completed for  %s", ctx.App.Key)
				for err := range errorChan {
					UpdateOutcome(event, err)
					logrus.Infof("Context outcome after reducing HA is updated for  %s", ctx.App.Key)
				}
			})
		}
	})
}

// TriggerAppTaskDown deletes application task for all contexts
func TriggerAppTaskDown(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {
	defer ginkgo.GinkgoRecover()
	event := &EventRecord{
		Event: Event{
			ID:   GenerateUUID(),
			Type: AppTaskDown,
		},
		Start:   time.Now().Format(time.RFC1123),
		Outcome: []error{},
	}

	defer func() {
		event.End = time.Now().Format(time.RFC1123)
		*recordChan <- event
	}()

	for _, ctx := range *contexts {
		Step(fmt.Sprintf("delete tasks for app: [%s]", ctx.App.Key), func() {
			err := Inst().S.DeleteTasks(ctx, nil)
			UpdateOutcome(event, err)
		})

		Step(fmt.Sprintf("validating context after delete tasks for app: [%s]",
			ctx.App.Key), func() {
			errorChan := make(chan error, errorChannelSize)
			ctx.SkipVolumeValidation = false
			ValidateContext(ctx, &errorChan)
			for err := range errorChan {
				UpdateOutcome(event, err)
			}
		})
	}
}

// TriggerCrashVolDriver crashes vol driver
func TriggerCrashVolDriver(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {
	defer ginkgo.GinkgoRecover()
	event := &EventRecord{
		Event: Event{
			ID:   GenerateUUID(),
			Type: CrashVolDriver,
		},
		Start:   time.Now().Format(time.RFC1123),
		Outcome: []error{},
	}

	defer func() {
		event.End = time.Now().Format(time.RFC1123)
		*recordChan <- event
	}()
	Step("crash volume driver in all nodes", func() {
		for _, appNode := range node.GetStorageDriverNodes() {
			Step(
				fmt.Sprintf("crash volume driver %s on node: %v",
					Inst().V.String(), appNode.Name),
				func() {
					taskStep := fmt.Sprintf("crash volume driver on node: %s",
						appNode.MgmtIp)
					event.Event.Type += "<br>" + taskStep
					errorChan := make(chan error, errorChannelSize)
					CrashVolDriverAndWait([]node.Node{appNode}, &errorChan)
					for err := range errorChan {
						UpdateOutcome(event, err)
					}
				})
		}
	})
}

// TriggerRestartVolDriver restarts volume driver and validates app
func TriggerRestartVolDriver(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {
	defer ginkgo.GinkgoRecover()
	event := &EventRecord{
		Event: Event{
			ID:   GenerateUUID(),
			Type: RestartVolDriver,
		},
		Start:   time.Now().Format(time.RFC1123),
		Outcome: []error{},
	}

	defer func() {
		event.End = time.Now().Format(time.RFC1123)
		*recordChan <- event
	}()
	Step("get nodes bounce volume driver", func() {
		for _, appNode := range node.GetStorageDriverNodes() {
			Step(
				fmt.Sprintf("stop volume driver %s on node: %s",
					Inst().V.String(), appNode.Name),
				func() {
					taskStep := fmt.Sprintf("stop volume driver on node: %s.",
						appNode.MgmtIp)
					event.Event.Type += "<br>" + taskStep
					errorChan := make(chan error, errorChannelSize)
					StopVolDriverAndWait([]node.Node{appNode}, &errorChan)
					for err := range errorChan {
						UpdateOutcome(event, err)
					}
				})

			Step(
				fmt.Sprintf("starting volume %s driver on node %s",
					Inst().V.String(), appNode.Name),
				func() {
					taskStep := fmt.Sprintf("starting volume driver on node: %s.",
						appNode.MgmtIp)
					event.Event.Type += "<br>" + taskStep
					errorChan := make(chan error, errorChannelSize)
					StartVolDriverAndWait([]node.Node{appNode}, &errorChan)
					for err := range errorChan {
						UpdateOutcome(event, err)
					}
				})

			Step("Giving few seconds for volume driver to stabilize", func() {
				time.Sleep(20 * time.Second)
			})

			for _, ctx := range *contexts {
				Step(fmt.Sprintf("RestartVolDriver: validating app [%s]", ctx.App.Key), func() {
					errorChan := make(chan error, errorChannelSize)
					ValidateContext(ctx, &errorChan)
					for err := range errorChan {
						UpdateOutcome(event, err)
					}
				})
			}
		}
	})
}

// TriggerRebootNodes reboots node on which apps are running
func TriggerRebootNodes(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {
	defer ginkgo.GinkgoRecover()
	event := &EventRecord{
		Event: Event{
			ID:   GenerateUUID(),
			Type: RebootNode,
		},
		Start:   time.Now().Format(time.RFC1123),
		Outcome: []error{},
	}

	defer func() {
		event.End = time.Now().Format(time.RFC1123)
		*recordChan <- event
	}()

	Step("get all nodes and reboot one by one", func() {
		nodesToReboot := node.GetWorkerNodes()

		// Reboot node and check driver status
		Step(fmt.Sprintf("reboot node one at a time from the node(s): %v", nodesToReboot), func() {
			// TODO: Below is the same code from existing nodeReboot test
			for _, n := range nodesToReboot {
				if n.IsStorageDriverInstalled {
					Step(fmt.Sprintf("reboot node: %s", n.Name), func() {
						taskStep := fmt.Sprintf("reboot node: %s.", n.MgmtIp)
						event.Event.Type += "<br>" + taskStep
						err := Inst().N.RebootNode(n, node.RebootNodeOpts{
							Force: true,
							ConnectionOpts: node.ConnectionOpts{
								Timeout:         1 * time.Minute,
								TimeBeforeRetry: 5 * time.Second,
							},
						})
						UpdateOutcome(event, err)
					})

					Step(fmt.Sprintf("wait for node: %s to be back up", n.Name), func() {
						err := Inst().N.TestConnection(n, node.ConnectionOpts{
							Timeout:         15 * time.Minute,
							TimeBeforeRetry: 10 * time.Second,
						})
						UpdateOutcome(event, err)
					})

					Step(fmt.Sprintf("wait for volume driver to stop on node: %v", n.Name), func() {
						err := Inst().V.WaitDriverDownOnNode(n)
						UpdateOutcome(event, err)
					})

					Step(fmt.Sprintf("wait to scheduler: %s and volume driver: %s to start",
						Inst().S.String(), Inst().V.String()), func() {

						err := Inst().S.IsNodeReady(n)
						UpdateOutcome(event, err)

						err = Inst().V.WaitDriverUpOnNode(n, Inst().DriverStartTimeout)
						UpdateOutcome(event, err)
					})

					Step("validate apps", func() {
						for _, ctx := range *contexts {
							Step(fmt.Sprintf("RebootNode: validating app [%s]", ctx.App.Key), func() {
								errorChan := make(chan error, errorChannelSize)
								ValidateContext(ctx, &errorChan)
								for err := range errorChan {
									UpdateOutcome(event, err)
								}
							})
						}
					})
				}
			}
		})
	})
}

// TriggerCrashNodes crashes Worker nodes
func TriggerCrashNodes(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {
	defer ginkgo.GinkgoRecover()
	event := &EventRecord{
		Event: Event{
			ID:   GenerateUUID(),
			Type: CrashNode,
		},
		Start:   time.Now().Format(time.RFC1123),
		Outcome: []error{},
	}

	defer func() {
		event.End = time.Now().Format(time.RFC1123)
		*recordChan <- event
	}()

	Step("get all nodes and crash one by one", func() {
		nodesToCrash := node.GetWorkerNodes()

		// Crash node and check driver status
		Step(fmt.Sprintf("crash node one at a time from the node(s): %v", nodesToCrash), func() {
			// TODO: Below is the same code from existing nodeCrash test
			for _, n := range nodesToCrash {
				if n.IsStorageDriverInstalled {
					Step(fmt.Sprintf("crash node: %s", n.Name), func() {
						taskStep := fmt.Sprintf("crash node: %s.", n.MgmtIp)
						event.Event.Type += "<br>" + taskStep
						err := Inst().N.CrashNode(n, node.CrashNodeOpts{
							Force: true,
							ConnectionOpts: node.ConnectionOpts{
								Timeout:         1 * time.Minute,
								TimeBeforeRetry: 5 * time.Second,
							},
						})
						UpdateOutcome(event, err)
					})

					Step(fmt.Sprintf("wait for node: %s to be back up", n.Name), func() {
						err := Inst().N.TestConnection(n, node.ConnectionOpts{
							Timeout:         15 * time.Minute,
							TimeBeforeRetry: 10 * time.Second,
						})
						UpdateOutcome(event, err)
					})

					Step(fmt.Sprintf("wait to scheduler: %s and volume driver: %s to start",
						Inst().S.String(), Inst().V.String()), func() {

						err := Inst().S.IsNodeReady(n)
						UpdateOutcome(event, err)

						err = Inst().V.WaitDriverUpOnNode(n, Inst().DriverStartTimeout)
						UpdateOutcome(event, err)
					})

					Step("validate apps", func() {
						for _, ctx := range *contexts {
							Step(fmt.Sprintf("CrashNode: validating app [%s]", ctx.App.Key), func() {
								errorChan := make(chan error, errorChannelSize)
								ValidateContext(ctx, &errorChan)
								for err := range errorChan {
									UpdateOutcome(event, err)
								}
							})
						}
					})
				}
			}
		})
	})
}

// TriggerVolumeClone clones all volumes, validates and destorys the clone
func TriggerVolumeClone(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {
	defer ginkgo.GinkgoRecover()
	event := &EventRecord{
		Event: Event{
			ID:   GenerateUUID(),
			Type: VolumeClone,
		},
		Start:   time.Now().Format(time.RFC1123),
		Outcome: []error{},
	}
	defer func() {
		event.End = time.Now().Format(time.RFC1123)
		*recordChan <- event
	}()
	Step("get volumes for all apps in test and clone them", func() {
		for _, ctx := range *contexts {
			var appVolumes []*volume.Volume
			var err error
			Step(fmt.Sprintf("get volumes for %s app", ctx.App.Key), func() {
				appVolumes, err = Inst().S.GetVolumes(ctx)
				UpdateOutcome(event, err)
				if len(appVolumes) == 0 {
					UpdateOutcome(event, fmt.Errorf("found no volumes for app %s", ctx.App.Key))
				}
			})
			for _, vol := range appVolumes {
				var clonedVolID string
				Step(fmt.Sprintf("Clone Volume %s", vol.Name), func() {
					logrus.Infof("Calling CloneVolume()...")
					clonedVolID, err = Inst().V.CloneVolume(vol.ID)
					UpdateOutcome(event, err)
				})
				Step(
					fmt.Sprintf("Validate successful clone %s", clonedVolID), func() {
						params := make(map[string]string)
						if Inst().ConfigMap != "" {
							params["auth-token"], err = Inst().S.GetTokenFromConfigMap(Inst().ConfigMap)
							UpdateOutcome(event, err)
						}
						err = Inst().V.ValidateCreateVolume(clonedVolID, params)
						UpdateOutcome(event, err)
					})
				Step(
					fmt.Sprintf("cleanup the cloned volume %s", clonedVolID), func() {
						err = Inst().V.DeleteVolume(clonedVolID)
						UpdateOutcome(event, err)
					})
			}
		}
	})
}

// TriggerVolumeResize increases all volumes size and validates app
func TriggerVolumeResize(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {
	defer ginkgo.GinkgoRecover()
	event := &EventRecord{
		Event: Event{
			ID:   GenerateUUID(),
			Type: VolumeResize,
		},
		Start:   time.Now().Format(time.RFC1123),
		Outcome: []error{},
	}

	defer func() {
		event.End = time.Now().Format(time.RFC1123)
		*recordChan <- event
	}()
	Step("get volumes for all apps in test and update size", func() {
		for _, ctx := range *contexts {
			var appVolumes []*volume.Volume
			var err error
			Step(fmt.Sprintf("get volumes for %s app", ctx.App.Key), func() {
				appVolumes, err = Inst().S.GetVolumes(ctx)
				logrus.Infof("len of app volumes is : %v", len(appVolumes))
				UpdateOutcome(event, err)
				if len(appVolumes) == 0 {
					UpdateOutcome(event, fmt.Errorf("found no volumes for app %s", ctx.App.Key))
				}
			})
			var requestedVols []*volume.Volume
			Step(
				fmt.Sprintf("increase volume size %s on app %s's volumes: %v",
					Inst().V.String(), ctx.App.Key, appVolumes),
				func() {
					logrus.Info("increasing volume size")
					requestedVols, err = Inst().S.ResizeVolume(ctx, Inst().ConfigMap)
					UpdateOutcome(event, err)
				})
			Step(
				fmt.Sprintf("validate successful volume size increase on app %s's volumes: %v",
					ctx.App.Key, appVolumes),
				func() {
					for _, v := range requestedVols {
						// Need to pass token before validating volume
						params := make(map[string]string)
						if Inst().ConfigMap != "" {
							params["auth-token"], err = Inst().S.GetTokenFromConfigMap(Inst().ConfigMap)
							UpdateOutcome(event, err)
						}
						err := Inst().V.ValidateUpdateVolume(v, params)
						UpdateOutcome(event, err)
					}
				})
		}
	})
}

// TriggerLocalSnapShot takes local snapshots of the volumes and validates snapshot
func TriggerLocalSnapShot(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {

	defer ginkgo.GinkgoRecover()
	uuid := GenerateUUID()
	event := &EventRecord{
		Event: Event{
			ID:   uuid,
			Type: LocalSnapShot,
		},
		Start:   time.Now().Format(time.RFC1123),
		Outcome: []error{},
	}

	defer func() {
		event.End = time.Now().Format(time.RFC1123)
		*recordChan <- event
	}()

	Step("Create and Validate LocalSnapshots", func() {

		for _, ctx := range *contexts {
			var appVolumes []*volume.Volume
			var err error
			if strings.Contains(ctx.App.Key, "localsnap") {

				appNamespace := ctx.App.Key + "-" + ctx.UID
				logrus.Infof("Namespace : %v", appNamespace)

				Step(fmt.Sprintf("create schedule policy for %s app", ctx.App.Key), func() {

					policyName := "localintervalpolicy"

					schedPolicy, err := storkops.Instance().GetSchedulePolicy(policyName)
					if err != nil {
						retain := 2
						interval := getCloudSnapInterval(LocalSnapShot)
						logrus.Infof("Creating a interval schedule policy %v with interval %v minutes", policyName, interval)
						schedPolicy = &storkv1.SchedulePolicy{
							ObjectMeta: meta_v1.ObjectMeta{
								Name: policyName,
							},
							Policy: storkv1.SchedulePolicyItem{
								Interval: &storkv1.IntervalPolicy{
									Retain:          storkv1.Retain(retain),
									IntervalMinutes: interval,
								},
							}}

						_, err = storkops.Instance().CreateSchedulePolicy(schedPolicy)
						logrus.Infof("Waiting for 10 mins for Snapshots to be completed")
						time.Sleep(10 * time.Minute)
					} else {
						logrus.Infof("schedPolicy is %v already exists", schedPolicy.Name)
					}

					UpdateOutcome(event, err)
				})
				logrus.Infof("Waiting for 2 mins for Snapshots to be completed")
				time.Sleep(2 * time.Minute)

				Step(fmt.Sprintf("get volumes for %s app", ctx.App.Key), func() {
					appVolumes, err = Inst().S.GetVolumes(ctx)
					UpdateOutcome(event, err)
					if len(appVolumes) == 0 {
						UpdateOutcome(event, fmt.Errorf("found no volumes for app %s", ctx.App.Key))
					}
				})
				logrus.Infof("Got volume count : %v", len(appVolumes))

				snapshotScheduleRetryInterval := 10 * time.Second
				snapshotScheduleRetryTimeout := 3 * time.Minute

				for _, v := range appVolumes {
					snapshotScheduleName := v.Name + "-interval-schedule"
					logrus.Infof("snapshotScheduleName : %v for volume: %s", snapshotScheduleName, v.Name)
					snapStatuses, err := storkops.Instance().ValidateSnapshotSchedule(snapshotScheduleName,
						appNamespace,
						snapshotScheduleRetryTimeout,
						snapshotScheduleRetryInterval)
					if err == nil {
						for k, v := range snapStatuses {
							logrus.Infof("Policy Type: %v", k)
							for _, e := range v {
								logrus.Infof("ScheduledVolumeSnapShot Name: %v", e.Name)
								logrus.Infof("ScheduledVolumeSnapShot status: %v", e.Status)
								snapData, err := Inst().S.GetSnapShotData(ctx, e.Name, appNamespace)
								UpdateOutcome(event, err)

								snapType := snapData.Spec.PortworxSnapshot.SnapshotType
								logrus.Infof("Snapshot Type: %v", snapType)
								if snapType != "local" {
									err = &scheduler.ErrFailedToGetVolumeParameters{
										App:   ctx.App,
										Cause: fmt.Sprintf("Snapshot Type: %s does not match", snapType),
									}
									UpdateOutcome(event, err)

								}

								snapID := snapData.Spec.PortworxSnapshot.SnapshotID
								logrus.Infof("Snapshot ID: %v", snapID)

								if snapData.Spec.VolumeSnapshotDataSource.PortworxSnapshot == nil ||
									len(snapData.Spec.VolumeSnapshotDataSource.PortworxSnapshot.SnapshotID) == 0 {
									err = &scheduler.ErrFailedToGetVolumeParameters{
										App:   ctx.App,
										Cause: fmt.Sprintf("volumesnapshotdata: %s does not have portworx volume source set", snapData.Metadata.Name),
									}
									UpdateOutcome(event, err)
								}
							}
						}

					} else {
						logrus.Infof("Got error while getting volume snapshot status :%v", err.Error())
					}
					UpdateOutcome(event, err)
				}

			}

		}

	})

}

// TriggerDeleteLocalSnapShot deletes local snapshots and snapshot schedules
func TriggerDeleteLocalSnapShot(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {
	defer ginkgo.GinkgoRecover()
	uuid := GenerateUUID()
	event := &EventRecord{
		Event: Event{
			ID:   uuid,
			Type: DeleteLocalSnapShot,
		},
		Start:   time.Now().Format(time.RFC1123),
		Outcome: []error{},
	}

	defer func() {
		event.End = time.Now().Format(time.RFC1123)
		*recordChan <- event
	}()

	Step("Delete Schedule Policy and LocalSnapshots and Validate", func() {

		for _, ctx := range *contexts {
			var appVolumes []*volume.Volume

			if strings.Contains(ctx.App.Key, "localsnap") {

				appNamespace := ctx.App.Key + "-" + ctx.UID
				logrus.Infof("Namespace : %v", appNamespace)
				Step(fmt.Sprintf("delete schedule policy for %s app", ctx.App.Key), func() {

					policyName := "localintervalpolicy"
					err := storkops.Instance().DeleteSchedulePolicy(policyName)
					UpdateOutcome(event, err)
					Step(fmt.Sprintf("get volumes for %s app", ctx.App.Key), func() {
						appVolumes, err = Inst().S.GetVolumes(ctx)
						UpdateOutcome(event, err)
						if len(appVolumes) == 0 {
							UpdateOutcome(event, fmt.Errorf("found no volumes for app %s", ctx.App.Key))
						}
					})
					snapshotScheduleRetryInterval := 10 * time.Second
					snapshotScheduleRetryTimeout := 3 * time.Minute
					for _, v := range appVolumes {
						snapshotScheduleName := v.Name + "-interval-schedule"
						logrus.Infof("snapshotScheduleName : %v for volume: %s", snapshotScheduleName, v.Name)
						snapStatuses, err := storkops.Instance().ValidateSnapshotSchedule(snapshotScheduleName,
							appNamespace,
							snapshotScheduleRetryTimeout,
							snapshotScheduleRetryInterval)

						if err == nil {
							for _, v := range snapStatuses {
								for _, e := range v {
									logrus.Infof("Deleting ScheduledVolumeSnapShot: %v", e.Name)
									err = Inst().S.DeleteSnapShot(ctx, e.Name, appNamespace)
									UpdateOutcome(event, err)

								}
							}
							err = storkops.Instance().DeleteSnapshotSchedule(snapshotScheduleName, appNamespace)
							UpdateOutcome(event, err)

						} else {
							logrus.Infof("Got error while getting volume snapshot status :%v", err.Error())
						}
					}

					snapshotList, err := Inst().S.GetShapShotsInNameSpace(ctx, appNamespace)
					UpdateOutcome(event, err)
					if len(snapshotList.Items) != 0 {
						logrus.Infof("Failed to delete snapshots in namespace %v", appNamespace)
						err = &scheduler.ErrFailedToDeleteTasks{
							App:   ctx.App,
							Cause: fmt.Sprintf("Failed to delete snapshots in namespace %v", appNamespace),
						}
						UpdateOutcome(event, err)

					}

				})
			}
		}

	})

}

// TriggerCloudSnapShot deploy Interval Policy and validates snapshot
func TriggerCloudSnapShot(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {
	defer ginkgo.GinkgoRecover()
	uuid := GenerateUUID()
	event := &EventRecord{
		Event: Event{
			ID:   uuid,
			Type: CloudSnapShot,
		},
		Start:   time.Now().Format(time.RFC1123),
		Outcome: []error{},
	}

	defer func() {
		event.End = time.Now().Format(time.RFC1123)
		*recordChan <- event
	}()

	snapshotScheduleRetryInterval := 10 * time.Second
	snapshotScheduleRetryTimeout := 3 * time.Minute

	Step("Validate Cloud Snaps", func() {

		for _, ctx := range *contexts {
			var appVolumes []*volume.Volume
			var err error
			if strings.Contains(ctx.App.Key, "cloudsnap") {

				appNamespace := ctx.App.Key + "-" + ctx.UID
				logrus.Infof("Namespace : %v", appNamespace)

				Step(fmt.Sprintf("create schedule policy for %s app", ctx.App.Key), func() {

					policyName := "intervalpolicy"

					schedPolicy, err := storkops.Instance().GetSchedulePolicy(policyName)
					if err != nil {
						retain := 2
						interval := getCloudSnapInterval(CloudSnapShot)
						logrus.Infof("Creating a interval schedule policy %v with interval %v minutes", policyName, interval)
						schedPolicy = &storkv1.SchedulePolicy{
							ObjectMeta: meta_v1.ObjectMeta{
								Name: policyName,
							},
							Policy: storkv1.SchedulePolicyItem{
								Interval: &storkv1.IntervalPolicy{
									Retain:          storkv1.Retain(retain),
									IntervalMinutes: interval,
								},
							}}

						_, err = storkops.Instance().CreateSchedulePolicy(schedPolicy)
						logrus.Infof("Waiting for 10 mins for Snapshots to be completed")
						time.Sleep(10 * time.Minute)
					} else {
						logrus.Infof("schedPolicy is %v already exists", schedPolicy.Name)
					}

					UpdateOutcome(event, err)
				})

				Step(fmt.Sprintf("get volumes for %s app", ctx.App.Key), func() {
					appVolumes, err = Inst().S.GetVolumes(ctx)
					UpdateOutcome(event, err)
					if len(appVolumes) == 0 {
						UpdateOutcome(event, fmt.Errorf("found no volumes for app %s", ctx.App.Key))
					}
				})
				logrus.Infof("Got volume count : %v", len(appVolumes))

				for _, v := range appVolumes {
					snapshotScheduleName := v.Name + "-interval-schedule"
					logrus.Infof("snapshotScheduleName : %v for volume: %s", snapshotScheduleName, v.Name)
					snapStatuses, err := storkops.Instance().ValidateSnapshotSchedule(snapshotScheduleName,
						appNamespace,
						snapshotScheduleRetryTimeout,
						snapshotScheduleRetryInterval)
					if err == nil {
						for k, v := range snapStatuses {
							logrus.Infof("Policy Type: %v", k)
							for _, e := range v {
								logrus.Infof("ScheduledVolumeSnapShot Name: %v", e.Name)
								logrus.Infof("ScheduledVolumeSnapShot status: %v", e.Status)
							}
						}
					} else {
						logrus.Infof("Got error while getting volume snapshot status :%v", err.Error())
					}
					UpdateOutcome(event, err)
				}

			}

		}

	})

}

// TriggerVolumeDelete delete the volumes
func TriggerVolumeDelete(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {
	defer ginkgo.GinkgoRecover()
	uuid := GenerateUUID()
	event := &EventRecord{
		Event: Event{
			ID:   uuid,
			Type: VolumesDelete,
		},
		Start:   time.Now().Format(time.RFC1123),
		Outcome: []error{},
	}

	defer func() {
		event.End = time.Now().Format(time.RFC1123)
		*recordChan <- event
	}()

	Step("Validate Delete Volumes", func() {
		opts := make(map[string]bool)

		for _, ctx := range *contexts {

			opts[SkipClusterScopedObjects] = true
			options := mapToVolumeOptions(opts)

			// Tear down storage objects
			vols := DeleteVolumes(ctx, options)

			// Tear down application
			Step(fmt.Sprintf("start destroying %s app", ctx.App.Key), func() {
				err := Inst().S.Destroy(ctx, opts)
				UpdateOutcome(event, err)
			})

			ValidateVolumesDeleted(ctx.App.Key, vols)
		}
		*contexts = nil
		TriggerDeployNewApps(contexts, recordChan)
	})
}

func createCloudCredential(req *api.CloudCredentialCreateRequest) error {
	backupDriver := Inst().Backup
	ctx, err := backup.GetPxCentralAdminCtx()

	if err != nil {
		return err
	}
	_, err = backupDriver.CreateCloudCredential(ctx, req)

	if err != nil {
		return err
	}

	return nil

}

func getCreateCloudCredentialRequest(uid string) (*api.CloudCredentialCreateRequest, error) {
	req := &api.CloudCredentialCreateRequest{
		CreateMetadata: &api.CreateMetadata{
			Name:  "CloudSnapTest",
			Uid:   uid,
			OrgId: "CloudSnapTest",
		},
		CloudCredential: &api.CloudCredentialInfo{},
	}

	provider, ok := os.LookupEnv("OBJECT_STORE_PROVIDER")
	logrus.Infof("Provider for credentail secret is %s", provider)
	if !ok {
		return nil, &errors.ErrNotFound{
			ID:   "OBJECT_STORE_PROVIDER",
			Type: "Environment Variable",
		}
	}
	logrus.Infof("Provider for credentail secret is %s", provider)

	switch provider {
	case "aws":
		s3AccessKey := os.Getenv("AWS_ACCESS_KEY_ID")
		s3SecretKey := os.Getenv("AWS_SECRET_ACCESS_KEY")
		req.CloudCredential.Type = api.CloudCredentialInfo_AWS
		req.CloudCredential.Config = &api.CloudCredentialInfo_AwsConfig{
			AwsConfig: &api.AWSConfig{
				AccessKey: s3AccessKey,
				SecretKey: s3SecretKey,
			},
		}
	case "azure":
		azureAccountName, ok := os.LookupEnv("AZURE_ACCOUNT_NAME")
		if !ok {
			return nil, &errors.ErrNotFound{
				ID:   "AZURE_ACCOUNT_NAME",
				Type: "Environment Variable",
			}
		}

		azureAccountKey, ok := os.LookupEnv("AZURE_ACCOUNT_KEY")
		if !ok {
			return nil, &errors.ErrNotFound{
				ID:   "AZURE_ACCOUNT_NAME",
				Type: "Environment Variable",
			}
		}
		clientSecret, ok := os.LookupEnv("AZURE_CLIENT_SECRET")
		if !ok {
			return nil, &errors.ErrNotFound{
				ID:   "AZURE_CLIENT_SECRET",
				Type: "Environment Variable",
			}
		}
		clientID, ok := os.LookupEnv("AZURE_CLIENT_ID")
		if !ok {
			return nil, &errors.ErrNotFound{
				ID:   "AZURE_CLIENT_ID",
				Type: "Environment Variable",
			}
		}
		tenantID, ok := os.LookupEnv("AZURE_TENANT_ID")
		if !ok {
			return nil, &errors.ErrNotFound{
				ID:   "AZURE_TENANT_ID",
				Type: "Environment Variable",
			}
		}
		subscriptionID, ok := os.LookupEnv("AZURE_SUBSCRIPTION_ID")
		if !ok {
			return nil, &errors.ErrNotFound{
				ID:   "AZURE_SUBSCRIPTION_ID",
				Type: "Environment Variable",
			}
		}
		req.CloudCredential.Type = api.CloudCredentialInfo_Azure
		req.CloudCredential.Config = &api.CloudCredentialInfo_AzureConfig{
			AzureConfig: &api.AzureConfig{
				AccountName:    azureAccountName,
				AccountKey:     azureAccountKey,
				ClientSecret:   clientSecret,
				ClientId:       clientID,
				TenantId:       tenantID,
				SubscriptionId: subscriptionID,
			},
		}
		req.CloudCredential.Type = api.CloudCredentialInfo_Azure
	case "google":
		projectID, ok := os.LookupEnv("GCP_PROJECT_ID_")
		if !ok {
			return nil, &errors.ErrNotFound{
				ID:   "GCP_PROJECT_ID_",
				Type: "Environment Variable",
			}
		}
		accountKey, ok := os.LookupEnv("GCP_ACCOUNT_KEY")
		if !ok {
			return nil, &errors.ErrNotFound{
				ID:   "GCP_ACCOUNT_KEY",
				Type: "Environment Variable",
			}
		}
		req.CloudCredential.Type = api.CloudCredentialInfo_Google
		req.CloudCredential.Config = &api.CloudCredentialInfo_GoogleConfig{
			GoogleConfig: &api.GoogleConfig{
				ProjectId: projectID,
				JsonKey:   accountKey,
			},
		}
	default:
		logrus.Errorf("provider needs to be either aws, azure or google")
	}
	return req, nil
}

// CollectEventRecords collects eventRecords from channel
// and stores in buffer for future email notifications
func CollectEventRecords(recordChan *chan *EventRecord) {
	eventRing = ring.New(100)
	for eventRecord := range *recordChan {
		eventRing.Value = eventRecord
		eventRing = eventRing.Next()
	}
}

// TriggerEmailReporter sends email with all reported errors
func TriggerEmailReporter(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {
	// emailRecords stores events to be notified

	emailData := emailData{}
	logrus.Infof("Generating email report: %s", time.Now().Format(time.RFC1123))

	var masterNodeList []string
	var pxStatus string

	for _, n := range node.GetMasterNodes() {
		masterNodeList = append(masterNodeList, n.Addresses...)
	}
	emailData.MasterIP = masterNodeList

	for _, n := range node.GetWorkerNodes() {
		status, err := Inst().V.GetNodeStatus(n)
		if err != nil {
			pxStatus = "ERROR GETTING STATUS"
		} else {
			pxStatus = status.String()
		}

		emailData.NodeInfo = append(emailData.NodeInfo, nodeInfo{MgmtIP: n.MgmtIp, NodeName: n.Name,
			PxVersion: n.NodeLabels["PX Version"], Status: pxStatus})
	}

	for k, v := range RunningTriggers {
		emailData.TriggersInfo = append(emailData.TriggersInfo, triggerInfo{Name: k, Duration: v})
	}
	for i := 0; i < eventRing.Len(); i++ {
		record := eventRing.Value
		if record != nil {
			emailData.EmailRecords.Records = append(emailData.EmailRecords.Records, *record.(*EventRecord))
			eventRing.Value = nil
		}
		eventRing = eventRing.Next()
	}

	content, err := prepareEmailBody(emailData)
	if err != nil {
		logrus.Errorf("Failed to prepare email body. Error: [%v]", err)
	}

	emailDetails := &email.Email{
		Subject:        subject,
		Content:        content,
		From:           from,
		To:             EmailRecipients,
		SendGridAPIKey: SendGridEmailAPIKey,
	}

	err = emailDetails.SendEmail()
	if err != nil {
		logrus.Errorf("Failed to send out email, because of Error: %q", err)
	}
}

// TriggerBackupApps takes backups of all namespaces of deployed apps
func TriggerBackupApps(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {
	defer ginkgo.GinkgoRecover()
	event := &EventRecord{
		Event: Event{
			ID:   GenerateUUID(),
			Type: BackupAllApps,
		},
		Start:   time.Now().Format(time.RFC1123),
		Outcome: []error{},
	}

	defer func() {
		event.End = time.Now().Format(time.RFC1123)
		*recordChan <- event
	}()
	Step("Update admin secret", func() {
		err := backup.UpdatePxBackupAdminSecret()
		ProcessErrorWithMessage(event, err, "Unable to update PxBackupAdminSecret")
	})
	backupCounter++
	bkpNamespaces := make([]string, 0)
	labelSelectors := make(map[string]string)
	for _, ctx := range *contexts {
		namespace := ctx.GetID()
		bkpNamespaces = append(bkpNamespaces, namespace)
	}
	Step("Backup all namespaces", func() {
		bkpNamespaceErrors := make(map[string]error)
		sourceClusterConfigPath, err := GetSourceClusterConfigPath()
		UpdateOutcome(event, err)
		SetClusterContext(sourceClusterConfigPath)
		for _, namespace := range bkpNamespaces {
			backupName := fmt.Sprintf("%s-%s-%d", BackupNamePrefix, namespace, backupCounter)
			Step(fmt.Sprintf("Create backup full name %s:%s:%s",
				sourceClusterName, namespace, backupName), func() {
				err = CreateBackupGetErr(backupName,
					sourceClusterName, backupLocationName, BackupLocationUID,
					[]string{namespace}, labelSelectors, OrgID)
				if err != nil {
					bkpNamespaceErrors[namespace] = err
				}
				UpdateOutcome(event, err)
			})
		}
		for _, namespace := range bkpNamespaces {
			backupName := fmt.Sprintf("%s-%s-%d", BackupNamePrefix, namespace, backupCounter)
			err, ok := bkpNamespaceErrors[namespace]
			if ok {
				logrus.Warningf("Skipping waiting for backup %s because %s", backupName, err)
				continue
			}
			Step(fmt.Sprintf("Wait for backup %s to complete", backupName), func() {
				ctx, err := backup.GetPxCentralAdminCtx()
				if err != nil {
					logrus.Errorf("Failed to fetch px-central-admin ctx: [%v]", err)
					bkpNamespaceErrors[namespace] = err
					UpdateOutcome(event, err)
				} else {
					err = Inst().Backup.WaitForBackupCompletion(
						ctx,
						backupName, OrgID,
						BackupRestoreCompletionTimeoutMin*time.Minute,
						RetrySeconds*time.Second)
					if err == nil {
						logrus.Infof("Backup [%s] completed successfully", backupName)
					} else {
						logrus.Errorf("Failed to wait for backup [%s] to complete. Error: [%v]",
							backupName, err)
						bkpNamespaceErrors[namespace] = err
						UpdateOutcome(event, err)
					}
				}
			})
		}
	})
}

// TriggerScheduledBackupAll creates scheduled backup if it doesn't exist and makes sure backups are correct otherwise
func TriggerScheduledBackupAll(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {
	defer ginkgo.GinkgoRecover()
	event := &EventRecord{
		Event: Event{
			ID:   GenerateUUID(),
			Type: BackupScheduleAll,
		},
		Start:   time.Now().Format(time.RFC1123),
		Outcome: []error{},
	}

	defer func() {
		event.End = time.Now().Format(time.RFC1123)
		*recordChan <- event
	}()

	err := backup.UpdatePxBackupAdminSecret()
	ProcessErrorWithMessage(event, err, "Unable to update PxBackupAdminSecret")

	err = DeleteNamespace()
	ProcessErrorWithMessage(event, err, "Failed to delete namespace")

	err = CreateNamespace([]string{"nginx"})
	ProcessErrorWithMessage(event, err, "Failed to create namespace")

	_, err = InspectScheduledBackup(BackupScheduleAllName, BackupScheduleAllUID)
	if ObjectExists(err) {
		namespaces := []string{"*"}
		BackupScheduleAllUID = uuid.New()
		SchedulePolicyAllUID = uuid.New()
		err = CreateScheduledBackup(BackupScheduleAllName, BackupScheduleAllUID,
			SchedulePolicyAllName, SchedulePolicyAllUID, ScheduledBackupAllNamespacesInterval, namespaces)
		ProcessErrorWithMessage(event, err, "Create scheduled backup failed")
	} else if err != nil {
		ProcessErrorWithMessage(event, err, "Inspecting scheduled backup failed")
	}

	latestBkp, err := WaitForScheduledBackup(backupScheduleNamePrefix+BackupScheduleAllName,
		defaultRetryInterval, ScheduledBackupAllNamespacesInterval*2)
	errorMessage := fmt.Sprintf("Failed to wait for a new scheduled backup from scheduled backup %s",
		backupScheduleNamePrefix+BackupScheduleAllName)
	ProcessErrorWithMessage(event, err, errorMessage)

	logrus.Infof("Verify namespaces")
	// Verify that all namespaces are present in latest backup
	latestBkpNamespaces := latestBkp.GetNamespaces()
	namespacesList, err := core.Instance().ListNamespaces(nil)
	ProcessErrorWithMessage(event, err, "List namespaces failed")

	if len(namespacesList.Items) != len(latestBkpNamespaces) {
		err = fmt.Errorf("backup backed up %d namespaces, but %d namespaces exist", len(latestBkpNamespaces),
			len(namespacesList.Items))
		ProcessErrorWithMessage(event, err, "Scheduled backup backed up wrong namespaces")
	}

	var namespaces []string

	for _, ns := range namespacesList.Items {
		namespaces = append(namespaces, ns.GetName())
	}

	sort.Strings(namespaces)
	sort.Strings(latestBkpNamespaces)

	for i, ns := range namespaces {
		if latestBkpNamespaces[i] != ns {
			err = fmt.Errorf("namespace %s not present in backup", ns)
			ProcessErrorWithMessage(event, err, "Scheduled backup backed up wrong namespaces")
		}
	}
}

// TriggerBackupSpecificResource backs up a specific resource in a namespace
// Creates config maps in the the specified namespaces and backups up only these config maps
func TriggerBackupSpecificResource(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {
	defer ginkgo.GinkgoRecover()
	event := &EventRecord{
		Event: Event{
			ID:   GenerateUUID(),
			Type: BackupSpecificResource,
		},
		Start:   time.Now().Format(time.RFC1123),
		Outcome: []error{},
	}
	namespaceResourceMap := make(map[string][]string)
	err := backup.UpdatePxBackupAdminSecret()
	ProcessErrorWithMessage(event, err, "Unable to update PxBackupAdminSecret")
	if err != nil {
		return
	}
	sourceClusterConfigPath, err := GetSourceClusterConfigPath()
	UpdateOutcome(event, err)
	if err != nil {
		return
	}
	SetClusterContext(sourceClusterConfigPath)
	backupCounter++
	bkpNamespaces := make([]string, 0)
	labelSelectors := make(map[string]string)
	bkpNamespaceErrors := make(map[string]error)
	for _, ctx := range *contexts {
		namespace := ctx.GetID()
		bkpNamespaces = append(bkpNamespaces, namespace)
	}
	Step("Create config maps", func() {
		configMapCount := 2
		for _, namespace := range bkpNamespaces {
			for i := 0; i < configMapCount; i++ {
				configName := fmt.Sprintf("%s-%d-%d", namespace, backupCounter, i)
				cm := &v1.ConfigMap{
					ObjectMeta: meta_v1.ObjectMeta{
						Name:      configName,
						Namespace: namespace,
					},
				}
				_, err := core.Instance().CreateConfigMap(cm)
				ProcessErrorWithMessage(event, err, fmt.Sprintf("Unable to create config map [%s]", configName))
				if err == nil {
					namespaceResourceMap[namespace] = append(namespaceResourceMap[namespace], configName)
				}
			}
		}
	})
	defer func() {
		Step("Clean up config maps", func() {
			for _, namespace := range bkpNamespaces {
				for _, configName := range namespaceResourceMap[namespace] {
					err := core.Instance().DeleteConfigMap(configName, namespace)
					ProcessErrorWithMessage(event, err, fmt.Sprintf("Unable to delete config map [%s]", configName))
				}
			}
		})
		event.End = time.Now().Format(time.RFC1123)
		*recordChan <- event
	}()
	bkpNames := make([]string, 0)
	Step("Create backups", func() {
		for _, namespace := range bkpNamespaces {
			backupName := fmt.Sprintf("%s-%s-%d", BackupNamePrefix, namespace, backupCounter)
			bkpNames = append(bkpNames, namespace)
			logrus.Infof("Create backup full name %s:%s:%s", sourceClusterName, namespace, backupName)
			backupCreateRequest := GetBackupCreateRequest(backupName, sourceClusterName, backupLocationName, BackupLocationUID,
				[]string{namespace}, labelSelectors, OrgID)
			backupCreateRequest.Name = backupName
			backupCreateRequest.ResourceTypes = []string{"ConfigMap"}
			err = CreateBackupFromRequest(backupName, OrgID, backupCreateRequest)
			UpdateOutcome(event, err)
			if err != nil {
				bkpNamespaceErrors[namespace] = err
			}
		}
	})
	for _, namespace := range bkpNames {
		backupName := fmt.Sprintf("%s-%s-%d", BackupNamePrefix, namespace, backupCounter)
		err, ok := bkpNamespaceErrors[namespace]
		if ok {
			logrus.Warningf("Skipping waiting for backup [%s] because [%s]", backupName, err)
			continue
		}
		Step(fmt.Sprintf("Wait for backup [%s] to complete", backupName), func() {
			ctx, err := backup.GetPxCentralAdminCtx()
			if err != nil {
				bkpNamespaceErrors[namespace] = err
				ProcessErrorWithMessage(event, err, fmt.Sprintf("Failed to fetch px-central-admin ctx: [%v]", err))
			} else {
				err = Inst().Backup.WaitForBackupCompletion(
					ctx,
					backupName, OrgID,
					BackupRestoreCompletionTimeoutMin*time.Minute,
					RetrySeconds*time.Second)
				if err == nil {
					logrus.Infof("Backup [%s] completed successfully", backupName)
				} else {
					bkpNamespaceErrors[namespace] = err
					ProcessErrorWithMessage(event, err, fmt.Sprintf("Failed to wait for backup [%s] to complete. Error: [%v]", backupName, err))
				}
			}
		})
	}
	Step("Check that only config maps are backed up", func() {
		for _, namespace := range bkpNames {
			backupName := fmt.Sprintf("%s-%s-%d", BackupNamePrefix, namespace, backupCounter)
			err, ok := bkpNamespaceErrors[namespace]
			if ok {
				logrus.Warningf("Skipping inspecting backup [%s] because [%s]", backupName, err)
				continue
			}
			bkpInspectResp, err := InspectBackup(backupName)
			UpdateOutcome(event, err)
			backupObj := bkpInspectResp.GetBackup()
			cmList, err := core.Instance().ListConfigMap(namespace, meta_v1.ListOptions{})
			//kube-root-ca.crt exists in every namespace but does not get backed up, so we subtract 1 from the count
			if backupObj.GetResourceCount() != uint64(len(cmList.Items)-1) {
				errMsg := fmt.Sprintf("Backup [%s] has an incorrect number of objects, expected [%d], actual [%d]", backupName, len(cmList.Items)-1, backupObj.GetResourceCount())
				err = fmt.Errorf(errMsg)
				ProcessErrorWithMessage(event, err, errMsg)
			}
			for _, resource := range backupObj.GetResources() {
				if resource.GetKind() != "ConfigMap" {
					errMsg := fmt.Sprintf("Backup [%s] contains non configMap resource, expected [configMap], actual [%v]", backupName, resource.GetKind())
					err = fmt.Errorf(errMsg)
					ProcessErrorWithMessage(event, err, errMsg)
				}
			}
		}
	})
}

// TriggerInspectBackup inspects backup and checks for errors
func TriggerInspectBackup(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {
	defer ginkgo.GinkgoRecover()
	event := &EventRecord{
		Event: Event{
			ID:   GenerateUUID(),
			Type: TestInspectBackup,
		},
		Start:   time.Now().Format(time.RFC1123),
		Outcome: []error{},
	}

	defer func() {
		event.End = time.Now().Format(time.RFC1123)
		*recordChan <- event
	}()

	logrus.Infof("Enumerating backups")
	bkpEnumerateReq := &api.BackupEnumerateRequest{
		OrgId: OrgID}
	ctx, err := backup.GetPxCentralAdminCtx()
	ProcessErrorWithMessage(event, err, "InspectBackup failed: Failed to get px-central admin context")
	curBackups, err := Inst().Backup.EnumerateBackup(ctx, bkpEnumerateReq)
	ProcessErrorWithMessage(event, err, "InspectBackup failed: Enumerate backup request failed")

	if len(curBackups.GetBackups()) == 0 {
		return
	}

	backupToInspect := curBackups.GetBackups()[0]

	backupInspectRequest := &api.BackupInspectRequest{
		Name:  backupToInspect.GetName(),
		OrgId: backupToInspect.GetOrgId(),
	}
	_, err = Inst().Backup.InspectBackup(ctx, backupInspectRequest)
	desc := fmt.Sprintf("InspectBackup failed: Inspect backup %s failed", backupToInspect.GetName())
	ProcessErrorWithMessage(event, err, desc)

}

// TriggerInspectRestore inspects restore and checks for errors
func TriggerInspectRestore(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {
	defer ginkgo.GinkgoRecover()
	event := &EventRecord{
		Event: Event{
			ID:   GenerateUUID(),
			Type: TestInspectRestore,
		},
		Start:   time.Now().Format(time.RFC1123),
		Outcome: []error{},
	}

	defer func() {
		event.End = time.Now().Format(time.RFC1123)
		*recordChan <- event
	}()

	logrus.Infof("Enumerating restores")
	restoreEnumerateReq := &api.RestoreEnumerateRequest{
		OrgId: OrgID}
	ctx, err := backup.GetPxCentralAdminCtx()
	ProcessErrorWithMessage(event, err, "InspectRestore failed: Failed to get px-central admin context")
	curRestores, err := Inst().Backup.EnumerateRestore(ctx, restoreEnumerateReq)
	ProcessErrorWithMessage(event, err, "InspectRestore failed: Enumerate restore request failed")

	if len(curRestores.GetRestores()) == 0 {
		return
	}

	restoreToInspect := curRestores.GetRestores()[0]

	restoreInspectRequest := &api.RestoreInspectRequest{
		Name:  restoreToInspect.GetName(),
		OrgId: restoreToInspect.GetOrgId(),
	}
	_, err = Inst().Backup.InspectRestore(ctx, restoreInspectRequest)
	desc := fmt.Sprintf("InspectRestore failed: Inspect restore %s failed", restoreToInspect.GetName())
	ProcessErrorWithMessage(event, err, desc)
}

// TriggerRestoreNamespace restores a namespace to a new namespace
func TriggerRestoreNamespace(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {
	defer ginkgo.GinkgoRecover()
	event := &EventRecord{
		Event: Event{
			ID:   GenerateUUID(),
			Type: RestoreNamespace,
		},
		Start:   time.Now().Format(time.RFC1123),
		Outcome: []error{},
	}

	defer func() {
		sourceClusterConfigPath, err := GetSourceClusterConfigPath()
		UpdateOutcome(event, err)
		SetClusterContext(sourceClusterConfigPath)
		event.End = time.Now().Format(time.RFC1123)
		*recordChan <- event
	}()

	restoreCounter++
	namespacesList, err := core.Instance().ListNamespaces(nil)
	ProcessErrorWithMessage(event, err, "Restore namespace failed: List namespaces failed")

	destClusterConfigPath, err := GetDestinationClusterConfigPath()
	ProcessErrorWithMessage(event, err, "Restore namespace failed: GetDestinationClusterConfigPath failed")
	SetClusterContext(destClusterConfigPath)

	logrus.Infof("Enumerating backups")
	bkpEnumerateReq := &api.BackupEnumerateRequest{
		OrgId: OrgID}
	ctx, err := backup.GetPxCentralAdminCtx()
	ProcessErrorWithMessage(event, err, "Restore namespace failed: Failed to get px-central admin context")
	curBackups, err := Inst().Backup.EnumerateBackup(ctx, bkpEnumerateReq)
	ProcessErrorWithMessage(event, err, "Restore namespace failed: Enumerate backup request failed")

	// Get a completed backup
	var backupToRestore *api.BackupObject
	backupToRestore = nil
	for _, bkp := range curBackups.GetBackups() {
		if bkp.GetStatus().GetStatus() == api.BackupInfo_StatusInfo_PartialSuccess ||
			bkp.GetStatus().GetStatus() == api.BackupInfo_StatusInfo_Success {
			backupToRestore = bkp
			break
		}
	}
	// If there is nothing to restore, return
	if backupToRestore == nil {
		return
	}
	restoreName := fmt.Sprintf("%s-%d", backupToRestore.GetName(), restoreCounter)

	// Pick one namespace to restore
	// In case destination cluster == source cluster, restore to a new namespace
	namespaceMapping := make(map[string]string)
	namespaces := backupToRestore.GetNamespaces()
	restoredNs := namespaces[0]
	if len(namespaces) > 0 {
		namespaceMapping[restoredNs] = fmt.Sprintf("%s-restore-%s-%d", restoredNs, Inst().InstanceID, restoreCounter)
	}

	restoreCreateRequest := &api.RestoreCreateRequest{
		CreateMetadata: &api.CreateMetadata{
			Name:  restoreName,
			OrgId: OrgID,
		},
		Backup:           backupToRestore.GetName(),
		Cluster:          destinationClusterName,
		NamespaceMapping: namespaceMapping,
	}
	_, err = Inst().Backup.CreateRestore(ctx, restoreCreateRequest)
	desc := fmt.Sprintf("Restore namespace failed: Create restore %s failed", restoreName)
	ProcessErrorWithMessage(event, err, desc)

	err = Inst().Backup.WaitForRestoreCompletion(ctx, restoreName, OrgID,
		BackupRestoreCompletionTimeoutMin*time.Minute,
		RetrySeconds*time.Second)
	desc = fmt.Sprintf("Restore namespace failed: Failed to wait for restore [%s] to complete.", restoreName)
	ProcessErrorWithMessage(event, err, desc)

	// Validate that one namespace is restored
	newNamespacesList, err := core.Instance().ListNamespaces(nil)
	ProcessErrorWithMessage(event, err, "Restore namespace failed: List namespaces failed")

	if len(newNamespacesList.Items) != len(namespacesList.Items)+1 {
		err = fmt.Errorf("restored %d namespaces instead of 1, %s",
			len(newNamespacesList.Items)-len(namespacesList.Items), restoreName)
		ProcessErrorWithMessage(event, err, "RestoreNamespace restored incorrect namespaces")
	}

	nsFound := false
	for _, ns := range newNamespacesList.Items {
		if ns.GetName() == restoredNs {
			nsFound = true
		}
	}
	if !nsFound {
		err = fmt.Errorf("namespace %s not found", restoredNs)
		ProcessErrorWithMessage(event, err, "RestoreNamespace restored incorrect namespaces")
	}
}

// TriggerDeleteBackup deletes a backup
func TriggerDeleteBackup(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {
	defer ginkgo.GinkgoRecover()
	event := &EventRecord{
		Event: Event{
			ID:   GenerateUUID(),
			Type: TestDeleteBackup,
		},
		Start:   time.Now().Format(time.RFC1123),
		Outcome: []error{},
	}

	defer func() {
		event.End = time.Now().Format(time.RFC1123)
		*recordChan <- event
	}()

	logrus.Infof("Enumerating backups")
	bkpEnumerateReq := &api.BackupEnumerateRequest{
		OrgId: OrgID}
	ctx, err := backup.GetPxCentralAdminCtx()
	ProcessErrorWithMessage(event, err, "DeleteBackup failed: Failed to get px-central admin context")
	curBackups, err := Inst().Backup.EnumerateBackup(ctx, bkpEnumerateReq)
	ProcessErrorWithMessage(event, err, "DeleteBackup failed: Enumerate backup request failed")

	if len(curBackups.GetBackups()) == 0 {
		return
	}

	backupToDelete := curBackups.GetBackups()[0]
	err = DeleteBackupAndDependencies(backupToDelete.GetName(), OrgID, backupToDelete.GetCluster())
	desc := fmt.Sprintf("DeleteBackup failed: Delete backup %s on cluster %s failed",
		backupToDelete.GetName(), backupToDelete.GetCluster())
	ProcessErrorWithMessage(event, err, desc)

}

// TriggerBackupSpecificResourceOnCluster backs up all PVCs on the source cluster
func TriggerBackupSpecificResourceOnCluster(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {
	defer ginkgo.GinkgoRecover()
	event := &EventRecord{
		Event: Event{
			ID:   GenerateUUID(),
			Type: BackupSpecificResourceOnCluster,
		},
		Start:   time.Now().Format(time.RFC1123),
		Outcome: []error{},
	}

	defer func() {
		event.End = time.Now().Format(time.RFC1123)
		*recordChan <- event
	}()
	err := backup.UpdatePxBackupAdminSecret()
	ProcessErrorWithMessage(event, err, "Unable to update PxBackupAdminSecret")
	if err != nil {
		return
	}
	sourceClusterConfigPath, err := GetSourceClusterConfigPath()
	UpdateOutcome(event, err)
	if err != nil {
		return
	}
	SetClusterContext(sourceClusterConfigPath)
	backupCounter++
	backupName := fmt.Sprintf("%s-%s-%d", BackupNamePrefix, Inst().InstanceID, backupCounter)
	namespaces := make([]string, 0)
	labelSelectors := make(map[string]string)
	totalPVC := 0
	Step("Backup all persistent volume claims on source cluster", func() {
		nsList, err := core.Instance().ListNamespaces(labelSelectors)
		UpdateOutcome(event, err)
		if err == nil {
			for _, ns := range nsList.Items {
				namespaces = append(namespaces, ns.Name)
			}
			backupCreateRequest := GetBackupCreateRequest(backupName, sourceClusterName, backupLocationName, BackupLocationUID,
				namespaces, labelSelectors, OrgID)
			backupCreateRequest.Name = backupName
			backupCreateRequest.ResourceTypes = []string{"PersistentVolumeClaim"}
			err = CreateBackupFromRequest(backupName, OrgID, backupCreateRequest)
			UpdateOutcome(event, err)
		}
	})
	if err != nil {
		return
	}
	Step("Wait for backup to complete", func() {
		ctx, err := backup.GetPxCentralAdminCtx()
		if err != nil {
			ProcessErrorWithMessage(event, err, fmt.Sprintf("Failed to fetch px-central-admin ctx: [%v]", err))
		} else {
			err = Inst().Backup.WaitForBackupCompletion(
				ctx,
				backupName, OrgID,
				BackupRestoreCompletionTimeoutMin*time.Minute,
				RetrySeconds*time.Second)
			if err == nil {
				logrus.Infof("Backup [%s] completed successfully", backupName)
			} else {
				ProcessErrorWithMessage(event, err, fmt.Sprintf("Failed to wait for backup [%s] to complete. Error: [%v]", backupName, err))
			}
		}
	})
	if err != nil {
		return
	}
	Step("Check PVCs in backup", func() {
		for _, ns := range namespaces {
			pvcList, err := core.Instance().GetPersistentVolumeClaims(ns, labelSelectors)
			UpdateOutcome(event, err)
			if err == nil {
				totalPVC += len(pvcList.Items) * 2
			}
		}
		bkpInspectResp, err := InspectBackup(backupName)
		UpdateOutcome(event, err)
		if err == nil {
			backupObj := bkpInspectResp.GetBackup()
			if backupObj.GetResourceCount() != uint64(totalPVC) { //Each backed up PVC should give a PVC and a PV, hence x2
				errMsg := fmt.Sprintf("Backup %s has incorrect number of objects, expected [%d], actual [%d]", backupName, totalPVC, backupObj.GetResourceCount())
				err = fmt.Errorf(errMsg)
				ProcessErrorWithMessage(event, err, errMsg)
			}
			for _, resource := range backupObj.GetResources() {
				if resource.GetKind() != "PersistentVolumeClaim" && resource.GetKind() != "PersistentVolume" {
					errMsg := fmt.Sprintf("Backup %s contains non PersistentVolumeClaim resource of type [%v]", backupName, resource.GetKind())
					err = fmt.Errorf(errMsg)
					ProcessErrorWithMessage(event, err, errMsg)
				}
			}
		}
	})
}

//TriggerBackupByLabel gives a label to random resources on the cluster and tries to back up only resources with that label
func TriggerBackupByLabel(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {
	defer ginkgo.GinkgoRecover()
	event := &EventRecord{
		Event: Event{
			ID:   GenerateUUID(),
			Type: BackupUsingLabelOnCluster,
		},
		Start:   time.Now().Format(time.RFC1123),
		Outcome: []error{},
	}
	labelKey := "backup-by-label"
	labelValue := uuid.New()
	defer func() {
		Step("Delete the temporary labels", func() {
			nsList, err := core.Instance().ListNamespaces(nil)
			UpdateOutcome(event, err)
			for _, ns := range nsList.Items {
				pvcList, err := core.Instance().GetPersistentVolumeClaims(ns.Name, nil)
				UpdateOutcome(event, err)
				for _, pvc := range pvcList.Items {
					pvcPointer, err := core.Instance().GetPersistentVolumeClaim(pvc.Name, ns.Name)
					UpdateOutcome(event, err)
					if err == nil {
						DeleteLabelFromResource(pvcPointer, labelKey)
					}
				}
				cmList, err := core.Instance().ListConfigMap(ns.Name, meta_v1.ListOptions{})
				UpdateOutcome(event, err)
				for _, cm := range cmList.Items {
					cmPointer, err := core.Instance().GetConfigMap(cm.Name, ns.Name)
					UpdateOutcome(event, err)
					if err == nil {
						DeleteLabelFromResource(cmPointer, labelKey)
					}
				}
				secretList, err := core.Instance().ListSecret(ns.Name, meta_v1.ListOptions{})
				UpdateOutcome(event, err)
				for _, secret := range secretList.Items {
					secretPointer, err := core.Instance().GetConfigMap(secret.Name, ns.Name)
					UpdateOutcome(event, err)
					if err == nil {
						DeleteLabelFromResource(secretPointer, labelKey)
					}
				}
			}
		})
		event.End = time.Now().Format(time.RFC1123)
		*recordChan <- event
	}()
	err := backup.UpdatePxBackupAdminSecret()
	ProcessErrorWithMessage(event, err, "Unable to update PxBackupAdminSecret")
	if err != nil {
		return
	}
	sourceClusterConfigPath, err := GetSourceClusterConfigPath()
	UpdateOutcome(event, err)
	if err != nil {
		return
	}
	SetClusterContext(sourceClusterConfigPath)
	backupCounter++
	backupName := fmt.Sprintf("%s-%s-%d", BackupNamePrefix, Inst().InstanceID, backupCounter)
	namespaces := make([]string, 0)
	labelSelectors := make(map[string]string)
	labeledResources := make(map[string]bool)
	Step("Add labels to random resources", func() {
		nsList, err := core.Instance().ListNamespaces(nil)
		UpdateOutcome(event, err)
		for _, ns := range nsList.Items {
			namespaces = append(namespaces, ns.Name)
			pvcList, err := core.Instance().GetPersistentVolumeClaims(ns.Name, nil)
			UpdateOutcome(event, err)
			for _, pvc := range pvcList.Items {
				pvcPointer, err := core.Instance().GetPersistentVolumeClaim(pvc.Name, ns.Name)
				UpdateOutcome(event, err)
				if err == nil {
					// Randomly choose some pvcs to add labels to for backup
					dice := rand.Intn(4)
					if dice == 1 {
						err = AddLabelToResource(pvcPointer, labelKey, labelValue)
						UpdateOutcome(event, err)
						if err == nil {
							resourceName := fmt.Sprintf("%s/%s/PersistentVolumeClaim", ns.Name, pvc.Name)
							labeledResources[resourceName] = true
						}
					}
				}
			}
			cmList, err := core.Instance().ListConfigMap(ns.Name, meta_v1.ListOptions{})
			UpdateOutcome(event, err)
			for _, cm := range cmList.Items {
				cmPointer, err := core.Instance().GetConfigMap(cm.Name, ns.Name)
				UpdateOutcome(event, err)
				if err == nil {
					// Randomly choose some configmaps to add labels to for backup
					dice := rand.Intn(4)
					if dice == 1 {
						err = AddLabelToResource(cmPointer, labelKey, labelValue)
						UpdateOutcome(event, err)
						if err == nil {
							resourceName := fmt.Sprintf("%s/%s/ConfigMap", ns.Name, cm.Name)
							labeledResources[resourceName] = true
						}
					}
				}
			}
			secretList, err := core.Instance().ListSecret(ns.Name, meta_v1.ListOptions{})
			UpdateOutcome(event, err)
			for _, secret := range secretList.Items {
				secretPointer, err := core.Instance().GetSecret(secret.Name, ns.Name)
				UpdateOutcome(event, err)
				if err == nil {
					// Randomly choose some secrets to add labels to for backup
					dice := rand.Intn(4)
					if dice == 1 {
						err = AddLabelToResource(secretPointer, labelKey, labelValue)
						UpdateOutcome(event, err)
						if err == nil {
							resourceName := fmt.Sprintf("%s/%s/Secret", ns.Name, secret.Name)
							labeledResources[resourceName] = true
						}
					}
				}
			}
		}
	})
	Step(fmt.Sprintf("Backup using label [%s=%s]", labelKey, labelValue), func() {
		labelSelectors[labelKey] = labelValue
		backupCreateRequest := GetBackupCreateRequest(backupName, sourceClusterName, backupLocationName, BackupLocationUID,
			namespaces, labelSelectors, OrgID)
		backupCreateRequest.Name = backupName
		err = CreateBackupFromRequest(backupName, OrgID, backupCreateRequest)
		UpdateOutcome(event, err)
		if err != nil {
			return
		}
	})
	Step("Wait for backup to complete", func() {
		ctx, err := backup.GetPxCentralAdminCtx()
		if err != nil {
			ProcessErrorWithMessage(event, err, fmt.Sprintf("Failed to fetch px-central-admin ctx: [%v]", err))
		} else {
			err = Inst().Backup.WaitForBackupCompletion(
				ctx,
				backupName, OrgID,
				BackupRestoreCompletionTimeoutMin*time.Minute,
				RetrySeconds*time.Second)
			if err == nil {
				logrus.Infof("Backup [%s] completed successfully", backupName)
			} else {
				ProcessErrorWithMessage(event, err, fmt.Sprintf("Failed to wait for backup [%s] to complete. Error: [%v]", backupName, err))
				return
			}
		}
	})
	Step("Check that we only backed up objects with specified labels", func() {
		bkpInspectResp, err := InspectBackup(backupName)
		UpdateOutcome(event, err)
		if err != nil {
			return
		}
		backupObj := bkpInspectResp.GetBackup()
		for _, resource := range backupObj.GetResources() {
			if resource.GetKind() == "PersistentVolume" { //PV are automatically backed up with PVCs
				continue
			}
			resourceName := fmt.Sprintf("%s/%s/%s", resource.Namespace, resource.Namespace, resource.GetKind())
			if _, ok := labeledResources[resourceName]; !ok {
				err = fmt.Errorf("Backup [%s] has a resource [%s]that shouldn't be there", backupName, resourceName)
				UpdateOutcome(event, err)
			}
		}
	})
}

// TriggerScheduledBackupScale creates a scheduled backup and checks that scaling an app is reflected
func TriggerScheduledBackupScale(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {
	defer ginkgo.GinkgoRecover()
	event := &EventRecord{
		Event: Event{
			ID:   GenerateUUID(),
			Type: BackupScheduleScale,
		},
		Start:   time.Now().Format(time.RFC1123),
		Outcome: []error{},
	}

	defer func() {
		event.End = time.Now().Format(time.RFC1123)
		*recordChan <- event
	}()

	err := backup.UpdatePxBackupAdminSecret()
	ProcessErrorWithMessage(event, err, "Unable to update PxBackupAdminSecret")

	bkpNamespaces := make([]string, 0)
	labelSelectors := make(map[string]string)

	for _, ctx := range *contexts {
		namespace := ctx.GetID()
		bkpNamespaces = append(bkpNamespaces, namespace)
	}

	_, err = InspectScheduledBackup(backupScheduleScaleName, BackupScheduleScaleUID)
	if ObjectExists(err) {
		BackupScheduleScaleUID = uuid.New()
		SchedulePolicyScaleUID = uuid.New()
		err = CreateScheduledBackup(backupScheduleScaleName, BackupScheduleScaleUID,
			SchedulePolicyScaleName, SchedulePolicyScaleUID,
			ScheduledBackupScaleInterval, bkpNamespaces)
		ProcessErrorWithMessage(event, err, "Create scheduled backup failed")
	} else if err != nil {
		ProcessErrorWithMessage(event, err, "Inspecting scheduled backup failed")
	}

	for _, ctx := range *contexts {
		Step(fmt.Sprintf("scale up app: %s by %d ", ctx.App.Key, len(node.GetWorkerNodes())), func() {
			applicationScaleUpMap, err := Inst().S.GetScaleFactorMap(ctx)
			UpdateOutcome(event, err)
			for name, scale := range applicationScaleUpMap {
				applicationScaleUpMap[name] = scale + int32(len(node.GetWorkerNodes()))
			}
			err = Inst().S.ScaleApplication(ctx, applicationScaleUpMap)
			UpdateOutcome(event, err)
		})

		Step("Giving few seconds for scaled up applications to stabilize", func() {
			time.Sleep(10 * time.Second)
		})

		ValidateContext(ctx)
	}

	scaleUpBkp, err := WaitForScheduledBackup(backupScheduleNamePrefix+backupScheduleScaleName,
		defaultRetryInterval, ScheduledBackupScaleInterval*2)
	errorMessage := fmt.Sprintf("Failed to wait for a new scheduled backup from scheduled backup %s",
		backupScheduleNamePrefix+backupScheduleScaleName)
	ProcessErrorWithMessage(event, err, errorMessage)

	logrus.Infof("Verify newest pods have been scaled in backup")
	for _, ns := range bkpNamespaces {
		pods, err := core.Instance().GetPods(ns, labelSelectors)
		UpdateOutcome(event, err)
		for _, pod := range pods.Items {
			found := false
			for _, resource := range scaleUpBkp.GetResources() {
				if resource.GetNamespace() == ns && resource.GetName() == pod.GetName() {
					found = true
				}
			}
			if !found {
				err = fmt.Errorf("pod %s in namespace %s not present in backup", pod.GetName(), ns)
				UpdateOutcome(event, err)
			}
		}
	}

	for _, resource := range scaleUpBkp.GetResources() {
		if resource.GetKind() != "Pod" {
			continue
		}
		ns := resource.GetNamespace()
		name := resource.GetName()
		_, err := core.Instance().GetPodByName(ns, name)
		if err != nil {
			err = fmt.Errorf("pod %s in namespace %s present in backup but not on cluster", name, ns)
			UpdateOutcome(event, err)
		}
	}

	for _, ctx := range *contexts {
		Step(fmt.Sprintf("scale down app %s by %d", ctx.App.Key, len(node.GetWorkerNodes())), func() {
			applicationScaleDownMap, err := Inst().S.GetScaleFactorMap(ctx)
			UpdateOutcome(event, err)
			for name, scale := range applicationScaleDownMap {
				applicationScaleDownMap[name] = scale - int32(len(node.GetWorkerNodes()))
			}
			err = Inst().S.ScaleApplication(ctx, applicationScaleDownMap)
			UpdateOutcome(event, err)
		})

		Step("Giving few seconds for scaled down applications to stabilize", func() {
			time.Sleep(10 * time.Second)
		})

		ValidateContext(ctx)
	}

	scaleDownBkp, err := WaitForScheduledBackup(backupScheduleNamePrefix+backupScheduleScaleName,
		defaultRetryInterval, ScheduledBackupScaleInterval*2)
	errorMessage = fmt.Sprintf("Failed to wait for a new scheduled backup from scheduled backup %s",
		backupScheduleNamePrefix+backupScheduleScaleName)
	ProcessErrorWithMessage(event, err, errorMessage)

	logrus.Infof("Verify pods have been scaled in backup")
	for _, ns := range bkpNamespaces {
		pods, err := core.Instance().GetPods(ns, labelSelectors)
		UpdateOutcome(event, err)
		for _, pod := range pods.Items {
			found := false
			for _, resource := range scaleDownBkp.GetResources() {
				if resource.GetNamespace() == ns && resource.GetName() == pod.GetName() {
					found = true
				}
			}
			if !found {
				err = fmt.Errorf("pod %s in namespace %s not present in backup", pod.GetName(), ns)
				UpdateOutcome(event, err)
			}
		}
	}

	for _, resource := range scaleDownBkp.GetResources() {
		if resource.GetKind() != "Pod" {
			continue
		}
		ns := resource.GetNamespace()
		name := resource.GetName()
		_, err := core.Instance().GetPodByName(ns, name)
		if err != nil {
			err = fmt.Errorf("pod %s in namespace %s present in backup but not on cluster", name, ns)
			UpdateOutcome(event, err)
		}
	}
}

//TriggerBackupRestartPX backs up an application and restarts Portworx during the backup
func TriggerBackupRestartPX(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {
	defer ginkgo.GinkgoRecover()
	event := &EventRecord{
		Event: Event{
			ID:   GenerateUUID(),
			Type: BackupRestartPX,
		},
		Start:   time.Now().Format(time.RFC1123),
		Outcome: []error{},
	}

	defer func() {
		event.End = time.Now().Format(time.RFC1123)
		*recordChan <- event
	}()

	Step("Update admin secret", func() {
		err := backup.UpdatePxBackupAdminSecret()
		ProcessErrorWithMessage(event, err, "Unable to update PxBackupAdminSecret")
	})
	backupCounter++
	bkpNamespaces := make([]string, 0)
	labelSelectors := make(map[string]string)
	sourceClusterConfigPath, err := GetSourceClusterConfigPath()
	UpdateOutcome(event, err)
	SetClusterContext(sourceClusterConfigPath)
	for _, ctx := range *contexts {
		namespace := ctx.GetID()
		bkpNamespaces = append(bkpNamespaces, namespace)
	}
	nsIndex := rand.Intn(len(bkpNamespaces))
	backupName := fmt.Sprintf("%s-%s-%d", BackupNamePrefix, bkpNamespaces[nsIndex], backupCounter)
	bkpError := false
	Step("Backup a single namespace", func() {
		Step(fmt.Sprintf("Create backup full name %s:%s:%s",
			sourceClusterName, bkpNamespaces[nsIndex], backupName), func() {
			err = CreateBackupGetErr(backupName,
				sourceClusterName, backupLocationName, BackupLocationUID,
				[]string{bkpNamespaces[nsIndex]}, labelSelectors, OrgID)
			if err != nil {
				bkpError = true
			}
			UpdateOutcome(event, err)
		})
	})

	Step("Restart Portworx", func() {
		nodes := node.GetStorageDriverNodes()
		nodeIndex := rand.Intn(len(nodes))
		logrus.Infof("Stop volume driver [%s] on node: [%s]", Inst().V.String(), nodes[nodeIndex].Name)
		StopVolDriverAndWait([]node.Node{nodes[nodeIndex]})
		logrus.Infof("Starting volume driver [%s] on node [%s]", Inst().V.String(), nodes[nodeIndex].Name)
		StartVolDriverAndWait([]node.Node{nodes[nodeIndex]})
		logrus.Infof("Giving a few seconds for volume driver to stabilize")
		time.Sleep(20 * time.Second)
	})

	Step("Wait for backup to complete", func() {
		if bkpError {
			logrus.Warningf("Skipping waiting for backup [%s] due to error", backupName)
		} else {
			ctx, err := backup.GetPxCentralAdminCtx()
			if err != nil {
				logrus.Errorf("Failed to fetch px-central-admin ctx: [%v]", err)
				UpdateOutcome(event, err)
			} else {
				err = Inst().Backup.WaitForBackupCompletion(
					ctx,
					backupName, OrgID,
					BackupRestoreCompletionTimeoutMin*time.Minute,
					RetrySeconds*time.Second)
				if err == nil {
					logrus.Infof("Backup [%s] completed successfully", backupName)
				} else {
					logrus.Errorf("Failed to wait for backup [%s] to complete. Error: [%v]",
						backupName, err)
					UpdateOutcome(event, err)
				}
			}
		}
	})
}

//TriggerBackupRestartNode backs up an application and restarts a node with Portworx during the backup
func TriggerBackupRestartNode(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {
	defer ginkgo.GinkgoRecover()
	event := &EventRecord{
		Event: Event{
			ID:   GenerateUUID(),
			Type: BackupRestartNode,
		},
		Start:   time.Now().Format(time.RFC1123),
		Outcome: []error{},
	}

	defer func() {
		event.End = time.Now().Format(time.RFC1123)
		*recordChan <- event
	}()
	Step("Update admin secret", func() {
		err := backup.UpdatePxBackupAdminSecret()
		ProcessErrorWithMessage(event, err, "Unable to update PxBackupAdminSecret")
	})
	backupCounter++
	bkpNamespaces := make([]string, 0)
	labelSelectors := make(map[string]string)
	sourceClusterConfigPath, err := GetSourceClusterConfigPath()
	UpdateOutcome(event, err)
	SetClusterContext(sourceClusterConfigPath)
	for _, ctx := range *contexts {
		namespace := ctx.GetID()
		bkpNamespaces = append(bkpNamespaces, namespace)
	}
	// Choose a random namespace to back up
	nsIndex := rand.Intn(len(bkpNamespaces))
	backupName := fmt.Sprintf("%s-%s-%d", BackupNamePrefix, bkpNamespaces[nsIndex], backupCounter)
	bkpError := false
	Step("Backup a single namespace", func() {
		Step(fmt.Sprintf("Create backup full name %s:%s:%s",
			sourceClusterName, bkpNamespaces[nsIndex], backupName), func() {
			err = CreateBackupGetErr(backupName,
				sourceClusterName, backupLocationName, BackupLocationUID,
				[]string{bkpNamespaces[nsIndex]}, labelSelectors, OrgID)
			if err != nil {
				bkpError = true
			}
			UpdateOutcome(event, err)
		})
	})

	Step("Restart a Portworx node", func() {
		nodes := node.GetStorageDriverNodes()
		// Choose a random node to reboot
		nodeIndex := rand.Intn(len(nodes))
		Step(fmt.Sprintf("reboot node: %s", nodes[nodeIndex].Name), func() {
			err := Inst().N.RebootNode(nodes[nodeIndex], node.RebootNodeOpts{
				Force: true,
				ConnectionOpts: node.ConnectionOpts{
					Timeout:         1 * time.Minute,
					TimeBeforeRetry: 5 * time.Second,
				},
			})
			expect(err).NotTo(haveOccurred())
			UpdateOutcome(event, err)
		})

		Step(fmt.Sprintf("wait for node: [%s] to be back up", nodes[nodeIndex].Name), func() {
			err := Inst().N.TestConnection(nodes[nodeIndex], node.ConnectionOpts{
				Timeout:         15 * time.Minute,
				TimeBeforeRetry: 10 * time.Second,
			})
			expect(err).NotTo(haveOccurred())
			UpdateOutcome(event, err)
		})

		Step(fmt.Sprintf("wait for volume driver to stop on node: [%v]", nodes[nodeIndex].Name), func() {
			err := Inst().V.WaitDriverDownOnNode(nodes[nodeIndex])
			expect(err).NotTo(haveOccurred())
			UpdateOutcome(event, err)
		})

		Step(fmt.Sprintf("wait to scheduler: [%s] and volume driver: [%s] to start",
			Inst().S.String(), Inst().V.String()), func() {

			err := Inst().S.IsNodeReady(nodes[nodeIndex])
			expect(err).NotTo(haveOccurred())
			UpdateOutcome(event, err)

			err = Inst().V.WaitDriverUpOnNode(nodes[nodeIndex], Inst().DriverStartTimeout)
			expect(err).NotTo(haveOccurred())
			UpdateOutcome(event, err)
		})

		Step(fmt.Sprintf("wait for px-backup pods to come up on node: [%v]", nodes[nodeIndex].Name), func() {
			// should probably make px-backup namespace a global constant
			timeout := 6 * time.Minute
			t := func() (interface{}, bool, error) {
				pxbPods, err := core.Instance().GetPodsByNode(nodes[nodeIndex].Name, "px-backup")
				if err != nil {
					logrus.Errorf("Failed to get apps on node [%s]", nodes[nodeIndex].Name)
					return "", true, err
				}
				for _, pod := range pxbPods.Items {
					if !core.Instance().IsPodReady(pod) {
						err = fmt.Errorf("Pod [%s] is not ready on node [%s] after %v", pod.Name, nodes[nodeIndex].Name, timeout)
						return "", true, err
					}
				}
				return "", false, nil
			}
			_, err := task.DoRetryWithTimeout(t, timeout, defaultRetryInterval)
			UpdateOutcome(event, err)
		})
	})

	Step("Wait for backup to complete", func() {
		if bkpError {
			logrus.Warningf("Skipping waiting for backup [%s] due to error", backupName)
		} else {
			ctx, err := backup.GetPxCentralAdminCtx()
			if err != nil {
				logrus.Errorf("Failed to fetch px-central-admin ctx: [%v]", err)
				UpdateOutcome(event, err)
			} else {
				err = Inst().Backup.WaitForBackupCompletion(
					ctx,
					backupName, OrgID,
					BackupRestoreCompletionTimeoutMin*time.Minute,
					RetrySeconds*time.Second)
				if err == nil {
					logrus.Infof("Backup [%s] completed successfully", backupName)
				} else {
					logrus.Errorf("Failed to wait for backup [%s] to complete. Error: [%v]",
						backupName, err)
					UpdateOutcome(event, err)
				}
			}
		}
	})
}

// TriggerBackupDeleteBackupPod backs up an application and restarts px-backup pod during the backup
func TriggerBackupDeleteBackupPod(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {
	defer ginkgo.GinkgoRecover()
	event := &EventRecord{
		Event: Event{
			ID:   GenerateUUID(),
			Type: BackupDeleteBackupPod,
		},
		Start:   time.Now().Format(time.RFC1123),
		Outcome: []error{},
	}

	defer func() {
		event.End = time.Now().Format(time.RFC1123)
		*recordChan <- event
	}()

	Step("Update admin secret", func() {
		err := backup.UpdatePxBackupAdminSecret()
		ProcessErrorWithMessage(event, err, "Unable to update PxBackupAdminSecret")
	})
	backupCounter++
	labelSelectors := make(map[string]string)
	sourceClusterConfigPath, err := GetSourceClusterConfigPath()
	UpdateOutcome(event, err)
	SetClusterContext(sourceClusterConfigPath)

	backupName := fmt.Sprintf("%s-%s-%d", BackupNamePrefix, "deleteBackupPod", backupCounter)
	Step("Backup all namespaces", func() {
		Step(fmt.Sprintf("Create backup full name %s:%s:%s",
			sourceClusterName, "all", backupName), func() {
			err = CreateBackupGetErr(backupName,
				sourceClusterName, backupLocationName, BackupLocationUID,
				[]string{"*"}, labelSelectors, OrgID)
			UpdateOutcome(event, err)
		})
	})

	Step("Delete px-backup pod", func() {
		ctx := &scheduler.Context{
			App: &spec.AppSpec{
				SpecList: []interface{}{
					&appsapi.Deployment{
						ObjectMeta: meta_v1.ObjectMeta{
							Name:      pxbackupDeploymentName,
							Namespace: pxbackupDeploymentNamespace,
						},
					},
				},
			},
		}
		err = Inst().S.DeleteTasks(ctx, nil)
		UpdateOutcome(event, err)

		err = Inst().S.WaitForRunning(ctx, BackupRestoreCompletionTimeoutMin, defaultRetryInterval)
		UpdateOutcome(event, err)
	})

	Step("Wait for backup to complete", func() {
		ctx, err := backup.GetPxCentralAdminCtx()
		if err != nil {
			logrus.Errorf("Failed to fetch px-central-admin ctx: [%v]", err)
			UpdateOutcome(event, err)
		} else {
			err = Inst().Backup.WaitForBackupCompletion(
				ctx,
				backupName, OrgID,
				BackupRestoreCompletionTimeoutMin*time.Minute,
				RetrySeconds*time.Second)
			if err == nil {
				logrus.Infof("Backup [%s] completed successfully", backupName)
			} else {
				logrus.Errorf("Failed to wait for backup [%s] to complete. Error: [%v]",
					backupName, err)
				UpdateOutcome(event, err)
			}
		}
	})
}

// TriggerBackupScaleMongo backs up an application and scales down Mongo pod during the backup
func TriggerBackupScaleMongo(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {
	defer ginkgo.GinkgoRecover()
	event := &EventRecord{
		Event: Event{
			ID:   GenerateUUID(),
			Type: BackupScaleMongo,
		},
		Start:   time.Now().Format(time.RFC1123),
		Outcome: []error{},
	}

	defer func() {
		event.End = time.Now().Format(time.RFC1123)
		*recordChan <- event
	}()

	Step("Update admin secret", func() {
		err := backup.UpdatePxBackupAdminSecret()
		ProcessErrorWithMessage(event, err, "Unable to update PxBackupAdminSecret")
	})
	backupCounter++
	labelSelectors := make(map[string]string)
	sourceClusterConfigPath, err := GetSourceClusterConfigPath()
	UpdateOutcome(event, err)
	SetClusterContext(sourceClusterConfigPath)

	backupName := fmt.Sprintf("%s-%s-%d", BackupNamePrefix, "scaleMongo", backupCounter)
	Step("Backup all namespaces", func() {
		Step(fmt.Sprintf("Create backup full name %s:%s:%s",
			sourceClusterName, "all", backupName), func() {
			err = CreateBackupGetErr(backupName,
				sourceClusterName, backupLocationName, BackupLocationUID,
				[]string{"*"}, labelSelectors, OrgID)
			UpdateOutcome(event, err)
		})
	})

	Step("Scale mongodb down to 0", func() {
		ctx := &scheduler.Context{
			App: &spec.AppSpec{
				SpecList: []interface{}{
					&appsapi.Deployment{
						ObjectMeta: meta_v1.ObjectMeta{
							Name:      pxbackupMongodbDeploymentName,
							Namespace: pxbackupMongodbDeploymentNamespace,
						},
					},
				},
			},
		}
		err = Inst().S.ScaleApplication(ctx, map[string]int32{
			pxbackupMongodbDeploymentName + k8s.StatefulSetSuffix: 0,
		})
		UpdateOutcome(event, err)

		Step("Giving few seconds for scaled up applications to stabilize", func() {
			time.Sleep(45 * time.Second)
		})
	})

	Step("Scale mongodb up to 3", func() {
		ctx := &scheduler.Context{
			App: &spec.AppSpec{
				SpecList: []interface{}{
					&appsapi.Deployment{
						ObjectMeta: meta_v1.ObjectMeta{
							Name:      pxbackupMongodbDeploymentName,
							Namespace: pxbackupMongodbDeploymentNamespace,
						},
					},
				},
			},
		}
		err = Inst().S.ScaleApplication(ctx, map[string]int32{
			pxbackupMongodbDeploymentName + k8s.StatefulSetSuffix: 3,
		})
		UpdateOutcome(event, err)
	})

	Step("Wait for backup to complete", func() {
		ctx, err := backup.GetPxCentralAdminCtx()
		if err != nil {
			logrus.Errorf("Failed to fetch px-central-admin ctx: [%v]", err)
			UpdateOutcome(event, err)
		} else {
			err = Inst().Backup.WaitForBackupCompletion(
				ctx,
				backupName, OrgID,
				BackupRestoreCompletionTimeoutMin*time.Minute,
				RetrySeconds*time.Second)
			if err == nil {
				logrus.Infof("Backup [%s] completed successfully", backupName)
			} else {
				logrus.Errorf("Failed to wait for backup [%s] to complete. Error: [%v]",
					backupName, err)
				UpdateOutcome(event, err)
			}
		}
	})
}

// TriggerPoolResizeDisk peforms resize-disk on the storage pools for the given contexts
func TriggerPoolResizeDisk(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {
	defer ginkgo.GinkgoRecover()
	event := &EventRecord{
		Event: Event{
			ID:   GenerateUUID(),
			Type: PoolResizeDisk,
		},
		Start:   time.Now().Format(time.RFC1123),
		Outcome: []error{},
	}

	defer func() {
		event.End = time.Now().Format(time.RFC1123)
		*recordChan <- event
	}()
	chaosLevel := getPoolExpandPercentage(PoolResizeDisk)

	Step(fmt.Sprintf("get storage pools and perform resize-disk by %v percentage on it ", chaosLevel), func() {
		time.Sleep(1 * time.Minute)
		for _, ctx := range *contexts {
			var appVolumes []*volume.Volume
			var err error
			Step("Get StoragePool IDs from the app volumes", func() {
				appVolumes, err = Inst().S.GetVolumes(ctx)
				UpdateOutcome(event, err)
				if len(appVolumes) == 0 {
					UpdateOutcome(event, fmt.Errorf("found no volumes for app %s", ctx.App.Key))
				}
				poolSet := make(map[string]struct{})
				var exists = struct{}{}
				for _, vol := range appVolumes {
					replicaSets, err := Inst().V.GetReplicaSets(vol)
					UpdateOutcome(event, err)

					for _, poolUUID := range replicaSets[0].PoolUuids {
						logrus.Infof("Pool UUID: %v, Vol: %v", poolUUID, vol.Name)
						poolSet[poolUUID] = exists

					}
				}

				for id := range poolSet {
					err = Inst().V.ResizeStoragePoolByPercentage(id, 2, uint64(chaosLevel))
					UpdateOutcome(event, err)
				}
				logrus.Infof("Waiting for 10 mins for resize to initiate and check status")
				time.Sleep(10 * time.Minute)
				Step("Validate the Pool status after re-size disk", func() {
					nodeList := node.GetStorageDriverNodes()
					if len(nodeList) == 0 {
						UpdateOutcome(event, fmt.Errorf("unable to get node list"))
					}
					for _, n := range nodeList {
						for _, p := range n.StoragePools {
							poolID := p.GetUuid()
							poolLastOperationType := p.GetLastOperation().GetType().String()
							poolLastOperationStatus := p.GetLastOperation().GetStatus().String()
							poolLastOperationMsg := p.GetLastOperation().GetMsg()
							logrus.Infof("Pool ID: %s, LastOperation: %s, Status: %s, Message:%s", poolID, poolLastOperationType, poolLastOperationStatus, poolLastOperationMsg)
							if poolLastOperationStatus != "OPERATION_SUCCESSFUL" {
								UpdateOutcome(event, fmt.Errorf("last operation %v for pool %v is failed with Msg: %v", poolLastOperationType, poolID, poolLastOperationMsg))
							}
						}
					}

				})

			})
		}
	})

}

// TriggerPoolAddDisk peforms add-disk on the storage pools for the given contexts
func TriggerPoolAddDisk(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {
	defer ginkgo.GinkgoRecover()
	event := &EventRecord{
		Event: Event{
			ID:   GenerateUUID(),
			Type: PoolResizeDisk,
		},
		Start:   time.Now().Format(time.RFC1123),
		Outcome: []error{},
	}

	defer func() {
		event.End = time.Now().Format(time.RFC1123)
		*recordChan <- event
	}()
	chaosLevel := getPoolExpandPercentage(PoolResizeDisk)
	Step(fmt.Sprintf("get storage pools and perform add-disk by %v percentage on it ", chaosLevel), func() {
		time.Sleep(1 * time.Minute)
		for _, ctx := range *contexts {
			var appVolumes []*volume.Volume
			var err error
			Step("Get StoragePool IDs from the app volumes", func() {
				appVolumes, err = Inst().S.GetVolumes(ctx)
				UpdateOutcome(event, err)
				if len(appVolumes) == 0 {
					UpdateOutcome(event, fmt.Errorf("found no volumes for app %s", ctx.App.Key))
				}
				poolSet := make(map[string]struct{})
				var exists = struct{}{}
				for _, vol := range appVolumes {
					replicaSets, err := Inst().V.GetReplicaSets(vol)
					UpdateOutcome(event, err)

					for _, poolUUID := range replicaSets[0].PoolUuids {
						logrus.Infof("Pool UUID: %v, Vol: %v", poolUUID, vol.Name)
						poolSet[poolUUID] = exists

					}
				}

				for id := range poolSet {
					err = Inst().V.ResizeStoragePoolByPercentage(id, 1, uint64(chaosLevel))
					UpdateOutcome(event, err)
				}
				logrus.Infof("Waiting for 10 mins for add disk to initiate and check status")
				time.Sleep(10 * time.Minute)
				Step("Validate the Pool status after add disk", func() {
					nodeList := node.GetStorageDriverNodes()
					if len(nodeList) == 0 {
						UpdateOutcome(event, fmt.Errorf("unable to get node list"))
					}
					for _, n := range nodeList {
						for _, p := range n.StoragePools {
							poolID := p.GetUuid()
							poolLastOperationType := p.GetLastOperation().GetType().String()
							poolLastOperationStatus := p.GetLastOperation().GetStatus().String()
							poolLastOperationMsg := p.GetLastOperation().GetMsg()
							logrus.Infof("Pool ID: %s, LastOperation: %s, Status: %s, Message:%s", poolID, poolLastOperationType, poolLastOperationStatus, poolLastOperationMsg)
							if poolLastOperationStatus != "OPERATION_SUCCESSFUL" {
								UpdateOutcome(event, fmt.Errorf("last operation %v for pool %v is failed with Msg: %v", poolLastOperationType, poolID, poolLastOperationMsg))
							}
						}
					}

				})

			})
		}
	})

}

// TriggerUpgradeStork peforms add-disk on the storage pools for the given contexts
func TriggerUpgradeStork(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {
	defer ginkgo.GinkgoRecover()
	event := &EventRecord{
		Event: Event{
			ID:   GenerateUUID(),
			Type: PoolResizeDisk,
		},
		Start:   time.Now().Format(time.RFC1123),
		Outcome: []error{},
	}

	defer func() {
		event.End = time.Now().Format(time.RFC1123)
		*recordChan <- event
	}()
	Step(fmt.Sprintf("Upgrading stork to latest version based on the compatible PX and storage driver upgrade version "),
		func() {
			err := Inst().V.UpgradeStork(Inst().StorageDriverUpgradeEndpointURL,
				Inst().StorageDriverUpgradeEndpointVersion)
			UpdateOutcome(event, err)

		})
}

func getPoolExpandPercentage(triggerType string) uint64 {
	var percentageValue uint64

	t := ChaosMap[triggerType]

	switch t {
	case 1:
		percentageValue = 100
	case 2:
		percentageValue = 90
	case 3:
		percentageValue = 80
	case 4:
		percentageValue = 70
	case 5:
		percentageValue = 60
	case 6:
		percentageValue = 50
	case 7:
		percentageValue = 40
	case 8:
		percentageValue = 30
	case 9:
		percentageValue = 20
	case 10:
		percentageValue = 10
	}
	return percentageValue

}

func getCloudSnapInterval(triggerType string) int {
	var interval int

	t := ChaosMap[triggerType]

	switch t {
	case 1:
		interval = 600
	case 2:
		interval = 500
	case 3:
		interval = 400
	case 4:
		interval = 300
	case 5:
		interval = 200
	case 6:
		interval = 100
	case 7:
		interval = 60
	case 8:
		interval = 30
	case 9:
		interval = 20
	case 10:
		interval = 10
	}
	return interval

}

func prepareEmailBody(eventRecords emailData) (string, error) {
	var err error
	t := template.New("t").Funcs(templateFuncs)
	t, err = t.Parse(htmlTemplate)
	if err != nil {
		logrus.Errorf("Cannot parse HTML template Err: %v", err)
		return "", err
	}
	var buf []byte
	buffer := bytes.NewBuffer(buf)
	err = t.Execute(buffer, eventRecords)
	if err != nil {
		logrus.Errorf("Cannot generate body from values, Err: %v", err)
		return "", err
	}

	return buffer.String(), nil
}

var templateFuncs = template.FuncMap{"rangeStruct": rangeStructer}

func rangeStructer(args ...interface{}) []interface{} {
	if len(args) == 0 {
		return nil
	}

	v := reflect.ValueOf(args[0])
	if v.Kind() != reflect.Struct {
		return nil
	}

	out := make([]interface{}, v.NumField())
	for i := 0; i < v.NumField(); i++ {
		out[i] = v.Field(i).Interface()
	}

	return out
}

var htmlTemplate = `<!DOCTYPE html>
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=utf-8" />
<script src="http://ajax.googleapis.com/ajax/libs/jquery/2.0.0/jquery.min.js"></script>
<style>
table {
  border-collapse: collapse;
}
th {
   background-color: #0ca1f0;
   text-align: center;
   padding: 3px;
}
td {
  text-align: center;
  padding: 3px;
}

tbody tr:nth-child(even) {
  background-color: #bac5ca;
}
tbody tr:last-child {
  background-color: #79ab78;
}
@media only screen and (max-width: 500px) {
	.wrapper table {
		width: 100% !important;
	}

	.wrapper .column {
		// make the column full width on small screens and allow stacking
		width: 100% !important;
		display: block !important;
	}
}
</style>
</head>
<body>
<h1>Torpedo Longevity Report</h1>
<hr/>
<h3>SetUp Details</h3>
<p><b>Master IP:</b> {{.MasterIP}}</p>
<table id="pxtable" border=1 width: 50% >
<tr>
   <td align="center"><h4>PX Node IP </h4></td>
   <td align="center"><h4>PX Node Name </h4></td>
   <td align="center"><h4>PX Version </h4></td>
   <td align="center"><h4>PX Status </h4></td>
 </tr>
{{range .NodeInfo}}<tr>
{{range rangeStruct .}} <td>{{.}}</td>
{{end}}</tr>
{{end}}
</table>
<hr/>
<h3>Running Event Details</h3>
<table border=1 width: 50%>
<tr>
   <td align="center"><h4>Trigget Name </h4></td>
   <td align="center"><h4>Interval </h4></td>
 </tr>
{{range .TriggersInfo}}<tr>
{{range rangeStruct .}} <td>{{.}}</td>
{{end}}</tr>
{{end}}
</table>
<hr/>
<h3>Event Details</h3>
<table border=1 width: 100%>
<tr>
   <td class="wrapper" width="600" align="center"><h4>Event </h4></td>
   <td align="center"><h4>Start Time </h4></td>
   <td align="center"><h4>End Time </h4></td>
   <td align="center"><h4>Errors </h4></td>
 </tr>
{{range .EmailRecords.Records}}<tr>
{{range rangeStruct .}} <td>{{.}}</td>
{{end}}</tr>
{{end}}
</table>
<script>
$('#pxtable tr td').each(function(){
  var cellValue = $(this).html();
  
    if (cellValue != "STATUS_OK") {
      $(this).css('background-color','red');
    }
});
</script>
<hr/>
</table>
</body>
</html>`
