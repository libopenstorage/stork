package tests

import (
	"bytes"
	"fmt"
	"math"
	"math/rand"
	"os"
	"os/exec"
	"reflect"
	"sort"
	"strings"
	"sync"
	"text/template"
	"time"

	apapi "github.com/libopenstorage/autopilot-api/pkg/apis/autopilot/v1alpha1"
	"github.com/portworx/torpedo/pkg/applicationbackup"
	"github.com/portworx/torpedo/pkg/aututils"
	"github.com/portworx/torpedo/pkg/log"
	"github.com/portworx/torpedo/pkg/units"
	"gopkg.in/natefinch/lumberjack.v2"

	"container/ring"

	"github.com/kubernetes-csi/external-snapshotter/client/v4/apis/volumesnapshot/v1beta1"
	"github.com/onsi/ginkgo"

	opsapi "github.com/libopenstorage/openstorage/api"
	"github.com/pborman/uuid"
	api "github.com/portworx/px-backup-api/pkg/apis/v1"
	"github.com/portworx/sched-ops/k8s/core"
	"github.com/portworx/sched-ops/task"

	storkapi "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	storkv1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	storage "github.com/portworx/sched-ops/k8s/storage"
	storkops "github.com/portworx/sched-ops/k8s/stork"
	"github.com/portworx/torpedo/drivers/backup"
	"github.com/portworx/torpedo/drivers/monitor/prometheus"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/portworx/torpedo/drivers/scheduler/k8s"
	"github.com/portworx/torpedo/drivers/scheduler/spec"
	"github.com/portworx/torpedo/drivers/volume"
	appsapi "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	storageapi "k8s.io/api/storage/v1"
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/portworx/torpedo/pkg/asyncdr"
	"github.com/portworx/torpedo/pkg/email"
	"github.com/portworx/torpedo/pkg/errors"
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
	// PureSecretName pure secret name
	PureSecretName = "px-pure-secret"
	// PureSecretDataField is pure json file name
	PureSecretDataField = "pure.json"
	// PureSnapShotClass is pure Snapshot class name
	PureSnapShotClass = "px-pure-snapshotclass"
	// PureBlockStorageClass is pure storage class for FA volumes
	PureBlockStorageClass = "px-pure-block"
	// PureFileStorageClass is pure storage class for FA volumes
	PureFileStorageClass = "px-pure-file"
	// ReclaimPolicyDelete is reclaim policy
	ReclaimPolicyDelete = "Delete"
	// PureBackend is a key for parameter map
	PureBackend = "backend"
	// EssentialsFaFbSKU is license strings for FA/FB essential license
	EssentialsFaFbSKU = "Portworx CSI for FA/FB"
	fastpathAppName   = "fastpath"
)

const (
	// PureTopologyField to check for pure Topology is enabled on cluster
	PureTopologyField = "pureTopology"
	// HyperConvergedTypeField to schedule apps on both storage and storageless nodes
	HyperConvergedTypeField = "hyperConverged"
)

const (
	validateReplicationUpdateTimeout = 2 * time.Hour
	errorChannelSize                 = 50
)

const (
	pxStatusError  = "ERROR GETTING PX STATUS"
	pxVersionError = "ERROR GETTING PX VERSION"
)

var longevityLogger *lumberjack.Logger

// EmailRecipients list of email IDs to send email to
var EmailRecipients []string

// RunningTriggers map of events and corresponding interval
var RunningTriggers map[string]time.Duration

// ChaosMap stores mapping between test trigger and its chaos level.
var ChaosMap map[string]int

// coresMap stores mapping between node name and cores generated.
var coresMap map[string]string

// SendGridEmailAPIKey holds API key used to interact
// with SendGrid Email APIs
var SendGridEmailAPIKey string

// backupCounter holds the iteration of TriggerBacku
var backupCounter = 0

// restoreCounter holds the iteration of TriggerRestore
var restoreCounter = 0

// newNamespaceCounter holds the count of current namespace
var newNamespaceCounter = 0

// jiraEvents to store raised jira events data
var jiraEvents = make(map[string][]string)

// isAutoFsTrimEnabled to store if auto fs trim enalbed
var isAutoFsTrimEnabled = false

// setIoPriority to set IOPriority
var setIoPriority = true

// isCsiVolumeSnapshotClassExist to store if snapshot class exist
var isCsiVolumeSnapshotClassExist = false

// isCsiRestoreStorageClassExist to store if restore storage class exist
var isCsiRestoreStorageClassExist = false

// isRelaxedReclaimEnabled to store if relaxed reclaim enalbed
var isRelaxedReclaimEnabled = false

// isTrashcanEnabled to store if trashcan enalbed
var isTrashcanEnabled = false

// volSnapshotClass is snapshot class for FA volumes
var volSnapshotClass *v1beta1.VolumeSnapshotClass

// pureStorageClassMap is map of pure storage class
var pureStorageClassMap map[string]*storageapi.StorageClass

// DefaultSnapshotRetainCount is default snapshot retain count
var DefaultSnapshotRetainCount = 10

// TotalTriggerCount is counter metric for test trigger
var TotalTriggerCount = prometheus.TorpedoTestTotalTriggerCount

// TestRunningState is gauage metric for test method running
var TestRunningState = prometheus.TorpedoTestRunning

// TestFailedCount is counter metric for test failed
var TestFailedCount = prometheus.TorpedoTestFailCount

// TestPassedCount is counter metric for test passed
var TestPassedCount = prometheus.TorpedoTestPassCount

// FailedTestAlert is a flag to alert test failed
var FailedTestAlert = prometheus.TorpedoAlertTestFailed

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

// decommissionedNode for rejoin test
var decommissionedNode = node.Node{}

// node with autopilot rule enabled
var autoPilotLabelNode node.Node

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
	MgmtIP     string
	NodeName   string
	PxVersion  string
	Status     string
	NodeStatus string
	Cores      string
}

type triggerInfo struct {
	Name     string
	Duration time.Duration
}

// FlashArray structure for pure secret
type FlashArray struct {
	MgmtEndPoint string
	APIToken     string
	Labels       map[string]string
}

// FlashBlade Structure for pure secret
type FlashBlade struct {
	MgmtEndPoint string
	APIToken     string
	NFSEndPoint  string
	Labels       map[string]string
}

// PureSecret represent px-pure-secret for FADA
type PureSecret struct {
	FlashArrays []FlashArray
	FlashBlades []FlashBlade
}

// GenerateUUID generates unique ID
func GenerateUUID() string {
	uuidbyte, _ := exec.Command("uuidgen").Output()
	return strings.TrimSpace(string(uuidbyte))
}

// UpdateOutcome updates outcome based on error
func UpdateOutcome(event *EventRecord, err error) {

	if err != nil && event != nil {
		Inst().M.IncrementCounterMetric(TestFailedCount, event.Event.Type)
		dash.VerifySafely(err, nil, fmt.Sprintf("verify if error occured for event %s", event.Event.Type))
		er := fmt.Errorf(err.Error() + "<br>")
		Inst().M.IncrementGaugeMetricsUsingAdditionalLabel(FailedTestAlert, event.Event.Type, err.Error())
		event.Outcome = append(event.Outcome, er)
		createLongevityJiraIssue(event, er)
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
	// RestartManyVolDriver restarts one or more volume drivers at time
	RestartManyVolDriver = "restartManyVolDriver"
	// RestartKvdbVolDriver restarat kvdb volume driver
	RestartKvdbVolDriver = "restartKvdbVolDriver"
	// CrashVolDriver crashes volume driver
	CrashVolDriver = "crashVolDriver"
	// RebootNode reboots all nodes one by one
	RebootNode = "rebootNode"
	// RebootManyNodes reboots one or more nodes at time
	RebootManyNodes = "rebootManyNodes"
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
	// CsiSnapShot takes csi snapshot of the volumes
	CsiSnapShot = "csiSnapShot"
	//CsiSnapRestore takes restore
	CsiSnapRestore = "csiSnapRestore"
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
	// UpgradeVolumeDriver  upgrade volume driver version to the latest build
	UpgradeVolumeDriver = "upgradeVolumeDriver"
	// AppTasksDown scales app up and down
	AppTasksDown = "appScaleUpAndDown"
	// AutoFsTrim enables Auto Fstrim in PX cluster
	AutoFsTrim = "autoFsTrim"
	// UpdateVolume provides option to update volume with properties like iopriority.
	UpdateVolume = "updateVolume"
	// NodeDecommission decommission random node in the PX cluster
	NodeDecommission = "nodeDecomm"
	//NodeRejoin rejoins the decommissioned node into the PX cluster
	NodeRejoin = "nodeRejoin"
	// RelaxedReclaim enables RelaxedReclaim in PX cluster
	RelaxedReclaim = "relaxedReclaim"
	// Trashcan enables Trashcan in PX cluster
	Trashcan = "trashcan"
	//KVDBFailover cyclic restart of kvdb nodes
	KVDBFailover = "kvdbFailover"
	// ValidateDeviceMapper validate device mapper cleanup
	ValidateDeviceMapper = "validateDeviceMapper"
	// AsyncDR runs Async DR between two clusters
	AsyncDR = "asyncdr"
	// AsyncDR Volume Only runs Async DR volume only migration between two clusters
	AsyncDRVolumeOnly = "asyncdrvolumeonly"
	// stork application backup runs stork backups for applications
	StorkApplicationBackup = "storkapplicationbackup"
	// stork application backup volume resize runs stork backups for applications and inject volume resize in between
	StorkAppBkpVolResize = "storkappbkpvolresize"
	// stork application backup volume resize runs stork backups for applications and inject volume ha-update in between
	StorkAppBkpHaUpdate = "storkappbkphaupdate"
	// stork application backup runs with px restart
	StorkAppBkpPxRestart = "storkappbkppxrestart"
	// HAIncreaseAndReboot performs repl-add
	HAIncreaseAndReboot = "haIncreaseAndReboot"
	// AddDrive performs drive add for on-prem cluster
	AddDrive = "addDrive"
	// AddDiskAndReboot performs add-disk and reboots node
	AddDiskAndReboot = "addDiskAndReboot"
	// ResizeDiskAndReboot performs  resize-disk and reboots node
	ResizeDiskAndReboot = "resizeDiskAndReboot"
	// AutopilotRebalance performs  pool rebalance
	AutopilotRebalance = "autopilotRebalance"
	// VolumeCreatePxRestart performs  volume create and px restart parallel
	VolumeCreatePxRestart = "volumeCreatePxRestart"
)

// TriggerCoreChecker checks if any cores got generated
func TriggerCoreChecker(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {
	defer ginkgo.GinkgoRecover()
	defer endLongevityTest()
	startLongevityTest(CoreChecker)
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
	coresMap = nil
	coresMap = make(map[string]string)
	setMetrics(*event)

	context("checking for core files...", func() {
		Step("verifying if core files are present on each node", func() {
			log.InfoD("verifying if core files are present on each node")
			nodes := node.GetWorkerNodes()
			dash.VerifyFatal(len(nodes) > 0, true, "Nodes registered?")
			log.Infof("len nodes: %v", len(nodes))
			for _, n := range nodes {
				if !n.IsStorageDriverInstalled {
					log.Infof("%v is not storage driver", n.Name)
					continue
				}
				log.Infof("looking for core files on node %s", n.Name)
				file, err := Inst().N.SystemCheck(n, node.ConnectionOpts{
					Timeout:         2 * time.Minute,
					TimeBeforeRetry: 10 * time.Second,
				})
				UpdateOutcome(event, err)

				if len(file) != 0 {
					log.Warnf("[%s] found on node [%s]", file, n.Name)
					coresMap[n.Name] = "1"
					createLongevityJiraIssue(event, fmt.Errorf("[%s] found on node [%s]", file, n.Name))
				} else {
					coresMap[n.Name] = ""

				}
			}
		})
	})
}

func startLongevityTest(testName string) {
	longevityLogger = CreateLogger(fmt.Sprintf("%s-%s.log", testName, time.Now().Format(time.RFC3339)))
	log.SetTorpedoFileOutput(longevityLogger)
	dash.TestCaseBegin(testName, fmt.Sprintf("validating %s in longevity cluster", testName), "", nil)
}
func endLongevityTest() {
	dash.TestCaseEnd()
	CloseLogger(longevityLogger)
}

// TriggerDeployNewApps deploys applications in separate namespaces
func TriggerDeployNewApps(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {
	defer ginkgo.GinkgoRecover()
	defer endLongevityTest()
	startLongevityTest(DeployApps)
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

	Inst().M.IncrementCounterMetric(TotalTriggerCount, event.Event.Type)
	Inst().M.SetGaugeMetricWithNonDefaultLabels(FailedTestAlert, 0, event.Event.Type, "")

	Step(fmt.Sprintf("Set throttle to re-sync"), func() {
		UpdateOutcome(event, updatePxRuntimeOpts())
	})

	errorChan := make(chan error, errorChannelSize)
	labels := Inst().TopologyLabels
	Step("Deploy applications", func() {
		if len(labels) > 0 {
			for i := 0; i < Inst().GlobalScaleFactor; i++ {
				newContexts := ScheduleAppsInTopologyEnabledCluster(
					fmt.Sprintf("longevity-%d", i), labels, &errorChan,
				)
				*contexts = append(*contexts, newContexts...)
			}
		} else {
			for i := 0; i < Inst().GlobalScaleFactor; i++ {
				newContexts := ScheduleApplications(fmt.Sprintf("longevity-%d", i), &errorChan)
				*contexts = append(*contexts, newContexts...)
			}
		}

		for _, ctx := range *contexts {
			log.Infof("Validating context: %v", ctx.App.Key)
			ctx.SkipVolumeValidation = false
			ValidateContext(ctx, &errorChan)
			// BUG: Execution doesn't resume here after ValidateContext called
			// Below code is never executed
			for err := range errorChan {
				log.Infof("Error: %v", err)
				UpdateOutcome(event, err)
			}
		}
	})
}

// TriggerVolumeCreatePXRestart create volume , attach , detach and reboot nodes parallely
func TriggerVolumeCreatePXRestart(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {

	defer ginkgo.GinkgoRecover()
	defer endLongevityTest()
	startLongevityTest(VolumeCreatePxRestart)
	event := &EventRecord{
		Event: Event{
			ID:   GenerateUUID(),
			Type: VolumeCreatePxRestart,
		},
		Start:   time.Now().Format(time.RFC1123),
		Outcome: []error{},
	}

	defer func() {
		event.End = time.Now().Format(time.RFC1123)
		*recordChan <- event
	}()

	setMetrics(*event)
	stepLog := "Create multiple volumes , attached and restart PX"
	var createdVolIDs map[string]string
	var err error
	volCreateCount := 10
	Step(stepLog, func() {
		log.InfoD(stepLog)

		stNodes := node.GetStorageNodes()
		index := randIntn(1, len(stNodes))[0]

		selectedNode := stNodes[index]

		log.InfoD("Creating and attaching %d volumes on node %s", volCreateCount, selectedNode.Name)

		wg := new(sync.WaitGroup)
		wg.Add(1)
		go func(appNode node.Node) {
			createdVolIDs, err = CreateMultiVolumesAndAttach(wg, volCreateCount, selectedNode.Id)
			if err != nil {
				UpdateOutcome(event, err)
			}
		}(selectedNode)
		time.Sleep(3 * time.Second)
		wg.Add(1)
		go func(appNode node.Node) {
			defer wg.Done()
			stepLog = fmt.Sprintf("restart volume driver %s on node: %s", Inst().V.String(), appNode.Name)
			Step(stepLog, func() {
				log.InfoD(stepLog)
				err = Inst().V.RestartDriver(appNode, nil)
				UpdateOutcome(event, err)

			})
		}(selectedNode)
		wg.Wait()

	})

	stepLog = "Validate the created volumes"
	Step(stepLog, func() {
		log.InfoD(stepLog)
		var cVol *opsapi.Volume
		var err error

		for vol, volPath := range createdVolIDs {
			//TODO: remove this retry once PWX-27773 is fixed
			t := func() (interface{}, bool, error) {
				cVol, err = Inst().V.InspectVolume(vol)
				if err != nil {
					return cVol, true, fmt.Errorf("error inspecting volume %s, err : %v", vol, err)
				}

				if !strings.Contains(cVol.DevicePath, "pxd/") {
					return cVol, true, fmt.Errorf("path %s is not correct", cVol.DevicePath)
				}
				if cVol.DevicePath == "" {
					return cVol, false, fmt.Errorf("device path is not present for volume: %s", vol)
				}
				return cVol, true, err
			}

			_, err := task.DoRetryWithTimeout(t, 5*time.Minute, 10*time.Second)
			if err != nil {
				UpdateOutcome(event, err)
			} else {
				dash.VerifySafely(cVol.State, opsapi.VolumeState_VOLUME_STATE_ATTACHED, fmt.Sprintf("Verify vol %s is attached", cVol.Id))
				dash.VerifySafely(cVol.DevicePath, volPath, fmt.Sprintf("Verify vol %s is has device path", cVol.Id))
			}

		}
	})

	stepLog = "Deleting the created volumes"
	Step(stepLog, func() {
		log.InfoD(stepLog)

		for vol := range createdVolIDs {
			log.Infof("Detaching and deleting volume: %s", vol)
			err := Inst().V.DetachVolume(vol)
			if err == nil {
				err = Inst().V.DeleteVolume(vol)
			}
			UpdateOutcome(event, err)

		}
	})
}

// TriggerHAIncreaseAndReboot triggers repl increase and reboots target and source nodes
func TriggerHAIncreaseAndReboot(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {
	defer ginkgo.GinkgoRecover()
	defer endLongevityTest()
	startLongevityTest(HAIncreaseAndReboot)
	event := &EventRecord{
		Event: Event{
			ID:   GenerateUUID(),
			Type: HAIncreaseAndReboot,
		},
		Start:   time.Now().Format(time.RFC1123),
		Outcome: []error{},
	}

	defer func() {
		event.End = time.Now().Format(time.RFC1123)
		*recordChan <- event
	}()

	setMetrics(*event)

	//Reboot target node and source node while repl increase is in progress
	stepLog := "get a volume to  increase replication factor and reboot source  and target node"
	Step(stepLog, func() {
		log.InfoD("get a volume to  increase replication factor and reboot source  and target node")
		storageNodeMap := make(map[string]node.Node)
		storageNodes, err := GetStorageNodes()
		UpdateOutcome(event, err)

		for _, n := range storageNodes {
			storageNodeMap[n.Id] = n
		}

		for _, ctx := range *contexts {
			var appVolumes []*volume.Volume
			var err error
			stepLog = fmt.Sprintf("get volumes for %s app", ctx.App.Key)
			Step(stepLog, func() {
				log.InfoD(stepLog)
				appVolumes, err = Inst().S.GetVolumes(ctx)
				UpdateOutcome(event, err)
				if len(appVolumes) == 0 {
					UpdateOutcome(event, fmt.Errorf("found no volumes for app %s", ctx.App.Key))
				}
			})

			if strings.Contains(ctx.App.Key, "fio-fstrim") {
				for _, v := range appVolumes {
					// Check if volumes are Pure FA/FB DA volumes
					isPureVol, err := Inst().V.IsPureVolume(v)
					if err != nil {
						UpdateOutcome(event, err)
					}
					if isPureVol {
						log.Warnf("Repl increase on Pure DA Volume [%s] not supported.Skiping this operation", v.Name)
						continue
					}

					currRep, err := Inst().V.GetReplicationFactor(v)
					UpdateOutcome(event, err)

					if currRep != 0 {
						//Changing replication factor to 1
						if currRep > 1 {
							log.Infof("Current replication is > 1, setting it to 1 before proceeding")
							opts := volume.Options{
								ValidateReplicationUpdateTimeout: validateReplicationUpdateTimeout,
							}
							rep := currRep
							for rep > 1 {
								err = Inst().V.SetReplicationFactor(v, rep-1, nil, nil, true, opts)
								if err != nil {
									log.Errorf("There is an error decreasing repl [%v]", err.Error())
									UpdateOutcome(event, err)

								}
								rep--

							}

						}
					}

					if err == nil {
						HaIncreaseRebootTargetNode(event, ctx, v, storageNodeMap)
						HaIncreaseRebootSourceNode(event, ctx, v, storageNodeMap)
					}
				}
			}
		}
		updateMetrics(*event)
	})
}

// TriggerHAIncrease performs repl-add on all volumes of given contexts
func TriggerHAIncrease(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {
	defer ginkgo.GinkgoRecover()
	defer endLongevityTest()
	startLongevityTest(HAIncrease)
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
	setMetrics(*event)

	expReplMap := make(map[*volume.Volume]int64)
	stepLog := "get volumes for all apps in test and increase replication factor"
	Step(stepLog, func() {
		log.InfoD(stepLog)
		time.Sleep(10 * time.Minute)
		for _, ctx := range *contexts {
			var appVolumes []*volume.Volume
			var err error
			stepLog = fmt.Sprintf("get volumes for %s app", ctx.App.Key)
			Step(stepLog, func() {
				log.InfoD(stepLog)
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
				// Check if volumes are Pure FA/FB DA volumes
				isPureVol, err := Inst().V.IsPureVolume(v)
				if err != nil {
					UpdateOutcome(event, err)
				}
				if isPureVol {
					log.Warnf("Repl increase on Pure DA Volume [%s] not supported.Skiping this operation", v.Name)
					continue
				}
				MaxRF := Inst().V.GetMaxReplicationFactor()
				stepLog = fmt.Sprintf("repl increase volume driver %s on app %s's volume: %v",
					Inst().V.String(), ctx.App.Key, v)
				Step(stepLog,
					func() {
						log.InfoD(stepLog)
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

						if MaxRF > 3 {
							MaxRF = 3
						}

						expRF := currRep + 1

						if expRF > MaxRF {
							errExpected = true
							expRF = currRep
						}
						log.InfoD("Expected Replication factor %v", expRF)
						log.InfoD("Max Replication factor %v", MaxRF)
						expReplMap[v] = expRF
						if !errExpected {
							if strings.Contains(ctx.App.Key, fastpathAppName) {
								newFastPathNode, err := AddFastPathLabel(ctx)
								if err == nil {
									defer Inst().S.RemoveLabelOnNode(*newFastPathNode, k8s.NodeType)
								}
								UpdateOutcome(event, err)
							}
							replSets, err := Inst().V.GetReplicaSets(v)
							if err != nil {
								log.Infof("Replica Set before ha-increase : %+v", replSets[0].Nodes)
							}
							err = Inst().V.SetReplicationFactor(v, expRF, nil, nil, true, opts)
							if err != nil {
								log.Errorf("There is a error setting repl [%v]", err.Error())
							}
							UpdateOutcome(event, err)
						} else {
							log.Warnf("cannot peform HA increase as new repl factor value is greater than max allowed %v", MaxRF)
						}
					})
				stepLog = fmt.Sprintf("validate successful repl increase on app %s's volume: %v",
					ctx.App.Key, v)
				Step(stepLog,
					func() {
						newRepl, err := Inst().V.GetReplicationFactor(v)
						UpdateOutcome(event, err)

						dash.VerifySafely(newRepl, expReplMap[v], fmt.Sprintf("validat repl update for volume %s", v.Name))
						log.InfoD("repl increase validation completed on app %s", v.Name)
						replSets, err := Inst().V.GetReplicaSets(v)
						if err != nil {
							log.Infof("Replica Set after ha-increase : %+v", replSets[0].Nodes)
						}
					})
			}
			stepLog = fmt.Sprintf("validating context after increasing HA for app: %s",
				ctx.App.Key)
			Step(stepLog, func() {
				log.InfoD(stepLog)
				errorChan := make(chan error, errorChannelSize)
				ctx.SkipVolumeValidation = true
				log.InfoD("Context Validation after increasing HA started for  %s", ctx.App.Key)
				ValidateContext(ctx, &errorChan)
				log.InfoD("Context Validation after increasing HA is completed for  %s", ctx.App.Key)
				for err := range errorChan {
					if err != nil {
						log.Errorf("There is a error in context validation [%v]", err.Error())
					}
					UpdateOutcome(event, err)
					log.Infof("Context outcome after increasing HA is updated for  %s", ctx.App.Key)
				}
				if strings.Contains(ctx.App.Key, fastpathAppName) {
					err := ValidateFastpathVolume(ctx, opsapi.FastpathStatus_FASTPATH_INACTIVE)
					UpdateOutcome(event, err)
				}
			})
		}
		updateMetrics(*event)
	})
}

// TriggerHADecrease performs repl-reduce on all volumes of given contexts
func TriggerHADecrease(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {
	defer ginkgo.GinkgoRecover()
	defer endLongevityTest()
	startLongevityTest(HADecrease)
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
	setMetrics(*event)

	expReplMap := make(map[*volume.Volume]int64)
	stepLog := "get volumes for all apps in test and decrease replication factor"
	Step(stepLog, func() {
		log.InfoD(stepLog)
		for _, ctx := range *contexts {
			var appVolumes []*volume.Volume
			var err error
			stepLog = fmt.Sprintf("get volumes for %s app", ctx.App.Key)
			Step(stepLog, func() {
				log.InfoD(stepLog)
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
				// Skipping repl decrease for pure volumes
				isPureVol, err := Inst().V.IsPureVolume(v)
				if err != nil {
					UpdateOutcome(event, err)
				}
				if isPureVol {
					log.Warnf("Repl decrease on Pure DA volume:[%s] not supported.Skipping repl decrease operation in pure volume", v.Name)
					continue
				}
				MinRF := Inst().V.GetMinReplicationFactor()
				stepLog = fmt.Sprintf("repl decrease volume driver %s on app %s's volume: %v",
					Inst().V.String(), ctx.App.Key, v)
				Step(stepLog,
					func() {
						log.InfoD(stepLog)
						errExpected := false
						currRep, err := Inst().V.GetReplicationFactor(v)
						UpdateOutcome(event, err)
						expRF := currRep - 1

						if expRF < MinRF {
							errExpected = true
							expRF = currRep
						}
						expReplMap[v] = expRF
						log.InfoD("Expected Replication factor %v", expRF)
						log.InfoD("Min Replication factor %v", MinRF)
						if !errExpected {
							replSets, err := Inst().V.GetReplicaSets(v)
							if err != nil {
								log.Infof("Replica Set before ha-decrease : %+v", replSets[0].Nodes)
							}
							err = Inst().V.SetReplicationFactor(v, currRep-1, nil, nil, true, opts)
							if err != nil {
								log.Errorf("There is an error decreasing repl [%v]", err.Error())
							}
							UpdateOutcome(event, err)
						} else {
							log.Warnf("cannot perfomr HA reduce as new repl factor is less than minimum value %v ", MinRF)
						}

					})
				stepLog = fmt.Sprintf("validate successful repl decrease on app %s's volume: %v",
					ctx.App.Key, v)
				Step(stepLog,
					func() {
						log.InfoD(stepLog)
						newRepl, err := Inst().V.GetReplicationFactor(v)
						UpdateOutcome(event, err)
						dash.VerifySafely(newRepl, expReplMap[v], "Validate reduced rep value for the volume")
						log.InfoD("repl decrease validation completed on app %s", v.Name)
						replSets, err := Inst().V.GetReplicaSets(v)
						if err != nil {
							log.Infof("Replica Set after ha-decrease : %+v", replSets[0].Nodes)
						}

					})
			}
			stepLog = fmt.Sprintf("validating context after reducing HA for app: %s",
				ctx.App.Key)
			Step(stepLog, func() {
				log.InfoD(stepLog)
				errorChan := make(chan error, errorChannelSize)
				ctx.SkipVolumeValidation = true
				log.InfoD("Context Validation after reducing HA started for  %s", ctx.App.Key)
				ValidateContext(ctx, &errorChan)
				log.InfoD("Context Validation after reducing HA is completed for  %s", ctx.App.Key)
				for err := range errorChan {
					UpdateOutcome(event, err)
					log.Infof("Context outcome after reducing HA is updated for  %s", ctx.App.Key)
				}
				if strings.Contains(ctx.App.Key, fastpathAppName) {
					err := ValidateFastpathVolume(ctx, opsapi.FastpathStatus_FASTPATH_ACTIVE)
					UpdateOutcome(event, err)
				}
			})
		}
		updateMetrics(*event)
	})
}

// TriggerAppTaskDown deletes application task for all contexts
func TriggerAppTaskDown(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {
	defer ginkgo.GinkgoRecover()
	defer endLongevityTest()
	startLongevityTest(AppTaskDown)
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
	setMetrics(*event)
	stepLog := ""
	for _, ctx := range *contexts {
		stepLog = fmt.Sprintf("delete tasks for app: [%s]", ctx.App.Key)
		Step(stepLog, func() {
			log.InfoD(stepLog)
			err := Inst().S.DeleteTasks(ctx, nil)
			if err != nil {
				PrintDescribeContext(ctx)
			}
			UpdateOutcome(event, err)
		})
		stepLog = fmt.Sprintf("validating context after delete tasks for app: [%s]",
			ctx.App.Key)
		Step(stepLog, func() {
			log.InfoD(stepLog)
			errorChan := make(chan error, errorChannelSize)
			ctx.SkipVolumeValidation = true
			ValidateContext(ctx, &errorChan)
			for err := range errorChan {
				UpdateOutcome(event, err)
			}
		})
	}
	updateMetrics(*event)
}

// TriggerCrashVolDriver crashes vol driver
func TriggerCrashVolDriver(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {
	defer ginkgo.GinkgoRecover()
	defer endLongevityTest()
	startLongevityTest(CrashVolDriver)
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
	setMetrics(*event)
	stepLog := "crash volume driver in all nodes"
	Step(stepLog, func() {
		log.InfoD(stepLog)
		for _, appNode := range node.GetStorageDriverNodes() {
			stepLog = fmt.Sprintf("crash volume driver %s on node: %v",
				Inst().V.String(), appNode.Name)
			Step(stepLog,
				func() {
					log.InfoD(stepLog)
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
		updateMetrics(*event)
	})
}

// TriggerRestartVolDriver restarts volume driver and validates app
func TriggerRestartVolDriver(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {
	defer ginkgo.GinkgoRecover()
	defer endLongevityTest()
	startLongevityTest(RestartVolDriver)
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
	setMetrics(*event)
	stepLog := "get nodes bounce volume driver"
	Step(stepLog, func() {
		log.InfoD(stepLog)
		for _, appNode := range node.GetStorageDriverNodes() {
			stepLog = fmt.Sprintf("stop volume driver %s on node: %s",
				Inst().V.String(), appNode.Name)
			Step(stepLog,
				func() {
					log.InfoD(stepLog)
					taskStep := fmt.Sprintf("stop volume driver on node: %s.",
						appNode.MgmtIp)
					event.Event.Type += "<br>" + taskStep
					errorChan := make(chan error, errorChannelSize)
					StopVolDriverAndWait([]node.Node{appNode}, &errorChan)
					for err := range errorChan {
						UpdateOutcome(event, err)
					}
				})
			stepLog = fmt.Sprintf("starting volume %s driver on node %s",
				Inst().V.String(), appNode.Name)
			Step(stepLog,
				func() {
					log.InfoD(stepLog)
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
				stepLog = fmt.Sprintf("RestartVolDriver: validating app [%s]", ctx.App.Key)
				Step(stepLog, func() {
					log.InfoD(stepLog)
					errorChan := make(chan error, errorChannelSize)
					ctx.ReadinessTimeout = time.Minute * 10
					ValidateContext(ctx, &errorChan)
					for err := range errorChan {
						UpdateOutcome(event, err)
					}
					if strings.Contains(ctx.App.Key, fastpathAppName) {
						err := ValidateFastpathVolume(ctx, opsapi.FastpathStatus_FASTPATH_ACTIVE)
						UpdateOutcome(event, err)
					}
				})
			}
		}
		updateMetrics(*event)
	})
}

// TriggerRestartManyVolDriver restarts one or more volume drivers and validates app
func TriggerRestartManyVolDriver(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {
	defer ginkgo.GinkgoRecover()
	defer endLongevityTest()
	startLongevityTest(RestartManyVolDriver)
	event := &EventRecord{
		Event: Event{
			ID:   GenerateUUID(),
			Type: RestartManyVolDriver,
		},
		Start:   time.Now().Format(time.RFC1123),
		Outcome: []error{},
	}

	defer func() {
		event.End = time.Now().Format(time.RFC1123)
		*recordChan <- event
	}()

	setMetrics(*event)

	driverNodesToRestart := getNodesByChaosLevel(RestartManyVolDriver)
	var wg sync.WaitGroup
	stepLog := "get nodes bounce volume driver"
	Step(stepLog, func() {
		log.InfoD(stepLog)
		for _, appNode := range driverNodesToRestart {
			wg.Add(1)
			go func(appNode node.Node) {
				defer wg.Done()
				stepLog = fmt.Sprintf("stop volume driver %s on node: %s", Inst().V.String(), appNode.Name)
				Step(stepLog, func() {
					log.InfoD(stepLog)
					taskStep := fmt.Sprintf("stop volume driver on node: %s.",
						appNode.MgmtIp)
					event.Event.Type += "<br>" + taskStep
					errorChan := make(chan error, errorChannelSize)
					StopVolDriverAndWait([]node.Node{appNode}, &errorChan)
					for err := range errorChan {
						UpdateOutcome(event, err)
					}
				})
			}(appNode)
		}

		Step("wait all the storage drivers to be stopped", func() {
			wg.Wait()
		})

		for _, appNode := range driverNodesToRestart {
			wg.Add(1)
			go func(appNode node.Node) {
				defer wg.Done()
				stepLog = fmt.Sprintf("starting volume %s driver on node %s", Inst().V.String(), appNode.Name)
				Step(stepLog, func() {
					log.InfoD(stepLog)
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
			}(appNode)
		}
		stepLog = "wait all the storage drivers to be up"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			wg.Wait()
		})
		for _, ctx := range *contexts {
			stepLog = fmt.Sprintf("RestartVolDriver: validating app [%s]", ctx.App.Key)
			Step(stepLog, func() {
				errorChan := make(chan error, errorChannelSize)
				ctx.ReadinessTimeout = time.Minute * 10
				ValidateContext(ctx, &errorChan)
				for err := range errorChan {
					UpdateOutcome(event, err)
				}
				if strings.Contains(ctx.App.Key, fastpathAppName) {
					err := ValidateFastpathVolume(ctx, opsapi.FastpathStatus_FASTPATH_ACTIVE)
					UpdateOutcome(event, err)
				}
			})
		}
		updateMetrics(*event)

	})
}

// TriggerRestartKvdbVolDriver restarts volume driver where kvdb resides and validates app
func TriggerRestartKvdbVolDriver(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {
	defer ginkgo.GinkgoRecover()
	defer endLongevityTest()
	startLongevityTest(RestartKvdbVolDriver)
	event := &EventRecord{
		Event: Event{
			ID:   GenerateUUID(),
			Type: RestartKvdbVolDriver,
		},
		Start:   time.Now().Format(time.RFC1123),
		Outcome: []error{},
	}

	defer func() {
		event.End = time.Now().Format(time.RFC1123)
		*recordChan <- event
	}()

	setMetrics(*event)
	stepLog := "get kvdb nodes bounce volume driver"
	Step(stepLog, func() {
		for _, appNode := range node.GetMetadataNodes() {
			stepLog = fmt.Sprintf("stop volume driver %s on node: %s",
				Inst().V.String(), appNode.Name)
			Step(stepLog,
				func() {
					log.InfoD(stepLog)
					taskStep := fmt.Sprintf("stop volume driver on node: %s.",
						appNode.MgmtIp)
					event.Event.Type += "<br>" + taskStep
					errorChan := make(chan error, errorChannelSize)
					StopVolDriverAndWait([]node.Node{appNode}, &errorChan)
					for err := range errorChan {
						UpdateOutcome(event, err)
					}
				})
			stepLog = fmt.Sprintf("starting volume %s driver on node %s",
				Inst().V.String(), appNode.Name)
			Step(stepLog,
				func() {
					log.InfoD(stepLog)
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
				stepLog = fmt.Sprintf("RestartVolDriver: validating app [%s]", ctx.App.Key)
				Step(stepLog, func() {
					log.InfoD(stepLog)
					errorChan := make(chan error, errorChannelSize)
					ctx.ReadinessTimeout = time.Minute * 10
					ValidateContext(ctx, &errorChan)
					for err := range errorChan {
						UpdateOutcome(event, err)
					}
					if strings.Contains(ctx.App.Key, fastpathAppName) {
						err := ValidateFastpathVolume(ctx, opsapi.FastpathStatus_FASTPATH_ACTIVE)
						UpdateOutcome(event, err)
					}
				})
			}
		}
		updateMetrics(*event)
	})
}

// TriggerRebootNodes reboots node on which apps are running
func TriggerRebootNodes(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {
	defer ginkgo.GinkgoRecover()
	defer endLongevityTest()
	startLongevityTest(RebootNode)
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

	setMetrics(*event)
	stepLog := "get all nodes and reboot one by one"
	Step(stepLog, func() {
		log.InfoD(stepLog)
		nodesToReboot := node.GetWorkerNodes()

		// Reboot node and check driver status
		stepLog = fmt.Sprintf("reboot node one at a time from the node(s): %v", nodesToReboot)
		Step(stepLog, func() {
			// TODO: Below is the same code from existing nodeReboot test
			log.InfoD(stepLog)
			for _, n := range nodesToReboot {
				if n.IsStorageDriverInstalled {
					stepLog = fmt.Sprintf("reboot node: %s", n.Name)
					Step(stepLog, func() {
						taskStep := fmt.Sprintf("reboot node: %s.", n.MgmtIp)
						event.Event.Type += "<br>" + taskStep
						err := Inst().N.RebootNode(n, node.RebootNodeOpts{
							Force: true,
							ConnectionOpts: node.ConnectionOpts{
								Timeout:         1 * time.Minute,
								TimeBeforeRetry: 5 * time.Second,
							},
						})
						if err != nil {
							log.Errorf("Error while rebooting node %v, err: %v", n.Name, err.Error())
						}
						UpdateOutcome(event, err)
					})
					stepLog = fmt.Sprintf("wait for node: %s to be back up", n.Name)
					Step(stepLog, func() {
						err := Inst().N.TestConnection(n, node.ConnectionOpts{
							Timeout:         15 * time.Minute,
							TimeBeforeRetry: 10 * time.Second,
						})
						if err != nil {
							log.Errorf("Error while testing node status %v, err: %v", n.Name, err.Error())
						}
						UpdateOutcome(event, err)
					})
					stepLog = fmt.Sprintf("wait for volume driver to stop on node: %v", n.Name)

					Step(stepLog, func() {
						err := Inst().V.WaitDriverDownOnNode(n)
						UpdateOutcome(event, err)
					})
					stepLog = fmt.Sprintf("wait to scheduler: %s and volume driver: %s to start",
						Inst().S.String(), Inst().V.String())
					Step(stepLog, func() {
						log.InfoD(stepLog)
						err := Inst().S.IsNodeReady(n)
						UpdateOutcome(event, err)

						err = Inst().V.WaitDriverUpOnNode(n, Inst().DriverStartTimeout)
						UpdateOutcome(event, err)
					})

					Step("validate apps", func() {
						for _, ctx := range *contexts {
							stepLog = fmt.Sprintf("RebootNode: validating app [%s]", ctx.App.Key)
							Step(stepLog, func() {
								errorChan := make(chan error, errorChannelSize)
								ValidateContext(ctx, &errorChan)
								for err := range errorChan {
									UpdateOutcome(event, err)
								}
								if strings.Contains(ctx.App.Key, fastpathAppName) {
									err := ValidateFastpathVolume(ctx, opsapi.FastpathStatus_FASTPATH_ACTIVE)
									UpdateOutcome(event, err)
								}
							})
						}
					})
				}
			}
			updateMetrics(*event)
		})
	})
}

// TriggerRebootManyNodes reboots one or more nodes on which apps are running
func TriggerRebootManyNodes(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {
	defer ginkgo.GinkgoRecover()
	defer endLongevityTest()
	startLongevityTest(RebootManyNodes)
	event := &EventRecord{
		Event: Event{
			ID:   GenerateUUID(),
			Type: RebootManyNodes,
		},
		Start:   time.Now().Format(time.RFC1123),
		Outcome: []error{},
	}

	defer func() {
		event.End = time.Now().Format(time.RFC1123)
		*recordChan <- event
	}()

	setMetrics(*event)
	stepLog := "get all nodes and reboot one by one"
	Step(stepLog, func() {
		log.InfoD(stepLog)
		nodesToReboot := getNodesByChaosLevel(RebootManyNodes)
		// Reboot node and check driver status
		stepLog = fmt.Sprintf("reboot the node(s): %v", nodesToReboot)
		Step(stepLog, func() {
			log.InfoD(stepLog)
			var wg sync.WaitGroup
			for _, n := range nodesToReboot {
				wg.Add(1)
				go func(n node.Node) {
					defer wg.Done()
					stepLog = fmt.Sprintf("reboot node: %s", n.Name)
					Step(stepLog, func() {
						log.InfoD(stepLog)
						taskStep := fmt.Sprintf("reboot node: %s.", n.MgmtIp)
						event.Event.Type += "<br>" + taskStep
						err := Inst().N.RebootNode(n, node.RebootNodeOpts{
							Force: true,
							ConnectionOpts: node.ConnectionOpts{
								Timeout:         1 * time.Minute,
								TimeBeforeRetry: 5 * time.Second,
							},
						})
						if err != nil {
							log.Errorf("Error while rebooting node %v, err: %v", n.Name, err.Error())
						}
						UpdateOutcome(event, err)
					})
				}(n)
			}
			stepLog = "wait all the nodes to be rebooted"
			Step(stepLog, func() {
				log.InfoD(stepLog)
				wg.Wait()
			})

			for _, n := range nodesToReboot {
				wg.Add(1)
				go func(n node.Node) {
					defer wg.Done()
					stepLog = fmt.Sprintf("wait for node: %s to be back up", n.Name)
					Step(stepLog, func() {
						log.InfoD(stepLog)
						err := Inst().N.TestConnection(n, node.ConnectionOpts{
							Timeout:         15 * time.Minute,
							TimeBeforeRetry: 10 * time.Second,
						})
						if err != nil {
							log.Errorf("Error while testing node status %v, err: %v", n.Name, err.Error())
						}
						UpdateOutcome(event, err)
					})

					stepLog = fmt.Sprintf("wait for volume driver to stop on node: %v", n.Name)
					Step(stepLog, func() {
						log.InfoD(stepLog)
						err := Inst().V.WaitDriverDownOnNode(n)
						UpdateOutcome(event, err)
					})
					stepLog = fmt.Sprintf("wait to scheduler: %s and volume driver: %s to start",
						Inst().S.String(), Inst().V.String())
					Step(stepLog, func() {
						log.InfoD(stepLog)
						err := Inst().S.IsNodeReady(n)
						UpdateOutcome(event, err)

						err = Inst().V.WaitDriverUpOnNode(n, Inst().DriverStartTimeout)
						UpdateOutcome(event, err)
					})
				}(n)
			}
			stepLog = "wait all the nodes to be up"
			Step(stepLog, func() {
				log.InfoD(stepLog)
				wg.Wait()
			})

			Step("validate apps", func() {
				for _, ctx := range *contexts {
					stepLog = fmt.Sprintf("RebootNode: validating app [%s]", ctx.App.Key)
					Step(stepLog, func() {
						log.InfoD(stepLog)
						errorChan := make(chan error, errorChannelSize)
						ValidateContext(ctx, &errorChan)
						for err := range errorChan {
							UpdateOutcome(event, err)
						}
						if strings.Contains(ctx.App.Key, fastpathAppName) {
							err := ValidateFastpathVolume(ctx, opsapi.FastpathStatus_FASTPATH_ACTIVE)
							UpdateOutcome(event, err)
						}
					})
				}
			})
		})
		updateMetrics(*event)
	})
}

func randIntn(n, maxNo int) []int {
	if n > maxNo {
		n = maxNo
	}
	rand.Seed(time.Now().UnixNano())
	generated := make(map[int]bool)
	for i := 0; i < n; i++ {
		randUniq := rand.Intn(maxNo)
		if !generated[randUniq] {
			generated[randUniq] = true
		}
	}
	generatedNs := make([]int, 0)
	for key := range generated {
		generatedNs = append(generatedNs, key)
	}
	return generatedNs
}

func getNodesByChaosLevel(triggerType string) []node.Node {
	t := ChaosMap[triggerType]
	stNodes := node.GetStorageNodes()
	stNodesLen := len(stNodes)
	nodes := make([]node.Node, 0)
	var nodeLen float32
	switch t {
	case 10:
		index := randIntn(1, stNodesLen)[0]
		return []node.Node{stNodes[index]}
	case 9:
		nodeLen = float32(stNodesLen) * 0.2
	case 8:
		nodeLen = float32(stNodesLen) * 0.3
	case 7:
		nodeLen = float32(stNodesLen) * 0.4
	case 6:
		nodeLen = float32(stNodesLen) * 0.5
	case 5:
		nodeLen = float32(stNodesLen) * 0.6
	case 4:
		nodeLen = float32(stNodesLen) * 0.7
	case 3:
		nodeLen = float32(stNodesLen) * 0.8
	case 2:
		nodeLen = float32(stNodesLen) * 0.9
	case 1:
		return stNodes
	}
	generatedNodeIndexes := randIntn(int(nodeLen), stNodesLen)
	for i := 0; i < len(generatedNodeIndexes); i++ {
		nodes = append(nodes, stNodes[generatedNodeIndexes[i]])
	}
	return nodes
}

// TriggerCrashNodes crashes Worker nodes
func TriggerCrashNodes(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {
	defer ginkgo.GinkgoRecover()
	defer endLongevityTest()
	startLongevityTest(CrashNode)
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
	stepLog := "get all nodes and crash one by one"
	Step(stepLog, func() {
		log.InfoD(stepLog)
		nodesToCrash := node.GetWorkerNodes()

		// Crash node and check driver status
		stepLog = fmt.Sprintf("crash node one at a time from the node(s): %v", nodesToCrash)
		Step(stepLog, func() {
			// TODO: Below is the same code from existing nodeCrash test
			log.InfoD(stepLog)
			for _, n := range nodesToCrash {
				if n.IsStorageDriverInstalled {
					stepLog = fmt.Sprintf("crash node: %s", n.Name)
					Step(stepLog, func() {
						log.InfoD(stepLog)
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
					stepLog = fmt.Sprintf("wait for node: %s to be back up", n.Name)
					Step(stepLog, func() {
						log.InfoD(stepLog)
						err := Inst().N.TestConnection(n, node.ConnectionOpts{
							Timeout:         15 * time.Minute,
							TimeBeforeRetry: 10 * time.Second,
						})
						UpdateOutcome(event, err)
					})
					stepLog = fmt.Sprintf("wait to scheduler: %s and volume driver: %s to start",
						Inst().S.String(), Inst().V.String())
					Step(stepLog, func() {
						log.InfoD(stepLog)
						err := Inst().S.IsNodeReady(n)
						UpdateOutcome(event, err)

						err = Inst().V.WaitDriverUpOnNode(n, Inst().DriverStartTimeout)
						UpdateOutcome(event, err)
					})

					Step("validate apps", func() {
						for _, ctx := range *contexts {
							stepLog = fmt.Sprintf("CrashNode: validating app [%s]", ctx.App.Key)
							Step(stepLog, func() {
								log.InfoD(stepLog)
								errorChan := make(chan error, errorChannelSize)
								ValidateContext(ctx, &errorChan)
								for err := range errorChan {
									UpdateOutcome(event, err)
								}
								if strings.Contains(ctx.App.Key, fastpathAppName) {
									err := ValidateFastpathVolume(ctx, opsapi.FastpathStatus_FASTPATH_ACTIVE)
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
	defer endLongevityTest()
	startLongevityTest(VolumeClone)
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

	setMetrics(*event)
	stepLog := "get volumes for all apps in test and clone them"

	Step(stepLog, func() {
		for _, ctx := range *contexts {
			var appVolumes []*volume.Volume
			var err error
			stepLog := fmt.Sprintf("get volumes for %s app", ctx.App.Key)
			Step(stepLog, func() {
				log.InfoD(stepLog)
				appVolumes, err = Inst().S.GetVolumes(ctx)
				UpdateOutcome(event, err)
				if len(appVolumes) == 0 {
					UpdateOutcome(event, fmt.Errorf("found no volumes for app %s", ctx.App.Key))
				}
			})
			for _, vol := range appVolumes {
				var clonedVolID string

				// Skip clone trigger for Pure FB volumes
				isPureFileVol, err := Inst().V.IsPureFileVolume(vol)
				if err != nil {
					log.Errorf("Checking pure file volume is for a volume %s failing with error: %v", vol.Name, err)
					UpdateOutcome(event, err)
				}

				if isPureFileVol {
					log.Warnf(
						"Clone is not supported for FB volumes: [%s]. "+
							"Skipping clone create for FB volume.", vol.Name,
					)
					continue
				}
				stepLog = fmt.Sprintf("Clone Volume %s", vol.Name)
				Step(stepLog, func() {
					log.InfoD(stepLog)
					log.Infof("Calling CloneVolume()...")
					clonedVolID, err = Inst().V.CloneVolume(vol.ID)
					UpdateOutcome(event, err)
				})
				stepLog = fmt.Sprintf("Validate successful clone %s", clonedVolID)
				Step(stepLog, func() {
					log.InfoD(stepLog)
					params := make(map[string]string)
					if Inst().ConfigMap != "" {
						params["auth-token"], err = Inst().S.GetTokenFromConfigMap(Inst().ConfigMap)
						UpdateOutcome(event, err)
					}
					err = Inst().V.ValidateCreateVolume(clonedVolID, params)
					UpdateOutcome(event, err)
				})
				stepLog = fmt.Sprintf("cleanup the cloned volume %s", clonedVolID)
				Step(stepLog, func() {
					log.InfoD(stepLog)
					err = Inst().V.DeleteVolume(clonedVolID)
					UpdateOutcome(event, err)
				})
			}
		}
		updateMetrics(*event)
	})
}

// TriggerVolumeResize increases all volumes size and validates app
func TriggerVolumeResize(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {
	defer ginkgo.GinkgoRecover()
	defer endLongevityTest()
	startLongevityTest(VolumeResize)
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

	Inst().M.IncrementGaugeMetric(TestRunningState, event.Event.Type)
	Inst().M.IncrementCounterMetric(TotalTriggerCount, event.Event.Type)
	stepLog := "get volumes for all apps in test and update size"
	Step(stepLog, func() {
		log.InfoD(stepLog)
		for _, ctx := range *contexts {
			var appVolumes []*volume.Volume
			var err error
			stepLog = fmt.Sprintf("get volumes for %s app", ctx.App.Key)
			Step(stepLog, func() {
				log.InfoD(stepLog)
				appVolumes, err = Inst().S.GetVolumes(ctx)
				log.Infof("len of app volumes is : %v", len(appVolumes))
				UpdateOutcome(event, err)
				if len(appVolumes) == 0 {
					UpdateOutcome(event, fmt.Errorf("found no volumes for app %s", ctx.App.Key))
				}
			})
			var requestedVols []*volume.Volume
			stepLog = fmt.Sprintf("increase volume size %s on app %s's volumes: %v",
				Inst().V.String(), ctx.App.Key, appVolumes)
			Step(stepLog,
				func() {
					log.InfoD(stepLog)
					log.InfoD("increasing volume size")
					requestedVols, err = Inst().S.ResizeVolume(ctx, Inst().ConfigMap)
					if err != nil && !(strings.Contains(err.Error(), "only dynamically provisioned pvc can be resized")) {
						UpdateOutcome(event, err)
					}
				})
			stepLog = fmt.Sprintf("validate successful volume size increase on app %s's volumes: %v",
				ctx.App.Key, appVolumes)
			Step(stepLog,
				func() {
					log.InfoD(stepLog)
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
		updateMetrics(*event)
	})
}

// TriggerLocalSnapShot takes local snapshots of the volumes and validates snapshot
func TriggerLocalSnapShot(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {
	defer ginkgo.GinkgoRecover()
	defer endLongevityTest()
	startLongevityTest(LocalSnapShot)
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

	setMetrics(*event)
	stepLog := "Create and Validate LocalSnapshots"
	Step(stepLog, func() {
		log.InfoD(stepLog)
		for _, ctx := range *contexts {
			var appVolumes []*volume.Volume
			var err error
			if strings.Contains(ctx.App.Key, "localsnap") {

				appNamespace := ctx.App.Key + "-" + ctx.UID
				log.Infof("Namespace : %v", appNamespace)
				stepLog = fmt.Sprintf("create schedule policy for %s app", ctx.App.Key)
				Step(stepLog, func() {
					log.InfoD(stepLog)
					policyName := "localintervalpolicy"
					schedPolicy, err := storkops.Instance().GetSchedulePolicy(policyName)
					if err != nil {
						retain := 2
						interval := getCloudSnapInterval(LocalSnapShot)
						log.InfoD("Creating a interval schedule policy %v with interval %v minutes", policyName, interval)
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
						log.Infof("Waiting for 10 mins for Snapshots to be completed")
						time.Sleep(10 * time.Minute)
					} else {
						log.Infof("schedPolicy is %v already exists", schedPolicy.Name)
					}

					UpdateOutcome(event, err)
				})
				log.Infof("Waiting for 2 mins for Snapshots to be completed")
				time.Sleep(2 * time.Minute)
				stepLog = fmt.Sprintf("get volumes for %s app", ctx.App.Key)
				Step(stepLog, func() {
					log.InfoD(stepLog)
					appVolumes, err = Inst().S.GetVolumes(ctx)
					UpdateOutcome(event, err)
					if len(appVolumes) == 0 {
						UpdateOutcome(event, fmt.Errorf("found no volumes for app %s", ctx.App.Key))
					}
				})
				log.Infof("Got volume count : %v", len(appVolumes))

				snapshotScheduleRetryInterval := 10 * time.Second
				snapshotScheduleRetryTimeout := 3 * time.Minute

				for _, v := range appVolumes {
					snapshotScheduleName := v.Name + "-interval-schedule"
					log.InfoD("snapshotScheduleName : %v for volume: %s", snapshotScheduleName, v.Name)
					snapStatuses, err := storkops.Instance().ValidateSnapshotSchedule(snapshotScheduleName,
						appNamespace,
						snapshotScheduleRetryTimeout,
						snapshotScheduleRetryInterval)
					if err == nil {
						for k, v := range snapStatuses {
							log.InfoD("Policy Type: %v", k)
							for _, e := range v {
								log.InfoD("ScheduledVolumeSnapShot Name: %v", e.Name)
								log.InfoD("ScheduledVolumeSnapShot status: %v", e.Status)
								snapData, err := Inst().S.GetSnapShotData(ctx, e.Name, appNamespace)
								UpdateOutcome(event, err)

								snapType := snapData.Spec.PortworxSnapshot.SnapshotType
								log.InfoD("Snapshot Type: %v", snapType)
								if snapType != "local" {
									err = &scheduler.ErrFailedToGetVolumeParameters{
										App:   ctx.App,
										Cause: fmt.Sprintf("Snapshot Type: %s does not match", snapType),
									}
									UpdateOutcome(event, err)

								}

								snapID := snapData.Spec.PortworxSnapshot.SnapshotID
								log.InfoD("Snapshot ID: %v", snapID)

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
						log.InfoD("Got error while getting volume snapshot status :%v", err.Error())
					}
					UpdateOutcome(event, err)
				}

			}

		}
		updateMetrics(*event)
	})

}

// TriggerDeleteLocalSnapShot deletes local snapshots and snapshot schedules
func TriggerDeleteLocalSnapShot(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {
	defer ginkgo.GinkgoRecover()
	defer endLongevityTest()
	startLongevityTest(DeleteLocalSnapShot)
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

	setMetrics(*event)
	stepLog := "Delete Schedule Policy and LocalSnapshots and Validate"
	Step(stepLog, func() {
		log.InfoD(stepLog)
		for _, ctx := range *contexts {
			var appVolumes []*volume.Volume

			if strings.Contains(ctx.App.Key, "localsnap") {

				appNamespace := ctx.App.Key + "-" + ctx.UID
				log.Infof("Namespace : %v", appNamespace)
				stepLog = fmt.Sprintf("delete schedule policy for %s app", ctx.App.Key)
				Step(stepLog, func() {
					log.InfoD(stepLog)
					policyName := "localintervalpolicy"
					err := storkops.Instance().DeleteSchedulePolicy(policyName)
					UpdateOutcome(event, err)
					stepLog = fmt.Sprintf("get volumes for %s app", ctx.App.Key)
					Step(stepLog, func() {
						log.InfoD(stepLog)
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
						log.InfoD("snapshotScheduleName : %v for volume: %s", snapshotScheduleName, v.Name)
						snapStatuses, err := storkops.Instance().ValidateSnapshotSchedule(snapshotScheduleName,
							appNamespace,
							snapshotScheduleRetryTimeout,
							snapshotScheduleRetryInterval)

						if err == nil {
							for _, v := range snapStatuses {
								for _, e := range v {
									log.InfoD("Deleting ScheduledVolumeSnapShot: %v", e.Name)
									err = Inst().S.DeleteSnapShot(ctx, e.Name, appNamespace)
									UpdateOutcome(event, err)

								}
							}
							err = storkops.Instance().DeleteSnapshotSchedule(snapshotScheduleName, appNamespace)
							UpdateOutcome(event, err)

						} else {
							log.InfoD("Got error while getting volume snapshot status :%v", err.Error())
						}
					}

					snapshotList, err := Inst().S.GetSnapshotsInNameSpace(ctx, appNamespace)
					UpdateOutcome(event, err)
					if len(snapshotList.Items) != 0 {
						log.InfoD("Failed to delete snapshots in namespace %v", appNamespace)
						err = &scheduler.ErrFailedToDeleteTasks{
							App:   ctx.App,
							Cause: fmt.Sprintf("Failed to delete snapshots in namespace %v", appNamespace),
						}
						UpdateOutcome(event, err)

					}

				})
			}
		}
		updateMetrics(*event)
	})

}

// TriggerCloudSnapShot deploy Interval Policy and validates snapshot
func TriggerCloudSnapShot(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {
	defer ginkgo.GinkgoRecover()
	defer endLongevityTest()
	startLongevityTest(CloudSnapShot)
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

	setMetrics(*event)

	snapshotScheduleRetryInterval := 10 * time.Second
	snapshotScheduleRetryTimeout := 3 * time.Minute
	stepLog := "Validate Cloud Snaps"
	Step(stepLog, func() {
		log.InfoD(stepLog)
		for _, ctx := range *contexts {
			var appVolumes []*volume.Volume
			var err error
			if strings.Contains(ctx.App.Key, "cloudsnap") {

				appNamespace := ctx.App.Key + "-" + ctx.UID
				log.Infof("Namespace : %v", appNamespace)
				stepLog = fmt.Sprintf("create schedule policy for %s app", ctx.App.Key)
				Step(stepLog, func() {
					log.InfoD(stepLog)
					policyName := "intervalpolicy"
					schedPolicy, err := storkops.Instance().GetSchedulePolicy(policyName)
					if err != nil {
						retain := 2
						interval := getCloudSnapInterval(CloudSnapShot)
						log.InfoD("Creating a interval schedule policy %v with interval %v minutes", policyName, interval)
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
						log.Infof("Waiting for 10 mins for Snapshots to be completed")
						time.Sleep(10 * time.Minute)
					} else {
						log.Infof("schedPolicy is %v already exists", schedPolicy.Name)
					}

					UpdateOutcome(event, err)
				})
				stepLog = fmt.Sprintf("get volumes for %s app", ctx.App.Key)

				Step(stepLog, func() {
					log.InfoD(stepLog)
					appVolumes, err = Inst().S.GetVolumes(ctx)
					UpdateOutcome(event, err)
					if len(appVolumes) == 0 {
						UpdateOutcome(event, fmt.Errorf("found no volumes for app %s", ctx.App.Key))
					}
				})
				log.Infof("Got volume count : %v", len(appVolumes))

				for _, v := range appVolumes {
					// Skip cloud snapshot trigger for Pure DA volumes
					isPureVol, err := Inst().V.IsPureVolume(v)
					if err != nil {
						UpdateOutcome(event, err)
					}
					if isPureVol {
						log.Warnf(
							"Cloud snapshot is not supported for Pure DA volumes: [%s],Skipping cloud snapshot trigger for pure volume.", v.Name,
						)
						continue
					}
					snapshotScheduleName := v.Name + "-interval-schedule"
					log.InfoD("snapshotScheduleName : %v for volume: %s", snapshotScheduleName, v.Name)
					snapStatuses, err := storkops.Instance().ValidateSnapshotSchedule(snapshotScheduleName,
						appNamespace,
						snapshotScheduleRetryTimeout,
						snapshotScheduleRetryInterval)
					if err == nil {
						for k, v := range snapStatuses {
							log.InfoD("Policy Type: %v", k)
							for _, e := range v {
								log.InfoD("ScheduledVolumeSnapShot Name: %v", e.Name)
								log.InfoD("ScheduledVolumeSnapShot status: %v", e.Status)
							}
						}
					} else {
						log.InfoD("Got error while getting volume snapshot status :%v", err.Error())
					}
					UpdateOutcome(event, err)
				}

			}

		}
		updateMetrics(*event)
	})

}

// TriggerVolumeDelete delete the volumes
func TriggerVolumeDelete(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {
	defer ginkgo.GinkgoRecover()
	defer endLongevityTest()
	startLongevityTest(VolumesDelete)
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

	setMetrics(*event)
	stepLog := "Validate Delete Volumes"
	Step(stepLog, func() {
		log.InfoD(stepLog)
		opts := make(map[string]bool)

		for _, ctx := range *contexts {

			opts[SkipClusterScopedObjects] = true
			options := mapToVolumeOptions(opts)

			// Tear down storage objects
			vols := DeleteVolumes(ctx, options)

			// Tear down application
			stepLog = fmt.Sprintf("start destroying %s app", ctx.App.Key)
			Step(stepLog, func() {
				log.InfoD(stepLog)
				err := Inst().S.Destroy(ctx, opts)
				UpdateOutcome(event, err)
			})

			ValidateVolumesDeleted(ctx.App.Key, vols)
		}
		*contexts = nil
		TriggerDeployNewApps(contexts, recordChan)
		updateMetrics(*event)
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
	log.Infof("Provider for credentail secret is %s", provider)
	if !ok {
		return nil, &errors.ErrNotFound{
			ID:   "OBJECT_STORE_PROVIDER",
			Type: "Environment Variable",
		}
	}
	log.Infof("Provider for credentail secret is %s", provider)

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
		log.Errorf("provider needs to be either aws, azure or google")
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
func TriggerEmailReporter() {
	// emailRecords stores events to be notified

	emailData := emailData{}
	log.Infof("Generating email report: %s", time.Now().Format(time.RFC1123))

	var masterNodeList []string
	var pxStatus string

	for _, n := range node.GetMasterNodes() {
		masterNodeList = append(masterNodeList, n.Addresses...)
	}
	emailData.MasterIP = masterNodeList

	for _, n := range node.GetWorkerNodes() {
		k8sNode, err := core.Instance().GetNodeByName(n.Name)
		k8sNodeStatus := "False"

		if err != nil {
			log.Errorf("Unable to get K8s node , Err : %v", err)
		} else {
			for _, condition := range k8sNode.Status.Conditions {
				if condition.Type == v1.NodeReady {
					if condition.Status == v1.ConditionTrue && !k8sNode.Spec.Unschedulable {
						k8sNodeStatus = "True"
						log.Infof("Node %v has Node Status True", n.Name)
					} else {
						log.Errorf("Node %v has Node Status False", n.Name)

					}
				}
			}

		}
		if n.StorageNode != nil {
			log.Infof("Getting node %s status", n.Name)
			status, err := Inst().V.GetNodeStatus(n)
			if err != nil {
				pxStatus = pxStatusError
			} else {
				pxStatus = status.String()
			}

			pxVersion, err := Inst().V.GetDriverVersionOnNode(n)
			if err != nil {
				pxVersion = pxVersionError
			}

			emailData.NodeInfo = append(emailData.NodeInfo, nodeInfo{MgmtIP: n.MgmtIp, NodeName: n.Name,
				PxVersion: pxVersion, Status: pxStatus, NodeStatus: k8sNodeStatus, Cores: coresMap[n.Name]})
		} else {
			log.Infof("node %s storage is nil, %+v", n.Name, n)
		}
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
		log.Errorf("Failed to prepare email body. Error: [%v]", err)
	}

	emailDetails := &email.Email{
		Subject:        subject,
		Content:        content,
		From:           from,
		To:             EmailRecipients,
		SendGridAPIKey: SendGridEmailAPIKey,
	}

	if err := emailDetails.SendEmail(); err != nil {
		log.Errorf("Failed to send out email, Err: %q", err)
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

	setMetrics(*event)

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
				SourceClusterName, namespace, backupName), func() {
				err = CreateBackupGetErr(backupName,
					SourceClusterName, backupLocationName, BackupLocationUID,
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
				log.Warnf("Skipping waiting for backup %s because %s", backupName, err)
				continue
			}
			Step(fmt.Sprintf("Wait for backup %s to complete", backupName), func() {
				ctx, err := backup.GetPxCentralAdminCtx()
				if err != nil {
					log.Errorf("Failed to fetch px-central-admin ctx: [%v]", err)
					bkpNamespaceErrors[namespace] = err
					UpdateOutcome(event, err)
				} else {
					err = Inst().Backup.WaitForBackupCompletion(
						ctx,
						backupName, OrgID,
						BackupRestoreCompletionTimeoutMin*time.Minute,
						RetrySeconds*time.Second)
					if err == nil {
						log.Infof("Backup [%s] completed successfully", backupName)
					} else {
						log.Errorf("Failed to wait for backup [%s] to complete. Error: [%v]",
							backupName, err)
						bkpNamespaceErrors[namespace] = err
						UpdateOutcome(event, err)
					}
				}
			})
		}
		updateMetrics(*event)
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

	setMetrics(*event)

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

	log.Infof("Verify namespaces")
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
	updateMetrics(*event)
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

	setMetrics(*event)

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
			log.Infof("Create backup full name %s:%s:%s", SourceClusterName, namespace, backupName)
			backupCreateRequest := GetBackupCreateRequest(backupName, SourceClusterName, backupLocationName, BackupLocationUID,
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
			log.Warnf("Skipping waiting for backup [%s] because [%s]", backupName, err)
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
					log.Infof("Backup [%s] completed successfully", backupName)
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
				log.Warnf("Skipping inspecting backup [%s] because [%s]", backupName, err)
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
		updateMetrics(*event)
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

	setMetrics(*event)

	log.Infof("Enumerating backups")
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
		Uid:   backupToInspect.GetUid(),
	}
	_, err = Inst().Backup.InspectBackup(ctx, backupInspectRequest)
	desc := fmt.Sprintf("InspectBackup failed: Inspect backup %s failed", backupToInspect.GetName())
	ProcessErrorWithMessage(event, err, desc)
	updateMetrics(*event)

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

	setMetrics(*event)

	log.Infof("Enumerating restores")
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
	updateMetrics(*event)
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

	setMetrics(*event)

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

	log.Infof("Enumerating backups")
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
		BackupRef: &api.ObjectRef{
			Name: backupToRestore.GetName(),
			Uid:  backupToRestore.GetUid(),
		},
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
	updateMetrics(*event)
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

	setMetrics(*event)

	log.Infof("Enumerating backups")
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
	err = DeleteBackupAndDependencies(backupToDelete.GetName(), backupToDelete.GetUid(), OrgID, backupToDelete.GetCluster())
	desc := fmt.Sprintf("DeleteBackup failed: Delete backup %s on cluster %s failed",
		backupToDelete.GetName(), backupToDelete.GetCluster())
	ProcessErrorWithMessage(event, err, desc)
	updateMetrics(*event)

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

	setMetrics(*event)

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
			backupCreateRequest := GetBackupCreateRequest(backupName, SourceClusterName, backupLocationName, BackupLocationUID,
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
				log.Infof("Backup [%s] completed successfully", backupName)
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
		updateMetrics(*event)
	})
}

// TriggerBackupByLabel gives a label to random resources on the cluster and tries to back up only resources with that label
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

	setMetrics(*event)

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
		backupCreateRequest := GetBackupCreateRequest(backupName, SourceClusterName, backupLocationName, BackupLocationUID,
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
				log.Infof("Backup [%s] completed successfully", backupName)
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
		updateMetrics(*event)
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

	setMetrics(*event)

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

	log.Infof("Verify newest pods have been scaled in backup")
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

	log.Infof("Verify pods have been scaled in backup")
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
	updateMetrics(*event)
}

// TriggerBackupRestartPX backs up an application and restarts Portworx during the backup
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

	setMetrics(*event)

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
			SourceClusterName, bkpNamespaces[nsIndex], backupName), func() {
			err = CreateBackupGetErr(backupName,
				SourceClusterName, backupLocationName, BackupLocationUID,
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
		log.Infof("Stop volume driver [%s] on node: [%s]", Inst().V.String(), nodes[nodeIndex].Name)
		StopVolDriverAndWait([]node.Node{nodes[nodeIndex]})
		log.Infof("Starting volume driver [%s] on node [%s]", Inst().V.String(), nodes[nodeIndex].Name)
		StartVolDriverAndWait([]node.Node{nodes[nodeIndex]})
		log.Infof("Giving a few seconds for volume driver to stabilize")
		time.Sleep(20 * time.Second)
	})

	Step("Wait for backup to complete", func() {
		if bkpError {
			log.Warnf("Skipping waiting for backup [%s] due to error", backupName)
		} else {
			ctx, err := backup.GetPxCentralAdminCtx()
			if err != nil {
				log.Errorf("Failed to fetch px-central-admin ctx: [%v]", err)
				UpdateOutcome(event, err)
			} else {
				err = Inst().Backup.WaitForBackupCompletion(
					ctx,
					backupName, OrgID,
					BackupRestoreCompletionTimeoutMin*time.Minute,
					RetrySeconds*time.Second)
				if err == nil {
					log.Infof("Backup [%s] completed successfully", backupName)
				} else {
					log.Errorf("Failed to wait for backup [%s] to complete. Error: [%v]",
						backupName, err)
					UpdateOutcome(event, err)
				}
			}
		}
		updateMetrics(*event)
	})
}

// TriggerBackupRestartNode backs up an application and restarts a node with Portworx during the backup
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

	setMetrics(*event)

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
			SourceClusterName, bkpNamespaces[nsIndex], backupName), func() {
			err = CreateBackupGetErr(backupName,
				SourceClusterName, backupLocationName, BackupLocationUID,
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
					log.Errorf("Failed to get apps on node [%s]", nodes[nodeIndex].Name)
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
			log.Warnf("Skipping waiting for backup [%s] due to error", backupName)
		} else {
			ctx, err := backup.GetPxCentralAdminCtx()
			if err != nil {
				log.Errorf("Failed to fetch px-central-admin ctx: [%v]", err)
				UpdateOutcome(event, err)
			} else {
				err = Inst().Backup.WaitForBackupCompletion(
					ctx,
					backupName, OrgID,
					BackupRestoreCompletionTimeoutMin*time.Minute,
					RetrySeconds*time.Second)
				if err == nil {
					log.Infof("Backup [%s] completed successfully", backupName)
				} else {
					log.Errorf("Failed to wait for backup [%s] to complete. Error: [%v]",
						backupName, err)
					UpdateOutcome(event, err)
				}
			}
		}
		updateMetrics(*event)
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

	setMetrics(*event)

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
			SourceClusterName, "all", backupName), func() {
			err = CreateBackupGetErr(backupName,
				SourceClusterName, backupLocationName, BackupLocationUID,
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
			log.Errorf("Failed to fetch px-central-admin ctx: [%v]", err)
			UpdateOutcome(event, err)
		} else {
			err = Inst().Backup.WaitForBackupCompletion(
				ctx,
				backupName, OrgID,
				BackupRestoreCompletionTimeoutMin*time.Minute,
				RetrySeconds*time.Second)
			if err == nil {
				log.Infof("Backup [%s] completed successfully", backupName)
			} else {
				log.Errorf("Failed to wait for backup [%s] to complete. Error: [%v]",
					backupName, err)
				UpdateOutcome(event, err)
			}
		}
		updateMetrics(*event)
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

	setMetrics(*event)

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
			SourceClusterName, "all", backupName), func() {
			err = CreateBackupGetErr(backupName,
				SourceClusterName, backupLocationName, BackupLocationUID,
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
			log.Errorf("Failed to fetch px-central-admin ctx: [%v]", err)
			UpdateOutcome(event, err)
		} else {
			err = Inst().Backup.WaitForBackupCompletion(
				ctx,
				backupName, OrgID,
				BackupRestoreCompletionTimeoutMin*time.Minute,
				RetrySeconds*time.Second)
			if err == nil {
				log.Infof("Backup [%s] completed successfully", backupName)
			} else {
				log.Errorf("Failed to wait for backup [%s] to complete. Error: [%v]",
					backupName, err)
				UpdateOutcome(event, err)
			}
		}
		updateMetrics(*event)
	})
}

func isPoolResizePossible(poolToBeResized *opsapi.StoragePool) (bool, error) {
	if poolToBeResized == nil {
		return false, fmt.Errorf("pool provided is nil")
	}

	if poolToBeResized != nil && poolToBeResized.LastOperation != nil {
		log.InfoD("Validating pool :%v to expand", poolToBeResized.Uuid)

		f := func() (interface{}, bool, error) {

			pools, err := Inst().V.ListStoragePools(meta_v1.LabelSelector{})
			if err != nil {
				return nil, true, fmt.Errorf("error getting pools list, Error :%v", err)
			}

			updatedPoolToBeResized := pools[poolToBeResized.Uuid]
			if updatedPoolToBeResized.LastOperation.Status != opsapi.SdkStoragePool_OPERATION_SUCCESSFUL {
				if updatedPoolToBeResized.LastOperation.Status == opsapi.SdkStoragePool_OPERATION_FAILED {
					return nil, false, fmt.Errorf("PoolResize has failed. Error: %s", updatedPoolToBeResized.LastOperation)

				}
				err = ValidatePoolRebalance()
				if err != nil {
					return nil, true, err
				}

				log.InfoD("Pool Resize is already in progress: %v", updatedPoolToBeResized.LastOperation)
				if strings.Contains(updatedPoolToBeResized.LastOperation.Msg, "Will not proceed with pool expansion") {
					return nil, false, fmt.Errorf("PoolResize has failed. Error: %s", updatedPoolToBeResized.LastOperation.Msg)
				}
				return nil, true, nil
			}
			return nil, false, nil
		}
		_, err := task.DoRetryWithTimeout(f, 10*time.Minute, 1*time.Minute)
		if err != nil {
			return false, err
		}

	}
	return true, nil
}

func waitForPoolToBeResized(initialSize uint64, poolIDToResize string) error {

	f := func() (interface{}, bool, error) {
		pools, err := Inst().V.ListStoragePools(meta_v1.LabelSelector{})
		if err != nil {
			return nil, false, fmt.Errorf("error getting pools list, Error :%v", err)
		}

		expandedPool := pools[poolIDToResize]
		if expandedPool.LastOperation != nil {
			log.InfoD("Current pool %s last operation status : %v", poolIDToResize, expandedPool.LastOperation.Status)
			if expandedPool.LastOperation.Status == opsapi.SdkStoragePool_OPERATION_FAILED {
				return nil, false, fmt.Errorf("PoolResize for %s has failed. Error: %s", poolIDToResize, expandedPool.LastOperation)
			}
		}

		newPoolSize := expandedPool.TotalSize / units.GiB
		err = ValidatePoolRebalance()
		if err != nil {
			return nil, true, fmt.Errorf("pool %s not been resized .Current size is %d,Error while pool rebalance: %v", poolIDToResize, newPoolSize, err)
		}

		if newPoolSize > initialSize {
			// storage pool resize has been completed
			return nil, true, nil
		}

		return nil, true, fmt.Errorf("pool %s not been resized .Current size is %d", poolIDToResize, newPoolSize)
	}

	_, err := task.DoRetryWithTimeout(f, 10*time.Minute, 1*time.Minute)
	return err
}

func getStoragePoolsToExpand() ([]*opsapi.StoragePool, error) {
	pools, err := Inst().V.ListStoragePools(meta_v1.LabelSelector{})
	if err != nil {
		err = fmt.Errorf("error getting storage pools list. Err: %v", err)
		return nil, err

	}

	if len(pools) == 0 {
		err = fmt.Errorf("length of pools should be greater than 0")
		return nil, err
	}

	// pick a random pools from a pools list and resize it
	expectedCapacity := (len(pools) / 2) + 1
	poolsToExpand := make([]*opsapi.StoragePool, 0)
	for _, pool := range pools {
		if len(poolsToExpand) <= expectedCapacity {
			poolsToExpand = append(poolsToExpand, pool)
		} else {
			break
		}

	}
	return poolsToExpand, nil

}

func initiatePoolExpansion(event *EventRecord, wg *sync.WaitGroup, pool *opsapi.StoragePool, chaosLevel uint64, resizeOperationType opsapi.SdkStoragePool_ResizeOperationType, doNodeReboot bool) {

	if wg != nil {
		defer wg.Done()
	}
	poolValidity, err := isPoolResizePossible(pool)
	if err != nil {
		log.Error(err.Error())
		UpdateOutcome(event, err)
	}

	expansionType := "resize-disk"

	if resizeOperationType == 1 {
		expansionType = "add-disk"
	}

	if poolValidity {
		initialPoolSize := pool.TotalSize / units.GiB

		err = Inst().V.ResizeStoragePoolByPercentage(pool.Uuid, resizeOperationType, uint64(chaosLevel))
		if err != nil {
			err = fmt.Errorf("error initiating pool [%v ] %v: [%v]", pool.Uuid, expansionType, err.Error())
			log.Error(err.Error())
			UpdateOutcome(event, err)
		} else {
			if doNodeReboot {
				err = WaitForExpansionToStart(pool.Uuid)
				log.Error(err.Error())
				UpdateOutcome(event, err)
				if err == nil {
					storageNode, err := GetNodeWithGivenPoolID(pool.Uuid)
					log.Error(err.Error())
					UpdateOutcome(event, err)
					err = RebootNodeAndWait(*storageNode)
					log.Error(err.Error())
					UpdateOutcome(event, err)
				}

			}
			err = waitForPoolToBeResized(initialPoolSize, pool.Uuid)
			if err != nil {
				err = fmt.Errorf("pool [%v] %v failed. Error: %v", pool.Uuid, expansionType, err)
				log.Error(err.Error())
				UpdateOutcome(event, err)
			}
		}
	}
}

// TriggerPoolResizeDisk peforms resize-disk on the storage pools for the given contexts
func TriggerPoolResizeDisk(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {
	defer ginkgo.GinkgoRecover()
	defer endLongevityTest()
	startLongevityTest(PoolResizeDisk)
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

	setMetrics(*event)

	chaosLevel := getPoolExpandPercentage(PoolResizeDisk)
	stepLog := fmt.Sprintf("get storage pools and perform resize-disk by %v percentage on it ", chaosLevel)
	Step(stepLog, func() {

		poolsToBeResized, err := getStoragePoolsToExpand()

		if err != nil {
			log.Error(err.Error())
			UpdateOutcome(event, err)
		}
		log.InfoD("Pools to resize-disk [%v]", poolsToBeResized)
		var wg sync.WaitGroup

		for _, pool := range poolsToBeResized {
			//Skipping pool resize if pool rebalance is enabled for the pool
			if !isPoolRebalanceEnabled(pool.Uuid) {
				//Initiating multiple pool expansions by resize-disk
				go initiatePoolExpansion(event, &wg, pool, chaosLevel, 2, false)
				wg.Add(1)
			}

		}
		wg.Wait()

	})

	stepLog = "validate all apps after pool resize using resize-disk operation"
	Step(stepLog, func() {
		log.InfoD(stepLog)
		errorChan := make(chan error, errorChannelSize)
		for _, ctx := range *contexts {
			ValidateContext(ctx, &errorChan)
			if strings.Contains(ctx.App.Key, fastpathAppName) {
				err := ValidateFastpathVolume(ctx, opsapi.FastpathStatus_FASTPATH_ACTIVE)
				UpdateOutcome(event, err)
			}
		}
		updateMetrics(*event)
	})
}

// TriggerPoolResizeDiskAndReboot performs resize-disk on a storage pool and reboots the node
func TriggerPoolResizeDiskAndReboot(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {
	defer ginkgo.GinkgoRecover()
	defer endLongevityTest()
	startLongevityTest(ResizeDiskAndReboot)
	event := &EventRecord{
		Event: Event{
			ID:   GenerateUUID(),
			Type: ResizeDiskAndReboot,
		},
		Start:   time.Now().Format(time.RFC1123),
		Outcome: []error{},
	}

	defer func() {
		event.End = time.Now().Format(time.RFC1123)
		*recordChan <- event
	}()

	setMetrics(*event)

	chaosLevel := getPoolExpandPercentage(ResizeDiskAndReboot)

	stepLog := fmt.Sprintf("get storage pools and perform resize-disk by %v percentage on it ", chaosLevel)
	Step(stepLog, func() {
		log.InfoD(stepLog)
		poolsToBeResized, err := getStoragePoolsToExpand()

		if err != nil {
			log.Error(err.Error())
			UpdateOutcome(event, err)
		}
		var poolToBeResized *opsapi.StoragePool
		if len(poolsToBeResized) > 0 {
			for _, pool := range poolsToBeResized {
				if !isPoolRebalanceEnabled(pool.Uuid) {
					poolToBeResized = pool
				}
			}
			log.InfoD("Pool to resize-disk [%v]", poolToBeResized)
			initiatePoolExpansion(event, nil, poolToBeResized, chaosLevel, 2, true)
		}
	})

	stepLog = "validate all apps after pool resize using resize-disk operation"
	Step(stepLog, func() {
		log.InfoD(stepLog)
		errorChan := make(chan error, errorChannelSize)
		for _, ctx := range *contexts {
			ValidateContext(ctx, &errorChan)
			if strings.Contains(ctx.App.Key, fastpathAppName) {
				err := ValidateFastpathVolume(ctx, opsapi.FastpathStatus_FASTPATH_ACTIVE)
				UpdateOutcome(event, err)
			}
		}
	})
	updateMetrics(*event)
}

// TriggerPoolAddDisk peforms add-disk on the storage pools for the given contexts
func TriggerPoolAddDisk(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {
	defer ginkgo.GinkgoRecover()
	defer endLongevityTest()
	startLongevityTest(PoolAddDisk)
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

	setMetrics(*event)

	chaosLevel := getPoolExpandPercentage(PoolAddDisk)
	stepLog := fmt.Sprintf("get storage pools and perform add-disk by %v percentage on it ", chaosLevel)
	Step(stepLog, func() {
		log.InfoD(stepLog)
		poolsToBeResized, err := getStoragePoolsToExpand()

		if err != nil {
			log.Error(err.Error())
			UpdateOutcome(event, err)
		}
		log.InfoD("Pools to add-disk [%v]", poolsToBeResized)

		var wg sync.WaitGroup

		for _, pool := range poolsToBeResized {
			//Skipping pool resize if pool rebalance is enabled for the pool
			if !isPoolRebalanceEnabled(pool.Uuid) {
				//Initiating multiple pool expansions by add-disk
				go initiatePoolExpansion(event, &wg, pool, chaosLevel, 1, false)
				wg.Add(1)

			}
		}
		wg.Wait()

	})
	stepLog = "validate all apps after pool resize using add-disk operation"
	Step(stepLog, func() {
		log.InfoD(stepLog)
		errorChan := make(chan error, errorChannelSize)
		for _, ctx := range *contexts {
			ValidateContext(ctx, &errorChan)
			if strings.Contains(ctx.App.Key, fastpathAppName) {
				err := ValidateFastpathVolume(ctx, opsapi.FastpathStatus_FASTPATH_ACTIVE)
				UpdateOutcome(event, err)
			}
		}
	})
	updateMetrics(*event)
}

// TriggerPoolAddDiskAndReboot performs add-disk and reboots the node
func TriggerPoolAddDiskAndReboot(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {
	defer ginkgo.GinkgoRecover()
	defer endLongevityTest()
	startLongevityTest(AddDiskAndReboot)
	event := &EventRecord{
		Event: Event{
			ID:   GenerateUUID(),
			Type: AddDiskAndReboot,
		},
		Start:   time.Now().Format(time.RFC1123),
		Outcome: []error{},
	}

	defer func() {
		event.End = time.Now().Format(time.RFC1123)
		*recordChan <- event
	}()

	setMetrics(*event)

	chaosLevel := getPoolExpandPercentage(AddDiskAndReboot)
	stepLog := fmt.Sprintf("get storage pools and perform add-disk by %v percentage on it ", chaosLevel)
	Step(stepLog, func() {
		log.InfoD(stepLog)
		poolsToBeResized, err := getStoragePoolsToExpand()

		if err != nil {
			log.Error(err.Error())
			UpdateOutcome(event, err)
		}
		var poolToBeResized *opsapi.StoragePool
		if len(poolsToBeResized) > 0 {
			for _, pool := range poolsToBeResized {
				if !isPoolRebalanceEnabled(pool.Uuid) {
					poolToBeResized = pool
				}
			}
			log.InfoD("Pool to resize-disk [%v]", poolToBeResized)
			initiatePoolExpansion(event, nil, poolToBeResized, chaosLevel, 1, true)
		}
	})
	stepLog = "validate all apps after pool resize using add-disk operation"
	Step(stepLog, func() {
		log.InfoD(stepLog)
		errorChan := make(chan error, errorChannelSize)
		for _, ctx := range *contexts {
			ValidateContext(ctx, &errorChan)
			if strings.Contains(ctx.App.Key, fastpathAppName) {
				err := ValidateFastpathVolume(ctx, opsapi.FastpathStatus_FASTPATH_ACTIVE)
				UpdateOutcome(event, err)
			}
		}
	})

	updateMetrics(*event)
}

func TriggerAutopilotPoolRebalance(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {
	defer ginkgo.GinkgoRecover()
	defer endLongevityTest()
	startLongevityTest(AutopilotRebalance)
	event := &EventRecord{
		Event: Event{
			ID:   GenerateUUID(),
			Type: AutopilotRebalance,
		},
		Start:   time.Now().Format(time.RFC1123),
		Outcome: []error{},
	}

	defer func() {
		event.End = time.Now().Format(time.RFC1123)
		*recordChan <- event
	}()
	poolLabel := map[string]string{"autopilot": "rebalance"}
	apRule := aututils.PoolRuleRebalanceAbsolute(120, 70, false)
	apRule.Spec.ActionsCoolDownPeriod = int64(getReblanceCoolOffPeriod(AutopilotRebalance))
	apRule.Spec.Selector = apapi.RuleObjectSelector{
		LabelSelector: meta_v1.LabelSelector{
			MatchLabels: poolLabel,
		},
	}

	if autoPilotLabelNode.Name == "" {

		Step("Create autopilot rule", func() {
			log.InfoD("Creating autopilot rule ; %+v", apRule)

			storageNodes := node.GetStorageDriverNodes()
			maxUsed := uint64(0)
			for _, sNode := range storageNodes {
				totalSize := uint64(0)
				for _, p := range sNode.StoragePools {
					totalSize += p.StoragePool.Used
				}
				if totalSize > maxUsed {
					autoPilotLabelNode = sNode
					maxUsed = totalSize
				}
			}

			log.InfoD("Adding label %s to the node %s", poolLabel, autoPilotLabelNode.Name)
			err := AddLabelsOnNode(autoPilotLabelNode, poolLabel)
			UpdateOutcome(event, err)
			if err == nil {
				_, err = Inst().S.CreateAutopilotRule(apRule)
				UpdateOutcome(event, err)
				//Removing the label if autopilot rule creation is failed
				if err != nil {
					for k := range poolLabel {
						Inst().S.RemoveLabelOnNode(autoPilotLabelNode, k)
						autoPilotLabelNode = node.Node{}
					}
				}
			} else {
				log.Warn("Skipping autopilot rule creation as node is not labelled")
			}

		})
	} else {

		Step("validate the  autopilot events", func() {
			log.InfoD("validate the  autopilot events for %s", apRule.Name)
			ruleEvents, err := core.Instance().ListEvents("", meta_v1.ListOptions{
				FieldSelector: fmt.Sprintf("involvedObject.kind=AutopilotRule,involvedObject.name=%s", apRule.Name),
			})
			UpdateOutcome(event, err)

			if err == nil {
				for _, ruleEvent := range ruleEvents.Items {
					//checking the events triggered in last 60 mins
					if ruleEvent.LastTimestamp.Unix() < meta_v1.Now().Unix()-3600 {
						continue
					}
					log.InfoD("autopilot rule event reason : %s and message: %s ", ruleEvent.Reason, ruleEvent.Message)
					if strings.Contains(ruleEvent.Reason, "FailedAction") || strings.Contains(ruleEvent.Reason, "RuleCheckFailed") {
						UpdateOutcome(event, fmt.Errorf("autopilot rule %s failed, Reason: %s, Message; %s", apRule.Name, ruleEvent.Reason, ruleEvent.Message))
					}

				}

			}

		})

		Step("validate Px on the rebalanced node", func() {
			log.InfoD("Validating PX on node : %s", autoPilotLabelNode.Name)
			err := Inst().V.WaitDriverUpOnNode(autoPilotLabelNode, 1*time.Minute)
			UpdateOutcome(event, err)

			err = ValidatePoolRebalance()

			UpdateOutcome(event, err)

		})
	}
}

// TriggerUpgradeVolumeDriver upgrades volume driver version to the latest build
func TriggerUpgradeVolumeDriver(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {
	defer ginkgo.GinkgoRecover()
	defer endLongevityTest()
	startLongevityTest(UpgradeVolumeDriver)
	event := &EventRecord{
		Event: Event{
			ID:   GenerateUUID(),
			Type: UpgradeVolumeDriver,
		},
		Start:   time.Now().Format(time.RFC1123),
		Outcome: []error{},
	}
	defer func() {
		event.End = time.Now().Format(time.RFC1123)
		*recordChan <- event
	}()

	setMetrics(*event)

	stepLog := "upgrade volume driver"
	context(stepLog, func() {
		if len(Inst().UpgradeStorageDriverEndpointList) == 0 {
			log.Fatalf("Unable to perform volume driver upgrade hops, none were given")
		}
		for _, upgradeHop := range strings.Split(Inst().UpgradeStorageDriverEndpointList, ",") {
			stepLog = "start the volume driver upgrade"
			Step(stepLog, func() {
				log.InfoD(stepLog)
				err := Inst().V.UpgradeDriver(upgradeHop)
				if err != nil {
					log.InfoD("Error upgrading volume driver, Err: %v", err.Error())
				}
				UpdateOutcome(event, err)

			})
			stepLog = "validate all apps after upgrade"
			Step(stepLog, func() {
				log.InfoD(stepLog)
				errorChan := make(chan error, errorChannelSize)
				for _, ctx := range *contexts {
					ctx.SkipVolumeValidation = true
					ValidateContext(ctx, &errorChan)
					if strings.Contains(ctx.App.Key, fastpathAppName) {
						err := ValidateFastpathVolume(ctx, opsapi.FastpathStatus_FASTPATH_ACTIVE)
						UpdateOutcome(event, err)
					}
				}
			})
		}
	})
	updateMetrics(*event)
}

// TriggerUpgradeStork peforms upgrade of the stork
func TriggerUpgradeStork(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {
	defer ginkgo.GinkgoRecover()
	defer endLongevityTest()
	startLongevityTest(UpgradeStork)
	event := &EventRecord{
		Event: Event{
			ID:   GenerateUUID(),
			Type: UpgradeStork,
		},
		Start:   time.Now().Format(time.RFC1123),
		Outcome: []error{},
	}

	defer func() {
		event.End = time.Now().Format(time.RFC1123)
		*recordChan <- event
	}()

	setMetrics(*event)
	stepLog := "Upgrading stork to latest version based on the compatible PX and storage driver upgrade version "
	Step(stepLog,
		func() {
			if len(Inst().UpgradeStorageDriverEndpointList) == 0 {
				log.Fatalf("Unable to perform volume driver upgrade hops, none were given")
			}
			for _, upgradeHop := range strings.Split(Inst().UpgradeStorageDriverEndpointList, ",") {
				log.InfoD(stepLog)
				err := Inst().V.UpgradeStork(upgradeHop)
				UpdateOutcome(event, err)
			}

		})
	updateMetrics(*event)
}

// TriggerAutoFsTrim enables Auto Fstrim in the PX Cluster
func TriggerAutoFsTrim(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {
	defer ginkgo.GinkgoRecover()
	defer endLongevityTest()
	startLongevityTest(AutoFsTrim)
	event := &EventRecord{
		Event: Event{
			ID:   GenerateUUID(),
			Type: AutoFsTrim,
		},
		Start:   time.Now().Format(time.RFC1123),
		Outcome: []error{},
	}

	defer func() {
		event.End = time.Now().Format(time.RFC1123)
		*recordChan <- event
	}()

	setMetrics(*event)

	stepLog := "Validate AutoFsTrim of the volumes"
	context(stepLog, func() {
		log.InfoD(stepLog)
		stepLog = "enable auto fstrim "
		Step(stepLog,
			func() {
				log.InfoD(stepLog)
				if !isAutoFsTrimEnabled {
					currNode := node.GetWorkerNodes()[0]
					err := Inst().V.SetClusterOpts(currNode, map[string]string{
						"--auto-fstrim": "on",
					})
					if err != nil {
						err = fmt.Errorf("error while enabling auto fstrim, Error:%v", err)
						log.Error(err.Error())
						UpdateOutcome(event, err)
					} else {
						log.InfoD("AutoFsTrim is successfully enabled")
						isAutoFsTrimEnabled = true
						time.Sleep(5 * time.Minute)
					}
				} else {
					log.InfoD("AutoFsTrim is already enabled")
				}

			})
		stepLog = "Validate AutoFsTrim Status "
		Step(stepLog,
			func() {
				log.InfoD(stepLog)
				validateAutoFsTrim(contexts, event)
			})
		stepLog = "Reboot attached node and validate AutoFsTrim Status "
		Step(stepLog,
			func() {
				log.InfoD(stepLog)
				for _, ctx := range *contexts {
					if strings.Contains(ctx.App.Key, "fstrim") {
						vols, _ := Inst().S.GetVolumes(ctx)
						for _, vol := range vols {

							n, err := Inst().V.GetNodeForVolume(vol, 1*time.Minute, 5*time.Second)
							UpdateOutcome(event, err)
							log.InfoD("volume %s is attached on node %s [%s]", vol.ID, n.SchedulerNodeName, n.Addresses[0])
							err = Inst().S.DisableSchedulingOnNode(*n)
							UpdateOutcome(event, err)

							Inst().V.StopDriver([]node.Node{*n}, false, nil)

							Inst().N.RebootNode(*n, node.RebootNodeOpts{
								Force: true,
								ConnectionOpts: node.ConnectionOpts{
									Timeout:         1 * time.Minute,
									TimeBeforeRetry: 5 * time.Second,
								},
							})

							log.InfoD("wait for a minute for node reboot")
							time.Sleep(1 * time.Minute)
							n2, err := Inst().V.GetNodeForVolume(vol, 1*time.Minute, 5*time.Second)
							if err != nil {

								log.InfoD("Got error while getting node for volume %v, wait for 2 minutes to retry. Error: %v", vol.ID, err)
								n2, err = Inst().V.GetNodeForVolume(vol, 3*time.Minute, 10*time.Second)
								if err != nil {
									err = fmt.Errorf("error while getting node for volume %v, Error: %v", vol.ID, err)
									UpdateOutcome(event, err)
								}

							}

							log.InfoD("volume %s is now attached on node %s [%s]", vol.ID, n2.SchedulerNodeName, n2.Addresses[0])
							errorChan := make(chan error, errorChannelSize)
							StartVolDriverAndWait([]node.Node{*n}, &errorChan)
							Inst().S.EnableSchedulingOnNode(*n)

						}

					}

				}
				validateAutoFsTrim(contexts, event)
			})
	})
	updateMetrics(*event)
}

// TriggerVolumeUpdate enables to test volume update
func TriggerVolumeUpdate(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {
	defer ginkgo.GinkgoRecover()
	defer endLongevityTest()
	startLongevityTest(UpdateVolume)
	event := &EventRecord{
		Event: Event{
			ID:   GenerateUUID(),
			Type: UpdateVolume,
		},
		Start:   time.Now().Format(time.RFC1123),
		Outcome: []error{},
	}
	defer func() {
		event.End = time.Now().Format(time.RFC1123)
		*recordChan <- event
	}()

	setMetrics(*event)
	stepLog := "Validate update of the volumes"
	context(stepLog, func() {
		log.InfoD(stepLog)
		stepLog = "Update Io priority on volumes "
		Step(stepLog,
			func() {
				log.InfoD(stepLog)
				updateIOPriorityOnVolumes(contexts, event)
				log.InfoD("Update IO priority call completed")
			})
	})
	updateMetrics(*event)
}

// updateIOPriorityOnVolumes this method is responsible for updating IO priority on Volumes.
func updateIOPriorityOnVolumes(contexts *[]*scheduler.Context, event *EventRecord) {
	for _, ctx := range *contexts {
		var appVolumes []*volume.Volume
		var err error
		appVolumes, err = Inst().S.GetVolumes(ctx)
		UpdateOutcome(event, err)
		if len(appVolumes) == 0 {
			UpdateOutcome(event, fmt.Errorf("found no volumes for app "))
		}
		for _, v := range appVolumes {
			log.InfoD("Getting info from volume: %s", v.ID)
			appVol, err := Inst().V.InspectVolume(v.ID)
			if err != nil {
				log.Errorf("Error inspecting volume: %v", err)
			}
			if requiredPriority, ok := appVol.Spec.VolumeLabels["priority_io"]; ok {
				if !setIoPriority {
					if requiredPriority == "low" {
						requiredPriority = "high"
					} else if requiredPriority == "high" {
						requiredPriority = "low"
					}
				}
				log.InfoD("Expected Priority %v", requiredPriority)
				log.InfoD("COS %v", appVol.Spec.GetCos().SimpleString())
				if !strings.EqualFold(requiredPriority, appVol.Spec.GetCos().SimpleString()) {
					err = Inst().V.UpdateIOPriority(v.ID, requiredPriority)
					if err != nil {
						UpdateOutcome(event, err)
					}
					//Verify Volume set with required IOPriority.
					appVol, err := Inst().V.InspectVolume(v.ID)
					log.InfoD("COS after update %v", appVol.Spec.GetCos().SimpleString())
					if !strings.EqualFold(requiredPriority, appVol.Spec.GetCos().SimpleString()) {
						err = fmt.Errorf("Failed to update volume %v with expected priority %v ", v.ID, requiredPriority)
						UpdateOutcome(event, err)
					}
					log.InfoD("Update IO priority on [%v] : [%v]", v.ID, requiredPriority)
				}
				log.InfoD("Completed update on %v", v.ID)
			}
		}
		// setIoPriority if IO priority is set to High then next iteration will be run with low.
		if setIoPriority {
			setIoPriority = false
		} else {
			setIoPriority = true
		}
	}
}

func validateAutoFsTrim(contexts *[]*scheduler.Context, event *EventRecord) {
	for _, ctx := range *contexts {
		var appVolumes []*volume.Volume
		var err error
		if strings.Contains(ctx.App.Key, "fstrim") {
			appVolumes, err = Inst().S.GetVolumes(ctx)
			UpdateOutcome(event, err)
			if len(appVolumes) == 0 {
				UpdateOutcome(event, fmt.Errorf("found no volumes for app %s", ctx.App.Key))
			}

			for _, v := range appVolumes {
				// Skip autofs trim status on Pure DA volumes
				isPureVol, err := Inst().V.IsPureVolume(v)
				if err != nil {
					UpdateOutcome(event, err)
				}
				if isPureVol {
					log.Warnf(
						"Autofs Trim is not supported for Pure DA volume: [%s]. "+
							"Skipping autofs trim status on pure volumes", v.Name,
					)
					continue
				}
				log.Infof("Getting info : %s", v.ID)
				appVol, err := Inst().V.InspectVolume(v.ID)
				if err != nil {
					log.Errorf("Error inspecting volume: %v", err)
				}
				attachedNode := appVol.AttachedOn

				fsTrimStatuses, err := Inst().V.GetAutoFsTrimStatus(attachedNode)
				if err != nil {
					UpdateOutcome(event, err)
				}

				val, ok := fsTrimStatuses[appVol.Id]
				var fsTrimStatus string

				if !ok {
					fsTrimStatus = waitForFsTrimStatus(event, attachedNode, appVol.Id)
				} else {
					fsTrimStatus = val.String()
				}

				if fsTrimStatus != "" {

					if strings.Contains(fsTrimStatus, "FAILED") {

						err = fmt.Errorf("AutoFstrim failed for volume %v, status: %v", v.ID, val.String())
						UpdateOutcome(event, err)

					} else {
						log.InfoD("Autofstrim status for volume %v, status: %v", v.ID, val.String())

					}
				} else {
					err = fmt.Errorf("autofstrim for volume %v not started", v.ID)
					log.Errorf("Error: %v", err)
					UpdateOutcome(event, err)
				}

			}

		}
	}

}

func waitForFsTrimStatus(event *EventRecord, attachedNode, volumeID string) string {
	doExit := false
	exitCount := 50

	for !doExit {
		log.Infof("Autofstrim for volume %v not started, retrying after 2 mins", volumeID)
		time.Sleep(2 * time.Minute)
		fsTrimStatuses, err := Inst().V.GetAutoFsTrimStatus(attachedNode)
		if err != nil {
			UpdateOutcome(event, err)
		}

		fsTrimStatus, isValueExist := fsTrimStatuses[volumeID]

		if isValueExist {
			return fsTrimStatus.String()
		}
		if exitCount == 0 {
			doExit = true
		}
		exitCount--
	}
	return ""
}

// TriggerTrashcan enables trashcan feature in the PX Cluster and validates it
func TriggerTrashcan(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {
	defer ginkgo.GinkgoRecover()
	defer endLongevityTest()
	startLongevityTest(Trashcan)
	event := &EventRecord{
		Event: Event{
			ID:   GenerateUUID(),
			Type: Trashcan,
		},
		Start:   time.Now().Format(time.RFC1123),
		Outcome: []error{},
	}

	defer func() {
		event.End = time.Now().Format(time.RFC1123)
		*recordChan <- event
	}()

	setMetrics(*event)

	stepLog := "Validate Trashcan feature of the volumes"

	context(stepLog, func() {
		log.InfoD(stepLog)
		if !isTrashcanEnabled {
			stepLog = "enable trashcan"
			Step(stepLog,
				func() {
					log.InfoD(stepLog)
					currNode := node.GetWorkerNodes()[0]
					err := Inst().V.SetClusterOptsWithConfirmation(currNode, map[string]string{
						"--volume-expiration-minutes": "600",
					})
					if err != nil {
						err = fmt.Errorf("error while enabling trashcan, Error:%v", err)
						log.Error(err.Error())
						UpdateOutcome(event, err)

					} else {
						log.InfoD("Trashcan is successfully enabled")
						isTrashcanEnabled = true
					}

				})
		} else {
			var trashcanVols []string
			var err error
			node := node.GetWorkerNodes()[0]
			stepLog = "Validating trashcan"
			Step(stepLog,
				func() {
					log.InfoD(stepLog)
					trashcanVols, err = Inst().V.GetTrashCanVolumeIds(node)
					if err != nil {
						log.InfoD("Error While getting trashcan volumes, Err %v", err)
					}

					if len(trashcanVols) == 0 {
						err = fmt.Errorf("no volumes present in trashcan")
						UpdateOutcome(event, err)

					}
				})
			stepLog = "Validating trashcan restore"
			Step(stepLog,
				func() {
					log.InfoD(stepLog)
					if len(trashcanVols) != 0 {
						volToRestore := trashcanVols[len(trashcanVols)-1]
						log.InfoD("Restoring vol [%v] from trashcan", volToRestore)
						volName := fmt.Sprintf("%s-res", volToRestore[len(volToRestore)-4:])
						pxctlCmdFull := fmt.Sprintf("v r %s --trashcan %s", volName, volToRestore)
						output, err := Inst().V.GetPxctlCmdOutput(node, pxctlCmdFull)
						if err == nil {
							log.InfoD("output: %v", output)
							if !strings.Contains(output, fmt.Sprintf("Successfully restored: %s", volName)) {
								err = fmt.Errorf("volume %v, restore from trashcan failed, Err: %v", volToRestore, output)
								UpdateOutcome(event, err)
							}
						} else {
							log.InfoD("Error restoring: %v", err)
							UpdateOutcome(event, err)
						}

					}

				})
		}
	})
	updateMetrics(*event)
}

// TriggerRelaxedReclaim enables Relaxed Reclaim in the PX Cluster
func TriggerRelaxedReclaim(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {
	defer ginkgo.GinkgoRecover()
	defer endLongevityTest()
	startLongevityTest(RelaxedReclaim)
	event := &EventRecord{
		Event: Event{
			ID:   GenerateUUID(),
			Type: RelaxedReclaim,
		},
		Start:   time.Now().Format(time.RFC1123),
		Outcome: []error{},
	}

	defer func() {
		event.End = time.Now().Format(time.RFC1123)
		*recordChan <- event
	}()

	setMetrics(*event)

	stepLog := "Validate Relaxed Reclaim of the volumes"
	context(stepLog, func() {
		log.InfoD(stepLog)
		if !isRelaxedReclaimEnabled {
			stepLog = "enable relaxed reclaim "
			Step(stepLog,
				func() {
					log.InfoD(stepLog)
					currNode := node.GetWorkerNodes()[0]
					err := Inst().V.SetClusterOptsWithConfirmation(currNode, map[string]string{
						"--relaxedreclaim-delete-seconds": "600",
					})
					if err != nil {
						err = fmt.Errorf("error while enabling relaxed reclaim, Error:%v", err)
						log.Error(err.Error())
						UpdateOutcome(event, err)

					} else {
						log.InfoD("RelaxedReclaim is successfully enabled")
						isRelaxedReclaimEnabled = true
					}

				})
		} else {
			stepLog = "Validating relaxed reclaim "
			Step(stepLog,
				func() {
					log.InfoD(stepLog)
					nodes := node.GetWorkerNodes()
					totalDeleted := 0
					totalPending := 0
					totalSkipped := 0

					for _, n := range nodes {
						nodeStatsMap, err := Inst().V.GetNodeStats(n)

						if err != nil {
							log.Errorf("error while getting node stats for node : %v, err: %v", n.Name, err)
						} else {
							log.InfoD("Node [%v] relaxed reclaim stats : %v", n.Name, nodeStatsMap[n.Name])

							totalDeleted += nodeStatsMap[n.Name]["deleted"]
							totalPending += nodeStatsMap[n.Name]["pending"]
							totalSkipped += nodeStatsMap[n.Name]["skipped"]
						}
					}

					totalVal := totalDeleted + totalPending + totalSkipped

					if totalVal == 0 {
						err := fmt.Errorf("no stats present for relaxed reclaim")
						UpdateOutcome(event, err)
					}

				})
		}
	})
	updateMetrics(*event)
}

// TriggerNodeDecommission decommission the node for the PX cluster
func TriggerNodeDecommission(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {
	defer ginkgo.GinkgoRecover()
	defer endLongevityTest()
	startLongevityTest(NodeDecommission)
	event := &EventRecord{
		Event: Event{
			ID:   GenerateUUID(),
			Type: NodeDecommission,
		},
		Start:   time.Now().Format(time.RFC1123),
		Outcome: []error{},
	}

	defer func() {
		event.End = time.Now().Format(time.RFC1123)
		*recordChan <- event
	}()

	setMetrics(*event)

	var workerNodes []node.Node
	var nodeToDecomm node.Node
	stepLog := "Decommission a random node"
	Step(stepLog, func() {
		log.InfoD(stepLog)
		workerNodes = node.GetWorkerNodes()
		index := rand.Intn(len(workerNodes))
		nodeToDecomm = workerNodes[index]
		stepLog = fmt.Sprintf("decommission node %s", nodeToDecomm.Name)
		Step(stepLog, func() {
			log.InfoD(stepLog)
			err := Inst().S.PrepareNodeToDecommission(nodeToDecomm, Inst().Provisioner)
			if err != nil {
				UpdateOutcome(event, err)
			} else {
				err = Inst().V.DecommissionNode(&nodeToDecomm)
				if err != nil {
					log.InfoD("Error while decommissioning the node: %v, Error:%v", nodeToDecomm.Name, err)
					UpdateOutcome(event, err)
				}

			}

			t := func() (interface{}, bool, error) {
				status, err := Inst().V.GetNodeStatus(nodeToDecomm)
				if err != nil {
					return false, true, fmt.Errorf("error getting node %v status", nodeToDecomm.Name)
				}
				if *status == opsapi.Status_STATUS_NONE {
					return true, false, nil
				}
				return false, true, fmt.Errorf("node %s not decomissioned yet,Current Status: %v", nodeToDecomm.Name, *status)
			}
			_, err = task.DoRetryWithTimeout(t, defaultTimeout, defaultRetryInterval)

			if err != nil {
				UpdateOutcome(event, err)
				err = Inst().V.RecoverNode(&nodeToDecomm)
				log.Errorf("Error recovering node after failed decommission, Err: %v", err)
				UpdateOutcome(event, err)
			} else {
				decommissionedNode = nodeToDecomm
			}

		})
		updateMetrics(*event)
	})

	for _, ctx := range *contexts {

		Step(fmt.Sprintf("validating context after node: [%s] decommission",
			nodeToDecomm.Name), func() {
			errorChan := make(chan error, errorChannelSize)
			ctx.SkipVolumeValidation = true
			ValidateContext(ctx, &errorChan)
			for err := range errorChan {
				UpdateOutcome(event, err)
				if strings.Contains(ctx.App.Key, fastpathAppName) {
					err := ValidateFastpathVolume(ctx, opsapi.FastpathStatus_FASTPATH_ACTIVE)
					UpdateOutcome(event, err)
				}
			}
		})
	}
}

// TriggerNodeRejoin rejoins the decommissioned node
func TriggerNodeRejoin(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {
	defer ginkgo.GinkgoRecover()
	defer endLongevityTest()
	startLongevityTest(NodeRejoin)
	event := &EventRecord{
		Event: Event{
			ID:   GenerateUUID(),
			Type: NodeRejoin,
		},
		Start:   time.Now().Format(time.RFC1123),
		Outcome: []error{},
	}

	defer func() {
		event.End = time.Now().Format(time.RFC1123)
		*recordChan <- event
	}()

	setMetrics(*event)

	var decommissionedNodeName string

	stepLog := "Rejoin the node"
	Step(stepLog, func() {
		log.InfoD(stepLog)
		if decommissionedNode.Name != "" {
			decommissionedNodeName = decommissionedNode.Name
			stepLog = fmt.Sprintf("Rejoin node %s", decommissionedNode.Name)
			Step(stepLog, func() {
				log.InfoD(stepLog)
				err := Inst().V.RejoinNode(&decommissionedNode)

				if err != nil {
					log.InfoD("Error while rejoining the node. error: %v", err)
					UpdateOutcome(event, err)
				} else {
					isNodeExist := false
					log.InfoD("Waiting for node to rejoin and refresh inventory")
					time.Sleep(90 * time.Second)
					t := func() (interface{}, bool, error) {
						err = Inst().S.RefreshNodeRegistry()
						if err != nil {
							log.Errorf("Error refreshing node registry, Error : %v", err)
							return "", true, err
						}
						latestNodes, err := Inst().V.GetDriverNodes()
						if err != nil {
							log.Errorf("Error getting px nodes, Error : %v", err)
							return "", true, err
						}

						for _, latestNode := range latestNodes {
							log.InfoD("Inspecting Node: %v", latestNode.Hostname)
							if latestNode.Hostname == decommissionedNode.Hostname {
								isNodeExist = true
								return "", false, nil
							}
						}
						return "", true, fmt.Errorf("node %s node yet rejoined", decommissionedNode.Hostname)
					}
					_, err := task.DoRetryWithTimeout(t, 5*time.Minute, 1*time.Minute)

					if !isNodeExist {
						err = fmt.Errorf("node %v rejoin failed,Error: %v", decommissionedNode.Hostname, err)
						log.Error(err.Error())
						UpdateOutcome(event, err)
					} else {
						log.InfoD("node %v rejoin is successful ", decommissionedNode.Hostname)
					}
				}
			})
			decommissionedNode = node.Node{}
		}
	})

	for _, ctx := range *contexts {

		Step(fmt.Sprintf("validating context after node: [%s] rejoin",
			decommissionedNodeName), func() {
			errorChan := make(chan error, errorChannelSize)
			ctx.SkipVolumeValidation = true
			ValidateContext(ctx, &errorChan)
			for err := range errorChan {
				UpdateOutcome(event, err)
			}
		})
	}
	updateMetrics(*event)
}

// TriggerCsiSnapShot takes csi snapshots of the volumes and validates snapshot
func TriggerCsiSnapShot(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {
	var err error
	defer ginkgo.GinkgoRecover()
	defer endLongevityTest()
	startLongevityTest(CsiSnapShot)
	uuid := GenerateUUID()
	event := &EventRecord{
		Event: Event{
			ID:   uuid,
			Type: CsiSnapShot,
		},
		Start:   time.Now().Format(time.RFC1123),
		Outcome: []error{},
	}

	defer func() {
		event.End = time.Now().Format(time.RFC1123)
		*recordChan <- event
	}()

	setMetrics(*event)

	err = fmt.Errorf("Testing failure alerts")
	UpdateOutcome(event, err)
	// Keeping retainSnapCount
	retainSnapCount := DefaultSnapshotRetainCount
	stepLog := "Create and Validate snapshots for FA DA volumes"
	Step(stepLog, func() {
		log.InfoD(stepLog)
		if !isCsiVolumeSnapshotClassExist {
			log.InfoD("Creating csi volume snapshot class")
			snapShotClassName := PureSnapShotClass + time.Now().Format("01-02-15h04m05s")
			if volSnapshotClass, err = Inst().S.CreateCsiSnapshotClass(snapShotClassName, "Delete"); err != nil {
				log.Errorf("Create volume snapshot class failed with error: [%v]", err)
				UpdateOutcome(event, err)
			}
			log.InfoD("Successfully created volume snapshot class: %v", volSnapshotClass.Name)
			isCsiVolumeSnapshotClassExist = true
			summary, err := Inst().V.GetLicenseSummary()

			if err != nil {
				log.Errorf("Failed to get license summary.Error: %v", err)
				UpdateOutcome(event, err)
			}

			// For essential license max 5 snapshots to be created for each volume
			if summary.SKU == EssentialsFaFbSKU {
				retainSnapCount = 4

			}
			log.InfoD("Cluster is having: [%v] license. Setting snap retain count to: [%v]", summary.SKU, retainSnapCount)
		}
		for _, ctx := range *contexts {
			var volumeSnapshotMap map[string]*v1beta1.VolumeSnapshot
			var err error
			stepLog = fmt.Sprintf("Deleting snapshots when retention count limit got exceeded for %s app", ctx.App.Key)
			Step(stepLog, func() {
				err = Inst().S.DeleteCsiSnapsForVolumes(ctx, retainSnapCount)
				if err != nil {
					log.Errorf("Snapshot delete is failing with error: [%v]", err)
					UpdateOutcome(event, err)
				}
			})
			stepLog = fmt.Sprintf("Creating snapshots for %s app", ctx.App.Key)
			Step(stepLog, func() {
				volumeSnapshotMap, err = Inst().S.CreateCsiSnapsForVolumes(ctx, volSnapshotClass.Name)
				if err != nil {
					log.Errorf("Creating volume snapshot failed with error: [%v]", err)
					UpdateOutcome(event, err)
				}
			})
			stepLog = fmt.Sprintf("Validate snapshot for %s app", ctx.App.Key)
			Step(stepLog, func() {
				log.InfoD(stepLog)
				if err = Inst().S.ValidateCsiSnapshots(ctx, volumeSnapshotMap); err != nil {
					log.Errorf("Validating volume snapshot failed with error: [%v]", err)
					UpdateOutcome(event, err)
				}
				log.InfoD("Successfully validated the snapshot for %s app", ctx.App.Key)
			})
		}
	})
	updateMetrics(*event)
}

// TriggerCsiSnapRestore create pvc from snapshot and validate the restored PVC
func TriggerCsiSnapRestore(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {
	defer ginkgo.GinkgoRecover()
	defer endLongevityTest()
	startLongevityTest(CsiSnapRestore)
	uuid := GenerateUUID()
	event := &EventRecord{
		Event: Event{
			ID:   uuid,
			Type: CsiSnapRestore,
		},
		Start:   time.Now().Format(time.RFC1123),
		Outcome: []error{},
	}

	defer func() {
		event.End = time.Now().Format(time.RFC1123)
		*recordChan <- event
	}()

	setMetrics(*event)

	var err error
	stepLog := "Restore the Snapshot and Validate the PVC"
	Step(stepLog, func() {
		log.InfoD(stepLog)
		if !isCsiRestoreStorageClassExist {
			var blockSc *storageapi.StorageClass
			var param = make(map[string]string)
			pureStorageClassMap = make(map[string]*storageapi.StorageClass)
			log.InfoD("Creating csi volume snapshot class")
			blkScName := PureBlockStorageClass + time.Now().Format("01-02-15h04m05s")
			// Adding a pure backend parameters
			param[PureBackend] = k8s.PureBlock
			if blockSc, err = createPureStorageClass(blkScName, param); err != nil {
				log.Errorf("StorageClass creation failed for SC: %s", blkScName)
				UpdateOutcome(event, err)
			}
			pureStorageClassMap[k8s.PureBlock] = blockSc
			isCsiRestoreStorageClassExist = true
		}
		for _, ctx := range *contexts {
			//var volumePVCMap map[string]v1.PersistentVolumeClaim
			stepLog = fmt.Sprintf("Restore and validate snapshot for %s app", ctx.App.Key)
			Step(stepLog, func() {
				log.InfoD(stepLog)
				_, err = Inst().S.RestoreCsiSnapAndValidate(ctx, pureStorageClassMap)
				if err != nil {
					log.Errorf("Restoring snapshot failed with error: [%v]", err)
					UpdateOutcome(event, err)
				}
			})
		}
	})
	updateMetrics(*event)
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

func createLongevityJiraIssue(event *EventRecord, err error) {

	actualEvent := strings.Split(event.Event.Type, "<br>")[0]
	eventsGenerated, ok := jiraEvents[actualEvent]
	issueExists := false
	t := time.Now().Format(time.RFC1123)
	if ok {
		log.Infof("Event type [%v] exists", actualEvent)

		for _, e := range eventsGenerated {
			iss := strings.Split(e, "->")[1]
			if strings.Contains(iss, err.Error()) {
				issueExists = true
			}
		}
	} else {
		log.Infof("Event type [%v] does not exists", actualEvent)
		errorsSlice := make([]string, 0)
		jiraEvents[actualEvent] = errorsSlice
	}

	if !issueExists {
		log.Info("Creating Jira Issue")

		//adding issue to existing jiraEvents
		issues := jiraEvents[actualEvent]
		issues = append(issues, fmt.Sprintf("%v->%v", t, err.Error()))
		jiraEvents[actualEvent] = issues

		summary := fmt.Sprintf("[%v]: Error %v occured in Torpedo Longevity", actualEvent, err)
		summary = strings.Replace(summary, "\r\n", "", -1)
		summary = strings.Replace(summary, "\n", "", -1)
		var lines []string

		var masterNodeIps []string

		for _, n := range node.GetMasterNodes() {
			masterNodeIps = append(masterNodeIps, n.Addresses...)
		}

		lines = append(lines, fmt.Sprintf("Master Node: %v", masterNodeIps))
		lines = append(lines, fmt.Sprintf("Error Occured time: %v", t))
		lines = append(lines, fmt.Sprintf("Event: %v", event.Event.Type))
		lines = append(lines, fmt.Sprintf("Error: %v", err.Error()))

		description := ""

		for _, line := range lines {
			description = description + fmt.Sprintf("%v\r\n", line)
		}

		CreateJiraIssueWithLogs(description, summary)

	}
}

// TriggerKVDBFailover performs kvdb failover in cyclic manner
func TriggerKVDBFailover(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {
	defer ginkgo.GinkgoRecover()
	defer endLongevityTest()
	startLongevityTest(KVDBFailover)
	event := &EventRecord{
		Event: Event{
			ID:   GenerateUUID(),
			Type: KVDBFailover,
		},
		Start:   time.Now().Format(time.RFC1123),
		Outcome: []error{},
	}
	defer func() {
		event.End = time.Now().Format(time.RFC1123)
		*recordChan <- event
	}()

	setMetrics(*event)
	stepLog := "perform kvdb failover in a cyclic manner"
	context(stepLog, func() {
		log.InfoD(stepLog)
		stepLog = "Get KVDB nodes and perform failover"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			nodes := node.GetWorkerNodes()

			kvdbMembers, err := Inst().V.GetKvdbMembers(nodes[0])

			if err != nil {
				err = fmt.Errorf("error getting kvdb members using node %v. cause: %v", nodes[0].Name, err)
				log.InfoD(err.Error())
				UpdateOutcome(event, err)
			}

			log.InfoD("Validating initial KVDB members")

			allhealthy := validateKVDBMembers(event, kvdbMembers, false)

			if allhealthy {
				nodeMap := node.GetNodesByVoDriverNodeID()

				for id := range kvdbMembers {
					kvdbNode := nodeMap[id]
					errorChan := make(chan error, errorChannelSize)
					StopVolDriverAndWait([]node.Node{kvdbNode}, &errorChan)
					for err := range errorChan {
						UpdateOutcome(event, err)
					}

					isKvdbStatusUpdated := false
					waitTime := 10

					for !isKvdbStatusUpdated && waitTime > 0 {
						newKvdbMembers, err := Inst().V.GetKvdbMembers(nodes[0])
						if err != nil {
							err = fmt.Errorf("error getting kvdb members using node %v, cause: %v ", nodes[0].Name, err)
							log.Error(err.Error())

						}
						m, ok := newKvdbMembers[id]

						if !ok && len(newKvdbMembers) > 0 {
							log.InfoD("node %v is no longer kvdb member", kvdbNode.Name)
							isKvdbStatusUpdated = true

						}
						if ok && !m.IsHealthy {
							log.InfoD("kvdb node %v isHealthy?: %v", id, m.IsHealthy)
							isKvdbStatusUpdated = true
						} else {
							log.InfoD("Waiting for kvdb node %v health status to update to false after PX is stopped", kvdbNode.Name)
							time.Sleep(1 * time.Minute)
						}

						waitTime--

					}
					if err != nil {
						log.Error(err.Error())
						UpdateOutcome(event, err)

					}

					waitTime = 15
					isKvdbMembersUpdated := false

					for !isKvdbMembersUpdated && waitTime > 0 {
						newKvdbMembers, err := Inst().V.GetKvdbMembers(nodes[0])
						if err != nil {
							err = fmt.Errorf("error getting kvdb members using node %v, cause: %v ", nodes[0].Name, err)
							log.Error(err.Error())
						}

						_, ok := newKvdbMembers[id]
						if ok {
							log.InfoD("Node %v still exist as a KVDB member. Waiting for failover to happen", kvdbNode.Name)
							time.Sleep(2 * time.Minute)
						} else {
							log.InfoD("node %v is no longer kvdb member", kvdbNode.Name)
							isKvdbMembersUpdated = true

						}
						waitTime--
					}

					if err != nil {
						log.Error(err.Error())
						UpdateOutcome(event, err)
					}

					newKvdbMembers, err := Inst().V.GetKvdbMembers(nodes[0])
					if err != nil {
						log.Error(err.Error())
						UpdateOutcome(event, err)
					}
					kvdbMemberStatus := validateKVDBMembers(event, newKvdbMembers, false)

					errorChan = make(chan error, errorChannelSize)

					if !kvdbMemberStatus {
						log.InfoD("Skipping remaining Kvdb node failovers as not all members are healthy")
						StartVolDriverAndWait([]node.Node{kvdbNode}, &errorChan)
						for err = range errorChan {
							UpdateOutcome(event, err)
						}
						break
					}

					StartVolDriverAndWait([]node.Node{kvdbNode}, &errorChan)
					for err = range errorChan {
						UpdateOutcome(event, err)
					}

				}
			} else {
				err = fmt.Errorf("not all kvdb members are healthy")
				log.Errorf(err.Error())
				UpdateOutcome(event, err)
			}

		})
	})
	updateMetrics(*event)
}

func validateKVDBMembers(event *EventRecord, kvdbMembers map[string]*volume.MetadataNode, isDestuctive bool) bool {
	log.InfoD("Current KVDB members: %v", kvdbMembers)

	allHealthy := true

	if len(kvdbMembers) == 0 {
		err := fmt.Errorf("No KVDB membes to validate")
		UpdateOutcome(event, err)
		return false
	}

	for id, m := range kvdbMembers {

		if !m.IsHealthy {
			err := fmt.Errorf("kvdb member node: %v is not healthy", id)
			allHealthy = allHealthy && false
			log.Warn(err.Error())
			if isDestuctive {
				UpdateOutcome(event, err)
			}
		} else {
			log.InfoD("KVDB member node %v is healthy", id)
		}
	}

	return allHealthy

}

// TriggerAppTasksDown performs app scale up and down according to chaos level
func TriggerAppTasksDown(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {
	defer ginkgo.GinkgoRecover()
	defer endLongevityTest()
	startLongevityTest(AppTasksDown)
	event := &EventRecord{
		Event: Event{
			ID:   GenerateUUID(),
			Type: AppTasksDown,
		},
		Start:   time.Now().Format(time.RFC1123),
		Outcome: []error{},
	}
	defer func() {
		event.End = time.Now().Format(time.RFC1123)
		*recordChan <- event
	}()

	setMetrics(*event)

	chaosLevel := ChaosMap[AppTasksDown]
	stepLog := "deletes all pods from a given app and validate if they recover"
	context(stepLog, func() {
		log.InfoD(stepLog)
		for _, ctx := range *contexts {
			for i := 0; i < chaosLevel; i++ {
				stepLog = fmt.Sprintf("delete tasks for app: %s", ctx.App.Key)
				Step(stepLog, func() {
					log.InfoD(stepLog)
					err := Inst().S.DeleteTasks(ctx, nil)
					if err != nil {
						PrintDescribeContext(ctx)
					}
					UpdateOutcome(event, err)
				})
				stepLog = "validate all apps after deletion"
				Step(stepLog, func() {
					log.InfoD(stepLog)
					errorChan := make(chan error, errorChannelSize)
					ctx.SkipVolumeValidation = true
					ValidateContext(ctx, &errorChan)
				})
			}
		}
	})
	updateMetrics(*event)
}

// TriggerValidateDeviceMapperCleanup validate device mapper device cleaned up for FA setup
func TriggerValidateDeviceMapperCleanup(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {
	defer ginkgo.GinkgoRecover()
	defer endLongevityTest()
	startLongevityTest(ValidateDeviceMapper)
	event := &EventRecord{
		Event: Event{
			ID:   GenerateUUID(),
			Type: ValidateDeviceMapper,
		},
		Start:   time.Now().Format(time.RFC1123),
		Outcome: []error{},
	}
	defer func() {
		event.End = time.Now().Format(time.RFC1123)
		*recordChan <- event
	}()

	setMetrics(*event)

	log.InfoD("Validating the deviceMapper devices cleaned up or not")
	stepLog := "Match the devicemapper devices in each node if it matches the expected count or not "
	Step(stepLog, func() {
		log.InfoD(stepLog)
		pureVolAttachedMap, err := Inst().V.GetNodePureVolumeAttachedCountMap()
		if err != nil {
			log.Error(err.Error())
			UpdateOutcome(event, err)
		}

		for _, n := range node.GetWorkerNodes() {
			log.InfoD("Validating the node: %v", n.Name)
			expectedDevMapperCount := 0
			storageNode, err := Inst().V.GetDriverNode(&n)
			if err != nil {
				log.Error(err.Error())
				UpdateOutcome(event, err)
			}

			if storageNode != nil {
				expectedDevMapperCount += len(storageNode.Disks)
				expectedDevMapperCount += pureVolAttachedMap[n.Name]
				actualCount, err := Inst().N.GetDeviceMapperCount(n, defaultTimeout)
				if err != nil {
					log.Error(err.Error())
					UpdateOutcome(event, err)
				}
				if int(math.Abs(float64(expectedDevMapperCount)-float64(actualCount))) > 1 || (expectedDevMapperCount == 0 && actualCount >= 1) || (actualCount == 0 && expectedDevMapperCount >= 1) {
					err := fmt.Errorf("device count mismatch in node: %v. Expected device: %v, Found %v device",
						n.Name, expectedDevMapperCount, actualCount)
					log.Error(err)
					UpdateOutcome(event, err)
				}
				log.InfoD("Successfully validated the deviceMapper device cleaned up in a node: %v", n.Name)
			}
		}
	})
	updateMetrics(*event)
}

// TriggerAddDrive performs add drive operation
func TriggerAddDrive(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {
	defer ginkgo.GinkgoRecover()
	defer endLongevityTest()
	startLongevityTest(AddDrive)
	event := &EventRecord{
		Event: Event{
			ID:   GenerateUUID(),
			Type: AddDrive,
		},
		Start:   time.Now().Format(time.RFC1123),
		Outcome: []error{},
	}

	defer func() {
		event.End = time.Now().Format(time.RFC1123)
		*recordChan <- event
	}()

	setMetrics(*event)
	stepLog := fmt.Sprintf("Perform add drive on all the worker nodes")
	Step(stepLog, func() {

		storageNodes := node.GetStorageNodes()

		isCloudDrive, err := IsCloudDriveInitialised(storageNodes[0])
		UpdateOutcome(event, err)
		systemOpts := node.SystemctlOpts{
			ConnectionOpts: node.ConnectionOpts{
				Timeout:         defaultTimeout,
				TimeBeforeRetry: defaultRetryInterval,
			},
			Action: "start",
		}
		if err == nil && !isCloudDrive {
			for _, storageNode := range storageNodes {
				log.InfoD("Get Block drives to add for node %s", storageNode.Name)
				blockDrives, err := Inst().N.GetBlockDrives(storageNode, systemOpts)
				UpdateOutcome(event, err)

				drvPaths := make([]string, 5)

				for _, drv := range blockDrives {
					if drv.MountPoint == "" && drv.FSType == "" && drv.Type == "disk" {
						drvPaths = append(drvPaths, drv.Path)
						break
					}
				}

				err = Inst().V.AddBlockDrives(&storageNode, drvPaths)
				if err != nil && strings.Contains(err.Error(), "no block drives available to add") {
					log.Warn(err.Error())
					continue
				}

				UpdateOutcome(event, err)
			}

			for _, ctx := range *contexts {
				stepLog = fmt.Sprintf("validating context after add drive on storage nodes")
				Step(stepLog, func() {
					log.InfoD(stepLog)
					errorChan := make(chan error, errorChannelSize)
					ctx.SkipVolumeValidation = true
					ValidateContext(ctx, &errorChan)
					for err := range errorChan {
						UpdateOutcome(event, err)
					}
				})
			}
		}

	})
	updateMetrics(*event)
}

// TriggerAsyncDR triggers Async DR
func TriggerAsyncDR(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {
	log.Infof("Async DR triggered at: %v", time.Now())
	defer ginkgo.GinkgoRecover()
	event := &EventRecord{
		Event: Event{
			ID:   GenerateUUID(),
			Type: AppTasksDown,
		},
		Start:   time.Now().Format(time.RFC1123),
		Outcome: []error{},
	}
	defer func() {
		event.End = time.Now().Format(time.RFC1123)
		*recordChan <- event
	}()

	setMetrics(*event)

	chaosLevel := ChaosMap[AsyncDR]
	var (
		migrationNamespaces   []string
		taskNamePrefix        = "async-dr-mig"
		allMigrations         []*storkapi.Migration
		includeResourcesFlag  = true
		startApplicationsFlag = false
	)

	Step(fmt.Sprintf("Deploy applications for migration, with frequency: %v", chaosLevel), func() {

		// Write kubeconfig files after reading from the config maps created by torpedo deploy script
		err := asyncdr.WriteKubeconfigToFiles()
		if err != nil {
			log.Errorf("Failed to write kubeconfig: %v", err)
		}

		err = SetSourceKubeConfig()
		if err != nil {
			log.Errorf("Failed to Set source kubeconfig: %v", err)
		}
		UpdateOutcome(event, err)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			taskName := fmt.Sprintf("%s-%d-%s", taskNamePrefix, i, time.Now().Format("15h03m05s"))
			log.Infof("Task name %s\n", taskName)
			appContexts := ScheduleApplications(taskName)
			*contexts = append(*contexts, appContexts...)
			ValidateApplications(*contexts)
			for _, ctx := range appContexts {
				// Override default App readiness time out of 5 mins with 10 mins
				ctx.ReadinessTimeout = appReadinessTimeout
				namespace := GetAppNamespace(ctx, taskName)
				migrationNamespaces = append(migrationNamespaces, namespace)
			}
			Step("Create cluster pair between source and destination clusters", func() {
				// Set cluster context to cluster where torpedo is running
				ScheduleValidateClusterPair(appContexts[0], false, true, defaultClusterPairDir, false)
			})
		}

		log.Infof("Migration Namespaces: %v", migrationNamespaces)

	})

	time.Sleep(5 * time.Minute)
	log.Info("Start migration")

	for i, currMigNamespace := range migrationNamespaces {
		migrationName := migrationKey + fmt.Sprintf("%d", i)
		currMig, err := asyncdr.CreateMigration(migrationName, currMigNamespace, asyncdr.DefaultClusterPairName, currMigNamespace, &includeResourcesFlag, &startApplicationsFlag)
		if err != nil {
			UpdateOutcome(event, fmt.Errorf("failed to create migration: %s in namespace %s. Error: [%v]", migrationKey, currMigNamespace, err))
		} else {
			allMigrations = append(allMigrations, currMig)
		}
	}

	// Validate all migrations
	for _, mig := range allMigrations {
		err := storkops.Instance().ValidateMigration(mig.Name, mig.Namespace, migrationRetryTimeout, migrationRetryInterval)
		if err != nil {
			UpdateOutcome(event, fmt.Errorf("failed to validate migration: %s in namespace %s. Error: [%v]", mig.Name, mig.Namespace, err))
		} else {
			UpdateOutcome(event, err)
		}
	}
	updateMetrics(*event)
}

// TriggerAsyncDRVolumeOnly triggers Async DR
func TriggerAsyncDRVolumeOnly(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {
	defer endLongevityTest()
	startLongevityTest(AsyncDRVolumeOnly)
	defer ginkgo.GinkgoRecover()
	log.InfoD("Volume Only Async DR triggered at: %v", time.Now())
	event := &EventRecord{
		Event: Event{
			ID:   GenerateUUID(),
			Type: AppTasksDown,
		},
		Start:   time.Now().Format(time.RFC1123),
		Outcome: []error{},
	}
	defer func() {
		event.End = time.Now().Format(time.RFC1123)
		*recordChan <- event
	}()

	setMetrics(*event)

	chaosLevel := ChaosMap[AsyncDRVolumeOnly]
	var (
		migrationNamespaces   []string
		taskNamePrefix        = "adr-vonly"
		allMigrations         []*storkapi.Migration
		includeResourcesFlag  = false
		startApplicationsFlag = false
	)

	Step(fmt.Sprintf("Deploy applications for volume-only migration, with frequency: %v", chaosLevel), func() {

		// Write kubeconfig files after reading from the config maps created by torpedo deploy script
		err := asyncdr.WriteKubeconfigToFiles()
		if err != nil {
			log.Errorf("Failed to write kubeconfig: %v", err)
		}

		err = SetSourceKubeConfig()
		if err != nil {
			log.Errorf("Failed to Set source kubeconfig: %v", err)
		}
		UpdateOutcome(event, err)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			taskName := fmt.Sprintf("%s-%d", taskNamePrefix, i)
			log.InfoD("Task name %s\n", taskName)
			appContexts := ScheduleApplications(taskName)
			*contexts = append(*contexts, appContexts...)
			ValidateApplications(*contexts)
			for _, ctx := range appContexts {
				// Override default App readiness time out of 5 mins with 10 mins
				ctx.ReadinessTimeout = appReadinessTimeout
				namespace := GetAppNamespace(ctx, taskName)
				migrationNamespaces = append(migrationNamespaces, namespace)
			}
			Step("Create cluster pair between source and destination clusters", func() {
				// Set cluster context to cluster where torpedo is running
				ScheduleValidateClusterPair(appContexts[0], false, true, defaultClusterPairDir, false)
			})
		}

		log.InfoD("Volume-only Migration Namespaces: %v", migrationNamespaces)

	})

	time.Sleep(5 * time.Minute)
	log.InfoD("Start volume only migration")

	for i, currMigNamespace := range migrationNamespaces {
		migrationName := migrationKey + fmt.Sprintf("%d", i)
		currMig, err := asyncdr.CreateMigration(migrationName, currMigNamespace, asyncdr.DefaultClusterPairName, currMigNamespace, &includeResourcesFlag, &startApplicationsFlag)
		if err != nil {
			UpdateOutcome(event, fmt.Errorf("failed to create migration: %s in namespace %s. Error: [%v]", migrationKey, currMigNamespace, err))
		} else {
			allMigrations = append(allMigrations, currMig)
		}
	}

	// Validate all migrations
	for _, mig := range allMigrations {
		err := storkops.Instance().ValidateMigration(mig.Name, mig.Namespace, migrationRetryTimeout, migrationRetryInterval)
		if err != nil {
			UpdateOutcome(event, fmt.Errorf("failed to validate migration: %s in namespace %s. Error: [%v]", mig.Name, mig.Namespace, err))
		}
		resp, get_mig_err := storkops.Instance().GetMigration(mig.Name, mig.Namespace)
		if get_mig_err != nil {
			UpdateOutcome(event, fmt.Errorf("failed to get migration: %s in namespace %s. Error: [%v]", mig.Name, mig.Namespace, get_mig_err))
		}
		resourcesMigrated := resp.Status.Summary.NumberOfMigratedResources
		if resourcesMigrated != 0 {
			UpdateOutcome(event, fmt.Errorf("resources should not migrate in volumeonlymigration case, numberOfmigratedresources should %d, getting %d",
				0, resourcesMigrated))
		} else {
			log.InfoD("Number of resources migrated in Volume Only migration should be 0, Resources migrated: %d", resourcesMigrated)
		}
	}
	updateMetrics(*event)
}

func TriggerStorkApplicationBackup(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {
	defer endLongevityTest()
	startLongevityTest(StorkApplicationBackup)
	defer ginkgo.GinkgoRecover()
	log.InfoD("Stork Appplication Backup triggered at: %v", time.Now())
	event := &EventRecord{
		Event: Event{
			ID:   GenerateUUID(),
			Type: AppTasksDown,
		},
		Start:   time.Now().Format(time.RFC1123),
		Outcome: []error{},
	}
	defer func() {
		event.End = time.Now().Format(time.RFC1123)
		*recordChan <- event
	}()
	setMetrics(*event)
	chaosLevel := ChaosMap[StorkApplicationBackup]

	var (
		s3SecretName     = "s3secret"
		backupNamespaces []string
		timeout          = 5 * time.Minute
		taskNamePrefix   = "stork-app-backup"
	)

	Step(fmt.Sprintf("Deploy applications for backup, with frequency: %v", chaosLevel), func() {

		// Write kubeconfig files after reading from the config maps created by torpedo deploy script
		err := asyncdr.WriteKubeconfigToFiles()
		if err != nil {
			UpdateOutcome(event, fmt.Errorf("unable to write kubeconfigs, getting error %v", err))
			return
		}
		err = SetSourceKubeConfig()
		if err != nil {
			UpdateOutcome(event, fmt.Errorf("getting error in setting source kubeconfig %v", err))
			return
		}
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			taskName := fmt.Sprintf("%s-%d", taskNamePrefix, i)
			log.Infof("Task name %s\n", taskName)
			appContexts := ScheduleApplications(taskName)
			*contexts = append(*contexts, appContexts...)
			ValidateApplications(*contexts)
			for _, ctx := range appContexts {
				namespace := GetAppNamespace(ctx, taskName)
				backupNamespaces = append(backupNamespaces, namespace)
			}
		}
		log.Infof("Backup applications, present in namespaces - %v", backupNamespaces)
		log.Infof("Start backup application")
		for i, currbkNamespace := range backupNamespaces {
			taskNamePrefix = taskNamePrefix + fmt.Sprintf("-%d", i)
			backuplocationname := taskNamePrefix + "-location" + time.Now().Format("15h03m05s")
			backupname := taskNamePrefix + time.Now().Format("15h03m05s")
			currBackupLocation, err := applicationbackup.CreateBackupLocation(backuplocationname, currbkNamespace, s3SecretName)
			if err != nil {
				UpdateOutcome(event, fmt.Errorf("backup location creation failed with %v", err))
				return
			}
			_, bkp_create_err := applicationbackup.CreateApplicationBackup(backupname, currbkNamespace, currBackupLocation)
			if bkp_create_err != nil {
				UpdateOutcome(event, fmt.Errorf("backup creation failed with %v", bkp_create_err))
			}
			bkp_comp_err := applicationbackup.WaitForAppBackupCompletion(backupname, currbkNamespace, timeout)
			if bkp_comp_err != nil {
				UpdateOutcome(event, fmt.Errorf("backup successful failed with %v", bkp_comp_err))
				return
			}
			log.InfoD("backup successful, backup name - %v, backup location - %v", backupname, backuplocationname)
		}
		updateMetrics(*event)
	})
}

func TriggerStorkAppBkpVolResize(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {
	defer endLongevityTest()
	startLongevityTest(StorkAppBkpVolResize)
	defer ginkgo.GinkgoRecover()
	log.InfoD("Stork Appplication Backup with volume resize triggered at: %v", time.Now())
	event := &EventRecord{
		Event: Event{
			ID:   GenerateUUID(),
			Type: AppTasksDown,
		},
		Start:   time.Now().Format(time.RFC1123),
		Outcome: []error{},
	}
	defer func() {
		event.End = time.Now().Format(time.RFC1123)
		*recordChan <- event
	}()
	setMetrics(*event)
	chaosLevel := ChaosMap[StorkAppBkpVolResize]

	var (
		s3SecretName   = "s3secret"
		timeout        = 5 * time.Minute
		taskNamePrefix = "stork-appbkp-volresize"
		appVolumes     []*volume.Volume
		requestedVols  []*volume.Volume
	)

	Step(fmt.Sprintf("Deploy applications for backup, with frequency: %v", chaosLevel), func() {

		// Write kubeconfig files after reading from the config maps created by torpedo deploy script
		err := asyncdr.WriteKubeconfigToFiles()
		if err != nil {
			UpdateOutcome(event, fmt.Errorf("unable to write kubeconfigs, getting error %v", err))
			return
		}
		err = SetSourceKubeConfig()
		if err != nil {
			UpdateOutcome(event, fmt.Errorf("getting error in setting source kubeconfig %v", err))
			return
		}
		UpdateOutcome(event, err)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			taskName := fmt.Sprintf("%s-%d", taskNamePrefix, i)
			log.Infof("Task name %s\n", taskName)
			appContexts := ScheduleApplications(taskName)
			*contexts = append(*contexts, appContexts...)
			ValidateApplications(*contexts)
			for _, ctx := range appContexts {
				currbkNamespace := GetAppNamespace(ctx, taskName)
				log.Infof("Backup applications, present in namespaces - %v", currbkNamespace)
				appVolumes, err = Inst().S.GetVolumes(ctx)
				log.Infof("len of app volumes is : %v", len(appVolumes))
				UpdateOutcome(event, err)
				if len(appVolumes) == 0 {
					UpdateOutcome(event, fmt.Errorf("found no volumes for app %s", ctx.App.Key))
				}
				log.Infof("Start backup application")
				taskNamePrefix = taskNamePrefix + fmt.Sprintf("-%d", i)
				backuplocationname := taskNamePrefix + "-location" + time.Now().Format("15h03m05s")
				backupname := taskNamePrefix + time.Now().Format("15h03m05s")
				currBackupLocation, err := applicationbackup.CreateBackupLocation(backuplocationname, currbkNamespace, s3SecretName)
				if err != nil {
					UpdateOutcome(event, fmt.Errorf("backup location creation failed with %v", err))
					return
				}
				_, bkp_create_err := applicationbackup.CreateApplicationBackup(backupname, currbkNamespace, currBackupLocation)
				if bkp_create_err != nil {
					UpdateOutcome(event, fmt.Errorf("backup creation failed with %v", bkp_create_err))
					return
				}
				bkp_start_err := applicationbackup.WaitForAppBackupToStart(backupname, currbkNamespace, timeout)
				if bkp_start_err == nil {
					log.Info("Application backup is in progress, starting volume resize")
					requestedVols, _ = Inst().S.ResizeVolume(ctx, "")
					log.Info("verify application backup successful")
					bkp_comp_err := applicationbackup.WaitForAppBackupCompletion(backupname, currbkNamespace, timeout)
					if bkp_comp_err != nil {
						UpdateOutcome(event, fmt.Errorf("backup successful failed with %v", bkp_comp_err))
						return
					}
					log.Info("application backup successful, verify volume resize successful")
					for _, v := range requestedVols {
						params := make(map[string]string)
						err := Inst().V.ValidateUpdateVolume(v, params)
						if err != nil {
							UpdateOutcome(event, fmt.Errorf("volume resize failed for %v volume", v))
						}
					}
				} else {
					UpdateOutcome(event, fmt.Errorf("backup start fail %v", bkp_start_err))
					return
				}
				log.InfoD("backup successful and volume resize injected during backup successfully, backup name - %v, backup location - %v", backupname, backuplocationname)
			}
			updateMetrics(*event)
		}
	})
}

func TriggerStorkAppBkpHaUpdate(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {
	defer endLongevityTest()
	startLongevityTest(StorkAppBkpHaUpdate)
	defer ginkgo.GinkgoRecover()
	log.InfoD("Stork Appplication Backup with HA update triggered at: %v", time.Now())
	event := &EventRecord{
		Event: Event{
			ID:   GenerateUUID(),
			Type: AppTasksDown,
		},
		Start:   time.Now().Format(time.RFC1123),
		Outcome: []error{},
	}
	defer func() {
		event.End = time.Now().Format(time.RFC1123)
		*recordChan <- event
	}()
	setMetrics(*event)
	chaosLevel := ChaosMap[StorkAppBkpHaUpdate]

	var (
		s3SecretName   = "s3secret"
		timeout        = 5 * time.Minute
		taskNamePrefix = "stork-appbkp-haupdate"
		appVolumes     []*volume.Volume
	)

	Step(fmt.Sprintf("Deploy applications for backup, with frequency: %v", chaosLevel), func() {

		// Write kubeconfig files after reading from the config maps created by torpedo deploy script
		err := asyncdr.WriteKubeconfigToFiles()
		if err != nil {
			UpdateOutcome(event, fmt.Errorf("unable to write kubeconfigs, getting error %v", err))
			return
		}
		err = SetSourceKubeConfig()
		if err != nil {
			UpdateOutcome(event, fmt.Errorf("getting error in setting source kubeconfig %v", err))
			return
		}
		UpdateOutcome(event, err)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			taskName := fmt.Sprintf("%s-%d", taskNamePrefix, i)
			log.Infof("Task name %s\n", taskName)
			appContexts := ScheduleApplications(taskName)
			*contexts = append(*contexts, appContexts...)
			ValidateApplications(*contexts)
			for _, ctx := range appContexts {
				currbkNamespace := GetAppNamespace(ctx, taskName)
				log.Infof("Backup applications, present in namespaces - %v", currbkNamespace)
				appVolumes, err = Inst().S.GetVolumes(ctx)
				log.Infof("len of app volumes is : %v", len(appVolumes))
				UpdateOutcome(event, err)
				if len(appVolumes) == 0 {
					UpdateOutcome(event, fmt.Errorf("found no volumes for app %s", ctx.App.Key))
					return
				}
				log.Infof("Start backup application")
				taskNamePrefix = taskNamePrefix + fmt.Sprintf("-%d", i)
				backuplocationname := taskNamePrefix + "-location" + time.Now().Format("15h03m05s")
				backupname := taskNamePrefix + time.Now().Format("15h03m05s")
				currBackupLocation, err := applicationbackup.CreateBackupLocation(backuplocationname, currbkNamespace, s3SecretName)
				if err != nil {
					UpdateOutcome(event, fmt.Errorf("backup location creation failed with %v", err))
					return
				}
				_, bkp_create_err := applicationbackup.CreateApplicationBackup(backupname, currbkNamespace, currBackupLocation)
				if bkp_create_err != nil {
					UpdateOutcome(event, fmt.Errorf("backup creation failed with %v", bkp_create_err))
					return
				}
				bkp_start_err := applicationbackup.WaitForAppBackupToStart(backupname, currbkNamespace, timeout)
				if bkp_start_err == nil {
					failure := false
					for _, v := range appVolumes {
						MaxRF := Inst().V.GetMaxReplicationFactor()
						log.Infof("Maximum replication factor is: %v\n", MaxRF)
						currAggr, err := Inst().V.GetAggregationLevel(v)
						if err != nil {
							UpdateOutcome(event, fmt.Errorf("failed to get aggregation level %v", bkp_create_err))
							failure = true
							return
						}
						log.Infof("Aggregation level is: %v\n", currAggr)
						if currAggr > 1 {
							MaxRF = int64(len(node.GetWorkerNodes())) / currAggr
						}
						currRep, err := Inst().V.GetReplicationFactor(v)
						if err != nil {
							UpdateOutcome(event, fmt.Errorf("failed to get replication factor %v", err))
							failure = true
							return
						}
						log.InfoD("Current replication factor is: %v\n", currRep)
						expRF := currRep + 1
						log.InfoD("Expected replication factor is: %v\n", expRF)
						if expRF > MaxRF {
							expRF = currRep - 1
							log.InfoD("as expRF is more than maxRF, will reduce the HA, new expected replication factor is %v", expRF)
						}
						err = Inst().V.SetReplicationFactor(v, expRF, nil, nil, true)
						if err != nil {
							UpdateOutcome(event, fmt.Errorf("failed to set replication factor %v", err))
							failure = true
							return
						}
						newRepl, err := Inst().V.GetReplicationFactor(v)
						if err != nil {
							UpdateOutcome(event, fmt.Errorf("failed to get replication factor %v", err))
							failure = true
							return
						}
						dash.VerifySafely(newRepl, expRF, fmt.Sprintf("validate repl update for volume %s", v.Name))
						log.InfoD("repl increase validation completed on volume %s", v.Name)
						log.InfoD("Reset replication factor on volume")
						err = Inst().V.SetReplicationFactor(v, currRep, nil, nil, true)
						if err != nil {
							UpdateOutcome(event, fmt.Errorf("failed to set replication factor %v", err))
							failure = true
							return
						}
					}
					if !failure {
						bkp_comp_err := applicationbackup.WaitForAppBackupCompletion(backupname, currbkNamespace, timeout)
						if bkp_comp_err != nil {
							UpdateOutcome(event, fmt.Errorf("backup completion failed with %v", bkp_comp_err))
							return
						}
						log.InfoD("backup successful and volume ha-update injected during backup successfully, backup name - %v, backup location - %v", backupname, backuplocationname)
					}
				} else {
					UpdateOutcome(event, fmt.Errorf("backup start fail %v", bkp_start_err))
					return
				}
				updateMetrics(*event)
			}
		}
	})
}

func TriggerStorkAppBkpPxRestart(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {
	defer endLongevityTest()
	startLongevityTest(StorkAppBkpPxRestart)
	defer ginkgo.GinkgoRecover()
	log.InfoD("Stork Appplication Backup with PX restart triggered at: %v", time.Now())
	event := &EventRecord{
		Event: Event{
			ID:   GenerateUUID(),
			Type: AppTasksDown,
		},
		Start:   time.Now().Format(time.RFC1123),
		Outcome: []error{},
	}
	defer func() {
		event.End = time.Now().Format(time.RFC1123)
		*recordChan <- event
	}()
	setMetrics(*event)
	chaosLevel := ChaosMap[StorkAppBkpPxRestart]

	var (
		s3SecretName   = "s3secret"
		timeout        = 5 * time.Minute
		taskNamePrefix = "stork-appbkp-pxrestart"
	)

	Step(fmt.Sprintf("Deploy applications for backup, with frequency: %v", chaosLevel), func() {

		// Write kubeconfig files after reading from the config maps created by torpedo deploy script
		err := asyncdr.WriteKubeconfigToFiles()
		if err != nil {
			UpdateOutcome(event, fmt.Errorf("unable to write kubeconfigs, getting error %v", err))
			return
		}
		err = SetSourceKubeConfig()
		if err != nil {
			UpdateOutcome(event, fmt.Errorf("getting error in setting source kubeconfig %v", err))
			return
		}
		UpdateOutcome(event, err)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			taskName := fmt.Sprintf("%s-%d", taskNamePrefix, i)
			log.Infof("Task name %s\n", taskName)
			appContexts := ScheduleApplications(taskName)
			*contexts = append(*contexts, appContexts...)
			ValidateApplications(*contexts)
			for _, ctx := range appContexts {
				currbkNamespace := GetAppNamespace(ctx, taskName)
				log.Infof("Backup applications, present in namespaces - %v", currbkNamespace)
				log.Infof("Start backup application")
				taskNamePrefix = taskNamePrefix + fmt.Sprintf("-%d", i)
				backuplocationname := taskNamePrefix + "-location" + time.Now().Format("15h03m05s")
				backupname := taskNamePrefix + time.Now().Format("15h03m05s")
				currBackupLocation, err := applicationbackup.CreateBackupLocation(backuplocationname, currbkNamespace, s3SecretName)
				if err != nil {
					UpdateOutcome(event, fmt.Errorf("backup location creation failed with %v", err))
					return
				}
				_, bkp_create_err := applicationbackup.CreateApplicationBackup(backupname, currbkNamespace, currBackupLocation)
				if bkp_create_err != nil {
					UpdateOutcome(event, fmt.Errorf("backup creation failed with %v", bkp_create_err))
					return
				}
				bkp_start_err := applicationbackup.WaitForAppBackupToStart(backupname, currbkNamespace, timeout)
				if bkp_start_err == nil {
					Step("Restart Portworx", func() {
						nodes := node.GetStorageDriverNodes()
						nodeIndex := rand.Intn(len(nodes))
						log.Infof("Stop volume driver [%s] on node: [%s]", Inst().V.String(), nodes[nodeIndex].Name)
						StopVolDriverAndWait([]node.Node{nodes[nodeIndex]})
						log.Infof("Starting volume driver [%s] on node [%s]", Inst().V.String(), nodes[nodeIndex].Name)
						StartVolDriverAndWait([]node.Node{nodes[nodeIndex]})
						log.Infof("Giving a few seconds for volume driver to stabilize")
						time.Sleep(20 * time.Second)
					})
				} else {
					UpdateOutcome(event, fmt.Errorf("backup start fail %v", bkp_start_err))
					return
				}
				log.InfoD("backup successful and px restart injected during backup successfully, backup name - %v, backup location - %v", backupname, backuplocationname)
			}
		}
		updateMetrics(*event)
	})
}

func prepareEmailBody(eventRecords emailData) (string, error) {
	var err error
	t := template.New("t").Funcs(templateFuncs)
	t, err = t.Parse(htmlTemplate)
	if err != nil {
		log.Errorf("Cannot parse HTML template Err: %v", err)
		return "", err
	}
	var buf []byte
	buffer := bytes.NewBuffer(buf)
	err = t.Execute(buffer, eventRecords)
	if err != nil {
		log.Errorf("Cannot generate body from values, Err: %v", err)
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

// createPureStorageClass create storage class
func createPureStorageClass(name string, params map[string]string) (*storageapi.StorageClass, error) {
	var reclaimPolicyDelete v1.PersistentVolumeReclaimPolicy
	var bindMode storageapi.VolumeBindingMode
	k8sStorage := storage.Instance()

	v1obj := meta_v1.ObjectMeta{
		Name: name,
	}
	reclaimPolicyDelete = v1.PersistentVolumeReclaimDelete
	bindMode = storageapi.VolumeBindingImmediate

	scObj := storageapi.StorageClass{
		ObjectMeta:        v1obj,
		Provisioner:       k8s.CsiProvisioner,
		Parameters:        params,
		ReclaimPolicy:     &reclaimPolicyDelete,
		VolumeBindingMode: &bindMode,
	}

	sc, err := k8sStorage.CreateStorageClass(&scObj)
	if err != nil {
		return nil, fmt.Errorf("failed to create CsiSnapshot storage class: %s.Error: %v", name, err)
	}
	return sc, err
}

func setMetrics(event EventRecord) {
	Inst().M.IncrementGaugeMetric(TestRunningState, event.Event.Type)
	Inst().M.IncrementCounterMetric(TotalTriggerCount, event.Event.Type)
	Inst().M.SetGaugeMetricWithNonDefaultLabels(FailedTestAlert, 0, event.Event.Type, "")
}

func updateMetrics(event EventRecord) {
	Inst().M.IncrementCounterMetric(TestPassedCount, event.Event.Type)
	Inst().M.DecrementGaugeMetric(TestRunningState, event.Event.Type)
}

func isPoolRebalanceEnabled(poolUUID string) bool {
	if autoPilotLabelNode.Name != "" {
		for _, pool := range autoPilotLabelNode.Pools {
			if pool.Uuid == poolUUID {
				return true
			}
		}
	}
	return false
}

func getReblanceCoolOffPeriod(triggerType string) int {
	var timePeriodInSeconds int

	t := ChaosMap[triggerType]

	baseInterval := 3600

	switch t {
	case 1:
		timePeriodInSeconds = baseInterval
	case 2:
		timePeriodInSeconds = 3 * baseInterval
	case 3:
		timePeriodInSeconds = 6 * baseInterval
	case 4:
		timePeriodInSeconds = 9 * baseInterval
	case 5:
		timePeriodInSeconds = 12 * baseInterval
	case 6:
		timePeriodInSeconds = 15 * baseInterval
	case 7:
		timePeriodInSeconds = 18 * baseInterval
	case 8:
		timePeriodInSeconds = 21 * baseInterval
	case 9:
		timePeriodInSeconds = 24 * baseInterval
	case 10:
		timePeriodInSeconds = 30 * baseInterval
	}
	return timePeriodInSeconds
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
   <td align="center"><h4>Node Status </h4></td>
   <td align="center"><h4>Cores </h4></td>
 </tr>
{{range .NodeInfo}}
<tr>
<td>{{ .MgmtIP }}</td>
<td>{{ .NodeName }}</td>
<td>{{ .PxVersion }}</td>
{{ if eq .Status "STATUS_OK"}}
<td bgcolor="green">{{ .Status }}</td>
{{ else }}
<td bgcolor="red">{{ .Status }}</td>
{{ end }}
{{ if eq .NodeStatus "True"}}
<td bgcolor="green">{{ .NodeStatus }}</td>
{{ else }}
<td bgcolor="red">{{ .NodeStatus }}</td>
{{ end }}
{{ if .Cores }}
<td bgcolor="red">1</td>
{{ else }}
<td>0</td>
{{ end }}
</tr>
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
   <td class="wrapper" width="200" align="center"><h4>Event </h4></td>
   <td align="center"><h4>Start Time </h4></td>
   <td align="center"><h4>End Time </h4></td>
   <td class="wrapper" width="600" align="center"><h4>Errors </h4></td>
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
