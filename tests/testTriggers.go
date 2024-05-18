package tests

import (
	"bytes"
	"container/ring"
	"fmt"
	"math"
	"math/rand"
	"net/url"
	"os"
	"os/exec"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"text/template"
	"time"

	"github.com/devans10/pugo/flasharray"
	oputil "github.com/libopenstorage/operator/pkg/util/test"
	"github.com/portworx/torpedo/drivers/scheduler/aks"
	"github.com/portworx/torpedo/drivers/scheduler/eks"
	"github.com/portworx/torpedo/drivers/scheduler/gke"
	"github.com/portworx/torpedo/drivers/scheduler/iks"
	"github.com/portworx/torpedo/pkg/osutils"

	volsnapv1 "github.com/kubernetes-csi/external-snapshotter/client/v6/apis/volumesnapshot/v1"
	snapv1 "github.com/kubernetes-incubator/external-storage/snapshot/pkg/apis/crd/v1"
	apios "github.com/libopenstorage/openstorage/api"
	opsapi "github.com/libopenstorage/openstorage/api"
	storkapi "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	storkv1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/onsi/ginkgo/v2"
	"github.com/pborman/uuid"
	api "github.com/portworx/px-backup-api/pkg/apis/v1"
	"github.com/portworx/sched-ops/k8s/apps"
	"github.com/portworx/sched-ops/k8s/core"
	"github.com/portworx/sched-ops/k8s/operator"
	storage "github.com/portworx/sched-ops/k8s/storage"
	storkops "github.com/portworx/sched-ops/k8s/stork"
	"github.com/portworx/sched-ops/task"
	"gopkg.in/natefinch/lumberjack.v2"
	appsapi "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	storageapi "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/portworx/torpedo/drivers/backup"
	"github.com/portworx/torpedo/drivers/monitor/prometheus"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/node/vsphere"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/portworx/torpedo/drivers/scheduler/k8s"
	"github.com/portworx/torpedo/drivers/scheduler/openshift"
	"github.com/portworx/torpedo/drivers/scheduler/spec"
	"github.com/portworx/torpedo/drivers/vcluster"
	"github.com/portworx/torpedo/drivers/volume"
	"github.com/portworx/torpedo/pkg/aetosutil"
	"github.com/portworx/torpedo/pkg/applicationbackup"
	"github.com/portworx/torpedo/pkg/asyncdr"
	"github.com/portworx/torpedo/pkg/aututils"
	"github.com/portworx/torpedo/pkg/email"
	"github.com/portworx/torpedo/pkg/errors"
	"github.com/portworx/torpedo/pkg/log"
	"github.com/portworx/torpedo/pkg/pureutils"
	"github.com/portworx/torpedo/pkg/stats"
	"github.com/portworx/torpedo/pkg/units"
)

const (
	from = "wilkins@portworx.com"

	// EmailRecipientsConfigMapField is field in config map whose value is comma
	// seperated list of email IDs which will receive email notifications about longevity
	EmailRecipientsConfigMapField = "emailRecipients"
	// DefaultEmailRecipient is list of email IDs that will receive email
	// notifications when no EmailRecipientsConfigMapField field present in configMap
	DefaultEmailRecipient = "test@portworx.com"
	// SendGridEmailAPIKeyField is field in config map which stores the SendGrid Email API key
	SendGridEmailAPIKeyField = "sendGridAPIKey"
	// EmailHostServerField is field in configmap which stores the server address
	EmailHostServerField = "emailHostServer"
	// EmailSubjectField is field in configmap which stores the subject(optional)
	EmailSubjectField = "emailSubject"
	// CreatedBeforeTimeForNsField is field which stores number of hours from now, used for deletion of old NS
	CreatedBeforeTimeForNsField = "createdbeforetimefornsfield"
	// MigrationIntervalField is a field in configmap which stores interval for schedule policy(optional)
	MigrationIntervalField = "migrationinterval"
	// MigrationsCountField is a field in configmap which stores no. of migrations to be run(optional)
	MigrationsCountField = "migrationscount"
	// UpgradeEndpoints is a field in the configmap that contains a comma-separated list of upgrade endpoints
	UpgradeEndpoints = "upgradeEndpoints"
	// SchedUpgradeHops is a field in the configmap that contains a comma-separated list of scheduler upgrade hops
	SchedUpgradeHops = "schedUpgradeHops"
	// BaseInterval is a field in the configmap that represents base interval in minutes
	BaseInterval = "baseInterval"
	// VclusterFioRunTimeField is a field in the configmap that represents fio run time in seconds
	VclusterFioRunTimeField = "vclusterFioRunTime"
	// VclusterFioTotalIteration is a field in the configmap that represents number of iterations to be performed
	VclusterFioTotalIterationField = "vclusterFioTotalIteration"
	// VclusterFioParallelAppsField is a field in the configmap that represents number of parallel fio apps to be run in vcluster
	VclusterFioParallelAppsField = "vclusterFioParallelApps"
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
	validateReplicationUpdateTimeout = 4 * time.Hour
	errorChannelSize                 = 50
	snapshotScheduleRetryInterval    = 2 * time.Minute
	snapshotScheduleRetryTimeout     = 60 * time.Minute
)

const (
	pxStatusError  = "ERROR GETTING PX STATUS"
	pxVersionError = "ERROR GETTING PX VERSION"
)

type ErrorInjection string

const (
	CRASH      ErrorInjection = "crash"
	PX_RESTART ErrorInjection = "px_restart"
	REBOOT     ErrorInjection = "reboot"
)

const (
	deploymentCount      = 250
	deploymentNamePrefix = "fada-scale-dep"
	pvcNamePrefix        = "fada-scale-pvc"
	fadaNamespacePrefix  = "fada-namespace"
	podAttachTimeout     = 15 * time.Minute
)

// TODO Need to add for AutoJournal
//var IOProfileChange = [4]apios.IoProfile{apios.IoProfile_IO_PROFILE_NONE, apios.IoProfile_IO_PROFILE_AUTO_JOURNAL, apios.IoProfile_IO_PROFILE_AUTO, apios.IoProfile_IO_PROFILE_DB_REMOTE}

var IOProfileChange = [3]apios.IoProfile{apios.IoProfile_IO_PROFILE_NONE, apios.IoProfile_IO_PROFILE_AUTO, apios.IoProfile_IO_PROFILE_DB_REMOTE}

var currentIOProfileChangeCounter = 0

var longevityLogger *lumberjack.Logger

// EmailRecipients list of email IDs to send email to
var EmailRecipients []string

// EmailServer to use for sending email
var EmailServer string

// EmailSubject to use for sending email
var EmailSubject string

// CreatedBeforeTimeforNS to use for number of hours elapsed to get age of NS
var CreatedBeforeTimeforNS int

// MigrationInterval to use for defining schedule policy for migrations
var MigrationInterval int

// MigrationsCount to use for number of migrations to be run
var MigrationsCount int

// RunningTriggers map of events and corresponding interval
var RunningTriggers map[string]time.Duration

// ChaosMap stores mapping between test trigger and its chaos level.
var ChaosMap map[string]int

// coresMap stores mapping between node name and cores generated.
var coresMap map[string]string

// SendGridEmailAPIKey holds API key used to interact
// with SendGrid Email APIs
var SendGridEmailAPIKey string

// backupCounter holds the iteration of TriggerBackup
var backupCounter = 0

// restoreCounter holds the iteration of TriggerRestore
var restoreCounter = 0

// newNamespaceCounter holds the count of current namespace
var newNamespaceCounter = 0

// jiraEvents to store raised jira events data
var jiraEvents = make(map[string][]string)

// isAutoFsTrimEnabled to store if auto fs trim enabled
var isAutoFsTrimEnabled = false

// setIoPriority to set IOPriority
var setIoPriority = true

// isCsiVolumeSnapshotClassExist to store if snapshot class exist
var isCsiVolumeSnapshotClassExist = false

// isCsiRestoreStorageClassExist to store if restore storage class exist
var isCsiRestoreStorageClassExist = false

// isRelaxedReclaimEnabled to store if relaxed reclaim enabled
var isRelaxedReclaimEnabled = false

// isTrashcanEnabled to store if trashcan enabled
var isTrashcanEnabled = false

// volSnapshotClass is snapshot class for FA volumes
var volSnapshotClass *volsnapv1.VolumeSnapshotClass

// pureStorageClassMap is map of pure storage class
var pureStorageClassMap map[string]*storageapi.StorageClass

// DefaultSnapshotRetainCount is default snapshot retain count
var DefaultSnapshotRetainCount = 10

// TotalTriggerCount is counter metric for test trigger
var TotalTriggerCount = prometheus.TorpedoTestTotalTriggerCount

// TestRunningState is gauge metric for test method running
var TestRunningState = prometheus.TorpedoTestRunning

// TestFailedCount is counter metric for test failed
var TestFailedCount = prometheus.TorpedoTestFailCount

// TestPassedCount is counter metric for test passed
var TestPassedCount = prometheus.TorpedoTestPassCount

// FailedTestAlert is a flag to alert test failed
var FailedTestAlert = prometheus.TorpedoAlertTestFailed

// FioRunTime is default fio run time in seconds
var VclusterFioRunTime = "86400"

// VclusterFioTotalIteration is the number of iteration  Fio to be run in vcluster
var VclusterFioTotalIteration = "1"

// VclusterParallelApps is the number of parallel apps to be run in vcluster
var VclusterFioParallelApps = "2"

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
var autoPilotRuleCreated bool

var cloudsnapMap = make(map[string]map[*volume.Volume]*storkv1.ScheduledVolumeSnapshotStatus)

// Counter is a thread-safe generic counter for keys of a comparable type
type Counter[K comparable] struct {
	sync.RWMutex
	countMap map[K]int
}

// Increment increases the count for the specified key by 1
func (c *Counter[K]) Increment(key K) {
	c.Lock()
	defer c.Unlock()
	c.countMap[key]++
}

// GetCount retrieves the count for the specified key
func (c *Counter[K]) GetCount(key K) int {
	c.RLock()
	defer c.RUnlock()
	return c.countMap[key]
}

// String returns a string representation of the count map
func (c *Counter[K]) String() string {
	c.RLock()
	defer c.RUnlock()
	return fmt.Sprintf("%v", c.countMap)
}

// NewCounter creates a new Counter instance
func NewCounter[K comparable]() *Counter[K] {
	return &Counter[K]{countMap: make(map[K]int)}
}

// TestExecutionCounter holds the count of executions for each test
var TestExecutionCounter = NewCounter[string]()

// emailRecords stores events for rendering
// email template
type emailRecords struct {
	Records []EventRecord
}

type emailData struct {
	MasterIP     []string
	DashboardURL string
	NodeInfo     []nodeInfo
	EmailRecords emailRecords
	TriggersInfo []triggerInfo
	MailSubject  string
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

type VolumeIOProfile struct {
	SpecInfo *volume.Volume
	Profile  apios.IoProfile
}

type storageClassOption func(*storageapi.StorageClass)

// GenerateUUID generates unique ID
func GenerateUUID() string {
	uuidbyte, _ := exec.Command("uuidgen").Output()
	return strings.TrimSpace(string(uuidbyte))
}

// UpdateOutcome updates outcome based on error
func UpdateOutcome(event *EventRecord, err error) {

	if err != nil {
		if event != nil {
			Inst().M.IncrementCounterMetric(TestFailedCount, event.Event.Type)
			actualEvent := strings.Split(event.Event.Type, "<br>")[0]
			log.Errorf("Event [%s] failed with error: %v", actualEvent, err)
			dash.VerifySafely(err, nil, fmt.Sprintf("verify if error occured for event %s", event.Event.Type))
			er := fmt.Errorf(err.Error() + "<br>")
			Inst().M.IncrementGaugeMetricsUsingAdditionalLabel(FailedTestAlert, event.Event.Type, err.Error())
			event.Outcome = append(event.Outcome, er)
			createLongevityJiraIssue(event, er)
		} else {
			log.FailOnError(err, "error in validation")
		}

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
	// ValidatePdsApps checks the healthstate of the pds apps
	ValidatePdsApps = "validatePdsApps"
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
	// RestartKvdbVolDriver restarts kvdb volume driver
	RestartKvdbVolDriver = "restartKvdbVolDriver"
	// CrashVolDriver crashes volume driver
	CrashVolDriver = "crashVolDriver"
	// CrashPXDaemon crashes px daemon service
	CrashPXDaemon = "crashPXDaemon"
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
	// VolumesDelete deletes the columns of the context
	VolumesDelete = "volumesDelete"
	// CloudSnapShot takes cloud snapshot of the volumes
	CloudSnapShot = "cloudSnapShot"
	//CloudSnapShotRestore in-place cloudsnap restores
	CloudSnapShotRestore = "cloudSnapShotRestore"
	// LocalSnapShot takes local snapshot of the volumes
	LocalSnapShot = "localSnapShot"
	// LocalSnapShotRestore restores local snapshot of the volumes
	LocalSnapShotRestore = "localSnapShotRestore"
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
	// MetadataPoolResizeDisk resize storage pool using resize-disk
	MetadataPoolResizeDisk = "metadatapoolResizeDisk"
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
	BackupRestartPortworx = "backupRestartPX"
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
	// UpgradeVolumeDriverFromCatalog upgrades volume driver from catalog according to the Inst().UpgradeStorageDriverEndpointList
	UpgradeVolumeDriverFromCatalog = "upgradeVolumeDriverFromCatalog"
	// UpgradeCluster upgrades the cluster according to the Inst().SchedUpgradeHops
	UpgradeCluster = "upgradeCluster"
	// AppTasksDown scales app up and down
	AppTasksDown = "appScaleUpAndDown"
	// AutoFsTrim enables Auto Fstrim in PX cluster
	AutoFsTrim = "autoFsTrim"
	// UpdateVolume provides option to update volume with properties like iopriority.
	UpdateVolume    = "updateVolume"
	UpdateIOProfile = "updateIOProfile"
	PowerOffAllVMs = "powerOffAllVMs"
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
	// MetroDR runs Metro DR between two clusters
	MetroDR = "metrodr"
	// MetroDRMigrationSchedule runs Metro DR migration schedule between two clusters
	MetroDRMigrationSchedule = "metrodrmigrationschedule"
	// AsyncDR runs Async DR between two clusters
	AsyncDR = "asyncdr"
	// AsyncDR PX restart on source runs Async DR migration between two clusters with px restart
	AsyncDRPXRestartSource = "asyncdrpxrestartsource"
	// AsyncDR PX restart on destination runs Async DR migration between two clusters with px restart
	AsyncDRPXRestartDest = "asyncdrpxrestartdest"
	// AsyncDR PX restart on destination runs Async DR migration between two clusters with kvdb restart
	AsyncDRPXRestartKvdb = "asyncdrpxrestartkvdb"
	// AsyncDRMigrationSchedule runs AsyncDR Migrationschedule between two clusters
	AsyncDRMigrationSchedule = "asyncdrmigrationschedule"
	// ConfluentAsyncDR runs Async DR between two clusters for Confluent kafka CRD
	ConfluentAsyncDR = "confluentasyncdr"
	// KafkaAsyncDR runs Async DR between two clusters for kafka CRD
	KafkaAsyncDR = "kafkaasyncdr"
	// MongoAsyncDR runs Async DR between two clusters for kafka CRD
	MongoAsyncDR = "mongoasyncdr"
	// AsyncDR Volume Only runs Async DR volume only migration between two clusters
	AsyncDRVolumeOnly = "asyncdrvolumeonly"
	// AutoFsTrimAsyncDR runs Async DR of volumes with FSTRIM=true parameter and validates it exists on DR as well
	AutoFsTrimAsyncDR = "autofstrimasyncdr"
	// IopsBwAsyncDR runs Async DR of volumes with volumes having iothrottle set
	IopsBwAsyncDR = "iopsbwasyncdr"
	// stork application backup runs stork backups for applications
	StorkApplicationBackup = "storkapplicationbackup"
	// stork application backup volume resize runs stork backups for applications and inject volume resize in between
	StorkAppBkpVolResize = "storkappbkpvolresize"
	// stork application backup volume resize runs stork backups for applications and inject volume ha-update in between
	StorkAppBkpHaUpdate = "storkappbkphaupdate"
	// stork application backup runs with px restart
	StorkAppBkpPxRestart = "storkappbkppxrestart"
	// stork application backup runs with pool resize
	StorkAppBkpPoolResize = "storkappbkppoolresize"
	// HAIncreaseAndReboot performs repl-add
	HAIncreaseAndReboot = "haIncreaseAndReboot"
	// HAIncreaseAndRestartPX performs repl-add and restart PX
	HAIncreaseAndRestartPX = "haIncreaseAndRestartPX"
	// HAIncreaseAndCrashPX performs repl-add and crash PX
	HAIncreaseAndCrashPX = "haIncreaseAndCrashPX"
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
	// DeleteOldNamespaces Performs deleting old NS which has age greater than specified in configmap
	DeleteOldNamespaces = "deleteoldnamespaces"

	// Volume update repl size and resize volume on aggregated volumes
	AggrVolDepReplResizeOps = "aggrVolDepReplResizeOps"

	// Add Drive to create new pool and resize Drive in maintenance mode
	AddResizePoolMaintenance = "addResizePoolInMaintenance"
	//AddStorageNode adds storage node to existing OCP set up
	AddStorageNode = "addStorageNode"
	//AddStoragelessNode adds storageless node to existing OCP set up
	AddStoragelessNode = "addStoragelessNode"
	//OCPStorageNodeRecycle recycle Storageless to Storagenode
	OCPStorageNodeRecycle = "ocpNodeStorageRecycle"
	// NodeMaintenanceCycle performs node maintenance enter and exit
	NodeMaintenanceCycle = "nodeMaintenanceCycle"
	// PoolMaintenanceCycle performs pool maintenance enter and exit
	PoolMaintenanceCycle = "poolMaintenanceCycle"
	//StorageFullPoolExpansion performs pool expansion of storage full pools
	StorageFullPoolExpansion = "storageFullPoolExpansion"

	// HAIncreaseWithPVCResize performs repl-add and resize PVC at same time
	HAIncreaseWithPVCResize = "haIncreaseWithPVCResize"

	// ReallocateSharedMount reallocated shared mount volumes
	ReallocateSharedMount = "reallocateSharedMount"

	// AddBackupCluster adds source and destination cluster
	AddBackupCluster = "pxbAddBackupCluster"

	//SetupBackupBucketAndCreds add creds and adds bucket for backup
	SetupBackupBucketAndCreds = "pxbSetupBackupBucketAndCreds"

	//SetupBackupLockedBucketAndCreds add creds and adds locked bucket for backup
	SetupBackupLockedBucketAndCreds = "pxbSetupBackupLockedBucketAndCreds"

	//DeployBackupApps deploys backup application
	DeployBackupApps = "pxbDeployBackupApps"

	//CreatePxBackup creates backup for longevity
	CreatePxBackup = "pxbCreatePxBackup"

	//CreatePxLockedBackup creates locked backup for longevity
	CreatePxLockedBackup = "pxbCreatePxLockedBackup"

	//CreatePxBackupAndRestore creates backup and Restores the backup
	CreatePxBackupAndRestore = "pxbCreateBackupAndRestore"

	//CreateRandomRestore creates backup and Restores the backup
	CreateRandomRestore = "pxbCreateRandomRestore"

	// CreateAndRunFioOnVcluster creates and runs fio on vcluster
	CreateAndRunFioOnVcluster = "createAndRunFioOnVcluster"

	//CreateAndRunMultipleFioOnVcluster creates and runs multiple fio on vcluster
	CreateAndRunMultipleFioOnVcluster = "createAndRunMultipleFioOnVcluster"

	//VolumeDriverDownVCluster creates and runs fio on vcluster and volume driver down
	VolumeDriverDownVCluster = "VolumeDriverDownVCluster"

	// SetDiscardMounts Sets and resets discard cluster wide options on the cluster
	SetDiscardMounts = "SetDiscardMounts"

	// ResetDiscardMounts Sets and resets discard cluster wide options on the cluster
	ResetDiscardMounts = "ResetDiscardMounts"

	// ScaleFADAVolumeAttach create and attach FADA volumes at scale
	ScaleFADAVolumeAttach = "ScaleFADAVolumeAttach"
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

	Step("checking for core files...", func() {
		Step("verifying if core files are present on each node", func() {
			log.InfoD("verifying if core files are present on each node")
			nodes := node.GetStorageDriverNodes()
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
	PrintPxctlStatus()
}
func endLongevityTest() {
	PrintPxctlStatus()
	dash.TestCaseEnd()
	CloseLogger(longevityLogger)
}

func updateLongevityStats(name, eventStatName string, dashStats map[string]string) {
	name = strings.Split(name, "<br>")[0] //discarding the extra strings attached to name if any
	version, err := Inst().V.GetDriverVersion()
	product := "px-enterprise"
	if eventStatName == stats.AsyncDREventName || eventStatName == stats.MetroDREventName || eventStatName == stats.StorkApplicationBackupEventName {
		version, err = asyncdr.GetStorkVersion()
		product = "stork"
	}
	if err != nil {
		log.Errorf("error getting px version. err: %+v", err)
	}
	dashStats["node-driver"] = Inst().N.String()
	dashStats["scheduler-driver"] = Inst().S.String()
	eventStat := &stats.EventStat{
		EventName: eventStatName,
		EventTime: time.Now().Format(time.RFC1123),
		Version:   version,
		DashStats: dashStats,
	}
	stats.PushStatsToAetos(dash, name, product, "Longevity", eventStat)
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
	dashStats := make(map[string]string)
	dashStats["app-list"] = strings.Join(Inst().AppList, ", ")
	dashStats["scale-factor"] = fmt.Sprintf("%d", Inst().GlobalScaleFactor)
	updateLongevityStats(DeployApps, stats.DeployAppsEventName, dashStats)
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
			errorChan = make(chan error, errorChannelSize)
			ValidateContext(ctx, &errorChan)
			for err := range errorChan {
				log.Infof("Error: %v", err)
				UpdateOutcome(event, err)
			}
		}

		if len(*contexts) > 2 {
			mid := len(*contexts) / 2
			for i, ctx := range *contexts {
				appVolumes, err := Inst().S.GetVolumes(ctx)
				UpdateOutcome(event, err)
				var volumeSpecUpdate *opsapi.VolumeSpecUpdate
				if i < mid {
					volumeSpecUpdate = &opsapi.VolumeSpecUpdate{
						IoThrottleOpt: &opsapi.VolumeSpecUpdate_IoThrottle{
							IoThrottle: &opsapi.IoThrottle{
								ReadBwMbytes:  10,
								WriteBwMbytes: 10,
							},
						},
					}
				} else {
					volumeSpecUpdate = &opsapi.VolumeSpecUpdate{
						IoThrottleOpt: &opsapi.VolumeSpecUpdate_IoThrottle{
							IoThrottle: &opsapi.IoThrottle{
								ReadIops:  1024,
								WriteIops: 1024,
							},
						},
					}
				}
				for _, appVol := range appVolumes {
					log.Infof(fmt.Sprintf("updating volume %s [app:%s] with volume spec: %+v", appVol.Name, ctx.App.Key, volumeSpecUpdate))
					err = Inst().V.UpdateVolumeSpec(appVol, volumeSpecUpdate)
				}
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
			// TODO: remove this retry once PWX-27773 is fixed
			t := func() (interface{}, bool, error) {
				cVol, err = Inst().V.InspectVolume(vol)
				if err != nil {
					return cVol, true, fmt.Errorf("error inspecting volume %s, err : %v", vol, err)
				}

				if !strings.Contains(cVol.DevicePath, "pxd/") {
					return cVol, true, fmt.Errorf("path %s is not correct", cVol.DevicePath)
				}
				// It is noted that the DevicePath is intermittently empty.
				// This check ensures the device path is not empty for attached volumes
				if cVol.State == apios.VolumeState_VOLUME_STATE_ATTACHED && cVol.AttachedState == apios.AttachState_ATTACH_STATE_EXTERNAL && cVol.DevicePath == "" {
					return cVol, false, fmt.Errorf("device path is not present for volume: %s", vol)
				}
				return cVol, true, err
			}

			_, err := task.DoRetryWithTimeout(t, 10*time.Minute, 10*time.Second)
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
	haIncreaseWithErrorInjection(event, contexts, REBOOT)

}

// TriggerHAIncreaseAndPXRestart triggers repl increase and restart PX on target and source nodes
func TriggerHAIncreaseAndPXRestart(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {
	defer ginkgo.GinkgoRecover()
	defer endLongevityTest()
	startLongevityTest(HAIncreaseAndRestartPX)
	event := &EventRecord{
		Event: Event{
			ID:   GenerateUUID(),
			Type: HAIncreaseAndRestartPX,
		},
		Start:   time.Now().Format(time.RFC1123),
		Outcome: []error{},
	}

	defer func() {
		event.End = time.Now().Format(time.RFC1123)
		*recordChan <- event
	}()

	setMetrics(*event)
	haIncreaseWithErrorInjection(event, contexts, PX_RESTART)

}

// TriggerHAIncreaseAndCrashPX triggers repl increase and crash PX on target and source nodes
func TriggerHAIncreaseAndCrashPX(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {
	defer ginkgo.GinkgoRecover()
	defer endLongevityTest()
	startLongevityTest(HAIncreaseAndCrashPX)
	event := &EventRecord{
		Event: Event{
			ID:   GenerateUUID(),
			Type: HAIncreaseAndCrashPX,
		},
		Start:   time.Now().Format(time.RFC1123),
		Outcome: []error{},
	}

	defer func() {
		event.End = time.Now().Format(time.RFC1123)
		*recordChan <- event
	}()

	setMetrics(*event)
	haIncreaseWithErrorInjection(event, contexts, CRASH)

}

func haIncreaseWithErrorInjection(event *EventRecord, contexts *[]*scheduler.Context, errorInj ErrorInjection) {
	//Reboot target node and source node while repl increase is in progress
	stepLog := "get a volume to  increase replication factor and reboot source  and target node"
	Step(stepLog, func() {
		log.InfoD("get a volume to  increase replication factor and reboot source  and target node")
		storageNodeMap := make(map[string]node.Node)
		storageNodes, err := GetStorageNodes()
		UpdateOutcome(event, err)

		for _, n := range storageNodes {
			//selecting healthy nodes for repl add operation
			err = isNodeHealthy(n, event.Event.Type)
			if err != nil {
				UpdateOutcome(event, err)
				continue
			}
			storageNodeMap[n.Id] = n
		}

		var selctx *scheduler.Context

		for _, ctx := range *contexts {
			var appVolumes []*volume.Volume
			stepLog = fmt.Sprintf("get volumes for %s app", ctx.App.Key)
			Step(stepLog, func() {
				log.InfoD(stepLog)
				appVolumes, err = Inst().S.GetVolumes(ctx)
				UpdateOutcome(event, err)
				if len(appVolumes) == 0 {
					UpdateOutcome(event, fmt.Errorf("found no volumes for app %s", ctx.App.Key))
				}
			})
			for _, v := range appVolumes {
				// Check if volumes are Pure FA/FB DA volumes
				isPureVol, err := Inst().V.IsPureVolume(v)
				if err != nil {
					UpdateOutcome(event, err)
				}
				if isPureVol {
					log.Warnf("Repl increase on Pure DA Volume [%s] not supported. Skipping this operation", v.Name)
					continue
				}

				size, err := GetVolumeConsumedSize(*v)
				if err != nil {
					UpdateOutcome(event, err)
					continue
				}

				if size < 50 {
					continue
				}
				selctx = ctx
				break
			}
		}

		if selctx != nil {
			var appVolumes []*volume.Volume
			stepLog = fmt.Sprintf("get volumes for %s app", selctx.App.Key)
			Step(stepLog, func() {
				log.InfoD(stepLog)
				appVolumes, err = Inst().S.GetVolumes(selctx)
				UpdateOutcome(event, err)
				if len(appVolumes) == 0 {
					UpdateOutcome(event, fmt.Errorf("found no volumes for app %s", selctx.App.Key))
					return
				}
			})

			initialRepls := make(map[*volume.Volume]int64)
			opts := volume.Options{
				ValidateReplicationUpdateTimeout: validateReplicationUpdateTimeout,
			}
			for _, v := range appVolumes {
				currRep, err := Inst().V.GetReplicationFactor(v)
				UpdateOutcome(event, err)
				replStatus, err := GetVolumeReplicationStatus(v)
				if err != nil {
					log.Error(err)
					UpdateOutcome(event, err)
					continue
				}
				if replStatus != "Up" {
					continue
				}
				initialRepls[v] = currRep

				if currRep != 0 {
					for {
						//Changing replication factor to 1
						if currRep > 1 {
							log.Infof("Current replication is > 1, reducing it before proceeding")

							dashStats := make(map[string]string)
							dashStats["volume-name"] = v.Name
							dashStats["curr-repl-factor"] = strconv.FormatInt(currRep, 10)
							dashStats["new-repl-factor"] = strconv.FormatInt(currRep-1, 10)
							updateLongevityStats(event.Event.Type, stats.HADecreaseEventName, dashStats)
							err = Inst().V.SetReplicationFactor(v, currRep-1, nil, nil, true, opts)
							if err != nil {
								log.Errorf("There is an error decreasing repl [%v]", err.Error())
								UpdateOutcome(event, err)
								return
							}

							log.Infof("waiting for 5 mins for data to deleted completely")
							time.Sleep(5 * time.Minute)
							currRep, err = Inst().V.GetReplicationFactor(v)
							if err != nil {
								log.Errorf("There is an error getting  repl  factor for vol [%s],err:[%v]", v.Name, err.Error())
								UpdateOutcome(event, err)
								return
							}
						} else {
							break
						}
					}
				}
				if err == nil {
					err := HaIncreaseErrorInjectionTargetNode(event, selctx, v, storageNodeMap, errorInj)
					log.Error(err)
					log.Debugf("Printing the volume inspect for the volume:%s ,volID:%s and namespace:%s after HaIncreaseErrorInjectionTargetNode ", v.Name, v.ID, v.Namespace)
					PrintInspectVolume(v.ID)
					UpdateOutcome(event, err)
					err = HaIncreaseErrorInjectSourceNode(event, selctx, v, storageNodeMap, errorInj)
					log.Error(err)
					log.Debugf("Printing the volume inspect for the volume:%s ,volID:%s and namespace:%s after HaIncreaseErrorInjectSourceNode ", v.Name, v.ID, v.Namespace)
					PrintInspectVolume(v.ID)
					UpdateOutcome(event, err)
				}
			}

			//Reverting back the initial replication factor
			for v, actualRep := range initialRepls {
				currRep, err := Inst().V.GetReplicationFactor(v)
				UpdateOutcome(event, err)
				for {
					if currRep > actualRep {
						err = Inst().V.SetReplicationFactor(v, currRep-1, nil, nil, true, opts)
						if err != nil {
							log.Errorf("There is an error decreasing repl [%v]", err.Error())
							UpdateOutcome(event, err)
							break
						}
						currRep, err = Inst().V.GetReplicationFactor(v)
						UpdateOutcome(event, err)
					} else {
						break
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
					continue
				}
				if isPureVol {
					log.Warnf("Repl increase on Pure DA Volume [%s] not supported. Skipping this operation", v.Name)
					continue
				}
				replStatus, err := GetVolumeReplicationStatus(v)
				if err != nil {
					UpdateOutcome(event, err)
					continue
				}
				if replStatus != "Up" {
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

						//Calculating Max Replication Factor allowed
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
							dashStats := make(map[string]string)
							dashStats["volume-name"] = v.Name
							dashStats["curr-repl-factor"] = strconv.FormatInt(currRep, 10)
							dashStats["new-repl-factor"] = strconv.FormatInt(expRF, 10)
							updateLongevityStats(HAIncrease, stats.HAIncreaseEventName, dashStats)
							err = Inst().V.SetReplicationFactor(v, expRF, nil, nil, true, opts)
							if err != nil {
								log.Debugf("Printing the volume inspect for the volume:%s ,volID:%s and namespace:%s", v.Name, v.ID, v.Namespace)
								PrintInspectVolume(v.ID)
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
				ctx.SkipVolumeValidation = false
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

// TriggerHAIncreasWithPVCResize performs repl-add on all volumes of given contexts and do pvc resize
func TriggerHAIncreasWithPVCResize(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {
	defer ginkgo.GinkgoRecover()
	defer endLongevityTest()
	startLongevityTest(HAIncreaseWithPVCResize)
	event := &EventRecord{
		Event: Event{
			ID:   GenerateUUID(),
			Type: HAIncreaseWithPVCResize,
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

			volumeResize := func(vol *volume.Volume) error {

				apiVol, err := Inst().V.InspectVolume(vol.ID)
				if err != nil {
					return err
				}

				curSize := apiVol.Spec.Size
				newSize := curSize + (uint64(10) * units.GiB)
				log.Infof("Initiating volume size increase on volume [%v] by size [%v] to [%v]",
					vol.ID, curSize/units.GiB, newSize/units.GiB)

				err = Inst().V.ResizeVolume(vol.ID, newSize)
				if err != nil {
					log.Debugf("Printing the volume inspect for the volume:%s ,volID:%s and namespace:%s after failure of resizing the volume", vol.Name, vol.ID, vol.Namespace)
					PrintInspectVolume(vol.ID)
					return err
				}

				// Wait for 2 seconds for Volume to update stats
				time.Sleep(2 * time.Second)
				volumeInspect, err := Inst().V.InspectVolume(vol.ID)
				if err != nil {
					return err
				}

				updatedSize := volumeInspect.Spec.Size
				if updatedSize <= curSize {
					return fmt.Errorf("volume did not update from [%v] to [%v] ",
						curSize/units.GiB, updatedSize/units.GiB)
				}

				return nil
			}
			for _, v := range appVolumes {
				// Check if volumes are Pure FA/FB DA volumes
				isPureVol, err := Inst().V.IsPureVolume(v)
				if err != nil {
					UpdateOutcome(event, err)
					continue
				}
				if isPureVol {
					log.Warnf("Repl increase on Pure DA Volume [%s] not supported. Skipping this operation", v.Name)
					continue
				}
				replStatus, err := GetVolumeReplicationStatus(v)
				if err != nil {
					UpdateOutcome(event, err)
					continue
				}

				if replStatus != "Up" {
					continue
				}
				stepLog = fmt.Sprintf("repl increase volume driver %s on app %s's volume: %v",
					Inst().V.String(), ctx.App.Key, v)
				Step(stepLog,
					func() {
						log.InfoD(stepLog)

						currRep, err := Inst().V.GetReplicationFactor(v)
						if err != nil {
							UpdateOutcome(event, err)
							return
						}

						if currRep == 1 {
							log.Warnf("skipping vol [%s] with repl factor 1", v.Name)
							return
						}

						log.Infof("Current replication is > 1, reducing it before proceeding")

						err = Inst().V.SetReplicationFactor(v, currRep-1, nil, nil, true, opts)
						if err != nil {
							log.Errorf("There is an error decreasing repl [%v]", err.Error())
							UpdateOutcome(event, err)
						}

						log.Infof("waiting for 5 mins for data to deleted completely")
						time.Sleep(5 * time.Minute)

						log.InfoD("Expected Replication factor for [%s] is %d", v.Name, currRep)

						expReplMap[v] = currRep

						if strings.Contains(ctx.App.Key, fastpathAppName) {
							newFastPathNode, err := AddFastPathLabel(ctx)
							if err == nil {
								defer Inst().S.RemoveLabelOnNode(*newFastPathNode, k8s.NodeType)
							}
							UpdateOutcome(event, err)
						}
						replSets, err := Inst().V.GetReplicaSets(v)
						if err != nil {
							log.Errorf("Replica Set before ha-increase : %+v", replSets[0].Nodes)
							UpdateOutcome(event, err)
						}
						err = Inst().V.SetReplicationFactor(v, currRep, nil, nil, false, opts)
						if err != nil {
							log.Debugf("Printing the volume inspect for the volume:%s ,volID:%s and namespace:%s after failing to set replication", v.Name, v.ID, v.Namespace)
							PrintInspectVolume(v.ID)
							log.Errorf("There is a error setting repl [%v]", err.Error())
							UpdateOutcome(event, err)
						}
						err = volumeResize(v)
						if err != nil {
							log.Debugf("Printing the volume inspect for the volume:%s ,volID:%s and namespace:%s after failure of a volume resize", v.Name, v.ID, v.Namespace)
							PrintInspectVolume(v.ID)
						}
						UpdateOutcome(event, err)

					})
				stepLog = fmt.Sprintf("validate successful repl increase on app %s's volume: %v",
					ctx.App.Key, v)
				Step(stepLog,
					func() {
						log.InfoD(stepLog)
						err = ValidateReplFactorUpdate(v, expReplMap[v])
						if err != nil {
							err = fmt.Errorf("error in ha-increse after  source node reboot. Error: %v", err)
							log.Error(err)
							UpdateOutcome(event, err)
						} else {
							dash.VerifySafely(true, true, fmt.Sprintf("repl successfully increased to %d", expReplMap[v]))
						}

					})
			}
			stepLog = fmt.Sprintf("validating context after increasing HA for app: %s",
				ctx.App.Key)
			Step(stepLog, func() {
				log.InfoD(stepLog)
				errorChan := make(chan error, errorChannelSize)
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
							dashStats := make(map[string]string)
							dashStats["volume-name"] = v.Name
							dashStats["curr-repl-factor"] = strconv.FormatInt(currRep, 10)
							dashStats["new-repl-factor"] = strconv.FormatInt(currRep-1, 10)
							updateLongevityStats(HADecrease, stats.HADecreaseEventName, dashStats)
							err = Inst().V.SetReplicationFactor(v, currRep-1, nil, nil, true, opts)
							if err != nil {
								log.Debugf("Printing the volume inspect for the volume:%s ,volID:%s and namespace:%s after failed to  decrease repl", v.Name, v.ID, v.Namespace)
								PrintInspectVolume(v.ID)
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
				ctx.SkipVolumeValidation = false
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
			dashStats := make(map[string]string)
			dashStats["task-name"] = ctx.App.Key
			updateLongevityStats(AppTaskDown, stats.DeletePodsEventName, dashStats)
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
			ctx.SkipVolumeValidation = false
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
			err := isNodeHealthy(appNode, event.Event.Type)
			if err != nil {
				UpdateOutcome(event, err)
				continue
			}
			stepLog = fmt.Sprintf("crash volume driver %s on node: %v",
				Inst().V.String(), appNode.Name)
			nodeContexts, err := GetContextsOnNode(contexts, &appNode)
			UpdateOutcome(event, err)

			Step(stepLog,
				func() {
					log.InfoD(stepLog)
					taskStep := fmt.Sprintf("crash volume driver on node: %s",
						appNode.MgmtIp)
					event.Event.Type += "<br>" + taskStep
					errorChan := make(chan error, errorChannelSize)
					dashStats := make(map[string]string)
					dashStats["node"] = appNode.Name
					updateLongevityStats(CrashVolDriver, stats.PXCrashEventName, dashStats)
					CrashVolDriverAndWait([]node.Node{appNode}, &errorChan)
					for err := range errorChan {
						UpdateOutcome(event, err)
					}
				})
			err = ValidateDataIntegrity(&nodeContexts)
			UpdateOutcome(event, err)
			validateContexts(event, contexts)
		}
		updateMetrics(*event)
	})
}

// TriggerCrashPXDaemon crashes vol driver
func TriggerCrashPXDaemon(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {
	defer ginkgo.GinkgoRecover()
	defer endLongevityTest()
	startLongevityTest(CrashPXDaemon)
	event := &EventRecord{
		Event: Event{
			ID:   GenerateUUID(),
			Type: CrashPXDaemon,
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
			err := isNodeHealthy(appNode, event.Event.Type)
			if err != nil {
				UpdateOutcome(event, err)
				continue
			}
			stepLog = fmt.Sprintf("crash volume driver %s on node: %v",
				Inst().V.String(), appNode.Name)
			nodeContexts, err := GetContextsOnNode(contexts, &appNode)
			UpdateOutcome(event, err)

			Step(stepLog,
				func() {
					log.InfoD(stepLog)
					taskStep := fmt.Sprintf("crash volume driver on node: %s",
						appNode.MgmtIp)
					event.Event.Type += "<br>" + taskStep
					errorChan := make(chan error, errorChannelSize)
					dashStats := make(map[string]string)
					dashStats["node"] = appNode.Name
					updateLongevityStats(CrashPXDaemon, stats.PXDaemonCrashEventName, dashStats)
					CrashPXDaemonAndWait([]node.Node{appNode}, &errorChan)
					for err := range errorChan {
						UpdateOutcome(event, err)
					}
				})
			err = ValidateDataIntegrity(&nodeContexts)
			UpdateOutcome(event, err)
			validateContexts(event, contexts)
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

			err := isNodeHealthy(appNode, event.Event.Type)
			if err != nil {
				UpdateOutcome(event, err)
				continue
			}

			stepLog = fmt.Sprintf("stop volume driver %s on node: %s",
				Inst().V.String(), appNode.Name)
			nodeContexts, err := GetContextsOnNode(contexts, &appNode)
			UpdateOutcome(event, err)
			Step(stepLog,
				func() {
					log.InfoD(stepLog)
					taskStep := fmt.Sprintf("stop volume driver on node: %s.",
						appNode.Name)
					event.Event.Type += "<br>" + taskStep
					errorChan := make(chan error, errorChannelSize)
					dashStats := make(map[string]string)
					dashStats["node"] = appNode.Name
					updateLongevityStats(RestartVolDriver, stats.PXRestartEventName, dashStats)
					StopVolDriverAndWait([]node.Node{appNode}, &errorChan)
					for err := range errorChan {
						UpdateOutcome(event, err)
					}
				})
			stepLog = "wait for 15 mins for apps and volumes to reallocate"
			Step(stepLog, func() {
				log.InfoD(stepLog)
				time.Sleep(15 * time.Minute)
				validateContexts(event, contexts)

			})
			stepLog = fmt.Sprintf("starting volume %s driver on node %s",
				Inst().V.String(), appNode.Name)
			Step(stepLog,
				func() {
					log.InfoD(stepLog)
					taskStep := fmt.Sprintf("starting volume driver on node: %s.",
						appNode.Name)
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
			err = ValidateDataIntegrity(&nodeContexts)
			UpdateOutcome(event, err)

			validateContexts(event, contexts)
		}
		updateMetrics(*event)
	})
}

// TriggerNodeMaintenanceCycle performs node maintenance enter and exit and validates app
func TriggerNodeMaintenanceCycle(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {
	defer ginkgo.GinkgoRecover()
	defer endLongevityTest()
	startLongevityTest(NodeMaintenanceCycle)
	event := &EventRecord{
		Event: Event{
			ID:   GenerateUUID(),
			Type: NodeMaintenanceCycle,
		},
		Start:   time.Now().Format(time.RFC1123),
		Outcome: []error{},
	}

	defer func() {
		event.End = time.Now().Format(time.RFC1123)
		*recordChan <- event
	}()
	setMetrics(*event)
	stepLog := "get nodes and perform maintenance cycle"
	Step(stepLog, func() {
		log.InfoD(stepLog)
		for _, appNode := range node.GetStorageNodes() {
			err := isNodeHealthy(appNode, event.Event.Type)
			if err != nil {
				UpdateOutcome(event, err)
				continue
			}
			stepLog = fmt.Sprintf("enter maintenance on node: %s", appNode.Name)
			nodeContexts, err := GetContextsOnNode(contexts, &appNode)
			UpdateOutcome(event, err)
			Step(stepLog,
				func() {
					log.InfoD(stepLog)
					taskStep := fmt.Sprintf("enter maintenance on node: %s.",
						appNode.Name)
					event.Event.Type += "<br>" + taskStep
					dashStats := make(map[string]string)
					dashStats["node"] = appNode.Name
					updateLongevityStats(NodeMaintenanceCycle, stats.NodeMaintenanceEventName, dashStats)
					err = Inst().V.EnterMaintenance(appNode)
					if err != nil {
						UpdateOutcome(event, err)
						return
					}

				})
			stepLog = "wait for 15 mins for apps and volumes to reallocate"
			Step(stepLog, func() {
				log.InfoD(stepLog)
				time.Sleep(15 * time.Minute)

			})
			stepLog = fmt.Sprintf("exit maintenance on node %s", appNode.Name)
			Step(stepLog,
				func() {
					log.InfoD(stepLog)
					taskStep := fmt.Sprintf("exit maintenance on node: %s.",
						appNode.Name)
					event.Event.Type += "<br>" + taskStep
					err = Inst().V.ExitMaintenance(appNode)
					if err != nil {
						UpdateOutcome(event, err)
						return
					}
				})

			Step("Giving few seconds for volume driver to stabilize", func() {
				time.Sleep(20 * time.Second)
			})
			err = ValidateDataIntegrity(&nodeContexts)
			UpdateOutcome(event, err)

			validateContexts(event, contexts)
		}
		updateMetrics(*event)
	})
}

// TriggerPoolMaintenanceCycle performs pool maintenance enter and exit and validates app
func TriggerPoolMaintenanceCycle(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {
	defer ginkgo.GinkgoRecover()
	defer endLongevityTest()
	startLongevityTest(PoolMaintenanceCycle)
	event := &EventRecord{
		Event: Event{
			ID:   GenerateUUID(),
			Type: PoolMaintenanceCycle,
		},
		Start:   time.Now().Format(time.RFC1123),
		Outcome: []error{},
	}

	defer func() {
		event.End = time.Now().Format(time.RFC1123)
		*recordChan <- event
	}()
	setMetrics(*event)
	stepLog := "get nodes and perform pool maintenance cycle"
	Step(stepLog, func() {
		log.InfoD(stepLog)
		for _, appNode := range node.GetStorageNodes() {
			err := isNodeHealthy(appNode, event.Event.Type)
			if err != nil {
				UpdateOutcome(event, err)
				continue
			}

			stepLog = fmt.Sprintf("enter pool maintenance on node: %s", appNode.Name)
			nodeContexts, err := GetContextsOnNode(contexts, &appNode)
			UpdateOutcome(event, err)
			Step(stepLog,
				func() {
					log.InfoD(stepLog)
					taskStep := fmt.Sprintf("enter pool maintenance on node: %s.",
						appNode.Name)
					event.Event.Type += "<br>" + taskStep
					dashStats := make(map[string]string)
					dashStats["node"] = appNode.Name
					updateLongevityStats(PoolMaintenanceCycle, stats.PoolMaintenanceEventName, dashStats)
					err = Inst().V.EnterPoolMaintenance(appNode)
					if err != nil {
						log.InfoD(fmt.Sprintf("Printing The storage pool status on Node:%s after entering pool Maintenance", appNode.Name))
						PrintSvPoolStatus(appNode)
						UpdateOutcome(event, err)
						return
					}

				})
			stepLog = "wait for 15 mins for apps and volumes to reallocate"
			Step(stepLog, func() {
				log.InfoD(stepLog)
				time.Sleep(15 * time.Minute)
			})
			stepLog = fmt.Sprintf("exit pool maintenance on node %s", appNode.Name)
			Step(stepLog,
				func() {
					log.InfoD(stepLog)
					taskStep := fmt.Sprintf("exit pool maintenance on node: %s.",
						appNode.Name)
					event.Event.Type += "<br>" + taskStep
					err = Inst().V.ExitPoolMaintenance(appNode)
					if err != nil {
						log.InfoD(fmt.Sprintf("Printing The storage pool status on Node:%s after pool Maintenance", appNode.Name))
						PrintSvPoolStatus(appNode)
						UpdateOutcome(event, err)
						return
					}
				})

			Step("Giving few seconds for volume driver to stabilize", func() {
				time.Sleep(20 * time.Second)
			})
			err = ValidateDataIntegrity(&nodeContexts)
			UpdateOutcome(event, err)

			validateContexts(event, contexts)
		}
		updateMetrics(*event)
	})
}

func isNodeHealthy(n node.Node, eventType string) error {
	status, err := Inst().V.GetNodeStatus(n)
	if err != nil {
		log.Errorf("Unable to get Node [%s] status, skipping [%s] for the node [%+v]. Error: [%v]", n.Name, eventType, n, err)
		return err
	}

	if *status != opsapi.Status_STATUS_OK {
		log.Errorf("Node [%s] is not healthy,  skipping [%s] for the node", n.Name, eventType)
		return fmt.Errorf("node [%s] has status [%v]", n.Name, status)
	}
	return nil
}

// TriggerStorageFullPoolExpansion performs pool expansion on storageFull nodes
func TriggerStorageFullPoolExpansion(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {
	defer ginkgo.GinkgoRecover()
	defer endLongevityTest()
	startLongevityTest(StorageFullPoolExpansion)
	event := &EventRecord{
		Event: Event{
			ID:   GenerateUUID(),
			Type: StorageFullPoolExpansion,
		},
		Start:   time.Now().Format(time.RFC1123),
		Outcome: []error{},
	}

	defer func() {
		event.End = time.Now().Format(time.RFC1123)
		*recordChan <- event
	}()
	setMetrics(*event)
	stepLog := "get nodes and expand storagefull pools"
	Step(stepLog, func() {
		log.InfoD(stepLog)
		for _, appNode := range node.GetStorageNodes() {

			poolsStatus, err := Inst().V.GetNodePoolsStatus(appNode)
			if err != nil {
				UpdateOutcome(event, err)
				continue
			}
			for poolUUID, v := range poolsStatus {
				if v == "Offline" {
					taskStep := fmt.Sprintf("expanding storagefull pool [%s] on node [%s]", poolUUID,
						appNode.Name)
					event.Event.Type += "<br>" + taskStep
					log.InfoD("Pool [%s] on node [%s] is offline", v, appNode.Name)
					poolResizeType := []opsapi.SdkStoragePool_ResizeOperationType{opsapi.SdkStoragePool_RESIZE_TYPE_AUTO,
						opsapi.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK, opsapi.SdkStoragePool_RESIZE_TYPE_ADD_DISK}
					resizeOpType := poolResizeType[rand.Intn(len(poolResizeType))]
					selectedPool, err := GetStoragePoolByUUID(poolUUID)
					if err != nil {
						UpdateOutcome(event, err)
						continue
					}
					if resizeOpType == opsapi.SdkStoragePool_RESIZE_TYPE_ADD_DISK {
						log.InfoD(fmt.Sprintf("Entering pool maintenance mode on node %s", appNode.Name))
						dashStats := make(map[string]string)
						dashStats["node"] = appNode.Name
						updateLongevityStats(StorageFullPoolExpansion, stats.PoolMaintenanceEventName, dashStats)
						err = Inst().V.EnterPoolMaintenance(appNode)
						if err != nil {
							UpdateOutcome(event, err)
							continue
						}

						status, err := Inst().V.GetNodePoolsStatus(appNode)
						if err != nil {
							UpdateOutcome(event, err)
							continue
						}
						log.InfoD(fmt.Sprintf("pool %s status %s", appNode.Name, status[poolUUID]))
					}
					stepLog = fmt.Sprintf("expand pool %s using %v", selectedPool.Uuid, resizeOpType)
					var expandedExpectedPoolSize uint64
					Step(stepLog, func() {
						log.InfoD(stepLog)
						expandedExpectedPoolSize = (selectedPool.TotalSize / units.GiB) * 2

						log.InfoD("Current Size of the pool %s is %d", selectedPool.Uuid, selectedPool.TotalSize/units.GiB)
						dashStats := make(map[string]string)
						dashStats["pool-uuid"] = selectedPool.Uuid
						dashStats["resize-operation"] = resizeOpType.String()
						dashStats["resize-size"] = fmt.Sprintf("%d", expandedExpectedPoolSize)
						statType := stats.ResizeDiskEventName
						if resizeOpType == opsapi.SdkStoragePool_RESIZE_TYPE_ADD_DISK {
							statType = stats.AddDiskEventName
						}
						updateLongevityStats(event.Event.Type, statType, dashStats)

						err = Inst().V.ExpandPool(selectedPool.Uuid, resizeOpType, expandedExpectedPoolSize, true)
						if err != nil {
							stNode, err := GetNodeFromPoolUUID(selectedPool.Uuid)
							if err != nil {
								log.Errorf(fmt.Sprintf("error getting the node for pool %s", selectedPool.Uuid))
							}
							log.InfoD(fmt.Sprintf("Printing The storage pool status after pool resize on Node:%s ", stNode.Name))
							PrintSvPoolStatus(*stNode)
							UpdateOutcome(event, err)
							return
						}
					})
					stepLog = fmt.Sprintf("Ensure that pool %s expansion is successful", selectedPool.Uuid)
					Step(stepLog, func() {
						log.InfoD(stepLog)
						err = waitForPoolToBeResized(expandedExpectedPoolSize, selectedPool.Uuid)
						UpdateOutcome(event, err)
						status, err := Inst().V.GetNodePoolsStatus(appNode)
						UpdateOutcome(event, err)

						log.InfoD(fmt.Sprintf("Pool %s has status %s", appNode.Name, status[selectedPool.Uuid]))
						if status[selectedPool.Uuid] == "In Maintenance" {
							log.InfoD(fmt.Sprintf("Exiting pool maintenance mode on node %s", appNode.Name))
							err = Inst().V.ExitPoolMaintenance(appNode)
							if err != nil {
								UpdateOutcome(event, err)
								return
							}
							expectedStatus := "Online"
							err = WaitForPoolStatusToUpdate(appNode, expectedStatus)
							if err != nil {
								UpdateOutcome(event, err)
								return
							}

						}

						resizedPool, err := GetStoragePoolByUUID(selectedPool.Uuid)
						if err != nil {
							UpdateOutcome(event, err)
							return
						}
						newPoolSize := resizedPool.TotalSize / units.GiB
						isExpansionSuccess := false
						expectedSizeWithJournal := expandedExpectedPoolSize - 3

						if newPoolSize >= expectedSizeWithJournal {
							isExpansionSuccess = true
						}
						dash.VerifySafely(isExpansionSuccess, true, fmt.Sprintf("expected new pool size to be %v or %v, got %v", expandedExpectedPoolSize, expectedSizeWithJournal, newPoolSize))
						nodeStatus, err := Inst().V.GetNodeStatus(appNode)
						if err != nil {
							UpdateOutcome(event, err)
							return
						}
						dash.VerifySafely(*nodeStatus, opsapi.Status_STATUS_OK, fmt.Sprintf("validate PX status on node %s", appNode.Name))
					})

				}
			}

			err = ValidateDataIntegrity(contexts)
			UpdateOutcome(event, err)

			validateContexts(event, contexts)
			err = Inst().V.RefreshDriverEndpoints()
			UpdateOutcome(event, err)
		}
		updateMetrics(*event)
	})
}

func validateContexts(event *EventRecord, contexts *[]*scheduler.Context) {
	actualEvent := strings.Split(event.Event.Type, "<br>")[0]
	for _, ctx := range *contexts {
		stepLog := fmt.Sprintf("%s: validating app [%s]", actualEvent, ctx.App.Key)
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
			err := isNodeHealthy(appNode, event.Event.Type)
			if err != nil {
				UpdateOutcome(event, err)
				continue
			}
			dashStats := make(map[string]string)
			dashStats["node"] = appNode.Name
			updateLongevityStats(RestartManyVolDriver, stats.PXRestartEventName, dashStats)
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
		err := ValidateDataIntegrity(contexts)
		UpdateOutcome(event, err)
		validateContexts(event, contexts)
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
		log.InfoD(stepLog)
		kvdbNodes, err := GetAllKvdbNodes()
		if err != nil {
			UpdateOutcome(event, err)
			return
		}
		stNodes := node.GetNodesByVoDriverNodeID()
		nodeContexts := make([]*scheduler.Context, 0)
		for _, kvdbNode := range kvdbNodes {

			appNode, ok := stNodes[kvdbNode.ID]
			if !ok {
				UpdateOutcome(event, fmt.Errorf("node with id %s not found in the nodes list", kvdbNode.ID))
				continue
			}

			stepLog = fmt.Sprintf("stop volume driver %s on node: %s",
				Inst().V.String(), appNode.Name)
			Step(stepLog,
				func() {
					log.InfoD(stepLog)
					appNodeContexts, err := GetContextsOnNode(contexts, &appNode)
					UpdateOutcome(event, err)
					nodeContexts = append(nodeContexts, appNodeContexts...)
					taskStep := fmt.Sprintf("stop volume driver on node: %s.",
						appNode.MgmtIp)
					event.Event.Type += "<br>" + taskStep
					errorChan := make(chan error, errorChannelSize)
					dashStats := make(map[string]string)
					dashStats["node"] = appNode.Name
					dashStats["kvdb"] = "true"
					updateLongevityStats(RestartKvdbVolDriver, stats.PXRestartEventName, dashStats)
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
			validateContexts(event, contexts)
		}
		err = ValidateDataIntegrity(&nodeContexts)
		UpdateOutcome(event, err)
		updateMetrics(*event)
	})
}

func TriggerValidatePdsApps(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {
	defer ginkgo.GinkgoRecover()
	defer endLongevityTest()
	startLongevityTest(ValidatePdsApps)
	event := &EventRecord{
		Event: Event{
			ID:   GenerateUUID(),
			Type: ValidatePdsApps,
		},
		Start:   time.Now().Format(time.RFC1123),
		Outcome: []error{},
	}

	defer func() {
		event.End = time.Now().Format(time.RFC1123)
		*recordChan <- event
	}()

	Step("validate pds-apps", func() {
		for _, ctx := range *contexts {
			stepLog := fmt.Sprintf("RebootNode: validating pds app [%s]", ctx.App.Key)
			Step(stepLog, func() {
				errorChan := make(chan error, errorChannelSize)
				ValidatePDSDataServices(ctx, &errorChan)
				for err := range errorChan {
					UpdateOutcome(event, err)
				}
			})
		}
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
		nodesToReboot := node.GetStorageDriverNodes()

		// Reboot node and check driver status
		stepLog = fmt.Sprintf("reboot node one at a time")
		Step(stepLog, func() {
			// TODO: Below is the same code from existing nodeReboot test
			log.InfoD(stepLog)
			nodeContexts := make([]*scheduler.Context, 0)
			for _, n := range nodesToReboot {
				if n.IsStorageDriverInstalled {
					err := isNodeHealthy(n, event.Event.Type)
					if err != nil {
						UpdateOutcome(event, err)
						continue
					}
					stepLog = fmt.Sprintf("reboot node: %s", n.Name)
					appNodeContexts, err := GetContextsOnNode(contexts, &n)
					UpdateOutcome(event, err)
					nodeContexts = append(nodeContexts, appNodeContexts...)
					Step(stepLog, func() {
						log.InfoD(stepLog)
						taskStep := fmt.Sprintf("reboot node: %s.", n.Name)
						event.Event.Type += "<br>" + taskStep
						dashStats := make(map[string]string)
						dashStats["node"] = n.Name
						updateLongevityStats(RebootNode, stats.NodeRebootEventName, dashStats)
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
					validateContexts(event, contexts)
				}
			}
			err := ValidateDataIntegrity(&nodeContexts)
			UpdateOutcome(event, err)
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
		selectedNodes := make([]node.Node, 0)
		for _, n := range nodesToReboot {
			err := isNodeHealthy(n, event.Event.Type)
			if err != nil {
				UpdateOutcome(event, err)
				continue
			}
			selectedNodes = append(selectedNodes, n)
		}
		// Reboot node and check driver status
		stepLog = fmt.Sprintf("rebooting [%d] node(s)", len(selectedNodes))
		Step(stepLog, func() {
			log.InfoD(stepLog)
			var wg sync.WaitGroup
			for _, n := range selectedNodes {
				dashStats := make(map[string]string)
				dashStats["node"] = n.Name
				updateLongevityStats(RebootManyNodes, stats.NodeRebootEventName, dashStats)
				wg.Add(1)
				go func(n node.Node) {
					defer wg.Done()
					err := isNodeHealthy(n, event.Event.Type)
					if err != nil {
						UpdateOutcome(event, err)
						return
					}
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

			for _, n := range selectedNodes {
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

			validateContexts(event, contexts)
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
	stNodes := node.GetStorageDriverNodes()
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
		nodesToCrash := node.GetStorageDriverNodes()

		// Crash node and check driver status
		stepLog = fmt.Sprintf("crash node one at a time from the node(s): %v", nodesToCrash)
		Step(stepLog, func() {
			// TODO: Below is the same code from existing nodeCrash test
			log.InfoD(stepLog)
			for _, n := range nodesToCrash {
				if n.IsStorageDriverInstalled {
					err := isNodeHealthy(n, event.Event.Type)
					if err != nil {
						UpdateOutcome(event, err)
						continue
					}
					stepLog = fmt.Sprintf("crash node: %s", n.Name)
					Step(stepLog, func() {
						log.InfoD(stepLog)
						taskStep := fmt.Sprintf("crash node: %s.", n.MgmtIp)
						event.Event.Type += "<br>" + taskStep
						dashStats := make(map[string]string)
						dashStats["node"] = n.Name
						updateLongevityStats(CrashNode, stats.NodeCrashEventName, dashStats)
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

					validateContexts(event, contexts)
				}
			}
		})
	})
}

// TriggerVolumeClone clones all volumes, validates and destroys the clone
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
					log.Debugf("Printing the volume inspect for the volume:%s ,volID:%s and namespace:%s after cloning the volume", vol.Name, vol.ID, vol.Namespace)
					PrintInspectVolume(vol.ID)
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
					params[k8s.SnapshotParent] = vol.Name
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

			requestedVols := make([]*volume.Volume, 0)
			stepLog = fmt.Sprintf("increase volume size %s on app %s's volumes: %v",
				Inst().V.String(), ctx.App.Key, appVolumes)
			Step(stepLog,
				func() {
					log.InfoD(stepLog)
					chaosLevel := getPoolExpandPercentage(VolumeResize)
					pvcs, err := GetContextPVCs(ctx)
					if err != nil {
						UpdateOutcome(event, err)
						return
					}
					for _, pvc := range pvcs {
						log.InfoD("increasing pvc [%s/%s]  size to %d", pvc.Namespace, pvc.Name, chaosLevel)
						dashStats := make(map[string]string)
						dashStats["pvc-name"] = pvc.Name
						dashStats["resize-by"] = fmt.Sprintf("%dGiB", chaosLevel)
						updateLongevityStats(VolumeResize, stats.VolumeResizeEventName, dashStats)
						resizedVol, err := Inst().S.ResizePVC(ctx, pvc, chaosLevel)
						if err != nil && !(strings.Contains(err.Error(), "only dynamically provisioned pvc can be resized")) {
							UpdateOutcome(event, err)
							continue
						}
						requestedVols = append(requestedVols, resizedVol)
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
						log.Debugf("Printing the volume inspect for the volume:%s ,volID:%s and namespace:%s after vol resize", v.Name, v.ID, v.Namespace)
						PrintInspectVolume(v.ID)
						err := Inst().V.ValidateUpdateVolume(v, params)
						if err != nil {
							log.Debugf("Printing the volume inspect for the volume:%s ,volID:%s and namespace:%s after vol resize", v.Name, v.ID, v.Namespace)
							PrintInspectVolume(v.ID)
						}
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
						dashStats := make(map[string]string)
						dashStats["app-name"] = ctx.App.Key
						dashStats["schedule-interval"] = fmt.Sprintf("%d mins", interval)
						dashStats["retain"] = fmt.Sprintf("%d", retain)
						updateLongevityStats(LocalSnapShot, stats.LocalsnapEventName, dashStats)
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
									dashStats := make(map[string]string)
									dashStats["app-name"] = ctx.App.Key
									dashStats["snapshot-name"] = e.Name
									updateLongevityStats(DeleteLocalSnapShot, stats.LocalsnapEventName, dashStats)
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

func TriggerLocalSnapshotRestore(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {
	defer ginkgo.GinkgoRecover()
	defer endLongevityTest()
	startLongevityTest(LocalSnapShotRestore)
	uuid := GenerateUUID()
	event := &EventRecord{
		Event: Event{
			ID:   uuid,
			Type: LocalSnapShotRestore,
		},
		Start:   time.Now().Format(time.RFC1123),
		Outcome: []error{},
	}

	defer func() {
		event.End = time.Now().Format(time.RFC1123)
		*recordChan <- event
	}()

	for _, ctx := range *contexts {
		var appVolumes []*volume.Volume
		var err error
		if strings.Contains(ctx.App.Key, "localsnap") {
			appNamespace := ctx.App.Key + "-" + ctx.UID
			log.Infof("Namespace : %v", appNamespace)
			stepLog := fmt.Sprintf("Getting app volumes for volume %s", ctx.App.Key)
			Step(stepLog, func() {
				log.InfoD(stepLog)
				appVolumes, err = Inst().S.GetVolumes(ctx)
				UpdateOutcome(event, err)
				if len(appVolumes) == 0 {
					UpdateOutcome(event, fmt.Errorf("no volumes found for [%s]", ctx.App.Key))
				}
			})
			log.Infof("Got volume count : %v", len(appVolumes))
			for _, v := range appVolumes {
				snapshotScheduleName := v.Name + "-interval-schedule"
				log.InfoD("snapshotScheduleName : %v for volume: %s", snapshotScheduleName, v.Name)
				var volumeSnapshotStatus *storkv1.ScheduledVolumeSnapshotStatus
				checkSnapshotSchedules := func() (interface{}, bool, error) {
					resp, err := storkops.Instance().GetSnapshotSchedule(snapshotScheduleName, appNamespace)
					if err != nil {
						return "", false, fmt.Errorf("error getting snapshot schedule for %s, volume:%s in namespace %s", snapshotScheduleName, v.Name, v.Namespace)
					}
					if len(resp.Status.Items) == 0 {
						return "", false, fmt.Errorf("no snapshot schedules found for %s, volume:%s in namespace %s", snapshotScheduleName, v.Name, v.Namespace)
					}

					log.Infof("%v", resp.Status.Items)
					for _, snapshotStatuses := range resp.Status.Items {
						if len(snapshotStatuses) > 0 {
							volumeSnapshotStatus = snapshotStatuses[len(snapshotStatuses)-1]
							if volumeSnapshotStatus == nil {
								return "", true, fmt.Errorf("SnapshotSchedule has an empty migration in it's most recent status")
							}
							if volumeSnapshotStatus.Status == snapv1.VolumeSnapshotConditionReady {
								return nil, false, nil
							}
							if volumeSnapshotStatus.Status == snapv1.VolumeSnapshotConditionError {
								return nil, false, fmt.Errorf("volume snapshot: %s failed. status: %v", volumeSnapshotStatus.Name, volumeSnapshotStatus.Status)
							}
							if volumeSnapshotStatus.Status == snapv1.VolumeSnapshotConditionPending {
								return nil, true, fmt.Errorf("volume Sanpshot %s is still pending", volumeSnapshotStatus.Name)
							}
						}
					}
					return nil, true, fmt.Errorf("volume Sanpshots for %s is not found", v.Name)
				}
				_, err = task.DoRetryWithTimeout(checkSnapshotSchedules, snapshotScheduleRetryTimeout, snapshotScheduleRetryInterval)
				if err != nil {
					UpdateOutcome(event, err)
					return
				}
				dashStats := make(map[string]string)
				dashStats["source-name"] = volumeSnapshotStatus.Name
				dashStats["source-namespace"] = appNamespace
				dashStats["destination-name"] = v.Name
				dashStats["destination-namespace"] = v.Namespace
				updateLongevityStats(LocalSnapShotRestore, stats.LocalsnapRestorEventName, dashStats)
				restoreSpec := &storkv1.VolumeSnapshotRestore{ObjectMeta: meta_v1.ObjectMeta{
					Name:      v.Name,
					Namespace: v.Namespace,
				}, Spec: storkv1.VolumeSnapshotRestoreSpec{SourceName: volumeSnapshotStatus.Name, SourceNamespace: appNamespace, GroupSnapshot: false}}
				restore, err := storkops.Instance().CreateVolumeSnapshotRestore(restoreSpec)
				if err != nil {
					UpdateOutcome(event, err)
					return
				}
				err = storkops.Instance().ValidateVolumeSnapshotRestore(restore.Name, restore.Namespace, snapshotScheduleRetryTimeout, snapshotScheduleRetryInterval)
				dash.VerifySafely(err, nil, fmt.Sprintf("validate snapshot restore source: %s , destination: %s in namespace %s", restore.Name, v.Name, v.Namespace))
				UpdateOutcome(event, err)

			}

		}
	}

	err := ValidateDataIntegrity(contexts)
	UpdateOutcome(event, err)

	setMetrics(*event)
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
	n := node.GetStorageDriverNodes()[0]
	uuidCmd := "cred list -j | grep uuid"

	output, err := Inst().V.GetPxctlCmdOutput(n, uuidCmd)
	if err != nil {
		UpdateOutcome(event, err)
		return
	}

	if output == "" {
		UpdateOutcome(event, fmt.Errorf("cloud cred is not created"))
		return
	}

	credUUID := strings.Split(strings.TrimSpace(output), " ")[1]
	credUUID = strings.ReplaceAll(credUUID, "\"", "")
	log.Infof("Got Cred UUID: %s", credUUID)

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
						err = CreatePXCloudCredential()
						if err != nil {
							UpdateOutcome(event, err)
							return
						}
						retain := 10
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

						dashStats := make(map[string]string)
						dashStats["app-name"] = ctx.App.Key
						dashStats["schedule-interval"] = fmt.Sprintf("%d mins", interval)
						dashStats["retain"] = fmt.Sprintf("%d", retain)
						updateLongevityStats(CloudSnapShot, stats.CloudsnapEventName, dashStats)
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
				scaleFactor := time.Duration(Inst().GlobalScaleFactor * len(appVolumes))
				err = Inst().S.ValidateVolumes(ctx, scaleFactor*defaultVolScaleTimeout, defaultRetryInterval, nil)
				if err != nil {
					UpdateOutcome(event, err)
				} else {
					snapMap := make(map[*volume.Volume]*storkv1.ScheduledVolumeSnapshotStatus)
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
						var volumeSnapshotStatus *storkv1.ScheduledVolumeSnapshotStatus
						checkSnapshotSchedules := func() (interface{}, bool, error) {
							resp, err := storkops.Instance().GetSnapshotSchedule(snapshotScheduleName, appNamespace)
							if err != nil {
								return "", false, fmt.Errorf("error getting snapshot schedule for %s, volume:%s in namespace %s", snapshotScheduleName, v.Name, v.Namespace)
							}
							if len(resp.Status.Items) == 0 {
								return "", false, fmt.Errorf("no snapshot schedules found for %s, volume:%s in namespace %s", snapshotScheduleName, v.Name, v.Namespace)
							}

							log.Infof("%v", resp.Status.Items)
							for _, snapshotStatuses := range resp.Status.Items {
								if len(snapshotStatuses) > 0 {
									volumeSnapshotStatus = snapshotStatuses[len(snapshotStatuses)-1]
									if volumeSnapshotStatus == nil {
										return "", true, fmt.Errorf("SnapshotSchedule has an empty migration in it's most recent status")
									}
									if volumeSnapshotStatus.Status == snapv1.VolumeSnapshotConditionReady {
										return nil, false, nil
									}
									if volumeSnapshotStatus.Status == snapv1.VolumeSnapshotConditionError {
										return nil, false, fmt.Errorf("volume snapshot: %s failed. status: %v", volumeSnapshotStatus.Name, volumeSnapshotStatus.Status)
									}
									if volumeSnapshotStatus.Status == snapv1.VolumeSnapshotConditionPending {
										return nil, true, fmt.Errorf("volume Sanpshot %s is still pending", volumeSnapshotStatus.Name)
									}
								}
							}
							return nil, true, fmt.Errorf("volume Sanpshots for %s is not found", v.Name)
						}
						_, err = task.DoRetryWithTimeout(checkSnapshotSchedules, snapshotScheduleRetryTimeout, snapshotScheduleRetryInterval)
						UpdateOutcome(event, err)
						snapData, err := Inst().S.GetSnapShotData(ctx, volumeSnapshotStatus.Name, appNamespace)
						UpdateOutcome(event, err)
						snapType := snapData.Spec.PortworxSnapshot.SnapshotType
						log.Infof("Snapshot Type: %v", snapType)
						if snapType != "cloud" {
							err = &scheduler.ErrFailedToGetVolumeParameters{
								App:   ctx.App,
								Cause: fmt.Sprintf("Snapshot Type: %s does not match", snapType),
							}
							UpdateOutcome(event, err)
						}
						condition := snapData.Status.Conditions[0]
						dash.VerifySafely(condition.Type == snapv1.VolumeSnapshotDataConditionReady, true, fmt.Sprintf("validate volume snapshot condition data for %s expteced: %v, actual %v", volumeSnapshotStatus.Name, snapv1.VolumeSnapshotDataConditionReady, condition.Type))

						snapID := snapData.Spec.PortworxSnapshot.SnapshotID
						log.Infof("Snapshot ID: %v", snapID)
						if snapData.Spec.VolumeSnapshotDataSource.PortworxSnapshot == nil ||
							len(snapData.Spec.VolumeSnapshotDataSource.PortworxSnapshot.SnapshotID) == 0 {
							err = &scheduler.ErrFailedToGetVolumeParameters{
								App:   ctx.App,
								Cause: fmt.Sprintf("volumesnapshotdata: %s does not have portworx volume source set", snapData.Metadata.Name),
							}
							UpdateOutcome(event, err)

						}
						snapMap[v] = volumeSnapshotStatus
					}
					cloudsnapMap[appNamespace] = snapMap
				}
			}
		}
		updateMetrics(*event)
	})

}

// TriggerCloudSnapshotRestore perform in-place cloud snap restore
func TriggerCloudSnapshotRestore(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {

	defer ginkgo.GinkgoRecover()
	defer endLongevityTest()
	startLongevityTest(CloudSnapShotRestore)
	uuid := GenerateUUID()
	event := &EventRecord{
		Event: Event{
			ID:   uuid,
			Type: CloudSnapShotRestore,
		},
		Start:   time.Now().Format(time.RFC1123),
		Outcome: []error{},
	}

	defer func() {
		event.End = time.Now().Format(time.RFC1123)
		*recordChan <- event
	}()

	defer func() {
		bucketName, err := GetCloudsnapBucketName(*contexts)
		UpdateOutcome(event, err)
		err = DeleteCloudSnapBucket(bucketName)
		UpdateOutcome(event, err)
	}()

	setMetrics(*event)

	stepLog := "Verify cloud snap restore"

	Step(stepLog, func() {
		for _, ctx := range *contexts {
			if strings.Contains(ctx.App.Key, "cloudsnap") {
				appNamespace := ctx.App.Key + "-" + ctx.UID
				snapSchedList, err := storkops.Instance().ListSnapshotSchedules(appNamespace)
				if err != nil {
					UpdateOutcome(event, err)
					return
				}
				vols, err := Inst().S.GetVolumes(ctx)
				UpdateOutcome(event, err)

				for _, vol := range vols {
					var snapshotScheduleName string
					for _, snap := range snapSchedList.Items {
						snapshotScheduleName = snap.Name
						if strings.Contains(snapshotScheduleName, vol.Name) {
							break
						}
					}
					resp, err := storkops.Instance().GetSnapshotSchedule(snapshotScheduleName, appNamespace)
					if err != nil {
						UpdateOutcome(event, err)
						return
					}
					var volumeSnapshotStatus *storkv1.ScheduledVolumeSnapshotStatus
				outer:
					for _, snapshotStatuses := range resp.Status.Items {
						for _, vsStatus := range snapshotStatuses {
							if vsStatus.Status == snapv1.VolumeSnapshotConditionReady {
								volumeSnapshotStatus = vsStatus
								break outer
							}
						}
					}
					if volumeSnapshotStatus != nil {
						dashStats := make(map[string]string)
						dashStats["source-name"] = volumeSnapshotStatus.Name
						dashStats["source-namespace"] = appNamespace
						dashStats["destination-name"] = vol.Name
						dashStats["destination-namespace"] = vol.Namespace
						updateLongevityStats(CloudSnapShotRestore, stats.CloudsnapRestorEventName, dashStats)
						restoreSpec := &storkv1.VolumeSnapshotRestore{ObjectMeta: meta_v1.ObjectMeta{
							Name:      vol.Name,
							Namespace: vol.Namespace,
						}, Spec: storkv1.VolumeSnapshotRestoreSpec{SourceName: volumeSnapshotStatus.Name, SourceNamespace: appNamespace, GroupSnapshot: false}}
						restore, err := storkops.Instance().CreateVolumeSnapshotRestore(restoreSpec)
						if err != nil {
							UpdateOutcome(event, err)
							return
						}
						err = storkops.Instance().ValidateVolumeSnapshotRestore(restore.Name, restore.Namespace, snapshotScheduleRetryTimeout, snapshotScheduleRetryInterval)
						dash.VerifySafely(err, nil, fmt.Sprintf("validate snapshot restore source: %s , destnation: %s in namespace %s", restore.Name, vol.Name, vol.Namespace))
						if err == nil {
							err = storkops.Instance().DeleteVolumeSnapshotRestore(restore.Name, restore.Namespace)
							if err != nil {
								UpdateOutcome(event, err)
								return
							}
						}
					} else {
						UpdateOutcome(event, fmt.Errorf("no snapshot with Ready status found for vol[%s] in namespace[%s]", vol.Name, vol.Namespace))
					}

				}

			}
		}

	})
	err := ValidateDataIntegrity(contexts)
	UpdateOutcome(event, err)

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

			err := ValidateVolumesDeleted(ctx.App.Key, vols)
			UpdateOutcome(event, err)
			checkLunsAfterVolumeDeletion(event, vols)
		}
		*contexts = nil
		TriggerDeployNewApps(contexts, recordChan)
		updateMetrics(*event)
	})
}

func checkLunsAfterVolumeDeletion(event *EventRecord, vols []*volume.Volume) {
	volDriverNamespace, err := Inst().V.GetVolumeDriverNamespace()
	if err != nil {
		UpdateOutcome(event, err)
		return
	}
	pxPureSecret, err := pureutils.GetPXPureSecret(volDriverNamespace)
	if err != nil {
		log.Warnf("no pure secret found, assuming not a FA/FB  backend, err: %v", err)
		return
	}
	cluster, err := Inst().V.InspectCurrentCluster()
	if err != nil {
		UpdateOutcome(event, err)
		return
	}
	log.Infof("Current cluster [%s] UID: [%s]", cluster.Cluster.Name, cluster.Cluster.Id)
	clusterUIDPrefix := strings.Split(cluster.Cluster.Id, "-")[0]

	pureClientMap := make(map[string]map[string]*flasharray.Client)
	pureClientMap["FADA"], err = pureutils.GetFAClientMapFromPXPureSecret(pxPureSecret)

	timeout := 10 * time.Minute
	t := func() (interface{}, bool, error) {
		for volType, clientMap := range pureClientMap {
			for mgmtEndPoint, client := range clientMap {
				pureVolumes, err := client.Volumes.ListVolumes(nil)
				if err != nil {
					return nil, true, fmt.Errorf("failed to list [%s] volumes from endpoint [%s].Err: %v", volType, mgmtEndPoint, err)

				}
				pureVolumeNames := make(map[string]bool)
				for _, pureVol := range pureVolumes {
					pureVolumeNames[pureVol.Name] = true
				}
				for _, vol := range vols {
					pureVolName := "px_" + clusterUIDPrefix + "-" + vol.Name
					if pureVolumeNames[pureVolName] {
						return nil, false, fmt.Errorf("pure volume [%s] still exists in pure client [%s]", pureVolName, mgmtEndPoint)
					}
				}
			}
		}
		return "", false, nil
	}
	_, err = task.DoRetryWithTimeout(t, timeout, 1*time.Minute)
	UpdateOutcome(event, err)
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
	log.Infof("Provider for credential secret is %s", provider)
	if !ok {
		return nil, &errors.ErrNotFound{
			ID:   "OBJECT_STORE_PROVIDER",
			Type: "Environment Variable",
		}
	}
	log.Infof("Provider for credential secret is %s", provider)

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

// CollectEventRecords collects eventRecords from a channel
// and stores in buffer for future email notifications
func CollectEventRecords(recordChan *chan *EventRecord) {
	eventRing = ring.New(100)
	for eventRecord := range *recordChan {
		eventRing.Value = eventRecord
		actualEvent := strings.Split(eventRecord.Event.Type, "<br>")[0]
		TestExecutionCounter.Increment(actualEvent)
		log.Infof("TestExecutionCountMap: %v", TestExecutionCounter.String())
		eventRing = eventRing.Next()
	}
}

// TriggerEmailReporter sends email with all reported errors
func TriggerEmailReporter() {
	// emailRecords stores events to be notified

	emailData := emailData{}
	timeString := time.Now().Format(time.RFC1123)
	log.Infof("Generating email report: %s", timeString)

	var masterNodeList []string
	var pxStatus string
	emailData.MailSubject = EmailSubject
	for _, n := range node.GetMasterNodes() {
		masterNodeList = append(masterNodeList, n.Addresses...)
	}
	emailData.MasterIP = masterNodeList
	emailData.DashboardURL = fmt.Sprintf("%s/resultSet/testSetID/%s", aetosutil.AetosBaseURL, os.Getenv("DASH_UID"))

	for _, n := range node.GetStorageDriverNodes() {
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
		Subject:         EmailSubject,
		Content:         content,
		From:            from,
		To:              EmailRecipients,
		EmailHostServer: EmailServer,
	}

	if err := emailDetails.SendEmail(); err != nil {
		log.Errorf("Failed to send out email, Err: %q", err)
	}

	//clearing core map content
	for k := range coresMap {
		delete(coresMap, k)
	}

	fileName := fmt.Sprintf("%s_%s", EmailSubject, timeString)
	fileName = regexp.MustCompile(`[^a-zA-Z0-9]+`).ReplaceAllString(fileName, "_")
	filePath := fmt.Sprintf("%s/%s.html", Inst().LogLoc, fileName)

	if err := os.WriteFile(filePath, []byte(content), 0664); err != nil {
		log.Errorf("Failed to create html report, Err: %q", err)
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
					SourceClusterName, backupLocationNameConst, BackupLocationUID,
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
// Creates config maps in the specified namespaces and backups up only these config maps
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
			backupCreateRequest := GetBackupCreateRequest(backupName, SourceClusterName, backupLocationNameConst, BackupLocationUID,
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
			backupCreateRequest := GetBackupCreateRequest(backupName, SourceClusterName, backupLocationNameConst, BackupLocationUID,
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
		backupCreateRequest := GetBackupCreateRequest(backupName, SourceClusterName, backupLocationNameConst, BackupLocationUID,
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
			Type: BackupRestartPortworx,
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
				SourceClusterName, backupLocationNameConst, BackupLocationUID,
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
				SourceClusterName, backupLocationNameConst, BackupLocationUID,
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
				SourceClusterName, backupLocationNameConst, BackupLocationUID,
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
				SourceClusterName, backupLocationNameConst, BackupLocationUID,
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
	stNode, err := GetNodeWithGivenPoolID(poolToBeResized.Uuid)
	if err != nil {
		return false, err
	}

	t := func() (interface{}, bool, error) {
		status, err := Inst().V.GetNodePoolsStatus(*stNode)
		if err != nil {
			return "", false, err
		}
		currStatus := status[poolToBeResized.Uuid]

		if currStatus == "Offline" {
			return "", true, fmt.Errorf("pool [%s] has current status [%s].Waiting rebalance to complete if in-progress", poolToBeResized.Uuid, currStatus)
		}
		return "", false, nil
	}

	_, err = task.DoRetryWithTimeout(t, 120*time.Minute, 2*time.Second)
	if err != nil {
		return false, err
	}
	return true, nil
}

func waitForPoolToBeResized(initialSize uint64, poolIDToResize string) error {

	cnt := 0
	currentLastMsg := ""
	f := func() (interface{}, bool, error) {
		expandedPool, err := GetStoragePoolByUUID(poolIDToResize)
		if err != nil {
			return nil, true, fmt.Errorf("error getting pool by using id %s", poolIDToResize)
		}

		if expandedPool == nil {
			return nil, false, fmt.Errorf("expanded pool value is nil")
		}
		if expandedPool.LastOperation != nil {
			log.Infof("Pool [%s] Resize Status : %v, Message : %s", expandedPool.Uuid, expandedPool.LastOperation.Status, expandedPool.LastOperation.Msg)
			if expandedPool.LastOperation.Status == opsapi.SdkStoragePool_OPERATION_FAILED {
				return nil, false, fmt.Errorf("pool %s expansion has failed. Error: %s", poolIDToResize, expandedPool.LastOperation)
			}
			if expandedPool.LastOperation.Status == opsapi.SdkStoragePool_OPERATION_PENDING {
				return nil, true, fmt.Errorf("pool %s is in pending state, waiting to start", poolIDToResize)
			}
			if expandedPool.LastOperation.Status == opsapi.SdkStoragePool_OPERATION_IN_PROGRESS {
				if strings.Contains(expandedPool.LastOperation.Msg, "Rebalance in progress") {
					if currentLastMsg == expandedPool.LastOperation.Msg {
						cnt += 1
					} else {
						cnt = 0
					}
					if cnt == 5 {
						return nil, false, fmt.Errorf("pool rebalance stuck at %s", currentLastMsg)
					}
					currentLastMsg = expandedPool.LastOperation.Msg

					return nil, true, fmt.Errorf("wait for pool rebalance to complete")
				}

				if strings.Contains(expandedPool.LastOperation.Msg, "No pending operation pool status: Maintenance") ||
					strings.Contains(expandedPool.LastOperation.Msg, "Storage rebalance complete pool status: Maintenance") {
					return nil, false, nil
				}

				return nil, true, fmt.Errorf("waiting for pool status to update")
			}
		}
		newPoolSize := expandedPool.TotalSize / units.GiB

		if newPoolSize >= initialSize {
			// storage pool resize has been completed
			return nil, false, nil
		}
		return nil, true, fmt.Errorf("pool has not been resized. Waiting...Current size is %d", newPoolSize)
	}

	_, err := task.DoRetryWithTimeout(f, 300*time.Minute, 2*time.Minute)
	return err
}

func getStoragePoolsToExpand() ([]*opsapi.StoragePool, error) {
	stNodes := node.GetStorageNodes()
	expectedCapacity := (len(stNodes) / 2) + 1
	poolsToExpand := make([]*opsapi.StoragePool, 0)
	for _, stNode := range stNodes {
		eligibility, err := GetPoolExpansionEligibility(&stNode)
		if err != nil {
			return nil, err
		}
		if len(poolsToExpand) <= expectedCapacity {
			log.Debugf("validating node [%s] for pool expansion", stNode.Id)
			if eligibility[stNode.Id] {
				for _, p := range stNode.Pools {
					log.Debugf("validating pool [%s] in node [%s] for pool expansion", p.Uuid, stNode.Id)
					if eligibility[p.Uuid] {
						log.Debugf("Marking pool [%s] for expansion", p.Uuid)
						poolsToExpand = append(poolsToExpand, p)
					}
				}
			}
			continue
		}
		break
	}
	return poolsToExpand, nil

}

// returns list of pools with metadata disk to expand
func getStorageMetadataPoolsToExpand() ([]*opsapi.StoragePool, error) {
	stNodes := node.GetStorageNodes()
	expectedCapacity := (len(stNodes) / 2) + 1
	poolsToExpand := make([]*opsapi.StoragePool, 0)
	for _, stNode := range stNodes {
		eligibility, err := GetPoolExpansionEligibility(&stNode)
		if err != nil {
			return nil, err
		}
		if len(poolsToExpand) <= expectedCapacity {
			if eligibility[stNode.Id] {
				for _, p := range stNode.Pools {
					metaPoolUUID, err := GetPoolUUIDWithMetadataDisk(stNode)
					if err != nil {
						return nil, err
					}
					if eligibility[p.Uuid] && p.Uuid == metaPoolUUID {
						poolsToExpand = append(poolsToExpand, p)
					}
				}
			}
			continue
		}
		break
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
		return
	}

	expansionType := "resize-disk"

	if resizeOperationType == 1 {
		expansionType = "add-disk"
	}

	if poolValidity {
		initialPoolSize := pool.TotalSize / units.GiB
		var pNode *node.Node

		dashStats := make(map[string]string)
		dashStats["pool-uuid"] = pool.Uuid
		dashStats["resize-operation"] = resizeOperationType.String()
		dashStats["resize-percentage"] = fmt.Sprintf("%d", chaosLevel)
		statType := stats.AddDiskEventName
		if resizeOperationType == opsapi.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK {
			statType = stats.ResizeDiskEventName
		}
		isDmthin, _ := IsDMthin()
		pNode, err = GetNodeFromPoolUUID(pool.Uuid)
		if err != nil {
			log.Error(err.Error())
			UpdateOutcome(event, err)
			return
		}
		if isDmthin && resizeOperationType == opsapi.SdkStoragePool_RESIZE_TYPE_ADD_DISK {
			err = EnterPoolMaintenance(*pNode)
			if err != nil {
				log.InfoD(fmt.Sprintf("Printing The storage pool status after failure of entering maintenance on Node:%s ", pNode.Name))
				PrintSvPoolStatus(*pNode)
				log.Error(err.Error())
				UpdateOutcome(event, err)
				return
			}
		}

		updateLongevityStats(event.Event.Type, statType, dashStats)
		err = Inst().V.ResizeStoragePoolByPercentage(pool.Uuid, resizeOperationType, uint64(chaosLevel))
		if err != nil {
			log.InfoD(fmt.Sprintf("Printing The storage pool status after pool resize failure on Node:%s ", pNode.Name))
			PrintSvPoolStatus(*pNode)
			err = fmt.Errorf("error initiating pool [%v ] %v: [%v]", pool.Uuid, expansionType, err.Error())
			log.Error(err.Error())
			UpdateOutcome(event, err)
		} else {
			if doNodeReboot {
				err = WaitForExpansionToStart(pool.Uuid)
				if err != nil {
					log.InfoD(fmt.Sprintf("Printing The storage pool status after pool resize failure on Node:%s ", pNode.Name))
					PrintSvPoolStatus(*pNode)
					log.Error(err.Error())
					UpdateOutcome(event, err)
				}

				if err == nil {
					storageNode, err := GetNodeWithGivenPoolID(pool.Uuid)
					if err != nil {
						log.Error(err.Error())
						UpdateOutcome(event, err)
					}
					dashStats = make(map[string]string)
					dashStats["node"] = storageNode.Name

					updateLongevityStats(event.Event.Type, stats.NodeRebootEventName, dashStats)
					if isDmthin && resizeOperationType == opsapi.SdkStoragePool_RESIZE_TYPE_ADD_DISK {
						//this is required as for Dmthin add-disk . pool will be in maintenance mode after node reboot
						err = RebootNodeAndWaitForPxDown(*storageNode)
					} else {
						err = RebootNodeAndWaitForPxUp(*storageNode)
					}
					if err != nil {
						log.Error(err.Error())
						UpdateOutcome(event, err)
					}
				}

			}
			err = waitForPoolToBeResized(initialPoolSize, pool.Uuid)
			if err != nil {
				log.InfoD(fmt.Sprintf("Printing The storage pool status on Node:%s  after pool resize failure", pNode.Name))
				PrintSvPoolStatus(*pNode)
				err = fmt.Errorf("pool [%v] %v failed. Error: %v", pool.Uuid, expansionType, err)
				UpdateOutcome(event, err)
			}

		}

		if pNode != nil && isDmthin && resizeOperationType == opsapi.SdkStoragePool_RESIZE_TYPE_ADD_DISK {
			err := ExitPoolMaintenance(*pNode)
			if err != nil {
				log.Error(err.Error())
				UpdateOutcome(event, err)
				return
			}
		}
	}
}

// TriggerMetadataPoolResizeDisk performs resize-disk on the storage pools for the given contexts
func TriggerMetadataPoolResizeDisk(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {
	defer ginkgo.GinkgoRecover()
	defer endLongevityTest()
	startLongevityTest(MetadataPoolResizeDisk)
	event := &EventRecord{
		Event: Event{
			ID:   GenerateUUID(),
			Type: MetadataPoolResizeDisk,
		},
		Start:   time.Now().Format(time.RFC1123),
		Outcome: []error{},
	}

	defer func() {
		event.End = time.Now().Format(time.RFC1123)
		*recordChan <- event
	}()

	setMetrics(*event)

	chaosLevel := getPoolExpandPercentage(MetadataPoolResizeDisk)
	stepLog := fmt.Sprintf("get storage pools and perform resize-disk by %v percentage on it ", chaosLevel)
	Step(stepLog, func() {

		poolsToBeResized, err := getStorageMetadataPoolsToExpand()

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

	validateContexts(event, contexts)
	updateMetrics(*event)
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
		nodeContexts := make([]*scheduler.Context, 0)
		if len(poolsToBeResized) > 0 {
			for _, pool := range poolsToBeResized {
				if !isPoolRebalanceEnabled(pool.Uuid) {
					poolToBeResized = pool
					break
				}
			}
			log.InfoD("Pool to resize-disk [%v]", poolToBeResized)
			storageNode, err := GetNodeWithGivenPoolID(poolToBeResized.Uuid)
			UpdateOutcome(event, err)

			if err == nil {
				appNodeContexts, err := GetContextsOnNode(contexts, storageNode)
				UpdateOutcome(event, err)
				nodeContexts = append(nodeContexts, appNodeContexts...)

			}
			initiatePoolExpansion(event, nil, poolToBeResized, chaosLevel, 2, true)
			log.InfoD(fmt.Sprintf("Printing The storage pool status after resizing disks in the pool and reboot on the node:%s", storageNode.Name))
			PrintSvPoolStatus(*storageNode)
		}
		err = ValidateDataIntegrity(&nodeContexts)
		UpdateOutcome(event, err)
	})

	validateContexts(event, contexts)
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
			Type: PoolAddDisk,
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
	validateContexts(event, contexts)
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
					break
				}
			}
			storageNode, err := GetNodeWithGivenPoolID(poolToBeResized.Uuid)
			UpdateOutcome(event, err)
			nodeContexts, err := GetContextsOnNode(contexts, storageNode)
			UpdateOutcome(event, err)

			log.InfoD("Pool to resize-disk [%v]", poolToBeResized)
			initiatePoolExpansion(event, nil, poolToBeResized, chaosLevel, 1, true)
			err = ValidateDataIntegrity(&nodeContexts)
			UpdateOutcome(event, err)
		}
	})

	validateContexts(event, contexts)
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

	apRule := aututils.PoolRuleRebalanceAbsolute(120, 70, false)
	apRule.Spec.ActionsCoolDownPeriod = int64(getReblanceCoolOffPeriod(AutopilotRebalance))

	if !autoPilotRuleCreated {
		log.InfoD("Creating autopilot rule ; %+v", apRule)
		_, err := Inst().S.CreateAutopilotRule(apRule)
		if err != nil {
			UpdateOutcome(event, err)
			return
		}
		autoPilotRuleCreated = true
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
			log.InfoD(fmt.Sprintf("Printing The storage pool status on reblanaced Node:%s ", autoPilotLabelNode.Name))
			PrintSvPoolStatus(autoPilotLabelNode)
			UpdateOutcome(event, err)
			err = ValidateRebalanceJobs(autoPilotLabelNode)
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
	Step(stepLog, func() {
		if len(Inst().UpgradeStorageDriverEndpointList) == 0 {
			log.Fatalf("Unable to perform volume driver upgrade hops, none were given")
		}
		for _, upgradeHop := range strings.Split(Inst().UpgradeStorageDriverEndpointList, ",") {
			stepLog = "start the volume driver upgrade"
			Step(stepLog, func() {
				log.InfoD(stepLog)
				dashStats := make(map[string]string)
				dashStats["upgrade-hop"] = upgradeHop
				updateLongevityStats(UpgradeVolumeDriver, stats.UpgradeVolumeDriverEventName, dashStats)
				err := Inst().V.UpgradeDriver(upgradeHop)
				if err != nil {
					log.InfoD("Error upgrading volume driver, Err: %v", err.Error())
				}
				UpdateOutcome(event, err)
				endpoint, version, err := SplitStorageDriverUpgradeURL(upgradeHop)
				if err != nil {
					log.InfoD("Error upgrading volume driver, Err: %v", err.Error())
				}
				log.InfoD("Updating StorageDriverUpgradeEndpoint: URL from [%s] to [%s], Version from [%s] to [%s]", Inst().StorageDriverUpgradeEndpointURL, endpoint, Inst().StorageDriverUpgradeEndpointVersion, version)
				Inst().StorageDriverUpgradeEndpointURL = endpoint
				Inst().StorageDriverUpgradeEndpointVersion = version
				err = ValidatePxLicenseSummary()
				if err != nil {
					err := fmt.Errorf("failed to validate license summary after upgrade. Err: [%v]", err)
					UpdateOutcome(event, err)
				}
			})
			validateContexts(event, contexts)
		}
	})
	err := ValidateDataIntegrity(contexts)
	UpdateOutcome(event, err)
	updateMetrics(*event)
}

// TriggerUpdateCluster upgrades the cluster and ensures everything is running fine
func TriggerUpdateCluster(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {
	defer ginkgo.GinkgoRecover()
	defer endLongevityTest()
	startLongevityTest(UpgradeCluster)
	event := &EventRecord{
		Event: Event{
			ID:   GenerateUUID(),
			Type: UpgradeCluster,
		},
		Start:   time.Now().Format(time.RFC1123),
		Outcome: []error{},
	}
	defer func() {
		event.End = time.Now().Format(time.RFC1123)
		*recordChan <- event
	}()
	setMetrics(*event)
	// This is an exact copy of UpgradeCluster test case
	Step("upgrade cluster and ensure everything is running fine", func() {
		var versions []string
		if len(Inst().SchedUpgradeHops) > 0 {
			versions = strings.Split(Inst().SchedUpgradeHops, ",")
		}
		for _, v := range versions {
			Step(fmt.Sprintf("start [%s] scheduler upgrade to version [%s]", Inst().S.String(), v), func() {
				stopSignal := make(chan struct{})

				var mError error
				opver, err := oputil.GetPxOperatorVersion()
				if err == nil && opver.GreaterThanOrEqual(PDBValidationMinOpVersion) {
					go DoPDBValidation(stopSignal, &mError)
					defer func() {
						close(stopSignal)
					}()
				} else {
					log.Warnf("PDB validation skipped. Current Px-Operator version: [%s], minimum required: [%s]. Error: [%v].", opver, PDBValidationMinOpVersion, err)
				}
				dashStats := make(map[string]string)
				dashStats["sched-upgrade-hop"] = v
				updateLongevityStats(UpgradeCluster, stats.UpdateClusterEventName, dashStats)
				err = Inst().S.UpgradeScheduler(v)
				if mError != nil {
					mError = fmt.Errorf("failed to validate PDB of px-storage during cluster upgrade. Err: [%v]", mError)
					UpdateOutcome(event, mError)
				}
				if err != nil {
					err = fmt.Errorf("failed to upgrade scheduler [%s] to version [%s]. Err: [%v]", Inst().S.String(), v, err)
					UpdateOutcome(event, err)
					return
				}

				// Sleep needed for AKS cluster upgrades
				if Inst().S.String() == aks.SchedName {
					log.Warnf("Warning! This is [%s] scheduler, during Node Pool upgrades, AKS creates extra node, this node then becomes PX node. "+
						"After the Node Pool upgrade is complete, AKS deletes this extra node, but PX Storage object still around for about ~20-30 mins. "+
						"Recommended config is that you deploy PX with 6 nodes in 3 zones and set MaxStorageNodesPerZone to 2, "+
						"so when extra AKS node gets created, PX gets deployed as Storageless node, otherwise if PX gets deployed as Storage node, "+
						"PX storage objects will never be deleted and validation might fail!", Inst().S.String())
					log.Infof("Sleeping for 30 minutes to let the cluster stabilize after the upgrade..")
					time.Sleep(30 * time.Minute)
				}

				// Sleep needed for GKE cluster upgrades
				if Inst().S.String() == gke.SchedName {
					log.Warnf("This is [%s] scheduler, during Node Pool upgrades, GKE creates an extra node. "+
						"After the Node Pool upgrade is complete, GKE deletes this extra node, but it takes some time.", Inst().S.String())
					log.Infof("Sleeping for 10 minutes to let the cluster stabilize after the upgrade..")
					time.Sleep(10 * time.Minute)
				}

				// Sleep needed for EKS cluster upgrades
				if Inst().S.String() == eks.SchedName {
					log.Warnf("This is [%s] scheduler, during Node Group upgrades, EKS creates an extra node. "+
						"After the Node Group upgrade is complete, EKS deletes this extra node, but it takes some time.", Inst().S.String())
					log.Infof("Sleeping for 30 minutes to let the cluster stabilize after the upgrade..")
					time.Sleep(30 * time.Minute)
				}

				// Sleep needed for IKS cluster upgrades
				if Inst().S.String() == iks.SchedName {
					log.Warnf("This is [%s] scheduler, during Worker Pool upgrades, IKS replaces all worker nodes. "+
						"The replacement might affect cluster capacity temporarily, requiring time for stabilization.", Inst().S.String())
					log.Infof("Sleeping for 30 minutes to let the cluster stabilize after the upgrade..")
					time.Sleep(30 * time.Minute)
				}

				PrintK8sClusterInfo()
			})

			Step("validate storage components", func() {
				urlToParse := fmt.Sprintf("%s/%s", Inst().StorageDriverUpgradeEndpointURL, Inst().StorageDriverUpgradeEndpointVersion)
				u, err := url.Parse(urlToParse)
				if err != nil {
					err = fmt.Errorf("error parsing PX version the url [%s]", urlToParse)
					UpdateOutcome(event, err)
					return
				}
				err = Inst().V.ValidateDriver(u.String(), true)
				if err != nil {
					err = fmt.Errorf("failed to validate volume driver after upgrade to %s. Err: [%v]", v, err)
					UpdateOutcome(event, err)
					return
				}

				// Printing cluster node info after the upgrade
				PrintK8sClusterInfo()
			})

			// TODO: This currently doesn't work for most distros and commenting out this change, see PTX-22409
			/*if Inst().S.String() != aks.SchedName {
			  dash.VerifyFatal(mError, nil, "validate no parallel upgrade of nodes")          }*/
			Step("update node drive endpoints", func() {
				// Update NodeRegistry, this is needed as node names and IDs might change after upgrade
				err := Inst().S.RefreshNodeRegistry()
				if err != nil {
					err = fmt.Errorf("failed to refresh node registry after upgrade to scheduler version [%s]. Err: [%v]", v, err)
					UpdateOutcome(event, err)
					return
				}

				// Refresh Driver Endpoints
				err = Inst().V.RefreshDriverEndpoints()
				if err != nil {
					err = fmt.Errorf("failed to refresh driver endpoints after upgrade to scheduler version [%s]. Err: [%v]", v, err)
					UpdateOutcome(event, err)
					return
				}

				// Printing pxctl status after the upgrade
				PrintPxctlStatus()
			})

			Step("Validate license summary against expected SKU and features", func() {
				log.Infof("Validating license summary against expected SKU and features")
				err := ValidatePxLicenseSummary()
				if err != nil {
					err := fmt.Errorf("failed to validate license summary against expected SKU and features. Err: [%v]", err)
					UpdateOutcome(event, err)
				}
			})

			Step("validate all apps after upgrade", func() {
				validateContexts(event, contexts)
			})
		}
	})

	err := ValidateDataIntegrity(contexts)
	UpdateOutcome(event, err)
	updateMetrics(*event)
}

// TriggerUpgradeVolumeDriverFromCatalog performs upgrade hops of volume driver based on a given list of upgradeEndpoints from marketplace
func TriggerUpgradeVolumeDriverFromCatalog(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {
	defer ginkgo.GinkgoRecover()
	defer endLongevityTest()
	startLongevityTest(UpgradeVolumeDriverFromCatalog)
	event := &EventRecord{
		Event: Event{
			ID:   GenerateUUID(),
			Type: UpgradeVolumeDriverFromCatalog,
		},
		Start:   time.Now().Format(time.RFC1123),
		Outcome: []error{},
	}
	defer func() {
		event.End = time.Now().Format(time.RFC1123)
		*recordChan <- event
	}()
	setMetrics(*event)
	// This is an exact copy of UpgradeVolumeDriverFromCatalog test case
	stepLog := "upgrade volume driver from catalog and ensure everything is running fine"
	Step(stepLog, func() {
		log.InfoD(stepLog)
		var (
			storageNodes      = node.GetStorageNodes()
			numOfNodes        = len(node.GetStorageDriverNodes())
			timeBeforeUpgrade time.Time
			timeAfterUpgrade  time.Time
		)
		validateContexts(event, contexts)
		stepLog = "start the upgrade of volume driver from catalog"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			if len(Inst().UpgradeStorageDriverEndpointList) == 0 {
				UpdateOutcome(event, fmt.Errorf("unable to perform volume driver upgrade hops, none were given"))
				return
			}
			if IsIksCluster() {
				log.Infof("Adding ibm helm repo [%s]", IBMHelmRepoName)
				cmd := fmt.Sprintf("helm repo add %s %s", IBMHelmRepoName, IBMHelmRepoURL)
				log.Infof("helm command: %v ", cmd)
				_, _, err := osutils.ExecShell(cmd)
				if err != nil {
					UpdateOutcome(event, fmt.Errorf("error adding helm repo [%s]. Err: [%v]", IBMHelmRepoName, err))
					return
				}
			} else {
				UpdateOutcome(event, fmt.Errorf("the cluster is neither IKS nor ROKS"))
				return
			}
			// Perform upgrade hops of volume driver based on a given list of upgradeEndpoints passed
			for i, upgradeHop := range strings.Split(Inst().UpgradeStorageDriverEndpointList, ",") {
				if upgradeHop == "" {
					UpdateOutcome(event, fmt.Errorf("the upgrade hop at index [%d] empty", i))
					return
				}
				currPXVersion, err := Inst().V.GetDriverVersionOnNode(storageNodes[0])
				if err != nil {
					UpdateOutcome(event, fmt.Errorf("error getting current driver version on node [%s], Err: [%v]", storageNodes[0].Name, err))
					return
				}
				timeBeforeUpgrade = time.Now()
				upgradeHopSplit := strings.Split(upgradeHop, "/")
				nextPXVersion := upgradeHopSplit[len(upgradeHopSplit)-1]
				if f, err := osutils.FileExists(IBMHelmValuesFile); err != nil {
					UpdateOutcome(event, fmt.Errorf("error checking for file [%s]. Err: [%v]", IBMHelmValuesFile, err))
					return
				} else {
					if f != nil {
						_, err = osutils.DeleteFile(IBMHelmValuesFile)
						if err != nil {
							UpdateOutcome(event, fmt.Errorf("error deleting file [%s]. Err: [%v]", IBMHelmValuesFile, err))
							return
						}
					}
				}
				pxNamespace, err := Inst().V.GetVolumeDriverNamespace()
				if err != nil {
					UpdateOutcome(event, fmt.Errorf("error getting portworx namespace. Err: [%v]", err))
					return
				}
				cmd := fmt.Sprintf("helm get values portworx -n %s > %s", pxNamespace, IBMHelmValuesFile)
				log.Infof("Running command: %v ", cmd)
				_, _, err = osutils.ExecShell(cmd)
				if err != nil {
					UpdateOutcome(event, fmt.Errorf("error getting values for portworx helm chart. Err: [%v]", err))
					return
				}
				f, err := osutils.FileExists(IBMHelmValuesFile)
				if err != nil {
					UpdateOutcome(event, fmt.Errorf("error checking for file [%s]. Err: [%v]", IBMHelmValuesFile, err))
					return
				}
				if f == nil {
					UpdateOutcome(event, fmt.Errorf("file [%s] does not exist", IBMHelmValuesFile))
					return
				}
				cmd = fmt.Sprintf("sed -i 's/imageVersion.*/imageVersion: %s/' %s", nextPXVersion, IBMHelmValuesFile)
				log.Infof("Running command: %v ", cmd)
				_, _, err = osutils.ExecShell(cmd)
				if err != nil {
					UpdateOutcome(event, fmt.Errorf("error updating px version in [%s]. Err: [%v]", IBMHelmValuesFile, err))
					return
				}
				dashStats := make(map[string]string)
				dashStats["upgrade-hop"] = upgradeHop
				updateLongevityStats(UpgradeVolumeDriverFromCatalog, stats.UpgradeVolumeDriverFromCatalogEventName, dashStats)
				cmd = fmt.Sprintf("helm upgrade portworx -n %s -f %s %s/portworx --debug", pxNamespace, IBMHelmValuesFile, IBMHelmRepoName)
				log.Infof("Running command: %v ", cmd)
				_, _, err = osutils.ExecShell(cmd)
				if err != nil {
					UpdateOutcome(event, fmt.Errorf("error running helm upgrade for portworx. Err: [%v]", err))
					return
				}
				time.Sleep(2 * time.Minute)
				stc, err := Inst().V.GetDriver()
				if err != nil {
					UpdateOutcome(event, fmt.Errorf("error getting storage cluster spec. Err: [%v]", err))
					return
				}
				k8sVersion, err := core.Instance().GetVersion()
				if err != nil {
					UpdateOutcome(event, fmt.Errorf("error getting k8s version. Err: [%v]", err))
					return
				}
				imageList, err := oputil.GetImagesFromVersionURL(upgradeHop, k8sVersion.String())
				if err != nil {
					UpdateOutcome(event, fmt.Errorf("error getting images using URL [%s] and k8s version [%s]. Err: [%v]", upgradeHop, k8sVersion.String(), err))
					return
				}
				storageClusterValidateTimeout := time.Duration(len(node.GetStorageDriverNodes())*9) * time.Minute
				err = oputil.ValidateStorageCluster(imageList, stc, storageClusterValidateTimeout, defaultRetryInterval, true)
				if err != nil {
					UpdateOutcome(event, fmt.Errorf("error validating storage cluster after upgrade. Err: [%v]", err))
					return
				}
				timeAfterUpgrade = time.Now()
				durationInMins := int(timeAfterUpgrade.Sub(timeBeforeUpgrade).Minutes())
				expectedUpgradeTime := 9 * len(node.GetStorageDriverNodes())
				upgradeStatus := "PASS"
				if durationInMins <= expectedUpgradeTime {
					log.InfoD("Upgrade successfully completed in %d minutes which is within %d minutes", durationInMins, expectedUpgradeTime)
				} else {
					log.Warnf("Upgrade took %d minutes more than expected time %d to complete minutes", durationInMins, expectedUpgradeTime)
					UpdateOutcome(event, fmt.Errorf("upgrade took [%v] minutes more than expected time [%v] to complete minutes", durationInMins, expectedUpgradeTime))
					upgradeStatus = "FAIL"
				}
				endpoint, version, err := SplitStorageDriverUpgradeURL(upgradeHop)
				if err != nil {
					log.InfoD("Error upgrading volume driver, Err: %v", err.Error())
				}
				log.InfoD("Updating from catalog StorageDriverUpgradeEndpoint: URL from [%s] to [%s], Version from [%s] to [%s]", Inst().StorageDriverUpgradeEndpointURL, endpoint, Inst().StorageDriverUpgradeEndpointVersion, version)
				Inst().StorageDriverUpgradeEndpointURL = endpoint
				Inst().StorageDriverUpgradeEndpointVersion = version
				err = ValidatePxLicenseSummary()
				if err != nil {
					err := fmt.Errorf("failed to validate license summary after upgrade. Err: [%v]", err)
					UpdateOutcome(event, err)
				}
				updatedPXVersion, err := Inst().V.GetDriverVersionOnNode(storageNodes[0])
				if err != nil {
					UpdateOutcome(event, fmt.Errorf("error getting updated driver version on node [%s], Err: [%v]", storageNodes[0].Name, err))
					return
				}
				majorVersion := strings.Split(currPXVersion, "-")[0]
				statsData := make(map[string]string)
				statsData["numOfNodes"] = fmt.Sprintf("%d", numOfNodes)
				statsData["fromVersion"] = currPXVersion
				statsData["toVersion"] = updatedPXVersion
				statsData["duration"] = fmt.Sprintf("%d mins", durationInMins)
				statsData["status"] = upgradeStatus
				dash.UpdateStats("px-upgrade-stats", "px-enterprise", "upgrade", majorVersion, statsData)
				// Validate Apps after volume driver upgrade
				validateContexts(event, contexts)
			}
		})
		err := ValidateDataIntegrity(contexts)
		UpdateOutcome(event, err)
		updateMetrics(*event)
	})
}

// TriggerUpgradeStork performs upgrade of the stork
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
				dashStats := make(map[string]string)
				dashStats["upgrade-hop"] = upgradeHop
				updateLongevityStats(UpgradeStork, stats.UpgradeStorkEventName, dashStats)
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
	Step(stepLog, func() {
		log.InfoD(stepLog)
		stepLog = "enable auto fstrim "
		Step(stepLog,
			func() {
				log.InfoD(stepLog)
				if !isAutoFsTrimEnabled {
					currNode := node.GetStorageDriverNodes()[0]
					err := Inst().V.SetClusterOpts(currNode, map[string]string{
						"--auto-fstrim": "on",
					})
					if err != nil {
						err = fmt.Errorf("error while enabling auto fstrim, Error:%v", err)
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
	Step(stepLog, func() {
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

//TriggerPowerOffVMs
func TriggerPowerOffAllVMs(contexts *[]*scheduler.Context, recordChan *chan *EventRecord){
	defer ginkgo.GinkgoRecover()
	defer endLongevityTest()
	startLongevityTest(PowerOffAllVMs)
	event := &EventRecord{
		Event: Event{
			ID:   GenerateUUID(),
			Type: PowerOffAllVMs,
		},
		Start:   time.Now().Format(time.RFC1123),
		Outcome: []error{},
	}
	defer func() {
		event.End = time.Now().Format(time.RFC1123)
		*recordChan <- event
	}()
	setMetrics(*event)
	stepLog := "Power off all worker nodes test "
	Step(stepLog, func() {
		log.Infof(stepLog)
		workerNodes:= node.GetWorkerNodes()
		var numberOfThread int = 5
		var numberOfNodePerThread int
		// If number of VMs to restarted is less than  numberOfThread then 
		// only one vm assigned to each thread, else assign  len(workerNodes)/numberOfThread
		// to per thread
		if len(workerNodes) < numberOfThread{
			numberOfThread = len(workerNodes)
			numberOfNodePerThread = 1
		}else{
			numberOfNodePerThread = len(workerNodes)/numberOfThread
		}
		var counter int = 0
		// Assign vms to every thread.
		nodesInThread := make([][]node.Node, numberOfThread)
		for t := 0; t < numberOfThread;  t++ {
			nodesInThread[t] = make([]node.Node, numberOfNodePerThread)
			for n:=0; n < numberOfNodePerThread; n++{
					nodesInThread[t][n] = workerNodes[counter]
					counter++
			}
		}
		//Create an additional thread for remainder. Example if 12 VMs, assign first 10 vms to 
		// 5 threads and assign remaining 2 vms 6th thread. 
		if counter < len(workerNodes){
			log.Infof("Additional nodes  : %d", len(workerNodes) - counter)
			additonalThread := make([]node.Node, len(workerNodes) - counter)
			var index int = 0
			for counter< len(workerNodes) {
				additonalThread[index] = workerNodes[counter]
				index++
				counter++
			}
			nodesInThread = append(nodesInThread, additonalThread)
			numberOfThread++
		}
		stepLog = "Power off all worker nodes in batches"
		Step(stepLog, func() {
			var poweroffwg sync.WaitGroup
			for i := 0; i < numberOfThread; i++ {
				poweroffwg.Add(1)
				go func(nodeList []node.Node) {
					defer poweroffwg.Done()
					for _, nodeInfo:= range nodeList{
							log.Infof("Node Name : %v", nodeInfo.Name)
							err:=Inst().N.PowerOffVM(nodeInfo)
							UpdateOutcome(event, err)
					}
				}(nodesInThread[i])
			}
			poweroffwg.Wait()
			log.Infof("Completed power off VMs")
			log.Infof("Wait for 5 minutes")
			time.Sleep(time.Duration(5 * time.Minute))
		})
		stepLog = "Power on all worker nodes"
		Step(stepLog, func() {
			var poweronwg sync.WaitGroup
			log.Infof("Poweron thread starts")
			for i := 0; i < numberOfThread; i++ {
				poweronwg.Add(1)
				go func(nodeList []node.Node) {
					defer poweronwg.Done()
					for _, nodeInfo:= range nodeList{
							log.Infof("Node Name : %v", nodeInfo.Name)
							err:=Inst().N.PowerOnVM(nodeInfo)
							UpdateOutcome(event, err)
					}
				}(nodesInThread[i])
			}
			poweronwg.Wait()
			log.Infof("Completed power on Nodes")
			for _, node := range workerNodes{
				err := Inst().S.IsNodeReady(node)
				err = Inst().V.WaitDriverUpOnNode(node, Inst().DriverStartTimeout)
				UpdateOutcome(event, err)
			}
		})
		stepLog = "Verify APP, volume staus and check data integrity if enabled"
		// //Wait for PX to be up on all worker nodes
		Step(stepLog, func() {
			for _, ctx := range *contexts {
				log.Infof("Validating context: %v", ctx.App.Key)
				ctx.SkipVolumeValidation = false
				errorChan:= make(chan error, errorChannelSize)
				ValidateContext(ctx, &errorChan)
				for err := range errorChan {
					UpdateOutcome(event, err)
				}
			}
			err:= ValidateDataIntegrity(contexts)
			UpdateOutcome(event, err)
	    })
		updateMetrics(*event)
	})
}

// TriggerVolumeUpdate enables to test volume update
func TriggerVolumeIOProfileUpdate(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {
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
	stepLog := "Validate IO profile update on volumes"
	Step(stepLog, func() {
		log.InfoD(stepLog)
		Step(stepLog,
			func() {
				log.InfoD(stepLog)
				var pvcProfileMap map[string]VolumeIOProfile
				pvcProfileMap, err := getIOProfileOnVolumes(contexts)
				if err != nil {
					UpdateOutcome(event, err)
				}
				for count := range [2]int{} {
					if count < 1 {
						updateIOProfile(event, pvcProfileMap, IOProfileChange[currentIOProfileChangeCounter])
						log.Infof("Update IO profile completed %s", IOProfileChange[currentIOProfileChangeCounter])
					} else {
						//Revert IO profile back
						revertIOProfile(event, pvcProfileMap)
					}
					for _, ctx := range *contexts {
						errorChan := make(chan error, errorChannelSize)
						log.Infof("Validating context: %v", ctx.App.Key)
						if count == 0 {
							ctx.SkipVolumeValidation = true
						} else {
							ctx.SkipVolumeValidation = false
						}
						ValidateContext(ctx, &errorChan)
						for err := range errorChan {
							UpdateOutcome(event, err)
						}
					}
					log.InfoD("Data integrity check has been started ")
					err := ValidateDataIntegrity(contexts)
					if err != nil {
						UpdateOutcome(event, err)
					} else {
						log.Infof("Data integrity check has been successful")
					}
					if count < 1 {
						log.InfoD("Wait for 15 minutes before resetting back")
						time.Sleep(15 * time.Minute)
						log.InfoD("Resetting IO profile back to %s ", IOProfileChange[currentIOProfileChangeCounter])
					}
				}
				currentIOProfileChangeCounter += 1
			})
		if currentIOProfileChangeCounter >= len(IOProfileChange) {
			//Reset back to 0
			currentIOProfileChangeCounter = 0
			log.InfoD("Resetting current counter ")
		}
	})
	log.InfoD("Update IO profile completed")
	updateMetrics(*event)
}

func getIOProfileOnVolumes(contexts *[]*scheduler.Context) (map[string]VolumeIOProfile, error) {
	pvcProfileMap := make(map[string]VolumeIOProfile)
	var appVolumes []*volume.Volume
	var err error
	var volumeInfo VolumeIOProfile
	for _, ctx := range *contexts {
		appVolumes, err = Inst().S.GetVolumes(ctx)
		if err != nil {
			log.Errorf("Error inspecting volume: %v", err)
			return nil, err
		}
		for _, v := range appVolumes {
			if v.ID == "" {
				log.InfoD("Volume info not available %v", v)
				continue
			}
			appVol, err := Inst().V.InspectVolume(v.ID)
			if err != nil {
				log.Errorf("Error inspecting volume: %v", err)
			}
			volumeInfo = VolumeIOProfile{v, appVol.Spec.IoProfile}
			pvcProfileMap[v.ID] = volumeInfo
		}
	}
	return pvcProfileMap, nil
}
func revertIOProfile(event *EventRecord, pvcProfileMap map[string]VolumeIOProfile) {
	var volumeSpec *apios.VolumeSpecUpdate
	for pvcName, v := range pvcProfileMap {
		log.InfoD("Getting info from volume: %s", pvcName)
		appVol, err := Inst().V.InspectVolume(pvcName)
		if err != nil {
			log.Errorf("Error inspecting volume: %v", err)
			UpdateOutcome(event, err)
		}
		log.InfoD("Volume: %s Current IO profile : %s, current derived IO profile %s", pvcName, appVol.Spec.IoProfile, appVol.DerivedIoProfile)
		if appVol.Spec.IoProfile != v.Profile {
			log.InfoD("Expected IO Profile change to  %v", v.Profile)
			volumeSpec = &apios.VolumeSpecUpdate{IoProfileOpt: &apios.VolumeSpecUpdate_IoProfile{IoProfile: v.Profile}}
			err = Inst().V.UpdateVolumeSpec(v.SpecInfo, volumeSpec)
			if err != nil {
				UpdateOutcome(event, err)
			}
			//Verify Volume set with required IO profile.
			appVol, err = Inst().V.InspectVolume(pvcName)
			if err != nil {
				log.Errorf("Error inspecting volume: %v", err)
				UpdateOutcome(event, err)
			}
			log.InfoD("IO profile after update %v", appVol.Spec.IoProfile.SimpleString())
			if v.Profile != appVol.Spec.IoProfile {
				err = fmt.Errorf("Failed to update volume %v with expected IO profile %v ", pvcName, v.Profile)
				UpdateOutcome(event, err)
			}

		}
	}
}
func updateIOProfile(event *EventRecord, pvcProfileMap map[string]VolumeIOProfile, ioProfileTo apios.IoProfile) {
	//Get all volumes and change IO profile on those volumes.
	var volumeSpec *apios.VolumeSpecUpdate
	for pvcName, v := range pvcProfileMap {
		log.InfoD("Getting info from volume: %s", pvcName)
		appVol, err := Inst().V.InspectVolume(pvcName)
		if err != nil {
			log.Errorf("Error inspecting volume: %v", err)
		}
		currentIOProfile := appVol.Spec.IoProfile
		derivedIOProfile := appVol.DerivedIoProfile
		log.InfoD("Volume: %s Current IO profile : %s, current derived IO profile %s", pvcName, currentIOProfile, derivedIOProfile)
		if currentIOProfile != ioProfileTo {
			if appVol.Spec.HaLevel == 1 && ioProfileTo == apios.IoProfile_IO_PROFILE_DB_REMOTE {
				log.InfoD(" HA of PVC  %v cannot be set to DB-REMOTE", pvcName)
			} else {
				log.InfoD("Expected IO Profile change to  %v", ioProfileTo)
				dashStats := make(map[string]string)
				dashStats["curr-io-profile"] = currentIOProfile.String()
				dashStats["derived-io-profile"] = derivedIOProfile.String()
				dashStats["new-io-profile"] = ioProfileTo.String()
				updateLongevityStats(event.Event.Type, stats.VolumeUpdateEventName, dashStats)
				volumeSpec = &apios.VolumeSpecUpdate{IoProfileOpt: &apios.VolumeSpecUpdate_IoProfile{IoProfile: ioProfileTo}}
				err = Inst().V.UpdateVolumeSpec(v.SpecInfo, volumeSpec)
				if err != nil {
					UpdateOutcome(event, err)
				}
				//Verify Volume set with required IO profile.
				appVol, err = Inst().V.InspectVolume(pvcName)
				if err != nil {
					log.Errorf("Error inspecting volume: %v", err)
					UpdateOutcome(event, err)
				}
				log.InfoD("IO profile after update %v", appVol.Spec.IoProfile.SimpleString())
				log.InfoD("Dervived IO profile after update %v", appVol.DerivedIoProfile.SimpleString())
				if ioProfileTo != appVol.Spec.IoProfile {
					err = fmt.Errorf("Failed to update volume %v with expected IO profile %v ", pvcName, ioProfileTo)
					UpdateOutcome(event, err)
				}
			}
			log.InfoD("Completed update on %v", pvcName)
		}
	}
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
						log.Debugf("Printing the volume inspect for the volume:%s ,volID:%s and namespace:%s after io prirority update", v.Name, v.ID, v.Namespace)
						PrintInspectVolume(v.ID)
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
		if strings.Contains(ctx.App.Key, "fstrim") {
			appVolumes, err := Inst().S.GetVolumes(ctx)
			if err != nil {
				UpdateOutcome(event, fmt.Errorf("error getting volumes for app %s: %v", ctx.App.Key, err))
				continue
			}
			if len(appVolumes) == 0 {
				UpdateOutcome(event, fmt.Errorf("found no volumes for app %s", ctx.App.Key))
				continue
			}

			for _, v := range appVolumes {
				// Skip autofstrim status on Pure DA volumes
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

				log.Infof("Getting volume %s inspect response", v.ID)
				appVol, err := Inst().V.InspectVolume(v.ID)
				if err != nil {
					UpdateOutcome(event, fmt.Errorf("error inspecting volume [%v]: %v", v.ID, err))
					continue
				}

				attachedNode := appVol.AttachedOn
				t := func() (interface{}, bool, error) {
					fsTrimStatuses, err := Inst().V.GetAutoFsTrimStatus(attachedNode)
					if err != nil {
						return nil, true, fmt.Errorf("error autofstrim status node %v status", attachedNode)
					}
					val, ok := fsTrimStatuses[appVol.Id]
					var fsTrimStatus opsapi.FilesystemTrim_FilesystemTrimStatus
					if !ok {
						fsTrimStatus, _ = waitForFsTrimStatus(event, attachedNode, appVol.Id)
					} else {
						fsTrimStatus = val
					}
					log.Infof("autofstrim status for volume %v, status: %v", appVol.Id, val.String())
					if fsTrimStatus != -1 {
						if fsTrimStatus == opsapi.FilesystemTrim_FS_TRIM_COMPLETED {
							return nil, false, nil
						} else if fsTrimStatus == opsapi.FilesystemTrim_FS_TRIM_FAILED {
							return nil, false, fmt.Errorf("autoFstrim failed for volume %v, status: %v", v.ID, val.String())
						} else {
							return nil, true, fmt.Errorf("current autofstrim status for volume %v is %v. Expected status is %v", v.ID, val.String(), opsapi.FilesystemTrim_FS_TRIM_COMPLETED)
						}
					} else {
						return nil, true, fmt.Errorf("autofstrim for volume %v not started yet", v.ID)
					}
				}
				_, err = task.DoRetryWithTimeout(t, defaultDriverStartTimeout, defaultRetryInterval)
				if err != nil {
					UpdateOutcome(event, err)
					return
				}
			}
		}
	}
}

func waitForFsTrimStatus(event *EventRecord, attachedNode, volumeID string) (opsapi.FilesystemTrim_FilesystemTrimStatus, error) {
	doExit := false
	exitCount := 5

	for !doExit {
		log.Infof("Autofstrim for volume %v not started, retrying after 2 mins", volumeID)
		time.Sleep(2 * time.Minute)
		fsTrimStatuses, err := Inst().V.GetAutoFsTrimStatus(attachedNode)
		if err != nil {
			if event != nil {
				UpdateOutcome(event, err)
			} else {
				return -1, nil
			}
		}

		fsTrimStatus, isValueExist := fsTrimStatuses[volumeID]
		if isValueExist {
			return fsTrimStatus, nil
		}
		if exitCount == 0 {
			doExit = true
		}
		exitCount--
	}
	return -1, nil
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

	Step(stepLog, func() {
		log.InfoD(stepLog)
		if !isTrashcanEnabled {
			stepLog = "enable trashcan"
			Step(stepLog,
				func() {
					log.InfoD(stepLog)
					currNode := node.GetStorageDriverNodes()[0]
					err := Inst().V.SetClusterOptsWithConfirmation(currNode, map[string]string{
						"--volume-expiration-minutes": "600",
					})
					if err != nil {
						err = fmt.Errorf("error while enabling trashcan, Error:%v", err)
						UpdateOutcome(event, err)

					} else {
						log.InfoD("Trashcan is successfully enabled")
						isTrashcanEnabled = true
					}

				})
		} else {
			var trashcanVols []string
			var err error
			node := node.GetStorageDriverNodes()[0]
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
	Step(stepLog, func() {
		log.InfoD(stepLog)
		if !isRelaxedReclaimEnabled {
			stepLog = "enable relaxed reclaim "
			Step(stepLog,
				func() {
					log.InfoD(stepLog)
					currNode := node.GetStorageDriverNodes()[0]
					err := Inst().V.SetClusterOptsWithConfirmation(currNode, map[string]string{
						"--relaxedreclaim-delete-seconds": "600",
					})
					if err != nil {
						log.Errorf(err.Error())
						err = fmt.Errorf("error while enabling relaxed reclaim, Error:%v", err)
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
					nodes := node.GetStorageDriverNodes()
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
		workerNodes = node.GetStorageDriverNodes()
		index := rand.Intn(len(workerNodes))
		nodeToDecomm = workerNodes[index]
		stepLog = fmt.Sprintf("decommission node %s", nodeToDecomm.Name)
		Step(stepLog, func() {
			log.InfoD(stepLog)
			nodeContexts, err := GetContextsOnNode(contexts, &nodeToDecomm)
			var suspendedScheds []*storkapi.VolumeSnapshotSchedule
			defer func() {
				if len(suspendedScheds) > 0 {
					for _, sched := range suspendedScheds {
						makeSuspend := false
						sched.Spec.Suspend = &makeSuspend
						_, err := storkops.Instance().UpdateSnapshotSchedule(sched)
						log.FailOnError(err, "error resuming volumes snapshot schedule for volume [%s] ", sched.Name)
					}
				}
			}()
			err = PrereqForNodeDecomm(nodeToDecomm, suspendedScheds)
			if err != nil {
				UpdateOutcome(event, err)
				return
			}
			err = Inst().S.PrepareNodeToDecommission(nodeToDecomm, Inst().Provisioner)
			if err != nil {
				UpdateOutcome(event, err)
				return
			}
			dashStats := make(map[string]string)
			dashStats["node"] = nodeToDecomm.Name
			updateLongevityStats(NodeDecommission, stats.NodeDecommEventName, dashStats)
			err = Inst().V.DecommissionNode(&nodeToDecomm)
			if err != nil {
				log.InfoD("Error while decommissioning the node: %v, Error:%v", nodeToDecomm.Name, err)
				UpdateOutcome(event, err)
			}

			t := func() (interface{}, bool, error) {
				status, err := Inst().V.GetNodeStatus(nodeToDecomm)
				if err != nil {
					return false, true, fmt.Errorf("error getting node %v status", nodeToDecomm.Name)
				}
				if *status == opsapi.Status_STATUS_NONE || *status == opsapi.Status_STATUS_OFFLINE {
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
			err = Inst().S.RefreshNodeRegistry()
			UpdateOutcome(event, err)
			err = Inst().V.RefreshDriverEndpoints()
			UpdateOutcome(event, err)
			err = ValidateDataIntegrity(&nodeContexts)
			UpdateOutcome(event, err)
		})
		updateMetrics(*event)
	})

	for _, ctx := range *contexts {

		Step(fmt.Sprintf("validating context after node: [%s] decommission",
			nodeToDecomm.Name), func() {
			errorChan := make(chan error, errorChannelSize)
			ctx.SkipVolumeValidation = true
			ValidateContext(ctx, &errorChan)
			ctx.SkipVolumeValidation = false
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
				//reboot required to remove encrypted dm devices if any
				err := Inst().N.RebootNode(decommissionedNode, node.RebootNodeOpts{
					Force: true,
					ConnectionOpts: node.ConnectionOpts{
						Timeout:         defaultTimeout,
						TimeBeforeRetry: defaultRetryInterval,
					},
				})

				if err != nil {
					log.Errorf("Error while rebooting node %s the node. error: %v", decommissionedNode.Name, err)
					UpdateOutcome(event, err)
				}
				dashStats := make(map[string]string)
				dashStats["node"] = decommissionedNode.Name
				updateLongevityStats(NodeRejoin, stats.NodeRejoinEventName, dashStats)
				err = Inst().V.RejoinNode(&decommissionedNode)

				if err != nil {
					log.InfoD("Error while rejoining the node. error: %v", err)
					UpdateOutcome(event, err)
				} else {

					log.InfoD("Waiting for node to rejoin and refresh inventory")
					time.Sleep(90 * time.Second)
					var rejoinedNode *opsapi.StorageNode
					t := func() (interface{}, bool, error) {
						latestNodes, err := Inst().V.GetDriverNodes()
						if err != nil {
							log.Errorf("Error getting px nodes, Error : %v", err)
							return "", true, err
						}

						for _, latestNode := range latestNodes {
							log.Infof("Inspecting Node: %v", latestNode.Hostname)
							if latestNode.Hostname == decommissionedNode.Hostname {
								rejoinedNode = latestNode
								return "", false, nil
							}
						}
						return "", true, fmt.Errorf("node %s node yet rejoined", decommissionedNode.Hostname)
					}
					_, err := task.DoRetryWithTimeout(t, 5*time.Minute, 1*time.Minute)

					if rejoinedNode == nil {
						err = fmt.Errorf("node %v rejoin failed,Error: %v", decommissionedNode.Hostname, err)
						log.Error(err.Error())
						UpdateOutcome(event, err)
					} else {
						err = Inst().S.RefreshNodeRegistry()
						UpdateOutcome(event, err)
						err = Inst().V.RefreshDriverEndpoints()
						UpdateOutcome(event, err)
						nodeWithNewID := node.Node{}

						for _, n := range node.GetStorageDriverNodes() {
							if n.Name == rejoinedNode.Hostname {
								nodeWithNewID = n
								break
							}
						}

						if nodeWithNewID.Hostname != "" {
							err = Inst().V.WaitDriverUpOnNode(nodeWithNewID, 10*time.Minute)
							UpdateOutcome(event, err)
							if err == nil {
								log.InfoD("node %v rejoin is successful ", decommissionedNode.Hostname)
							}
						} else {
							err = fmt.Errorf("node [%s] not found in the node registry after rejoining", decommissionedNode.Hostname)
						}
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
			ctx.SkipVolumeValidation = false
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
			var volumeSnapshotMap map[string]*volsnapv1.VolumeSnapshot
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
		interval = 45
	case 9:
		interval = 30
	case 10:
		interval = 20
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
	Step(stepLog, func() {
		log.InfoD(stepLog)
		stepLog = "Get KVDB nodes and perform failover"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			nodes := node.GetStorageDriverNodes()

			kvdbMembers, err := Inst().V.GetKvdbMembers(nodes[0])

			if err != nil {
				err = fmt.Errorf("error getting kvdb members using node %v. cause: %v", nodes[0].Name, err)
				log.InfoD(err.Error())
				UpdateOutcome(event, err)
			}

			log.InfoD("Validating initial KVDB members")

			allhealthy := validateKVDBMembers(event, kvdbMembers, false)

			if allhealthy {
				kvdbNodeIDMap := make(map[string]string, len(kvdbMembers))
				for id, m := range kvdbMembers {
					kvdbNodeIDMap[id] = m.Name
				}

				nodeMap := node.GetNodesByVoDriverNodeID()
				nodeContexts := make([]*scheduler.Context, 0)
				log.Infof("KVDB node map is [%v]", kvdbNodeIDMap)

				for kvdbID, nodeID := range kvdbNodeIDMap {
					kvdbNode := nodeMap[nodeID]

					appNodeContexts, err := GetContextsOnNode(contexts, &kvdbNode)
					nodeContexts = append(nodeContexts, appNodeContexts...)
					errorChan := make(chan error, errorChannelSize)
					dashStats := make(map[string]string)
					dashStats["node"] = kvdbNode.Name
					dashStats["kvdb"] = "true"
					updateLongevityStats(KVDBFailover, stats.PXRestartEventName, dashStats)
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
						m, ok := newKvdbMembers[kvdbID]

						if !ok && len(newKvdbMembers) > 0 {
							log.InfoD("node %v is no longer kvdb member", kvdbNode.Name)
							isKvdbStatusUpdated = true

						}
						if ok && !m.IsHealthy {
							log.InfoD("kvdb node %v isHealthy?: %v", nodeID, m.IsHealthy)
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

						_, ok := newKvdbMembers[kvdbID]
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
				err = ValidateDataIntegrity(&nodeContexts)
				UpdateOutcome(event, err)
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

	allHealthy := true

	if len(kvdbMembers) == 0 {
		err := fmt.Errorf("no KVDB membes to validate")
		UpdateOutcome(event, err)
		return false
	}
	log.InfoD("Current KVDB members are")
	for _, m := range kvdbMembers {
		log.InfoD(m.Name)
	}

	for _, m := range kvdbMembers {

		if !m.IsHealthy {
			err := fmt.Errorf("kvdb member node: %v is not healthy", m.Name)
			allHealthy = false
			log.Warn(err.Error())
			if isDestuctive {
				UpdateOutcome(event, err)
			}
		} else {
			log.InfoD("KVDB member node %v is healthy", m.Name)
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
	Step(stepLog, func() {
		log.InfoD(stepLog)
		for _, ctx := range *contexts {
			for i := 0; i < chaosLevel; i++ {
				stepLog = fmt.Sprintf("delete tasks for app: %s", ctx.App.Key)
				Step(stepLog, func() {
					log.InfoD(stepLog)
					dashStats := make(map[string]string)
					dashStats["task-name"] = ctx.App.Key
					updateLongevityStats(AppTasksDown, stats.DeletePodsEventName, dashStats)
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
					ctx.SkipClusterScopedObject = false
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

		for _, n := range node.GetStorageDriverNodes() {
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
					ctx.SkipVolumeValidation = false
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
	defer endLongevityTest()
	startLongevityTest(AsyncDR)
	defer ginkgo.GinkgoRecover()
	log.Infof("Async DR triggered at: %v", time.Now())
	defer ginkgo.GinkgoRecover()
	event := &EventRecord{
		Event: Event{
			ID:   GenerateUUID(),
			Type: AsyncDR,
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
		includeVolumesFlag    = true
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

	log.Info("Start migration")

	for i, currMigNamespace := range migrationNamespaces {
		migrationName := migrationKey + fmt.Sprintf("%d", i) + time.Now().Format("15h03m05s")
		currMig, err := asyncdr.CreateMigration(migrationName, currMigNamespace, asyncdr.DefaultClusterPairName, currMigNamespace, &includeVolumesFlag, &includeResourcesFlag, &startApplicationsFlag, nil)
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
			return
		}
		dashStats := stats.GetStorkMigrationStats(mig)
		updateLongevityStats(AsyncDR, stats.AsyncDREventName, dashStats)
	}
	updateMetrics(*event)
}

func TriggerMetroDR(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {
	defer endLongevityTest()
	startLongevityTest(MetroDR)
	defer ginkgo.GinkgoRecover()
	log.InfoD("Metro DR test triggered at: %v", time.Now())
	event := &EventRecord{
		Event: Event{
			ID:   GenerateUUID(),
			Type: MetroDR,
		},
		Start:   time.Now().Format(time.RFC1123),
		Outcome: []error{},
	}
	defer func() {
		event.End = time.Now().Format(time.RFC1123)
		*recordChan <- event
	}()

	setMetrics(*event)

	chaosLevel := ChaosMap[MetroDR]
	var (
		migrationNamespaces      []string
		taskNamePrefix           = "metro-dr-mig"
		allMigrations            []*storkapi.Migration
		includeVolumesFlag       = false
		includeResourcesFlag     = true
		startApplicationsFlag    = false
		clusterDomainWaitTimeout = 10 * time.Minute
		defaultWaitInterval      = 10 * time.Second
	)

	listCdsTask := func() (interface{}, bool, error) {
		// Fetch the cluster domains
		cdses, err := storkops.Instance().ListClusterDomainStatuses()
		if err != nil || len(cdses.Items) == 0 {
			log.Infof("Failed to list cluster domains statuses. Error: %v. List of cluster domains: %v", err, len(cdses.Items))
			return "", true, fmt.Errorf("failed to list cluster domains statuses")
		}
		cds := cdses.Items[0]
		if len(cds.Status.ClusterDomainInfos) == 0 {
			log.Infof("Found 0 cluster domain info objects in cluster domain status.")
			return "", true, fmt.Errorf("failed to list cluster domains statuses")
		}
		return "", false, nil
	}

	_, err := task.DoRetryWithTimeout(listCdsTask, clusterDomainWaitTimeout, defaultWaitInterval)
	if err != nil {
		UpdateOutcome(event, fmt.Errorf("Failed to get cluster domains status, Please check metro DR setup"))
		return
	}

	Step(fmt.Sprintf("Deploy applications for migration, with frequency: %v", chaosLevel), func() {
		// Write kubeconfig files after reading from the config maps created by torpedo deploy script
		err := asyncdr.WriteKubeconfigToFiles()
		if err != nil {
			log.Errorf("Failed to write kubeconfig: %v", err)
			return
		}
		err = SetSourceKubeConfig()
		if err != nil {
			log.Errorf("Failed to Set source kubeconfig: %v", err)
			return
		}
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			taskName := fmt.Sprintf("%s-%d", taskNamePrefix, i)
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
				ScheduleValidateClusterPair(appContexts[0], true, true, defaultClusterPairDir, false)
			})
		}

		log.Infof("Migration Namespaces: %v", migrationNamespaces)

	})

	log.InfoD("Start migration")

	for i, currMigNamespace := range migrationNamespaces {
		migrationName := metromigrationKey + fmt.Sprintf("%d", i) + time.Now().Format("15h03m05s")
		currMig, err := asyncdr.CreateMigration(migrationName, currMigNamespace, asyncdr.DefaultClusterPairName, currMigNamespace, &includeVolumesFlag, &includeResourcesFlag, &startApplicationsFlag, nil)
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
			return
		}
		dashStats := stats.GetStorkMigrationStats(mig)
		updateLongevityStats(MetroDR, stats.MetroDREventName, dashStats)
	}
	updateMetrics(*event)
}

// TriggerAsyncDRPXRestartSource triggers Async DR with PX restart on source
func TriggerAsyncDRPXRestartSource(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {
	defer endLongevityTest()
	startLongevityTest(AsyncDRPXRestartSource)
	defer ginkgo.GinkgoRecover()
	log.Infof("Async DR PX restart on source trigger triggered at: %v", time.Now())
	defer ginkgo.GinkgoRecover()
	event := &EventRecord{
		Event: Event{
			ID:   GenerateUUID(),
			Type: AsyncDRPXRestartSource,
		},
		Start:   time.Now().Format(time.RFC1123),
		Outcome: []error{},
	}
	defer func() {
		event.End = time.Now().Format(time.RFC1123)
		*recordChan <- event
	}()

	setMetrics(*event)

	chaosLevel := ChaosMap[AsyncDRPXRestartSource]
	var (
		migrationNamespaces   []string
		taskNamePrefix        = "async-dr-pxrs"
		allMigrations         []*storkapi.Migration
		includeVolumesFlag    = true
		includeResourcesFlag  = true
		startApplicationsFlag = false
	)

	Step(fmt.Sprintf("Deploy applications for migration, with frequency: %v", chaosLevel), func() {

		// Write kubeconfig files after reading from the config maps created by torpedo deploy script
		err := asyncdr.WriteKubeconfigToFiles()
		if err != nil {
			log.Errorf("Failed to write kubeconfig: %v", err)
			UpdateOutcome(event, err)
			return
		}

		err = SetSourceKubeConfig()
		if err != nil {
			log.Errorf("Failed to Set source kubeconfig: %v", err)
			UpdateOutcome(event, err)
			return
		}
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

	log.InfoD("Start migration")

	for i, currMigNamespace := range migrationNamespaces {
		migrationName := migrationKey + fmt.Sprintf("%d", i) + time.Now().Format("15h03m05s")
		currMig, err := asyncdr.CreateMigration(migrationName, currMigNamespace, asyncdr.DefaultClusterPairName, currMigNamespace, &includeVolumesFlag, &includeResourcesFlag, &startApplicationsFlag, nil)
		if err != nil {
			UpdateOutcome(event, fmt.Errorf("failed to create migration: %s in namespace %s. Error: [%v]", migrationKey, currMigNamespace, err))
			return
		} else {
			allMigrations = append(allMigrations, currMig)
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
		}
	}

	// Validate all migrations
	for _, mig := range allMigrations {
		err := storkops.Instance().ValidateMigration(mig.Name, mig.Namespace, migrationRetryTimeout, migrationRetryInterval)
		if err != nil {
			UpdateOutcome(event, fmt.Errorf("failed to validate migration: %s in namespace %s. Error: [%v]", mig.Name, mig.Namespace, err))
			return
		}
		dashStats := stats.GetStorkMigrationStats(mig)
		updateLongevityStats(AsyncDRPXRestartSource, stats.AsyncDREventName, dashStats)
	}
	updateMetrics(*event)
}

// TriggerAsyncDRPXRestartDest triggers Async DR with PX restart on destination
func TriggerAsyncDRPXRestartDest(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {
	defer endLongevityTest()
	startLongevityTest(AsyncDRPXRestartDest)
	defer ginkgo.GinkgoRecover()
	log.Infof("Async DR PX restart on destination trigger triggered at: %v", time.Now())
	defer ginkgo.GinkgoRecover()
	event := &EventRecord{
		Event: Event{
			ID:   GenerateUUID(),
			Type: AsyncDRPXRestartDest,
		},
		Start:   time.Now().Format(time.RFC1123),
		Outcome: []error{},
	}
	defer func() {
		event.End = time.Now().Format(time.RFC1123)
		*recordChan <- event
	}()

	setMetrics(*event)

	chaosLevel := ChaosMap[AsyncDRPXRestartDest]
	var (
		migrationNamespaces   []string
		taskNamePrefix        = "async-dr-pxrd"
		allMigrations         []*storkapi.Migration
		includeVolumesFlag    = true
		includeResourcesFlag  = true
		startApplicationsFlag = false
	)

	Step(fmt.Sprintf("Deploy applications for migration, with frequency: %v", chaosLevel), func() {

		// Write kubeconfig files after reading from the config maps created by torpedo deploy script
		err := asyncdr.WriteKubeconfigToFiles()
		if err != nil {
			log.Errorf("Failed to write kubeconfig: %v", err)
			UpdateOutcome(event, err)
			return
		}

		err = SetSourceKubeConfig()
		if err != nil {
			log.Errorf("Failed to Set source kubeconfig: %v", err)
			UpdateOutcome(event, err)
			return
		}
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

	log.InfoD("Start migration")

	for i, currMigNamespace := range migrationNamespaces {
		migrationName := migrationKey + fmt.Sprintf("%d", i) + time.Now().Format("15h03m05s")
		currMig, err := asyncdr.CreateMigration(migrationName, currMigNamespace, asyncdr.DefaultClusterPairName, currMigNamespace, &includeVolumesFlag, &includeResourcesFlag, &startApplicationsFlag, nil)
		if err != nil {
			UpdateOutcome(event, fmt.Errorf("failed to create migration: %s in namespace %s. Error: [%v]", migrationKey, currMigNamespace, err))
			return
		} else {
			allMigrations = append(allMigrations, currMig)
			Step("Restart Portworx", func() {
				err = SetDestinationKubeConfig()
				if err != nil {
					log.Errorf("Failed to Set destination kubeconfig: %v", err)
					UpdateOutcome(event, err)
					return
				}
				nodes := node.GetStorageDriverNodes()
				nodeIndex := rand.Intn(len(nodes))
				log.Infof("Stop volume driver [%s] on node: [%s]", Inst().V.String(), nodes[nodeIndex].Name)
				StopVolDriverAndWait([]node.Node{nodes[nodeIndex]})
				log.Infof("Starting volume driver [%s] on node [%s]", Inst().V.String(), nodes[nodeIndex].Name)
				StartVolDriverAndWait([]node.Node{nodes[nodeIndex]})
				log.Infof("Giving a few seconds for volume driver to stabilize")
				time.Sleep(20 * time.Second)
				err = SetSourceKubeConfig()
				if err != nil {
					log.Errorf("Failed to Set source kubeconfig: %v", err)
					return
				}
			})
		}
	}

	// Validate all migrations
	for _, mig := range allMigrations {
		err := storkops.Instance().ValidateMigration(mig.Name, mig.Namespace, migrationRetryTimeout, migrationRetryInterval)
		if err != nil {
			UpdateOutcome(event, fmt.Errorf("failed to validate migration: %s in namespace %s. Error: [%v]", mig.Name, mig.Namespace, err))
			return
		}
		dashStats := stats.GetStorkMigrationStats(mig)
		updateLongevityStats(AsyncDRPXRestartDest, stats.AsyncDREventName, dashStats)
	}
	updateMetrics(*event)
}

// TriggerAsyncDRPXRestartKvdb triggers Async DR with kvdb restart
func TriggerAsyncDRPXRestartKvdb(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {
	defer endLongevityTest()
	startLongevityTest(AsyncDRPXRestartKvdb)
	defer ginkgo.GinkgoRecover()
	log.Infof("Async DR kvdb restart trigger triggered at: %v", time.Now())
	defer ginkgo.GinkgoRecover()
	event := &EventRecord{
		Event: Event{
			ID:   GenerateUUID(),
			Type: AsyncDRPXRestartKvdb,
		},
		Start:   time.Now().Format(time.RFC1123),
		Outcome: []error{},
	}
	defer func() {
		event.End = time.Now().Format(time.RFC1123)
		*recordChan <- event
	}()

	setMetrics(*event)

	chaosLevel := ChaosMap[AsyncDRPXRestartKvdb]
	var (
		migrationNamespaces   []string
		taskNamePrefix        = "async-dr-rkvdb"
		allMigrations         []*storkapi.Migration
		includeVolumesFlag    = true
		includeResourcesFlag  = true
		startApplicationsFlag = false
	)

	Step(fmt.Sprintf("Deploy applications for migration, with frequency: %v", chaosLevel), func() {

		// Write kubeconfig files after reading from the config maps created by torpedo deploy script
		err := asyncdr.WriteKubeconfigToFiles()
		if err != nil {
			log.Errorf("Failed to write kubeconfig: %v", err)
			UpdateOutcome(event, err)
			return
		}

		err = SetSourceKubeConfig()
		if err != nil {
			log.Errorf("Failed to Set source kubeconfig: %v", err)
			UpdateOutcome(event, err)
			return
		}
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

	log.InfoD("Collect KVDB node")
	kvdbNodes, err := GetAllKvdbNodes()
	if err != nil {
		log.Infof("Getting kvdb nodes throwing error, err: %v", err)
		return
	}
	stNodes := node.GetNodesByVoDriverNodeID()
	var appNode node.Node
	for _, kvdbNode := range kvdbNodes {
		var ok bool
		appNode, ok = stNodes[kvdbNode.ID]
		if ok {
			break
		}
	}

	log.InfoD("Start migration")

	for i, currMigNamespace := range migrationNamespaces {
		migrationName := migrationKey + fmt.Sprintf("%d", i) + time.Now().Format("15h03m05s")
		currMig, err := asyncdr.CreateMigration(migrationName, currMigNamespace, asyncdr.DefaultClusterPairName, currMigNamespace, &includeVolumesFlag, &includeResourcesFlag, &startApplicationsFlag, nil)
		if err != nil {
			UpdateOutcome(event, fmt.Errorf("failed to create migration: %s in namespace %s. Error: [%v]", migrationKey, currMigNamespace, err))
			return
		} else {
			allMigrations = append(allMigrations, currMig)
			Step(fmt.Sprintf("stop volume driver %s on node: %s", Inst().V.String(), appNode.Name), func() {
				StopVolDriverAndWait([]node.Node{appNode})
				log.Infof("Starting volume driver [%s] on node [%s]", Inst().V.String(), appNode.Name)
				StartVolDriverAndWait([]node.Node{appNode})
				log.Infof("Giving a few seconds for volume driver to stabilize")
				time.Sleep(20 * time.Second)
			})
		}
	}

	// Validate all migrations
	for _, mig := range allMigrations {
		err := storkops.Instance().ValidateMigration(mig.Name, mig.Namespace, migrationRetryTimeout, migrationRetryInterval)
		if err != nil {
			UpdateOutcome(event, fmt.Errorf("failed to validate migration: %s in namespace %s. Error: [%v]", mig.Name, mig.Namespace, err))
			return
		}
		dashStats := stats.GetStorkMigrationStats(mig)
		updateLongevityStats(AsyncDRPXRestartKvdb, stats.AsyncDREventName, dashStats)
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
			Type: AsyncDRVolumeOnly,
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
		includeVolumesFlag    = true
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
		migrationName := migrationKey + fmt.Sprintf("%d", i) + time.Now().Format("15h03m05s")
		currMig, err := asyncdr.CreateMigration(migrationName, currMigNamespace, asyncdr.DefaultClusterPairName, currMigNamespace, &includeVolumesFlag, &includeResourcesFlag, &startApplicationsFlag, nil)
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
			return
		}
		resp, get_mig_err := storkops.Instance().GetMigration(mig.Name, mig.Namespace)
		if get_mig_err != nil {
			UpdateOutcome(event, fmt.Errorf("failed to get migration: %s in namespace %s. Error: [%v]", mig.Name, mig.Namespace, get_mig_err))
			return
		}
		volumesMigrated := resp.Status.Summary.NumberOfMigratedVolumes
		resourcesMigrated := resp.Status.Summary.NumberOfMigratedResources
		expectedresourcesMigrated := 2 * volumesMigrated
		if resourcesMigrated != expectedresourcesMigrated {
			UpdateOutcome(event, fmt.Errorf("Number of resources migrated should be %d, got %d", expectedresourcesMigrated, resourcesMigrated))
		} else {
			log.InfoD("Number of resources migrated: %d", resourcesMigrated)
		}
		dashStats := stats.GetStorkMigrationStats(mig)
		updateLongevityStats(AsyncDRVolumeOnly, stats.AsyncDREventName, dashStats)
	}
	updateMetrics(*event)
}

func TriggerStorkApplicationBackup(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {
	defer endLongevityTest()
	startLongevityTest(StorkApplicationBackup)
	defer ginkgo.GinkgoRecover()
	log.InfoD("Stork Application Backup triggered at: %v", time.Now())
	event := &EventRecord{
		Event: Event{
			ID:   GenerateUUID(),
			Type: StorkApplicationBackup,
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
				return
			}
			bkp_comp_err := applicationbackup.WaitForAppBackupCompletion(backupname, currbkNamespace, timeout)
			if bkp_comp_err != nil {
				UpdateOutcome(event, fmt.Errorf("backup successful failed with %v", bkp_comp_err))
				return
			}
			log.InfoD("backup successful, backup name - %v, backup location - %v", backupname, backuplocationname)
			dashStats, err := stats.GetStorkBackupStats(backupname, currbkNamespace)
			if err != nil {
				log.InfoD("Not able to get stats, err: %v", err)
			}
			updateLongevityStats(StorkApplicationBackup, stats.StorkApplicationBackupEventName, dashStats)
		}
	})
	updateMetrics(*event)
}

func TriggerStorkAppBkpVolResize(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {
	defer endLongevityTest()
	startLongevityTest(StorkAppBkpVolResize)
	defer ginkgo.GinkgoRecover()
	log.InfoD("Stork Application Backup with volume resize triggered at: %v", time.Now())
	event := &EventRecord{
		Event: Event{
			ID:   GenerateUUID(),
			Type: StorkAppBkpVolResize,
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
				dashStats, err := stats.GetStorkBackupStats(backupname, currbkNamespace)
				if err != nil {
					log.InfoD("Not able to get stats, err: %v", err)
				}
				updateLongevityStats(StorkAppBkpVolResize, stats.StorkApplicationBackupEventName, dashStats)
			}
		}
	})
	updateMetrics(*event)
}

func TriggerStorkAppBkpHaUpdate(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {
	defer endLongevityTest()
	startLongevityTest(StorkAppBkpHaUpdate)
	defer ginkgo.GinkgoRecover()
	log.InfoD("Stork Application Backup with HA update triggered at: %v", time.Now())
	event := &EventRecord{
		Event: Event{
			ID:   GenerateUUID(),
			Type: StorkAppBkpHaUpdate,
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
				bkp, bkp_create_err := applicationbackup.CreateApplicationBackup(backupname, currbkNamespace, currBackupLocation)
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
						bkp_comp_err := applicationbackup.WaitForAppBackupCompletion(bkp.Name, bkp.Namespace, timeout)
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
				dashStats, err := stats.GetStorkBackupStats(backupname, currbkNamespace)
				if err != nil {
					log.InfoD("Not able to get stats, err: %v", err)
				}
				updateLongevityStats(StorkAppBkpHaUpdate, stats.StorkApplicationBackupEventName, dashStats)
			}
		}
	})
	updateMetrics(*event)
}

func TriggerStorkAppBkpPxRestart(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {
	defer endLongevityTest()
	startLongevityTest(StorkAppBkpPxRestart)
	defer ginkgo.GinkgoRecover()
	log.InfoD("Stork Application Backup with PX restart triggered at: %v", time.Now())
	event := &EventRecord{
		Event: Event{
			ID:   GenerateUUID(),
			Type: StorkAppBkpPxRestart,
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
				bkp, bkp_create_err := applicationbackup.CreateApplicationBackup(backupname, currbkNamespace, currBackupLocation)
				if bkp_create_err != nil {
					UpdateOutcome(event, fmt.Errorf("backup creation failed with %v", bkp_create_err))
					return
				}
				bkp_start_err := applicationbackup.WaitForAppBackupToStart(bkp.Name, bkp.Namespace, timeout)
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
				bkp_comp_err := applicationbackup.WaitForAppBackupCompletion(bkp.Name, bkp.Namespace, timeout)
				if bkp_comp_err != nil {
					UpdateOutcome(event, fmt.Errorf("backup completion failed with %v", bkp_comp_err))
					return
				}
				log.InfoD("backup successful and px restart injected during backup successfully, backup name - %v, backup location - %v", bkp.Name, currBackupLocation.Name)
				dashStats, err := stats.GetStorkBackupStats(backupname, currbkNamespace)
				if err != nil {
					log.InfoD("Not able to get stats, err: %v", err)
				}
				updateLongevityStats(StorkAppBkpPxRestart, stats.StorkApplicationBackupEventName, dashStats)
			}
		}
	})
	updateMetrics(*event)
}

func TriggerStorkAppBkpPoolResize(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {
	defer endLongevityTest()
	startLongevityTest(StorkAppBkpPoolResize)
	defer ginkgo.GinkgoRecover()
	log.InfoD("Stork Application Backup with Pool resize triggered at: %v", time.Now())
	event := &EventRecord{
		Event: Event{
			ID:   GenerateUUID(),
			Type: StorkAppBkpPoolResize,
		},
		Start:   time.Now().Format(time.RFC1123),
		Outcome: []error{},
	}
	defer func() {
		event.End = time.Now().Format(time.RFC1123)
		*recordChan <- event
	}()
	setMetrics(*event)
	chaosLevel := ChaosMap[StorkAppBkpPoolResize]

	var (
		s3SecretName   = "s3secret"
		timeout        = 5 * time.Minute
		taskNamePrefix = "stork-appbkp-poolresize"
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
				bkp, bkp_create_err := applicationbackup.CreateApplicationBackup(backupname, currbkNamespace, currBackupLocation)
				if bkp_create_err != nil {
					UpdateOutcome(event, fmt.Errorf("backup creation failed with %v", bkp_create_err))
					return
				}
				bkp_start_err := applicationbackup.WaitForAppBackupToStart(bkp.Name, bkp.Namespace, timeout)
				if bkp_start_err == nil {
					chaosLevel := getPoolExpandPercentage(StorkAppBkpPoolResize)
					stepLog := fmt.Sprintf("get storage pools and perform resize-disk by %v percentage on it ", chaosLevel)
					Step(stepLog, func() {
						poolsToBeResized, err := getStoragePoolsToExpand()
						if err != nil {
							log.Error(err.Error())
							UpdateOutcome(event, err)
							return
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
					})
				} else {
					UpdateOutcome(event, fmt.Errorf("backup start fail %v", bkp_start_err))
					return
				}
				bkp_comp_err := applicationbackup.WaitForAppBackupCompletion(bkp.Name, bkp.Namespace, timeout)
				if bkp_comp_err != nil {
					UpdateOutcome(event, fmt.Errorf("backup completion failed with %v", bkp_comp_err))
					return
				}
				log.InfoD("backup successful and pool resize injected during backup successfully, backup name - %v, backup location - %v", bkp.Name, currBackupLocation.Name)
				dashStats, err := stats.GetStorkBackupStats(backupname, currbkNamespace)
				if err != nil {
					log.InfoD("Not able to get stats, err: %v", err)
				}
				updateLongevityStats(StorkAppBkpPoolResize, stats.StorkApplicationBackupEventName, dashStats)
			}
		}
	})
	updateMetrics(*event)
}

func TriggerConfluentAsyncDR(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {
	defer endLongevityTest()
	startLongevityTest(ConfluentAsyncDR)
	defer ginkgo.GinkgoRecover()
	log.InfoD("Confluent CRD Async DR triggered at: %v", time.Now())
	event := &EventRecord{
		Event: Event{
			ID:   GenerateUUID(),
			Type: ConfluentAsyncDR,
		},
		Start:   time.Now().Format(time.RFC1123),
		Outcome: []error{},
	}
	defer func() {
		event.End = time.Now().Format(time.RFC1123)
		*recordChan <- event
	}()

	setMetrics(*event)

	chaosLevel := ChaosMap[ConfluentAsyncDR]
	stepLog := "Export kubeconfigs"
	Step(stepLog, func() {
		log.InfoD(stepLog)
		// Write kubeconfig files after reading from the config maps created by torpedo deploy script
		err := asyncdr.WriteKubeconfigToFiles()
		if err != nil {
			UpdateOutcome(event, fmt.Errorf("Failed to write kubeconfig, err: %v", err))
			return
		}
		err = SetSourceKubeConfig()
		if err != nil {
			UpdateOutcome(event, fmt.Errorf("Failed to set source config, err: %v", err))
			return
		}
	})
	stepLog = fmt.Sprintf("Deploy applications with %v chaos level", chaosLevel)
	Step(stepLog, func() {
		log.InfoD(stepLog)
		appName := "confluent"
		appPath := "https://raw.githubusercontent.com/confluentinc/confluent-kubernetes-examples/master/quickstart-deploy/confluent-platform.yaml"
		appData := asyncdr.GetAppData(appName)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			log.InfoD("Preparing apps now")
			pods_created, err := asyncdr.PrepareApp(appName, appPath)
			if err != nil {
				UpdateOutcome(event, fmt.Errorf("Failed to create app pods, err: %v", err))
				return
			}
			err = ValidateCRMigration(pods_created, appData)
			if err != nil {
				UpdateOutcome(event, fmt.Errorf("Failed to validate the Crs, err: %v", err))
				return
			}
			err = DeleteCrAndRepo(appData, appPath)
			if err != nil {
				UpdateOutcome(event, fmt.Errorf("Failed to delete the Crs, err: %v", err))
				return
			}
		}
	})
}

func TriggerKafkaAsyncDR(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {
	defer endLongevityTest()
	startLongevityTest(KafkaAsyncDR)
	defer ginkgo.GinkgoRecover()
	log.InfoD("Kafka CRD Async DR triggered at: %v", time.Now())
	event := &EventRecord{
		Event: Event{
			ID:   GenerateUUID(),
			Type: KafkaAsyncDR,
		},
		Start:   time.Now().Format(time.RFC1123),
		Outcome: []error{},
	}
	defer func() {
		event.End = time.Now().Format(time.RFC1123)
		*recordChan <- event
	}()

	setMetrics(*event)

	chaosLevel := ChaosMap[KafkaAsyncDR]
	stepLog := "Export kubeconfigs"
	Step(stepLog, func() {
		log.InfoD(stepLog)
		// Write kubeconfig files after reading from the config maps created by torpedo deploy script
		err := asyncdr.WriteKubeconfigToFiles()
		if err != nil {
			UpdateOutcome(event, fmt.Errorf("Failed to write kubeconfig, err: %v", err))
			return
		}
		err = SetSourceKubeConfig()
		if err != nil {
			UpdateOutcome(event, fmt.Errorf("Failed to set source config, err: %v", err))
			return
		}
	})
	stepLog = fmt.Sprintf("Deploy applications with %v chaos level", chaosLevel)
	Step(stepLog, func() {
		log.InfoD(stepLog)
		appName := "kafka"
		appPath := "/torpedo/deployments/customconfigs/kafkacr.yaml"
		appData := asyncdr.GetAppData(appName)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			log.InfoD("Preparing apps now")
			pods_created, err := asyncdr.PrepareApp(appName, appPath)
			if err != nil {
				UpdateOutcome(event, fmt.Errorf("Failed to create app pods, err: %v", err))
				return
			}
			err = ValidateCRMigration(pods_created, appData)
			if err != nil {
				UpdateOutcome(event, fmt.Errorf("Failed to validate the Crs, err: %v", err))
				return
			}
			err = DeleteCrAndRepo(appData, appPath)
			if err != nil {
				UpdateOutcome(event, fmt.Errorf("Failed to delete the Crs, err: %v", err))
				return
			}
		}
	})
}

func TriggerMongoAsyncDR(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {
	defer endLongevityTest()
	startLongevityTest(MongoAsyncDR)
	defer ginkgo.GinkgoRecover()
	log.InfoD("Mongo CRD Async DR triggered at: %v", time.Now())
	event := &EventRecord{
		Event: Event{
			ID:   GenerateUUID(),
			Type: MongoAsyncDR,
		},
		Start:   time.Now().Format(time.RFC1123),
		Outcome: []error{},
	}
	defer func() {
		event.End = time.Now().Format(time.RFC1123)
		*recordChan <- event
	}()

	setMetrics(*event)

	chaosLevel := ChaosMap[MongoAsyncDR]
	stepLog := "Export kubeconfigs"
	Step(stepLog, func() {
		log.InfoD(stepLog)
		// Write kubeconfig files after reading from the config maps created by torpedo deploy script
		err := asyncdr.WriteKubeconfigToFiles()
		if err != nil {
			UpdateOutcome(event, fmt.Errorf("Failed to write kubeconfig, err: %v", err))
			return
		}
		err = SetSourceKubeConfig()
		if err != nil {
			UpdateOutcome(event, fmt.Errorf("Failed to set source config, err: %v", err))
			return
		}
	})
	stepLog = fmt.Sprintf("Deploy applications with %v chaos level", chaosLevel)
	Step(stepLog, func() {
		log.InfoD(stepLog)
		appName := "mongo"
		appPath := "/root/tornew/torpedo/deployments/customconfigs/mongocr.yaml"
		appData := asyncdr.GetAppData(appName)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			log.InfoD("Preparing apps now")
			pods_created, err := asyncdr.PrepareApp(appName, appPath)
			if err != nil {
				UpdateOutcome(event, fmt.Errorf("Failed to create app pods, err: %v", err))
				return
			}
			err = ValidateCRMigration(pods_created, appData)
			if err != nil {
				UpdateOutcome(event, fmt.Errorf("Failed to validate the Crs, err: %v", err))
				return
			}
			err = DeleteCrAndRepo(appData, appPath)
			if err != nil {
				UpdateOutcome(event, fmt.Errorf("Failed to delete the Crs, err: %v", err))
				return
			}
		}
	})
}

func TriggerAutoFsTrimAsyncDR(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {
	defer endLongevityTest()
	startLongevityTest(AutoFsTrimAsyncDR)
	defer ginkgo.GinkgoRecover()
	log.InfoD("Async DR with autofstrim volumes triggered at: %v", time.Now())
	event := &EventRecord{
		Event: Event{
			ID:   GenerateUUID(),
			Type: AutoFsTrimAsyncDR,
		},
		Start:   time.Now().Format(time.RFC1123),
		Outcome: []error{},
	}
	defer func() {
		event.End = time.Now().Format(time.RFC1123)
		*recordChan <- event
	}()

	setMetrics(*event)

	var (
		migrationNamespaces   []string
		taskNamePrefix        = "adr-fstrim"
		allMigrations         []*storkapi.Migration
		includeVolumesFlag    = true
		includeResourcesFlag  = true
		startApplicationsFlag = false
		appVolumes            []*volume.Volume
	)
	chaosLevel := ChaosMap[AutoFsTrimAsyncDR]
	Step(fmt.Sprintf("Deploy applications for migration, with frequency: %v", chaosLevel), func() {

		// Write kubeconfig files after reading from the config maps created by torpedo deploy script
		err := asyncdr.WriteKubeconfigToFiles()
		if err != nil {
			UpdateOutcome(event, fmt.Errorf("failed to write kubeconfig: %v", err))
			return
		}
		err = SetSourceKubeConfig()
		if err != nil {
			UpdateOutcome(event, fmt.Errorf("failed to Set source kubeconfig: %v", err))
			return
		}

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
				// Get Autofstrim status for vols
				appVolumes, err = Inst().S.GetVolumes(ctx)
				if err != nil {
					UpdateOutcome(event, fmt.Errorf("not able to get appVolumes, err is: %v", err))
					return
				}
				for _, vol := range appVolumes {
					cVol, err := Inst().V.InspectVolume(vol.ID)
					if err != nil {
						UpdateOutcome(event, fmt.Errorf("error inspecting volume %v", vol.Name))
						return
					}
					if !cVol.Spec.AutoFstrim {
						UpdateOutcome(event, fmt.Errorf("fstrim should be enable for volume %v, please use spec which use autofstrim volumes", vol.Name))
						return
					}
				}
				Step("Create cluster pair between source and destination clusters", func() {
					// Set cluster context to cluster where torpedo is running
					ScheduleValidateClusterPair(appContexts[0], false, true, defaultClusterPairDir, false)
				})
			}
		}
	})

	log.Infof("Migration Namespaces: %v", migrationNamespaces)
	log.Infof("Start migration")

	for i, currMigNamespace := range migrationNamespaces {
		migrationName := migrationKey + fmt.Sprintf("%d", i) + time.Now().Format("15h03m05s")
		currMig, err := asyncdr.CreateMigration(migrationName, currMigNamespace, asyncdr.DefaultClusterPairName, currMigNamespace, &includeVolumesFlag, &includeResourcesFlag, &startApplicationsFlag, nil)
		if err != nil {
			UpdateOutcome(event, fmt.Errorf("failed to create migration: %s in namespace %s. Error: [%v]", migrationKey, currMigNamespace, err))
			return
		} else {
			allMigrations = append(allMigrations, currMig)
		}
	}

	// Validate all migrations
	for _, mig := range allMigrations {
		err := storkops.Instance().ValidateMigration(mig.Name, mig.Namespace, migrationRetryTimeout, migrationRetryInterval)
		if err != nil {
			UpdateOutcome(event, fmt.Errorf("failed to validate migration: %s in namespace %s. Error: [%v]", mig.Name, mig.Namespace, err))
			dashStats := stats.GetStorkMigrationStats(mig)
			updateLongevityStats(AutoFsTrimAsyncDR, stats.AsyncDREventName, dashStats)
		}
	}

	// Validate autofstrim on destination volumes
	err := SetDestinationKubeConfig()
	if err != nil {
		UpdateOutcome(event, fmt.Errorf("failed to Set destination kubeconfig: %v", err))
		return
	}
	for _, vol := range appVolumes {
		cVol, err := Inst().V.InspectVolume(vol.ID)
		if err != nil {
			UpdateOutcome(event, fmt.Errorf("unable to inspect volume %v, err is %v", vol.Name, err))
			return
		}
		dash.VerifyFatal(cVol.Spec.AutoFstrim, true, fmt.Sprintf("fstrim should be enable for volume %v, It is %v on volume", vol.Name, cVol.Spec.AutoFstrim))
		dash.VerifyFatal(cVol.Spec.Nodiscard, true, fmt.Sprintf("nodiscard should be enable for volume %v, It is %v on volume", vol.Name, cVol.Spec.Nodiscard))
	}
	err = SetSourceKubeConfig()
	if err != nil {
		UpdateOutcome(event, fmt.Errorf("failed to Set Source kubeconfig post test completion: %v", err))
		return
	}
	updateMetrics(*event)
}

func TriggerIopsBwAsyncDR(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {
	defer endLongevityTest()
	startLongevityTest(IopsBwAsyncDR)
	defer ginkgo.GinkgoRecover()
	log.InfoD("Async DR with volumes having Iops and BW  triggered at: %v", time.Now())
	event := &EventRecord{
		Event: Event{
			ID:   GenerateUUID(),
			Type: IopsBwAsyncDR,
		},
		Start:   time.Now().Format(time.RFC1123),
		Outcome: []error{},
	}
	defer func() {
		event.End = time.Now().Format(time.RFC1123)
		*recordChan <- event
	}()

	setMetrics(*event)

	var (
		migrationNamespaces   []string
		taskNamePrefix        = "adr-iops"
		allMigrations         []*storkapi.Migration
		includeVolumesFlag    = true
		includeResourcesFlag  = true
		startApplicationsFlag = false
		appVolumes            []*volume.Volume
		expected_iot          *apios.IoThrottle
	)
	chaosLevel := ChaosMap[IopsBwAsyncDR]
	Step(fmt.Sprintf("Deploy applications for migration, with frequency: %v", chaosLevel), func() {

		// Write kubeconfig files after reading from the config maps created by torpedo deploy script
		err := asyncdr.WriteKubeconfigToFiles()
		if err != nil {
			UpdateOutcome(event, fmt.Errorf("Failed to write kubeconfig: %v", err))
			return
		}
		err = SetSourceKubeConfig()
		if err != nil {
			UpdateOutcome(event, fmt.Errorf("Failed to Set source kubeconfig: %v", err))
			return
		}

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
				// Get Autofstrim status for vols
				appVolumes, err = Inst().S.GetVolumes(ctx)
				if err != nil {
					UpdateOutcome(event, fmt.Errorf("not able to get appVolumes, err is: %v", err))
					return
				}
				for _, vol := range appVolumes {
					cVol, err := Inst().V.InspectVolume(vol.ID)
					if err != nil {
						UpdateOutcome(event, fmt.Errorf("error inspecting volume %v", vol.Name))
						return
					}
					if cVol.Spec.IoThrottle == nil {
						UpdateOutcome(event, fmt.Errorf("iothrottle value should present in volume %v for this test, please use spec which use volumes with iothrottle", vol.Name))
						return
					}
					expected_iot = cVol.Spec.IoThrottle
					break
				}
				Step("Create cluster pair between source and destination clusters", func() {
					// Set cluster context to cluster where torpedo is running
					ScheduleValidateClusterPair(appContexts[0], false, true, defaultClusterPairDir, false)
				})
			}
		}
	})

	log.Infof("Migration Namespaces: %v", migrationNamespaces)
	log.Infof("Start migration")

	for i, currMigNamespace := range migrationNamespaces {
		migrationName := migrationKey + fmt.Sprintf("%d", i) + time.Now().Format("15h03m05s")
		currMig, err := asyncdr.CreateMigration(migrationName, currMigNamespace, asyncdr.DefaultClusterPairName, currMigNamespace, &includeVolumesFlag, &includeResourcesFlag, &startApplicationsFlag, nil)
		if err != nil {
			UpdateOutcome(event, fmt.Errorf("failed to create migration: %s in namespace %s. Error: [%v]", migrationKey, currMigNamespace, err))
			return
		} else {
			allMigrations = append(allMigrations, currMig)
		}
	}

	// Validate all migrations
	for _, mig := range allMigrations {
		err := storkops.Instance().ValidateMigration(mig.Name, mig.Namespace, migrationRetryTimeout, migrationRetryInterval)
		if err != nil {
			UpdateOutcome(event, fmt.Errorf("failed to validate migration: %s in namespace %s. Error: [%v]", mig.Name, mig.Namespace, err))
			dashStats := stats.GetStorkMigrationStats(mig)
			updateLongevityStats(IopsBwAsyncDR, stats.AsyncDREventName, dashStats)
		}
	}

	// Validate Iops and Bw on destination volumes
	err := SetDestinationKubeConfig()
	if err != nil {
		UpdateOutcome(event, fmt.Errorf("failed to Set destination kubeconfig: %v", err))
		return
	}
	for _, vol := range appVolumes {
		cVol, err := Inst().V.InspectVolume(vol.ID)
		if err != nil {
			UpdateOutcome(event, fmt.Errorf("error inspecting volume %v, err is %v", vol.Name, err))
		}
		actual_iot := cVol.Spec.IoThrottle
		if actual_iot.ReadBwMbytes != expected_iot.ReadBwMbytes {
			UpdateOutcome(event, fmt.Errorf("read bw on volume %v, expected: %v, got: %v", vol.Name, expected_iot.ReadBwMbytes, actual_iot.ReadBwMbytes))
		}
		if actual_iot.WriteIops != expected_iot.WriteIops {
			UpdateOutcome(event, fmt.Errorf("write iops on volume %v, expected: %v, got: %v", vol.Name, expected_iot.WriteIops, actual_iot.WriteIops))
		}
	}
	err = SetSourceKubeConfig()
	if err != nil {
		UpdateOutcome(event, fmt.Errorf("failed to Set Source kubeconfig post test completion: %v", err))
		return
	}
	updateMetrics(*event)
}

func TriggerDeleteOldNamespaces(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {
	defer endLongevityTest()
	startLongevityTest(DeleteOldNamespaces)
	defer ginkgo.GinkgoRecover()
	log.InfoD("DeleteOldNamespaces triggered at: %v", time.Now())
	event := &EventRecord{
		Event: Event{
			ID:   GenerateUUID(),
			Type: DeleteOldNamespaces,
		},
		Start:   time.Now().Format(time.RFC1123),
		Outcome: []error{},
	}
	defer func() {
		event.End = time.Now().Format(time.RFC1123)
		*recordChan <- event
	}()
	setMetrics(*event)

	if CreatedBeforeTimeforNS != 0 {
		Step("Collect and Delete Ns on source", func() {
			nss := asyncdr.CollectNsForDeletion(map[string]string{"creator": "torpedo"}, time.Duration(CreatedBeforeTimeforNS))
			log.InfoD("collected these namespaces %v, to be deleted on source cluster, they are created [%v Hours] ago", nss, CreatedBeforeTimeforNS)
			asyncdr.WaitForNamespaceDeletion(nss)
		})
		Step("Collect and Delete Ns on destination", func() {
			SetDestinationKubeConfig()
			nsd := asyncdr.CollectNsForDeletion(map[string]string{"creator": "torpedo"}, time.Duration(CreatedBeforeTimeforNS))
			log.InfoD("collected these namespaces %v, to be deleted on destination cluster, they are created [%v Hours] ago", nsd, CreatedBeforeTimeforNS)
			asyncdr.WaitForNamespaceDeletion(nsd)
		})
	} else {
		log.InfoD("Skipping deletion of old NS as CreatedBeforeTimeforNS is not set or set to 0")
		return
	}
}

func TriggerAsyncDRMigrationSchedule(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {
	defer endLongevityTest()
	startLongevityTest(AsyncDRMigrationSchedule)
	defer ginkgo.GinkgoRecover()
	log.InfoD("Async DR Migrationschedule triggered at: %v", time.Now())
	event := &EventRecord{
		Event: Event{
			ID:   GenerateUUID(),
			Type: AsyncDRMigrationSchedule,
		},
		Start:   time.Now().Format(time.RFC1123),
		Outcome: []error{},
	}
	defer func() {
		event.End = time.Now().Format(time.RFC1123)
		*recordChan <- event
	}()

	setMetrics(*event)

	var (
		migrationNamespaces   []string
		taskNamePrefix        = "async-dr-mig-sched"
		allMigrationsSched    = make(map[string]string)
		includeResourcesFlag  = true
		includeVolumesFlag    = true
		startApplicationsFlag = false
		scpolName             = "async-policy"
		suspendSched          = false
		autoSuspend           = false
		schdPol               *storkapi.SchedulePolicy
		err                   error
		makeSuspend           = true
	)
	chaosLevel := ChaosMap[AsyncDRMigrationSchedule]

	Step(fmt.Sprintf("Deploy applications for migration, with frequency: %v", chaosLevel), func() {
		err = asyncdr.WriteKubeconfigToFiles()
		if err != nil {
			UpdateOutcome(event, fmt.Errorf("failed to write kubeconfig: %v", err))
			return
		}
		err = SetSourceKubeConfig()
		if err != nil {
			UpdateOutcome(event, fmt.Errorf("failed to Set source kubeconfig: %v", err))
			return
		}
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
		log.InfoD("Migration Namespaces: %v", migrationNamespaces)
	})

	Step("Create Schedule Policy", func() {
		schdPol, err = asyncdr.CreateSchedulePolicy(scpolName, MigrationInterval)
		if err != nil {
			UpdateOutcome(event, fmt.Errorf("schedule policy creation error: %v", err))
			return
		} else {
			log.InfoD("schedule Policy created with %v mins of interval", MigrationInterval)
		}
	})

	Step("Create Migration Schedule", func() {
		for i, currMigNamespace := range migrationNamespaces {
			migrationScheduleName := migrationKey + "schedule-" + fmt.Sprintf("%d", i)
			currMigSched, createMigSchedErr := asyncdr.CreateMigrationSchedule(
				migrationScheduleName, currMigNamespace, asyncdr.DefaultClusterPairName, currMigNamespace, &includeVolumesFlag,
				&includeResourcesFlag, &startApplicationsFlag, schdPol.Name, &suspendSched, autoSuspend,
				nil, nil, nil, nil, nil, "", "", nil, nil, nil)
			if createMigSchedErr != nil {
				UpdateOutcome(event, fmt.Errorf("failed to create migrationschedule wit error %v", err))
				return
			}
			allMigrationsSched[currMigNamespace] = currMigSched.Name
			time.Sleep(30 * time.Second)
			migSchedResp, err := storkops.Instance().GetMigrationSchedule(currMigSched.Name, currMigNamespace)
			if err != nil {
				UpdateOutcome(event, fmt.Errorf("failed to get migrationschedule, error: %v", err))
				return
			}
			if len(migSchedResp.Status.Items) == 0 {
				UpdateOutcome(event, fmt.Errorf("0 migrations have yet run for the migration schedule"))
				return
			}
			expectedMigs, migScheduleStats, err := asyncdr.WaitForNumOfMigration(migSchedResp.Name, currMigNamespace, MigrationsCount, MigrationInterval)
			if err != nil {
				UpdateOutcome(event, fmt.Errorf("couldn't complete %v migrations due to error: %v", MigrationsCount, err))
				return
			} else {
				for mig, status := range expectedMigs {
					if status != "Successful" {
						UpdateOutcome(event, fmt.Errorf("migration [%v] did not complete successfully, All migrations with status are: %v",
							mig, expectedMigs))
					}
				}
			}
			storkops.Instance().ValidateMigrationSchedule(migSchedResp.Name, currMigNamespace, migrationRetryTimeout, migrationRetryInterval)
			for _, dashStats := range migScheduleStats {
				updateLongevityStats(AsyncDRMigrationSchedule, stats.AsyncDREventName, dashStats)
			}
		}
	})

	Step("Suspend Migration Schedule", func() {
		log.InfoD("All of these migrationschedules need to be suspended: %v", allMigrationsSched)
		for namespace, mig := range allMigrationsSched {
			migrationSchedule, err := storkops.Instance().GetMigrationSchedule(mig, namespace)
			if err != nil {
				UpdateOutcome(event, fmt.Errorf("failed to get migrationschedule"))
			}
			migrationSchedule.Spec.Suspend = &makeSuspend
			_, err = storkops.Instance().UpdateMigrationSchedule(migrationSchedule)
			if err != nil {
				UpdateOutcome(event, fmt.Errorf("couldn't suspend migration %v due to error %v", migrationSchedule.Name, err))
			} else {
				log.InfoD("migrationSchedule %v, suspended successfully", migrationSchedule.Name)
			}
		}
	})
}

func TriggerMetroDRMigrationSchedule(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {
	defer endLongevityTest()
	startLongevityTest(MetroDRMigrationSchedule)
	defer ginkgo.GinkgoRecover()
	log.InfoD("Metro DR Migrationschedule triggered at: %v", time.Now())
	event := &EventRecord{
		Event: Event{
			ID:   GenerateUUID(),
			Type: MetroDRMigrationSchedule,
		},
		Start:   time.Now().Format(time.RFC1123),
		Outcome: []error{},
	}
	defer func() {
		event.End = time.Now().Format(time.RFC1123)
		*recordChan <- event
	}()

	setMetrics(*event)

	var (
		migrationNamespaces      []string
		taskNamePrefix           = "metro-dr-mig-sched"
		clusterDomainWaitTimeout = 10 * time.Minute
		defaultWaitInterval      = 10 * time.Second
		allMigrationsSched       = make(map[string]string)
		includeVolumesFlag       = false
		includeResourcesFlag     = true
		startApplicationsFlag    = false
		scpolName                = "async-policy"
		suspendSched             = false
		autoSuspend              = false
		schdPol                  *storkapi.SchedulePolicy
		err                      error
		makeSuspend              = true
	)

	listCdsTask := func() (interface{}, bool, error) {
		// Fetch the cluster domains
		cdses, err := storkops.Instance().ListClusterDomainStatuses()
		if err != nil || len(cdses.Items) == 0 {
			log.Infof("Failed to list cluster domains statuses. Error: %v. List of cluster domains: %v", err, len(cdses.Items))
			return "", true, fmt.Errorf("failed to list cluster domains statuses")
		}
		cds := cdses.Items[0]
		if len(cds.Status.ClusterDomainInfos) == 0 {
			log.Infof("Found 0 cluster domain info objects in cluster domain status.")
			return "", true, fmt.Errorf("failed to list cluster domains statuses")
		}
		return "", false, nil
	}

	_, err = task.DoRetryWithTimeout(listCdsTask, clusterDomainWaitTimeout, defaultWaitInterval)
	if err != nil {
		UpdateOutcome(event, fmt.Errorf("Failed to get cluster domains status, Please check metro DR setup"))
		return
	}

	chaosLevel := ChaosMap[MetroDRMigrationSchedule]

	Step(fmt.Sprintf("Deploy applications for migration, with frequency: %v", chaosLevel), func() {
		err = asyncdr.WriteKubeconfigToFiles()
		if err != nil {
			UpdateOutcome(event, fmt.Errorf("failed to write kubeconfig: %v", err))
			return
		}
		err = SetSourceKubeConfig()
		if err != nil {
			UpdateOutcome(event, fmt.Errorf("failed to Set source kubeconfig: %v", err))
			return
		}
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
				ScheduleValidateClusterPair(appContexts[0], true, true, defaultClusterPairDir, false)
			})
		}
		log.InfoD("Migration Namespaces: %v", migrationNamespaces)
	})

	Step("Create Schedule Policy", func() {
		schdPol, err = asyncdr.CreateSchedulePolicy(scpolName, MigrationInterval)
		if err != nil {
			UpdateOutcome(event, fmt.Errorf("schedule policy creation error: %v", err))
			return
		} else {
			log.InfoD("schedule Policy created with %v mins of interval", MigrationInterval)
		}
	})

	Step("Create Migration Schedule", func() {
		for i, currMigNamespace := range migrationNamespaces {
			migrationScheduleName := metromigrationKey + "schedule-" + fmt.Sprintf("%d", i)
			currMigSched, createMigSchedErr := asyncdr.CreateMigrationSchedule(
				migrationScheduleName, currMigNamespace, asyncdr.DefaultClusterPairName, currMigNamespace, &includeVolumesFlag,
				&includeResourcesFlag, &startApplicationsFlag, schdPol.Name, &suspendSched, autoSuspend,
				nil, nil, nil, nil, nil, "", "", nil, nil, nil)
			if createMigSchedErr != nil {
				UpdateOutcome(event, fmt.Errorf("failed to create migrationschedule wit error %v", err))
				return
			}
			allMigrationsSched[currMigNamespace] = currMigSched.Name
			time.Sleep(30 * time.Second)
			migSchedResp, err := storkops.Instance().GetMigrationSchedule(currMigSched.Name, currMigNamespace)
			if err != nil {
				UpdateOutcome(event, fmt.Errorf("failed to get migrationschedule, error: %v", err))
				return
			}
			if len(migSchedResp.Status.Items) == 0 {
				UpdateOutcome(event, fmt.Errorf("0 migrations have yet run for the migration schedule"))
				return
			}
			expectedMigs, migScheduleStats, err := asyncdr.WaitForNumOfMigration(migSchedResp.Name, currMigNamespace, MigrationsCount, MigrationInterval)
			if err != nil {
				UpdateOutcome(event, fmt.Errorf("couldn't complete %v migrations due to error: %v", MigrationsCount, err))
				return
			} else {
				for mig, status := range expectedMigs {
					if status != "Successful" {
						UpdateOutcome(event, fmt.Errorf("migration [%v] did not complete successfully, All migrations with status are: %v",
							mig, expectedMigs))
					}
				}
			}
			storkops.Instance().ValidateMigrationSchedule(migSchedResp.Name, currMigNamespace, migrationRetryTimeout, migrationRetryInterval)
			for _, dashStats := range migScheduleStats {
				updateLongevityStats(MetroDRMigrationSchedule, stats.MetroDREventName, dashStats)
			}
		}
	})

	Step("Suspend Migration Schedule", func() {
		log.InfoD("All of these migrationschedules need to be suspended: %v", allMigrationsSched)
		for namespace, mig := range allMigrationsSched {
			migrationSchedule, err := storkops.Instance().GetMigrationSchedule(mig, namespace)
			if err != nil {
				UpdateOutcome(event, fmt.Errorf("failed to get migrationschedule"))
			}
			migrationSchedule.Spec.Suspend = &makeSuspend
			_, err = storkops.Instance().UpdateMigrationSchedule(migrationSchedule)
			if err != nil {
				UpdateOutcome(event, fmt.Errorf("couldn't suspend migration %v due to error %v", migrationSchedule.Name, err))
			} else {
				log.InfoD("migrationSchedule %v, suspended successfully", migrationSchedule.Name)
			}
		}
	})
}

// AggrVolDepReplResizeOps crashes vol driver
func TriggerAggrVolDepReplResizeOps(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {
	/*
	   TO run this test please make sure to run the applications which creates aggr volumes
	*/
	defer ginkgo.GinkgoRecover()
	defer endLongevityTest()
	startLongevityTest(AggrVolDepReplResizeOps)
	event := &EventRecord{
		Event: Event{
			ID:   GenerateUUID(),
			Type: AggrVolDepReplResizeOps,
		},
		Start:   time.Now().Format(time.RFC1123),
		Outcome: []error{},
	}

	defer func() {
		event.End = time.Now().Format(time.RFC1123)
		*recordChan <- event
	}()

	setMetrics(*event)

	stepLog := "Repl factor update and Volume Update on aggregated volume"
	Step(stepLog, func() {
		log.InfoD(stepLog)
		log.Infof("Starting test case here !!")

		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			*contexts = append(*contexts, ScheduleApplications(fmt.Sprintf("aggrvoldeprepresize-%d", i))...)
		}
		ValidateApplications(*contexts)

		allVolsCreated := []*volume.Volume{}
		for _, eachContext := range *contexts {
			vols, err := Inst().S.GetVolumes(eachContext)
			if err != nil {
				UpdateOutcome(event, fmt.Errorf("Failed to get app %s's volumes", eachContext.App.Key))
				return
			}
			for _, eachVol := range vols {
				aggrLevel, err := Inst().V.GetAggregationLevel(eachVol)
				if err != nil {
					UpdateOutcome(event, fmt.Errorf("failed with error while checking for aggr level on volume [%v]", aggrLevel))
					return
				}
				// Pick volumes with aggr level > 1
				if aggrLevel > 1 {
					allVolsCreated = append(allVolsCreated, eachVol)
				}
			}
		}

		if len(allVolsCreated) < 1 {
			err := fmt.Errorf("no volumes created with aggregation level > 1 in the contexts")
			UpdateOutcome(event, fmt.Errorf("volume with aggregator level > 1 created? Error [%v]", err))
			return
		}

		teardownContext := func() {
			opts := make(map[string]bool)
			opts[scheduler.OptionsWaitForResourceLeakCleanup] = true

			for _, ctx := range *contexts {
				TearDownContext(ctx, opts)
			}
		}
		defer teardownContext()

		setReplFactor := func(volName *volume.Volume) {
			volAggrLevel, err := Inst().V.GetAggregationLevel(volName)
			if err != nil {
				UpdateOutcome(event, fmt.Errorf("failed with error while checking for aggr level on volume [%v]", volAggrLevel))
				return
			}

			getReplicaSets, err := Inst().V.GetReplicaSets(volName)
			if err != nil {
				UpdateOutcome(event, fmt.Errorf("Failed to get replication factor on the volume"))
				return
			}

			storageNodes := node.GetStorageNodes()
			maxReplFactor := 3

			if volAggrLevel == 3 {
				if len(storageNodes) >= 6 && len(storageNodes) < 9 {
					maxReplFactor = 2
				}
				if len(storageNodes) < 6 {
					maxReplFactor = 1
				}
			}
			if volAggrLevel == 2 {
				if len(storageNodes) >= 6 {
					maxReplFactor = 3
				}
				if len(storageNodes) < 6 {
					maxReplFactor = 2
				}
			}

			if len(getReplicaSets[0].Nodes) == 3 || len(getReplicaSets[0].Nodes) == 1 {
				if len(getReplicaSets[0].Nodes) < maxReplFactor {
					err := Inst().V.SetReplicationFactor(volName, 2, nil, nil, true)
					if err != nil {
						UpdateOutcome(event, fmt.Errorf("failed to set replicaiton for Volume [%v]", volName.Name))
					}
				}
			}

			if len(getReplicaSets[0].Nodes) == 2 && len(getReplicaSets[0].Nodes) < maxReplFactor {
				err := Inst().V.SetReplicationFactor(volName, 3, nil, nil, true)
				if err != nil {
					UpdateOutcome(event, fmt.Errorf("failed to set replicaiton for Volume [%v] with error : [%v]", volName.Name, err))
				}
			}
		}

		// Set replication factor to 3 on all the volumes present in the cluster
		for _, eachVol := range allVolsCreated {
			setReplFactor(eachVol)
		}

		log.InfoD("Initiate Volume resize continuously")
		volumeResize := func(vol *volume.Volume) error {

			apiVol, err := Inst().V.InspectVolume(vol.ID)
			if err != nil {
				return err
			}

			curSize := apiVol.Spec.Size
			newSize := curSize + (uint64(10) * units.GiB)
			log.Infof("Initiating volume size increase on volume [%v] by size [%v] to [%v]",
				vol.ID, curSize/units.GiB, newSize/units.GiB)

			err = Inst().V.ResizeVolume(vol.ID, newSize)
			if err != nil {
				log.Debugf("Printing the volume inspect for the volume:%s ,volID:%s and namespace:%s after resize", vol.Name, vol.ID, vol.Namespace)
				PrintInspectVolume(vol.ID)
				return err
			}

			// Wait for 2 seconds for Volume to update stats
			time.Sleep(2 * time.Second)
			volumeInspect, err := Inst().V.InspectVolume(vol.ID)
			if err != nil {
				return err
			}

			updatedSize := volumeInspect.Spec.Size
			if updatedSize <= curSize {
				return fmt.Errorf("volume did not update from [%v] to [%v] ",
					curSize/units.GiB, updatedSize/units.GiB)
			}
			return nil
		}

		// Resize volume created with contexts
		for _, eachVol := range allVolsCreated {
			log.Infof("Resizing Volumes created [%v]", eachVol.Name)
			err := volumeResize(eachVol)
			if err != nil {
				log.Debugf("Printing the volume inspect for the volume:%s ,volID:%s and namespace:%s after resize", eachVol.Name, eachVol.ID, eachVol.Namespace)
				PrintInspectVolume(eachVol.ID)
				UpdateOutcome(event, fmt.Errorf("Resizing volume failed on the cluster err: [%v]", err))
			}
		}

		// Set replication factor to 3 on all the volumes present in the cluster
		for _, eachVol := range allVolsCreated {
			setReplFactor(eachVol)
		}

		for _, eachVol := range allVolsCreated {
			log.InfoD("Validating Volume Status of Volume [%v]", eachVol.ID)
			status, err := IsVolumeStatusUP(eachVol)
			if err != nil {
				log.Debugf("Printing the volume inspect for the volume:%s ,volID:%s and namespace:%s after io prirority update", eachVol.Name, eachVol.ID, eachVol.Namespace)
				PrintInspectVolume(eachVol.ID)
				UpdateOutcome(event, fmt.Errorf("error validating volume status"))
			}
			dash.VerifyFatal(status == true, true, "is volume status up ?")
		}
		updateMetrics(*event)
	})
}
func TriggerAddOCPStorageNode(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {

	defer endLongevityTest()
	startLongevityTest(AddStorageNode)
	defer ginkgo.GinkgoRecover()

	event := &EventRecord{
		Event: Event{
			ID:   GenerateUUID(),
			Type: AddStorageNode,
		},
		Start:   time.Now().Format(time.RFC1123),
		Outcome: []error{},
	}

	defer func() {
		event.End = time.Now().Format(time.RFC1123)
		*recordChan <- event
	}()

	if Inst().S.String() != openshift.SchedName {
		log.Warnf("Failed: This test is not supported for scheduler: [%s]", Inst().S.String())
		return
	}

	setMetrics(*event)

	stepLog := "Adding new node to cluster"

	isClusterScaled := true
	var numOfStorageNodes int
	Step(stepLog, func() {
		log.InfoD(stepLog)
		stc, err := Inst().V.GetDriver()
		if err != nil {
			UpdateOutcome(event, err)
			return
		}
		maxStorageNodesPerZone := *stc.Spec.CloudStorage.MaxStorageNodesPerZone
		numOfStorageNodes = len(node.GetStorageNodes())
		log.Infof("maxStorageNodesPerZone %d", int(maxStorageNodesPerZone))
		log.Infof("numOfStorageNodes %d", numOfStorageNodes)

		var updatedMaxStorageNodesPerZone uint32 = 0
		if int(maxStorageNodesPerZone) == numOfStorageNodes {
			//increase max per zone
			updatedMaxStorageNodesPerZone = maxStorageNodesPerZone + 1
		}

		if int(maxStorageNodesPerZone) < numOfStorageNodes {
			//updating max per zone
			updatedMaxStorageNodesPerZone = uint32(numOfStorageNodes) + 1
		}
		if updatedMaxStorageNodesPerZone != 0 {

			stc.Spec.CloudStorage.MaxStorageNodesPerZone = &updatedMaxStorageNodesPerZone
			log.InfoD("updating maxStorageNodesPerZone from %d to %d", maxStorageNodesPerZone, updatedMaxStorageNodesPerZone)
			pxOperator := operator.Instance()
			_, err = pxOperator.UpdateStorageCluster(stc)
			if err != nil {
				UpdateOutcome(event, err)
				isClusterScaled = false
				return
			}

		}
		//Scaling the cluster by one node
		expReplicas := len(node.GetWorkerNodes()) + 1
		log.InfoD("scaling up the cluster to replicas %d", expReplicas)
		dashStats := make(map[string]string)
		dashStats["curr-scale"] = fmt.Sprintf("%d", len(node.GetWorkerNodes()))
		dashStats["new-scale"] = fmt.Sprintf("%d", expReplicas)
		dashStats["storage-node"] = "true"
		updateLongevityStats(AddStorageNode, stats.NodeScaleUpEventName, dashStats)
		err = Inst().S.SetASGClusterSize(int64(expReplicas), 10*time.Minute)
		if err != nil {
			UpdateOutcome(event, err)
			isClusterScaled = false
			return
		}

	})

	if !isClusterScaled {
		return
	}

	err := Inst().S.RefreshNodeRegistry()
	UpdateOutcome(event, err)

	err = Inst().V.RefreshDriverEndpoints()
	UpdateOutcome(event, err)

	stepLog = "validate PX on all nodes after cluster scale up"
	hasPXUp := true
	Step(stepLog, func() {
		log.InfoD(stepLog)
		nodes := node.GetWorkerNodes()
		for _, n := range nodes {
			log.InfoD("Check PX status on %v", n.Name)
			err := Inst().V.WaitForPxPodsToBeUp(n)
			if err != nil {
				hasPXUp = false
				UpdateOutcome(event, err)
			}
		}
	})
	if !hasPXUp {
		return
	}

	err = Inst().V.RefreshDriverEndpoints()
	UpdateOutcome(event, err)

	updatedStorageNodesCount := len(node.GetStorageNodes())
	expectedStorageNodeCount := numOfStorageNodes + 1
	//In some cases after new storage node is added, existing storageless node is converted to storage, and this needs some time to repo to update
	t := func() (interface{}, bool, error) {
		err := Inst().V.RefreshDriverEndpoints()
		if err != nil {
			log.Warnf("failed to refesh node drivers, err: %v", err)
			return nil, true, err
		}
		updatedStorageNodesCount = len(node.GetStorageNodes())
		if updatedStorageNodesCount != expectedStorageNodeCount {
			return nil, true, fmt.Errorf("storage nodes [%d] didnt match with expected [%d]. Retrying the check after 30 secs", updatedStorageNodesCount, expectedStorageNodeCount)
		}

		return nil, false, nil
	}
	_, err = task.DoRetryWithTimeout(t, 30*time.Minute, 30*time.Second)
	if updatedStorageNodesCount != expectedStorageNodeCount {
		log.Errorf(fmt.Sprintf("storage nodes [%d] didnt match with expected [%d]. Retrying the check after 30 secs", updatedStorageNodesCount, expectedStorageNodeCount))
		PrintPxctlStatus()
	}
	dash.VerifySafely(err, nil, "verify new storage node is added")

	validateContexts(event, contexts)
	updateMetrics(*event)

}

func TriggerAddOCPStoragelessNode(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {

	defer endLongevityTest()
	startLongevityTest(AddStoragelessNode)
	defer ginkgo.GinkgoRecover()

	event := &EventRecord{
		Event: Event{
			ID:   GenerateUUID(),
			Type: AddStoragelessNode,
		},
		Start:   time.Now().Format(time.RFC1123),
		Outcome: []error{},
	}

	defer func() {
		event.End = time.Now().Format(time.RFC1123)
		*recordChan <- event
	}()

	if Inst().S.String() != openshift.SchedName {
		log.Warnf("Failed: This test is not supported for scheduler: [%s]", Inst().S.String())
		return
	}

	setMetrics(*event)

	stepLog := "Adding new storageless node to cluster"

	isClusterScaled := true
	var numOfStoragelessNodes int
	Step(stepLog, func() {
		log.InfoD(stepLog)
		stc, err := Inst().V.GetDriver()
		if err != nil {
			UpdateOutcome(event, err)
			return
		}
		maxStorageNodesPerZone := *stc.Spec.CloudStorage.MaxStorageNodesPerZone
		numOfStoragelessNodes = len(node.GetStorageLessNodes())
		log.Infof("maxStorageNodesPerZone %d", int(maxStorageNodesPerZone))
		log.Infof("numOfStoragelessNodes %d", numOfStoragelessNodes)

		numOfStorageNodes := len(node.GetStorageNodes())

		if int(maxStorageNodesPerZone) > numOfStorageNodes {
			//updating max per zone
			updatedMaxStorageNodesPerZone := uint32(numOfStorageNodes)
			stc.Spec.CloudStorage.MaxStorageNodesPerZone = &updatedMaxStorageNodesPerZone
			log.InfoD("updating maxStorageNodesPerZone from %d to %d", maxStorageNodesPerZone, updatedMaxStorageNodesPerZone)
			pxOperator := operator.Instance()
			_, err = pxOperator.UpdateStorageCluster(stc)
			if err != nil {
				UpdateOutcome(event, err)
				isClusterScaled = false
				return
			}

		}
		//Scaling the cluster by one node
		expReplicas := len(node.GetWorkerNodes()) + 1
		log.InfoD("scaling up the cluster to replicas %d", expReplicas)
		dashStats := make(map[string]string)
		dashStats["curr-scale"] = fmt.Sprintf("%d", len(node.GetWorkerNodes()))
		dashStats["new-scale"] = fmt.Sprintf("%d", expReplicas)
		dashStats["storage-node"] = "false"
		updateLongevityStats(AddStoragelessNode, stats.NodeScaleUpEventName, dashStats)
		err = Inst().S.SetASGClusterSize(int64(expReplicas), 10*time.Minute)
		if err != nil {
			UpdateOutcome(event, err)
			isClusterScaled = false
			return
		}

	})

	if !isClusterScaled {
		return
	}
	err := Inst().S.RefreshNodeRegistry()
	UpdateOutcome(event, err)

	err = Inst().V.RefreshDriverEndpoints()
	UpdateOutcome(event, err)

	stepLog = "validate PX on all nodes after cluster scale up"
	hasPXUp := true
	Step(stepLog, func() {
		log.InfoD(stepLog)
		nodes := node.GetWorkerNodes()
		for _, n := range nodes {
			log.InfoD("Check PX status on %v", n.Name)
			err := Inst().V.WaitForPxPodsToBeUp(n)
			if err != nil {
				hasPXUp = false
				UpdateOutcome(event, err)
			}
		}
	})
	if !hasPXUp {
		return
	}
	err = Inst().V.RefreshDriverEndpoints()
	UpdateOutcome(event, err)

	updatedStoragelessNodesCount := len(node.GetStorageLessNodes())

	expectedStorageLessNodeCount := numOfStoragelessNodes + 1
	if updatedStoragelessNodesCount != expectedStorageLessNodeCount {
		PrintPxctlStatus()
	}
	dash.VerifySafely(updatedStoragelessNodesCount, expectedStorageLessNodeCount, "verify new storageless node is added")

	validateContexts(event, contexts)
	updateMetrics(*event)
}

func TriggerOCPStorageNodeRecycle(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {

	defer endLongevityTest()
	startLongevityTest(OCPStorageNodeRecycle)
	defer ginkgo.GinkgoRecover()

	event := &EventRecord{
		Event: Event{
			ID:   GenerateUUID(),
			Type: OCPStorageNodeRecycle,
		},
		Start:   time.Now().Format(time.RFC1123),
		Outcome: []error{},
	}

	defer func() {
		event.End = time.Now().Format(time.RFC1123)
		*recordChan <- event
	}()

	if Inst().S.String() != openshift.SchedName {
		log.Warnf("Failed: This test is not supported for scheduler: [%s]", Inst().S.String())
		return
	}

	if Inst().N.String() != vsphere.DriverName {
		log.Warnf("Failed: This test is not supported for node driver: [%s]", Inst().N.String())
		return
	}

	setMetrics(*event)

	stepLog := "Recycle Storage node in the cluster"

	stNodes := node.GetStorageNodes()
	stlessNodes := node.GetStorageLessNodes()
	Step(stepLog, func() {
		log.InfoD(stepLog)

		index := randIntn(1, len(stNodes))[0]
		delNode := stNodes[index]
		Step(
			fmt.Sprintf("Recycle a storage node: [%s] and validating the drives", delNode.Name),
			func() {
				dashStats := make(map[string]string)
				dashStats["node"] = delNode.Name
				updateLongevityStats(OCPStorageNodeRecycle, stats.NodeRecycleEventName, dashStats)
				err := Inst().S.DeleteNode(delNode)
				UpdateOutcome(event, err)

				stepLog = fmt.Sprintf("wait for %s minutes for auto recovery of storeage nodes",
					Inst().AutoStorageNodeRecoveryTimeout.String())

				Step(stepLog, func() {
					log.InfoD(stepLog)
					time.Sleep(Inst().AutoStorageNodeRecoveryTimeout)
				})
				err = Inst().S.RefreshNodeRegistry()
				UpdateOutcome(event, err)

				err = Inst().V.RefreshDriverEndpoints()
				UpdateOutcome(event, err)
			})
		Step(fmt.Sprintf("Listing all nodes after recycling a storage node %s", delNode.Name), func() {
			workerNodes := node.GetWorkerNodes()
			for x, wNode := range workerNodes {
				log.Infof("WorkerNode[%d] is: [%s] and volDriverID is [%s]", x, wNode.Name, wNode.VolDriverNodeID)
			}
		})

	})

	stepLog = "validate PX on all nodes after node recycling"
	hasPXUp := true
	Step(stepLog, func() {
		log.InfoD(stepLog)
		nodes := node.GetWorkerNodes()
		for _, n := range nodes {
			log.InfoD("Check PX status on %v", n.Name)
			err := Inst().V.WaitForPxPodsToBeUp(n)
			if err != nil {
				hasPXUp = false
				UpdateOutcome(event, err)
			}
		}
	})
	if !hasPXUp {
		return
	}
	err := Inst().V.RefreshDriverEndpoints()
	UpdateOutcome(event, err)

	updatedStoragelessNodesCount := len(node.GetStorageLessNodes())
	dash.VerifySafely(len(stlessNodes), updatedStoragelessNodesCount, "verify storageless nodes count after node recycle")
	updatedStorageNodesCount := len(node.GetStorageNodes())
	dash.VerifySafely(len(stNodes), updatedStorageNodesCount, "verify storage node count after node recycle")

	validateContexts(event, contexts)
	updateMetrics(*event)
}

// TriggerReallocSharedMount peforms sharedv4 and sharedv4_svc volumes reallocation
func TriggerReallocSharedMount(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {
	defer ginkgo.GinkgoRecover()
	defer endLongevityTest()
	startLongevityTest(ReallocateSharedMount)
	event := &EventRecord{
		Event: Event{
			ID:   GenerateUUID(),
			Type: ReallocateSharedMount,
		},
		Start:   time.Now().Format(time.RFC1123),
		Outcome: []error{},
	}

	defer func() {
		event.End = time.Now().Format(time.RFC1123)
		*recordChan <- event
	}()
	setMetrics(*event)
	stepLog := "get nodes with shared mount and reboot them"
	Step(stepLog, func() {
		log.InfoD(stepLog)
		for _, ctx := range *contexts {
			vols, err := Inst().S.GetVolumes(ctx)
			if err != nil {
				UpdateOutcome(event, err)
				continue
			}

			for _, vol := range vols {
				if vol.Shared {

					n, err := Inst().V.GetNodeForVolume(vol, 1*time.Minute, 5*time.Second)
					if err != nil {
						UpdateOutcome(event, err)
						continue
					}

					log.InfoD("volume %s is attached on node %s [%s]", vol.ID, n.SchedulerNodeName, n.Addresses[0])

					// Workaround to avoid PWX-24277 for now.
					Step(fmt.Sprintf("wait until volume %v status is Up", vol.ID), func() {
						log.InfoD("wait until volume %v status is Up", vol.ID)

						t := func() (interface{}, bool, error) {
							connOpts := node.ConnectionOpts{
								Timeout:         1 * time.Minute,
								TimeBeforeRetry: 5 * time.Second,
								Sudo:            true,
							}
							cmd := fmt.Sprintf("pxctl volume inspect %s | grep \"Replication Status\"", vol.ID)
							volStatus, err := Inst().N.RunCommandWithNoRetry(*n, cmd, connOpts)
							if err != nil {
								log.Warnf("failed to get replication state of volume %v: %v", vol.ID, err)
								return nil, true, err
							}

							if strings.Contains(volStatus, "Up") {
								log.InfoD("volume %v: %v", vol.ID, volStatus)
								return nil, false, nil
							}
							return nil, true, fmt.Errorf("volum status is not Up, Curr status: %s", volStatus)
						}

						_, err = task.DoRetryWithTimeout(t, 30*time.Minute, 30*time.Second)
						UpdateOutcome(event, err)
					})

					dashStats := make(map[string]string)
					dashStats["node"] = n.Name
					dashStats["volume"] = vol.Name
					updateLongevityStats(ReallocateSharedMount, stats.PXRestartEventName, dashStats)

					err = Inst().S.DisableSchedulingOnNode(*n)
					dash.VerifySafely(err == nil, true, fmt.Sprintf("Disable sceduling on node : %s", n.Name))

					err = Inst().V.StopDriver([]node.Node{*n}, false, nil)
					dash.VerifySafely(err == nil, true, fmt.Sprintf("Stop volume driver on node : %s success ?", n.Name))

					err = Inst().N.RebootNode(*n, node.RebootNodeOpts{
						Force: true,
						ConnectionOpts: node.ConnectionOpts{
							Timeout:         1 * time.Minute,
							TimeBeforeRetry: 1 * time.Second,
						},
					})
					UpdateOutcome(event, err)

					// as we keep the storage driver down on node until we check if the volume, we wait a minute for
					// reboot to occur then we force driver to refresh endpoint to pick another storage node which is up
					log.InfoD("wait for %v for node reboot", 1*time.Minute)
					time.Sleep(1 * time.Minute)

					if Inst().S.String() != openshift.SchedName {
						// Start NFS server to avoid pods stuck in terminating state (PWX-24274)
						err = Inst().N.Systemctl(*n, "nfs-server.service", node.SystemctlOpts{
							Action: "start",
							ConnectionOpts: node.ConnectionOpts{
								Timeout:         5 * time.Minute,
								TimeBeforeRetry: 10 * time.Second,
							}})
						UpdateOutcome(event, err)
					}

					ctx.RefreshStorageEndpoint = true
					n2, err := Inst().V.GetNodeForVolume(vol, 1*time.Minute, 10*time.Second)

					UpdateOutcome(event, err)
					if n2 != nil {
						// the mount should move to another node otherwise fail
						log.InfoD("volume %s is now attached on node %s [%s]", vol.ID, n2.SchedulerNodeName, n2.Addresses[0])
						dash.VerifySafely(n.SchedulerNodeName != n2.SchedulerNodeName, true, "Volume is scheduled on different nodes?")

						StartVolDriverAndWait([]node.Node{*n})
						err = Inst().S.EnableSchedulingOnNode(*n)
						UpdateOutcome(event, err)

					}
				}
			}
			log.InfoD("validating applications")
			ValidateApplications(*contexts)
		}
		updateMetrics(*event)
	})
}

// TriggerCreateAndRunFioOnVcluster creates and runs fio on vcluster
func TriggerCreateAndRunFioOnVcluster(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {
	defer ginkgo.GinkgoRecover()
	defer endLongevityTest()
	startLongevityTest(CreateAndRunFioOnVcluster)
	event := &EventRecord{
		Event: Event{
			ID:   GenerateUUID(),
			Type: CreateAndRunFioOnVcluster,
		},
		Start:   time.Now().Format(time.RFC1123),
		Outcome: []error{},
	}
	defer func() {
		event.End = time.Now().Format(time.RFC1123)
		*recordChan <- event
	}()

	setMetrics(*event)

	vc := &vcluster.VCluster{}
	var scName string
	var pvcName string
	var appNS string

	fioOptions := vcluster.FIOOptions{
		Name:      "mytest",
		IOEngine:  "libaio",
		RW:        "randwrite",
		BS:        "4k",
		NumJobs:   1,
		Size:      "500m",
		TimeBased: true,
		Runtime:   VclusterFioRunTime,
		EndFsync:  1,
	}

	defer func() {
		// VCluster, StorageClass and Namespace cleanup
		log.InfoD("Cleaning up vcluster: %v in namespace: %v", vc.Name, vc.Namespace)
		err := vc.VClusterCleanup(scName)
		if err != nil {
			UpdateOutcome(event, err)
		}
	}()

	vc, err := vcluster.NewVCluster("my-vcluster-fio")
	if err != nil {
		UpdateOutcome(event, err)
		return
	}
	err = vc.CreateAndWaitVCluster()
	if err != nil {
		UpdateOutcome(event, err)
		return
	}

	setMetrics(*event)
	stepLog := fmt.Sprintf("Create FIO app on VCluster and run it for %v seconds", VclusterFioRunTime)

	Step(stepLog, func() {
		log.InfoD(stepLog)
		// Create Storage Class on Host Cluster
		scName = fmt.Sprintf("fio-app-sc-%v", time.Now().Unix())
		err := CreateVclusterStorageClass(scName)
		if err != nil {
			UpdateOutcome(event, err)
		}

		// Create PVC on VCluster
		appNS = scName + "-ns"
		pvcName, err = vc.CreatePVC("", scName, appNS, "")
		if err != nil {
			UpdateOutcome(event, err)
		}

		jobName := "fio-job"

		// Create FIO Deployment on VCluster using the above PVC
		timeInseconds, err := strconv.Atoi(VclusterFioRunTime)
		if err != nil {
			log.InfoD("Failed to convert value %v to int with error: %v", VclusterFioRunTime, err)
			UpdateOutcome(event, err)
		}

		duration := time.Duration(timeInseconds) * time.Second

		err = vc.CreateFIODeployment(pvcName, appNS, fioOptions, jobName, duration)
		if err != nil {
			UpdateOutcome(event, err)
		}

		updateMetrics(*event)
	})

}

// TriggerCreateAndRunMultipleFioOnVcluster creates and runs multiple fio on vcluster
func TriggerCreateAndRunMultipleFioOnVcluster(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {
	defer ginkgo.GinkgoRecover()
	defer endLongevityTest()
	startLongevityTest(CreateAndRunMultipleFioOnVcluster)
	event := &EventRecord{
		Event: Event{
			ID:   GenerateUUID(),
			Type: CreateAndRunMultipleFioOnVcluster,
		},
		Start:   time.Now().Format(time.RFC1123),
		Outcome: []error{},
	}
	defer func() {
		event.End = time.Now().Format(time.RFC1123)
		*recordChan <- event
	}()

	setMetrics(*event)

	vc := &vcluster.VCluster{}
	var scName string
	var appNS string

	defer func() {
		// VCluster, StorageClass and Namespace cleanup
		log.InfoD("Cleaning up vcluster: %v in namespace: %v", vc.Name, vc.Namespace)
		err := vc.VClusterCleanup(scName)
		if err != nil {
			UpdateOutcome(event, err)
		}
	}()

	vc, err := vcluster.NewVCluster("my-vcluster-multi-fio")
	if err != nil {
		UpdateOutcome(event, err)
		return
	}

	err = vc.CreateAndWaitVCluster()
	if err != nil {
		UpdateOutcome(event, err)
		return
	}

	envValueIterations := VclusterFioTotalIteration
	envValueBatch := VclusterFioParallelApps
	batchCount := 2
	totalIterations := 1
	if envValueIterations != "" {
		var err error
		totalIterations, err = strconv.Atoi(envValueIterations)
		if err != nil {
			log.Errorf("Failed to convert value %v to int with error: %v", envValueIterations, err)
			UpdateOutcome(event, err)
			totalIterations = 1
		} else {
			log.InfoD("Total iteration successfully picked up from configmap :%v", totalIterations)
		}
	}
	if envValueBatch != "" {
		var err error
		batchCount, err = strconv.Atoi(envValueBatch)
		if err != nil {
			log.Errorf("Failed to convert value %v to int with error: %v", envValueBatch, err)
			UpdateOutcome(event, err)
			batchCount = 2
		} else {
			log.InfoD("batchcount successfully picked up from configmap :%v", batchCount)
		}
	}
	fioOptions := vcluster.FIOOptions{
		Name:      "mytest",
		IOEngine:  "libaio",
		RW:        "randwrite",
		BS:        "4k",
		NumJobs:   1,
		Size:      "500m",
		TimeBased: true,
		Runtime:   VclusterFioRunTime,
		EndFsync:  1,
	}

	setMetrics(*event)

	stepLog := fmt.Sprintf("Create multiple FIO app on VCluster and run it for %v seconds", VclusterFioRunTime)

	Step(stepLog, func() {
		log.InfoD(stepLog)

		// Create Storage Class on Host Cluster
		scName = fmt.Sprintf("fio-app-sc-%v", time.Now().Unix())
		log.InfoD("Creating StorageClass with name: %v", scName)
		err = CreateVclusterStorageClass(scName)
		if err != nil {
			UpdateOutcome(event, err)
		}

		appNS = scName + "-ns"
		for i := 0; i < totalIterations; i++ {
			var wg sync.WaitGroup
			var jobNames []string
			for j := 0; j < batchCount; j++ {
				wg.Add(1)
				go func(idx int) {
					defer wg.Done()
					pvcNameSuffix := fmt.Sprintf("-pvc-%d-%d-%d", i, j, idx)
					jobName := fmt.Sprintf("fio-job-%d-%d-%d", i, j, idx)
					jobNames = append(jobNames, jobName)
					log.InfoD("creating PVC with name: %v", scName+pvcNameSuffix)
					pvcName, err := vc.CreatePVC(scName+pvcNameSuffix, scName, appNS, "")
					if err != nil {
						UpdateOutcome(event, err)
						return
					}
					// Create FIO Deployment on VCluster using the above PVC
					timeInseconds, err := strconv.Atoi(VclusterFioRunTime)
					if err != nil {
						log.InfoD("Failed to convert value %v to int with error: %v", VclusterFioRunTime, err)
						UpdateOutcome(event, err)
					}

					duration := time.Duration(timeInseconds) * time.Second
					// Create FIO Deployment on VCluster using the above PVC
					log.InfoD("creating FIO deployment with name: %v using pvc: %v", jobName, pvcName)
					err = vc.CreateFIODeployment(pvcName, appNS, fioOptions, jobName, duration)
					if err != nil {
						UpdateOutcome(event, err)
						return
					}
				}(i + j)
			}
			wg.Wait()
			for _, jobName := range jobNames {
				log.Infof("deleting job with name: %v", jobName)
				err := vc.DeleteJobOnVcluster(appNS, jobName)
				if err != nil {
					UpdateOutcome(event, err)
					continue
				}
			}
		}
	})
}

func TriggerVolumeDriverDownVCluster(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {
	defer ginkgo.GinkgoRecover()
	defer endLongevityTest()
	startLongevityTest(VolumeDriverDownVCluster)
	event := &EventRecord{
		Event: Event{
			ID:   GenerateUUID(),
			Type: VolumeDriverDownVCluster,
		},
		Start:   time.Now().Format(time.RFC1123),
		Outcome: []error{},
	}
	defer func() {
		event.End = time.Now().Format(time.RFC1123)
		*recordChan <- event
	}()
	vc := &vcluster.VCluster{}
	var scName string
	var pvcName string
	var appNS string

	defer func() {
		// VCluster, StorageClass and Namespace cleanup
		log.InfoD("Cleaning up vcluster: %v in namespace: %v", vc.Name, vc.Namespace)
		err := vc.VClusterCleanup(scName)
		if err != nil {
			UpdateOutcome(event, err)
		}
	}()

	vc, err := vcluster.NewVCluster("my-vcluster-driver-down")
	if err != nil {
		UpdateOutcome(event, err)
		return
	}
	err = vc.CreateAndWaitVCluster()
	if err != nil {
		UpdateOutcome(event, err)
		return
	}
	stepLog := "Creates Nginx Deployment on Vcluster, Brings Down Portworx on node running Nginx and then deletes Nginx deployment. Brings up Px again"
	Step(stepLog, func() {
		log.InfoD(stepLog)

		// Create Storage Class on Host Cluster
		scName = fmt.Sprintf("nginx-app-sc-%v", time.Now().Unix())
		err = CreateVclusterStorageClass(scName)
		if err != nil {
			UpdateOutcome(event, err)
			return
		}
		log.Infof("Successfully created StorageClass with name: %v", scName)

		// Create PVC on VCluster
		appNS = scName + "-ns"
		pvcName, err = vc.CreatePVC("", scName, appNS, "")
		if err != nil {
			UpdateOutcome(event, err)
			return
		}
		log.Infof("Successfully created PVC with name: %v", pvcName)

		deploymentName := "nginx-deployment"
		// Create Nginx Deployment on VCluster using the above PVC
		err = vc.CreateNginxDeployment(pvcName, appNS, deploymentName)
		if err != nil {
			UpdateOutcome(event, err)
			return
		}

		log.Infof("Successfully created Nginx App on Vcluster")
		log.Infof("Hard Sleep for 10 seconds after creation of Nginx Deployment")
		time.Sleep(10 * time.Second)

		// Validate if Nginx Deployment is healthy or not
		err = vc.IsDeploymentHealthy(appNS, deploymentName, 1)
		if err != nil {
			UpdateOutcome(event, err)
			return
		}
		log.Infof("Nginx Deployment %s is healthy. Will kill Px and delete Nginx deployment now", deploymentName)
		podNodes, err := vc.GetDeploymentPodNodes(appNS, deploymentName)
		if err != nil {
			UpdateOutcome(event, err)
			return
		}
		var nodesToReboot []string
		for _, appNode := range node.GetWorkerNodes() {
			for _, podNode := range podNodes {
				if appNode.Name == podNode {
					nodesToReboot = append(nodesToReboot, appNode.Name)
				}
			}
		}
		stepLog = "get nodes bounce volume driver"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			for _, appNode := range node.GetWorkerNodes() {
				for _, nodes := range nodesToReboot {
					if appNode.Name == nodes {
						stepLog = fmt.Sprintf("stop volume driver %s on node: %s",
							Inst().V.String(), appNode.Name)
						Step(stepLog,
							func() {
								log.InfoD(stepLog)
								StopVolDriverAndWait([]node.Node{appNode})
							})
					}
				}
			}
			// Validates if VCluster is still accessible or not by listing namespaces within it
			err = vc.WaitForVClusterAccess()
			if err != nil {
				UpdateOutcome(event, err)
				return
			}
			log.Infof("Vcluster has become responsive - going ahead with the test")
			// Deleting Deployment from Vcluster now once it becomes accessible
			err = vc.DeleteDeploymentOnVCluster(appNS, deploymentName)
			if err != nil {
				UpdateOutcome(event, err)
				return
			}
			log.Infof("Successfully deleted Nginx deployment %v on Vcluster %v", deploymentName, vc.Name)

			for _, appNode := range node.GetWorkerNodes() {
				for _, nodes := range nodesToReboot {
					if appNode.Name == nodes {
						stepLog = fmt.Sprintf("start volume driver %s on node: %s",
							Inst().V.String(), appNode.Name)
						Step(stepLog,
							func() {
								log.InfoD(stepLog)
								StartVolDriverAndWait([]node.Node{appNode})
							})
					}
				}
			}
			stepLog = "Giving few seconds for volume driver to stabilize"
			Step(stepLog, func() {
				log.InfoD(stepLog)
				time.Sleep(20 * time.Second)
			})
		})
	})

}

func TriggerSetDiscardMounts(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {
	defer ginkgo.GinkgoRecover()
	defer endLongevityTest()
	startLongevityTest(SetDiscardMounts)
	event := &EventRecord{
		Event: Event{
			ID:   GenerateUUID(),
			Type: SetDiscardMounts,
		},
		Start:   time.Now().Format(time.RFC1123),
		Outcome: []error{},
	}
	defer func() {
		event.End = time.Now().Format(time.RFC1123)
		*recordChan <- event
	}()

	setMetrics(*event)

	stepLog := "setting and unsetting discard mount options on regular intervals "
	Step(stepLog, func() {
		log.InfoD(stepLog)
		log.Infof("Starting test case here !!")

		for _, appNode := range node.GetStorageNodes() {
			err := isNodeHealthy(appNode, event.Event.Type)
			if err != nil {
				UpdateOutcome(event, err)
				continue
			}
			// Setting discard-mount-option on the node
			err = SetUnSetDiscardMountRTOptions(&appNode, false)
			if err != nil {
				UpdateOutcome(event, err)
				continue
			}
			// Verify if the cluster options set for run time parameters
			optionsMap := []string{"NodeRuntimeOptions"}
			cOptions, err := Inst().V.GetClusterOpts(appNode, optionsMap)
			if !strings.Contains(cOptions["NodeRuntimeOptions"], "discard_mount_force:1") {
				UpdateOutcome(event, err)
				continue
			}
		}
	})
}

func TriggerResetDiscardMounts(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {
	defer ginkgo.GinkgoRecover()
	defer endLongevityTest()
	startLongevityTest(ResetDiscardMounts)
	event := &EventRecord{
		Event: Event{
			ID:   GenerateUUID(),
			Type: ResetDiscardMounts,
		},
		Start:   time.Now().Format(time.RFC1123),
		Outcome: []error{},
	}
	defer func() {
		event.End = time.Now().Format(time.RFC1123)
		*recordChan <- event
	}()

	setMetrics(*event)

	stepLog := "setting and unsetting discard mount options on regular intervals "
	Step(stepLog, func() {
		log.InfoD(stepLog)
		log.Infof("Starting test case here !!")

		for _, appNode := range node.GetStorageNodes() {
			err := isNodeHealthy(appNode, event.Event.Type)
			if err != nil {
				UpdateOutcome(event, err)
				continue
			}
			// Setting discard-mount-option on the node
			err = SetUnSetDiscardMountRTOptions(&appNode, true)
			if err != nil {
				UpdateOutcome(event, err)
				continue
			}
			// Verify if the cluster options set for run time parameters
			optionsMap := []string{"NodeRuntimeOptions"}
			cOptions, err := Inst().V.GetClusterOpts(appNode, optionsMap)
			if !strings.Contains(cOptions["NodeRuntimeOptions"], "discard_mount_force:0") {
				UpdateOutcome(event, err)
				continue
			}
		}
	})
}

func TriggerScaleFADAVolumeAttach(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {
	defer endLongevityTest()
	startLongevityTest(ScaleFADAVolumeAttach)
	defer ginkgo.GinkgoRecover()
	var wg sync.WaitGroup

	event := &EventRecord{
		Event: Event{
			ID:   GenerateUUID(),
			Type: ScaleFADAVolumeAttach,
		},
		Start:   time.Now().Format(time.RFC1123),
		Outcome: []error{},
	}

	defer func() {
		event.End = time.Now().Format(time.RFC1123)
		*recordChan <- event
	}()

	setMetrics(*event)

	stepLog := "Adding FADA volumes at scale to validate FADA volumes attachment in scale "

	Step(stepLog, func() {
		log.InfoD(stepLog)
		var param = make(map[string]string)
		var appContexts []*scheduler.Context
		sem := make(chan struct{}, 10)

		fadaScName := PureBlockStorageClass + time.Now().Format("01-02-15h04m05s")
		log.Infof("Creating pure_block storage class class: %s", fadaScName)
		param[PureBackend] = k8s.PureBlock
		_, err := createPureStorageClass(fadaScName, param)
		if err != nil {
			log.Errorf("StorageClass creation failed for SC: %s", fadaScName)
			UpdateOutcome(event, err)
		}
		log.InfoD("Deployng FADA based applications")
		startTime := time.Now()
		for x := 0; x < deploymentCount; x++ {
			pvcName := fmt.Sprintf("%s-%d", pvcNamePrefix, x)
			namespace := fmt.Sprintf("%s-%d", fadaNamespacePrefix, x)
			deploymentName := fmt.Sprintf("%s-%d", fadaScName, x)
			wg.Add(1)
			sem <- struct{}{}
			go func(scName string, pvcName string, ns string, depName string, wg *sync.WaitGroup, ctx *[]*scheduler.Context, event *EventRecord, sem chan struct{}) {
				deployFadaApps(fadaScName, pvcName, namespace, deploymentName, wg, ctx, event)
				<-sem
			}(fadaScName, pvcName, namespace, deploymentName, &wg, &appContexts, event, sem)
		}
		wg.Wait()
		close(sem)
		log.InfoD("Validating applications context")
		validateContexts(event, &appContexts)
		log.Infof("Attaching [%d] FADA volumes took: [%v]", deploymentCount, time.Since(startTime))
		if time.Since(startTime) > podAttachTimeout {
			UpdateOutcome(event, fmt.Errorf("failed to complete all fada pods attachment in [%v]", podAttachTimeout))
		}

		stepLog = "Cleaning up the FADA deployments"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			sem := make(chan struct{}, 10)
			for x := 0; x < deploymentCount; x++ {
				sem <- struct{}{}
				wg.Add(1)
				go func(ctx *scheduler.Context, wg *sync.WaitGroup, event *EventRecord, sem chan struct{}) {
					cleanupDeployment(ctx, wg, event)
					<-sem
				}(appContexts[x], &wg, event, sem)
			}
			wg.Wait()
			close(sem)
			log.InfoD("Successfully cleaned up the FADA deployments")
		})
		updateMetrics(*event)
	})
}

// deployFadaApps deploy deployment using FADA volumes
func deployFadaApps(scName string, pvcName string, ns string, depName string, wg *sync.WaitGroup, ctx *[]*scheduler.Context, event *EventRecord) {
	defer wg.Done()

	metadata := make(map[string]string, 0)
	pvcSize := "1Gi"
	metadata["app"] = "fada-data-app"

	if err := createNameSpace(ns, metadata); err != nil {
		UpdateOutcome(event, fmt.Errorf("failed to create namespace: %s. Err: %v", ns, err))
	}
	if err := createPVC(pvcName, scName, pvcSize, ns); err != nil {
		UpdateOutcome(event, fmt.Errorf("failed to create pvc: [%s] in ns: [%s]. Err: %v", pvcName, ns, err))
	}
	if err := createDeployment(depName, ns, pvcName, ctx); err != nil {
		UpdateOutcome(event, fmt.Errorf("failed to create deployment: %s. Err: %v", depName, err))
	}
}

func createDeployment(depName string, ns string, pvcName string, ctx *[]*scheduler.Context) error {
	request := make(map[v1.ResourceName]resource.Quantity, 0)
	limit := make(map[v1.ResourceName]resource.Quantity, 0)
	cpu, err := resource.ParseQuantity("50m")
	if err != nil {
		return fmt.Errorf("failed to parse cpu request size. Err: %v", err)
	}
	memory, err := resource.ParseQuantity("64Mi")
	if err != nil {
		return fmt.Errorf("failed to parse memory request size. Err: %v", err)
	}
	request["cpu"], request["memory"] = cpu, memory

	cpuLimit, err := resource.ParseQuantity("100m")
	if err != nil {
		return fmt.Errorf("failed to parse cpu limit size. Err: %v", err)
	}
	memLimit, err := resource.ParseQuantity("128Mi")
	if err != nil {
		return fmt.Errorf("failed to parse memory limit size. Err: %v", err)
	}
	limit["cpu"], limit["memory"] = cpuLimit, memLimit
	deployment := getDeploymentObject(depName, ns, pvcName, request, limit)
	deployment, err = apps.Instance().CreateDeployment(deployment, metav1.CreateOptions{})
	if err != nil {
		return err
	}

	(*ctx) = append(*ctx, &scheduler.Context{
		App: &spec.AppSpec{
			SpecList: []interface{}{deployment},
		},
	})
	return nil
}

func getDeploymentObject(depName string, ns string, pvcName string, request, limit map[v1.ResourceName]resource.Quantity) *appsapi.Deployment {
	var replica int32 = 1
	label := make(map[string]string, 0)
	label["app"] = "fada-data-app"
	volMounts := []v1.VolumeMount{{
		Name:      "fada-data-vol",
		MountPath: "/mnt/data"},
	}
	volumes := []v1.Volume{{
		Name: "fada-data-vol",
		VolumeSource: v1.VolumeSource{
			PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{
				ClaimName: pvcName,
			}}},
	}
	commands := []string{
		"sh", "-c", "sleep 3600",
	}

	// containers spec
	containers := []v1.Container{
		{
			Name:         "franzlaender",
			Image:        "ubuntu:latest",
			Command:      commands,
			VolumeMounts: volMounts,
			Resources: v1.ResourceRequirements{
				Requests: request,
				Limits:   limit,
			},
			ImagePullPolicy: v1.PullIfNotPresent,
		},
	}
	return &appsapi.Deployment{
		ObjectMeta: meta_v1.ObjectMeta{
			Name:      depName,
			Namespace: ns,
		},
		Spec: appsapi.DeploymentSpec{
			Replicas: &replica,
			Selector: &metav1.LabelSelector{
				MatchLabels: label,
			},
			Template: v1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: label,
				},
				Spec: v1.PodSpec{
					RestartPolicy: "Always",
					Containers:    containers,
					Volumes:       volumes,
				},
			},
		},
	}
}

func createNameSpace(namespace string, label map[string]string) error {
	nsSpec := &v1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name:   namespace,
			Labels: label,
		},
	}
	_, err := k8sCore.CreateNamespace(nsSpec)
	return err
}

func createPVC(pvcName string, scName string, pvcSize string, ns string) error {
	size, err := resource.ParseQuantity(pvcSize)
	if err != nil {
		return fmt.Errorf("failed to parse pvc size: %s", pvcSize)
	}
	pvcClaimSpec := k8s.MakePVC(size, ns, pvcName, scName)
	_, err = k8sCore.CreatePersistentVolumeClaim(pvcClaimSpec)
	return err
}

func cleanupDeployment(ctx *scheduler.Context, wg *sync.WaitGroup, event *EventRecord) {
	defer wg.Done()
	defer ginkgo.GinkgoRecover()
	var deployment *appsapi.Deployment
	var depname, namespace string
	if len(ctx.App.SpecList) > 0 {
		deployment = ctx.App.SpecList[0].(*appsapi.Deployment)
		depname = deployment.ObjectMeta.Name
		namespace = deployment.ObjectMeta.Namespace
		if err := apps.Instance().DeleteDeployment(depname, namespace); err != nil {
			UpdateOutcome(event, fmt.Errorf(
				"failed to delete deployment: %s. Err: %v", depname, err))
		}
		if len(deployment.Spec.Template.Spec.Volumes) > 0 {
			for _, vol := range deployment.Spec.Template.Spec.Volumes {
				pvcName := vol.VolumeSource.PersistentVolumeClaim.ClaimName
				if err := k8sCore.DeletePersistentVolumeClaim(pvcName, namespace); err != nil {
					UpdateOutcome(event, err)
				}
			}
		}
	}
	if err := k8sCore.DeleteNamespace(namespace); err != nil {
		UpdateOutcome(event, err)
	}
}

// CreateStorageClass method creates a storageclass using host's k8s clientset on host cluster
func CreateVclusterStorageClass(scName string, opts ...storageClassOption) error {
	params := make(map[string]string)
	params["repl"] = "2"
	params["priority_io"] = "high"
	params["io_profile"] = "auto"
	v1obj := meta_v1.ObjectMeta{
		Name: scName,
	}
	reclaimPolicyDelete := v1.PersistentVolumeReclaimDelete
	bindMode := storageapi.VolumeBindingImmediate
	scObj := storageapi.StorageClass{
		ObjectMeta:        v1obj,
		Provisioner:       k8s.CsiProvisioner,
		Parameters:        params,
		ReclaimPolicy:     &reclaimPolicyDelete,
		VolumeBindingMode: &bindMode,
	}
	// Applying each extra option to Storage class definition
	for _, opt := range opts {
		opt(&scObj)
	}
	k8sStorage := storage.Instance()
	if _, err := k8sStorage.CreateStorageClass(&scObj); err != nil {
		return err
	}
	return nil
}

// GetContextPVCs returns pvc from the given context
func GetContextPVCs(context *scheduler.Context) ([]*v1.PersistentVolumeClaim, error) {
	updatedPVCs := make([]*v1.PersistentVolumeClaim, 0)
	for _, specObj := range context.App.SpecList {

		if obj, ok := specObj.(*v1.PersistentVolumeClaim); ok {
			pvc, err := k8sCore.GetPersistentVolumeClaim(obj.Name, obj.Namespace)
			if err != nil {
				return nil, err
			}
			updatedPVCs = append(updatedPVCs, pvc)

		}
	}
	return updatedPVCs, nil

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

func getPXVersion(nd node.Node) string {
	pxVersion, err := Inst().V.GetDriverVersionOnNode(nd)
	if err != nil {
		return "Couldn't get PX version"
	}
	return pxVersion
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
<h1> {{ .MailSubject }} </h1>
<hr/>
<h3>SetUp Details</h3>
<p><b>Master IP:</b> {{.MasterIP}}</p>
<p><b>Dashboard URL:</b> {{.DashboardURL}}</p>
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
   <td align="center"><h4>Trigger Name </h4></td>
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
