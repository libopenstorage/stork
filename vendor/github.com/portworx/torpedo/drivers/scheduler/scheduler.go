package scheduler

import (
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	storageapi "k8s.io/api/storage/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1beta1 "github.com/kubernetes-csi/external-snapshotter/client/v4/apis/volumesnapshot/v1beta1"
	snapv1 "github.com/kubernetes-incubator/external-storage/snapshot/pkg/apis/crd/v1"
	apapi "github.com/libopenstorage/autopilot-api/pkg/apis/autopilot/v1alpha1"
	"github.com/portworx/torpedo/drivers/api"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/scheduler/spec"
	"github.com/portworx/torpedo/drivers/volume"
	"github.com/portworx/torpedo/pkg/errors"
)

// Options specifies keys for a key-value pair that can be passed to scheduler methods
const (
	// OptionsWaitForDestroy Wait for the destroy to finish before returning
	OptionsWaitForDestroy = "WAIT_FOR_DESTROY"
	// OptionsWaitForResourceLeak Wait for all the resources to be cleaned up after destroying
	OptionsWaitForResourceLeakCleanup = "WAIT_FOR_RESOURCE_LEAK_CLEANUP"
	SecretVault                       = "vault"
	SecretK8S                         = "k8s"
)

// Context holds the execution context of a test task.
type Context struct {
	// UID unique object identifier
	UID string
	// App defines a k8s application specification
	App *spec.AppSpec
	// ScheduleOptions are options that callers to pass to influence the apps that get schduled
	ScheduleOptions ScheduleOptions
	// SkipVolumeValidation for cases when use volume driver other than portworx
	SkipVolumeValidation bool
	// SkipClusterScopedObject for cases of multi-cluster backup when Storage class does not restored
	SkipClusterScopedObject bool
	// RefreshStorageEndpoint force refresh the storage driver endpoint
	RefreshStorageEndpoint bool
	// ReadinessTimeout time within which context is expected to be up
	ReadinessTimeout time.Duration
	// HelmRepo info for helm chart schedules
	HelmRepo *HelmRepo
}

// DeepCopy create a copy of Context
func (in *Context) DeepCopy() *Context {
	if in == nil {
		return nil
	}
	out := new(Context)
	out.UID = in.UID
	out.App = in.App.DeepCopy()
	return out
}

// GetID returns the unique ID for the context. This encompasses the instance ID
// provided by users during schedule of the context and the ID of the app specs
func (in *Context) GetID() string {
	return in.App.GetID(in.UID)
}

// AppConfig custom settings
type AppConfig struct {
	Replicas             int      `yaml:"replicas"`
	VolumeSize           string   `yaml:"volume_size"`
	WorkloadSize         string   `yaml:"workload_size"`
	ClaimsCount          int      `yaml:"claims_count"`
	CustomCommand        []string `yaml:"custom_command"`
	CustomArgs           []string `yaml:"custom_args"`
	StorageClassSharedv4 string   `yaml:"storage_class_sharedv4"`
	PVCAccessMode        string   `yaml:"pvc_access_mode"`
	Repl                 string   `yaml:"repl"`
	Fs                   string   `yaml:"fs"`
	AggregationLevel     string   `yaml:"aggregation_level"`
}

// InitOptions initialization options
type InitOptions struct {

	// SpecDir app spec directory
	SpecDir string
	// VolDriverName volume driver name
	VolDriverName string
	// NodeDriverName node driver name
	NodeDriverName string
	// MonitorDriverName monitor driver name
	MonitorDriverName string
	// ConfigMap  identifies what config map should be used to
	SecretConfigMapName string
	// HelmValuesConfigMapName custom values for helm charts
	HelmValuesConfigMapName string
	// CustomAppConfig custom settings for apps
	CustomAppConfig map[string]AppConfig
	// StorageProvisioner name
	StorageProvisioner string
	// SecretType secret used for encryption keys
	SecretType string
	// VaultAddress vault api address
	VaultAddress string
	// VaultToken vault authentication token
	VaultToken string
	// PureVolumes identifies if this setup is using Pure backend
	PureVolumes bool
	// PureSANType identifies which SAN type is being used for Pure volumes
	PureSANType string
	// RunCSISnapshotAndRestoreManyTest identifies if Pure clone many test is enabled
	RunCSISnapshotAndRestoreManyTest bool
	//SecureApps identifies apps to be deployed with secure annotation in storage class
	SecureApps []string
	// AnthosAdminWorkStationNodeIP needed for anthos scheduler
	AnthosAdminWorkStationNodeIP string
	// AnthosInstancePath needed for anthos scheduler
	AnthosInstancePath string
}

// ScheduleOptions are options that callers to pass to influence the apps that get schduled
type ScheduleOptions struct {
	// AppKeys identified a list of applications keys that users wants to schedule (Optional)
	AppKeys []string
	// Nodes restricts the applications to get scheduled only on these nodes (Optional)
	Nodes []node.Node
	// StorageProvisioner identifies what storage provider should be used
	StorageProvisioner string
	// ConfigMap  identifies what config map should be used to
	ConfigMap string
	// AutopilotRule identifies options for autopilot (Optional)
	AutopilotRule apapi.AutopilotRule
	// Scheduler  identifies what scheduler will be used
	Scheduler string
	// Labels is a map of {key,value} pairs for labeling spec objects
	Labels map[string]string
	// PvcNodesAnnotation is a comma separated Node ID's  to use for replication sets of the volume
	PvcNodesAnnotation []string
	// PvcSize is the size of PVC
	PvcSize int64
	// Upgrade specifies whether to schedule an upgrade job
	Upgrade bool
	// Namespace to schedule app installation if not empty
	Namespace string
	// TopoLogy Labels
	TopologyLabels []map[string]string
}

// Driver must be implemented to provide test support to various schedulers.
type Driver interface {
	spec.Parser

	// Init initializes the scheduler driver
	Init(schedOpts InitOptions) error

	// String returns the string name of this driver.
	String() string

	// IsNodeReady checks if node is in ready state. Returns nil if ready.
	IsNodeReady(n node.Node) error

	// GetNodesForApp returns nodes on which given app context is running
	GetNodesForApp(*Context) ([]node.Node, error)

	// Schedule starts applications and returns a context for each one of them
	Schedule(instanceID string, opts ScheduleOptions) ([]*Context, error)

	// ScheduleWithCustomAppSpecs starts applications with custom app specs and returns a context for each one of them
	ScheduleWithCustomAppSpecs(apps []*spec.AppSpec, instanceID string, options ScheduleOptions) ([]*Context, error)

	// WaitForRunning waits for application to start running.
	WaitForRunning(cc *Context, timeout, retryInterval time.Duration) error

	// AddTasks adds tasks to an existing context
	AddTasks(*Context, ScheduleOptions) error

	// ScheduleUninstall uninstalls tasks from an existing context
	ScheduleUninstall(*Context, ScheduleOptions) error

	// RemoveAppSpecsByName removes certain specs from list to avoid validation
	RemoveAppSpecsByName(ctx *Context, removeSpecs []interface{}) error

	// UpdateTasksID updates task IDs in the given context
	UpdateTasksID(*Context, string) error

	// Destroy removes a application. It does not delete the volumes of the task.
	Destroy(*Context, map[string]bool) error

	// WaitForDestroy waits for application to destroy.
	WaitForDestroy(*Context, time.Duration) error

	// SelectiveWaitForTermination waits for application pods to be terminated except on the nodes
	// provided in the exclude list
	SelectiveWaitForTermination(ctx *Context, timeout time.Duration, excludeList []node.Node) error

	// DeleteTasks deletes all tasks of the application (not the application). DeleteTasksOptions is optional.
	DeleteTasks(*Context, *DeleteTasksOptions) error

	// GetVolumeDriverVolumeName returns name of volume which is refered by volume driver
	GetVolumeDriverVolumeName(name string, namespace string) (string, error)

	// GetVolumeParameters Returns a maps, each item being a volume and it's options
	GetVolumeParameters(*Context) (map[string]map[string]string, error)

	// ValidateVolumes validates storage volumes in the provided context
	ValidateVolumes(cc *Context, timeout, retryInterval time.Duration, options *VolumeOptions) error

	// ValidateTopologyLabel validate topology Labels for App
	ValidateTopologyLabel(cc *Context) error

	// GetSnapShotData retruns volumesnapshotdata
	GetSnapShotData(ctx *Context, snapshotName, snapshotNameSpace string) (*snapv1.VolumeSnapshotData, error)

	// DeleteSnapshots  delete the snapshots
	DeleteSnapShot(ctx *Context, snapshotName, snapshotNameSpace string) error

	// GetSnapshotsInNameSpace get the snapshots list for the namespace
	GetSnapshotsInNameSpace(ctx *Context, snapshotNameSpace string) (*snapv1.VolumeSnapshotList, error)

	// DeleteVolumes will delete all storage volumes for the given context
	DeleteVolumes(*Context, *VolumeOptions) ([]*volume.Volume, error)

	// GetVolumes returns all storage volumes for the given context
	GetVolumes(*Context) ([]*volume.Volume, error)

	// GetPureVolumes returns all PureVolumes is enabled by type (PureBlock or PureFile)
	GetPureVolumes(*Context, string) ([]*volume.Volume, error)

	// GetPodsForPVC returns pods using the pvc
	GetPodsForPVC(pvcname, namespace string) ([]corev1.Pod, error)

	// GetPodLog returns logs for all the pods in the specified context
	GetPodLog(ctx *Context, sinceSeconds int64, containerName string) (map[string]string, error)

	// ResizeVolume resizes all the volumes of a given context
	ResizeVolume(*Context, string) ([]*volume.Volume, error)

	// GetSnapshots returns all storage snapshots for the given context
	GetSnapshots(*Context) ([]*volume.Snapshot, error)

	// Describe generates a bundle that can be used by support - logs, cores, states, etc
	Describe(*Context) (string, error)

	// ScaleApplication scales the current applications using the new scales from the GetScaleFactorMap.
	ScaleApplication(*Context, map[string]int32) error

	// GetScaleFactorMap gets a map of current applications to their new scales, based on "factor"
	GetScaleFactorMap(*Context) (map[string]int32, error)

	// StopSchedOnNode stops scheduler service on the given node
	StopSchedOnNode(n node.Node) error

	// StartSchedOnNode starts scheduler service on the given node
	StartSchedOnNode(n node.Node) error

	// RefreshNodeRegistry refreshes node registry
	RefreshNodeRegistry() error

	// RescanSpecs specified in specDir
	RescanSpecs(specDir, storageDriver string) error

	// EnableSchedulingOnNode enable apps to be scheduled to a given node
	EnableSchedulingOnNode(n node.Node) error

	// DisableSchedulingOnNode disable apps to be scheduled to a given node
	DisableSchedulingOnNode(n node.Node) error

	// PrepareNodeToDecommission prepares a given node for decommissioning
	PrepareNodeToDecommission(n node.Node, provisioner string) error

	// IsScalable check if a given spec is scalable or not
	IsScalable(spec interface{}) bool

	// ValidateVolumeSnapshotRestore return nil if snapshot is restored successuflly to
	// parent volumes
	ValidateVolumeSnapshotRestore(*Context, time.Time) error

	// GetTokenFromConfigMap gets token for a volume
	GetTokenFromConfigMap(string) (string, error)

	// AddLabelOnNode adds key value label on the node
	AddLabelOnNode(node.Node, string, string) error

	// RemoveLabelOnNode removes label on the node
	RemoveLabelOnNode(node.Node, string) error

	// IsAutopilotEnabledForVolume checks if autopilot enabled for a given volume
	IsAutopilotEnabledForVolume(*volume.Volume) bool

	// SaveSchedulerLogsToFile gathers all scheduler logs into a file
	SaveSchedulerLogsToFile(node.Node, string) error

	// GetAutopilotNamespace gets the Autopilot namespace
	GetAutopilotNamespace() (string, error)

	// GetIOBandwidth gets container start and end time
	GetIOBandwidth(string, string) (int, error)

	// CreateAutopilotRule creates the AutopilotRule object
	CreateAutopilotRule(apRule apapi.AutopilotRule) (*apapi.AutopilotRule, error)

	// GetAutopilotRule gets the AutopilotRule for the provided name
	GetAutopilotRule(name string) (*apapi.AutopilotRule, error)

	// UpdateAutopilotRule updates the AutopilotRule
	UpdateAutopilotRule(*apapi.AutopilotRule) (*apapi.AutopilotRule, error)

	// ListAutopilotRules lists AutopilotRules
	ListAutopilotRules() (*apapi.AutopilotRuleList, error)

	// DeleteAutopilotRules deletes AutopilotRule
	DeleteAutopilotRule(name string) error

	// GetActionApproval gets the ActionApproval for the provided name
	GetActionApproval(namespace, name string) (*apapi.ActionApproval, error)

	// UpdateActionApproval updates the ActionApproval
	UpdateActionApproval(namespace string, actionApproval *apapi.ActionApproval) (*apapi.ActionApproval, error)

	// DeleteActionApproval deletes the ActionApproval of the given name
	DeleteActionApproval(namespace, name string) error

	// ListActionApprovals lists ActionApproval
	ListActionApprovals(namespace string) (*apapi.ActionApprovalList, error)

	// GetEvents should return all the events from the scheduler since the time torpedo started
	GetEvents() map[string][]Event

	// ValidateAutopilotEvents validates events for PVCs injected by autopilot
	ValidateAutopilotEvents(ctx *Context) error

	// ValidateAutopilotRuleObject validates Autopilot rule object
	ValidateAutopilotRuleObjects() error

	// GetWorkloadSizeFromAppSpec gets workload size from an application spec
	GetWorkloadSizeFromAppSpec(ctx *Context) (uint64, error)

	// SetConfig sets connnection config (e.g. kubeconfig in case of k8s) for scheduler driver
	SetConfig(configPath string) error

	// UpgradeScheduler upgrades the scheduler on the cluster to the specified version
	UpgradeScheduler(version string) error

	// CreateSecret creates new secret with given name in given namespace
	CreateSecret(namespace, name, dataField, secretDataString string) error

	// GetSecret returns secret with given name in given namespace
	GetSecretData(namespace, name, dataField string) (string, error)

	// DeleteSecret deletes secret with given name in given namespace
	DeleteSecret(namespace, name string) error

	// RecyleNode deletes nodes with given node
	RecycleNode(n node.Node) error

	// CreateCsiSnapshotClass create csi snapshot class
	CreateCsiSnapshotClass(snapClassName string, deleionPolicy string) (*v1beta1.VolumeSnapshotClass, error)

	// CreateCsiSnapshot create csi snapshot for given pvc
	// TODO: there's probably better place to place this test, it creates the snapshot and also does the validation.
	// At the same time, there's also other validation functions in this interface as well. So we should look into ways
	// to make the interface consistent
	CreateCsiSnapshot(name string, namespace string, class string, pvc string) (*v1beta1.VolumeSnapshot, error)

	// CSISnapshotTest create csi snapshot and return a pvc using that snapshot
	// TODO: there's probably better place to place this test, it creates the snapshot and also does the validation.
	// At the same time, there's also other validation functions in this interface as well. So we should look into ways
	// to make the interface consistent
	CSISnapshotTest(*Context, CSISnapshotRequest) error

	// CSISnapshotAndRestoreMany create a single snapshot and try to restore many volumes
	CSISnapshotAndRestoreMany(*Context, CSISnapshotRequest) error

	// CSICloneTest clones a volume and validate the content
	CSICloneTest(*Context, CSICloneRequest) error

	// CreateCsiSnapsForVolumes create csi snapshots for all volumes in a context
	CreateCsiSnapsForVolumes(*Context, string) (map[string]*v1beta1.VolumeSnapshot, error)

	// GetCsiSnapshots return snapshot lists for a volume
	GetCsiSnapshots(string, string) ([]*v1beta1.VolumeSnapshot, error)

	// ValidateCsiSnapshots validate csi snapshots in the context
	ValidateCsiSnapshots(*Context, map[string]*v1beta1.VolumeSnapshot) error

	// RestoreCsiSnapAndValidate restore csi snapshot and validate the restore.
	RestoreCsiSnapAndValidate(*Context, map[string]*storageapi.StorageClass) (map[string]corev1.PersistentVolumeClaim, error)

	// DeleteCsiSnapsForVolumes delete csi snapshots for app volumes
	DeleteCsiSnapsForVolumes(ctx *Context, retainCount int) error

	// DeleteCsiSnapshot delete a snapshots from namespace
	DeleteCsiSnapshot(ctx *Context, snapshotName string, snapshotNameSpace string) error

	// GetPodsRestartCount gets restart count maps for pods in given namespace
	GetPodsRestartCount(namespace string, label map[string]string) (map[*corev1.Pod]int32, error)

	// AddNamespaceLabel adds a label key=value on the given namespace
	AddNamespaceLabel(namespace string, labelMap map[string]string) error

	// RemoveNamespaceLabel removes the label with key on given namespace
	RemoveNamespaceLabel(namespace string, labelMap map[string]string) error

	// GetNamespaceLabel gets the labels on given namespace
	GetNamespaceLabel(namespace string) (map[string]string, error)
}

var (
	schedulers = make(map[string]Driver)
)

// DeleteTasksOptions are options supplied to the DeleteTasks API
type DeleteTasksOptions struct {
	api.TriggerOptions
}

// UpgradeAutopilotOptions are options supplied to the UpgradeAutopilot API
type UpgradeAutopilotOptions struct {
	api.TriggerOptions
}

// VolumeOptions are options supplied to the scheduler Volume APIs
type VolumeOptions struct {
	// SkipClusterScopedObjects skips volume operations on cluster scoped objects like storage class
	SkipClusterScopedObjects bool
	// ExpectError indicates a boolean that tells the code to expect an error
	ExpectError bool
}

// Event collects kubernetes events data for further validation
type Event struct {
	Message   string
	EventTime v1.MicroTime
	Count     int32
	LastSeen  v1.Time
	Kind      string
	Type      string
}

// HelmRepo has the related info about the repo
type HelmRepo struct {
	RepoName    string `yaml:"reponame"`
	ChartName   string `yaml:"chartname"`
	ReleaseName string `yaml:"releasename"`
	URL         string
	Namespace   string
	Version     string
	Values      string
}

// Register registers the given scheduler driver
func Register(name string, d Driver) error {
	if _, ok := schedulers[name]; !ok {
		schedulers[name] = d
	} else {
		return fmt.Errorf("scheduler driver: %s is already registered", name)
	}

	return nil
}

// Get returns a registered scheduler test provider.
func Get(name string) (Driver, error) {
	if d, ok := schedulers[name]; ok {
		return d, nil
	}
	return nil, &errors.ErrNotFound{
		ID:   name,
		Type: "Scheduler",
	}
}

// CSISnapshotRequest contains the necessary info to create a CSI snapshot for validation purpose
type CSISnapshotRequest struct {
	Namespace         string
	Timestamp         string
	OriginalPVCName   string
	SnapName          string
	RestoredPVCName   string
	SnapshotclassName string
}

// CSICloneRequest contains the necessary info to clone from an existing CSI volume
type CSICloneRequest struct {
	Namespace       string
	Timestamp       string
	OriginalPVCName string
	RestoredPVCName string
}
