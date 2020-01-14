package scheduler

import (
	"fmt"
	"time"

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
)

// Context holds the execution context of a test task.
type Context struct {
	// UID unique object identifier
	UID string
	// App defines a k8s application specification
	App *spec.AppSpec
	// Options are options that callers to pass to influence the apps that get schduled
	Options ScheduleOptions
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

// AutopilotRuleConditionExpressions are the conditions to check on the rule objects
type AutopilotRuleConditionExpressions struct {
	Key      string
	Operator string
	Values   []string
}

// AutopilotRuleActions are the actions to run for the rule when the conditions are met
type AutopilotRuleActions struct {
	Name   string
	Params map[string]string
}

// AutopilotRuleParameters are rule parameters for Autopilot
type AutopilotRuleParameters struct {
	// ActionsCoolDownPeriod is the duration in seconds for which autopilot will not
	// re-trigger any actions once they have been executed.
	ActionsCoolDownPeriod int64
	// MatchLabels is a map of {key,value} pairs. A single {key,value} in the matchLabels
	// map is equivalent to an element of matchExpressions, whose key field is "key", the
	// operator is "In", and the values array contains only "value". The requirements are ANDed.
	// +optional
	MatchLabels map[string]string
	// RuleAction defines an action for the rule
	RuleActions []AutopilotRuleActions
	// RuleConditionExpressions defines the conditions for the rule
	RuleConditionExpressions []AutopilotRuleConditionExpressions
	// ExpectedPVCSize is the expected PVC size after resize
	ExpectedPVCSize int64
}

// AutopilotParameters are parameters that using for Autopilot
type AutopilotParameters struct {
	// Enabled indicates if the Autopilot is enabled
	Enabled bool
	// The unique Autopilotrule name within a namespace
	Name string
	// Namespace is the kubernetes namespace in which Autopilot rule will be created
	Namespace string
	// PollInterval defined the interval in seconds at which the conditions for the
	// rule are queried from the metrics provider
	PollInterval int64
	// AutopilotRuleParameters are the parameters that will be used for Autopilot rule
	AutopilotRuleParameters AutopilotRuleParameters
}

// AppConfig custom settings
type AppConfig struct {
	Replicas   int    `yaml:"replicas"`
	VolumeSize string `yaml:"volume_size"`
}

// InitOptions initialization options
type InitOptions struct {

	// SpecDir app spec directory
	SpecDir string
	// VolDriverName volume driver name
	VolDriverName string
	// NodeDriverName node driver name
	NodeDriverName string
	// ConfigMap  identifies what config map should be used to
	SecretConfigMapName string
	// CustomAppConfig custom settings for apps
	CustomAppConfig map[string]AppConfig
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
	// AutopilotParameters identifies options for autopilot (Optional)
	AutopilotParameters *AutopilotParameters
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

	// WaitForRunning waits for application to start running.
	WaitForRunning(cc *Context, timeout, retryInterval time.Duration) error

	// AddTasks adds tasks to an existing context
	AddTasks(*Context, ScheduleOptions) error

	// UpdateTasksID updates task IDs in the given context
	UpdateTasksID(*Context, string) error

	// Destroy removes a application. It does not delete the volumes of the task.
	Destroy(*Context, map[string]bool) error

	// WaitForDestroy waits for application to destroy.
	WaitForDestroy(*Context, time.Duration) error

	// DeleteTasks deletes all tasks of the application (not the applicaton)
	DeleteTasks(*Context) error

	// GetVolumeParameters Returns a maps, each item being a volume and it's options
	GetVolumeParameters(*Context) (map[string]map[string]string, error)

	// InspectVolumes inspects a storage volume.
	InspectVolumes(cc *Context, timeout, retryInterval time.Duration) error

	// DeleteVolumes will delete all storage volumes for the given context
	DeleteVolumes(*Context) ([]*volume.Volume, error)

	// GetVolumes returns all storage volumes for the given context
	GetVolumes(*Context) ([]*volume.Volume, error)

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
	RescanSpecs(specDir string) error

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

	// Get token for a volume
	GetTokenFromConfigMap(string) (string, error)
}

var (
	schedulers = make(map[string]Driver)
)

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
