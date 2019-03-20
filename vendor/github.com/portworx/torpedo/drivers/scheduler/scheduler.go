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
	UID string
	App *spec.AppSpec
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

// ScheduleOptions are options that callers to pass to influence the apps that get schduled
type ScheduleOptions struct {
	// AppKeys identified a list of applications keys that users wants to schedule (Optional)
	AppKeys []string
	// Nodes restricts the applications to get scheduled only on these nodes (Optional)
	Nodes []node.Node
}

// Driver must be implemented to provide test support to various schedulers.
type Driver interface {
	spec.Parser

	// Init initializes the scheduler driver
	Init(string, string, string) error

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

	// Destroy removes a application. It does not delete the volumes of the task.
	Destroy(*Context, map[string]bool) error

	// WaitForDestroy waits for application to destroy.
	WaitForDestroy(*Context) error

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
	ResizeVolume(*Context) ([]*volume.Volume, error)

	// GetSnapshots returns all storage snapshots for the given context
	GetSnapshots(*Context) ([]*volume.Snapshot, error)

	// Describe generates a bundle that can be used by support - logs, cores, states, etc
	Describe(*Context) (string, error)

	// Scale the current applications using the new scales from the GetScaleFactorMap.
	ScaleApplication(*Context, map[string]int32) error

	// Get a map of current applications to their new scales, based on "factor"
	GetScaleFactorMap(*Context) (map[string]int32, error)

	// Stop scheduler service on the given node
	StopSchedOnNode(n node.Node) error

	// Start scheduler service on the given node
	StartSchedOnNode(n node.Node) error

	// RescanSpecs specified in specDir
	RescanSpecs(specDir string) error
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
