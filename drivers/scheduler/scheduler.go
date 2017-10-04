package scheduler

import (
	"github.com/Sirupsen/logrus"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/scheduler/spec"
	"github.com/portworx/torpedo/pkg/errors"
)

// Context holds the execution context and output values of a test task.
type Context struct {
	UID    string
	App    *spec.AppSpec
	Status int
	Stdout string
	Stderr string
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
	Init(specDir string) error

	// String returns the string name of this driver.
	String() string

	// GetNodes returns an array of all nodes in the cluster.
	GetNodes() []node.Node

	// IsNodeReady checks if node is in ready state. Returns nil if ready.
	IsNodeReady(n node.Node) error

	// GetNodesForApp returns nodes on which given app context is running
	GetNodesForApp(*Context) ([]node.Node, error)

	// Schedule starts applications and returns a context for each one of them
	Schedule(instanceID string, opts ScheduleOptions) ([]*Context, error)

	// WaitForRunning waits for application to start running.
	WaitForRunning(*Context) error

	// Destroy removes a application. It does not delete the volumes of the task.
	Destroy(*Context) error

	// WaitForDestroy waits for application to destroy.
	WaitForDestroy(*Context) error

	// DeleteTasks deletes all tasks of the application (not the applicaton)
	DeleteTasks(*Context) error

	// GetVolumes Returns list of volume IDs using by given context
	GetVolumes(*Context) ([]string, error)

	// GetVolumeParameters Returns a maps, each item being a volume and it's options
	GetVolumeParameters(*Context) (map[string]map[string]string, error)

	// InspectVolumes inspects a storage volume.
	InspectVolumes(*Context) error

	// DeleteVolumes will delete a storage volume.
	DeleteVolumes(*Context) error

	// Describe generates a bundle that can be used by support - logs, cores, states, etc
	Describe(*Context) (string, error)
}

var (
	schedulers = make(map[string]Driver)
)

// Register registers the given scheduler driver
func Register(name string, d Driver) error {
	logrus.Infof("Registering sched driver: %v", name)
	schedulers[name] = d
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
