package scheduler

import (
	"github.com/Sirupsen/logrus"
	"github.com/portworx/torpedo/drivers"
	"github.com/portworx/torpedo/drivers/scheduler/k8s/spec"
	"github.com/portworx/torpedo/pkg/errors"
)

// NodeType identifies the type of the cluster node
type NodeType string

const (
	// NodeTypeMaster identifies a cluster node that is a master/manager
	NodeTypeMaster NodeType = "Master"
	// NodeTypeWorker identifies a cluster node that is a worker
	NodeTypeWorker NodeType = "Worker"
)

// Node encapsulates a node in the cluster
type Node struct {
	Name      string
	Addresses []string
	Type      NodeType
}

// Context holds the execution context and output values of a test task.
type Context struct {
	UID    string
	App    spec.AppSpec
	Status int
	Stdout string
	Stderr string
}

// ScheduleOptions are options that callers to pass to influence the apps that get schduled
type ScheduleOptions struct {
	// AppKeys identified a list of applications keys that users wants to schedule (Optional)
	AppKeys []string
	// Nodes restricts the applications to get scheduled only on these nodes (Optional)
	Nodes []Node
}

// Driver must be implemented to provide test support to various schedulers.
type Driver interface {
	// Driver provides the basic service manipulation routines.
	drivers.Driver

	// String returns the string name of this driver.
	String() string

	// GetNodes returns an array of all nodes in the cluster.
	GetNodes() []Node

	// Schedule starts tasks and returns a context for each one of them
	Schedule(instanceID string, opts ScheduleOptions) ([]*Context, error)

	// WaitForRunning waits for task to complete.
	WaitForRunning(*Context) error

	// Destroy removes a task. It does not delete the volumes of the task.
	Destroy(*Context) error

	// WaitForDestroy waits for task to destroy.
	WaitForDestroy(*Context) error

	// GetVolumes Returns list of volume IDs using by given context
	GetVolumes(*Context) ([]string, error)

	// GetVolumeParameters Returns a maps, each item being a volume and it's options
	GetVolumeParameters(*Context) (map[string]map[string]string, error)

	// InspectVolumes inspects a storage volume.
	InspectVolumes(*Context) error

	// DeleteVolumes will delete a storage volume.
	DeleteVolumes(*Context) error
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
