package scheduler

import (
	"errors"
	"os"
	"strings"

	"github.com/portworx/torpedo/services"
)

var (
	nodes []string
)

const (
	// LocalHost will pin a task to the node the task is created on.
	LocalHost = "localhost"
	// ExternalHost will pick any other host in the cluster other than the
	// one the task is created on.
	ExternalHost = "externalhost"
)

// Volume specifies the parameters for creating an external volume.
type Volume struct {
	Driver string
	Name   string
	Path   string
	Size   int // in MB
	Opt    []string
}

// Task specifies the Docker properties of a test task.
type Task struct {
	Name string
	Img  string
	Tag  string
	Env  []string
	Cmd  []string
	Vol  Volume
	IP   string
}

// Context holds the execution context and output values of a test task.
type Context struct {
	ID     string
	Task   Task
	Status int
	Stdout string
	Stderr string
}

// Driver must be implemented to provide test support to various schedulers.
type Driver interface {
	// Services provides the basic service manipulation routines.
	services.Service

	// Init initializes this driver.  Parameters are provided as env variables.
	Init() error

	// GetNodes returns an array of all nodes in the cluster.
	GetNodes() ([]string, error)

	// Create creates a task context.  Does not start the task.
	Create(Task) (*Context, error)

	// Schedule starts a task
	Schedule(*Context) error

	// WaitDone waits for task to complete.
	WaitDone(*Context) error

	// Run runs a task to completion.
	Run(*Context) error

	// Destroy removes a task.  Must also delete the external volume.
	Destroy(*Context) error

	// DestroyByName removes a task by name.  Must also delete the external volume.
	DestroyByName(ip, name string) error

	// InspectVolume inspects a storage volume.
	InspectVolume(ip, name string) (*Volume, error)

	// DeleteVolume will delete a storage volume.
	DeleteVolume(ip, name string) error
}

var (
	schedulers = make(map[string]Driver)
)

func register(name string, d Driver) error {
	schedulers[name] = d
	return nil
}

// Get returns a registered scheduler test provider.
func Get(name string) (Driver, error) {
	nodes = strings.Split(os.Getenv("CLUSTER_NODES"), ",")

	if d, ok := schedulers[name]; ok {
		return d, nil
	}
	return nil, errors.New("No such scheduler driver installed")
}
