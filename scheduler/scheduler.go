package scheduler

import (
	"errors"
)

type Volume struct {
	Driver string
	Name   string
	Path   string
	Size   uint64 // in MB
	Opt    []string
}

type Task struct {
	Name string
	Img  string
	Tag  string
	Opt  []string
	Env  []string
	Cmd  string
	Vol  Volume
}

type Context struct {
	Id     string
	NodeIp string
	Stdout string
	Stderr string
}

type Driver interface {
	// Init initializes this driver.  Parameters are provided as env variables.
	Init() error

	// GetNodes returns an array of all nodes in the cluster.
	GetNodes() ([]string, error)

	// Create creates a task context.  Does not start the task.
	Create(Task) (*Context, error)

	// Run runs a task to completion.
	Run(*Context) error

	// InspectVolume inspects a storage volume.
	InspectVolume(name string) (*Volume, error)
}

var (
	drivers map[string]Driver
)

func register(name string, d Driver) error {
	return nil
}

func Get(name string) (Driver, error) {
	if d, ok := drivers[name]; ok {
		return d, nil
	} else {
		return nil, errors.New("No such scheduler driver installed")
	}
}

func init() {
	drivers = make(map[string]Driver)
}
