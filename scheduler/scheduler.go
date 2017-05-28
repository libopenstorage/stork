package scheduler

import (
	"errors"
)

type Volume struct {
	Driver string
	Name   string
	Path   string
	Size   int // in MB
	Opt    []string
}

type Task struct {
	Name string
	Img  string
	Tag  string
	Env  []string
	Cmd  []string
	Vol  Volume
}

type Context struct {
	Id     string
	Task   Task
	Ip     string
	Status int
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

	// Start task
	Start(*Context) error

	// WaitDone waits for task to complete.
	WaitDone(*Context) error

	// Run runs a task to completion.
	Run(*Context) error

	// Destroy removes a task.  Must also delete the external volume.
	Destroy(*Context) error

	// DestroyByName removes a task by name.  Must also delete the external volume.
	DestroyByName(string) error

	// InspectVolume inspects a storage volume.
	InspectVolume(name string) (*Volume, error)
}

var (
	drivers = make(map[string]Driver)
)

func register(name string, d Driver) error {
	drivers[name] = d
	return nil
}

func Get(name string) (Driver, error) {
	if d, ok := drivers[name]; ok {
		return d, nil
	} else {
		return nil, errors.New("No such scheduler driver installed")
	}
}
