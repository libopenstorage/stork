package scheduler

import (
	"errors"
)

type Task struct {
}

type Context struct {
}

type Driver interface {
	// Init initializes this driver.  Parameters are provided as env variables.
	Init() error

	// GetNodes returns an array of all nodes in the cluster.
	GetNodes() ([]string, error)

	// Create creates a task context.  Does not start the task.
	Create(Task) (*Context, error)
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
