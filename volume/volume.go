package volume

import (
	"errors"
)

type Driver interface {
	// String returns the string name of this driver.
	String() string

	// Exit must cause the volume driver to exit on a given node.
	Exit(Ip string) error

	// Start must cause the volume driver to start on a given node.
	Start(Ip string) error
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
		return nil, errors.New("No such volume driver installed")
	}
}
