package volume

import (
	"errors"
	"os"
	"strings"
)

// Driver defines an external volume driver interface that must be implemented
// by any external storage provider that wants to qualify their product with
// CSI.
type Driver interface {
	// String returns the string name of this driver.
	String() string

	// Init initializes the volume driver.
	Init() error

	// CleanupVolume forcefully unmounts/detaches and deletes a storage volume.
	// This is only called by Torpedo during cleanup operations, it is not
	// used during orchestration simulations.
	CleanupVolume(name string) error

	// Stop must cause the volume driver to exit or get killed on a given node.
	Stop(ip string) error

	// Start must cause the volume driver to start on a given node.
	Start(ip string) error

	// WaitStart must wait till the volume driver becomes usable on a given node.
	WaitStart(ip string) error
}

var (
	nodes   []string
	drivers = make(map[string]Driver)
)

func register(name string, d Driver) error {
	drivers[name] = d
	return nil
}

// Get an external storage provider to be used with Torpedo.
func Get(name string) (Driver, error) {
	d, ok := drivers[name]
	if ok {
		return d, nil
	}

	return nil, errors.New("No such volume driver installed")
}

func init() {
	nodes = strings.Split(os.Getenv("CLUSTER_NODES"), ",")
}
