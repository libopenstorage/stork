package volume

import (
	"github.com/Sirupsen/logrus"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/pkg/errors"
)

// Driver defines an external volume driver interface that must be implemented
// by any external storage provider that wants to qualify their product with
// Torpedo.  The functions defined here are meant to be destructive and illustrative
// of failure scenarious that can happen with an external storage provider.
type Driver interface {
	// Init initializes the volume driver under the given scheduler
	Init(sched string) error

	// String returns the string name of this driver.
	String() string

	// CleanupVolume forcefully unmounts/detaches and deletes a storage volume.
	// This is only called by Torpedo during cleanup operations, it is not
	// used during orchestration simulations.
	CleanupVolume(name string) error

	// InspectVolume inspects a storage volume. params are the custom volume options passed when creating the volume.
	InspectVolume(name string, params map[string]string) error

	// Stop must cause the volume driver to exit or get killed on a given node.
	StopDriver(n node.Node) error

	// Start must cause the volume driver to start on a given node.
	StartDriver(n node.Node) error

	// WaitStart must wait till the volume driver becomes usable on a given node.
	WaitStart(n node.Node) error
}

var (
	volDrivers = make(map[string]Driver)
)

// Register registers the given volume driver
func Register(name string, d Driver) error {
	logrus.Infof("Registering volume driver: %v", name)
	volDrivers[name] = d
	return nil
}

// Get an external storage provider to be used with Torpedo.
func Get(name string) (Driver, error) {
	d, ok := volDrivers[name]
	if ok {
		return d, nil
	}

	return nil, &errors.ErrNotFound{
		ID:   name,
		Type: "VolumeDriver",
	}
}
