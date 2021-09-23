package snapshotsinstance

import (
	"fmt"
	"sync"

	"github.com/portworx/kdmp/pkg/snapshots"
	"github.com/portworx/kdmp/pkg/snapshots/externalstorage"
)

var (
	mu         sync.Mutex
	driversMap = map[string]snapshots.Driver{
		snapshots.ExternalStorage: externalstorage.Driver{},
	}
)

// Add append a driver to the drivers list.
func Add(driver snapshots.Driver) error {
	mu.Lock()
	defer mu.Unlock()

	if driver == nil {
		return fmt.Errorf("driver is nil")
	}

	driversMap[driver.Name()] = driver
	return nil
}

// TODO: replace get with DriverFor(provisioner) func to provide snapshot backend for a storage backend

// Get retrieves a driver for provided name.
func Get(name string) (snapshots.Driver, error) {
	mu.Lock()
	defer mu.Unlock()

	driver, ok := driversMap[name]
	if !ok || driver == nil {
		return nil, fmt.Errorf("%q driver: not found", name)
	}

	return driver, nil
}
