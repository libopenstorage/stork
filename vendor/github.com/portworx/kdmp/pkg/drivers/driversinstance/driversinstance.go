package driversinstance

import (
	"fmt"
	"sync"

	"github.com/portworx/kdmp/pkg/drivers"
	"github.com/portworx/kdmp/pkg/drivers/kopiabackup"
	"github.com/portworx/kdmp/pkg/drivers/kopiadelete"
	"github.com/portworx/kdmp/pkg/drivers/kopiarestore"
	"github.com/portworx/kdmp/pkg/drivers/resticbackup"
	"github.com/portworx/kdmp/pkg/drivers/resticrestore"
	"github.com/portworx/kdmp/pkg/drivers/rsync"
)

var (
	mu         sync.Mutex
	driversMap = map[string]drivers.Interface{
		drivers.Rsync:         rsync.Driver{},
		drivers.ResticBackup:  resticbackup.Driver{},
		drivers.ResticRestore: resticrestore.Driver{},
		drivers.KopiaBackup:   kopiabackup.Driver{},
		drivers.KopiaRestore:  kopiarestore.Driver{},
		drivers.KopiaDelete:   kopiadelete.Driver{},
	}
)

// Add append a driver to the drivers list.
func Add(driver drivers.Interface) error {
	mu.Lock()
	defer mu.Unlock()

	if driver == nil {
		return fmt.Errorf("driver is nil")
	}

	driversMap[driver.Name()] = driver
	return nil
}

// Get retrieves a driver for provided name.
func Get(name string) (drivers.Interface, error) {
	mu.Lock()
	defer mu.Unlock()

	driver, ok := driversMap[name]
	if !ok || driver == nil {
		return nil, fmt.Errorf("%q driver: not found", name)
	}

	return driver, nil
}
