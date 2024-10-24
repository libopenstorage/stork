package objectstore

import (
	"fmt"
	"time"

	stork_api "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/portworx/sched-ops/k8s/stork"
	"github.com/pure-px/torpedo/pkg/errors"
)

var (
	objectstoredriver = make(map[string]Driver)
	k8sStork          = stork.Instance()
)

const (
	driverName           = "objectstore"
	defaultRetryInterval = 10 * time.Second
	defaultTimeout       = 2 * time.Minute
)

// Driver defines an external volume driver interface that must be implemented
type Driver interface {
	// String returns the string name of this driver.
	String() string

	// ValidateBackupsDeletedFromCloud validates if bucket has been deleted from the cloud objectstore
	ValidateBackupsDeletedFromCloud(backupLocation *stork_api.BackupLocation, backupPath string) error

	// TODO: Actually add these per PWX-19768
	// ListBuckets()
	ListBuckets() ([]string, error)

	// ListFilesInBucket(bucketName string) ([]string, error)
	ListFilesInBucket(bucketName string) ([]string, error)

	// CheckConnection() error
	CheckConnection() error
}

type objstore struct {
	DefaultDriver
}

// Get returns the objecstore drive
func Get() (Driver, error) {
	d, ok := objectstoredriver[driverName]
	if ok {
		return d, nil
	}

	return nil, &errors.ErrNotFound{
		ID:   driverName,
		Type: "ObjectstoreDriver",
	}
}

// Register registers the objectstore driver
func Register(driverName string, d Driver) error {
	if _, ok := objectstoredriver[driverName]; !ok {
		objectstoredriver[driverName] = d
	} else {
		return fmt.Errorf("objecstore driver: %s is already registered", driverName)
	}

	fmt.Printf("Successfully registered objectstore driver %s \n", driverName)
	return nil
}

func (o *objstore) String() string {
	return driverName
}

func (o *objstore) ListBuckets() ([]string, error) {
	return []string{}, nil
}

func (o *objstore) ListFilesInBucket(bucketName string) ([]string, error) {
	return []string{}, nil
}

func (o *objstore) CheckConnection() error {
	return nil
}
func init() {
	Register(driverName, &objstore{})
}
