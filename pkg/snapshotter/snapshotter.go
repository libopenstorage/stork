package snapshotter

import (
	"fmt"
	"sync"

	v1 "k8s.io/api/core/v1"
)

const (
	// StorkSnapshotRestoreNamespacesAnnotation is annotation used to specify the
	// comma separated list of namespaces to which the snapshot can be restored
	StorkSnapshotRestoreNamespacesAnnotation = "stork.libopenstorage.org/snapshot-restore-namespaces"

	// StorkSnapshotSourceNamespaceAnnotation Annotation used to specify the
	// source of the snapshot when creating a PVC
	StorkSnapshotSourceNamespaceAnnotation = "stork.libopenstorage.org/snapshot-source-namespace"
)

// List of supported snapshot drivers.
var (
	ExternalStorage = "external-storage"
)

// Status is a snapshot status.
type Status string

// List of known snapshot states.
var (
	StatusInProgress Status = "InProgress"
	StatusReady      Status = "Ready"
	StatusFailed     Status = "Failed"
	StatusUnknown    Status = "Unknown"
)

// SnapshotInfo provides the current state of Snapshot
type SnapshotInfo struct {
	// Status provides the current status of snapshot
	Status Status
	// Reason provides additional context to the status of the snapshot
	Reason string
	// Size is the actual size of the snapshot
	Size uint64
	// Class defines the driver specific snapshot attributes
	Class interface{}
	// SnapshotRequest is the original driver specific snapshot request
	SnapshotRequest interface{}
	// Content is the actual volume snapshot of a volume
	Content interface{}
}

// RestoreInfo provides the current status of Restore operation from a snapshot
type RestoreInfo struct {
	// Status provides the current status of snapshot
	Status Status
	// Reason provides additional context to the status of the snapshot
	Reason string
	// VolumeName is the name of the volume being restored
	VolumeName string
	// Size is the size of the PVC being restored
	Size uint64
}

// Driver interface provides APIs for a storage provider
// to take a local snapshot and create a PVC referencing it.
type Driver interface {
	// CreateSnapshot creates a volume snapshot for a pvc.
	CreateSnapshot(opts ...Option) (snapName, namespace, driverName string, err error)
	// DeleteSnapshot removes a snapshot.
	DeleteSnapshot(name, namespace string, retain bool) error
	// SnapshotStatus returns a status for a snapshot.
	SnapshotStatus(name, namespace string) (SnapshotInfo, error)
	// RestoreVolumeClaim creates a persistent volume claim from a provided snapshot.
	RestoreVolumeClaim(opts ...Option) (*v1.PersistentVolumeClaim, error)
	// RestoreStatus returns the status of a restore operation
	RestoreStatus(pvcName, namespace string) (RestoreInfo, error)
	// CancelRestore cancels a restore operation
	CancelRestore(pvcName, namespace string) error
}

// Snapshotter inteface returns a Driver object
type Snapshotter interface {
	// Driver returns the driver based on the provided name
	Driver(name string) (Driver, error)
}

// NewDefaultSnapshotter returns the default implementation of Snapshotter
func NewDefaultSnapshotter() Snapshotter {
	providers := make(map[string]Driver)
	return &defaultSnapshotter{providers: providers}
}

type defaultSnapshotter struct {
	providersLock sync.Mutex
	providers     map[string]Driver
}

func (d *defaultSnapshotter) Driver(name string) (Driver, error) {
	d.providersLock.Lock()
	defer d.providersLock.Unlock()
	if driver, exists := d.providers[name]; exists {
		return driver, nil
	}

	if name == csiProviderName {
		driver, err := NewCSIDriver()
		if err != nil {
			return nil, err
		}
		d.providers[name] = driver
		return driver, nil
	}
	return nil, fmt.Errorf("driver not found")
}
