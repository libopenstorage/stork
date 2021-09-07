package snapshots

import (
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

// Driver defines a snapshot interface.
type Driver interface {
	// Name returns a name of the driver backend.
	Name() string
	// CreateSnapshot creates a volume snapshot for a pvc.
	CreateSnapshot(opts ...Option) (name, namespace string, err error)
	// DeleteSnapshot removes a snapshot.
	DeleteSnapshot(name, namespace string) error
	// SnapshotStatus returns a status for a snapshot.
	SnapshotStatus(name, namespace string) (Status, error)
	// RestoreVolumeClaim creates a persistent volume claim from a provided snapshot.
	RestoreVolumeClaim(opts ...Option) (*v1.PersistentVolumeClaim, error)
}
