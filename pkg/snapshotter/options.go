package snapshotter

import (
	"fmt"
	"strings"

	v1 "k8s.io/api/core/v1"
)

// Option is used for snapshot configuration.
type Option func(opts *Options) error

// Options defines all snapshot parameters.
type Options struct {
	// Name is a snapshot name.
	Name string
	// Namespace is a snapshot namespace.
	Namespace string
	// PVCName is a persistent volume claim name to make snapshot from or restore to.
	PVCName string
	// PVCName is a persistent volume claim namespace to make snapshot from or restore to.
	PVCNamespace string
	// PVC is a persistent volume claim spec to restore snapshot to.
	PVC v1.PersistentVolumeClaim
	// RestoreNamespace is the namespace to which a snapshot should be restored to.
	RestoreNamespace string
	// SnapshotClassName is the name of the VolumeSnapshotClass requested by the VolumeSnapshot.
	SnapshotClassName string
	// RestoreSnapshotName is the name of the snapshot from which restore will be performed
	RestoreSnapshotName string
	// Annotations are the annotations that can be applied on the snapshot related objects
	Annotations map[string]string
}

// Name is used to set a snapshot name.
func Name(name string) Option {
	return func(opts *Options) error {
		if strings.TrimSpace(name) == "" {
			return fmt.Errorf("snapshot name is empty")
		}
		opts.Name = name
		return nil
	}
}

// Namespace is used to set a snapshot namespace.
func Namespace(name string) Option {
	return func(opts *Options) error {
		if strings.TrimSpace(name) == "" {
			return fmt.Errorf("snapshot namespace is empty")
		}
		opts.Namespace = name
		return nil
	}
}

// PVCName is a persistent volume claim name to make snapshot from.
func PVCName(name string) Option {
	return func(opts *Options) error {
		if strings.TrimSpace(name) == "" {
			return fmt.Errorf("persistent volume claim name is empty")
		}
		opts.PVCName = name
		return nil
	}
}

// PVCNamespace is a persistent volume claim namespace to make snapshot from.
func PVCNamespace(ns string) Option {
	return func(opts *Options) error {
		if strings.TrimSpace(ns) == "" {
			return fmt.Errorf("persistent volume claim namespace is empty")
		}
		opts.PVCNamespace = ns
		return nil
	}
}

// PVC is a persistent volume claim spec to restore snapshot to.
func PVC(pvc v1.PersistentVolumeClaim) Option {
	return func(opts *Options) error {
		opts.PVC = pvc
		return nil
	}
}

// RestoreNamespace is the namespace to which a snapshot should be restored to.
func RestoreNamespace(namespace string) Option {
	return func(opts *Options) error {
		opts.RestoreNamespace = namespace
		return nil
	}
}

// SnapshotClassName is the name of the VolumeSnapshotClass requested by the VolumeSnapshot.
func SnapshotClassName(name string) Option {
	return func(opts *Options) error {
		opts.SnapshotClassName = name
		return nil
	}
}

// RestoreSnapshotName is the snapshot name from which a PVC will be restored
func RestoreSnapshotName(name string) Option {
	return func(opts *Options) error {
		if strings.TrimSpace(name) == "" {
			return fmt.Errorf("restore snapshot name is empty")
		}
		opts.RestoreSnapshotName = name
		return nil
	}
}

// Annotations are the annotations applied on the snapshot related objects
func Annotations(annotations map[string]string) Option {
	return func(opts *Options) error {
		opts.Annotations = make(map[string]string)
		for k, v := range annotations {
			opts.Annotations[k] = v
		}
		return nil
	}
}
