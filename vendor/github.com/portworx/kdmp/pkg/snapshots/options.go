package snapshots

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
	// PVCSpec is a persistent volume claim spec to restore snapshot to.
	PVCSpec v1.PersistentVolumeClaimSpec
	// RestoreNamespaces is annotation used to specify the comma separated list of namespaces
	// to which the snapshot can be restored
	RestoreNamespaces string
	// SnapshotClassName is the name of the VolumeSnapshotClass requested by the VolumeSnapshot.
	SnapshotClassName string
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

// PVCSpec is a persistent volume claim spec to restore snapshot to.
func PVCSpec(spec v1.PersistentVolumeClaimSpec) Option {
	return func(opts *Options) error {
		// TODO: spec validation?
		opts.PVCSpec = spec
		return nil
	}
}

// RestoreNamespaces is a list of namespaces snapshot is allowed restored to.
func RestoreNamespaces(namespaces ...string) Option {
	return func(opts *Options) error {
		opts.RestoreNamespaces = strings.Join(namespaces, ",")
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
