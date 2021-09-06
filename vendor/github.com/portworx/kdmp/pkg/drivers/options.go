package drivers

import (
	"fmt"
	"strings"
)

// JobOption is used for job configuration.
type JobOption func(opts *JobOpts) error

// JobOpts defines all job parameters.
type JobOpts struct {
	SourcePVCName           string
	DestinationPVCName      string
	Namespace               string
	BackupLocationName      string
	BackupLocationNamespace string
	VolumeBackupName        string
	VolumeBackupNamespace   string
	DataExportName          string
	Labels                  map[string]string
}

// WithSourcePVC is job parameter.
func WithSourcePVC(name string) JobOption {
	return func(opts *JobOpts) error {
		if strings.TrimSpace(name) == "" {
			return fmt.Errorf("source pvc name should be set")
		}
		opts.SourcePVCName = strings.TrimSpace(name)
		return nil
	}
}

// WithDestinationPVC is job parameter.
func WithDestinationPVC(name string) JobOption {
	return func(opts *JobOpts) error {
		opts.DestinationPVCName = strings.TrimSpace(name)
		return nil
	}
}

// WithNamespace is job parameter.
func WithNamespace(ns string) JobOption {
	return func(opts *JobOpts) error {
		if strings.TrimSpace(ns) == "" {
			return fmt.Errorf("namespace should be set")
		}
		opts.Namespace = strings.TrimSpace(ns)
		return nil
	}
}

// WithBackupLocationName is job parameter.
func WithBackupLocationName(name string) JobOption {
	return func(opts *JobOpts) error {
		opts.BackupLocationName = strings.TrimSpace(name)
		return nil
	}
}

// WithBackupLocationNamespace is job parameter.
func WithBackupLocationNamespace(ns string) JobOption {
	return func(opts *JobOpts) error {
		opts.BackupLocationNamespace = strings.TrimSpace(ns)
		return nil
	}
}

// WithVolumeBackupName is job parameter.
func WithVolumeBackupName(name string) JobOption {
	return func(opts *JobOpts) error {
		opts.VolumeBackupName = strings.TrimSpace(name)
		return nil
	}
}

// WithVolumeBackupNamespace is job parameter.
func WithVolumeBackupNamespace(ns string) JobOption {
	return func(opts *JobOpts) error {
		opts.VolumeBackupNamespace = strings.TrimSpace(ns)
		return nil
	}
}

// WithLabels is job parameter.
func WithLabels(l map[string]string) JobOption {
	return func(opts *JobOpts) error {
		opts.Labels = l
		return nil
	}
}

// WithDataExportName is job parameter.
func WithDataExportName(name string) JobOption {
	return func(opts *JobOpts) error {
		opts.DataExportName = name
		return nil
	}
}
