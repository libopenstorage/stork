package drivers

import (
	"fmt"
	"strings"
)

// JobOption is used for job configuration.
type JobOption func(opts *JobOpts) error

// JobOpts defines all job parameters.
type JobOpts struct {
	SourcePVCName               string
	SourcePVCNamespace          string
	DestinationPVCName          string
	Namespace                   string
	BackupLocationName          string
	BackupLocationNamespace     string
	VolumeBackupName            string
	VolumeBackupNamespace       string
	VolumeBackupDeleteName      string
	VolumeBackupDeleteNamespace string
	DataExportName              string
	SnapshotID                  string
	CredSecretName              string
	CredSecretNamespace         string
	MaintenanceStatusName       string
	MaintenanceStatusNamespace  string
	JobName                     string
	JobNamespace                string
	ServiceAccountName          string
	BackupObjectName            string
	BackupObjectUID             string
	Labels                      map[string]string
	CertSecretName              string
	CertSecretNamespace         string
	MaintenanceType             string
	RepoPVCName                 string
	Compression                 string
}

// WithBackupObjectName is job parameter.
func WithBackupObjectName(name string) JobOption {
	return func(opts *JobOpts) error {
		if strings.TrimSpace(name) == "" {
			return fmt.Errorf("backupObject name should be set")
		}
		opts.BackupObjectName = strings.TrimSpace(name)
		return nil
	}
}

// WithBackupObjectUID is job parameter.
func WithBackupObjectUID(uid string) JobOption {
	return func(opts *JobOpts) error {
		if strings.TrimSpace(uid) == "" {
			return fmt.Errorf("backupObject uid should be set")
		}
		opts.BackupObjectUID = strings.TrimSpace(uid)
		return nil
	}
}

// WithJobName is job parameter.
func WithJobName(name string) JobOption {
	return func(opts *JobOpts) error {
		if strings.TrimSpace(name) == "" {
			return fmt.Errorf("job name should be set")
		}
		opts.JobName = strings.TrimSpace(name)
		return nil
	}
}

// WithJobNamespace is job parameter.
func WithJobNamespace(namespace string) JobOption {
	return func(opts *JobOpts) error {
		if strings.TrimSpace(namespace) == "" {
			return fmt.Errorf("job namespace should be set")
		}
		opts.JobNamespace = strings.TrimSpace(namespace)
		return nil
	}
}

// WithServiceAccountName is job parameter.
func WithServiceAccountName(serviceAccountName string) JobOption {
	return func(opts *JobOpts) error {
		if strings.TrimSpace(serviceAccountName) == "" {
			return fmt.Errorf("serviceAccountname should be set")
		}
		opts.ServiceAccountName = strings.TrimSpace(serviceAccountName)
		return nil
	}
}

// WithSnapshotID is job parameter.
func WithSnapshotID(snapshotID string) JobOption {
	return func(opts *JobOpts) error {
		if strings.TrimSpace(snapshotID) == "" {
			return fmt.Errorf("snapshotID should be set")
		}
		opts.SnapshotID = strings.TrimSpace(snapshotID)
		return nil
	}
}

// WithMaintenanceStatusName is job parameter.
func WithMaintenanceStatusName(name string) JobOption {
	return func(opts *JobOpts) error {
		if strings.TrimSpace(name) == "" {
			return fmt.Errorf("maintenance status CR name should be set")
		}
		opts.MaintenanceStatusName = strings.TrimSpace(name)
		return nil
	}
}

// WithMaintenanceStatusNamespace is job parameter.
func WithMaintenanceStatusNamespace(namespace string) JobOption {
	return func(opts *JobOpts) error {
		if strings.TrimSpace(namespace) == "" {
			return fmt.Errorf("maintenance status CR namepace should be set")
		}
		opts.MaintenanceStatusNamespace = strings.TrimSpace(namespace)
		return nil
	}
}

// WithCredSecretName is job parameter.
func WithCredSecretName(name string) JobOption {
	return func(opts *JobOpts) error {
		if strings.TrimSpace(name) == "" {
			return fmt.Errorf("cred secret name should be set")
		}
		opts.CredSecretName = strings.TrimSpace(name)
		return nil
	}
}

// WithCredSecretNamespace is job parameter.
func WithCredSecretNamespace(namespace string) JobOption {
	return func(opts *JobOpts) error {
		if strings.TrimSpace(namespace) == "" {
			return fmt.Errorf("cred secret namespace should be set")
		}
		opts.CredSecretNamespace = strings.TrimSpace(namespace)
		return nil
	}
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

// WithSourcePVCNamespace is job parameter.
func WithSourcePVCNamespace(namespace string) JobOption {
	return func(opts *JobOpts) error {
		if strings.TrimSpace(namespace) == "" {
			return fmt.Errorf("source pvc namespace should be set")
		}
		opts.SourcePVCNamespace = strings.TrimSpace(namespace)
		return nil
	}
}

// WithDestinationPVC is job parameter.
func WithDestinationPVC(name string) JobOption {
	return func(opts *JobOpts) error {
		if strings.TrimSpace(name) == "" {
			return fmt.Errorf("destination pvc name should be set")
		}
		opts.DestinationPVCName = strings.TrimSpace(name)
		return nil
	}
}

// WithRepoPVC is job parameter.
func WithRepoPVC(name string) JobOption {
	return func(opts *JobOpts) error {
		if strings.TrimSpace(name) == "" {
			return fmt.Errorf("repo pvc name should be set")
		}
		opts.RepoPVCName = strings.TrimSpace(name)
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
		if strings.TrimSpace(name) == "" {
			return fmt.Errorf("backuplocation name should be set")
		}
		opts.BackupLocationName = strings.TrimSpace(name)
		return nil
	}
}

// WithBackupLocationNamespace is job parameter.
func WithBackupLocationNamespace(ns string) JobOption {
	return func(opts *JobOpts) error {
		if strings.TrimSpace(ns) == "" {
			return fmt.Errorf("backuplocation namespace should be set")
		}
		opts.BackupLocationNamespace = strings.TrimSpace(ns)
		return nil
	}
}

// WithVolumeBackupName is job parameter.
func WithVolumeBackupName(name string) JobOption {
	return func(opts *JobOpts) error {
		if strings.TrimSpace(name) == "" {
			return fmt.Errorf("volumebackup name should be set")
		}
		opts.VolumeBackupName = strings.TrimSpace(name)
		return nil
	}
}

// WithVolumeBackupNamespace is job parameter.
func WithVolumeBackupNamespace(ns string) JobOption {
	return func(opts *JobOpts) error {
		if strings.TrimSpace(ns) == "" {
			return fmt.Errorf("volumebackup namespace should be set")
		}
		opts.VolumeBackupNamespace = strings.TrimSpace(ns)
		return nil
	}
}

// WithVolumeBackupDeleteName is job parameter.
func WithVolumeBackupDeleteName(name string) JobOption {
	return func(opts *JobOpts) error {
		if strings.TrimSpace(name) == "" {
			return fmt.Errorf("volumeBackupDelete name should be set")
		}
		opts.VolumeBackupDeleteName = strings.TrimSpace(name)
		return nil
	}
}

// WithVolumeBackupDeleteNamespace is job parameter.
func WithVolumeBackupDeleteNamespace(ns string) JobOption {
	return func(opts *JobOpts) error {
		if strings.TrimSpace(ns) == "" {
			return fmt.Errorf("volumeBackupDelete namespace should be set")
		}
		opts.VolumeBackupDeleteNamespace = strings.TrimSpace(ns)
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
		if strings.TrimSpace(name) == "" {
			return fmt.Errorf("dataexport namespace should be set")
		}
		opts.DataExportName = name
		return nil
	}
}

// WithCertSecretName is job parameter.
func WithCertSecretName(name string) JobOption {
	return func(opts *JobOpts) error {
		opts.CertSecretName = name
		return nil
	}
}

// WithCertSecretNamespace is job parameter.
func WithCertSecretNamespace(namespace string) JobOption {
	return func(opts *JobOpts) error {
		opts.CertSecretNamespace = namespace
		return nil
	}
}

// WithMaintenanceType is job parameter.
func WithMaintenanceType(maintenanceType string) JobOption {
	return func(opts *JobOpts) error {
		opts.MaintenanceType = maintenanceType
		return nil
	}
}

// WithCompressionType is job parameter.
func WithCompressionType(compressionType string) JobOption {
	return func(opts *JobOpts) error {
		opts.Compression = compressionType
		return nil
	}
}
