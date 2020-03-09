package backup

import (
	"fmt"
	"time"

	api "github.com/portworx/px-backup-api/pkg/apis/v1"
	"github.com/portworx/torpedo/pkg/errors"
)

// Image Generic struct
type Image struct {
	Type    string
	Version string
}

// Driver for backup
type Driver interface {
	// Org interface
	Org
	// CloudCredential interface
	CloudCredential
	// Cluster interface
	Cluster
	// Backup location interface
	BLocation
	// Backup interface
	Backup
	// Restore interface
	Restore
	// SchedulePolicy interface
	SchedulePolicy
	// ScheduleBackup
	ScheduleBackup
	// Init initializes the backup driver under a given scheduler
	Init(schedulerDriverName string, nodeDriverName string, volumeDriverName string, token string) error
	// String returns the name of this driver
	String() string
}

// Org object interface
type Org interface {
	// CreateOrganization creates Organization
	CreateOrganization(req *api.OrganizationCreateRequest) (*api.OrganizationCreateResponse, error)

	// GetOrganization enumerates organizations
	EnumerateOrganization() (*api.OrganizationEnumerateResponse, error)
}

// CloudCredential object interface
type CloudCredential interface {
	// CreateCloudCredential creates cloud credential objects
	CreateCloudCredential(req *api.CloudCredentialCreateRequest) (*api.CloudCredentialCreateResponse, error)

	// InspectCloudCredential describes the cloud credential
	InspectCloudCredential(req *api.CloudCredentialInspectRequest) (*api.CloudCredentialInspectResponse, error)

	// EnumerateCloudCredential lists the cloud credentials for given Org
	EnumerateCloudCredential(req *api.CloudCredentialEnumerateRequest) (*api.CloudCredentialEnumerateResponse, error)

	// DeletrCloudCredential deletes a cloud credential object
	DeleteCloudCredential(req *api.CloudCredentialDeleteRequest) (*api.CloudCredentialDeleteResponse, error)
}

// Cluster obj interface
type Cluster interface {
	// CreateCluster creates a cluste object
	CreateCluster(req *api.ClusterCreateRequest) (*api.ClusterCreateResponse, error)

	// EnumerateCluster enumerates the cluster objects
	EnumerateCluster(req *api.ClusterEnumerateRequest) (*api.ClusterEnumerateResponse, error)

	// InsepctCluster describes a cluster
	InspectCluster(req *api.ClusterInspectRequest) (*api.ClusterInspectResponse, error)

	// DeleteCluster deletes a cluster object
	DeleteCluster(req *api.ClusterDeleteRequest) (*api.ClusterDeleteResponse, error)
}

// BLocation obj interface
type BLocation interface {
	// CreateBackupLocation creates backup location object
	CreateBackupLocation(req *api.BackupLocationCreateRequest) (*api.BackupLocationCreateResponse, error)

	// EnumerateBackupLocation lists backup locations for an org
	EnumerateBackupLocation(req *api.BackupLocationEnumerateRequest) (*api.BackupLocationEnumerateResponse, error)

	// InspectBackupLocation enumerates backup location objects
	InspectBackupLocation(req *api.BackupLocationInspectRequest) (*api.BackupLocationInspectResponse, error)

	// DeleteBackupLocation deletes backup location objects
	DeleteBackupLocation(req *api.BackupLocationDeleteRequest) (*api.BackupLocationDeleteResponse, error)
}

// Backup obj interface
type Backup interface {
	// CreateBackup creates backup
	CreateBackup(req *api.BackupCreateRequest) (*api.BackupCreateResponse, error)

	// EnumerateBackup enumerates backup objects
	EnumerateBackup(req *api.BackupEnumerateRequest) (*api.BackupEnumerateResponse, error)

	// InspectBackup inspects a backup object
	InspectBackup(req *api.BackupInspectRequest) (*api.BackupInspectResponse, error)

	// DeleteBackup deletes backup
	DeleteBackup(req *api.BackupDeleteRequest) (*api.BackupDeleteResponse, error)

	// WaitForBackupCompletion waits for backup to complete successfully
	// or till timeout is reached. API should poll every `timeBeforeRetry` duration
	WaitForBackupCompletion(backupName string, orgID string,
		timeout time.Duration, timeBeforeRetry time.Duration) error
}

// Restore object interface
type Restore interface {
	// CreateRestore creates restore
	CreateRestore(req *api.RestoreCreateRequest) (*api.RestoreCreateResponse, error)

	// EnumerateRestore lists restore objects
	EnumerateRestore(req *api.RestoreEnumerateRequest) (*api.RestoreEnumerateResponse, error)

	// InspectRestore inspects a restore object
	InspectRestore(req *api.RestoreInspectRequest) (*api.RestoreInspectResponse, error)

	// DeleteRestore deletes a restore object
	DeleteRestore(req *api.RestoreDeleteRequest) (*api.RestoreDeleteResponse, error)

	// WaitForRestoreCompletion waits for restore to complete successfully
	// or till timeout is reached. API should poll every `timeBeforeRetry` duration
	WaitForRestoreCompletion(restoreName string, orgID string,
		timeout time.Duration, timeBeforeRetry time.Duration) error
}

// SchedulePolicy interface
type SchedulePolicy interface {
	// CreateSchedulePolicy
	CreateSchedulePolicy(req *api.SchedulePolicyCreateRequest) (*api.SchedulePolicyCreateResponse, error)
	// EnumerateSchedulePolicy
	EnumerateSchedulePolicy(req *api.SchedulePolicyEnumerateRequest) (*api.SchedulePolicyEnumerateResponse, error)
	// InspectSchedulePolicy
	InspectSchedulePolicy(req *api.SchedulePolicyInspectRequest) (*api.SchedulePolicyInspectResponse, error)
	// DeleteSchedulePolicy
	DeleteSchedulePolicy(req *api.SchedulePolicyDeleteRequest) (*api.SchedulePolicyDeleteResponse, error)
}

// ScheduleBackup interface
type ScheduleBackup interface {
	// CreateBackupSchedule
	CreateBackupSchedule(req *api.BackupScheduleCreateRequest) (*api.BackupScheduleCreateResponse, error)
	// EnumerateBackupSchedule
	EnumerateBackupSchedule(req *api.BackupScheduleEnumerateRequest) (*api.BackupScheduleEnumerateResponse, error)
	// InspectBackupSchedule
	InspectBackupSchedule(req *api.BackupScheduleInspectRequest) (*api.BackupScheduleInspectResponse, error)
	// DeleteBackupSchedule
	DeleteBackupSchedule(req *api.BackupScheduleDeleteRequest) (*api.BackupScheduleDeleteResponse, error)
}

var backupDrivers = make(map[string]Driver)

// Register backup driver
func Register(name string, d Driver) error {
	if _, ok := backupDrivers[name]; !ok {
		backupDrivers[name] = d
	} else {
		return fmt.Errorf("backup driver: %s is already registered", name)
	}

	return nil
}

// Get backup driver name
func Get(name string) (Driver, error) {
	d, ok := backupDrivers[name]
	if ok {
		return d, nil
	}

	return nil, &errors.ErrNotFound{
		ID:   name,
		Type: "BackupDriver",
	}
}
