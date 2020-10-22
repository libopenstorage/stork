package backup

import (
	"context"
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
	// License
	License
	// Rule
	Rule

	// Init initializes the backup driver under a given scheduler
	Init(schedulerDriverName string, nodeDriverName string, volumeDriverName string, token string) error

	// WaitForBackupRunning waits for backup to start running.
	WaitForBackupRunning(ctx context.Context, req *api.BackupInspectRequest, timeout, retryInterval time.Duration) error

	// WaitForRestoreRunning waits for restore to start running.
	WaitForRestoreRunning(ctx context.Context, req *api.RestoreInspectRequest, timeout, retryInterval time.Duration) error

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

	// UpdateCloudCredential updates cloud credential objects
	UpdateCloudCredential(req *api.CloudCredentialUpdateRequest) (*api.CloudCredentialUpdateResponse, error)

	// InspectCloudCredential describes the cloud credential
	InspectCloudCredential(req *api.CloudCredentialInspectRequest) (*api.CloudCredentialInspectResponse, error)

	// EnumerateCloudCredential lists the cloud credentials for given Org
	EnumerateCloudCredential(req *api.CloudCredentialEnumerateRequest) (*api.CloudCredentialEnumerateResponse, error)

	// DeletrCloudCredential deletes a cloud credential object
	DeleteCloudCredential(req *api.CloudCredentialDeleteRequest) (*api.CloudCredentialDeleteResponse, error)
}

// Cluster obj interface
type Cluster interface {
	// CreateCluster creates a cluster object
	CreateCluster(req *api.ClusterCreateRequest) (*api.ClusterCreateResponse, error)

	// UpdateCluster updates a cluster object
	UpdateCluster(req *api.ClusterUpdateRequest) (*api.ClusterUpdateResponse, error)

	// EnumerateCluster enumerates the cluster objects
	EnumerateCluster(req *api.ClusterEnumerateRequest) (*api.ClusterEnumerateResponse, error)

	// InsepctCluster describes a cluster
	InspectCluster(req *api.ClusterInspectRequest) (*api.ClusterInspectResponse, error)

	// DeleteCluster deletes a cluster object
	DeleteCluster(req *api.ClusterDeleteRequest) (*api.ClusterDeleteResponse, error)

	// WaitForClusterDeletion waits for cluster to be deleted successfully
	// or till timeout is reached. API should poll every `timeBeforeRetry` duration
	WaitForClusterDeletion(
		ctx context.Context,
		clusterName,
		orgID string,
		timeout time.Duration,
		timeBeforeRetry time.Duration,
	) error
}

// BLocation obj interface
type BLocation interface {
	// CreateBackupLocation creates backup location object
	CreateBackupLocation(req *api.BackupLocationCreateRequest) (*api.BackupLocationCreateResponse, error)

	// UpdateBackupLocation updates backup location object
	UpdateBackupLocation(req *api.BackupLocationUpdateRequest) (*api.BackupLocationUpdateResponse, error)

	// EnumerateBackupLocation lists backup locations for an org
	EnumerateBackupLocation(req *api.BackupLocationEnumerateRequest) (*api.BackupLocationEnumerateResponse, error)

	// InspectBackupLocation enumerates backup location objects
	InspectBackupLocation(req *api.BackupLocationInspectRequest) (*api.BackupLocationInspectResponse, error)

	// DeleteBackupLocation deletes backup location objects
	DeleteBackupLocation(req *api.BackupLocationDeleteRequest) (*api.BackupLocationDeleteResponse, error)

	// ValidateBackupLocation validates the backuplocation object
	ValidateBackupLocation(req *api.BackupLocationValidateRequest) (*api.BackupLocationValidateResponse, error)

	// WaitForBackupLocationDeletion watis for backup location to be deleted
	WaitForBackupLocationDeletion(ctx context.Context, backupLocationName string, orgID string,
		timeout time.Duration, timeBeforeRetry time.Duration) error
}

// Backup obj interface
type Backup interface {
	// CreateBackup creates backup
	CreateBackup(req *api.BackupCreateRequest) (*api.BackupCreateResponse, error)

	// UpdateBackup updates backup object
	UpdateBackup(req *api.BackupUpdateRequest) (*api.BackupUpdateResponse, error)

	// EnumerateBackup enumerates backup objects
	EnumerateBackup(req *api.BackupEnumerateRequest) (*api.BackupEnumerateResponse, error)

	// InspectBackup inspects a backup object
	InspectBackup(req *api.BackupInspectRequest) (*api.BackupInspectResponse, error)

	// DeleteBackup deletes backup
	DeleteBackup(req *api.BackupDeleteRequest) (*api.BackupDeleteResponse, error)

	// WaitForBackupCompletion waits for backup to complete successfully
	// or till timeout is reached. API should poll every `timeBeforeRetry`
	WaitForBackupCompletion(ctx context.Context, backupName string, orgID string,
		timeout time.Duration, timeBeforeRetry time.Duration) error

	// WaitForBackupDeletion waits for backup to be deleted successfully
	// or till timeout is reached. API should poll every `timeBeforeRetry
	WaitForBackupDeletion(ctx context.Context, backupName string, orgID string,
		timeout time.Duration, timeBeforeRetry time.Duration) error

	// WaitForBackupDeletion waits for restore to be deleted successfully
	// or till timeout is reached. API should poll every `timeBeforeRetry
	WaitForRestoreDeletion(ctx context.Context, restoreName string, orgID string,
		timeout time.Duration, timeBeforeRetry time.Duration) error

	// WaitForDeletePending waits for the backup to transitioned to
	// delete pending state. API should poll every `timeBeforeRetry
	WaitForDeletePending(ctx context.Context, backupName string, orgID string,
		timeout time.Duration, timeBeforeRetry time.Duration) error

	// GetVolumeBackupIDs return volume backup IDs of initiated backup
	GetVolumeBackupIDs(ctx context.Context, backupName string, namespace string,
		clusterObj *api.ClusterObject, orgID string) ([]string, error)
}

// Restore object interface
type Restore interface {
	// CreateRestore creates restore object
	CreateRestore(req *api.RestoreCreateRequest) (*api.RestoreCreateResponse, error)

	// UpdateRestore updates restore object
	UpdateRestore(req *api.RestoreUpdateRequest) (*api.RestoreUpdateResponse, error)

	// EnumerateRestore lists restore objects
	EnumerateRestore(req *api.RestoreEnumerateRequest) (*api.RestoreEnumerateResponse, error)

	// InspectRestore inspects a restore object
	InspectRestore(req *api.RestoreInspectRequest) (*api.RestoreInspectResponse, error)

	// DeleteRestore deletes a restore object
	DeleteRestore(req *api.RestoreDeleteRequest) (*api.RestoreDeleteResponse, error)

	// WaitForRestoreCompletion waits for restore to complete successfully
	// or till timeout is reached. API should poll every `timeBeforeRetry` duration
	WaitForRestoreCompletion(ctx context.Context, restoreName string, orgID string,
		timeout time.Duration, timeBeforeRetry time.Duration) error
}

// SchedulePolicy interface
type SchedulePolicy interface {
	// CreateSchedulePolicy
	CreateSchedulePolicy(req *api.SchedulePolicyCreateRequest) (*api.SchedulePolicyCreateResponse, error)

	// UpdateSchedulePolicy
	UpdateSchedulePolicy(req *api.SchedulePolicyUpdateRequest) (*api.SchedulePolicyUpdateResponse, error)

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

	// UpdateBackupSchedule
	UpdateBackupSchedule(req *api.BackupScheduleUpdateRequest) (*api.BackupScheduleUpdateResponse, error)

	// EnumerateBackupSchedule
	EnumerateBackupSchedule(req *api.BackupScheduleEnumerateRequest) (*api.BackupScheduleEnumerateResponse, error)

	// InspectBackupSchedule
	InspectBackupSchedule(req *api.BackupScheduleInspectRequest) (*api.BackupScheduleInspectResponse, error)

	// DeleteBackupSchedule
	DeleteBackupSchedule(req *api.BackupScheduleDeleteRequest) (*api.BackupScheduleDeleteResponse, error)

	// BackupScheduleWaitForNBackupsCompletion, waits for backup schedule to complete successfully
	// or till timeout is reached. API should poll every `timeBeforeRetry` duration
	BackupScheduleWaitForNBackupsCompletion(ctx context.Context, name, orgID string, count int,
		timeout time.Duration, timeBeforeRetry time.Duration) error
	// WaitForBackupScheduleDeletion waits for backupschedule to be deleted successfully
	// or till timeout is reached. API should poll every `timeBeforeRetry` duration
	// This wait function is for the backupschedule deletion with delete-backup option set.
	WaitForBackupScheduleDeletion(ctx context.Context, backupScheduleName, namespace, orgID string,
		clusterObj *api.ClusterObject, timeout time.Duration, timeBeforeRetry time.Duration) error
}

// License interface
type License interface {
	// ActivateLicense
	ActivateLicense(req *api.LicenseActivateRequest) (*api.LicenseActivateResponse, error)

	// InspectLicense
	InspectLicense(req *api.LicenseInspectRequest) (*api.LicenseInspectResponse, error)

	// WaitForLicenseActivation
	WaitForLicenseActivation(ctx context.Context, req *api.LicenseInspectRequest, timeout, retryInterval time.Duration) error
}

// Rule interface
type Rule interface {
	// CreateRule creates rule object
	CreateRule(req *api.RuleCreateRequest) (*api.RuleCreateResponse, error)

	// UpdateRule updates rule object
	UpdateRule(req *api.RuleUpdateRequest) (*api.RuleUpdateResponse, error)

	// EnumerateRule enumerates rule objects
	EnumerateRule(req *api.RuleEnumerateRequest) (*api.RuleEnumerateResponse, error)

	// InspectRule inspects a rule object
	InspectRule(req *api.RuleInspectRequest) (*api.RuleInspectResponse, error)

	// DeleteRule deletes a rule
	DeleteRule(req *api.RuleDeleteRequest) (*api.RuleDeleteResponse, error)
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
