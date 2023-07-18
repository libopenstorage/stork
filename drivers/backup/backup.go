package backup

import (
	"context"
	"fmt"
	"time"

	api "github.com/portworx/px-backup-api/pkg/apis/v1"
	"github.com/portworx/torpedo/pkg/errors"
	"github.com/portworx/torpedo/pkg/log"
)

// Image Generic struct
type Image struct {
	Type    string
	Version string
}

type Weekday string

const (
	Monday    Weekday = "Mon"
	Tuesday           = "Tue"
	Wednesday         = "Wed"
	Thursday          = "Thu"
	Friday            = "Fri"
	Saturday          = "Sat"
	Sunday            = "Sun"
)

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
	// Version
	Version

	// Init initializes the backup driver under a given scheduler
	Init(schedulerDriverName string, nodeDriverName string, volumeDriverName string, token string) error

	// WaitForBackupRunning waits for backup to start running.
	WaitForBackupRunning(ctx context.Context, req *api.BackupInspectRequest, timeout, retryInterval time.Duration) error

	// WaitForRestoreRunning waits for restore to start running.
	WaitForRestoreRunning(ctx context.Context, req *api.RestoreInspectRequest, timeout, retryInterval time.Duration) error

	// String returns the name of this driver
	String() string
}

// Version object interface
type Version interface {
	// GetPxBackupVersion Gets version of Px-Backup API server
	GetPxBackupVersion(ctx context.Context, req *api.VersionGetRequest) (*api.VersionGetResponse, error)
}

// Org object interface
type Org interface {
	// CreateOrganization creates Organization
	CreateOrganization(ctx context.Context, req *api.OrganizationCreateRequest) (*api.OrganizationCreateResponse, error)

	// GetOrganization enumerates organizations
	EnumerateOrganization(ctx context.Context) (*api.OrganizationEnumerateResponse, error)
}

// CloudCredential object interface
type CloudCredential interface {
	// CreateCloudCredential creates cloud credential objects
	CreateCloudCredential(ctx context.Context, req *api.CloudCredentialCreateRequest) (*api.CloudCredentialCreateResponse, error)

	// UpdateCloudCredential updates cloud credential objects
	UpdateCloudCredential(ctx context.Context, req *api.CloudCredentialUpdateRequest) (*api.CloudCredentialUpdateResponse, error)

	// InspectCloudCredential describes the cloud credential
	InspectCloudCredential(ctx context.Context, req *api.CloudCredentialInspectRequest) (*api.CloudCredentialInspectResponse, error)

	// EnumerateCloudCredential lists the cloud credentials for given Org
	EnumerateCloudCredential(ctx context.Context, req *api.CloudCredentialEnumerateRequest) (*api.CloudCredentialEnumerateResponse, error)

	// DeleteCloudCredential deletes a cloud credential object
	DeleteCloudCredential(ctx context.Context, req *api.CloudCredentialDeleteRequest) (*api.CloudCredentialDeleteResponse, error)

	// UpdateOwnershipCloudCredential update ownership of cloud credential object
	UpdateOwnershipCloudCredential(ctx context.Context, req *api.CloudCredentialOwnershipUpdateRequest) (*api.CloudCredentialOwnershipUpdateResponse, error)
}

// Cluster obj interface
type Cluster interface {
	// CreateCluster creates a cluster object
	CreateCluster(ctx context.Context, req *api.ClusterCreateRequest) (*api.ClusterCreateResponse, error)

	// UpdateCluster updates a cluster object
	UpdateCluster(ctx context.Context, req *api.ClusterUpdateRequest) (*api.ClusterUpdateResponse, error)

	// EnumerateCluster enumerates the cluster objects
	EnumerateCluster(ctx context.Context, req *api.ClusterEnumerateRequest) (*api.ClusterEnumerateResponse, error)

	// InspectCluster describes a cluster
	InspectCluster(ctx context.Context, req *api.ClusterInspectRequest) (*api.ClusterInspectResponse, error)

	// DeleteCluster deletes a cluster object
	DeleteCluster(ctx context.Context, req *api.ClusterDeleteRequest) (*api.ClusterDeleteResponse, error)

	// ClusterUpdateBackupShare updates ownership details for backup share at cluster
	ClusterUpdateBackupShare(ctx context.Context, req *api.ClusterBackupShareUpdateRequest) (*api.ClusterBackupShareUpdateResponse, error)

	// WaitForClusterDeletion waits for cluster to be deleted successfully
	// or till timeout is reached. API should poll every `timeBeforeRetry` duration
	WaitForClusterDeletion(
		ctx context.Context,
		clusterName,
		orgID string,
		timeout time.Duration,
		timeBeforeRetry time.Duration,
	) error

	// GetClusterUID returns uid of the given cluster name in an organization
	GetClusterUID(ctx context.Context, orgID string, clusterName string) (string, error)

	// GetClusterName returns name of the given cluster uid in an organization
	GetClusterName(ctx context.Context, orgID string, clusterUid string) (string, error)

	// GetClusterStatus returns status of the given cluster name in an organization
	GetClusterStatus(orgID string, clusterName string, ctx context.Context) (api.ClusterInfo_StatusInfo_Status, error)
}

// BLocation obj interface
type BLocation interface {
	// CreateBackupLocation creates backup location object
	CreateBackupLocation(ctx context.Context, req *api.BackupLocationCreateRequest) (*api.BackupLocationCreateResponse, error)

	// UpdateBackupLocation updates backup location object
	UpdateBackupLocation(ctx context.Context, req *api.BackupLocationUpdateRequest) (*api.BackupLocationUpdateResponse, error)

	// EnumerateBackupLocation lists backup locations for an org
	EnumerateBackupLocation(ctx context.Context, req *api.BackupLocationEnumerateRequest) (*api.BackupLocationEnumerateResponse, error)

	// InspectBackupLocation enumerates backup location objects
	InspectBackupLocation(ctx context.Context, req *api.BackupLocationInspectRequest) (*api.BackupLocationInspectResponse, error)

	// DeleteBackupLocation deletes backup location objects
	DeleteBackupLocation(ctx context.Context, req *api.BackupLocationDeleteRequest) (*api.BackupLocationDeleteResponse, error)

	// ValidateBackupLocation validates the backuplocation object
	ValidateBackupLocation(ctx context.Context, req *api.BackupLocationValidateRequest) (*api.BackupLocationValidateResponse, error)

	// UpdateOwnershipBackupLocation updates backuplocation ownership
	UpdateOwnershipBackupLocation(ctx context.Context, req *api.BackupLocationOwnershipUpdateRequest) (*api.BackupLocationOwnershipUpdateResponse, error)

	// WaitForBackupLocationDeletion watis for backup location to be deleted
	WaitForBackupLocationDeletion(ctx context.Context, backupLocationName, backupLocationUID string, orgID string,
		timeout time.Duration, timeBeforeRetry time.Duration) error
}

// Backup obj interface
type Backup interface {
	// CreateBackup creates backup
	CreateBackup(ctx context.Context, req *api.BackupCreateRequest) (*api.BackupCreateResponse, error)

	// UpdateBackup updates backup object
	UpdateBackup(ctx context.Context, req *api.BackupUpdateRequest) (*api.BackupUpdateResponse, error)

	// EnumerateBackup enumerates backup objects
	EnumerateBackup(ctx context.Context, req *api.BackupEnumerateRequest) (*api.BackupEnumerateResponse, error)

	// InspectBackup inspects a backup object
	InspectBackup(ctx context.Context, req *api.BackupInspectRequest) (*api.BackupInspectResponse, error)

	// DeleteBackup deletes backup
	DeleteBackup(ctx context.Context, req *api.BackupDeleteRequest) (*api.BackupDeleteResponse, error)

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

	// GetBackupUID returns uid of the given backup name in an organization
	GetBackupUID(ctx context.Context, backupName string, orgID string) (string, error)

	// GetBackupName returns name of the given backup uid in an organization
	GetBackupName(ctx context.Context, backupUid string, orgID string) (string, error)

	// UpdateBackupShare updates backupshare of existing backup object
	UpdateBackupShare(ctx context.Context, req *api.BackupShareUpdateRequest) (*api.BackupShareUpdateResponse, error)
}

// Restore object interface
type Restore interface {
	// CreateRestore creates restore object
	CreateRestore(ctx context.Context, req *api.RestoreCreateRequest) (*api.RestoreCreateResponse, error)

	// UpdateRestore updates restore object
	UpdateRestore(ctx context.Context, req *api.RestoreUpdateRequest) (*api.RestoreUpdateResponse, error)

	// EnumerateRestore lists restore objects
	EnumerateRestore(ctx context.Context, req *api.RestoreEnumerateRequest) (*api.RestoreEnumerateResponse, error)

	// InspectRestore inspects a restore object
	InspectRestore(ctx context.Context, req *api.RestoreInspectRequest) (*api.RestoreInspectResponse, error)

	// DeleteRestore deletes a restore object
	DeleteRestore(ctx context.Context, req *api.RestoreDeleteRequest) (*api.RestoreDeleteResponse, error)

	// WaitForRestoreCompletion waits for restore to complete successfully
	// or till timeout is reached. API should poll every `timeBeforeRetry` duration
	WaitForRestoreCompletion(ctx context.Context, restoreName string, orgID string,
		timeout time.Duration, timeBeforeRetry time.Duration) error
}

// SchedulePolicy interface
type SchedulePolicy interface {
	// CreateSchedulePolicy
	CreateSchedulePolicy(ctx context.Context, req *api.SchedulePolicyCreateRequest) (*api.SchedulePolicyCreateResponse, error)

	// UpdateSchedulePolicy
	UpdateSchedulePolicy(ctx context.Context, req *api.SchedulePolicyUpdateRequest) (*api.SchedulePolicyUpdateResponse, error)

	// EnumerateSchedulePolicy
	EnumerateSchedulePolicy(ctx context.Context, req *api.SchedulePolicyEnumerateRequest) (*api.SchedulePolicyEnumerateResponse, error)

	// InspectSchedulePolicy
	InspectSchedulePolicy(ctx context.Context, req *api.SchedulePolicyInspectRequest) (*api.SchedulePolicyInspectResponse, error)

	// DeleteSchedulePolicy
	DeleteSchedulePolicy(ctx context.Context, req *api.SchedulePolicyDeleteRequest) (*api.SchedulePolicyDeleteResponse, error)

	// UpdateOwnershipSchedulePolicy updating ownership of schedule policy
	UpdateOwnershipSchedulePolicy(ctx context.Context, req *api.SchedulePolicyOwnershipUpdateRequest) (*api.SchedulePolicyOwnershipUpdateResponse, error)

	// CreateIntervalSchedulePolicy creates interval schedule policy object
	CreateIntervalSchedulePolicy(retain int64, min int64, incrCount uint64) *api.SchedulePolicyInfo

	// CreateDailySchedulePolicy creates daily schedule policy object
	CreateDailySchedulePolicy(retain int64, time string, incrCount uint64) *api.SchedulePolicyInfo

	// CreateWeeklySchedulePolicy creates weekly schedule policy object
	CreateWeeklySchedulePolicy(retain int64, day Weekday, time string, incrCount uint64) *api.SchedulePolicyInfo

	// CreateMonthlySchedulePolicy creates monthly schedule policy object
	CreateMonthlySchedulePolicy(retain int64, date int64, time string, incrCount uint64) *api.SchedulePolicyInfo

	// BackupSchedulePolicy creates backup schedule policy
	BackupSchedulePolicy(name string, uid string, orgId string, schedulePolicyInfo *api.SchedulePolicyInfo) error

	// DeleteBackupSchedulePolicy deletes the list of schedule policies
	DeleteBackupSchedulePolicy(orgID string, policyList []string) error

	// GetSchedulePolicyUid gets the uid for the given schedule policy
	GetSchedulePolicyUid(orgID string, ctx context.Context, schedulePolicyName string) (string, error)
}

// ScheduleBackup interface
type ScheduleBackup interface {
	// CreateBackupSchedule
	CreateBackupSchedule(ctx context.Context, req *api.BackupScheduleCreateRequest) (*api.BackupScheduleCreateResponse, error)

	// UpdateBackupSchedule
	UpdateBackupSchedule(ctx context.Context, req *api.BackupScheduleUpdateRequest) (*api.BackupScheduleUpdateResponse, error)

	// EnumerateBackupSchedule
	EnumerateBackupSchedule(ctx context.Context, req *api.BackupScheduleEnumerateRequest) (*api.BackupScheduleEnumerateResponse, error)

	// InspectBackupSchedule
	InspectBackupSchedule(ctx context.Context, req *api.BackupScheduleInspectRequest) (*api.BackupScheduleInspectResponse, error)

	// DeleteBackupSchedule
	DeleteBackupSchedule(ctx context.Context, req *api.BackupScheduleDeleteRequest) (*api.BackupScheduleDeleteResponse, error)

	// BackupScheduleWaitForNBackupsCompletion, waits for backup schedule to complete successfully
	// or till timeout is reached. API should poll every `timeBeforeRetry` duration
	BackupScheduleWaitForNBackupsCompletion(ctx context.Context, name, orgID string, count int,
		timeout time.Duration, timeBeforeRetry time.Duration) error
	// WaitForBackupScheduleDeletion waits for backupschedule to be deleted successfully
	// or till timeout is reached. API should poll every `timeBeforeRetry` duration
	// This wait function is for the backupschedule deletion with delete-backup option set.
	WaitForBackupScheduleDeletion(ctx context.Context, backupScheduleName, namespace, orgID string,
		clusterObj *api.ClusterObject, timeout time.Duration, timeBeforeRetry time.Duration) error

	// GetAllScheduleBackupNames returns names of all scheduled backups for the given schedule
	GetAllScheduleBackupNames(ctx context.Context, scheduleName string, orgID string) ([]string, error)

	// GetAllScheduleBackupUIDs returns uids of all scheduled backups for the given schedule
	GetAllScheduleBackupUIDs(ctx context.Context, scheduleName string, orgID string) ([]string, error)
}

// License interface
type License interface {
	// ActivateLicense
	ActivateLicense(ctx context.Context, req *api.LicenseActivateRequest) (*api.LicenseActivateResponse, error)

	// InspectLicense
	InspectLicense(ctx context.Context, req *api.LicenseInspectRequest) (*api.LicenseInspectResponse, error)

	// WaitForLicenseActivation
	WaitForLicenseActivation(ctx context.Context, req *api.LicenseInspectRequest, timeout, retryInterval time.Duration) error
}

// Rule interface
type Rule interface {
	// CreateRule creates rule object
	CreateRule(ctx context.Context, req *api.RuleCreateRequest) (*api.RuleCreateResponse, error)

	// UpdateRule updates rule object
	UpdateRule(ctx context.Context, req *api.RuleUpdateRequest) (*api.RuleUpdateResponse, error)

	// EnumerateRule enumerates rule objects
	EnumerateRule(ctx context.Context, req *api.RuleEnumerateRequest) (*api.RuleEnumerateResponse, error)

	// InspectRule inspects a rule object
	InspectRule(ctx context.Context, req *api.RuleInspectRequest) (*api.RuleInspectResponse, error)

	// DeleteRule deletes a rule
	DeleteRule(ctx context.Context, req *api.RuleDeleteRequest) (*api.RuleDeleteResponse, error)

	// UpdateOwnershipRule update ownership of rule
	UpdateOwnershipRule(ctx context.Context, req *api.RuleOwnershipUpdateRequest) (*api.RuleOwnershipUpdateResponse, error)

	// CreateRuleForBackup creates backup rule
	CreateRuleForBackup(appName string, orgID string, prePostFlag string) (bool, string, error)

	// DeleteRuleForBackup deletes backup rule
	DeleteRuleForBackup(orgID string, ruleName string) error

	// GetRuleUid fetches uid for the given rule
	GetRuleUid(orgID string, ctx context.Context, ruleName string) (string, error)
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

func init() {
	str, err := GetPxCentralAdminPwd()
	if err != nil {
		log.Errorf("Error fetching password from secret: %v", err)
	}
	PxCentralAdminPwd = str
}
