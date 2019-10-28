package k8s

import (
	"fmt"
	"time"

	snap_v1 "github.com/kubernetes-incubator/external-storage/snapshot/pkg/apis/crd/v1"
	"github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/portworx/sched-ops/task"
	v1 "k8s.io/api/core/v1"
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// SnapshotScheduleOps is an interface to perform k8s VolumeSnapshotSchedule operations
type SnapshotScheduleOps interface {
	// GetSnapshotSchedule gets the SnapshotSchedule
	GetSnapshotSchedule(string, string) (*v1alpha1.VolumeSnapshotSchedule, error)
	// CreateSnapshotSchedule creates a SnapshotSchedule
	CreateSnapshotSchedule(*v1alpha1.VolumeSnapshotSchedule) (*v1alpha1.VolumeSnapshotSchedule, error)
	// UpdateSnapshotSchedule updates the SnapshotSchedule
	UpdateSnapshotSchedule(*v1alpha1.VolumeSnapshotSchedule) (*v1alpha1.VolumeSnapshotSchedule, error)
	// ListSnapshotSchedules lists all the SnapshotSchedules
	ListSnapshotSchedules(string) (*v1alpha1.VolumeSnapshotScheduleList, error)
	// DeleteSnapshotSchedule deletes the SnapshotSchedule
	DeleteSnapshotSchedule(string, string) error
	// ValidateSnapshotSchedule validates the given SnapshotSchedule. It checks the status of each of
	// the snapshots triggered for this schedule and returns a map of successfull snapshots. The key of the
	// map will be the schedule type and value will be list of snapshots for that schedule type.
	// The caller is expected to validate if the returned map has all snapshots expected at that point of time
	ValidateSnapshotSchedule(string, string, time.Duration, time.Duration) (
		map[v1alpha1.SchedulePolicyType][]*v1alpha1.ScheduledVolumeSnapshotStatus, error)
}

// GroupSnapshotOps is an interface to perform k8s GroupVolumeSnapshot operations
type GroupSnapshotOps interface {
	// GetGroupSnapshot returns the group snapshot for the given name and namespace
	GetGroupSnapshot(name, namespace string) (*v1alpha1.GroupVolumeSnapshot, error)
	// ListGroupSnapshots lists all group snapshots for the given namespace
	ListGroupSnapshots(namespace string) (*v1alpha1.GroupVolumeSnapshotList, error)
	// CreateGroupSnapshot creates the given group snapshot
	CreateGroupSnapshot(*v1alpha1.GroupVolumeSnapshot) (*v1alpha1.GroupVolumeSnapshot, error)
	// UpdateGroupSnapshot updates the given group snapshot
	UpdateGroupSnapshot(*v1alpha1.GroupVolumeSnapshot) (*v1alpha1.GroupVolumeSnapshot, error)
	// DeleteGroupSnapshot deletes the group snapshot with the given name and namespace
	DeleteGroupSnapshot(name, namespace string) error
	// ValidateGroupSnapshot checks if the group snapshot with given name and namespace is in ready state
	//  If retry is true, the validation will be retried with given timeout and retry internal
	ValidateGroupSnapshot(name, namespace string, retry bool, timeout, retryInterval time.Duration) error
	// GetSnapshotsForGroupSnapshot returns all child snapshots for the group snapshot
	GetSnapshotsForGroupSnapshot(name, namespace string) ([]*snap_v1.VolumeSnapshot, error)
}

// VolumeSnapshotRestoreOps is interface to perform isnapshot restore using CRD
type VolumeSnapshotRestoreOps interface {
	// CreateVolumeSnapshotRestore restore snapshot to pvc specifed in CRD, if no pvcs defined we restore to
	// parent volumes
	CreateVolumeSnapshotRestore(snap *v1alpha1.VolumeSnapshotRestore) (*v1alpha1.VolumeSnapshotRestore, error)
	// UpdateVolumeSnapshotRestore updates given volumesnapshorestore CRD
	UpdateVolumeSnapshotRestore(snap *v1alpha1.VolumeSnapshotRestore) (*v1alpha1.VolumeSnapshotRestore, error)
	// GetVolumeSnapshotRestore returns details of given restore crd status
	GetVolumeSnapshotRestore(name, namespace string) (*v1alpha1.VolumeSnapshotRestore, error)
	// ListVolumeSnapshotRestore return list of volumesnapshotrestores in given namespaces
	ListVolumeSnapshotRestore(namespace string) (*v1alpha1.VolumeSnapshotRestoreList, error)
	// DeleteVolumeSnapshotRestore delete given volumesnapshotrestore CRD
	DeleteVolumeSnapshotRestore(name, namespace string) error
	// ValidateVolumeSnapshotRestore validates given volumesnapshotrestore CRD
	ValidateVolumeSnapshotRestore(name, namespace string, timeout, retry time.Duration) error
}

// RuleOps is an interface to perform operations for k8s stork rule
type RuleOps interface {
	// GetRule fetches the given stork rule
	GetRule(name, namespace string) (*v1alpha1.Rule, error)
	// CreateRule creates the given stork rule
	CreateRule(rule *v1alpha1.Rule) (*v1alpha1.Rule, error)
	// DeleteRule deletes the given stork rule
	DeleteRule(name, namespace string) error
}

// ClusterPairOps is an interface to perfrom k8s ClusterPair operations
type ClusterPairOps interface {
	// CreateClusterPair creates the ClusterPair
	CreateClusterPair(*v1alpha1.ClusterPair) (*v1alpha1.ClusterPair, error)
	// GetClusterPair gets the ClusterPair
	GetClusterPair(string, string) (*v1alpha1.ClusterPair, error)
	// ListClusterPairs gets all the ClusterPairs
	ListClusterPairs(string) (*v1alpha1.ClusterPairList, error)
	// UpdateClusterPair updates the ClusterPair
	UpdateClusterPair(*v1alpha1.ClusterPair) (*v1alpha1.ClusterPair, error)
	// DeleteClusterPair deletes the ClusterPair
	DeleteClusterPair(string, string) error
	// ValidateClusterPair validates clusterpair status
	ValidateClusterPair(string, string, time.Duration, time.Duration) error
}

// ClusterDomainsOps is an interface to perform k8s ClusterDomains operations
type ClusterDomainsOps interface {
	// CreateClusterDomainsStatus creates the ClusterDomainStatus
	CreateClusterDomainsStatus(*v1alpha1.ClusterDomainsStatus) (*v1alpha1.ClusterDomainsStatus, error)
	// GetClusterDomainsStatus gets the ClusterDomainsStatus
	GetClusterDomainsStatus(string) (*v1alpha1.ClusterDomainsStatus, error)
	// UpdateClusterDomainsStatus updates the ClusterDomainsStatus
	UpdateClusterDomainsStatus(*v1alpha1.ClusterDomainsStatus) (*v1alpha1.ClusterDomainsStatus, error)
	// DeleteClusterDomainsStatus deletes the ClusterDomainsStatus
	DeleteClusterDomainsStatus(string) error
	// ListClusterDomainStatuses lists ClusterDomainsStatus
	ListClusterDomainStatuses() (*v1alpha1.ClusterDomainsStatusList, error)
	// ValidateClusterDomainsStatus validates the ClusterDomainsStatus
	ValidateClusterDomainsStatus(string, map[string]bool, time.Duration, time.Duration) error

	// CreateClusterDomainUpdate creates the ClusterDomainUpdate
	CreateClusterDomainUpdate(*v1alpha1.ClusterDomainUpdate) (*v1alpha1.ClusterDomainUpdate, error)
	// GetClusterDomainUpdate gets the ClusterDomainUpdate
	GetClusterDomainUpdate(string) (*v1alpha1.ClusterDomainUpdate, error)
	// UpdateClusterDomainUpdate updates the ClusterDomainUpdate
	UpdateClusterDomainUpdate(*v1alpha1.ClusterDomainUpdate) (*v1alpha1.ClusterDomainUpdate, error)
	// DeleteClusterDomainUpdate deletes the ClusterDomainUpdate
	DeleteClusterDomainUpdate(string) error
	// ValidateClusterDomainUpdate validates ClusterDomainUpdate
	ValidateClusterDomainUpdate(string, time.Duration, time.Duration) error
	// ListClusterDomainUpdates lists ClusterDomainUpdates
	ListClusterDomainUpdates() (*v1alpha1.ClusterDomainUpdateList, error)
}

// MigrationOps is an interface to perfrom k8s Migration operations
type MigrationOps interface {
	// CreateMigration creates the Migration
	CreateMigration(*v1alpha1.Migration) (*v1alpha1.Migration, error)
	// GetMigration gets the Migration
	GetMigration(string, string) (*v1alpha1.Migration, error)
	// ListMigrations lists all the Migrations
	ListMigrations(string) (*v1alpha1.MigrationList, error)
	// UpdateMigration updates the Migration
	UpdateMigration(*v1alpha1.Migration) (*v1alpha1.Migration, error)
	// DeleteMigration deletes the Migration
	DeleteMigration(string, string) error
	// ValidateMigration validate the Migration status
	ValidateMigration(string, string, time.Duration, time.Duration) error
	// GetMigrationSchedule gets the MigrationSchedule
	GetMigrationSchedule(string, string) (*v1alpha1.MigrationSchedule, error)
	// CreateMigrationSchedule creates a MigrationSchedule
	CreateMigrationSchedule(*v1alpha1.MigrationSchedule) (*v1alpha1.MigrationSchedule, error)
	// UpdateMigrationSchedule updates the MigrationSchedule
	UpdateMigrationSchedule(*v1alpha1.MigrationSchedule) (*v1alpha1.MigrationSchedule, error)
	// ListMigrationSchedules lists all the MigrationSchedules
	ListMigrationSchedules(string) (*v1alpha1.MigrationScheduleList, error)
	// DeleteMigrationSchedule deletes the MigrationSchedule
	DeleteMigrationSchedule(string, string) error
	// ValidateMigrationSchedule validates the given MigrationSchedule. It checks the status of each of
	// the migrations triggered for this schedule and returns a map of successfull migrations. The key of the
	// map will be the schedule type and value will be list of migrations for that schedule type.
	// The caller is expected to validate if the returned map has all migrations expected at that point of time
	ValidateMigrationSchedule(string, string, time.Duration, time.Duration) (
		map[v1alpha1.SchedulePolicyType][]*v1alpha1.ScheduledMigrationStatus, error)
}

// SchedulePolicyOps is an interface to manage SchedulePolicy Object
type SchedulePolicyOps interface {
	// CreateSchedulePolicy creates a SchedulePolicy
	CreateSchedulePolicy(*v1alpha1.SchedulePolicy) (*v1alpha1.SchedulePolicy, error)
	// GetSchedulePolicy gets the SchedulePolicy
	GetSchedulePolicy(string) (*v1alpha1.SchedulePolicy, error)
	// ListSchedulePolicies lists all the SchedulePolicies
	ListSchedulePolicies() (*v1alpha1.SchedulePolicyList, error)
	// UpdateSchedulePolicy updates the SchedulePolicy
	UpdateSchedulePolicy(*v1alpha1.SchedulePolicy) (*v1alpha1.SchedulePolicy, error)
	// DeleteSchedulePolicy deletes the SchedulePolicy
	DeleteSchedulePolicy(string) error
}

// BackupLocationOps is an interface to perfrom k8s BackupLocation operations
type BackupLocationOps interface {
	// CreateBackupLocation creates the BackupLocation
	CreateBackupLocation(*v1alpha1.BackupLocation) (*v1alpha1.BackupLocation, error)
	// GetBackupLocation gets the BackupLocation
	GetBackupLocation(string, string) (*v1alpha1.BackupLocation, error)
	// ListBackupLocations lists all the BackupLocations
	ListBackupLocations(string) (*v1alpha1.BackupLocationList, error)
	// UpdateBackupLocation updates the BackupLocation
	UpdateBackupLocation(*v1alpha1.BackupLocation) (*v1alpha1.BackupLocation, error)
	// DeleteBackupLocation deletes the BackupLocation
	DeleteBackupLocation(string, string) error
	// ValidateBackupLocation validates the BackupLocation
	ValidateBackupLocation(string, string, time.Duration, time.Duration) error
}

// ApplicationBackupRestoreOps is an interface to perfrom k8s Application Backup
// and Restore operations
type ApplicationBackupRestoreOps interface {
	// CreateApplicationBackup creates the ApplicationBackup
	CreateApplicationBackup(*v1alpha1.ApplicationBackup) (*v1alpha1.ApplicationBackup, error)
	// GetApplicationBackup gets the ApplicationBackup
	GetApplicationBackup(string, string) (*v1alpha1.ApplicationBackup, error)
	// ListApplicationBackups lists all the ApplicationBackups
	ListApplicationBackups(string) (*v1alpha1.ApplicationBackupList, error)
	// UpdateApplicationBackup updates the ApplicationBackup
	UpdateApplicationBackup(*v1alpha1.ApplicationBackup) (*v1alpha1.ApplicationBackup, error)
	// DeleteApplicationBackup deletes the ApplicationBackup
	DeleteApplicationBackup(string, string) error
	// ValidateApplicationBackup validates the ApplicationBackup
	ValidateApplicationBackup(string, string, time.Duration, time.Duration) error
	// CreateApplicationRestore creates the ApplicationRestore
	CreateApplicationRestore(*v1alpha1.ApplicationRestore) (*v1alpha1.ApplicationRestore, error)
	// GetApplicationRestore gets the ApplicationRestore
	GetApplicationRestore(string, string) (*v1alpha1.ApplicationRestore, error)
	// ListApplicationRestores lists all the ApplicationRestores
	ListApplicationRestores(string) (*v1alpha1.ApplicationRestoreList, error)
	// UpdateApplicationRestore updates the ApplicationRestore
	UpdateApplicationRestore(*v1alpha1.ApplicationRestore) (*v1alpha1.ApplicationRestore, error)
	// DeleteApplicationRestore deletes the ApplicationRestore
	DeleteApplicationRestore(string, string) error
	// ValidateApplicationRestore validates the ApplicationRestore
	ValidateApplicationRestore(string, string, time.Duration, time.Duration) error
	// GetApplicationBackupSchedule gets the ApplicationBackupSchedule
	GetApplicationBackupSchedule(string, string) (*v1alpha1.ApplicationBackupSchedule, error)
	// CreateApplicationBackupSchedule creates an ApplicationBackupSchedule
	CreateApplicationBackupSchedule(*v1alpha1.ApplicationBackupSchedule) (*v1alpha1.ApplicationBackupSchedule, error)
	// UpdateApplicationBackupSchedule updates the ApplicationBackupSchedule
	UpdateApplicationBackupSchedule(*v1alpha1.ApplicationBackupSchedule) (*v1alpha1.ApplicationBackupSchedule, error)
	// ListApplicationBackupSchedules lists all the ApplicationBackupSchedules
	ListApplicationBackupSchedules(string) (*v1alpha1.ApplicationBackupScheduleList, error)
	// DeleteApplicationBackupSchedule deletes the ApplicationBackupSchedule
	DeleteApplicationBackupSchedule(string, string) error
	// ValidateApplicationBackupSchedule validates the given ApplicationBackupSchedule. It checks the status of each of
	// the backups triggered for this schedule and returns a map of successfull backups. The key of the
	// map will be the schedule type and value will be list of backups for that schedule type.
	// The caller is expected to validate if the returned map has all backups expected at that point of time
	ValidateApplicationBackupSchedule(string, string, time.Duration, time.Duration) (
		map[v1alpha1.SchedulePolicyType][]*v1alpha1.ScheduledApplicationBackupStatus, error)
}

// ApplicationCloneOps is an interface to perform k8s Application Clone operations
type ApplicationCloneOps interface {
	// CreateApplicationClone creates the ApplicationClone
	CreateApplicationClone(*v1alpha1.ApplicationClone) (*v1alpha1.ApplicationClone, error)
	// GetApplicationClone gets the ApplicationClone
	GetApplicationClone(string, string) (*v1alpha1.ApplicationClone, error)
	// ListApplicationClones lists all the ApplicationClones
	ListApplicationClones(string) (*v1alpha1.ApplicationCloneList, error)
	// UpdateApplicationClone updates the ApplicationClone
	UpdateApplicationClone(*v1alpha1.ApplicationClone) (*v1alpha1.ApplicationClone, error)
	// DeleteApplicationClone deletes the ApplicationClone
	DeleteApplicationClone(string, string) error
	// ValidateApplicationClone validates the ApplicationClone
	ValidateApplicationClone(string, string, time.Duration, time.Duration) error
}

// VolumeSnapshotSchedule APIs - BEGIN

func (k *k8sOps) GetSnapshotSchedule(name string, namespace string) (*v1alpha1.VolumeSnapshotSchedule, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	return k.storkClient.Stork().VolumeSnapshotSchedules(namespace).Get(name, meta_v1.GetOptions{})
}

func (k *k8sOps) ListSnapshotSchedules(namespace string) (*v1alpha1.VolumeSnapshotScheduleList, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	return k.storkClient.Stork().VolumeSnapshotSchedules(namespace).List(meta_v1.ListOptions{})
}

func (k *k8sOps) CreateSnapshotSchedule(snapshotSchedule *v1alpha1.VolumeSnapshotSchedule) (*v1alpha1.VolumeSnapshotSchedule, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	return k.storkClient.Stork().VolumeSnapshotSchedules(snapshotSchedule.Namespace).Create(snapshotSchedule)
}

func (k *k8sOps) UpdateSnapshotSchedule(snapshotSchedule *v1alpha1.VolumeSnapshotSchedule) (*v1alpha1.VolumeSnapshotSchedule, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	return k.storkClient.Stork().VolumeSnapshotSchedules(snapshotSchedule.Namespace).Update(snapshotSchedule)
}
func (k *k8sOps) DeleteSnapshotSchedule(name string, namespace string) error {
	if err := k.initK8sClient(); err != nil {
		return err
	}

	return k.storkClient.Stork().VolumeSnapshotSchedules(namespace).Delete(name, &meta_v1.DeleteOptions{
		PropagationPolicy: &deleteForegroundPolicy,
	})
}

func (k *k8sOps) ValidateSnapshotSchedule(name string, namespace string, timeout, retryInterval time.Duration) (
	map[v1alpha1.SchedulePolicyType][]*v1alpha1.ScheduledVolumeSnapshotStatus, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}
	t := func() (interface{}, bool, error) {
		resp, err := k.GetSnapshotSchedule(name, namespace)
		if err != nil {
			return nil, true, err
		}

		if len(resp.Status.Items) == 0 {
			return nil, true, &ErrFailedToValidateCustomSpec{
				Name:  name,
				Cause: fmt.Sprintf("0 snapshots have yet run for the snapshot schedule"),
				Type:  resp,
			}
		}

		failedSnapshots := make([]string, 0)
		pendingSnapshots := make([]string, 0)
		for _, snapshotStatuses := range resp.Status.Items {
			if len(snapshotStatuses) > 0 {
				status := snapshotStatuses[len(snapshotStatuses)-1]
				if status == nil {
					return nil, true, &ErrFailedToValidateCustomSpec{
						Name:  name,
						Cause: "SnapshotSchedule has an empty migration in it's most recent status",
						Type:  resp,
					}
				}

				if status.Status == snap_v1.VolumeSnapshotConditionReady {
					continue
				}

				if status.Status == snap_v1.VolumeSnapshotConditionError {
					failedSnapshots = append(failedSnapshots,
						fmt.Sprintf("snapshot: %s failed. status: %v", status.Name, status.Status))
				} else {
					pendingSnapshots = append(pendingSnapshots,
						fmt.Sprintf("snapshot: %s is not done. status: %v", status.Name, status.Status))
				}
			}
		}

		if len(failedSnapshots) > 0 {
			return nil, false, &ErrFailedToValidateCustomSpec{
				Name: name,
				Cause: fmt.Sprintf("SnapshotSchedule failed as one or more snapshots have failed. %s",
					failedSnapshots),
				Type: resp,
			}
		}

		if len(pendingSnapshots) > 0 {
			return nil, true, &ErrFailedToValidateCustomSpec{
				Name: name,
				Cause: fmt.Sprintf("SnapshotSchedule has certain snapshots pending: %s",
					pendingSnapshots),
				Type: resp,
			}
		}

		return resp.Status.Items, false, nil
	}

	ret, err := task.DoRetryWithTimeout(t, timeout, retryInterval)
	if err != nil {
		return nil, err
	}

	snapshots, ok := ret.(map[v1alpha1.SchedulePolicyType][]*v1alpha1.ScheduledVolumeSnapshotStatus)
	if !ok {
		return nil, fmt.Errorf("invalid type when checking snapshot schedules: %v", snapshots)
	}

	return snapshots, nil
}

// VolumeSnapshotSchedule APIs - END

// GroupSnapshot APIs - BEGIN

func (k *k8sOps) GetGroupSnapshot(name, namespace string) (*v1alpha1.GroupVolumeSnapshot, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	return k.storkClient.Stork().GroupVolumeSnapshots(namespace).Get(name, meta_v1.GetOptions{})
}

func (k *k8sOps) ListGroupSnapshots(namespace string) (*v1alpha1.GroupVolumeSnapshotList, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	return k.storkClient.Stork().GroupVolumeSnapshots(namespace).List(meta_v1.ListOptions{})
}

func (k *k8sOps) CreateGroupSnapshot(snap *v1alpha1.GroupVolumeSnapshot) (*v1alpha1.GroupVolumeSnapshot, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	return k.storkClient.Stork().GroupVolumeSnapshots(snap.Namespace).Create(snap)
}

func (k *k8sOps) UpdateGroupSnapshot(snap *v1alpha1.GroupVolumeSnapshot) (*v1alpha1.GroupVolumeSnapshot, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	return k.storkClient.Stork().GroupVolumeSnapshots(snap.Namespace).Update(snap)
}

func (k *k8sOps) DeleteGroupSnapshot(name, namespace string) error {
	if err := k.initK8sClient(); err != nil {
		return err
	}

	return k.storkClient.Stork().GroupVolumeSnapshots(namespace).Delete(name, &meta_v1.DeleteOptions{
		PropagationPolicy: &deleteForegroundPolicy,
	})
}

func (k *k8sOps) ValidateGroupSnapshot(name, namespace string, retry bool, timeout, retryInterval time.Duration) error {
	t := func() (interface{}, bool, error) {
		snap, err := k.GetGroupSnapshot(name, namespace)
		if err != nil {
			return "", true, err
		}

		if len(snap.Status.VolumeSnapshots) == 0 {
			return "", true, &ErrSnapshotNotReady{
				ID:    name,
				Cause: fmt.Sprintf("group snapshot has 0 child snapshots yet"),
			}
		}

		if snap.Status.Stage == v1alpha1.GroupSnapshotStageFinal {
			if snap.Status.Status == v1alpha1.GroupSnapshotSuccessful {
				// Perform extra check that all child snapshots are also ready
				notDoneChildSnaps := make([]string, 0)
				for _, childSnap := range snap.Status.VolumeSnapshots {
					conditions := childSnap.Conditions
					if len(conditions) == 0 {
						notDoneChildSnaps = append(notDoneChildSnaps, childSnap.VolumeSnapshotName)
						continue
					}

					lastCondition := conditions[0]
					if lastCondition.Status != v1.ConditionTrue || lastCondition.Type != snap_v1.VolumeSnapshotConditionReady {
						notDoneChildSnaps = append(notDoneChildSnaps, childSnap.VolumeSnapshotName)
						continue
					}
				}

				if len(notDoneChildSnaps) > 0 {
					return "", false, &ErrSnapshotFailed{
						ID: name,
						Cause: fmt.Sprintf("group snapshot is marked as successfull "+
							" but following child volumesnapshots are in pending or error state: %s", notDoneChildSnaps),
					}
				}

				return "", false, nil
			}

			if snap.Status.Status == v1alpha1.GroupSnapshotFailed {
				return "", false, &ErrSnapshotFailed{
					ID:    name,
					Cause: fmt.Sprintf("group snapshot is in failed state"),
				}
			}
		}

		return "", true, &ErrSnapshotNotReady{
			ID:    name,
			Cause: fmt.Sprintf("stage: %s status: %s", snap.Status.Stage, snap.Status.Status),
		}
	}

	if retry {
		if _, err := task.DoRetryWithTimeout(t, timeout, retryInterval); err != nil {
			return err
		}
	} else {
		if _, _, err := t(); err != nil {
			return err
		}
	}

	return nil
}

func (k *k8sOps) GetSnapshotsForGroupSnapshot(name, namespace string) ([]*snap_v1.VolumeSnapshot, error) {
	snap, err := k.GetGroupSnapshot(name, namespace)
	if err != nil {
		return nil, err
	}

	if len(snap.Status.VolumeSnapshots) == 0 {
		return nil, fmt.Errorf("group snapshot: [%s] %s does not have any volume snapshots", namespace, name)
	}

	snapshots := make([]*snap_v1.VolumeSnapshot, 0)
	for _, snapStatus := range snap.Status.VolumeSnapshots {
		snap, err := k.GetSnapshot(snapStatus.VolumeSnapshotName, namespace)
		if err != nil {
			return nil, err
		}

		snapshots = append(snapshots, snap)
	}

	return snapshots, nil
}

// GroupSnapshot APIs - END

// Restore Snapshot APIs - BEGIN

func (k *k8sOps) CreateVolumeSnapshotRestore(snapRestore *v1alpha1.VolumeSnapshotRestore) (*v1alpha1.VolumeSnapshotRestore, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}
	return k.storkClient.Stork().VolumeSnapshotRestores(snapRestore.Namespace).Create(snapRestore)
}

func (k *k8sOps) UpdateVolumeSnapshotRestore(snapRestore *v1alpha1.VolumeSnapshotRestore) (*v1alpha1.VolumeSnapshotRestore, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}
	return k.storkClient.Stork().VolumeSnapshotRestores(snapRestore.Namespace).Update(snapRestore)
}

func (k *k8sOps) GetVolumeSnapshotRestore(name, namespace string) (*v1alpha1.VolumeSnapshotRestore, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}
	return k.storkClient.Stork().VolumeSnapshotRestores(namespace).Get(name, meta_v1.GetOptions{})
}

func (k *k8sOps) ListVolumeSnapshotRestore(namespace string) (*v1alpha1.VolumeSnapshotRestoreList, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}
	return k.storkClient.Stork().VolumeSnapshotRestores(namespace).List(meta_v1.ListOptions{})
}

func (k *k8sOps) DeleteVolumeSnapshotRestore(name, namespace string) error {
	if err := k.initK8sClient(); err != nil {
		return err
	}
	return k.storkClient.Stork().VolumeSnapshotRestores(namespace).Delete(name, &meta_v1.DeleteOptions{})
}

func (k *k8sOps) ValidateVolumeSnapshotRestore(name, namespace string, timeout, retryInterval time.Duration) error {
	t := func() (interface{}, bool, error) {
		if err := k.initK8sClient(); err != nil {
			return "", true, err
		}

		snapRestore, err := k.storkClient.Stork().VolumeSnapshotRestores(namespace).Get(name, meta_v1.GetOptions{})
		if err != nil {
			return "", true, err
		}

		if snapRestore.Status.Status == v1alpha1.VolumeSnapshotRestoreStatusSuccessful {
			return "", false, nil
		}
		return "", true, &ErrFailedToValidateCustomSpec{
			Name: snapRestore.Name,
			Cause: fmt.Sprintf("VolumeSnapshotRestore failed . Error: %v .Expected status: %v Actual status: %v",
				err, v1alpha1.VolumeSnapshotRestoreStatusSuccessful, snapRestore.Status.Status),
			Type: snapRestore,
		}
	}
	if _, err := task.DoRetryWithTimeout(t, timeout, retryInterval); err != nil {
		return err
	}

	return nil
}

// Restore Snapshot APIs - END

// Rule APIs - BEGIN

func (k *k8sOps) GetRule(name, namespace string) (*v1alpha1.Rule, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, nil
	}

	return k.storkClient.Stork().Rules(namespace).Get(name, meta_v1.GetOptions{})
}

func (k *k8sOps) CreateRule(rule *v1alpha1.Rule) (*v1alpha1.Rule, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, nil
	}

	return k.storkClient.Stork().Rules(rule.GetNamespace()).Create(rule)
}

func (k *k8sOps) DeleteRule(name, namespace string) error {
	if err := k.initK8sClient(); err != nil {
		return nil
	}

	return k.storkClient.Stork().Rules(namespace).Delete(name, &meta_v1.DeleteOptions{
		PropagationPolicy: &deleteForegroundPolicy,
	})
}

// Rule APIs - END

// ClusterPair APIs - BEGIN

func (k *k8sOps) GetClusterPair(name string, namespace string) (*v1alpha1.ClusterPair, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	return k.storkClient.Stork().ClusterPairs(namespace).Get(name, meta_v1.GetOptions{})
}

func (k *k8sOps) ListClusterPairs(namespace string) (*v1alpha1.ClusterPairList, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	return k.storkClient.Stork().ClusterPairs(namespace).List(meta_v1.ListOptions{})
}

func (k *k8sOps) CreateClusterPair(pair *v1alpha1.ClusterPair) (*v1alpha1.ClusterPair, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	return k.storkClient.Stork().ClusterPairs(pair.Namespace).Create(pair)
}

func (k *k8sOps) UpdateClusterPair(pair *v1alpha1.ClusterPair) (*v1alpha1.ClusterPair, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	return k.storkClient.Stork().ClusterPairs(pair.Namespace).Update(pair)
}

func (k *k8sOps) DeleteClusterPair(name string, namespace string) error {
	if err := k.initK8sClient(); err != nil {
		return err
	}

	return k.storkClient.Stork().ClusterPairs(namespace).Delete(name, &meta_v1.DeleteOptions{
		PropagationPolicy: &deleteForegroundPolicy,
	})
}

func (k *k8sOps) ValidateClusterPair(name string, namespace string, timeout, retryInterval time.Duration) error {
	if err := k.initK8sClient(); err != nil {
		return err
	}
	t := func() (interface{}, bool, error) {
		clusterPair, err := k.GetClusterPair(name, namespace)
		if err != nil {
			return "", true, err
		}

		if clusterPair.Status.SchedulerStatus == v1alpha1.ClusterPairStatusReady &&
			(clusterPair.Status.StorageStatus == v1alpha1.ClusterPairStatusReady ||
				clusterPair.Status.StorageStatus == v1alpha1.ClusterPairStatusNotProvided) {
			return "", false, nil
		} else if clusterPair.Status.SchedulerStatus == v1alpha1.ClusterPairStatusError ||
			clusterPair.Status.StorageStatus == v1alpha1.ClusterPairStatusError {
			return "", true, &ErrFailedToValidateCustomSpec{
				Name:  name,
				Cause: fmt.Sprintf("Storage Status: %v \t Scheduler Status: %v", clusterPair.Status.StorageStatus, clusterPair.Status.SchedulerStatus),
				Type:  clusterPair,
			}
		}

		return "", true, &ErrFailedToValidateCustomSpec{
			Name:  name,
			Cause: fmt.Sprintf("Storage Status: %v \t Scheduler Status: %v", clusterPair.Status.StorageStatus, clusterPair.Status.SchedulerStatus),
			Type:  clusterPair,
		}
	}

	if _, err := task.DoRetryWithTimeout(t, timeout, retryInterval); err != nil {
		return err
	}

	return nil
}

// ClusterPair APIs - END

// Migration APIs - BEGIN

func (k *k8sOps) GetMigration(name string, namespace string) (*v1alpha1.Migration, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	return k.storkClient.Stork().Migrations(namespace).Get(name, meta_v1.GetOptions{})
}

func (k *k8sOps) ListMigrations(namespace string) (*v1alpha1.MigrationList, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	return k.storkClient.Stork().Migrations(namespace).List(meta_v1.ListOptions{})
}

func (k *k8sOps) CreateMigration(migration *v1alpha1.Migration) (*v1alpha1.Migration, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	return k.storkClient.Stork().Migrations(migration.Namespace).Create(migration)
}

func (k *k8sOps) DeleteMigration(name string, namespace string) error {
	if err := k.initK8sClient(); err != nil {
		return err
	}

	return k.storkClient.Stork().Migrations(namespace).Delete(name, &meta_v1.DeleteOptions{
		PropagationPolicy: &deleteForegroundPolicy,
	})
}

func (k *k8sOps) UpdateMigration(migration *v1alpha1.Migration) (*v1alpha1.Migration, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	return k.storkClient.Stork().Migrations(migration.Namespace).Update(migration)
}

func (k *k8sOps) ValidateMigration(name string, namespace string, timeout, retryInterval time.Duration) error {
	if err := k.initK8sClient(); err != nil {
		return err
	}
	t := func() (interface{}, bool, error) {
		resp, err := k.GetMigration(name, namespace)
		if err != nil {
			return "", true, err
		}

		if resp.Status.Status == v1alpha1.MigrationStatusSuccessful {
			return "", false, nil
		} else if resp.Status.Status == v1alpha1.MigrationStatusFailed {
			return "", false, &ErrFailedToValidateCustomSpec{
				Name:  name,
				Cause: fmt.Sprintf("Migration Status %v", resp.Status.Status),
				Type:  resp,
			}
		}

		return "", true, &ErrFailedToValidateCustomSpec{
			Name:  name,
			Cause: fmt.Sprintf("Migration Status %v", resp.Status.Status),
			Type:  resp,
		}
	}

	if _, err := task.DoRetryWithTimeout(t, timeout, retryInterval); err != nil {
		return err
	}

	return nil
}

func (k *k8sOps) GetMigrationSchedule(name string, namespace string) (*v1alpha1.MigrationSchedule, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	return k.storkClient.Stork().MigrationSchedules(namespace).Get(name, meta_v1.GetOptions{})
}

func (k *k8sOps) ListMigrationSchedules(namespace string) (*v1alpha1.MigrationScheduleList, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	return k.storkClient.Stork().MigrationSchedules(namespace).List(meta_v1.ListOptions{})
}

func (k *k8sOps) CreateMigrationSchedule(migrationSchedule *v1alpha1.MigrationSchedule) (*v1alpha1.MigrationSchedule, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	return k.storkClient.Stork().MigrationSchedules(migrationSchedule.Namespace).Create(migrationSchedule)
}

func (k *k8sOps) UpdateMigrationSchedule(migrationSchedule *v1alpha1.MigrationSchedule) (*v1alpha1.MigrationSchedule, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	return k.storkClient.Stork().MigrationSchedules(migrationSchedule.Namespace).Update(migrationSchedule)
}
func (k *k8sOps) DeleteMigrationSchedule(name string, namespace string) error {
	if err := k.initK8sClient(); err != nil {
		return err
	}

	return k.storkClient.Stork().MigrationSchedules(namespace).Delete(name, &meta_v1.DeleteOptions{
		PropagationPolicy: &deleteForegroundPolicy,
	})
}

func (k *k8sOps) ValidateMigrationSchedule(name string, namespace string, timeout, retryInterval time.Duration) (
	map[v1alpha1.SchedulePolicyType][]*v1alpha1.ScheduledMigrationStatus, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}
	t := func() (interface{}, bool, error) {
		resp, err := k.GetMigrationSchedule(name, namespace)
		if err != nil {
			return nil, true, err
		}

		if len(resp.Status.Items) == 0 {
			return nil, true, &ErrFailedToValidateCustomSpec{
				Name:  name,
				Cause: fmt.Sprintf("0 migrations have yet run for the migration schedule"),
				Type:  resp,
			}
		}

		failedMigrations := make([]string, 0)
		pendingMigrations := make([]string, 0)
		for _, migrationStatuses := range resp.Status.Items {
			// The check below assumes that the status will not have a failed migration if the last one succeeded
			// so just get the last status
			if len(migrationStatuses) > 0 {
				status := migrationStatuses[len(migrationStatuses)-1]
				if status == nil {
					return nil, true, &ErrFailedToValidateCustomSpec{
						Name:  name,
						Cause: "MigrationSchedule has an empty migration in it's most recent status",
						Type:  resp,
					}
				}

				if status.Status == v1alpha1.MigrationStatusSuccessful {
					continue
				}

				if status.Status == v1alpha1.MigrationStatusFailed {
					failedMigrations = append(failedMigrations,
						fmt.Sprintf("migration: %s failed. status: %v", status.Name, status.Status))
				} else {
					pendingMigrations = append(pendingMigrations,
						fmt.Sprintf("migration: %s is not done. status: %v", status.Name, status.Status))
				}
			}
		}

		if len(failedMigrations) > 0 {
			return nil, false, &ErrFailedToValidateCustomSpec{
				Name: name,
				Cause: fmt.Sprintf("MigrationSchedule failed as one or more migrations have failed. %s",
					failedMigrations),
				Type: resp,
			}
		}

		if len(pendingMigrations) > 0 {
			return nil, true, &ErrFailedToValidateCustomSpec{
				Name: name,
				Cause: fmt.Sprintf("MigrationSchedule has certain migrations pending: %s",
					pendingMigrations),
				Type: resp,
			}
		}

		return resp.Status.Items, false, nil
	}

	ret, err := task.DoRetryWithTimeout(t, timeout, retryInterval)
	if err != nil {
		return nil, err
	}

	migrations, ok := ret.(map[v1alpha1.SchedulePolicyType][]*v1alpha1.ScheduledMigrationStatus)
	if !ok {
		return nil, fmt.Errorf("invalid type when checking migration schedules: %v", migrations)
	}

	return migrations, nil
}

// Migration APIs - END

// SchedulePolicy APIs - BEGIN

func (k *k8sOps) GetSchedulePolicy(name string) (*v1alpha1.SchedulePolicy, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	return k.storkClient.Stork().SchedulePolicies().Get(name, meta_v1.GetOptions{})
}

func (k *k8sOps) ListSchedulePolicies() (*v1alpha1.SchedulePolicyList, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	return k.storkClient.Stork().SchedulePolicies().List(meta_v1.ListOptions{})
}

func (k *k8sOps) CreateSchedulePolicy(schedulePolicy *v1alpha1.SchedulePolicy) (*v1alpha1.SchedulePolicy, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	return k.storkClient.Stork().SchedulePolicies().Create(schedulePolicy)
}

func (k *k8sOps) DeleteSchedulePolicy(name string) error {
	if err := k.initK8sClient(); err != nil {
		return err
	}

	return k.storkClient.Stork().SchedulePolicies().Delete(name, &meta_v1.DeleteOptions{
		PropagationPolicy: &deleteForegroundPolicy,
	})
}

func (k *k8sOps) UpdateSchedulePolicy(schedulePolicy *v1alpha1.SchedulePolicy) (*v1alpha1.SchedulePolicy, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	return k.storkClient.Stork().SchedulePolicies().Update(schedulePolicy)
}

// SchedulePolicy APIs - END

// ClusterDomain CRD - BEGIN

// CreateClusterDomainsStatus creates the ClusterDomainStatus
func (k *k8sOps) CreateClusterDomainsStatus(clusterDomainsStatus *v1alpha1.ClusterDomainsStatus) (*v1alpha1.ClusterDomainsStatus, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}
	return k.storkClient.Stork().ClusterDomainsStatuses().Create(clusterDomainsStatus)
}

// GetClusterDomainsStatus gets the ClusterDomainsStatus
func (k *k8sOps) GetClusterDomainsStatus(name string) (*v1alpha1.ClusterDomainsStatus, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}
	return k.storkClient.Stork().ClusterDomainsStatuses().Get(name, meta_v1.GetOptions{})
}

// UpdateClusterDomainsStatus updates the ClusterDomainsStatus
func (k *k8sOps) UpdateClusterDomainsStatus(clusterDomainsStatus *v1alpha1.ClusterDomainsStatus) (*v1alpha1.ClusterDomainsStatus, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}
	return k.storkClient.Stork().ClusterDomainsStatuses().Update(clusterDomainsStatus)
}

// DeleteClusterDomainsStatus deletes the ClusterDomainsStatus
func (k *k8sOps) DeleteClusterDomainsStatus(name string) error {
	if err := k.initK8sClient(); err != nil {
		return err
	}
	return k.storkClient.Stork().ClusterDomainsStatuses().Delete(name, &meta_v1.DeleteOptions{
		PropagationPolicy: &deleteForegroundPolicy,
	})
}

func (k *k8sOps) ValidateClusterDomainsStatus(name string, domainMap map[string]bool, timeout, retryInterval time.Duration) error {
	if err := k.initK8sClient(); err != nil {
		return err
	}
	t := func() (interface{}, bool, error) {
		cds, err := k.GetClusterDomainsStatus(name)
		if err != nil {
			return "", true, err
		}

		for _, domainInfo := range cds.Status.ClusterDomainInfos {
			isActive, _ := domainMap[domainInfo.Name]
			if isActive {
				if domainInfo.State != v1alpha1.ClusterDomainActive {
					return "", true, &ErrFailedToValidateCustomSpec{
						Name: domainInfo.Name,
						Cause: fmt.Sprintf("ClusterDomainsStatus mismatch. For domain %v "+
							"expected to be active found inactive", domainInfo.Name),
						Type: cds,
					}
				}
			} else {
				if domainInfo.State != v1alpha1.ClusterDomainInactive {
					return "", true, &ErrFailedToValidateCustomSpec{
						Name: domainInfo.Name,
						Cause: fmt.Sprintf("ClusterDomainsStatus mismatch. For domain %v "+
							"expected to be inactive found active", domainInfo.Name),
						Type: cds,
					}
				}
			}
		}

		return "", false, nil

	}
	if _, err := task.DoRetryWithTimeout(t, timeout, retryInterval); err != nil {
		return err
	}

	return nil

}

// ListClusterDomainStatuses lists ClusterDomainsStatus
func (k *k8sOps) ListClusterDomainStatuses() (*v1alpha1.ClusterDomainsStatusList, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}
	return k.storkClient.Stork().ClusterDomainsStatuses().List(meta_v1.ListOptions{})
}

// CreateClusterDomainUpdate creates the ClusterDomainUpdate
func (k *k8sOps) CreateClusterDomainUpdate(clusterDomainUpdate *v1alpha1.ClusterDomainUpdate) (*v1alpha1.ClusterDomainUpdate, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}
	return k.storkClient.Stork().ClusterDomainUpdates().Create(clusterDomainUpdate)
}

// GetClusterDomainUpdate gets the ClusterDomainUpdate
func (k *k8sOps) GetClusterDomainUpdate(name string) (*v1alpha1.ClusterDomainUpdate, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}
	return k.storkClient.Stork().ClusterDomainUpdates().Get(name, meta_v1.GetOptions{})
}

// UpdateClusterDomainUpdate updates the ClusterDomainUpdate
func (k *k8sOps) UpdateClusterDomainUpdate(clusterDomainUpdate *v1alpha1.ClusterDomainUpdate) (*v1alpha1.ClusterDomainUpdate, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}
	return k.storkClient.Stork().ClusterDomainUpdates().Update(clusterDomainUpdate)
}

// DeleteClusterDomainUpdate deletes the ClusterDomainUpdate
func (k *k8sOps) DeleteClusterDomainUpdate(name string) error {
	if err := k.initK8sClient(); err != nil {
		return err
	}
	return k.storkClient.Stork().ClusterDomainUpdates().Delete(name, &meta_v1.DeleteOptions{
		PropagationPolicy: &deleteForegroundPolicy,
	})
}

// ValidateClusterDomainUpdate validates ClusterDomainUpdate
func (k *k8sOps) ValidateClusterDomainUpdate(name string, timeout, retryInterval time.Duration) error {
	if err := k.initK8sClient(); err != nil {
		return err
	}
	t := func() (interface{}, bool, error) {
		resp, err := k.GetClusterDomainUpdate(name)
		if err != nil {
			return "", true, err
		}

		if resp.Status.Status == v1alpha1.ClusterDomainUpdateStatusSuccessful {
			return "", false, nil
		} else if resp.Status.Status == v1alpha1.ClusterDomainUpdateStatusFailed {
			return "", false, &ErrFailedToValidateCustomSpec{
				Name:  name,
				Cause: fmt.Sprintf("ClusterDomainUpdate Status %v", resp.Status.Status),
				Type:  resp,
			}
		}

		return "", true, &ErrFailedToValidateCustomSpec{
			Name:  name,
			Cause: fmt.Sprintf("ClusterDomainUpdate Status %v", resp.Status.Status),
			Type:  resp,
		}
	}
	if _, err := task.DoRetryWithTimeout(t, timeout, retryInterval); err != nil {
		return err
	}

	return nil
}

// ListClusterDomainUpdates lists ClusterDomainUpdates
func (k *k8sOps) ListClusterDomainUpdates() (*v1alpha1.ClusterDomainUpdateList, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}
	return k.storkClient.Stork().ClusterDomainUpdates().List(meta_v1.ListOptions{})
}

// ClusterDomain CRD - END

// BackupLocation APIs - BEGIN

func (k *k8sOps) GetBackupLocation(name string, namespace string) (*v1alpha1.BackupLocation, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	backupLocation, err := k.storkClient.Stork().BackupLocations(namespace).Get(name, meta_v1.GetOptions{})
	if err != nil {
		return nil, err
	}
	err = backupLocation.UpdateFromSecret(k.client)
	if err != nil {
		return nil, err
	}
	return backupLocation, nil
}

func (k *k8sOps) ListBackupLocations(namespace string) (*v1alpha1.BackupLocationList, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	backupLocations, err := k.storkClient.Stork().BackupLocations(namespace).List(meta_v1.ListOptions{})
	if err != nil {
		return nil, err
	}
	for i := range backupLocations.Items {
		err = backupLocations.Items[i].UpdateFromSecret(k.client)
		if err != nil {
			return nil, err
		}
	}
	return backupLocations, nil
}

func (k *k8sOps) CreateBackupLocation(backupLocation *v1alpha1.BackupLocation) (*v1alpha1.BackupLocation, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	return k.storkClient.Stork().BackupLocations(backupLocation.Namespace).Create(backupLocation)
}

func (k *k8sOps) DeleteBackupLocation(name string, namespace string) error {
	if err := k.initK8sClient(); err != nil {
		return err
	}

	return k.storkClient.Stork().BackupLocations(namespace).Delete(name, &meta_v1.DeleteOptions{
		PropagationPolicy: &deleteForegroundPolicy,
	})
}

func (k *k8sOps) UpdateBackupLocation(backupLocation *v1alpha1.BackupLocation) (*v1alpha1.BackupLocation, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	return k.storkClient.Stork().BackupLocations(backupLocation.Namespace).Update(backupLocation)
}

func (k *k8sOps) ValidateBackupLocation(name, namespace string, timeout, retryInterval time.Duration) error {
	t := func() (interface{}, bool, error) {
		if err := k.initK8sClient(); err != nil {
			return "", true, err
		}

		resp, err := k.GetBackupLocation(name, namespace)
		if err != nil {
			return "", true, &ErrFailedToValidateCustomSpec{
				Name:  name,
				Cause: fmt.Sprintf("BackupLocation failed . Error: %v", err),
				Type:  resp,
			}
		}
		return "", false, nil
	}

	if _, err := task.DoRetryWithTimeout(t, timeout, retryInterval); err != nil {
		return err
	}
	return nil
}

// BackupLocation APIs - END

// ApplicationBackupRestore APIs - BEGIN

func (k *k8sOps) GetApplicationBackup(name string, namespace string) (*v1alpha1.ApplicationBackup, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	return k.storkClient.Stork().ApplicationBackups(namespace).Get(name, meta_v1.GetOptions{})
}

func (k *k8sOps) ListApplicationBackups(namespace string) (*v1alpha1.ApplicationBackupList, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	return k.storkClient.Stork().ApplicationBackups(namespace).List(meta_v1.ListOptions{})
}

func (k *k8sOps) CreateApplicationBackup(backup *v1alpha1.ApplicationBackup) (*v1alpha1.ApplicationBackup, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	return k.storkClient.Stork().ApplicationBackups(backup.Namespace).Create(backup)
}

func (k *k8sOps) DeleteApplicationBackup(name string, namespace string) error {
	if err := k.initK8sClient(); err != nil {
		return err
	}

	return k.storkClient.Stork().ApplicationBackups(namespace).Delete(name, &meta_v1.DeleteOptions{
		PropagationPolicy: &deleteForegroundPolicy,
	})
}

func (k *k8sOps) UpdateApplicationBackup(backup *v1alpha1.ApplicationBackup) (*v1alpha1.ApplicationBackup, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	return k.storkClient.Stork().ApplicationBackups(backup.Namespace).Update(backup)
}

func (k *k8sOps) ValidateApplicationBackup(name, namespace string, timeout, retryInterval time.Duration) error {
	t := func() (interface{}, bool, error) {
		if err := k.initK8sClient(); err != nil {
			return "", true, err
		}

		applicationbackup, err := k.GetApplicationBackup(name, namespace)
		if err != nil {
			return "", true, err
		}

		if applicationbackup.Status.Status == v1alpha1.ApplicationBackupStatusSuccessful {
			return "", false, nil
		}

		return "", true, &ErrFailedToValidateCustomSpec{
			Name:  applicationbackup.Name,
			Cause: fmt.Sprintf("Application backup failed . Error: %v .Expected status: %v Actual status: %v", err, v1alpha1.ApplicationBackupStatusSuccessful, applicationbackup.Status.Status),
			Type:  applicationbackup,
		}

	}

	if _, err := task.DoRetryWithTimeout(t, timeout, retryInterval); err != nil {
		return err
	}
	return nil
}

func (k *k8sOps) GetApplicationRestore(name string, namespace string) (*v1alpha1.ApplicationRestore, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	return k.storkClient.Stork().ApplicationRestores(namespace).Get(name, meta_v1.GetOptions{})
}

func (k *k8sOps) ListApplicationRestores(namespace string) (*v1alpha1.ApplicationRestoreList, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	return k.storkClient.Stork().ApplicationRestores(namespace).List(meta_v1.ListOptions{})
}

func (k *k8sOps) CreateApplicationRestore(restore *v1alpha1.ApplicationRestore) (*v1alpha1.ApplicationRestore, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	return k.storkClient.Stork().ApplicationRestores(restore.Namespace).Create(restore)
}

func (k *k8sOps) DeleteApplicationRestore(name string, namespace string) error {
	if err := k.initK8sClient(); err != nil {
		return err
	}

	return k.storkClient.Stork().ApplicationRestores(namespace).Delete(name, &meta_v1.DeleteOptions{
		PropagationPolicy: &deleteForegroundPolicy,
	})
}

func (k *k8sOps) ValidateApplicationRestore(name, namespace string, timeout, retryInterval time.Duration) error {
	t := func() (interface{}, bool, error) {
		if err := k.initK8sClient(); err != nil {
			return "", true, err
		}

		applicationrestore, err := k.storkClient.Stork().ApplicationRestores(namespace).Get(name, meta_v1.GetOptions{})
		if err != nil {
			return "", true, err
		}

		if applicationrestore.Status.Status == v1alpha1.ApplicationRestoreStatusSuccessful {
			return "", false, nil
		}
		return "", true, &ErrFailedToValidateCustomSpec{
			Name:  applicationrestore.Name,
			Cause: fmt.Sprintf("Application restore failed . Error: %v .Expected status: %v Actual status: %v", err, v1alpha1.ApplicationRestoreStatusSuccessful, applicationrestore.Status.Status),
			Type:  applicationrestore,
		}
	}
	if _, err := task.DoRetryWithTimeout(t, timeout, retryInterval); err != nil {
		return err
	}
	return nil
}

func (k *k8sOps) UpdateApplicationRestore(restore *v1alpha1.ApplicationRestore) (*v1alpha1.ApplicationRestore, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	return k.storkClient.Stork().ApplicationRestores(restore.Namespace).Update(restore)
}

func (k *k8sOps) GetApplicationBackupSchedule(name string, namespace string) (*v1alpha1.ApplicationBackupSchedule, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	return k.storkClient.Stork().ApplicationBackupSchedules(namespace).Get(name, meta_v1.GetOptions{})
}

func (k *k8sOps) ListApplicationBackupSchedules(namespace string) (*v1alpha1.ApplicationBackupScheduleList, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	return k.storkClient.Stork().ApplicationBackupSchedules(namespace).List(meta_v1.ListOptions{})
}

func (k *k8sOps) CreateApplicationBackupSchedule(applicationBackupSchedule *v1alpha1.ApplicationBackupSchedule) (*v1alpha1.ApplicationBackupSchedule, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	return k.storkClient.Stork().ApplicationBackupSchedules(applicationBackupSchedule.Namespace).Create(applicationBackupSchedule)
}

func (k *k8sOps) UpdateApplicationBackupSchedule(applicationBackupSchedule *v1alpha1.ApplicationBackupSchedule) (*v1alpha1.ApplicationBackupSchedule, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	return k.storkClient.Stork().ApplicationBackupSchedules(applicationBackupSchedule.Namespace).Update(applicationBackupSchedule)
}

func (k *k8sOps) DeleteApplicationBackupSchedule(name string, namespace string) error {
	if err := k.initK8sClient(); err != nil {
		return err
	}

	return k.storkClient.Stork().ApplicationBackupSchedules(namespace).Delete(name, &meta_v1.DeleteOptions{
		PropagationPolicy: &deleteForegroundPolicy,
	})
}

func (k *k8sOps) ValidateApplicationBackupSchedule(name string, namespace string, timeout, retryInterval time.Duration) (
	map[v1alpha1.SchedulePolicyType][]*v1alpha1.ScheduledApplicationBackupStatus, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}
	t := func() (interface{}, bool, error) {
		resp, err := k.GetApplicationBackupSchedule(name, namespace)
		if err != nil {
			return nil, true, err
		}

		if len(resp.Status.Items) == 0 {
			return nil, true, &ErrFailedToValidateCustomSpec{
				Name:  name,
				Cause: fmt.Sprintf("0 backups have yet run for the backup schedule"),
				Type:  resp,
			}
		}

		failedBackups := make([]string, 0)
		pendingBackups := make([]string, 0)
		for _, backupStatuses := range resp.Status.Items {
			// The check below assumes that the status will not have a failed
			// backup if the last one succeeded so just get the last status
			if len(backupStatuses) > 0 {
				status := backupStatuses[len(backupStatuses)-1]
				if status == nil {
					return nil, true, &ErrFailedToValidateCustomSpec{
						Name:  name,
						Cause: "ApplicationBackupSchedule has an empty backup in it's most recent status",
						Type:  resp,
					}
				}

				if status.Status == v1alpha1.ApplicationBackupStatusSuccessful {
					continue
				}

				if status.Status == v1alpha1.ApplicationBackupStatusFailed {
					failedBackups = append(failedBackups,
						fmt.Sprintf("backup: %s failed. status: %v", status.Name, status.Status))
				} else {
					pendingBackups = append(pendingBackups,
						fmt.Sprintf("backup: %s is not done. status: %v", status.Name, status.Status))
				}
			}
		}

		if len(failedBackups) > 0 {
			return nil, false, &ErrFailedToValidateCustomSpec{
				Name: name,
				Cause: fmt.Sprintf("ApplicationBackupSchedule failed as one or more backups have failed. %s",
					failedBackups),
				Type: resp,
			}
		}

		if len(pendingBackups) > 0 {
			return nil, true, &ErrFailedToValidateCustomSpec{
				Name: name,
				Cause: fmt.Sprintf("ApplicationBackupSchedule has certain migrations pending: %s",
					pendingBackups),
				Type: resp,
			}
		}

		return resp.Status.Items, false, nil
	}

	ret, err := task.DoRetryWithTimeout(t, timeout, retryInterval)
	if err != nil {
		return nil, err
	}

	backups, ok := ret.(map[v1alpha1.SchedulePolicyType][]*v1alpha1.ScheduledApplicationBackupStatus)
	if !ok {
		return nil, fmt.Errorf("invalid type when checking backup schedules: %v", backups)
	}

	return backups, nil
}

// ApplicationBackupRestore APIs - END

// ApplicationClone APIs - BEGIN

func (k *k8sOps) GetApplicationClone(name string, namespace string) (*v1alpha1.ApplicationClone, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	return k.storkClient.Stork().ApplicationClones(namespace).Get(name, meta_v1.GetOptions{})
}

func (k *k8sOps) ListApplicationClones(namespace string) (*v1alpha1.ApplicationCloneList, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	return k.storkClient.Stork().ApplicationClones(namespace).List(meta_v1.ListOptions{})
}

func (k *k8sOps) CreateApplicationClone(clone *v1alpha1.ApplicationClone) (*v1alpha1.ApplicationClone, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	return k.storkClient.Stork().ApplicationClones(clone.Namespace).Create(clone)
}

func (k *k8sOps) DeleteApplicationClone(name string, namespace string) error {
	if err := k.initK8sClient(); err != nil {
		return err
	}

	return k.storkClient.Stork().ApplicationClones(namespace).Delete(name, &meta_v1.DeleteOptions{
		PropagationPolicy: &deleteForegroundPolicy,
	})
}

func (k *k8sOps) UpdateApplicationClone(clone *v1alpha1.ApplicationClone) (*v1alpha1.ApplicationClone, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	return k.storkClient.Stork().ApplicationClones(clone.Namespace).Update(clone)
}

func (k *k8sOps) ValidateApplicationClone(name, namespace string, timeout, retryInterval time.Duration) error {
	t := func() (interface{}, bool, error) {
		if err := k.initK8sClient(); err != nil {
			return "", true, err
		}

		applicationclone, err := k.storkClient.Stork().ApplicationClones(namespace).Get(name, meta_v1.GetOptions{})
		if err != nil {
			return "", true, err
		}

		if applicationclone.Status.Status == v1alpha1.ApplicationCloneStatusSuccessful {
			return "", false, nil
		}
		return "", true, &ErrFailedToValidateCustomSpec{
			Name:  applicationclone.Name,
			Cause: fmt.Sprintf("Application Clone failed . Error: %v .Expected status: %v Actual status: %v", err, v1alpha1.ApplicationCloneStatusSuccessful, applicationclone.Status.Status),
			Type:  applicationclone,
		}
	}
	if _, err := task.DoRetryWithTimeout(t, timeout, retryInterval); err != nil {
		return err
	}
	return nil
}

// ApplicationClone APIs - END
