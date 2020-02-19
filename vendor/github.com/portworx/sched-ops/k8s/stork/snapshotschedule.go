package stork

import (
	"fmt"
	"time"

	snapv1 "github.com/kubernetes-incubator/external-storage/snapshot/pkg/apis/crd/v1"
	storkv1alpha1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/portworx/sched-ops/k8s/errors"
	"github.com/portworx/sched-ops/task"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// SnapshotScheduleOps is an interface to perform k8s VolumeSnapshotSchedule operations
type SnapshotScheduleOps interface {
	// GetSnapshotSchedule gets the SnapshotSchedule
	GetSnapshotSchedule(string, string) (*storkv1alpha1.VolumeSnapshotSchedule, error)
	// CreateSnapshotSchedule creates a SnapshotSchedule
	CreateSnapshotSchedule(*storkv1alpha1.VolumeSnapshotSchedule) (*storkv1alpha1.VolumeSnapshotSchedule, error)
	// UpdateSnapshotSchedule updates the SnapshotSchedule
	UpdateSnapshotSchedule(*storkv1alpha1.VolumeSnapshotSchedule) (*storkv1alpha1.VolumeSnapshotSchedule, error)
	// ListSnapshotSchedules lists all the SnapshotSchedules
	ListSnapshotSchedules(string) (*storkv1alpha1.VolumeSnapshotScheduleList, error)
	// DeleteSnapshotSchedule deletes the SnapshotSchedule
	DeleteSnapshotSchedule(string, string) error
	// ValidateSnapshotSchedule validates the given SnapshotSchedule. It checks the status of each of
	// the snapshots triggered for this schedule and returns a map of successfull snapshots. The key of the
	// map will be the schedule type and value will be list of snapshots for that schedule type.
	// The caller is expected to validate if the returned map has all snapshots expected at that point of time
	ValidateSnapshotSchedule(string, string, time.Duration, time.Duration) (
		map[storkv1alpha1.SchedulePolicyType][]*storkv1alpha1.ScheduledVolumeSnapshotStatus, error)
}

// CreateSnapshotSchedule creates a SnapshotSchedule
func (c *Client) CreateSnapshotSchedule(snapshotSchedule *storkv1alpha1.VolumeSnapshotSchedule) (*storkv1alpha1.VolumeSnapshotSchedule, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.stork.StorkV1alpha1().VolumeSnapshotSchedules(snapshotSchedule.Namespace).Create(snapshotSchedule)
}

// GetSnapshotSchedule gets the SnapshotSchedule
func (c *Client) GetSnapshotSchedule(name string, namespace string) (*storkv1alpha1.VolumeSnapshotSchedule, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.stork.StorkV1alpha1().VolumeSnapshotSchedules(namespace).Get(name, metav1.GetOptions{})
}

// ListSnapshotSchedules lists all the SnapshotSchedules
func (c *Client) ListSnapshotSchedules(namespace string) (*storkv1alpha1.VolumeSnapshotScheduleList, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.stork.StorkV1alpha1().VolumeSnapshotSchedules(namespace).List(metav1.ListOptions{})
}

// UpdateSnapshotSchedule updates the SnapshotSchedule
func (c *Client) UpdateSnapshotSchedule(snapshotSchedule *storkv1alpha1.VolumeSnapshotSchedule) (*storkv1alpha1.VolumeSnapshotSchedule, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.stork.StorkV1alpha1().VolumeSnapshotSchedules(snapshotSchedule.Namespace).Update(snapshotSchedule)
}

// DeleteSnapshotSchedule deletes the SnapshotSchedule
func (c *Client) DeleteSnapshotSchedule(name string, namespace string) error {
	if err := c.initClient(); err != nil {
		return err
	}
	return c.stork.StorkV1alpha1().VolumeSnapshotSchedules(namespace).Delete(name, &metav1.DeleteOptions{
		PropagationPolicy: &deleteForegroundPolicy,
	})
}

// ValidateSnapshotSchedule validates the given SnapshotSchedule. It checks the status of each of
// the snapshots triggered for this schedule and returns a map of successfull snapshots. The key of the
// map will be the schedule type and value will be list of snapshots for that schedule type.
// The caller is expected to validate if the returned map has all snapshots expected at that point of time
func (c *Client) ValidateSnapshotSchedule(name string, namespace string, timeout, retryInterval time.Duration) (
	map[storkv1alpha1.SchedulePolicyType][]*storkv1alpha1.ScheduledVolumeSnapshotStatus, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	t := func() (interface{}, bool, error) {
		resp, err := c.GetSnapshotSchedule(name, namespace)
		if err != nil {
			return nil, true, err
		}

		if len(resp.Status.Items) == 0 {
			return nil, true, &errors.ErrFailedToValidateCustomSpec{
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
					return nil, true, &errors.ErrFailedToValidateCustomSpec{
						Name:  name,
						Cause: "SnapshotSchedule has an empty migration in it's most recent status",
						Type:  resp,
					}
				}

				if status.Status == snapv1.VolumeSnapshotConditionReady {
					continue
				}

				if status.Status == snapv1.VolumeSnapshotConditionError {
					failedSnapshots = append(failedSnapshots,
						fmt.Sprintf("snapshot: %s failed. status: %v", status.Name, status.Status))
				} else {
					pendingSnapshots = append(pendingSnapshots,
						fmt.Sprintf("snapshot: %s is not done. status: %v", status.Name, status.Status))
				}
			}
		}

		if len(failedSnapshots) > 0 {
			return nil, false, &errors.ErrFailedToValidateCustomSpec{
				Name: name,
				Cause: fmt.Sprintf("SnapshotSchedule failed as one or more snapshots have failed. %s",
					failedSnapshots),
				Type: resp,
			}
		}

		if len(pendingSnapshots) > 0 {
			return nil, true, &errors.ErrFailedToValidateCustomSpec{
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

	snapshots, ok := ret.(map[storkv1alpha1.SchedulePolicyType][]*storkv1alpha1.ScheduledVolumeSnapshotStatus)
	if !ok {
		return nil, fmt.Errorf("invalid type when checking snapshot schedules: %v", snapshots)
	}

	return snapshots, nil
}
