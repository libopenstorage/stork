package stork

import (
	"context"
	"fmt"
	"time"

	snapv1 "github.com/kubernetes-incubator/external-storage/snapshot/pkg/apis/crd/v1"
	storkv1alpha1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	schederrors "github.com/portworx/sched-ops/k8s/errors"
	"github.com/portworx/sched-ops/task"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// GroupSnapshotOps is an interface to perform k8s GroupVolumeSnapshot operations
type GroupSnapshotOps interface {
	// GetGroupSnapshot returns the group snapshot for the given name and namespace
	GetGroupSnapshot(name, namespace string) (*storkv1alpha1.GroupVolumeSnapshot, error)
	// ListGroupSnapshots lists all group snapshots for the given namespace
	ListGroupSnapshots(namespace string) (*storkv1alpha1.GroupVolumeSnapshotList, error)
	// CreateGroupSnapshot creates the given group snapshot
	CreateGroupSnapshot(*storkv1alpha1.GroupVolumeSnapshot) (*storkv1alpha1.GroupVolumeSnapshot, error)
	// UpdateGroupSnapshot updates the given group snapshot
	UpdateGroupSnapshot(*storkv1alpha1.GroupVolumeSnapshot) (*storkv1alpha1.GroupVolumeSnapshot, error)
	// DeleteGroupSnapshot deletes the group snapshot with the given name and namespace
	DeleteGroupSnapshot(name, namespace string) error
	// ValidateGroupSnapshot checks if the group snapshot with given name and namespace is in ready state
	//  If retry is true, the validation will be retried with given timeout and retry internal
	ValidateGroupSnapshot(name, namespace string, retry bool, timeout, retryInterval time.Duration) error
	// GetSnapshotsForGroupSnapshot returns all child snapshots for the group snapshot
	GetSnapshotsForGroupSnapshot(name, namespace string) ([]*snapv1.VolumeSnapshot, error)
}

// GetGroupSnapshot returns the group snapshot for the given name and namespace
func (c *Client) GetGroupSnapshot(name, namespace string) (*storkv1alpha1.GroupVolumeSnapshot, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.stork.StorkV1alpha1().GroupVolumeSnapshots(namespace).Get(name, metav1.GetOptions{})
}

// ListGroupSnapshots lists all group snapshots for the given namespace
func (c *Client) ListGroupSnapshots(namespace string) (*storkv1alpha1.GroupVolumeSnapshotList, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.stork.StorkV1alpha1().GroupVolumeSnapshots(namespace).List(metav1.ListOptions{})
}

// CreateGroupSnapshot creates the given group snapshot
func (c *Client) CreateGroupSnapshot(snap *storkv1alpha1.GroupVolumeSnapshot) (*storkv1alpha1.GroupVolumeSnapshot, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.stork.StorkV1alpha1().GroupVolumeSnapshots(snap.Namespace).Create(snap)
}

// UpdateGroupSnapshot updates the given group snapshot
func (c *Client) UpdateGroupSnapshot(snap *storkv1alpha1.GroupVolumeSnapshot) (*storkv1alpha1.GroupVolumeSnapshot, error) {
	return c.stork.StorkV1alpha1().GroupVolumeSnapshots(snap.Namespace).Update(snap)
}

// DeleteGroupSnapshot deletes the group snapshot with the given name and namespace
func (c *Client) DeleteGroupSnapshot(name, namespace string) error {
	if err := c.initClient(); err != nil {
		return err
	}
	return c.stork.StorkV1alpha1().GroupVolumeSnapshots(namespace).Delete(name, &metav1.DeleteOptions{
		PropagationPolicy: &deleteForegroundPolicy,
	})
}

// ValidateGroupSnapshot checks if the group snapshot with given name and namespace is in ready state
//  If retry is true, the validation will be retried with given timeout and retry internal
func (c *Client) ValidateGroupSnapshot(name, namespace string, retry bool, timeout, retryInterval time.Duration) error {
	if err := c.initClient(); err != nil {
		return err
	}
	t := func() (interface{}, bool, error) {
		snap, err := c.GetGroupSnapshot(name, namespace)
		if err != nil {
			return "", true, err
		}

		if len(snap.Status.VolumeSnapshots) == 0 {
			return "", true, schederrors.ErrSnapshotNotReady{
				ID:    name,
				Cause: fmt.Sprintf("group snapshot has 0 child snapshots yet"),
			}
		}

		if snap.Status.Stage == storkv1alpha1.GroupSnapshotStageFinal {
			if snap.Status.Status == storkv1alpha1.GroupSnapshotSuccessful {
				// Perform extra check that all child snapshots are also ready
				notDoneChildSnaps := make([]string, 0)
				for _, childSnap := range snap.Status.VolumeSnapshots {
					conditions := childSnap.Conditions
					if len(conditions) == 0 {
						notDoneChildSnaps = append(notDoneChildSnaps, childSnap.VolumeSnapshotName)
						continue
					}

					lastCondition := conditions[0]
					if lastCondition.Status != corev1.ConditionTrue || lastCondition.Type != snapv1.VolumeSnapshotConditionReady {
						notDoneChildSnaps = append(notDoneChildSnaps, childSnap.VolumeSnapshotName)
						continue
					}
				}

				if len(notDoneChildSnaps) > 0 {
					return "", false, schederrors.ErrSnapshotFailed{
						ID: name,
						Cause: fmt.Sprintf("group snapshot is marked as successfull "+
							" but following child volumesnapshots are in pending or error state: %s", notDoneChildSnaps),
					}
				}

				return "", false, nil
			}

			if snap.Status.Status == storkv1alpha1.GroupSnapshotFailed {
				return "", false, schederrors.ErrSnapshotFailed{
					ID:    name,
					Cause: fmt.Sprintf("group snapshot is in failed state"),
				}
			}
		}

		return "", true, schederrors.ErrSnapshotNotReady{
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

// GetSnapshotsForGroupSnapshot returns all child snapshots for the group snapshot
func (c *Client) GetSnapshotsForGroupSnapshot(name, namespace string) ([]*snapv1.VolumeSnapshot, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	snap, err := c.GetGroupSnapshot(name, namespace)
	if err != nil {
		return nil, err
	}

	if len(snap.Status.VolumeSnapshots) == 0 {
		return nil, fmt.Errorf("group snapshot: [%s] %s does not have any volume snapshots", namespace, name)
	}

	snapshots := make([]*snapv1.VolumeSnapshot, 0)
	for _, snapStatus := range snap.Status.VolumeSnapshots {
		snap, err := c.getSnapshot(snapStatus.VolumeSnapshotName, namespace)
		if err != nil {
			return nil, err
		}

		snapshots = append(snapshots, snap)
	}

	return snapshots, nil
}

func (c *Client) getSnapshot(name string, namespace string) (*snapv1.VolumeSnapshot, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}

	var result snapv1.VolumeSnapshot
	if err := c.snap.Get().
		Name(name).
		Resource(snapv1.VolumeSnapshotResourcePlural).
		Namespace(namespace).
		Do(context.TODO()).Into(&result); err != nil {
		return nil, err
	}

	return &result, nil
}
