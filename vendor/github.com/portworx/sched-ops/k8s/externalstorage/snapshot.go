package externalstorage

import (
	"context"
	"fmt"
	"time"

	snapv1 "github.com/kubernetes-incubator/external-storage/snapshot/pkg/apis/crd/v1"
	schederrors "github.com/portworx/sched-ops/k8s/errors"
	"github.com/portworx/sched-ops/task"
	corev1 "k8s.io/api/core/v1"
)

// SnapshotOps is an interface to perform k8s VolumeSnapshot operations
type SnapshotOps interface {
	// GetSnapshot returns the snapshot for given name and namespace
	GetSnapshot(name string, namespace string) (*snapv1.VolumeSnapshot, error)
	// ListSnapshots lists all snapshots in the given namespace
	ListSnapshots(namespace string) (*snapv1.VolumeSnapshotList, error)
	// CreateSnapshot creates the given snapshot
	CreateSnapshot(*snapv1.VolumeSnapshot) (*snapv1.VolumeSnapshot, error)
	// UpdateSnapshot updates the given snapshot
	UpdateSnapshot(*snapv1.VolumeSnapshot) (*snapv1.VolumeSnapshot, error)
	// DeleteSnapshot deletes the given snapshot
	DeleteSnapshot(name string, namespace string) error
	// ValidateSnapshot validates the given snapshot.
	ValidateSnapshot(name string, namespace string, retry bool, timeout, retryInterval time.Duration) error
	// GetVolumeForSnapshot returns the volumeID for the given snapshot
	GetVolumeForSnapshot(name string, namespace string) (string, error)
	// GetSnapshotStatus returns the status of the given snapshot
	GetSnapshotStatus(name string, namespace string) (*snapv1.VolumeSnapshotStatus, error)
	// GetSnapshotData returns the snapshot for given name
	GetSnapshotData(name string) (*snapv1.VolumeSnapshotData, error)
	// CreateSnapshotData creates the given volume snapshot data object
	CreateSnapshotData(*snapv1.VolumeSnapshotData) (*snapv1.VolumeSnapshotData, error)
	// DeleteSnapshotData deletes the given snapshot
	DeleteSnapshotData(name string) error
	// ValidateSnapshotData validates the given snapshot data object
	ValidateSnapshotData(name string, retry bool, timeout, retryInterval time.Duration) error
}

// CreateSnapshot creates the given snapshot
func (c *Client) CreateSnapshot(snap *snapv1.VolumeSnapshot) (*snapv1.VolumeSnapshot, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	var result snapv1.VolumeSnapshot
	if err := c.snap.Post().
		Name(snap.Metadata.Name).
		Resource(snapv1.VolumeSnapshotResourcePlural).
		Namespace(snap.Metadata.Namespace).
		Body(snap).
		Do(context.TODO()).Into(&result); err != nil {
		return nil, err
	}
	return &result, nil
}

// UpdateSnapshot updates the given snapshot
func (c *Client) UpdateSnapshot(snap *snapv1.VolumeSnapshot) (*snapv1.VolumeSnapshot, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}

	var result snapv1.VolumeSnapshot
	if err := c.snap.Put().
		Name(snap.Metadata.Name).
		Resource(snapv1.VolumeSnapshotResourcePlural).
		Namespace(snap.Metadata.Namespace).
		Body(snap).
		Do(context.TODO()).Into(&result); err != nil {
		return nil, err
	}
	return &result, nil
}

// DeleteSnapshot deletes the given snapshot
func (c *Client) DeleteSnapshot(name string, namespace string) error {
	if err := c.initClient(); err != nil {
		return err
	}
	return c.snap.Delete().
		Name(name).
		Resource(snapv1.VolumeSnapshotResourcePlural).
		Namespace(namespace).
		Do(context.TODO()).Error()
}

// ValidateSnapshot validates the given snapshot.
func (c *Client) ValidateSnapshot(name string, namespace string, retry bool, timeout, retryInterval time.Duration) error {
	if err := c.initClient(); err != nil {
		return err
	}
	t := func() (interface{}, bool, error) {
		status, err := c.GetSnapshotStatus(name, namespace)
		if err != nil {
			return "", true, err
		}

		for _, condition := range status.Conditions {
			if condition.Type == snapv1.VolumeSnapshotConditionReady && condition.Status == corev1.ConditionTrue {
				return "", false, nil
			} else if condition.Type == snapv1.VolumeSnapshotConditionError && condition.Status == corev1.ConditionTrue {
				return "", true, &schederrors.ErrSnapshotFailed{
					ID:    name,
					Cause: fmt.Sprintf("Snapshot Status %v", status),
				}
			}
		}

		return "", true, &schederrors.ErrSnapshotNotReady{
			ID:    name,
			Cause: fmt.Sprintf("Snapshot Status %v", status),
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

// ValidateSnapshotData validates the given snapshot data object
func (c *Client) ValidateSnapshotData(name string, retry bool, timeout, retryInterval time.Duration) error {
	if err := c.initClient(); err != nil {
		return err
	}

	t := func() (interface{}, bool, error) {
		snapData, err := c.GetSnapshotData(name)
		if err != nil {
			return "", true, err
		}

		for _, condition := range snapData.Status.Conditions {
			if condition.Status == corev1.ConditionTrue {
				if condition.Type == snapv1.VolumeSnapshotDataConditionReady {
					return "", false, nil
				} else if condition.Type == snapv1.VolumeSnapshotDataConditionError {
					return "", true, &schederrors.ErrSnapshotDataFailed{
						ID:    name,
						Cause: fmt.Sprintf("SnapshotData Status %v", snapData.Status),
					}
				}
			}
		}

		return "", true, &schederrors.ErrSnapshotDataNotReady{
			ID:    name,
			Cause: fmt.Sprintf("SnapshotData Status %v", snapData.Status),
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

// GetVolumeForSnapshot returns the volumeID for the given snapshot
func (c *Client) GetVolumeForSnapshot(name string, namespace string) (string, error) {
	snapshot, err := c.GetSnapshot(name, namespace)
	if err != nil {
		return "", err
	}

	return snapshot.Metadata.Name, nil
}

// GetSnapshot returns the snapshot for given name and namespace
func (c *Client) GetSnapshot(name string, namespace string) (*snapv1.VolumeSnapshot, error) {
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

// ListSnapshots lists all snapshots in the given namespace
func (c *Client) ListSnapshots(namespace string) (*snapv1.VolumeSnapshotList, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}

	var result snapv1.VolumeSnapshotList
	if err := c.snap.Get().
		Resource(snapv1.VolumeSnapshotResourcePlural).
		Namespace(namespace).
		Do(context.TODO()).Into(&result); err != nil {
		return nil, err
	}

	return &result, nil
}

// GetSnapshotStatus returns the status of the given snapshot
func (c *Client) GetSnapshotStatus(name string, namespace string) (*snapv1.VolumeSnapshotStatus, error) {
	snapshot, err := c.GetSnapshot(name, namespace)
	if err != nil {
		return nil, err
	}

	return &snapshot.Status, nil
}

// GetSnapshotData returns the snapshot for given name
func (c *Client) GetSnapshotData(name string) (*snapv1.VolumeSnapshotData, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}

	var result snapv1.VolumeSnapshotData
	if err := c.snap.Get().
		Name(name).
		Resource(snapv1.VolumeSnapshotDataResourcePlural).
		Do(context.TODO()).Into(&result); err != nil {
		return nil, err
	}

	return &result, nil
}

// CreateSnapshotData creates the given volume snapshot data object
func (c *Client) CreateSnapshotData(snapData *snapv1.VolumeSnapshotData) (*snapv1.VolumeSnapshotData, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}

	var result snapv1.VolumeSnapshotData
	if err := c.snap.Post().
		Name(snapData.Metadata.Name).
		Resource(snapv1.VolumeSnapshotDataResourcePlural).
		Body(snapData).
		Do(context.TODO()).Into(&result); err != nil {
		return nil, err
	}
	return &result, nil
}

// DeleteSnapshotData deletes the given snapshot
func (c *Client) DeleteSnapshotData(name string) error {
	if err := c.initClient(); err != nil {
		return err
	}
	return c.snap.Delete().
		Name(name).
		Resource(snapv1.VolumeSnapshotDataResourcePlural).
		Do(context.TODO()).Error()
}
