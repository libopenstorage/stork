package stork

import (
	"fmt"
	"time"

	storkv1alpha1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/portworx/sched-ops/k8s/errors"
	"github.com/portworx/sched-ops/task"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// VolumeSnapshotRestoreOps is interface to perform isnapshot restore using CRD
type VolumeSnapshotRestoreOps interface {
	// CreateVolumeSnapshotRestore restore snapshot to pvc specifed in CRD, if no pvcs defined we restore to
	// parent volumes
	CreateVolumeSnapshotRestore(snap *storkv1alpha1.VolumeSnapshotRestore) (*storkv1alpha1.VolumeSnapshotRestore, error)
	// UpdateVolumeSnapshotRestore updates given volumesnapshorestore CRD
	UpdateVolumeSnapshotRestore(snap *storkv1alpha1.VolumeSnapshotRestore) (*storkv1alpha1.VolumeSnapshotRestore, error)
	// GetVolumeSnapshotRestore returns details of given restore crd status
	GetVolumeSnapshotRestore(name, namespace string) (*storkv1alpha1.VolumeSnapshotRestore, error)
	// ListVolumeSnapshotRestore return list of volumesnapshotrestores in given namespaces
	ListVolumeSnapshotRestore(namespace string) (*storkv1alpha1.VolumeSnapshotRestoreList, error)
	// DeleteVolumeSnapshotRestore delete given volumesnapshotrestore CRD
	DeleteVolumeSnapshotRestore(name, namespace string) error
	// ValidateVolumeSnapshotRestore validates given volumesnapshotrestore CRD
	ValidateVolumeSnapshotRestore(name, namespace string, timeout, retry time.Duration) error
}

// CreateVolumeSnapshotRestore restore snapshot to pvc specifed in CRD, if no pvcs defined we restore to
// parent volumes
func (c *Client) CreateVolumeSnapshotRestore(snapRestore *storkv1alpha1.VolumeSnapshotRestore) (*storkv1alpha1.VolumeSnapshotRestore, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.stork.StorkV1alpha1().VolumeSnapshotRestores(snapRestore.Namespace).Create(snapRestore)
}

// UpdateVolumeSnapshotRestore updates given volumesnapshorestore CRD
func (c *Client) UpdateVolumeSnapshotRestore(snapRestore *storkv1alpha1.VolumeSnapshotRestore) (*storkv1alpha1.VolumeSnapshotRestore, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.stork.StorkV1alpha1().VolumeSnapshotRestores(snapRestore.Namespace).Update(snapRestore)
}

// GetVolumeSnapshotRestore returns details of given restore crd status
func (c *Client) GetVolumeSnapshotRestore(name, namespace string) (*storkv1alpha1.VolumeSnapshotRestore, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.stork.StorkV1alpha1().VolumeSnapshotRestores(namespace).Get(name, metav1.GetOptions{})
}

// ListVolumeSnapshotRestore return list of volumesnapshotrestores in given namespaces
func (c *Client) ListVolumeSnapshotRestore(namespace string) (*storkv1alpha1.VolumeSnapshotRestoreList, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.stork.StorkV1alpha1().VolumeSnapshotRestores(namespace).List(metav1.ListOptions{})
}

// DeleteVolumeSnapshotRestore delete given volumesnapshotrestore CRD
func (c *Client) DeleteVolumeSnapshotRestore(name, namespace string) error {
	if err := c.initClient(); err != nil {
		return err
	}
	return c.stork.StorkV1alpha1().VolumeSnapshotRestores(namespace).Delete(name, &metav1.DeleteOptions{})
}

// ValidateVolumeSnapshotRestore validates given volumesnapshotrestore CRD
func (c *Client) ValidateVolumeSnapshotRestore(name, namespace string, timeout, retryInterval time.Duration) error {
	if err := c.initClient(); err != nil {
		return err
	}
	t := func() (interface{}, bool, error) {
		snapRestore, err := c.stork.StorkV1alpha1().VolumeSnapshotRestores(namespace).Get(name, metav1.GetOptions{})
		if err != nil {
			return "", true, err
		}

		if snapRestore.Status.Status == storkv1alpha1.VolumeSnapshotRestoreStatusSuccessful {
			return "", false, nil
		}
		return "", true, &errors.ErrFailedToValidateCustomSpec{
			Name: snapRestore.Name,
			Cause: fmt.Sprintf("VolumeSnapshotRestore failed . Error: %v .Expected status: %v Actual status: %v",
				err, storkv1alpha1.VolumeSnapshotRestoreStatusSuccessful, snapRestore.Status.Status),
			Type: snapRestore,
		}
	}
	if _, err := task.DoRetryWithTimeout(t, timeout, retryInterval); err != nil {
		return err
	}

	return nil
}
