package externalsnapshotter

import (
	"context"

	v1 "github.com/kubernetes-csi/external-snapshotter/client/v6/apis/volumesnapshot/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// SnapshotClassOps is an interface to perform k8s VolumeSnapshotClass operations
type SnapshotClassOps interface {
	// CreateSnapshotClass creates the given snapshot class
	CreateSnapshotClass(snap *v1.VolumeSnapshotClass) (*v1.VolumeSnapshotClass, error)
	// GetSnapshotClass returns the snapshot class for given name
	GetSnapshotClass(name string) (*v1.VolumeSnapshotClass, error)
	// ListSnapshotClasses lists all snapshot classes
	ListSnapshotClasses() (*v1.VolumeSnapshotClassList, error)
	// UpdateSnapshotClass updates the given snapshot class
	UpdateSnapshotClass(snap *v1.VolumeSnapshotClass) (*v1.VolumeSnapshotClass, error)
	// DeleteSnapshotClass deletes the given snapshot class
	DeleteSnapshotClass(name string) error
}

// CreateSnapshotClass creates the given snapshot class.
func (c *Client) CreateSnapshotClass(snap *v1.VolumeSnapshotClass) (*v1.VolumeSnapshotClass, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.client.VolumeSnapshotClasses().Create(context.TODO(), snap, metav1.CreateOptions{})
}

// GetSnapshotClass returns the snapshot class for given name
func (c *Client) GetSnapshotClass(name string) (*v1.VolumeSnapshotClass, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.client.VolumeSnapshotClasses().Get(context.TODO(), name, metav1.GetOptions{})
}

// ListSnapshotClasses lists all snapshot classes
func (c *Client) ListSnapshotClasses() (*v1.VolumeSnapshotClassList, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.client.VolumeSnapshotClasses().List(context.TODO(), metav1.ListOptions{})
}

// UpdateSnapshotClass updates the given snapshot class
func (c *Client) UpdateSnapshotClass(snap *v1.VolumeSnapshotClass) (*v1.VolumeSnapshotClass, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.client.VolumeSnapshotClasses().Update(context.TODO(), snap, metav1.UpdateOptions{})
}

// DeleteSnapshotClass deletes the given snapshot
func (c *Client) DeleteSnapshotClass(name string) error {
	if err := c.initClient(); err != nil {
		return err
	}
	return c.client.VolumeSnapshotClasses().Delete(context.TODO(), name, metav1.DeleteOptions{})
}
