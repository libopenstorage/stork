package externalsnapshotter

import (
	"context"

	"github.com/kubernetes-csi/external-snapshotter/client/v4/apis/volumesnapshot/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// SnapshotOps is an interface to perform k8s VolumeSnapshot operations
type SnapshotOps interface {
	// CreateSnapshot creates the given snapshot
	CreateSnapshot(snap *v1beta1.VolumeSnapshot) (*v1beta1.VolumeSnapshot, error)
	// GetSnapshot returns the snapshot for given name and namespace
	GetSnapshot(name string, namespace string) (*v1beta1.VolumeSnapshot, error)
	// ListSnapshots lists all snapshots in the given namespace
	ListSnapshots(namespace string) (*v1beta1.VolumeSnapshotList, error)
	// UpdateSnapshot updates the given snapshot
	UpdateSnapshot(snap *v1beta1.VolumeSnapshot) (*v1beta1.VolumeSnapshot, error)
	// DeleteSnapshot deletes the given snapshot
	DeleteSnapshot(name string, namespace string) error
}

// CreateSnapshot creates the given snapshot.
func (c *Client) CreateSnapshot(snap *v1beta1.VolumeSnapshot) (*v1beta1.VolumeSnapshot, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.client.VolumeSnapshots(snap.Namespace).Create(context.TODO(), snap, metav1.CreateOptions{})
}

// GetSnapshot returns the snapshot for given name and namespace
func (c *Client) GetSnapshot(name string, namespace string) (*v1beta1.VolumeSnapshot, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.client.VolumeSnapshots(namespace).Get(context.TODO(), name, metav1.GetOptions{})
}

// ListSnapshots lists all snapshots in the given namespace
func (c *Client) ListSnapshots(namespace string) (*v1beta1.VolumeSnapshotList, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.client.VolumeSnapshots(namespace).List(context.TODO(), metav1.ListOptions{})
}

// UpdateSnapshot updates the given snapshot
func (c *Client) UpdateSnapshot(snap *v1beta1.VolumeSnapshot) (*v1beta1.VolumeSnapshot, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.client.VolumeSnapshots(snap.Namespace).Update(context.TODO(), snap, metav1.UpdateOptions{})
}

// DeleteSnapshot deletes the given snapshot
func (c *Client) DeleteSnapshot(name string, namespace string) error {
	if err := c.initClient(); err != nil {
		return err
	}
	return c.client.VolumeSnapshots(namespace).Delete(context.TODO(), name, metav1.DeleteOptions{})
}
