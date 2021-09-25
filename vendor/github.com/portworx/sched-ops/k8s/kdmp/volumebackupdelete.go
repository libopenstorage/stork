package kdmp

import (
	"context"

	kdmpv1alpha1 "github.com/portworx/kdmp/pkg/apis/kdmp/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// VolumeBackupDeleteOps is an interface to perfrom k8s volume delete CR crud operations
type VolumeBackupDeleteOps interface {
	// CreateVolumeBackupDelete creates the VolumeBackupDelete CR
	CreateVolumeBackupDelete(*kdmpv1alpha1.VolumeBackupDelete) (*kdmpv1alpha1.VolumeBackupDelete, error)
	// GetVolumeBackupDelete gets the VolumeBackupDelete CR
	GetVolumeBackupDelete(string, string) (*kdmpv1alpha1.VolumeBackupDelete, error)
	// ListVolumeBackupDelete lists all the VolumeBackupDelete CRs
	ListVolumeBackupDelete(string) (*kdmpv1alpha1.VolumeBackupDeleteList, error)
	// UpdateVolumeBackupDelete updates the VolumeBackupDelete CR
	UpdateVolumeBackupDelete(*kdmpv1alpha1.VolumeBackupDelete) (*kdmpv1alpha1.VolumeBackupDelete, error)
	// DeleteVolumeBackupDelete deletes the VolumeBackupDelete CR
	DeleteVolumeBackupDelete(string, string) error
}

// CreateVolumeBackupDelete creates the VolumeBackupDelete CR
func (c *Client) CreateVolumeBackupDelete(delete *kdmpv1alpha1.VolumeBackupDelete) (*kdmpv1alpha1.VolumeBackupDelete, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.kdmp.KdmpV1alpha1().VolumeBackupDeletes(delete.Namespace).Create(context.TODO(), delete, metav1.CreateOptions{})
}

// GetVolumeBackupDelete gets the VolumeBackupDelete CR
func (c *Client) GetVolumeBackupDelete(name, namespace string) (*kdmpv1alpha1.VolumeBackupDelete, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.kdmp.KdmpV1alpha1().VolumeBackupDeletes(namespace).Get(context.TODO(), name, metav1.GetOptions{})
}

// ListVolumeBackupDelete lists all the VolumeBackupDelete CR
func (c *Client) ListVolumeBackupDelete(namespace string) (*kdmpv1alpha1.VolumeBackupDeleteList, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.kdmp.KdmpV1alpha1().VolumeBackupDeletes(namespace).List(context.TODO(), metav1.ListOptions{})
}

// DeleteVolumeBackupDelete deletes the VolumeBackupDelete CR
func (c *Client) DeleteVolumeBackupDelete(name string, namespace string) error {
	if err := c.initClient(); err != nil {
		return err
	}
	return c.kdmp.KdmpV1alpha1().VolumeBackupDeletes(namespace).Delete(context.TODO(), name, metav1.DeleteOptions{
		PropagationPolicy: &deleteForegroundPolicy,
	})
}

// UpdateVolumeBackupDelete updates the VolumeBackupDelete CR
func (c *Client) UpdateVolumeBackupDelete(delete *kdmpv1alpha1.VolumeBackupDelete) (*kdmpv1alpha1.VolumeBackupDelete, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.kdmp.KdmpV1alpha1().VolumeBackupDeletes(delete.Namespace).Update(context.TODO(), delete, metav1.UpdateOptions{})
}
