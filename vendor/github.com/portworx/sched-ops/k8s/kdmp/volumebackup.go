package kdmp

import (
	"context"

	kdmpv1alpha1 "github.com/portworx/kdmp/pkg/apis/kdmp/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// VolumeBackupOps is an interface to perfrom k8s volume backup CR crud operations
type VolumeBackupOps interface {
	// CreateVolumeBackup creates the VolumeBackup CR
	CreateVolumeBackup(*kdmpv1alpha1.VolumeBackup) (*kdmpv1alpha1.VolumeBackup, error)
	// GetVolumeBackup gets the VolumeBackup CR
	GetVolumeBackup(string, string) (*kdmpv1alpha1.VolumeBackup, error)
	// ListVolumeBackup lists all the VolumeBackup CRs
	ListVolumeBackup(namespace string, filterOptions metav1.ListOptions) (*kdmpv1alpha1.VolumeBackupList, error)
	// UpdateVolumeBackup updates the VolumeBackup CR
	UpdateVolumeBackup(*kdmpv1alpha1.VolumeBackup) (*kdmpv1alpha1.VolumeBackup, error)
	// DeleteVolumeBackup deletes the VolumeBackup CR
	DeleteVolumeBackup(string, string) error
}

// CreateVolumeBackup creates the VolumeBackup CR
func (c *Client) CreateVolumeBackup(backup *kdmpv1alpha1.VolumeBackup) (*kdmpv1alpha1.VolumeBackup, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.kdmp.KdmpV1alpha1().VolumeBackups(backup.Namespace).Create(context.TODO(), backup, metav1.CreateOptions{})
}

// GetVolumeBackup gets the VolumeBackup CR
func (c *Client) GetVolumeBackup(name, namespace string) (*kdmpv1alpha1.VolumeBackup, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.kdmp.KdmpV1alpha1().VolumeBackups(namespace).Get(context.TODO(), name, metav1.GetOptions{})
}

// ListVolumeBackup lists all the VolumeBackup CR
func (c *Client) ListVolumeBackup(namespace string, filterOptions metav1.ListOptions) (*kdmpv1alpha1.VolumeBackupList, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.kdmp.KdmpV1alpha1().VolumeBackups(namespace).List(context.TODO(), filterOptions)
}

// DeleteVolumeBackup deletes the VolumeBackup CR
func (c *Client) DeleteVolumeBackup(name string, namespace string) error {
	if err := c.initClient(); err != nil {
		return err
	}
	return c.kdmp.KdmpV1alpha1().VolumeBackups(namespace).Delete(context.TODO(), name, metav1.DeleteOptions{
		PropagationPolicy: &deleteForegroundPolicy,
	})
}

// UpdateVolumeBackup updates the VolumeBackup CR
func (c *Client) UpdateVolumeBackup(backup *kdmpv1alpha1.VolumeBackup) (*kdmpv1alpha1.VolumeBackup, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.kdmp.KdmpV1alpha1().VolumeBackups(backup.Namespace).Update(context.TODO(), backup, metav1.UpdateOptions{})
}
