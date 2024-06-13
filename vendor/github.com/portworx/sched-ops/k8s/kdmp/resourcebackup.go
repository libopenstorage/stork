package kdmp

import (
	"context"

	kdmpv1alpha1 "github.com/portworx/kdmp/pkg/apis/kdmp/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ResourceBackupOps is an interface to perform k8s ResourceExport CR crud operations
type ResourceBackupOps interface {
	// CreateResourceExport creates the ResourceExport CR
	CreateResourceBackup(*kdmpv1alpha1.ResourceBackup) (*kdmpv1alpha1.ResourceBackup, error)
	// GetResourceBackup gets the ResourceBackup CR
	GetResourceBackup(string, string) (*kdmpv1alpha1.ResourceBackup, error)
	// ListResourceBackup lists all the ResourceBackup CRs
	ListResourceBackup(namespace string, filterOptions metav1.ListOptions) (*kdmpv1alpha1.ResourceBackupList, error)
	// UpdateResourceBackup updates the ResourceBackup CR
	UpdateResourceBackup(*kdmpv1alpha1.ResourceBackup) (*kdmpv1alpha1.ResourceBackup, error)
	// DeleteResourceBackup deletes the ResourceBackup CR
	DeleteResourceBackup(string, string) error
}

// CreateResourceBackup creates the ResourceBackup CR
func (c *Client) CreateResourceBackup(backup *kdmpv1alpha1.ResourceBackup) (*kdmpv1alpha1.ResourceBackup, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.kdmp.KdmpV1alpha1().ResourceBackups(backup.Namespace).Create(context.TODO(), backup, metav1.CreateOptions{})
}

// GetResourceBackup gets the ResourceBackup CR
func (c *Client) GetResourceBackup(name, namespace string) (*kdmpv1alpha1.ResourceBackup, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.kdmp.KdmpV1alpha1().ResourceBackups(namespace).Get(context.TODO(), name, metav1.GetOptions{})
}

// ListResourceBackup lists all the ResourceBackup CR
func (c *Client) ListResourceBackup(namespace string, filterOptions metav1.ListOptions) (*kdmpv1alpha1.ResourceBackupList, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.kdmp.KdmpV1alpha1().ResourceBackups(namespace).List(context.TODO(), filterOptions)
}

// DeleteResourceBackup deletes the ResourceBackup CR
func (c *Client) DeleteResourceBackup(name string, namespace string) error {
	if err := c.initClient(); err != nil {
		return err
	}
	return c.kdmp.KdmpV1alpha1().ResourceBackups(namespace).Delete(context.TODO(), name, metav1.DeleteOptions{
		PropagationPolicy: &deleteForegroundPolicy,
	})
}

// UpdateResourceBackup deletes the ResourceBackup CR
func (c *Client) UpdateResourceBackup(backup *kdmpv1alpha1.ResourceBackup) (*kdmpv1alpha1.ResourceBackup, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.kdmp.KdmpV1alpha1().ResourceBackups(backup.Namespace).Update(context.TODO(), backup, metav1.UpdateOptions{})
}
