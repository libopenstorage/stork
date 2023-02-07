package kdmp

import (
	"context"

	kdmpv1alpha1 "github.com/portworx/kdmp/pkg/apis/kdmp/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ResourceExportOps is an interface to perform k8s ResourceExport CR crud operations
type ResourceExportOps interface {
	// CreateResourceExport creates the ResourceExport CR
	CreateResourceExport(*kdmpv1alpha1.ResourceExport) (*kdmpv1alpha1.ResourceExport, error)
	// GetResourceExport gets the ResourceExport CR
	GetResourceExport(string, string) (*kdmpv1alpha1.ResourceExport, error)
	// ListResourceExport lists all the ResourceExport CRs
	ListResourceExport(namespace string, filterOptions metav1.ListOptions) (*kdmpv1alpha1.ResourceExportList, error)
	// UpdateResourceExport updates the ResourceExport CR
	UpdateResourceExport(*kdmpv1alpha1.ResourceExport) (*kdmpv1alpha1.ResourceExport, error)
	// DeleteResourceExport deletes the ResourceExport CR
	DeleteResourceExport(string, string) error
}

// CreateResourceExport creates the ResourceExport CR
func (c *Client) CreateResourceExport(export *kdmpv1alpha1.ResourceExport) (*kdmpv1alpha1.ResourceExport, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.kdmp.KdmpV1alpha1().ResourceExports(export.Namespace).Create(context.TODO(), export, metav1.CreateOptions{})
}

// GetResourceExport gets the ResourceExport CR
func (c *Client) GetResourceExport(name, namespace string) (*kdmpv1alpha1.ResourceExport, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.kdmp.KdmpV1alpha1().ResourceExports(namespace).Get(context.TODO(), name, metav1.GetOptions{})
}

// ListResourceExport lists all the ResourceExport CR
func (c *Client) ListResourceExport(namespace string, filterOptions metav1.ListOptions) (*kdmpv1alpha1.ResourceExportList, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.kdmp.KdmpV1alpha1().ResourceExports(namespace).List(context.TODO(), filterOptions)
}

// DeleteResourceExport deletes the ResourceExport CR
func (c *Client) DeleteResourceExport(name string, namespace string) error {
	if err := c.initClient(); err != nil {
		return err
	}
	return c.kdmp.KdmpV1alpha1().ResourceExports(namespace).Delete(context.TODO(), name, metav1.DeleteOptions{
		PropagationPolicy: &deleteForegroundPolicy,
	})
}

// UpdateResourceExport deletes the ResourceExport CR
func (c *Client) UpdateResourceExport(export *kdmpv1alpha1.ResourceExport) (*kdmpv1alpha1.ResourceExport, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.kdmp.KdmpV1alpha1().ResourceExports(export.Namespace).Update(context.TODO(), export, metav1.UpdateOptions{})
}
