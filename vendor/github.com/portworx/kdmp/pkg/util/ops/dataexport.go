package ops

import (
	"context"
	"fmt"
	"time"

	kdmpv1alpha1 "github.com/portworx/kdmp/pkg/apis/kdmp/v1alpha1"
	"github.com/portworx/sched-ops/k8s/errors"
	"github.com/portworx/sched-ops/task"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// DataExportOps is an interface to perform k8s DataExport operations
type DataExportOps interface {
	// CreateDataExport creates the DataExport
	CreateDataExport(ctx context.Context, cdataExport *kdmpv1alpha1.DataExport) (*kdmpv1alpha1.DataExport, error)
	// GetDataExport gets the DataExport
	GetDataExport(ctx context.Context, name string, namespace string) (*kdmpv1alpha1.DataExport, error)
	// ListDataExports lists all the DataExports
	ListDataExports(ctx context.Context, namespace string) (*kdmpv1alpha1.DataExportList, error)
	// UpdateDataExport updates the DataExport
	UpdateDataExport(ctx context.Context, dataExport *kdmpv1alpha1.DataExport) (*kdmpv1alpha1.DataExport, error)
	// DeleteDataExport deletes the DataExport
	DeleteDataExport(ctx context.Context, name string, namespace string) error
	// ValidateDataExport validates the DataExport
	ValidateDataExport(ctx context.Context, name string, namespace string, timeout, retryInterval time.Duration) error
}

// CreateDataExport creates the DataExport
func (c *Client) CreateDataExport(ctx context.Context, dataExport *kdmpv1alpha1.DataExport) (*kdmpv1alpha1.DataExport, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.kdmp.KdmpV1alpha1().DataExports(dataExport.Namespace).Create(ctx, dataExport, metav1.CreateOptions{})
}

// GetDataExport gets the DataExport
func (c *Client) GetDataExport(ctx context.Context, name string, namespace string) (*kdmpv1alpha1.DataExport, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.kdmp.KdmpV1alpha1().DataExports(namespace).Get(ctx, name, metav1.GetOptions{})
}

// ListDataExports lists all the DataExports
func (c *Client) ListDataExports(ctx context.Context, namespace string) (*kdmpv1alpha1.DataExportList, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.kdmp.KdmpV1alpha1().DataExports(namespace).List(ctx, metav1.ListOptions{})
}

// UpdateDataExport updates the DataExport
func (c *Client) UpdateDataExport(ctx context.Context, dataExport *kdmpv1alpha1.DataExport) (*kdmpv1alpha1.DataExport, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.kdmp.KdmpV1alpha1().DataExports(dataExport.Namespace).Update(ctx, dataExport, metav1.UpdateOptions{})
}

// PatchDataExport applies a patch for a given dataExport.
func (c *Client) PatchDataExport(ctx context.Context, name, ns string, pt types.PatchType, jsonPatch []byte) (*kdmpv1alpha1.DataExport, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.kdmp.KdmpV1alpha1().DataExports(ns).Patch(ctx, name, pt, jsonPatch, metav1.PatchOptions{})
}

// DeleteDataExport deletes the DataExport
func (c *Client) DeleteDataExport(ctx context.Context, name string, namespace string) error {
	if err := c.initClient(); err != nil {
		return err
	}
	return c.kdmp.KdmpV1alpha1().DataExports(namespace).Delete(ctx, name, metav1.DeleteOptions{
		PropagationPolicy: &deleteForegroundPolicy,
	})
}

// ValidateDataExport validates the DataExport
func (c *Client) ValidateDataExport(ctx context.Context, name, namespace string, timeout, retryInterval time.Duration) error {
	if err := c.initClient(); err != nil {
		return err
	}
	t := func() (interface{}, bool, error) {
		resp, err := c.GetDataExport(ctx, name, namespace)
		if err != nil {
			return "", true, &errors.ErrFailedToValidateCustomSpec{
				Name:  name,
				Cause: fmt.Sprintf("DataExport failed. Error: %v", err),
				Type:  resp,
			}
		}
		return "", false, nil
	}

	if _, err := task.DoRetryWithTimeout(t, timeout, retryInterval); err != nil {
		return err
	}
	return nil
}
