package kdmp

import (
	"context"
	"fmt"
	"time"

	"github.com/portworx/sched-ops/k8s/errors"
	"github.com/portworx/sched-ops/task"

	kdmpv1alpha1 "github.com/portworx/kdmp/pkg/apis/kdmp/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// DataExportOps is an interface to perfrom k8s dataexport CR crud operations
type DataExportOps interface {
	// CreateDataExport creates the DataExport CR
	CreateDataExport(*kdmpv1alpha1.DataExport) (*kdmpv1alpha1.DataExport, error)
	// GetDataExport gets the DataExport CR
	GetDataExport(string, string) (*kdmpv1alpha1.DataExport, error)
	// ListDataExport lists all the DataExport CRs
	ListDataExport(string) (*kdmpv1alpha1.DataExportList, error)
	// UpdateDataExport updates the DataExport CR
	UpdateDataExport(*kdmpv1alpha1.DataExport) (*kdmpv1alpha1.DataExport, error)
	// DeleteDataExport deletes the DataExport CR
	DeleteDataExport(string, string) error
	// ValidateDataExport validates the DataExport CR
	ValidateDataExport(string, string, time.Duration, time.Duration) error
}

// CreateDataExport creates the DataExport CR
func (c *Client) CreateDataExport(export *kdmpv1alpha1.DataExport) (*kdmpv1alpha1.DataExport, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.kdmp.KdmpV1alpha1().DataExports(export.Namespace).Create(context.TODO(), export, metav1.CreateOptions{})
}

// GetDataExport gets the DataExport CR
func (c *Client) GetDataExport(name, namespace string) (*kdmpv1alpha1.DataExport, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.kdmp.KdmpV1alpha1().DataExports(namespace).Get(context.TODO(), name, metav1.GetOptions{})
}

// ListDataExport lists all the DataExport CR
func (c *Client) ListDataExport(namespace string) (*kdmpv1alpha1.DataExportList, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.kdmp.KdmpV1alpha1().DataExports(namespace).List(context.TODO(), metav1.ListOptions{})
}

// DeleteDataExport deletes the DataExport CR
func (c *Client) DeleteDataExport(name string, namespace string) error {
	if err := c.initClient(); err != nil {
		return err
	}
	return c.kdmp.KdmpV1alpha1().DataExports(namespace).Delete(context.TODO(), name, metav1.DeleteOptions{
		PropagationPolicy: &deleteForegroundPolicy,
	})
}

// UpdateDataExport deletes the DataExport CR
func (c *Client) UpdateDataExport(export *kdmpv1alpha1.DataExport) (*kdmpv1alpha1.DataExport, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.kdmp.KdmpV1alpha1().DataExports(export.Namespace).Update(context.TODO(), export, metav1.UpdateOptions{})
}

// ValidateDataExport validates DataExport CR
func (c *Client) ValidateDataExport(name string, namespace string, timeout, retryInterval time.Duration) error {
	if err := c.initClient(); err != nil {
		return err
	}
	t := func() (interface{}, bool, error) {
		dataExport, err := c.GetDataExport(name, namespace)
		if err != nil {
			return "", true, err
		}

		if dataExport.Status.Stage == kdmpv1alpha1.DataExportStageFinal &&
			dataExport.Status.Status == kdmpv1alpha1.DataExportStatusSuccessful {
			return "", false, nil
		} else if dataExport.Status.Status == kdmpv1alpha1.DataExportStatusFailed {
			return "", true, &errors.ErrFailedToValidateCustomSpec{
				Name:  name,
				Cause: fmt.Sprintf("Stage: %v \t Status: %v", dataExport.Status.Stage, dataExport.Status.Status),
				Type:  dataExport,
			}
		}

		return "", true, &errors.ErrFailedToValidateCustomSpec{
			Name:  name,
			Cause: fmt.Sprintf("Stage: %v \t Status: %v", dataExport.Status.Stage, dataExport.Status.Status),
			Type:  dataExport,
		}
	}

	if _, err := task.DoRetryWithTimeout(t, timeout, retryInterval); err != nil {
		return err
	}

	return nil
}
