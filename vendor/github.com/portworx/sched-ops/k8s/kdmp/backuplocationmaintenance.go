package kdmp

import (
	"context"

	kdmpv1alpha1 "github.com/portworx/kdmp/pkg/apis/kdmp/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// BackupLocationMaintenanceOps is an interface to perfrom k8s backuplocationmaintenance CR crud operations
type BackupLocationMaintenanceOps interface {
	// CreateBackupLocationMaintenance creates the BackupLocationMaintenance CR
	CreateBackupLocationMaintenance(*kdmpv1alpha1.BackupLocationMaintenance) (*kdmpv1alpha1.BackupLocationMaintenance, error)
	// GetBackupLocationMaintenance gets the BackupLocationMaintenanc CR
	GetBackupLocationMaintenance(string, string) (*kdmpv1alpha1.BackupLocationMaintenance, error)
	// ListBackupLocationMaintenance lists all the BackupLocationMaintenance CRs
	ListBackupLocationMaintenance(string) (*kdmpv1alpha1.BackupLocationMaintenanceList, error)
	// UpdateBackupLocationMaintenance updates the BackupLocationMaintenance CR
	UpdateBackupLocationMaintenance(*kdmpv1alpha1.BackupLocationMaintenance) (*kdmpv1alpha1.BackupLocationMaintenance, error)
	// DeleteBackupLocationMaintenance deletes the BackupLocationMaintenance CR
	DeleteBackupLocationMaintenance(string, string) error
}

// CreateBackupLocationMaintenance creates the BackupLocationMaintenance CR
func (c *Client) CreateBackupLocationMaintenance(blMaintenance *kdmpv1alpha1.BackupLocationMaintenance) (*kdmpv1alpha1.BackupLocationMaintenance, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.kdmp.KdmpV1alpha1().BackupLocationMaintenances(blMaintenance.Namespace).Create(context.TODO(), blMaintenance, metav1.CreateOptions{})
}

// GetBackupLocationMaintenance gets the BackupLocationMaintenance CR
func (c *Client) GetBackupLocationMaintenance(name, namespace string) (*kdmpv1alpha1.BackupLocationMaintenance, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.kdmp.KdmpV1alpha1().BackupLocationMaintenances(namespace).Get(context.TODO(), name, metav1.GetOptions{})
}

// ListBackupLocationMaintenance lists all the BackupLocationMaintenance CR
func (c *Client) ListBackupLocationMaintenance(namespace string) (*kdmpv1alpha1.BackupLocationMaintenanceList, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.kdmp.KdmpV1alpha1().BackupLocationMaintenances(namespace).List(context.TODO(), metav1.ListOptions{})
}

// DeleteBackupLocationMaintenance deletes the BackupLocationMaintenance CR
func (c *Client) DeleteBackupLocationMaintenance(name string, namespace string) error {
	if err := c.initClient(); err != nil {
		return err
	}
	return c.kdmp.KdmpV1alpha1().BackupLocationMaintenances(namespace).Delete(context.TODO(), name, metav1.DeleteOptions{
		PropagationPolicy: &deleteForegroundPolicy,
	})
}

// UpdateBackupLocationMaintenance updates the BackupLocationMaintenance CR
func (c *Client) UpdateBackupLocationMaintenance(blMaintenance *kdmpv1alpha1.BackupLocationMaintenance) (*kdmpv1alpha1.BackupLocationMaintenance, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.kdmp.KdmpV1alpha1().BackupLocationMaintenances(blMaintenance.Namespace).Update(context.TODO(), blMaintenance, metav1.UpdateOptions{})
}
