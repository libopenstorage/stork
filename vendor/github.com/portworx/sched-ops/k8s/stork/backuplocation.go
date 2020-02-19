package stork

import (
	"fmt"
	"time"

	storkv1alpha1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/portworx/sched-ops/k8s/errors"
	"github.com/portworx/sched-ops/task"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// BackupLocationOps is an interface to perform k8s BackupLocation operations
type BackupLocationOps interface {
	// CreateBackupLocation creates the BackupLocation
	CreateBackupLocation(*storkv1alpha1.BackupLocation) (*storkv1alpha1.BackupLocation, error)
	// GetBackupLocation gets the BackupLocation
	GetBackupLocation(string, string) (*storkv1alpha1.BackupLocation, error)
	// ListBackupLocations lists all the BackupLocations
	ListBackupLocations(string) (*storkv1alpha1.BackupLocationList, error)
	// UpdateBackupLocation updates the BackupLocation
	UpdateBackupLocation(*storkv1alpha1.BackupLocation) (*storkv1alpha1.BackupLocation, error)
	// DeleteBackupLocation deletes the BackupLocation
	DeleteBackupLocation(string, string) error
	// ValidateBackupLocation validates the BackupLocation
	ValidateBackupLocation(string, string, time.Duration, time.Duration) error
}

// CreateBackupLocation creates the BackupLocation
func (c *Client) CreateBackupLocation(backupLocation *storkv1alpha1.BackupLocation) (*storkv1alpha1.BackupLocation, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.stork.StorkV1alpha1().BackupLocations(backupLocation.Namespace).Create(backupLocation)
}

// GetBackupLocation gets the BackupLocation
func (c *Client) GetBackupLocation(name string, namespace string) (*storkv1alpha1.BackupLocation, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	backupLocation, err := c.stork.StorkV1alpha1().BackupLocations(namespace).Get(name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}

	// TODO: use secrets/corev1 client instead of clientset
	err = backupLocation.UpdateFromSecret(c.kube)
	if err != nil {
		return nil, err
	}
	return backupLocation, nil
}

// ListBackupLocations lists all the BackupLocations
func (c *Client) ListBackupLocations(namespace string) (*storkv1alpha1.BackupLocationList, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	backupLocations, err := c.stork.StorkV1alpha1().BackupLocations(namespace).List(metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	for i := range backupLocations.Items {
		err = backupLocations.Items[i].UpdateFromSecret(c.kube)
		if err != nil {
			return nil, err
		}
	}
	return backupLocations, nil
}

// UpdateBackupLocation updates the BackupLocation
func (c *Client) UpdateBackupLocation(backupLocation *storkv1alpha1.BackupLocation) (*storkv1alpha1.BackupLocation, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.stork.StorkV1alpha1().BackupLocations(backupLocation.Namespace).Update(backupLocation)
}

// DeleteBackupLocation deletes the BackupLocation
func (c *Client) DeleteBackupLocation(name string, namespace string) error {
	if err := c.initClient(); err != nil {
		return err
	}
	return c.stork.StorkV1alpha1().BackupLocations(namespace).Delete(name, &metav1.DeleteOptions{
		PropagationPolicy: &deleteForegroundPolicy,
	})
}

// ValidateBackupLocation validates the BackupLocation
func (c *Client) ValidateBackupLocation(name, namespace string, timeout, retryInterval time.Duration) error {
	if err := c.initClient(); err != nil {
		return err
	}
	t := func() (interface{}, bool, error) {
		resp, err := c.GetBackupLocation(name, namespace)
		if err != nil {
			return "", true, &errors.ErrFailedToValidateCustomSpec{
				Name:  name,
				Cause: fmt.Sprintf("BackupLocation failed . Error: %v", err),
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
