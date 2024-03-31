package storage

import (
	"context"

	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// CsiDriverOps is an interface to perform k8s CsiDriver readonly operations
type CsiDriverOps interface {
	// ListCsiDrivers lists all CSI drivers
	ListCsiDrivers() (*storagev1.CSIDriverList, error)
	// GetCsiDriver returns the CSI driver for the given name
	GetCsiDriver(name string) (*storagev1.CSIDriver, error)
}

// ListCsiDrivers lists all CSI drivers
func (c *Client) ListCsiDrivers() (*storagev1.CSIDriverList, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}

	return c.storage.CSIDrivers().List(context.TODO(), metav1.ListOptions{})
}

// GetCsiDriver returns the CSI driver for the given name
func (c *Client) GetCsiDriver(name string) (*storagev1.CSIDriver, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}

	return c.storage.CSIDrivers().Get(context.TODO(), name, metav1.GetOptions{})
}
