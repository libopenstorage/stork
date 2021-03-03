package storage

import (
	"context"

	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
)

// StorageClassOps is an interface to perform k8s storage class operations
type StorageClassOps interface {
	// GetStorageClasses returns all storageClasses that match given optional label selector
	GetStorageClasses(labelSelector map[string]string) (*storagev1.StorageClassList, error)
	// GetStorageClass returns the storage class for the give namme
	GetStorageClass(name string) (*storagev1.StorageClass, error)
	// CreateStorageClass creates the given storage class
	CreateStorageClass(sc *storagev1.StorageClass) (*storagev1.StorageClass, error)
	// DeleteStorageClass deletes the given storage class
	DeleteStorageClass(name string) error
	// GetStorageClassParams returns the parameters of the given sc in the native map format
	GetStorageClassParams(sc *storagev1.StorageClass) (map[string]string, error)
	// ValidateStorageClass validates the given storage class
	// TODO: This is currently the same as GetStorageClass. If no one is using it,
	// we should remove this method
	ValidateStorageClass(name string) (*storagev1.StorageClass, error)
}

// GetStorageClasses returns all storageClasses that match given optional label selector
func (c *Client) GetStorageClasses(labelSelector map[string]string) (*storagev1.StorageClassList, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}

	return c.storage.StorageClasses().List(context.TODO(), metav1.ListOptions{
		LabelSelector: labels.FormatLabels(labelSelector),
	})
}

// GetStorageClass returns the storage class for the give namme
func (c *Client) GetStorageClass(name string) (*storagev1.StorageClass, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}

	return c.storage.StorageClasses().Get(context.TODO(), name, metav1.GetOptions{})
}

// CreateStorageClass creates the given storage class
func (c *Client) CreateStorageClass(sc *storagev1.StorageClass) (*storagev1.StorageClass, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}

	return c.storage.StorageClasses().Create(context.TODO(), sc, metav1.CreateOptions{})
}

// DeleteStorageClass deletes the given storage class
func (c *Client) DeleteStorageClass(name string) error {
	if err := c.initClient(); err != nil {
		return err
	}

	return c.storage.StorageClasses().Delete(context.TODO(), name, metav1.DeleteOptions{})
}

// GetStorageClassParams returns the parameters of the given sc in the native map format
func (c *Client) GetStorageClassParams(sc *storagev1.StorageClass) (map[string]string, error) {
	sc, err := c.GetStorageClass(sc.Name)
	if err != nil {
		return nil, err
	}

	return sc.Parameters, nil
}

// ValidateStorageClass validates the given storage class
func (c *Client) ValidateStorageClass(name string) (*storagev1.StorageClass, error) {
	return c.GetStorageClass(name)
}
