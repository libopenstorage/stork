package storage

import (
	"context"

	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
)

// ScOps is an interface to perform k8s storage class operations
type ScOps interface {
	// GetStorageClasses returns all storageClasses that match given optional label selector
	GetStorageClasses(labelSelector map[string]string) (*storagev1.StorageClassList, error)
	// GetStorageClass returns the storage class for the give namme
	GetStorageClass(name string) (*storagev1.StorageClass, error)
	// GetDefaultStorageClasses returns all storageClasses that are set as default
	GetDefaultStorageClasses() (*storagev1.StorageClassList, error)
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
	// AnnotateStorageClassAsDefault annotates a given storage class as default Storage class
	AnnotateStorageClassAsDefault(name string) error
}

const (
	defaultStorageclassAnnotationKey = "storageclass.kubernetes.io/is-default-class"
)

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

// GetDefaultStorageClasses returns all storageClasses that are set as default
func (c *Client) GetDefaultStorageClasses() (*storagev1.StorageClassList, error) {
	defaultStorageClasses := &storagev1.StorageClassList{}
	var storageClassList *storagev1.StorageClassList
	if err := c.initClient(); err != nil {
		return nil, err
	}

	storageClassList, err := c.storage.StorageClasses().List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	for _, sc := range storageClassList.Items {
		if val, ok := sc.Annotations[defaultStorageclassAnnotationKey]; ok {
			if val == "true" {
				defaultStorageClasses.Items = append(defaultStorageClasses.Items, sc)
			}
		}
	}
	return defaultStorageClasses, nil
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

// AnnotateStorageClassAsDefault annotates a given storage class as default Storage class
func (c *Client) AnnotateStorageClassAsDefault(name string) error {
	if err := c.initClient(); err != nil {
		return err
	}
	sc, err := c.GetStorageClass(name)
	if err != nil {
		return err
	}
	if sc.Annotations == nil {
		sc.Annotations = make(map[string]string)
	}
	sc.Annotations[defaultStorageclassAnnotationKey] = "true"
	_, err = c.storage.StorageClasses().Update(context.TODO(), sc, metav1.UpdateOptions{})
	if err != nil {
		return err
	}
	return nil
}
