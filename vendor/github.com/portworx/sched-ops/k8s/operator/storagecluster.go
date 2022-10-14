package operator

import (
	"context"

	corev1 "github.com/libopenstorage/operator/pkg/apis/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// StorageClusterOps is an interface to perfrom k8s StorageCluster operations
type StorageClusterOps interface {
	// CreateStorageCluster creates the given StorageCluster
	CreateStorageCluster(*corev1.StorageCluster) (*corev1.StorageCluster, error)
	// UpdateStorageCluster updates the given StorageCluster
	UpdateStorageCluster(*corev1.StorageCluster) (*corev1.StorageCluster, error)
	// GetStorageCluster gets the StorageCluster with given name and namespace
	GetStorageCluster(string, string) (*corev1.StorageCluster, error)
	// ListStorageClusters lists all the StorageClusters
	ListStorageClusters(string) (*corev1.StorageClusterList, error)
	// DeleteStorageCluster deletes the given StorageCluster
	DeleteStorageCluster(string, string) error
	// UpdateStorageClusterStatus update the status of given StorageCluster
	UpdateStorageClusterStatus(*corev1.StorageCluster) (*corev1.StorageCluster, error)
}

// CreateStorageCluster creates the given StorageCluster
func (c *Client) CreateStorageCluster(cluster *corev1.StorageCluster) (*corev1.StorageCluster, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}

	ns := cluster.Namespace
	if len(ns) == 0 {
		ns = metav1.NamespaceDefault
	}

	return c.ost.CoreV1().StorageClusters(ns).Create(context.TODO(), cluster, metav1.CreateOptions{})
}

// UpdateStorageCluster updates the given StorageCluster
func (c *Client) UpdateStorageCluster(cluster *corev1.StorageCluster) (*corev1.StorageCluster, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}

	return c.ost.CoreV1().StorageClusters(cluster.Namespace).Update(context.TODO(), cluster, metav1.UpdateOptions{})
}

// GetStorageCluster gets the StorageCluster with given name and namespace
func (c *Client) GetStorageCluster(name, namespace string) (*corev1.StorageCluster, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.ost.CoreV1().StorageClusters(namespace).Get(context.TODO(), name, metav1.GetOptions{})
}

// ListStorageClusters lists all the StorageClusters
func (c *Client) ListStorageClusters(namespace string) (*corev1.StorageClusterList, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.ost.CoreV1().StorageClusters(namespace).List(context.TODO(), metav1.ListOptions{})
}

// DeleteStorageCluster deletes the given StorageCluster
func (c *Client) DeleteStorageCluster(name, namespace string) error {
	if err := c.initClient(); err != nil {
		return err
	}

	// TODO Temporary removing PropagationPolicy: &deleteForegroundPolicy from metav1.DeleteOptions{}, until we figure out the correct policy to use
	return c.ost.CoreV1().StorageClusters(namespace).Delete(context.TODO(), name, metav1.DeleteOptions{})
}

// UpdateStorageClusterStatus update the status of given StorageCluster
func (c *Client) UpdateStorageClusterStatus(cluster *corev1.StorageCluster) (*corev1.StorageCluster, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.ost.CoreV1().StorageClusters(cluster.Namespace).UpdateStatus(context.TODO(), cluster, metav1.UpdateOptions{})
}
