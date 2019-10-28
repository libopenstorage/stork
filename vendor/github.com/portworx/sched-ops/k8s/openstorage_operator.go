package k8s

import (
	corev1alpha1 "github.com/libopenstorage/operator/pkg/apis/core/v1alpha1"
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// StorageClusterOps is an interface to perfrom k8s StorageCluster operations
type StorageClusterOps interface {
	// GetStorageCluster gets the StorageCluster with given name and namespace
	GetStorageCluster(string, string) (*corev1alpha1.StorageCluster, error)
	// ListStorageClusters lists all the StorageClusters
	ListStorageClusters(string) (*corev1alpha1.StorageClusterList, error)
	// UpdateStorageClusterStatus update the status of given StorageCluster
	UpdateStorageClusterStatus(*corev1alpha1.StorageCluster) (*corev1alpha1.StorageCluster, error)
}

// StorageCluster APIs - BEGIN

func (k *k8sOps) GetStorageCluster(name, namespace string) (*corev1alpha1.StorageCluster, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	return k.ostClient.CoreV1alpha1().StorageClusters(namespace).Get(name, meta_v1.GetOptions{})
}

func (k *k8sOps) ListStorageClusters(namespace string) (*corev1alpha1.StorageClusterList, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	return k.ostClient.CoreV1alpha1().StorageClusters(namespace).List(meta_v1.ListOptions{})
}

func (k *k8sOps) UpdateStorageClusterStatus(cluster *corev1alpha1.StorageCluster) (*corev1alpha1.StorageCluster, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	return k.ostClient.CoreV1alpha1().StorageClusters(cluster.Namespace).UpdateStatus(cluster)
}

// StorageCluster APIs - END
