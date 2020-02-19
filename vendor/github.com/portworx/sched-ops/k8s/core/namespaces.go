package core

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// NamespaceOps is an interface to perform namespace operations
type NamespaceOps interface {
	// ListNamespaces returns all the namespaces
	ListNamespaces(labelSelector map[string]string) (*corev1.NamespaceList, error)
	// GetNamespace returns a namespace object for given name
	GetNamespace(name string) (*corev1.Namespace, error)
	// CreateNamespace creates a namespace with given name and metadata
	CreateNamespace(name string, metadata map[string]string) (*corev1.Namespace, error)
	// DeleteNamespace deletes a namespace with given name
	DeleteNamespace(name string) error
}

// ListNamespaces returns all the namespaces
func (c *Client) ListNamespaces(labelSelector map[string]string) (*corev1.NamespaceList, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}

	return c.core.Namespaces().List(metav1.ListOptions{
		LabelSelector: mapToCSV(labelSelector),
	})
}

// GetNamespace returns a namespace object for given name
func (c *Client) GetNamespace(name string) (*corev1.Namespace, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}

	return c.core.Namespaces().Get(name, metav1.GetOptions{})
}

// CreateNamespace creates a namespace with given name and metadata
func (c *Client) CreateNamespace(name string, metadata map[string]string) (*corev1.Namespace, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}

	return c.core.Namespaces().Create(&corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name:   name,
			Labels: metadata,
		},
	})
}

// DeleteNamespace deletes a namespace with given name
func (c *Client) DeleteNamespace(name string) error {
	if err := c.initClient(); err != nil {
		return err
	}

	return c.core.Namespaces().Delete(name, &metav1.DeleteOptions{})
}
