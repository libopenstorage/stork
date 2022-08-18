package stork

import (
	"context"

	storkv1alpha1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ResourceTransformOps is an interface to perform k8s ResourceTransformOps operations
type ResourceTransformOps interface {
	// CreateResourceTransformation creates the ResourceTransformation
	CreateResourceTransformation(*storkv1alpha1.ResourceTransformation) (*storkv1alpha1.ResourceTransformation, error)
	// GetResourceTransformation gets the ResourceTransformation
	GetResourceTransformation(string, string) (*storkv1alpha1.ResourceTransformation, error)
	// ListResourceTransformations lists all the ResourceTransformations
	ListResourceTransformations(namespace string, filterOptions metav1.ListOptions) (*storkv1alpha1.ResourceTransformationList, error)
	// UpdateResourceTransformation updates the ResourceTransformation
	UpdateResourceTransformation(*storkv1alpha1.ResourceTransformation) (*storkv1alpha1.ResourceTransformation, error)
	// DeleteResourceTransformation deletes the ResourceTransformation
	DeleteResourceTransformation(string, string) error
}

// CreateResourceTransformation creates the ResourceTransformation
func (c *Client) CreateResourceTransformation(ResourceTransformation *storkv1alpha1.ResourceTransformation) (*storkv1alpha1.ResourceTransformation, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.stork.StorkV1alpha1().ResourceTransformations(ResourceTransformation.Namespace).Create(context.TODO(), ResourceTransformation, metav1.CreateOptions{})
}

// GetResourceTransformation gets the ResourceTransformation
func (c *Client) GetResourceTransformation(name string, namespace string) (*storkv1alpha1.ResourceTransformation, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	ResourceTransformation, err := c.stork.StorkV1alpha1().ResourceTransformations(namespace).Get(context.TODO(), name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	return ResourceTransformation, nil
}

// ListResourceTransformations lists all the ResourceTransformations
func (c *Client) ListResourceTransformations(namespace string, filterOptions metav1.ListOptions) (*storkv1alpha1.ResourceTransformationList, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	ResourceTransformations, err := c.stork.StorkV1alpha1().ResourceTransformations(namespace).List(context.TODO(), filterOptions)
	if err != nil {
		return nil, err
	}
	return ResourceTransformations, nil
}

// UpdateResourceTransformation updates the ResourceTransformation
func (c *Client) UpdateResourceTransformation(ResourceTransformation *storkv1alpha1.ResourceTransformation) (*storkv1alpha1.ResourceTransformation, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.stork.StorkV1alpha1().ResourceTransformations(ResourceTransformation.Namespace).Update(context.TODO(), ResourceTransformation, metav1.UpdateOptions{})
}

// DeleteResourceTransformation deletes the ResourceTransformation
func (c *Client) DeleteResourceTransformation(name string, namespace string) error {
	if err := c.initClient(); err != nil {
		return err
	}
	return c.stork.StorkV1alpha1().ResourceTransformations(namespace).Delete(context.TODO(), name, metav1.DeleteOptions{
		PropagationPolicy: &deleteForegroundPolicy,
	})
}
