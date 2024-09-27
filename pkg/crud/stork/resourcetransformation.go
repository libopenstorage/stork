package stork

import (
	"context"
	"fmt"
	"time"

	storkv1alpha1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/portworx/sched-ops/k8s/errors"
	"github.com/portworx/sched-ops/task"
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
	// ValidateResourceTransformation validates resource transformation status
	ValidateResourceTransformation(string, string, time.Duration, time.Duration) error
}

// CreateResourceTransformation creates the ResourceTransformation CR
func (c *Client) CreateResourceTransformation(ResourceTransformation *storkv1alpha1.ResourceTransformation) (*storkv1alpha1.ResourceTransformation, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.stork.StorkV1alpha1().ResourceTransformations(ResourceTransformation.Namespace).Create(context.TODO(), ResourceTransformation, metav1.CreateOptions{})
}

// GetResourceTransformation gets the ResourceTransformation CR
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

// ListResourceTransformations lists all the ResourceTransformations CR
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

// UpdateResourceTransformation updates the ResourceTransformation CR
func (c *Client) UpdateResourceTransformation(ResourceTransformation *storkv1alpha1.ResourceTransformation) (*storkv1alpha1.ResourceTransformation, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.stork.StorkV1alpha1().ResourceTransformations(ResourceTransformation.Namespace).Update(context.TODO(), ResourceTransformation, metav1.UpdateOptions{})
}

// DeleteResourceTransformation deletes the ResourceTransformation CR
func (c *Client) DeleteResourceTransformation(name string, namespace string) error {
	if err := c.initClient(); err != nil {
		return err
	}
	return c.stork.StorkV1alpha1().ResourceTransformations(namespace).Delete(context.TODO(), name, metav1.DeleteOptions{
		PropagationPolicy: &deleteForegroundPolicy,
	})
}

// ValidateResourceTransformation validates ResourceTransformation CR status
func (c *Client) ValidateResourceTransformation(name string, namespace string, timeout, retryInterval time.Duration) error {
	if err := c.initClient(); err != nil {
		return err
	}
	t := func() (interface{}, bool, error) {
		transform, err := c.GetResourceTransformation(name, namespace)
		if err != nil {
			return "", true, err
		}

		if transform.Status.Status == storkv1alpha1.ResourceTransformationStatusReady {
			return "", false, nil
		} else if transform.Status.Status == storkv1alpha1.ResourceTransformationStatusFailed {
			return "", true, &errors.ErrFailedToValidateCustomSpec{
				Name:  name,
				Cause: fmt.Sprintf("Status: %v \t Resource Spec: %v", transform.Status.Status, transform.Status.Resources),
				Type:  transform,
			}
		}

		return "", true, &errors.ErrFailedToValidateCustomSpec{
			Name:  name,
			Cause: fmt.Sprintf("Status: %v", transform.Status.Status),
			Type:  transform,
		}
	}

	if _, err := task.DoRetryWithTimeout(t, timeout, retryInterval); err != nil {
		return err
	}

	return nil
}
