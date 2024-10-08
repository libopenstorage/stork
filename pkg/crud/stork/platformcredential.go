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

// PlatformCredentialOps is an interface to perform k8s PlatformCredential operations
type PlatformCredentialOps interface {
	// CreatePlatformCredential creates the PlatformCredential
	CreatePlatformCredential(*storkv1alpha1.PlatformCredential) (*storkv1alpha1.PlatformCredential, error)
	// GetPlatformCredential gets the PlatformCredential
	GetPlatformCredential(string, string) (*storkv1alpha1.PlatformCredential, error)
	// ListPlatformCredential lists all the PlatformCredential
	ListPlatformCredential(namespace string, filterOptions metav1.ListOptions) (*storkv1alpha1.PlatformCredentialList, error)
	// UpdatePlatformCredential updates the PlatformCredential
	UpdatePlatformCredential(*storkv1alpha1.PlatformCredential) (*storkv1alpha1.PlatformCredential, error)
	// DeletePlatformCredential deletes the PlatformCredential
	DeletePlatformCredential(string, string) error
	// ValidatePlatformCredential validates the PlatformCredential
	ValidatePlatformCredential(string, string, time.Duration, time.Duration) error
}

// CreatePlatformCredential creates the PlatformCredential
func (c *Client) CreatePlatformCredential(platformcredential *storkv1alpha1.PlatformCredential) (*storkv1alpha1.PlatformCredential, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.stork.StorkV1alpha1().PlatformCredentials(platformcredential.Namespace).Create(context.TODO(), platformcredential, metav1.CreateOptions{})
}

// GetPlatformCredential gets the PlatformCredential
func (c *Client) GetPlatformCredential(name string, namespace string) (*storkv1alpha1.PlatformCredential, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	platformCredential, err := c.stork.StorkV1alpha1().PlatformCredentials(namespace).Get(context.TODO(), name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}

	// TODO: use secrets/corev1 client instead of clientset
	err = platformCredential.UpdateFromSecret(c.kube)
	if err != nil {
		return nil, err
	}
	return platformCredential, nil
}

// ListPlatformCredential lists all the PlatformCredentials
func (c *Client) ListPlatformCredential(namespace string, filterOptions metav1.ListOptions) (*storkv1alpha1.PlatformCredentialList, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	platformCredentials, err := c.stork.StorkV1alpha1().PlatformCredentials(namespace).List(context.TODO(), filterOptions)
	if err != nil {
		return nil, err
	}
	for i := range platformCredentials.Items {
		err = platformCredentials.Items[i].UpdateFromSecret(c.kube)
		if err != nil {
			return nil, err
		}
	}
	return platformCredentials, nil
}

// UpdatePlatformCredential updates the PlatformCredential
func (c *Client) UpdatePlatformCredential(platformCredential *storkv1alpha1.PlatformCredential) (*storkv1alpha1.PlatformCredential, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.stork.StorkV1alpha1().PlatformCredentials(platformCredential.Namespace).Update(context.TODO(), platformCredential, metav1.UpdateOptions{})
}

// DeletePlatformCredential deletes the PlatformCredential
func (c *Client) DeletePlatformCredential(name string, namespace string) error {
	if err := c.initClient(); err != nil {
		return err
	}
	return c.stork.StorkV1alpha1().PlatformCredentials(namespace).Delete(context.TODO(), name, metav1.DeleteOptions{
		PropagationPolicy: &deleteForegroundPolicy,
	})
}

// ValidatePlatformCredential validates the platformCredential
func (c *Client) ValidatePlatformCredential(name, namespace string, timeout, retryInterval time.Duration) error {
	if err := c.initClient(); err != nil {
		return err
	}
	t := func() (interface{}, bool, error) {
		resp, err := c.GetPlatformCredential(name, namespace)
		if err != nil {
			return "", true, &errors.ErrFailedToValidateCustomSpec{
				Name:  name,
				Cause: fmt.Sprintf("PlatformCredential failed . Error: %v", err),
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
