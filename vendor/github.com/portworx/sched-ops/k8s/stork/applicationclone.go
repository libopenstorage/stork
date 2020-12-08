package stork

import (
	"fmt"
	"time"

	storkv1alpha1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/portworx/sched-ops/k8s/errors"
	"github.com/portworx/sched-ops/task"
	"github.com/sirupsen/logrus"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ApplicationCloneOps is an interface to perform k8s Application Clone operations
type ApplicationCloneOps interface {
	// CreateApplicationClone creates the ApplicationClone
	CreateApplicationClone(*storkv1alpha1.ApplicationClone) (*storkv1alpha1.ApplicationClone, error)
	// GetApplicationClone gets the ApplicationClone
	GetApplicationClone(string, string) (*storkv1alpha1.ApplicationClone, error)
	// ListApplicationClones lists all the ApplicationClones
	ListApplicationClones(string) (*storkv1alpha1.ApplicationCloneList, error)
	// UpdateApplicationClone updates the ApplicationClone
	UpdateApplicationClone(*storkv1alpha1.ApplicationClone) (*storkv1alpha1.ApplicationClone, error)
	// DeleteApplicationClone deletes the ApplicationClone
	DeleteApplicationClone(string, string) error
	// ValidateApplicationClone validates the ApplicationClone
	ValidateApplicationClone(string, string, time.Duration, time.Duration) error
	// WatchApplicationClone watch the ApplicationClone
	WatchApplicationClone(namespace string, fn WatchFunc, listOptions metav1.ListOptions) error
}

// CreateApplicationClone creates the ApplicationClone
func (c *Client) CreateApplicationClone(clone *storkv1alpha1.ApplicationClone) (*storkv1alpha1.ApplicationClone, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.stork.StorkV1alpha1().ApplicationClones(clone.Namespace).Create(clone)
}

// GetApplicationClone gets the ApplicationClone
func (c *Client) GetApplicationClone(name string, namespace string) (*storkv1alpha1.ApplicationClone, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.stork.StorkV1alpha1().ApplicationClones(namespace).Get(name, metav1.GetOptions{})
}

// ListApplicationClones lists all the ApplicationClones
func (c *Client) ListApplicationClones(namespace string) (*storkv1alpha1.ApplicationCloneList, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.stork.StorkV1alpha1().ApplicationClones(namespace).List(metav1.ListOptions{})
}

// DeleteApplicationClone deletes the ApplicationClone
func (c *Client) DeleteApplicationClone(name string, namespace string) error {
	if err := c.initClient(); err != nil {
		return err
	}
	return c.stork.StorkV1alpha1().ApplicationClones(namespace).Delete(name, &metav1.DeleteOptions{
		PropagationPolicy: &deleteForegroundPolicy,
	})
}

// UpdateApplicationClone updates the ApplicationClone
func (c *Client) UpdateApplicationClone(clone *storkv1alpha1.ApplicationClone) (*storkv1alpha1.ApplicationClone, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.stork.StorkV1alpha1().ApplicationClones(clone.Namespace).Update(clone)
}

// ValidateApplicationClone validates the ApplicationClone
func (c *Client) ValidateApplicationClone(name, namespace string, timeout, retryInterval time.Duration) error {
	if err := c.initClient(); err != nil {
		return err
	}
	t := func() (interface{}, bool, error) {
		applicationclone, err := c.stork.StorkV1alpha1().ApplicationClones(namespace).Get(name, metav1.GetOptions{})
		if err != nil {
			return "", true, err
		}

		if applicationclone.Status.Status == storkv1alpha1.ApplicationCloneStatusSuccessful {
			return "", false, nil
		}
		return "", true, &errors.ErrFailedToValidateCustomSpec{
			Name:  applicationclone.Name,
			Cause: fmt.Sprintf("Application Clone failed . Error: %v .Expected status: %v Actual status: %v", err, storkv1alpha1.ApplicationCloneStatusSuccessful, applicationclone.Status.Status),
			Type:  applicationclone,
		}
	}
	if _, err := task.DoRetryWithTimeout(t, timeout, retryInterval); err != nil {
		return err
	}
	return nil
}

// WatchApplicationClone sets up a watcher that listens for changes on application backups
func (c *Client) WatchApplicationClone(namespace string, fn WatchFunc, listOptions metav1.ListOptions) error {
	if err := c.initClient(); err != nil {
		return err
	}

	listOptions.Watch = true
	watchInterface, err := c.stork.StorkV1alpha1().ApplicationClones(namespace).Watch(listOptions)
	if err != nil {
		logrus.WithError(err).Error("error invoking the watch api for application clones")
		return err
	}

	// fire off watch function
	go c.handleWatch(watchInterface, &storkv1alpha1.ApplicationClone{}, "", fn, listOptions)
	return nil
}
