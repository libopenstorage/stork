package stork

import (
	"fmt"
	"time"

	storkv1alpha1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/portworx/sched-ops/k8s/errors"
	"github.com/portworx/sched-ops/task"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ApplicationRegistrationOps is an interface to perform k8s ApplicationRegistration operations
type ApplicationRegistrationOps interface {
	// CreateApplicationRegistration creates the ApplicationRegistration
	CreateApplicationRegistration(*storkv1alpha1.ApplicationRegistration) (*storkv1alpha1.ApplicationRegistration, error)
	// GetApplicationRegistration gets the ApplicationRegistration
	GetApplicationRegistration(string) (*storkv1alpha1.ApplicationRegistration, error)
	// ListApplicationRegistrations lists all the ApplicationRegistrations
	ListApplicationRegistrations() (*storkv1alpha1.ApplicationRegistrationList, error)
	// UpdateApplicationRegistration updates the ApplicationRegistration
	UpdateApplicationRegistration(*storkv1alpha1.ApplicationRegistration) (*storkv1alpha1.ApplicationRegistration, error)
	// DeleteApplicationRegistration deletes the ApplicationRegistration
	DeleteApplicationRegistration(string) error
	// ValidateApplicationRegistration validates the ApplicationRegistration
	ValidateApplicationRegistration(string, time.Duration, time.Duration) error
}

// CreateApplicationRegistration creates the ApplicationRegistration
func (c *Client) CreateApplicationRegistration(ApplicationRegistration *storkv1alpha1.ApplicationRegistration) (*storkv1alpha1.ApplicationRegistration, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.stork.StorkV1alpha1().ApplicationRegistrations().Create(ApplicationRegistration)
}

// GetApplicationRegistration gets the ApplicationRegistration
func (c *Client) GetApplicationRegistration(name string) (*storkv1alpha1.ApplicationRegistration, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.stork.StorkV1alpha1().ApplicationRegistrations().Get(name, metav1.GetOptions{})
}

// ListApplicationRegistrations lists all the ApplicationRegistrations
func (c *Client) ListApplicationRegistrations() (*storkv1alpha1.ApplicationRegistrationList, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.stork.StorkV1alpha1().ApplicationRegistrations().List(metav1.ListOptions{})

}

// UpdateApplicationRegistration updates the ApplicationRegistration
func (c *Client) UpdateApplicationRegistration(ApplicationRegistration *storkv1alpha1.ApplicationRegistration) (*storkv1alpha1.ApplicationRegistration, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.stork.StorkV1alpha1().ApplicationRegistrations().Update(ApplicationRegistration)
}

// DeleteApplicationRegistration deletes the ApplicationRegistration
func (c *Client) DeleteApplicationRegistration(name string) error {
	if err := c.initClient(); err != nil {
		return err
	}
	return c.stork.StorkV1alpha1().ApplicationRegistrations().Delete(name, &metav1.DeleteOptions{
		PropagationPolicy: &deleteForegroundPolicy,
	})
}

// ValidateApplicationRegistration validates the ApplicationRegistration
func (c *Client) ValidateApplicationRegistration(name string, timeout, retryInterval time.Duration) error {
	if err := c.initClient(); err != nil {
		return err
	}
	t := func() (interface{}, bool, error) {
		resp, err := c.GetApplicationRegistration(name)
		if err != nil {
			return "", true, &errors.ErrFailedToValidateCustomSpec{
				Name:  name,
				Cause: fmt.Sprintf("ApplicationRegistration failed . Error: %v", err),
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
