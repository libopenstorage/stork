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

// ActionOps is an interface to manage Action Object
type ActionOps interface {
	// CreateAction creates a Action
	CreateAction(*storkv1alpha1.Action) (*storkv1alpha1.Action, error)
	// GetAction gets the Action
	GetAction(string, string) (*storkv1alpha1.Action, error)
	// ListActions lists all the Actions
	ListActions(namespace string) (*storkv1alpha1.ActionList, error)
	// UpdateAction updates the Action
	UpdateAction(*storkv1alpha1.Action) (*storkv1alpha1.Action, error)
	// DeleteAction deletes the Action
	DeleteAction(string, string) error
	// ValidateAction validates the Action
	ValidateAction(name string, namespace string, timeout, retryInterval time.Duration) error
}

// CreateAction creates a Action
func (c *Client) CreateAction(Action *storkv1alpha1.Action) (*storkv1alpha1.Action, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.stork.StorkV1alpha1().Actions(Action.Namespace).Create(context.TODO(), Action, metav1.CreateOptions{})
}

// GetAction gets the Action
func (c *Client) GetAction(name string, namespace string) (*storkv1alpha1.Action, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.stork.StorkV1alpha1().Actions(namespace).Get(context.TODO(), name, metav1.GetOptions{})
}

// ListActions lists all the Actions
func (c *Client) ListActions(namespace string) (*storkv1alpha1.ActionList, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.stork.StorkV1alpha1().Actions(namespace).List(context.TODO(), metav1.ListOptions{})
}

// UpdateAction updates the Action
func (c *Client) UpdateAction(Action *storkv1alpha1.Action) (*storkv1alpha1.Action, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.stork.StorkV1alpha1().Actions(Action.Namespace).Update(context.TODO(), Action, metav1.UpdateOptions{})
}

// DeleteAction deletes the Action
func (c *Client) DeleteAction(name string, namespace string) error {
	if err := c.initClient(); err != nil {
		return err
	}
	return c.stork.StorkV1alpha1().Actions(namespace).Delete(context.TODO(), name, metav1.DeleteOptions{
		PropagationPolicy: &deleteForegroundPolicy,
	})
}

// ValidateAction validate the Action status
func (c *Client) ValidateAction(name string, namespace string, timeout, retryInterval time.Duration) error {
	if err := c.initClient(); err != nil {
		return err
	}
	t := func() (interface{}, bool, error) {
		resp, err := c.GetAction(name, namespace)
		if err != nil {
			return "", true, err
		}

		if resp.Status == storkv1alpha1.ActionStatusSuccessful {
			return "", false, nil
		} else if resp.Status == storkv1alpha1.ActionStatusFailed {
			return "", false, &errors.ErrFailedToValidateCustomSpec{
				Name:  name,
				Cause: fmt.Sprintf("Action Status %v", resp.Status),
				Type:  resp,
			}
		}

		return "", true, &errors.ErrFailedToValidateCustomSpec{
			Name:  name,
			Cause: fmt.Sprintf("Action Status %v", resp.Status),
			Type:  resp,
		}
	}

	if _, err := task.DoRetryWithTimeout(t, timeout, retryInterval); err != nil {
		return err
	}

	return nil
}
