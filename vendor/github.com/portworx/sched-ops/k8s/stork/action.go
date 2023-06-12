package stork

import (
	"context"

	storkv1alpha1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
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
}

// CreateAction creates a Action
func (c *Client) CreateAction(action *storkv1alpha1.Action) (*storkv1alpha1.Action, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.stork.StorkV1alpha1().Actions(action.Namespace).Create(context.TODO(), action, metav1.CreateOptions{})
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
func (c *Client) UpdateAction(action *storkv1alpha1.Action) (*storkv1alpha1.Action, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.stork.StorkV1alpha1().Actions(action.Namespace).Update(context.TODO(), action, metav1.UpdateOptions{})
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
