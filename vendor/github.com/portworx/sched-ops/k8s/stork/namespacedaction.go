package stork

import (
	"context"

	storkv1alpha1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// NamespacedActionOps is an interface to manage NamespacedAction Object
type NamespacedActionOps interface {
	// CreateNamespacedAction creates a NamespacedAction
	CreateNamespacedAction(*storkv1alpha1.NamespacedAction) (*storkv1alpha1.NamespacedAction, error)
	// GetNamespacedAction gets the NamespacedAction
	GetNamespacedAction(string, string) (*storkv1alpha1.NamespacedAction, error)
	// ListNamespacedActions lists all the NamespacedActions
	ListNamespacedActions(namespace string, filterOptions metav1.ListOptions) (*storkv1alpha1.NamespacedActionList, error)
	// UpdateNamespacedAction updates the NamespacedAction
	UpdateNamespacedAction(*storkv1alpha1.NamespacedAction) (*storkv1alpha1.NamespacedAction, error)
	// DeleteNamespacedAction deletes the NamespacedAction
	DeleteNamespacedAction(string, string) error
}

// CreateNamespacedAction creates a NamespacedAction
func (c *Client) CreateNamespacedAction(namespacedAction *storkv1alpha1.NamespacedAction) (*storkv1alpha1.NamespacedAction, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.stork.StorkV1alpha1().NamespacedActions(namespacedAction.Namespace).Create(context.TODO(), namespacedAction, metav1.CreateOptions{})
}

// GetNamespacedAction gets the NamespacedAction
func (c *Client) GetNamespacedAction(name string, namespace string) (*storkv1alpha1.NamespacedAction, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.stork.StorkV1alpha1().NamespacedActions(namespace).Get(context.TODO(), name, metav1.GetOptions{})
}

// ListNamespacedActions lists all the NamespacedActions
func (c *Client) ListNamespacedActions(namespace string, filterOptions metav1.ListOptions) (*storkv1alpha1.NamespacedActionList, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.stork.StorkV1alpha1().NamespacedActions(namespace).List(context.TODO(), filterOptions)
}

// UpdateNamespacedAction updates the NamespacedAction
func (c *Client) UpdateNamespacedAction(namespacedAction *storkv1alpha1.NamespacedAction) (*storkv1alpha1.NamespacedAction, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.stork.StorkV1alpha1().NamespacedActions(namespacedAction.Namespace).Update(context.TODO(), namespacedAction, metav1.UpdateOptions{})
}

// DeleteNamespacedAction deletes the NamespacedAction
func (c *Client) DeleteNamespacedAction(name string, namespace string) error {
	if err := c.initClient(); err != nil {
		return err
	}
	return c.stork.StorkV1alpha1().NamespacedActions(namespace).Delete(context.TODO(), name, metav1.DeleteOptions{
		PropagationPolicy: &deleteForegroundPolicy,
	})
}
