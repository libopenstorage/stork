package stork

import (
	storkv1alpha1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// RuleOps is an interface to perform operations for k8s stork rule
type RuleOps interface {
	// GetRule fetches the given stork rule
	GetRule(name, namespace string) (*storkv1alpha1.Rule, error)
	// CreateRule creates the given stork rule
	CreateRule(rule *storkv1alpha1.Rule) (*storkv1alpha1.Rule, error)
	// UpdateRule updates the given stork rule
	UpdateRule(rule *storkv1alpha1.Rule) (*storkv1alpha1.Rule, error)
	// DeleteRule deletes the given stork rule
	DeleteRule(name, namespace string) error
}

// GetRule fetches the given stork rule
func (c *Client) GetRule(name, namespace string) (*storkv1alpha1.Rule, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.stork.StorkV1alpha1().Rules(namespace).Get(name, metav1.GetOptions{})
}

// CreateRule creates the given stork rule
func (c *Client) CreateRule(rule *storkv1alpha1.Rule) (*storkv1alpha1.Rule, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.stork.StorkV1alpha1().Rules(rule.GetNamespace()).Create(rule)
}

// UpdateRule updates the given stork rule
func (c *Client) UpdateRule(rule *storkv1alpha1.Rule) (*storkv1alpha1.Rule, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.stork.StorkV1alpha1().Rules(rule.GetNamespace()).Update(rule)
}

// DeleteRule deletes the given stork rule
func (c *Client) DeleteRule(name, namespace string) error {
	if err := c.initClient(); err != nil {
		return err
	}
	return c.stork.StorkV1alpha1().Rules(namespace).Delete(name, &metav1.DeleteOptions{
		PropagationPolicy: &deleteForegroundPolicy,
	})
}
