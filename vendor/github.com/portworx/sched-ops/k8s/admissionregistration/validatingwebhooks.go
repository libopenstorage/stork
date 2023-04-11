package admissionregistration

import (
	"context"

	hook "k8s.io/api/admissionregistration/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ValidatingWebhookConfigurationOps is an interface to perform CRUD ops on mutatting webhook controller
type ValidatingWebhookConfigurationOps interface {
	// GetValidatingWebhookConfiguration returns given ValidatingWebhookConfiguration
	GetValidatingWebhookConfiguration(name string) (*hook.ValidatingWebhookConfiguration, error)
	// CreateValidatingWebhookConfiguration creates given ValidatingWebhookConfiguration
	CreateValidatingWebhookConfiguration(cfg *hook.ValidatingWebhookConfiguration) (*hook.ValidatingWebhookConfiguration, error)
	// UpdateValidatingWebhookConfiguration updates given ValidatingWebhookConfiguration
	UpdateValidatingWebhookConfiguration(cfg *hook.ValidatingWebhookConfiguration) (*hook.ValidatingWebhookConfiguration, error)
	// DeleteValidatingWebhookConfiguration deletes given ValidatingWebhookConfiguration
	DeleteValidatingWebhookConfiguration(name string) error
}

// GetValidatingWebhookConfiguration returns given ValidatingWebhookConfiguration
func (c *Client) GetValidatingWebhookConfiguration(name string) (*hook.ValidatingWebhookConfiguration, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.admissionv1.ValidatingWebhookConfigurations().Get(context.TODO(), name, metav1.GetOptions{})
}

// CreateValidatingWebhookConfiguration creates given ValidatingWebhookConfiguration
func (c *Client) CreateValidatingWebhookConfiguration(cfg *hook.ValidatingWebhookConfiguration) (*hook.ValidatingWebhookConfiguration, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.admissionv1.ValidatingWebhookConfigurations().Create(context.TODO(), cfg, metav1.CreateOptions{})
}

// UpdateValidatingWebhookConfiguration updates given ValidatingWebhookConfiguration
func (c *Client) UpdateValidatingWebhookConfiguration(cfg *hook.ValidatingWebhookConfiguration) (*hook.ValidatingWebhookConfiguration, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.admissionv1.ValidatingWebhookConfigurations().Update(context.TODO(), cfg, metav1.UpdateOptions{})
}

// DeleteValidatingWebhookConfiguration deletes given ValidatingWebhookConfiguration
func (c *Client) DeleteValidatingWebhookConfiguration(name string) error {
	if err := c.initClient(); err != nil {
		return err
	}
	return c.admissionv1.ValidatingWebhookConfigurations().Delete(context.TODO(), name, metav1.DeleteOptions{})
}
