package admissionregistration

import (
	"context"

	hook "k8s.io/api/admissionregistration/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ValidatingWebhookConfigurationV1beta1Ops is interface to perform CRUD ops on mutatting webhook controller
type ValidatingWebhookConfigurationV1beta1Ops interface {
	// GetValidatingWebhookConfigurationV1beta1 returns given ValidatingWebhookConfiguration
	GetValidatingWebhookConfigurationV1beta1(name string) (*hook.ValidatingWebhookConfiguration, error)
	// CreateValidatingWebhookConfigurationV1beta1 creates given ValidatingWebhookConfiguration
	CreateValidatingWebhookConfigurationV1beta1(cfg *hook.ValidatingWebhookConfiguration) (*hook.ValidatingWebhookConfiguration, error)
	// UpdateValidatingWebhookConfigurationV1beta1 updates given ValidatingWebhookConfiguration
	UpdateValidatingWebhookConfigurationV1beta1(cfg *hook.ValidatingWebhookConfiguration) (*hook.ValidatingWebhookConfiguration, error)
	// DeleteValidatingWebhookConfigurationV1beta1 deletes given ValidatingWebhookConfiguration
	DeleteValidatingWebhookConfigurationV1beta1(name string) error
}

// GetValidatingWebhookConfigurationV1beta1 returns given ValidatingWebhookConfiguration
func (c *Client) GetValidatingWebhookConfigurationV1beta1(name string) (*hook.ValidatingWebhookConfiguration, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.admissionv1beta1.ValidatingWebhookConfigurations().Get(context.TODO(), name, metav1.GetOptions{})
}

// CreateValidatingWebhookConfigurationV1beta1 creates given ValidatingWebhookConfiguration
func (c *Client) CreateValidatingWebhookConfigurationV1beta1(cfg *hook.ValidatingWebhookConfiguration) (*hook.ValidatingWebhookConfiguration, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.admissionv1beta1.ValidatingWebhookConfigurations().Create(context.TODO(), cfg, metav1.CreateOptions{})
}

// UpdateValidatingWebhookConfigurationV1beta1 updates given ValidatingWebhookConfiguration
func (c *Client) UpdateValidatingWebhookConfigurationV1beta1(cfg *hook.ValidatingWebhookConfiguration) (*hook.ValidatingWebhookConfiguration, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.admissionv1beta1.ValidatingWebhookConfigurations().Update(context.TODO(), cfg, metav1.UpdateOptions{})
}

// DeleteValidatingWebhookConfigurationV1beta1 deletes given ValidatingWebhookConfiguration
func (c *Client) DeleteValidatingWebhookConfigurationV1beta1(name string) error {
	if err := c.initClient(); err != nil {
		return err
	}
	return c.admissionv1beta1.ValidatingWebhookConfigurations().Delete(context.TODO(), name, metav1.DeleteOptions{})
}
