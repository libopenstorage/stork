package admissionregistration

import (
	"context"

	hook "k8s.io/api/admissionregistration/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// MutatingWebhookConfigurationOps is interface to perform CRUD ops on mutatting webhook controller
type MutatingWebhookConfigurationOps interface {
	// GetMutatingWebhookConfiguration returns a given MutatingWebhookConfiguration
	GetMutatingWebhookConfiguration(name string) (*hook.MutatingWebhookConfiguration, error)
	// CreateMutatingWebhookConfiguration creates given MutatingWebhookConfiguration
	CreateMutatingWebhookConfiguration(req *hook.MutatingWebhookConfiguration) (*hook.MutatingWebhookConfiguration, error)
	// UpdateMutatingWebhookConfiguration updates given MutatingWebhookConfiguration
	UpdateMutatingWebhookConfiguration(*hook.MutatingWebhookConfiguration) (*hook.MutatingWebhookConfiguration, error)
	// DeleteMutatingWebhookConfiguration deletes given MutatingWebhookConfiguration
	DeleteMutatingWebhookConfiguration(name string) error
}

// GetMutatingWebhookConfiguration returns a given MutatingWebhookConfiguration
func (c *Client) GetMutatingWebhookConfiguration(name string) (*hook.MutatingWebhookConfiguration, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.admission.MutatingWebhookConfigurations().Get(context.TODO(), name, metav1.GetOptions{})
}

// CreateMutatingWebhookConfiguration creates given MutatingWebhookConfiguration
func (c *Client) CreateMutatingWebhookConfiguration(cfg *hook.MutatingWebhookConfiguration) (*hook.MutatingWebhookConfiguration, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.admission.MutatingWebhookConfigurations().Create(context.TODO(), cfg, metav1.CreateOptions{})
}

// UpdateMutatingWebhookConfiguration updates given MutatingWebhookConfiguration
func (c *Client) UpdateMutatingWebhookConfiguration(cfg *hook.MutatingWebhookConfiguration) (*hook.MutatingWebhookConfiguration, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.admission.MutatingWebhookConfigurations().Update(context.TODO(), cfg, metav1.UpdateOptions{})
}

// DeleteMutatingWebhookConfiguration deletes given MutatingWebhookConfiguration
func (c *Client) DeleteMutatingWebhookConfiguration(name string) error {
	if err := c.initClient(); err != nil {
		return err
	}
	return c.admission.MutatingWebhookConfigurations().Delete(context.TODO(), name, metav1.DeleteOptions{})
}
