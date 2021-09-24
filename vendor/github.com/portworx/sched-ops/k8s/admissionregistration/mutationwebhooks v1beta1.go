package admissionregistration

import (
	"context"

	hook "k8s.io/api/admissionregistration/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// MutatingWebhookConfigurationOps is interface to perform CRUD ops on mutatting webhook controller
type MutatingWebhookConfigurationV1beta1Ops interface {
	// GetMutatingWebhookConfigurationV1beta1 returns a given MutatingWebhookConfiguration
	GetMutatingWebhookConfigurationV1beta1(name string) (*hook.MutatingWebhookConfiguration, error)
	// CreateMutatingWebhookConfigurationV1beta1 creates given MutatingWebhookConfiguration
	CreateMutatingWebhookConfigurationV1beta1(req *hook.MutatingWebhookConfiguration) (*hook.MutatingWebhookConfiguration, error)
	// UpdateMutatingWebhookConfigurationV1beta1 updates given MutatingWebhookConfiguration
	UpdateMutatingWebhookConfigurationV1beta1(*hook.MutatingWebhookConfiguration) (*hook.MutatingWebhookConfiguration, error)
	// DeleteMutatingWebhookConfigurationV1beta1 deletes given MutatingWebhookConfiguration
	DeleteMutatingWebhookConfigurationV1beta1(name string) error
}

// GetMutatingWebhookConfiguration returns a given MutatingWebhookConfiguration
func (c *Client) GetMutatingWebhookConfigurationV1beta1(name string) (*hook.MutatingWebhookConfiguration, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.admission.MutatingWebhookConfigurations().Get(context.TODO(), name, metav1.GetOptions{})
}

// CreateMutatingWebhookConfiguration creates given MutatingWebhookConfiguration
func (c *Client) CreateMutatingWebhookConfigurationV1beta1(cfg *hook.MutatingWebhookConfiguration) (*hook.MutatingWebhookConfiguration, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.admission.MutatingWebhookConfigurations().Create(context.TODO(), cfg, metav1.CreateOptions{})
}

// UpdateMutatingWebhookConfiguration updates given MutatingWebhookConfiguration
func (c *Client) UpdateMutatingWebhookConfigurationV1beta1(cfg *hook.MutatingWebhookConfiguration) (*hook.MutatingWebhookConfiguration, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.admission.MutatingWebhookConfigurations().Update(context.TODO(), cfg, metav1.UpdateOptions{})
}

// DeleteMutatingWebhookConfiguration deletes given MutatingWebhookConfiguration
func (c *Client) DeleteMutatingWebhookConfigurationV1beta1(name string) error {
	if err := c.initClient(); err != nil {
		return err
	}
	return c.admission.MutatingWebhookConfigurations().Delete(context.TODO(), name, metav1.DeleteOptions{})
}
