package prometheus

import (
	"context"

	monitoringv1alpha1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
)

// AlertManagerConfig adds receiver to alert manager which is responsible sending alerts
type AlertManagerConfigOps interface {
	// List of alertmanagerconfigs that match the namespace
	ListAlertManagerConfigs(namespace string, labelSelectors map[string]string) (*monitoringv1alpha1.AlertmanagerConfigList, error)
	// Get alertmanagerconfigs that matches the name
	GetAlertManagerConfig(name string, namespace string) (*monitoringv1alpha1.AlertmanagerConfig, error)
	// Create a new alertmanagerconfig
	CreateAlertManagerConfig(*monitoringv1alpha1.AlertmanagerConfig) (*monitoringv1alpha1.AlertmanagerConfig, error)
	// Update one alertmanagerconfig
	UpdateAlertManagerConfig(*monitoringv1alpha1.AlertmanagerConfig) (*monitoringv1alpha1.AlertmanagerConfig, error)
	// Delete one alertmanagerconfig
	DeleteAlertManagerConfig(name string, namespace string) error
	// Delete a list of alertmanagerconfig based on the namespace
	DeleteCollectionAlertManagerConfig(namespace string, labelSelectors map[string]string) error
}

// List of alert manager configs that match the namespace
func (c *Client) ListAlertManagerConfigs(namespace string, labelSelectors map[string]string) (*monitoringv1alpha1.AlertmanagerConfigList, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}

	return c.prometheus.MonitoringV1alpha1().AlertmanagerConfigs(namespace).List(context.TODO(), listOptions(labelSelectors))
}

// Get one alert manager config
func (c *Client) GetAlertManagerConfig(name string, namespace string) (*monitoringv1alpha1.AlertmanagerConfig, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}

	return c.prometheus.MonitoringV1alpha1().AlertmanagerConfigs(namespace).Get(context.TODO(), name, metav1.GetOptions{})
}

// Creates a new AlertmanagerConfig in the given namespace, provided the secret is provided
func (c *Client) CreateAlertManagerConfig(amc *monitoringv1alpha1.AlertmanagerConfig) (*monitoringv1alpha1.AlertmanagerConfig, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	ns := amc.Namespace
	if len(ns) == 0 {
		ns = corev1.NamespaceDefault
	}
	return c.prometheus.MonitoringV1alpha1().AlertmanagerConfigs(ns).Create(context.TODO(), amc, metav1.CreateOptions{})
}

// Updates one alert manager config
func (c *Client) UpdateAlertManagerConfig(amc *monitoringv1alpha1.AlertmanagerConfig) (*monitoringv1alpha1.AlertmanagerConfig, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.prometheus.MonitoringV1alpha1().AlertmanagerConfigs(amc.Namespace).Update(context.TODO(), amc, metav1.UpdateOptions{})
}

// Deletes single alert manager config
func (c *Client) DeleteAlertManagerConfig(name string, namespace string) error {
	if err := c.initClient(); err != nil {
		return err
	}

	return c.prometheus.MonitoringV1alpha1().AlertmanagerConfigs(namespace).Delete(context.TODO(), name, metav1.DeleteOptions{
		PropagationPolicy: &deleteForegroundPolicy,
	})
}

func (c *Client) DeleteCollectionAlertManagerConfig(namespace string, labelSelectors map[string]string) error {
	if err := c.initClient(); err != nil {
		return err
	}
	deleteOpts := metav1.DeleteOptions{
		PropagationPolicy: &deleteForegroundPolicy,
	}

	return c.prometheus.MonitoringV1alpha1().AlertmanagerConfigs(namespace).DeleteCollection(context.TODO(), deleteOpts, listOptions(labelSelectors))
}

func listOptions(labelSelectors map[string]string) metav1.ListOptions {
	if labelSelectors != nil {
		return metav1.ListOptions{
			LabelSelector: labels.Set(labelSelectors).String(),
		}
	}
	return metav1.ListOptions{}
}
