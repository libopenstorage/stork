package prometheus

import (
	"context"

	monitoringv1alpha1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
)

// AlertManagerConfigOps adds receiver to alert manager which is responsible sending alerts
type AlertManagerConfigOps interface {
	// ListAlertManagerConfigs lists alertmanagerconfigs that match the namespace
	ListAlertManagerConfigs(namespace string, labelSelectors map[string]string) (*monitoringv1alpha1.AlertmanagerConfigList, error)
	// GetAlertManagerConfig get alertmanagerconfigs that matches the name
	GetAlertManagerConfig(name string, namespace string) (*monitoringv1alpha1.AlertmanagerConfig, error)
	// CreateAlertManagerConfig Create a new alertmanagerconfig
	CreateAlertManagerConfig(*monitoringv1alpha1.AlertmanagerConfig) (*monitoringv1alpha1.AlertmanagerConfig, error)
	//UpdateAlertManagerConfig  Update one alertmanagerconfig
	UpdateAlertManagerConfig(*monitoringv1alpha1.AlertmanagerConfig) (*monitoringv1alpha1.AlertmanagerConfig, error)
	// DeleteAlertManagerConfig Delete one alertmanagerconfigs
	DeleteAlertManagerConfig(name string, namespace string) error
	// DeleteCollectionAlertManagerConfig Delete a list of alertmanagerconfig based on the namespace
	DeleteCollectionAlertManagerConfig(namespace string, labelSelectors map[string]string) error
}

// ListAlertManagerConfigs  List of alert manager configs that match the namespace
func (c *Client) ListAlertManagerConfigs(namespace string, labelSelectors map[string]string) (*monitoringv1alpha1.AlertmanagerConfigList, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}

	return c.prometheus.MonitoringV1alpha1().AlertmanagerConfigs(namespace).List(context.TODO(), listOptions(labelSelectors))
}

// GetAlertManagerConfig Get one alert manager config
func (c *Client) GetAlertManagerConfig(name string, namespace string) (*monitoringv1alpha1.AlertmanagerConfig, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}

	return c.prometheus.MonitoringV1alpha1().AlertmanagerConfigs(namespace).Get(context.TODO(), name, metav1.GetOptions{})
}

// CreateAlertManagerConfig Creates a new AlertmanagerConfig in the given namespace, provided the secret is provided
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

// UpdateAlertManagerConfig  Updates one alert manager config
func (c *Client) UpdateAlertManagerConfig(amc *monitoringv1alpha1.AlertmanagerConfig) (*monitoringv1alpha1.AlertmanagerConfig, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.prometheus.MonitoringV1alpha1().AlertmanagerConfigs(amc.Namespace).Update(context.TODO(), amc, metav1.UpdateOptions{})
}

// DeleteAlertManagerConfig Deletes single alert manager config
func (c *Client) DeleteAlertManagerConfig(name string, namespace string) error {
	if err := c.initClient(); err != nil {
		return err
	}

	return c.prometheus.MonitoringV1alpha1().AlertmanagerConfigs(namespace).Delete(context.TODO(), name, metav1.DeleteOptions{
		PropagationPolicy: &deleteForegroundPolicy,
	})
}

// DeleteCollectionAlertManagerConfig Deletes a list of alert manager config based on the namespace
func (c *Client) DeleteCollectionAlertManagerConfig(namespace string, labelSelectors map[string]string) error {
	if err := c.initClient(); err != nil {
		return err
	}
	deleteOpts := metav1.DeleteOptions{
		PropagationPolicy: &deleteForegroundPolicy,
	}

	return c.prometheus.MonitoringV1alpha1().AlertmanagerConfigs(namespace).DeleteCollection(context.TODO(), deleteOpts, listOptions(labelSelectors))
}

// listOptions returns a list options with label selectors
func listOptions(labelSelectors map[string]string) metav1.ListOptions {
	if labelSelectors != nil {
		return metav1.ListOptions{
			LabelSelector: labels.Set(labelSelectors).String(),
		}
	}
	return metav1.ListOptions{}
}
