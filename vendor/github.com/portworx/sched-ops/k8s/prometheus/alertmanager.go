package prometheus

import (
	"context"

	monitoringv1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// AlertManagerOps is an interface to perform AlertManager operations
type AlertManagerOps interface {
	// ListAlertManagers lists all alertmanager instances in a given namespace
	ListAlertManagers(namespace string) (*monitoringv1.AlertmanagerList, error)
	// GetAlertManager gets the alert manager that matches the given name
	GetAlertManager(name, namespace string) (*monitoringv1.Alertmanager, error)
	// CreateAlertManager creates the given alert manager
	CreateAlertManager(*monitoringv1.Alertmanager) (*monitoringv1.Alertmanager, error)
	// UpdateAlertManager updates the given alert manager
	UpdateAlertManager(*monitoringv1.Alertmanager) (*monitoringv1.Alertmanager, error)
	// DeleteAlertManager deletes the given alert manager
	DeleteAlertManager(name, namespace string) error
}

// ListAlertManagers lists all alertmanager instances in a given namespace
func (c *Client) ListAlertManagers(namespace string) (*monitoringv1.AlertmanagerList, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}

	return c.prometheus.MonitoringV1().Alertmanagers(namespace).List(context.TODO(), metav1.ListOptions{})
}

// GetAlertManager gets the alert manager that matches the given name
func (c *Client) GetAlertManager(name string, namespace string) (*monitoringv1.Alertmanager, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}

	return c.prometheus.MonitoringV1().Alertmanagers(namespace).Get(context.TODO(), name, metav1.GetOptions{})
}

// CreateAlertManager creates the given alert manager
func (c *Client) CreateAlertManager(alertmanager *monitoringv1.Alertmanager) (*monitoringv1.Alertmanager, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}

	ns := alertmanager.Namespace
	if len(ns) == 0 {
		ns = corev1.NamespaceDefault
	}

	return c.prometheus.MonitoringV1().Alertmanagers(ns).Create(context.TODO(), alertmanager, metav1.CreateOptions{})
}

// UpdateAlertManager updates the given alert manager
func (c *Client) UpdateAlertManager(alertmanager *monitoringv1.Alertmanager) (*monitoringv1.Alertmanager, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}

	return c.prometheus.MonitoringV1().Alertmanagers(alertmanager.Namespace).Update(context.TODO(), alertmanager, metav1.UpdateOptions{})
}

// DeleteAlertManager deletes the given alert manager
func (c *Client) DeleteAlertManager(name, namespace string) error {
	if err := c.initClient(); err != nil {
		return err
	}

	return c.prometheus.MonitoringV1().Alertmanagers(namespace).Delete(context.TODO(), name, metav1.DeleteOptions{
		PropagationPolicy: &deleteForegroundPolicy,
	})
}
