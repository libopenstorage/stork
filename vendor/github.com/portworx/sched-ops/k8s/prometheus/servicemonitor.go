package prometheus

import (
	"context"

	monitoringv1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ServiceMonitorOps is an interface to perform ServiceMonitor operations
type ServiceMonitorOps interface {
	// ListServiceMonitors lists all servicemonitors in a given namespace
	ListServiceMonitors(namespace string) (*monitoringv1.ServiceMonitorList, error)
	// GetServiceMonitor gets the service monitor instance that matches the given name
	GetServiceMonitor(name, namespace string) (*monitoringv1.ServiceMonitor, error)
	// CreateServiceMonitor creates the given service monitor
	CreateServiceMonitor(*monitoringv1.ServiceMonitor) (*monitoringv1.ServiceMonitor, error)
	// UpdateServiceMonitor updates the given service monitor
	UpdateServiceMonitor(*monitoringv1.ServiceMonitor) (*monitoringv1.ServiceMonitor, error)
	// DeleteServiceMonitor deletes the given service monitor
	DeleteServiceMonitor(name, namespace string) error
}

// ListServiceMonitors lists all servicemonitors in a given namespace
func (c *Client) ListServiceMonitors(namespace string) (*monitoringv1.ServiceMonitorList, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}

	return c.prometheus.MonitoringV1().ServiceMonitors(namespace).List(context.TODO(), metav1.ListOptions{})
}

// GetServiceMonitor gets the service monitor instance that matches the given name
func (c *Client) GetServiceMonitor(name string, namespace string) (*monitoringv1.ServiceMonitor, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}

	return c.prometheus.MonitoringV1().ServiceMonitors(namespace).Get(context.TODO(), name, metav1.GetOptions{})
}

// CreateServiceMonitor creates the given service monitor
func (c *Client) CreateServiceMonitor(serviceMonitor *monitoringv1.ServiceMonitor) (*monitoringv1.ServiceMonitor, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}

	ns := serviceMonitor.Namespace
	if len(ns) == 0 {
		ns = corev1.NamespaceDefault
	}

	return c.prometheus.MonitoringV1().ServiceMonitors(ns).Create(context.TODO(), serviceMonitor, metav1.CreateOptions{})
}

// UpdateServiceMonitor updates the given service monitor
func (c *Client) UpdateServiceMonitor(serviceMonitor *monitoringv1.ServiceMonitor) (*monitoringv1.ServiceMonitor, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}

	return c.prometheus.MonitoringV1().ServiceMonitors(serviceMonitor.Namespace).Update(context.TODO(), serviceMonitor, metav1.UpdateOptions{})
}

// DeleteServiceMonitor deletes the given service monitor
func (c *Client) DeleteServiceMonitor(name, namespace string) error {
	if err := c.initClient(); err != nil {
		return err
	}

	return c.prometheus.MonitoringV1().ServiceMonitors(namespace).Delete(context.TODO(), name, metav1.DeleteOptions{
		PropagationPolicy: &deleteForegroundPolicy,
	})
}
