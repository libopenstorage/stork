package prometheus

import (
	"context"

	monitoringv1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// PodOps is an interface to perform Prometheus operations
type PodOps interface {
	// ListPrometheuses lists all prometheus instances in a given namespace
	ListPrometheuses(namespace string) (*monitoringv1.PrometheusList, error)
	// GetPrometheus gets the prometheus instance that matches the given name
	GetPrometheus(name, namespace string) (*monitoringv1.Prometheus, error)
	// CreatePrometheus creates the given prometheus
	CreatePrometheus(*monitoringv1.Prometheus) (*monitoringv1.Prometheus, error)
	// UpdatePrometheus updates the given prometheus
	UpdatePrometheus(*monitoringv1.Prometheus) (*monitoringv1.Prometheus, error)
	// DeletePrometheus deletes the given prometheus
	DeletePrometheus(name, namespace string) error
}

// ListPrometheuses lists all prometheus instances in a given namespace
func (c *Client) ListPrometheuses(namespace string) (*monitoringv1.PrometheusList, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}

	return c.prometheus.MonitoringV1().Prometheuses(namespace).List(context.TODO(), metav1.ListOptions{})
}

// GetPrometheus gets the prometheus instance that matches the given name
func (c *Client) GetPrometheus(name string, namespace string) (*monitoringv1.Prometheus, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}

	return c.prometheus.MonitoringV1().Prometheuses(namespace).Get(context.TODO(), name, metav1.GetOptions{})
}

// CreatePrometheus creates the given prometheus
func (c *Client) CreatePrometheus(prometheus *monitoringv1.Prometheus) (*monitoringv1.Prometheus, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}

	ns := prometheus.Namespace
	if len(ns) == 0 {
		ns = corev1.NamespaceDefault
	}

	return c.prometheus.MonitoringV1().Prometheuses(ns).Create(context.TODO(), prometheus, metav1.CreateOptions{})
}

// UpdatePrometheus updates the given prometheus
func (c *Client) UpdatePrometheus(prometheus *monitoringv1.Prometheus) (*monitoringv1.Prometheus, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}

	return c.prometheus.MonitoringV1().Prometheuses(prometheus.Namespace).Update(context.TODO(), prometheus, metav1.UpdateOptions{})
}

// DeletePrometheus deletes the given prometheus
func (c *Client) DeletePrometheus(name, namespace string) error {
	if err := c.initClient(); err != nil {
		return err
	}

	return c.prometheus.MonitoringV1().Prometheuses(namespace).Delete(context.TODO(), name, metav1.DeleteOptions{
		PropagationPolicy: &deleteForegroundPolicy,
	})
}
