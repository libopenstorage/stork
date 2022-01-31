package prometheus

import (
	"context"

	monitoringv1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// RuleOps is an interface to perform PrometheusRule operations
type RuleOps interface {
	// ListPrometheusRules creates the given prometheus rule
	ListPrometheusRules(namespace string) (*monitoringv1.PrometheusRuleList, error)
	// GetPrometheusRule gets the prometheus rule that matches the given name
	GetPrometheusRule(name, namespace string) (*monitoringv1.PrometheusRule, error)
	// CreatePrometheusRule creates the given prometheus rule
	CreatePrometheusRule(*monitoringv1.PrometheusRule) (*monitoringv1.PrometheusRule, error)
	// UpdatePrometheusRule updates the given prometheus rule
	UpdatePrometheusRule(*monitoringv1.PrometheusRule) (*monitoringv1.PrometheusRule, error)
	// DeletePrometheusRule deletes the given prometheus rule
	DeletePrometheusRule(name, namespace string) error
}

// ListPrometheusRules creates the given prometheus rule
func (c *Client) ListPrometheusRules(namespace string) (*monitoringv1.PrometheusRuleList, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}

	return c.prometheus.MonitoringV1().PrometheusRules(namespace).List(context.TODO(), metav1.ListOptions{})
}

// GetPrometheusRule gets the prometheus rule that matches the given name
func (c *Client) GetPrometheusRule(name string, namespace string) (*monitoringv1.PrometheusRule, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}

	return c.prometheus.MonitoringV1().PrometheusRules(namespace).Get(context.TODO(), name, metav1.GetOptions{})
}

// CreatePrometheusRule creates the given prometheus rule
func (c *Client) CreatePrometheusRule(rule *monitoringv1.PrometheusRule) (*monitoringv1.PrometheusRule, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}

	ns := rule.Namespace
	if len(ns) == 0 {
		ns = corev1.NamespaceDefault
	}

	return c.prometheus.MonitoringV1().PrometheusRules(ns).Create(context.TODO(), rule, metav1.CreateOptions{})
}

// UpdatePrometheusRule updates the given prometheus rule
func (c *Client) UpdatePrometheusRule(rule *monitoringv1.PrometheusRule) (*monitoringv1.PrometheusRule, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}

	return c.prometheus.MonitoringV1().PrometheusRules(rule.Namespace).Update(context.TODO(), rule, metav1.UpdateOptions{})
}

// DeletePrometheusRule deletes the given prometheus rule
func (c *Client) DeletePrometheusRule(name, namespace string) error {
	if err := c.initClient(); err != nil {
		return err
	}

	return c.prometheus.MonitoringV1().PrometheusRules(namespace).Delete(context.TODO(), name, metav1.DeleteOptions{
		PropagationPolicy: &deleteForegroundPolicy,
	})
}
