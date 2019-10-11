package k8s

import (
	monitoringv1 "github.com/coreos/prometheus-operator/pkg/apis/monitoring/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// PrometheusOps is an interface to perform Prometheus object operations
type PrometheusOps interface {
	ServiceMonitorOps
	PrometheusRuleOps
}

// ServiceMonitorOps is an interface to perform ServiceMonitor operations
type ServiceMonitorOps interface {
	// CreateServiceMonitor creates the given service monitor
	CreateServiceMonitor(*monitoringv1.ServiceMonitor) (*monitoringv1.ServiceMonitor, error)
	// UpdateServiceMonitor updates the given service monitor
	UpdateServiceMonitor(*monitoringv1.ServiceMonitor) (*monitoringv1.ServiceMonitor, error)
	// DeleteServiceMonitor deletes the given service monitor
	DeleteServiceMonitor(name, namespace string) error
}

// PrometheusRuleOps is an interface to perform PrometheusRule operations
type PrometheusRuleOps interface {
	// CreatePrometheusRule creates the given prometheus rule
	CreatePrometheusRule(*monitoringv1.PrometheusRule) (*monitoringv1.PrometheusRule, error)
	// UpdatePrometheusRule updates the given prometheus rule
	UpdatePrometheusRule(*monitoringv1.PrometheusRule) (*monitoringv1.PrometheusRule, error)
	// DeletePrometheusRule deletes the given prometheus rule
	DeletePrometheusRule(name, namespace string) error
}

func (k *k8sOps) CreateServiceMonitor(serviceMonitor *monitoringv1.ServiceMonitor) (*monitoringv1.ServiceMonitor, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	ns := serviceMonitor.Namespace
	if len(ns) == 0 {
		ns = v1.NamespaceDefault
	}

	return k.prometheusClient.MonitoringV1().ServiceMonitors(ns).Create(serviceMonitor)
}

func (k *k8sOps) UpdateServiceMonitor(serviceMonitor *monitoringv1.ServiceMonitor) (*monitoringv1.ServiceMonitor, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	return k.prometheusClient.MonitoringV1().ServiceMonitors(serviceMonitor.Namespace).Update(serviceMonitor)
}

func (k *k8sOps) DeleteServiceMonitor(name, namespace string) error {
	if err := k.initK8sClient(); err != nil {
		return err
	}

	return k.prometheusClient.MonitoringV1().ServiceMonitors(namespace).Delete(name, &metav1.DeleteOptions{
		PropagationPolicy: &deleteForegroundPolicy,
	})
}

func (k *k8sOps) CreatePrometheusRule(rule *monitoringv1.PrometheusRule) (*monitoringv1.PrometheusRule, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	ns := rule.Namespace
	if len(ns) == 0 {
		ns = v1.NamespaceDefault
	}

	return k.prometheusClient.MonitoringV1().PrometheusRules(ns).Create(rule)
}

func (k *k8sOps) UpdatePrometheusRule(rule *monitoringv1.PrometheusRule) (*monitoringv1.PrometheusRule, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	return k.prometheusClient.MonitoringV1().PrometheusRules(rule.Namespace).Update(rule)
}

func (k *k8sOps) DeletePrometheusRule(name, namespace string) error {
	if err := k.initK8sClient(); err != nil {
		return err
	}

	return k.prometheusClient.MonitoringV1().PrometheusRules(namespace).Delete(name, &metav1.DeleteOptions{
		PropagationPolicy: &deleteForegroundPolicy,
	})
}
