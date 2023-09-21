package monitor

import (
	"fmt"
	"github.com/portworx/torpedo/pkg/errors"
	"github.com/portworx/torpedo/pkg/log"
	"github.com/prometheus/client_golang/prometheus"
)

type Driver interface {

	// Init initializes the monitor driver
	Init(string, string) error

	// String returns the string name of this driver.
	String() string

	// IncrementGaugeMetric increments gauge metrics
	IncrementGaugeMetric(metric *prometheus.GaugeVec, testName string)

	// DecrementGaugeMetric decrements gauge metrics
	DecrementGaugeMetric(metric *prometheus.GaugeVec, testName string)

	// IncrementCounterMetric increments counter metrics
	IncrementCounterMetric(metric *prometheus.CounterVec, testName string)

	// IncrementGaugeMetricsUsingAdditionalLabel increments gauge metrics using additional labels
	IncrementGaugeMetricsUsingAdditionalLabel(metric *prometheus.GaugeVec, testName string, additionalLabels ...string)

	// SetGaugeMetric sets gauge metrics to the value provided
	SetGaugeMetric(metric *prometheus.GaugeVec, value float64, testName string)

	// SetGaugeMetricWithNonDefaultLabels sets gauge metrics with non default labels to the value provided
	SetGaugeMetricWithNonDefaultLabels(metric *prometheus.GaugeVec, value float64, testName string, additionalLabels ...string)
}

var (
	monitor = make(map[string]Driver)
)

// Get returns a registered scheduler test provider.
func Get(name string) (Driver, error) {
	if m, ok := monitor[name]; ok {
		return m, nil
	}
	return nil, &errors.ErrNotFound{
		ID:   name,
		Type: "Monitor",
	}
}

// Register registers the given scheduler driver
func Register(name string, m Driver) error {
	if _, ok := monitor[name]; !ok {
		log.Infof("Registering driver name: %s", name)
		monitor[name] = m
	} else {
		return fmt.Errorf("monitor driver: %s is already registered", name)
	}
	return nil
}
