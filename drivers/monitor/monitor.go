package monitor

import (
	"fmt"
	"github.com/portworx/torpedo/pkg/log"

	"github.com/portworx/torpedo/pkg/errors"
	prometheus "github.com/prometheus/client_golang/prometheus"
)

type Driver interface {

	// Init initializes the monitor driver
	Init(string, string) error

	// String returns the string name of this driver.
	String() string

	// IncrementGaugeMetrics increment Gauge metrics
	IncrementGaugeMetric(metric *prometheus.GaugeVec, testName string)

	// DecrementGaugeMetrics decrement Gauge metrics
	DecrementGaugeMetric(metric *prometheus.GaugeVec, testName string)

	// IncrementCounterMetrics increment counter metrics
	IncrementCounterMetric(metric *prometheus.CounterVec, testName string)

	// IncrementGaugeMetricsUsingAdditionalLabel increment gauage metrics with additional labels
	IncrementGaugeMetricsUsingAdditionalLabel(metric *prometheus.GaugeVec, testName string, additionalLabels ...string)

	// SetGaugeMetrics set with value provided
	SetGaugeMetric(metric *prometheus.GaugeVec, value float64, testName string)

	// SetGaugeMetricWithNonDefaultLabels set value for guage metric
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
		log.GetLogInstance().Infof("Registering driver name: %s", name)
		monitor[name] = m
	} else {
		return fmt.Errorf("monitor driver: %s is already registered", name)
	}
	return nil
}
