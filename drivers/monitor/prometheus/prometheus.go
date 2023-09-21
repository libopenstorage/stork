package prometheus

import (
	"github.com/portworx/torpedo/pkg/log"
	"net/http"

	"github.com/portworx/torpedo/drivers/monitor"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Prom struct {
	port           string
	jobName        string
	jobType        string
	vCenterName    string
	vCenterCluster string
	vCenterNetwork string
}

const (
	// DriverName is the name of the Prometheus monitor driver implementation
	DriverName = "prometheus"

	// DefaultMetricsPort is the default port for exposing metrics
	DefaultMetricsPort = "8181"
)

var (
	// metricsLabel consists of predefined labels that are common across multiple metrics
	metricsLabel = []string{
		// Test Name
		"test_name",
		// Torpedo Job Name
		"job_name",
		// Torpedo Job Type
		"job_type",
	}
)

// Torpedo metrics
var (
	// TorpedoAlertTestFailed is a gauge metric that is set when a test fails
	TorpedoAlertTestFailed = AddGaugeMetric("torpedo_alert_test_failed", "Torpedo test failed alert", "error_string")

	// TorpedoTestRunning is a gauge metric that indicates the running state of a test
	TorpedoTestRunning = AddGaugeMetric("torpedo_test_running", "Torpedo test running state")

	// TorpedoTestTotalTriggerCount is a counter metric for the total number of test triggers
	TorpedoTestTotalTriggerCount = AddCounterMetric("torpedo_test_total_trigger_count", "Torpedo test total trigger count")

	// TorpedoTestPassCount is a counter metric for the total number of passed tests
	TorpedoTestPassCount = AddCounterMetric("torpedo_test_pass_count", "Torpedo test pass count")

	// TorpedoTestFailCount is a counter metric for the total number of failed tests
	TorpedoTestFailCount = AddCounterMetric("torpedo_test_fail_count", "Torpedo test fail count")
)

// AddGaugeMetric adds GaugeVec metrics
func AddGaugeMetric(name string, helpMessage string, additionalLabels ...string) *prometheus.GaugeVec {
	additionalLabels = append(metricsLabel, additionalLabels...)
	gauageMetrics := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: name,
			Help: helpMessage,
		},
		additionalLabels,
	)
	prometheus.MustRegister(gauageMetrics)
	return gauageMetrics
}

// AddCounterMetric adds CounterVec metrics
func AddCounterMetric(name string, helpMessage string, additionalLabels ...string) *prometheus.CounterVec {
	additionalLabels = append(metricsLabel, additionalLabels...)
	counterMetrics := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: name,
			Help: helpMessage,
		}, additionalLabels,
	)
	prometheus.MustRegister(counterMetrics)
	return counterMetrics
}

// IncrementGaugeMetric increments gauge metrics
func (p *Prom) IncrementGaugeMetric(metric *prometheus.GaugeVec, testName string) {
	metric.WithLabelValues(testName, p.jobName, p.jobType).Inc()
}

// IncrementGaugeMetricsUsingAdditionalLabel increments gauge metrics using additional labels
func (p *Prom) IncrementGaugeMetricsUsingAdditionalLabel(metric *prometheus.GaugeVec, testName string, additionalLabels ...string) {
	lvs := []string{testName, p.jobName, p.jobType}
	lvs = append(lvs, additionalLabels...)
	metric.WithLabelValues(lvs...).Inc()
}

// DecrementGaugeMetric decrements gauge metrics
func (p *Prom) DecrementGaugeMetric(metric *prometheus.GaugeVec, testName string) {
	metric.WithLabelValues(testName, p.jobName, p.jobType).Dec()
}

// IncrementCounterMetric increments counter metrics
func (p *Prom) IncrementCounterMetric(metric *prometheus.CounterVec, testName string) {
	metric.WithLabelValues(testName, p.jobName, p.jobType).Inc()
}

// SetGaugeMetric sets gauge metrics to the value provided
func (p *Prom) SetGaugeMetric(metric *prometheus.GaugeVec, value float64, testName string) {
	metric.WithLabelValues(testName, p.jobName, p.jobType).Set(value)
}

// SetGaugeMetricWithNonDefaultLabels sets gauge metrics with non default labels to the value provided
func (p *Prom) SetGaugeMetricWithNonDefaultLabels(metric *prometheus.GaugeVec, value float64, testName string, additionalLabels ...string) {
	lvs := []string{testName, p.jobName, p.jobType}
	lvs = append(lvs, additionalLabels...)
	metric.WithLabelValues(lvs...).Set(value)
}

// String returns the name of the Prometheus monitor driver implementation
func (p *Prom) String() string {
	return DriverName
}

// Init initializes Prom object and starts metrics handler
func (p *Prom) Init(jobName string, jobType string) error {
	if p.port == "" {
		p.port = DefaultMetricsPort
	}
	p.jobName = jobName
	p.jobType = jobType
	http.Handle("/metrics", promhttp.Handler())
	log.Infof("Starting http service for %s on port %s", DriverName, p.port)
	go func() {
		if err := http.ListenAndServe(":"+p.port, nil); err != nil {
			log.Errorf("Failed to start http service for %s on port %s due to %v", DriverName, p.port, err)
		}
	}()
	return nil
}

func init() {
	p := &Prom{}
	_ = monitor.Register(DriverName, p)
}
