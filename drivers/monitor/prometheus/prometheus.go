package prometheus

import (
	"github.com/portworx/torpedo/pkg/log"
	"net/http"

	"github.com/portworx/torpedo/drivers/monitor"
	prometheus "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Prom struct {
	port           string
	jobName        string
	jobType        string
	vCenterName    string
	vCenerCluster  string
	vCenterNetwork string
}

const (
	// DriverName is the name of the Prometheus monitor driver implementation
	DriverName = "prometheus"

	// DefaultMetricsPort is port used for exposing metrics
	DefaultMetricsPort = ":8181"
)

var (
	//metricsLabel
	metricsLabel = []string{
		// Test Name
		"test_name",

		// Torpedo Job Name
		"job_name",

		// Torpedo Job Type
		"job_type",
	}
)

// Define Torpedo metrics
var (
	// torpedoAlertTestFailed will be set when test failed
	TorpedoAlertTestFailed = AddGaugeMetric("torpedo_alert_test_failed", "Alert for torpedo test failed", "error_string")

	// torpedoTestRunning tells about test execution state
	TorpedoTestRunning = AddGaugeMetric("torpedo_test_running", "Torpedo test executing state ")

	// torpedoTestTriggerCount counter tells number of time test triggered
	TorpedoTestTotalTriggerCount = AddCounterMetric("torpedo_test_total_trigger_count", "Torpedo test total trigger count")

	// torpedoTestPassCount counter counts number of time test passed
	TorpedoTestPassCount = AddCounterMetric("torpedo_test_pass_count", "Torpedo test pass count")

	// torpedoTestFailCount counter counts number of time test fail
	TorpedoTestFailCount = AddCounterMetric("torpedo_test_fail_count", "Torpedo test fail count")
)

// AddGaugeMetrics adds GaugeVec metrics
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

// IncrementGaugeMetric increment Gauage Metrics
func (p *Prom) IncrementGaugeMetric(metric *prometheus.GaugeVec, testName string) {
	metric.WithLabelValues(testName, p.jobName, p.jobType).Inc()
}

// IncrementGaugeMetricsUsingAdditionalLabel increment Gauage Metrics
func (p *Prom) IncrementGaugeMetricsUsingAdditionalLabel(metric *prometheus.GaugeVec, testName string, additionalLabels ...string) {
	lvs := []string{testName, p.jobName, p.jobType}
	lvs = append(lvs, additionalLabels...)
	metric.WithLabelValues(lvs...).Inc()
}

// DecrementGaugeMetric decrement Gauage Metrics
func (p *Prom) DecrementGaugeMetric(metric *prometheus.GaugeVec, testName string) {
	metric.WithLabelValues(testName, p.jobName, p.jobType).Dec()
}

// IncrementCounterMetric increment Gauage Metrics
func (p *Prom) IncrementCounterMetric(metric *prometheus.CounterVec, testName string) {
	metric.WithLabelValues(testName, p.jobName, p.jobType).Inc()
}

// SetGaugeMetric set GaugeMetricVec value to given value
func (p *Prom) SetGaugeMetric(metric *prometheus.GaugeVec, value float64, testName string) {
	metric.WithLabelValues(testName, p.jobName, p.jobType).Set(value)
}

// SetGaugeMetric set GaugeMetricVec value to given value
func (p *Prom) SetGaugeMetricWithNonDefaultLabels(metric *prometheus.GaugeVec, value float64, testName string, additionalLabels ...string) {
	lvs := []string{testName, p.jobName, p.jobType}
	lvs = append(lvs, additionalLabels...)
	metric.WithLabelValues(lvs...).Set(value)
}

// String returns the string name of this driver.
func (p *Prom) String() string {
	return DriverName
}

// Init method init Prom object and start metrics handler
func (p *Prom) Init(jobName string, jobType string) error {
	p.port = DefaultMetricsPort
	p.jobName = jobName
	p.jobType = jobType
	http.Handle("/metrics", promhttp.Handler())
	log.Infof("Starting http service on port: %s", p.port)
	go http.ListenAndServe(p.port, nil)
	return nil
}

func init() {
	p := &Prom{}
	monitor.Register(DriverName, p)
}
