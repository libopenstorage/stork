package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	// HyperConvergedPodsCounter for pods hyper-converged by stork scheduler i.e scheduled on
	// node where replicas for all pod volumes exists
	HyperConvergedPodsCounter = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "hyperconverged_pods_total",
		Help: "The total number of pods hyperconverge by stork scheduler",
	}, []string{"pod", "namespace"})
	// NonHyperConvergePodsCounter for pods which are placed on driver node but not on node
	// where pod volume replicas exists
	NonHyperConvergePodsCounter = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "non_hyperconverged_pods_total",
		Help: "The total number of pods that are not hyper-converge by stork scheduler",
	}, []string{"pod", "namespace"})
	// SemiHyperConvergePodsCounter for pods which scheduled on node where replicas for all
	// volumes does not exists
	SemiHyperConvergePodsCounter = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "semi_hyperconverged_pods_total",
		Help: "The total number of pods that partially hyper-converge by stork scheduler",
	}, []string{"pod", "namespace"})
	// HealthCounter for pods which are rescheduled by stork monitor
	HealthCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "pods_reschduled_total",
		Help: "The total number of pods reschduler by stork pod monitor",
	})
)

//InitPublisher and handler for the metric
func init() {
	prometheus.MustRegister(HyperConvergedPodsCounter)
	prometheus.MustRegister(NonHyperConvergePodsCounter)
	prometheus.MustRegister(SemiHyperConvergePodsCounter)
	prometheus.MustRegister(HealthCounter)
}
