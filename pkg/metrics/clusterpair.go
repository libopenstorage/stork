package metrics

import (
	"fmt"

	stork_api "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/apimachinery/pkg/runtime"
)

var (
	// clusterpairStatusCounter for clusterpair status
	clusterpairSchedStatusCounter = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "clusterpair_scheduler_status",
		Help: "Status of clusterpair",
	}, []string{MetricName, MetricNamespace})
	clusterpairStorageStatusCounter = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "clusterpair_storage_status",
		Help: "Status of clusterpair",
	}, []string{MetricName, MetricNamespace})
)

var (
	// ClusterpairStatus map of clusterpair status
	ClusterpairStatus = map[stork_api.ClusterPairStatusType]float64{
		stork_api.ClusterPairStatusInitial:     0,
		stork_api.ClusterPairStatusPending:     1,
		stork_api.ClusterPairStatusReady:       2,
		stork_api.ClusterPairStatusError:       3,
		stork_api.ClusterPairStatusDegraded:    4,
		stork_api.ClusterPairStatusDeleting:    5,
		stork_api.ClusterPairStatusNotProvided: 6,
	}
)

func watchclusterpairCR(object runtime.Object) error {
	clusterpair, ok := object.(*stork_api.ClusterPair)
	if !ok {
		err := fmt.Errorf("invalid object type on clusterpair watch: %v", object)
		return err
	}
	labels := make(prometheus.Labels)
	labels[MetricName] = clusterpair.Name
	labels[MetricNamespace] = clusterpair.Namespace
	if clusterpair.DeletionTimestamp != nil {
		clusterpairSchedStatusCounter.Delete(labels)
		clusterpairStorageStatusCounter.Delete(labels)
		return nil
	}
	// Set clusterpair Status counter
	clusterpairSchedStatusCounter.With(labels).Set(ClusterpairStatus[clusterpair.Status.SchedulerStatus])
	clusterpairStorageStatusCounter.With(labels).Set(ClusterpairStatus[clusterpair.Status.StorageStatus])
	return nil
}

func init() {
	prometheus.MustRegister(clusterpairSchedStatusCounter)
	prometheus.MustRegister(clusterpairStorageStatusCounter)
}
