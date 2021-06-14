package metrics

import (
	"fmt"

	stork_api "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/apimachinery/pkg/runtime"
)

var (
	volumeSnapshotScheduleStatusCounter = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "stork_volume_snapshotschedule_status",
		Help: "Status of volume snapshot schedules",
	}, []string{metricName, metricNamespace, metricPolicy})
)

func watchVolumeSnapshotScheduleCR(object runtime.Object) error {
	volumeSnapshotSchedule, ok := object.(*stork_api.VolumeSnapshotSchedule)
	if !ok {
		err := fmt.Errorf("invalid object type on volume snapshot schedule watch: %v", object)
		return err
	}
	labels := make(prometheus.Labels)
	labels[metricName] = volumeSnapshotSchedule.Name
	labels[metricNamespace] = volumeSnapshotSchedule.Namespace
	labels[metricPolicy] = volumeSnapshotSchedule.Spec.SchedulePolicyName
	suspend := volumeSnapshotSchedule.Spec.Suspend

	if volumeSnapshotSchedule.DeletionTimestamp != nil {
		volumeSnapshotScheduleStatusCounter.Delete(labels)
		return nil
	}
	if *suspend {
		volumeSnapshotScheduleStatusCounter.With(labels).Set(1)
	} else {
		volumeSnapshotScheduleStatusCounter.With(labels).Set(0)
	}
	return nil
}

func init() {
	prometheus.MustRegister(volumeSnapshotScheduleStatusCounter)
}
