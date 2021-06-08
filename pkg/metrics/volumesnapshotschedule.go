package metrics

import (
	"fmt"

	snapv1 "github.com/kubernetes-incubator/external-storage/snapshot/pkg/apis/crd/v1"
	stork_api "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	snapshotcontrollers "github.com/libopenstorage/stork/pkg/snapshot/controllers"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

var (
	volumeSnapshotScheduleStatusCounter = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "stork_volume_snapshotschedule_status",
		Help: "Status of volume snapshot schedules",
	}, []string{metricName, metricNamespace, metricPolicy})
	volumeSnapshotStatusCounter = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "stork_volume_snapshot_status",
		Help: "Status of volume snapshots",
	}, []string{metricName, metricNamespace, metricSchedule})
)

var (
	// volumeSnapshotStatus map of volume snapshot status to enum
	volumeSnapshotStatus = map[snapv1.VolumeSnapshotConditionType]float64{
		snapv1.VolumeSnapshotConditionReady:   0,
		snapv1.VolumeSnapshotConditionError:   1,
		snapv1.VolumeSnapshotConditionPending: 2,
	}
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

func watchVolumeSnapshotCR(object runtime.Object) error {
	logrus.Infof("testing, starting the watch")
	snapshot, ok := object.(*snapv1.VolumeSnapshot)
	if !ok {
		err := fmt.Errorf("invalid object type on volume snapshot watch: %v", object)
		return err
	}

	labels := make(prometheus.Labels)
	labels[metricName] = snapshot.Metadata.Name
	labels[metricNamespace] = snapshot.Metadata.Namespace
	sched := ""
	if snapshot.Metadata.Annotations != nil {
		sched = snapshot.Metadata.Annotations[snapshotcontrollers.SnapshotScheduleNameAnnotation]
	}
	labels[metricSchedule] = sched

	if snapshot.Metadata.DeletionTimestamp != nil {
		volumeSnapshotStatusCounter.Delete(labels)
		return nil
	}
	status, err := getVolumeSnapshotStatus(*snapshot)

	if err != nil {
		logrus.Infof("testing, error, %v", err)
		return err
	}

	logrus.Infof("testing, status: %v", status)

	volumeSnapshotStatusCounter.With(labels).Set(volumeSnapshotStatus[status])
	return nil
}

func getVolumeSnapshotStatus(snapshot snapv1.VolumeSnapshot) (snapv1.VolumeSnapshotConditionType, error) {
	if snapshot.Status.Conditions == nil || len(snapshot.Status.Conditions) == 0 {
		return snapv1.VolumeSnapshotConditionPending, nil
	}
	lastCondition := snapshot.Status.Conditions[len(snapshot.Status.Conditions)-1]
	if lastCondition.Type == snapv1.VolumeSnapshotConditionReady && lastCondition.Status == v1.ConditionTrue {
		return snapv1.VolumeSnapshotConditionReady, nil
	} else if lastCondition.Type == snapv1.VolumeSnapshotConditionError && lastCondition.Status == v1.ConditionTrue {
		return snapv1.VolumeSnapshotConditionError, nil
	} else if lastCondition.Type == snapv1.VolumeSnapshotConditionPending &&
		(lastCondition.Status == v1.ConditionTrue || lastCondition.Status == v1.ConditionUnknown) {
		return snapv1.VolumeSnapshotConditionPending, nil
	}
	return snapv1.VolumeSnapshotConditionPending, nil
}

func init() {
	prometheus.MustRegister(volumeSnapshotScheduleStatusCounter)
	prometheus.MustRegister(volumeSnapshotStatusCounter)
}
