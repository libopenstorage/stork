package metrics

import (
	"fmt"

	stork_api "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/apimachinery/pkg/runtime"
)

var (
	// RestoreStatusCounter for application restore CR status on server
	restoreStatusCounter = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "application_restore_status",
		Help: "Status of application restores",
	}, []string{MetricName, MetricNamespace})
	// RestoreStageCounter for application restore CR stages on server
	restoreStageCounter = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "application_restore_stage",
		Help: "Stage of application restore",
	}, []string{MetricName, MetricNamespace})
	// RestoreDurationCounter for time taken by application restore to complete
	restoreDurationCounter = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "application_restore_duration",
		Help: "Duration of application restores",
	}, []string{MetricName, MetricNamespace})
	// RestoreSizeCounter for application restore size
	restoreSizeCounter = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "application_restore_size",
		Help: "Size of application restores",
	}, []string{MetricName, MetricNamespace})
)

var (
	// RestoreStatus map of application restore status to enum
	RestoreStatus = map[stork_api.ApplicationRestoreStatusType]float64{
		stork_api.ApplicationRestoreStatusInitial:        0,
		stork_api.ApplicationRestoreStatusPending:        1,
		stork_api.ApplicationRestoreStatusInProgress:     2,
		stork_api.ApplicationRestoreStatusFailed:         3,
		stork_api.ApplicationRestoreStatusPartialSuccess: 4,
		stork_api.ApplicationRestoreStatusRetained:       5,
		stork_api.ApplicationRestoreStatusSuccessful:     6,
	}

	// RestoreStage map of application restore stage to enum
	RestoreStage = map[stork_api.ApplicationRestoreStageType]float64{
		stork_api.ApplicationRestoreStageInitial:      0,
		stork_api.ApplicationRestoreStageVolumes:      1,
		stork_api.ApplicationRestoreStageApplications: 2,
		stork_api.ApplicationRestoreStageFinal:        3,
	}
)

func watchRestoreCR(object runtime.Object) error {
	restore, ok := object.(*stork_api.ApplicationRestore)
	if !ok {
		err := fmt.Errorf("invalid object type on restore watch: %v", object)
		return err
	}
	labels := make(prometheus.Labels)
	labels[MetricName] = restore.Name
	labels[MetricNamespace] = restore.Namespace
	if restore.DeletionTimestamp != nil {
		restoreStatusCounter.Delete(labels)
		restoreStageCounter.Delete(labels)
		restoreDurationCounter.Delete(labels)
		restoreSizeCounter.Delete(labels)
		return nil
	}
	// Set Restore Status counter
	restoreStatusCounter.With(labels).Set(RestoreStatus[restore.Status.Status])
	// Set Restore Stage Counter
	restoreStageCounter.With(labels).Set(RestoreStage[restore.Status.Stage])
	if restore.Status.Stage == stork_api.ApplicationRestoreStageFinal && (restore.Status.Status == stork_api.ApplicationRestoreStatusSuccessful ||
		restore.Status.Status == stork_api.ApplicationRestoreStatusPartialSuccess ||
		restore.Status.Status == stork_api.ApplicationRestoreStatusFailed) {
		st := restore.CreationTimestamp.Unix()
		et := restore.Status.FinishTimestamp.Unix()
		// Set Restore Duration
		restoreDurationCounter.With(labels).Set(float64(et - st))
		// Set Restore Size
		restoreSizeCounter.With(labels).Set(float64(restore.Status.TotalSize))
	}

	return nil
}

func init() {
	prometheus.MustRegister(restoreStatusCounter)
	prometheus.MustRegister(restoreStageCounter)
	prometheus.MustRegister(restoreDurationCounter)
	prometheus.MustRegister(restoreSizeCounter)
}
