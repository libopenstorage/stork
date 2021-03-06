package metrics

import (
	"fmt"

	stork_api "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	app_backup "github.com/libopenstorage/stork/pkg/applicationmanager/controllers"
	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/apimachinery/pkg/runtime"
)

var (
	// backupStatusCounter for application backup CR status on server
	backupStatusCounter = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "stork_application_backup_status",
		Help: "Status of application backups",
	}, []string{metricName, metricNamespace, metricSchedule})
	// backupStageCounter for application backup CR stages on server
	backupStageCounter = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "stork_application_backup_stage",
		Help: "Stage of application backups",
	}, []string{metricName, metricNamespace, metricSchedule})
	// backupDurationCounter for time taken by application backup to complete
	backupDurationCounter = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "stork_application_backup_duration",
		Help: "Duration of application backups",
	}, []string{metricName, metricNamespace, metricSchedule})
	// backupSizeCounter for application backup size
	backupSizeCounter = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "stork_application_backup_size",
		Help: "Size of application backups",
	}, []string{metricName, metricNamespace, metricSchedule})
	// backupScheduleStatusCounter for application backup schedule CR status on server
	backupScheduleStatusCounter = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "stork_application_backup_schedule_status",
		Help: "Status of application backup Schedules",
	}, []string{metricName, metricNamespace})
)

var (
	// backupStatus map of application backup status to enum
	backupStatus = map[stork_api.ApplicationBackupStatusType]float64{
		stork_api.ApplicationBackupStatusInitial:        0,
		stork_api.ApplicationBackupStatusPending:        1,
		stork_api.ApplicationBackupStatusInProgress:     2,
		stork_api.ApplicationBackupStatusFailed:         3,
		stork_api.ApplicationBackupStatusPartialSuccess: 4,
		stork_api.ApplicationBackupStatusSuccessful:     5,
	}
	// backupStage map of application backup stage to enum
	backupStage = map[stork_api.ApplicationBackupStageType]float64{
		stork_api.ApplicationBackupStageInitial:      0,
		stork_api.ApplicationBackupStagePreExecRule:  1,
		stork_api.ApplicationBackupStagePostExecRule: 2,
		stork_api.ApplicationBackupStageVolumes:      3,
		stork_api.ApplicationBackupStageApplications: 4,
		stork_api.ApplicationBackupStageFinal:        5,
	}
)

func watchBackupCR(object runtime.Object) error {
	backup, ok := object.(*stork_api.ApplicationBackup)
	if !ok {
		err := fmt.Errorf("invalid object type on backup watch: %v", object)
		return err
	}
	labels := make(prometheus.Labels)
	labels[metricName] = backup.Name
	labels[metricNamespace] = backup.Namespace
	sched := ""
	if backup.Annotations != nil {
		sched = backup.Annotations[app_backup.ApplicationBackupScheduleNameAnnotation]
	}
	labels[metricSchedule] = sched
	if backup.DeletionTimestamp != nil {
		backupStatusCounter.Delete(labels)
		backupStageCounter.Delete(labels)
		backupDurationCounter.Delete(labels)
		backupSizeCounter.Delete(labels)
		return nil
	}
	// Set Backup Status counter
	backupStatusCounter.With(labels).Set(backupStatus[backup.Status.Status])
	// Set Backup Stage Counter
	backupStageCounter.With(labels).Set(backupStage[backup.Status.Stage])
	if backup.Status.Stage == stork_api.ApplicationBackupStageFinal && (backup.Status.Status == stork_api.ApplicationBackupStatusSuccessful ||
		backup.Status.Status == stork_api.ApplicationBackupStatusPartialSuccess ||
		backup.Status.Status == stork_api.ApplicationBackupStatusFailed) {
		st := backup.Status.TriggerTimestamp.Unix()
		et := backup.Status.FinishTimestamp.Unix()
		// Set backup Duration
		backupDurationCounter.With(labels).Set(float64(et - st))
		// Set BackupSize
		backupSizeCounter.With(labels).Set(float64(backup.Status.TotalSize))
	}

	return nil
}

func watchBackupScheduleCR(object runtime.Object) error {
	bkpSched, ok := object.(*stork_api.ApplicationBackupSchedule)
	if !ok {
		err := fmt.Errorf("invalid object type on backup schedule watch: %v", object)
		return err
	}
	labels := make(prometheus.Labels)
	labels[metricName] = bkpSched.Name
	labels[metricNamespace] = bkpSched.Namespace
	if bkpSched.DeletionTimestamp != nil {
		backupScheduleStatusCounter.Delete(labels)
		return nil
	}
	// Set Backup Schedule Status counter
	backupScheduleStatusCounter.With(labels).Set(float64(len(bkpSched.Status.Items)))
	return nil
}

func init() {
	prometheus.MustRegister(backupStatusCounter)
	prometheus.MustRegister(backupStageCounter)
	prometheus.MustRegister(backupDurationCounter)
	prometheus.MustRegister(backupSizeCounter)
	prometheus.MustRegister(backupScheduleStatusCounter)
}
