package metrics

import (
	"fmt"

	stork_api "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/apimachinery/pkg/runtime"
)

var (
	// migrationStatusCounter for migration status
	migrationStatusCounter = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "migration_status",
		Help: "Status of migration",
	}, []string{metricName, metricNamespace, metricSchedule})
	// migrationStageCounter for migration CR stages on server
	migrationStageCounter = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "migration_stage",
		Help: "Stage of migration",
	}, []string{metricName, metricNamespace, metricSchedule})
	// migrationDurationCounter for time taken by migration to complete
	migrationDurationCounter = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "migration_duration",
		Help: "Duration of migrations",
	}, []string{metricName, metricNamespace, metricSchedule})
	// migrationScheduleCounter for migration schedule status
	migrationScheduleCounter = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "migration_schedule_status",
		Help: "Status of migration schedules",
	}, []string{metricName, metricNamespace})
)

var (
	// migrationStatus map of application migration status to enum
	migrationStatus = map[stork_api.MigrationStatusType]float64{
		stork_api.MigrationStatusInitial:        0,
		stork_api.MigrationStatusPending:        1,
		stork_api.MigrationStatusInProgress:     2,
		stork_api.MigrationStatusFailed:         3,
		stork_api.MigrationStatusPartialSuccess: 4,
		stork_api.MigrationStatusSuccessful:     5,
		stork_api.MigrationStatusPurged:         6,
	}

	// migrationStage map of application migration stage to enum
	migrationStage = map[stork_api.MigrationStageType]float64{
		stork_api.MigrationStageInitial:      0,
		stork_api.MigrationStagePreExecRule:  1,
		stork_api.MigrationStagePostExecRule: 2,
		stork_api.MigrationStageVolumes:      3,
		stork_api.MigrationStageApplications: 4,
		stork_api.MigrationStageFinal:        5,
	}
)

func watchmigrationCR(object runtime.Object) error {
	migration, ok := object.(*stork_api.Migration)
	if !ok {
		err := fmt.Errorf("invalid object type on migration watch: %v", object)
		return err
	}
	labels := make(prometheus.Labels)
	labels[metricName] = migration.Name
	labels[metricNamespace] = migration.Namespace
	sched := ""
	for _, v := range migration.OwnerReferences {
		sched = v.Name
	}
	labels[metricSchedule] = sched

	if migration.DeletionTimestamp != nil {
		migrationStatusCounter.Delete(labels)
		migrationStageCounter.Delete(labels)
		migrationDurationCounter.Delete(labels)
		return nil
	}
	// Set migration Status counter
	migrationStatusCounter.With(labels).Set(migrationStatus[migration.Status.Status])
	// Set migration Stage Counter
	migrationStageCounter.With(labels).Set(migrationStage[migration.Status.Stage])
	if migration.Status.Stage == stork_api.MigrationStageFinal && (migration.Status.Status == stork_api.MigrationStatusSuccessful ||
		migration.Status.Status == stork_api.MigrationStatusPartialSuccess ||
		migration.Status.Status == stork_api.MigrationStatusFailed) {
		st := migration.CreationTimestamp.Unix()
		et := migration.Status.FinishTimestamp.Unix()
		// Set migration Duration
		migrationDurationCounter.With(labels).Set(float64(et - st))
	}

	return nil
}

func watchmigrationScheduleCR(object runtime.Object) error {
	migrSched, ok := object.(*stork_api.MigrationSchedule)
	if !ok {
		err := fmt.Errorf("invalid object type on migration schedule watch: %v", object)
		return err
	}
	labels := make(prometheus.Labels)
	labels[metricName] = migrSched.Name
	labels[metricNamespace] = migrSched.Namespace

	if migrSched.DeletionTimestamp != nil {
		migrationScheduleCounter.Delete(labels)
		return nil
	}
	// Set migration schedule counter
	// TODO: should we set status of migration schedule here suspend/resume here ?
	migrationScheduleCounter.With(labels).Set(float64(len(migrSched.Status.Items)))
	return nil
}

func init() {
	prometheus.MustRegister(migrationStatusCounter)
	prometheus.MustRegister(migrationStageCounter)
	prometheus.MustRegister(migrationDurationCounter)
	prometheus.MustRegister(migrationScheduleCounter)
}
