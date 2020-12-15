// +build unittest

package metrics

import (
	"strconv"
	"testing"
	"time"

	storkv1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/portworx/sched-ops/k8s/stork"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func createMigration(name, ns string, status storkv1.MigrationStatusType, stage storkv1.MigrationStageType, vol, app int) (*storkv1.Migration, error) {
	migration := &storkv1.Migration{}
	migration.Name = name
	migration.Namespace = ns
	resp, err := stork.Instance().CreateMigration(migration)
	if err != nil {
		return nil, err
	}
	for i := 0; i < vol; i++ {
		vol := &storkv1.MigrationVolumeInfo{
			Volume:                "vol" + strconv.Itoa(i+1),
			PersistentVolumeClaim: "pvc" + strconv.Itoa(i+1),
		}
		resp.Status.Stage = stage
		resp.Status.Status = status
		resp.Status.Volumes = append(migration.Status.Volumes, vol)
	}
	updated, err := stork.Instance().UpdateMigration(resp)
	if err != nil {
		return nil, err
	}
	return updated, nil
}

func TestMigrationSuccessMetrics(t *testing.T) {
	defer resetTest()
	resp, err := createMigration("test", "test", storkv1.MigrationStatusInProgress, storkv1.MigrationStageVolumes, 2, 2)
	require.NoError(t, err)
	time.Sleep(3 * time.Second)

	// InProgress
	require.Equal(t, float64(MigrationStatus[storkv1.MigrationStatusInProgress]), testutil.ToFloat64(migrationStatusCounter), "migration_status does not matched")
	// Volume
	require.Equal(t, float64(MigrationStage[storkv1.MigrationStageVolumes]), testutil.ToFloat64(migrationStageCounter), "migration_stage does not matched")

	resp.Status.Stage = storkv1.MigrationStageFinal
	resp.Status.Status = storkv1.MigrationStatusSuccessful
	_, err = stork.Instance().UpdateMigration(resp)
	require.NoError(t, err)
	time.Sleep(3 * time.Second)
	// Successful
	require.Equal(t, float64(MigrationStatus[storkv1.MigrationStatusSuccessful]), testutil.ToFloat64(migrationStatusCounter), "migration_status does not matched")
	// Final
	require.Equal(t, float64(MigrationStage[storkv1.MigrationStageFinal]), testutil.ToFloat64(migrationStageCounter), "migration_stage does not matched")

	err = stork.Instance().DeleteMigration("test", "test")
	require.NoError(t, err)
}

func TestMigrationFailureMetrics(t *testing.T) {
	defer resetTest()
	_, err := createMigration("test", "test-fail", storkv1.MigrationStatusFailed, storkv1.MigrationStageInitial, 2, 2)
	require.NoError(t, err)
	time.Sleep(3 * time.Second)

	labels := make(prometheus.Labels)
	labels[MetricName] = "test"
	labels[MetricNamespace] = "test-fail"
	labels[MetricSchedule] = ""

	// Failure
	require.Equal(t, float64(MigrationStatus[storkv1.MigrationStatusFailed]), testutil.ToFloat64(migrationStatusCounter.With(labels)), "migration_status does not matched")
	// Initial
	require.Equal(t, float64(MigrationStage[storkv1.MigrationStageInitial]), testutil.ToFloat64(migrationStageCounter.With(labels)), "migration_stage does not matched")
}
