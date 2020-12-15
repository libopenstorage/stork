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

func createApplicationBackup(name, ns string, status storkv1.ApplicationBackupStatusType, stage storkv1.ApplicationBackupStageType, vol, app int) (*storkv1.ApplicationBackup, error) {
	backup := &storkv1.ApplicationBackup{}
	backup.Name = name
	backup.Namespace = ns
	resp, err := stork.Instance().CreateApplicationBackup(backup)
	if err != nil {
		return nil, err
	}
	for i := 0; i < vol; i++ {
		vol := &storkv1.ApplicationBackupVolumeInfo{
			Volume:                "vol" + strconv.Itoa(i+1),
			PersistentVolumeClaim: "pvc" + strconv.Itoa(i+1),
		}
		resp.Status.Stage = stage
		resp.Status.Status = status
		resp.Status.Volumes = append(backup.Status.Volumes, vol)
	}
	updated, err := stork.Instance().UpdateApplicationBackup(resp)
	if err != nil {
		return nil, err
	}
	return updated, nil
}

func TestBackupSuccessMetrics(t *testing.T) {
	defer resetTest()
	resp, err := createApplicationBackup("test", "test", storkv1.ApplicationBackupStatusInProgress, storkv1.ApplicationBackupStageVolumes, 2, 2)
	require.NoError(t, err)
	time.Sleep(3 * time.Second)

	// InProgress
	require.Equal(t, float64(backupStatus[storkv1.ApplicationBackupStatusInProgress]), testutil.ToFloat64(backupStatusCounter), "application_backup_status does not matched")
	// Volume
	require.Equal(t, float64(backupStage[storkv1.ApplicationBackupStageVolumes]), testutil.ToFloat64(backupStageCounter), "application_backup_stage does not matched")

	resp.Status.Stage = storkv1.ApplicationBackupStageFinal
	resp.Status.Status = storkv1.ApplicationBackupStatusSuccessful
	resp.Status.TotalSize = 1024
	_, err = stork.Instance().UpdateApplicationBackup(resp)
	require.NoError(t, err)
	time.Sleep(3 * time.Second)
	// Successful
	require.Equal(t, float64(backupStatus[storkv1.ApplicationBackupStatusSuccessful]), testutil.ToFloat64(backupStatusCounter), "application_backup_status does not matched")
	// Final
	require.Equal(t, float64(backupStage[storkv1.ApplicationBackupStageFinal]), testutil.ToFloat64(backupStageCounter), "application_backup_stage does not matched")
	// Size
	require.Equal(t, float64(1024), testutil.ToFloat64(backupSizeCounter), "application_backup_size does not matched")

	err = stork.Instance().DeleteApplicationBackup("test", "test")
	require.NoError(t, err)
}

func TestBackupFailureMetrics(t *testing.T) {
	defer resetTest()
	_, err := createApplicationBackup("test", "test-fail", storkv1.ApplicationBackupStatusFailed, storkv1.ApplicationBackupStageInitial, 2, 2)
	require.NoError(t, err)
	time.Sleep(3 * time.Second)

	labels := make(prometheus.Labels)
	labels[metricName] = "test"
	labels[metricNamespace] = "test-fail"
	labels[MetricSchedule] = ""

	// Failure
	require.Equal(t, float64(backupStatus[storkv1.ApplicationBackupStatusFailed]), testutil.ToFloat64(backupStatusCounter.With(labels)), "application_backup_status does not matched")
	// Initial
	require.Equal(t, float64(backupStage[storkv1.ApplicationBackupStageInitial]), testutil.ToFloat64(backupStageCounter.With(labels)), "application_backup_stage does not matched")
}
