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

func createApplicationRestore(name, ns string, status storkv1.ApplicationRestoreStatusType, stage storkv1.ApplicationRestoreStageType, vol, app int) (*storkv1.ApplicationRestore, error) {
	restore := &storkv1.ApplicationRestore{}
	restore.Name = name
	restore.Namespace = ns
	resp, err := stork.Instance().CreateApplicationRestore(restore)
	if err != nil {
		return nil, err
	}
	for i := 0; i < vol; i++ {
		vol := &storkv1.ApplicationRestoreVolumeInfo{
			RestoreVolume:         "vol" + strconv.Itoa(i+1),
			PersistentVolumeClaim: "pvc" + strconv.Itoa(i+1),
		}
		resp.Status.Stage = stage
		resp.Status.Status = status
		resp.Status.Volumes = append(restore.Status.Volumes, vol)
	}
	updated, err := stork.Instance().UpdateApplicationRestore(resp)
	if err != nil {
		return nil, err
	}
	return updated, nil
}

func TestRestoreSuccessMetrics(t *testing.T) {
	defer resetTest()
	resp, err := createApplicationRestore("test", "test", storkv1.ApplicationRestoreStatusInProgress, storkv1.ApplicationRestoreStageVolumes, 2, 2)
	require.NoError(t, err)
	time.Sleep(3 * time.Second)

	// InProgress
	require.Equal(t, float64(RestoreStatus[storkv1.ApplicationRestoreStatusInProgress]), testutil.ToFloat64(restoreStatusCounter), "application_restore_status does not matched")
	// Volume
	require.Equal(t, float64(RestoreStage[storkv1.ApplicationRestoreStageVolumes]), testutil.ToFloat64(restoreStageCounter), "application_restore_stage does not matched")

	resp.Status.Stage = storkv1.ApplicationRestoreStageFinal
	resp.Status.Status = storkv1.ApplicationRestoreStatusSuccessful
	resp.Status.TotalSize = 1024
	_, err = stork.Instance().UpdateApplicationRestore(resp)
	require.NoError(t, err)
	time.Sleep(3 * time.Second)
	// Successful
	require.Equal(t, float64(RestoreStatus[storkv1.ApplicationRestoreStatusSuccessful]), testutil.ToFloat64(restoreStatusCounter), "application_restore_status does not matched")
	// Final
	require.Equal(t, float64(RestoreStage[storkv1.ApplicationRestoreStageFinal]), testutil.ToFloat64(restoreStageCounter), "application_restore_stage does not matched")
	// Size
	require.Equal(t, float64(1024), testutil.ToFloat64(restoreSizeCounter), "application_restore_size does not matched")

	err = stork.Instance().DeleteApplicationRestore("test", "test")
	require.NoError(t, err)
}

func TestRestoreFailureMetrics(t *testing.T) {
	defer resetTest()
	_, err := createApplicationRestore("test", "test-fail", storkv1.ApplicationRestoreStatusFailed, storkv1.ApplicationRestoreStageInitial, 2, 2)
	require.NoError(t, err)
	time.Sleep(3 * time.Second)

	labels := make(prometheus.Labels)
	labels[MetricName] = "test"
	labels[MetricNamespace] = "test-fail"

	// Failure
	require.Equal(t, float64(RestoreStatus[storkv1.ApplicationRestoreStatusFailed]), testutil.ToFloat64(restoreStatusCounter.With(labels)), "application_restore_status does not matched")
	// Initial
	require.Equal(t, float64(RestoreStage[storkv1.ApplicationRestoreStageInitial]), testutil.ToFloat64(restoreStageCounter.With(labels)), "application_restore_stage does not matched")
}
