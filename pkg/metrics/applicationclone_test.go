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

func createApplicationClone(name, ns string, status storkv1.ApplicationCloneStatusType, stage storkv1.ApplicationCloneStageType, vol, app int) (*storkv1.ApplicationClone, error) {
	clone := &storkv1.ApplicationClone{}
	clone.Name = name
	clone.Namespace = ns
	resp, err := stork.Instance().CreateApplicationClone(clone)
	if err != nil {
		return nil, err
	}
	for i := 0; i < vol; i++ {
		vol := &storkv1.ApplicationCloneVolumeInfo{
			Volume:                "vol" + strconv.Itoa(i+1),
			PersistentVolumeClaim: "pvc" + strconv.Itoa(i+1),
		}
		resp.Status.Stage = stage
		resp.Status.Status = status
		resp.Status.Volumes = append(clone.Status.Volumes, vol)
	}
	updated, err := stork.Instance().UpdateApplicationClone(resp)
	if err != nil {
		return nil, err
	}
	return updated, nil
}

func TestCloneSuccessMetrics(t *testing.T) {
	defer resetTest()
	resp, err := createApplicationClone("test", "test", storkv1.ApplicationCloneStatusInProgress, storkv1.ApplicationCloneStageVolumes, 2, 2)
	require.NoError(t, err)
	time.Sleep(3 * time.Second)

	// InProgress
	require.Equal(t, float64(cloneStatus[storkv1.ApplicationCloneStatusInProgress]), testutil.ToFloat64(cloneStatusCounter), "application_clone_status does not matched")
	// Volume
	require.Equal(t, float64(cloneStage[storkv1.ApplicationCloneStageVolumes]), testutil.ToFloat64(cloneStageCounter), "application_clone_stage does not matched")

	resp.Status.Stage = storkv1.ApplicationCloneStageFinal
	resp.Status.Status = storkv1.ApplicationCloneStatusSuccessful
	_, err = stork.Instance().UpdateApplicationClone(resp)
	require.NoError(t, err)
	time.Sleep(3 * time.Second)
	// Successful
	require.Equal(t, float64(cloneStatus[storkv1.ApplicationCloneStatusSuccessful]), testutil.ToFloat64(cloneStatusCounter), "application_clone_status does not matched")
	// Final
	require.Equal(t, float64(cloneStage[storkv1.ApplicationCloneStageFinal]), testutil.ToFloat64(cloneStageCounter), "application_clone_stage does not matched")

	err = stork.Instance().DeleteApplicationClone("test", "test")
	require.NoError(t, err)
}

func TestCloneFailureMetrics(t *testing.T) {
	defer resetTest()
	_, err := createApplicationClone("test", "test-fail", storkv1.ApplicationCloneStatusFailed, storkv1.ApplicationCloneStageInitial, 2, 2)
	require.NoError(t, err)
	time.Sleep(3 * time.Second)

	labels := make(prometheus.Labels)
	labels[metricName] = "test"
	labels[metricNamespace] = "test-fail"

	// Failure
	require.Equal(t, float64(cloneStatus[storkv1.ApplicationCloneStatusFailed]), testutil.ToFloat64(cloneStatusCounter.With(labels)), "application_clone_status does not matched")
	// Initial
	require.Equal(t, float64(cloneStage[storkv1.ApplicationCloneStageInitial]), testutil.ToFloat64(cloneStageCounter.With(labels)), "application_clone_stage does not matched")
}
