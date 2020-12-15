// +build unittest

package metrics

import (
	"testing"
	"time"

	storkv1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/portworx/sched-ops/k8s/stork"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func createClusterPair(name, ns string, storage, sched storkv1.ClusterPairStatusType) (*storkv1.ClusterPair, error) {
	pair := &storkv1.ClusterPair{}
	pair.Name = name
	pair.Namespace = ns
	resp, err := stork.Instance().CreateClusterPair(pair)
	if err != nil {
		return nil, err
	}
	resp.Status.StorageStatus = storage
	resp.Status.SchedulerStatus = sched
	updated, err := stork.Instance().UpdateClusterPair(resp)
	if err != nil {
		return nil, err
	}
	return updated, nil
}

func TestClusterPairSuccessMetrics(t *testing.T) {
	defer resetTest()
	_, err := createClusterPair("test", "test", storkv1.ClusterPairStatusReady, storkv1.ClusterPairStatusReady)
	require.NoError(t, err)
	time.Sleep(3 * time.Second)

	require.Equal(t, float64(ClusterpairStatus[storkv1.ClusterPairStatusReady]), testutil.ToFloat64(clusterpairSchedStatusCounter), "clusterpair_sched_status does not matched")
	require.Equal(t, float64(ClusterpairStatus[storkv1.ClusterPairStatusReady]), testutil.ToFloat64(clusterpairStorageStatusCounter), "clusterpair_storage_status does not matched")

	err = stork.Instance().DeleteClusterPair("test", "test")
	require.NoError(t, err)
}

func TestClusterPairFailureMetrics(t *testing.T) {
	defer resetTest()
	_, err := createClusterPair("test", "test-fail", storkv1.ClusterPairStatusDegraded, storkv1.ClusterPairStatusError)
	require.NoError(t, err)
	time.Sleep(3 * time.Second)

	labels := make(prometheus.Labels)
	labels[MetricName] = "test"
	labels[MetricNamespace] = "test-fail"
	require.Equal(t, float64(ClusterpairStatus[storkv1.ClusterPairStatusError]), testutil.ToFloat64(clusterpairSchedStatusCounter.With(labels)), "clusterpair_sched_status does not matched")
	require.Equal(t, float64(ClusterpairStatus[storkv1.ClusterPairStatusDegraded]), testutil.ToFloat64(clusterpairStorageStatusCounter.With(labels)), "clusterpair_storage_status does not matched")

	err = stork.Instance().DeleteClusterPair("test", "test-fail")
	require.NoError(t, err)
}
