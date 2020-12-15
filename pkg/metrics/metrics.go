package metrics

import (
	storkops "github.com/portworx/sched-ops/k8s/stork"
	"github.com/sirupsen/logrus"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	// metricName for stork prometheus metrics
	metricName = "name"
	// metricNamespace for stork prometheus metrics
	metricNamespace = "namespace"
	// MetricSchedule for stork prometheus metrics
	MetricSchedule = "schedule"
)

// StartMetrics watch over stork controllers to collect metrics
func StartMetrics() error {
	if err := storkops.Instance().WatchApplicationBackup("", watchBackupCR, metav1.ListOptions{}); err != nil {
		logrus.Errorf("failed to watch applicationbackups due to: %v", err)
		return err
	}
	if err := storkops.Instance().WatchApplicationRestore("", watchRestoreCR, metav1.ListOptions{}); err != nil {
		logrus.Errorf("failed to watch applicationrestores due to: %v", err)
		return err
	}
	if err := storkops.Instance().WatchApplicationClone("", watchCloneCR, metav1.ListOptions{}); err != nil {
		logrus.Errorf("failed to watch applicationclones due to: %v", err)
		return err
	}
	if err := storkops.Instance().WatchClusterPair("", watchclusterpairCR, metav1.ListOptions{}); err != nil {
		logrus.Errorf("failed to watch clusterpair due to: %v", err)
		return err
	}
	if err := storkops.Instance().WatchMigration("", watchmigrationCR, metav1.ListOptions{}); err != nil {
		logrus.Errorf("failed to watch migration due to: %v", err)
		return err
	}
	return nil
}
