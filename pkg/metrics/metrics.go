package metrics

import (
	"fmt"
	"time"

	"github.com/libopenstorage/stork/pkg/apis/stork"
	stork_api "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/portworx/sched-ops/k8s/apiextensions"
	storkops "github.com/portworx/sched-ops/k8s/stork"
	"github.com/sirupsen/logrus"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	// metricName for stork prometheus metrics
	metricName = "name"
	// metricNamespace for stork prometheus metrics
	metricNamespace = "namespace"
	// metricSchedule for stork prometheus metrics
	metricSchedule = "schedule"
	// metricPolicy for stork prometheus metrics
	metricPolicy = "policy"
	// waitInterval to wait for crd registration
	waitInterval = 5 * time.Second
)

// StartMetrics watch over stork controllers to collect metrics
func StartMetrics(enableApplicationController, enableMigrationController bool) error {
	go func() {
		if enableApplicationController {
			for {
				isCRDRegistered := true
				crdName := fmt.Sprintf("%s.%s", stork_api.ApplicationBackupResourcePlural, stork.GroupName)
				if _, err := apiextensions.Instance().GetCRD(crdName, metav1.GetOptions{}); err != nil {
					logrus.Errorf("failed to retrive applicationbackups crds: %v", err)
					isCRDRegistered = false
				}
				crdName = fmt.Sprintf("%s.%s", stork_api.ApplicationRestoreResourcePlural, stork.GroupName)
				if _, err := apiextensions.Instance().GetCRD(crdName, metav1.GetOptions{}); err != nil {
					logrus.Errorf("failed to retrive applicationrestores crds: %v", err)
					isCRDRegistered = false
				}
				crdName = fmt.Sprintf("%s.%s", stork_api.ApplicationCloneResourcePlural, stork.GroupName)
				if _, err := apiextensions.Instance().GetCRD(crdName, metav1.GetOptions{}); err != nil {
					logrus.Errorf("failed to retrive applicationclones crds: %v", err)
					isCRDRegistered = false
				}
				if isCRDRegistered {
					break
				} else {
					time.Sleep(waitInterval * time.Second)
				}
			}
			if err := storkops.Instance().WatchApplicationBackup("", watchBackupCR, metav1.ListOptions{}); err != nil {
				logrus.Errorf("failed to watch applicationbackups due to: %v", err)
			}
			if err := storkops.Instance().WatchApplicationRestore("", watchRestoreCR, metav1.ListOptions{}); err != nil {
				logrus.Errorf("failed to watch applicationrestores due to: %v", err)
			}
			if err := storkops.Instance().WatchApplicationClone("", watchCloneCR, metav1.ListOptions{}); err != nil {
				logrus.Errorf("failed to watch applicationclones due to: %v", err)
			}
			if err := storkops.Instance().WatchApplicationBackupSchedule("", watchBackupScheduleCR, metav1.ListOptions{}); err != nil {
				logrus.Errorf("failed to watch applicationbackup schedules due to: %v", err)
			}
			if err := storkops.Instance().WatchVolumeSnapshotSchedule("", watchVolumeSnapshotScheduleCR, metav1.ListOptions{}); err != nil {
				logrus.Errorf("failed to watch volume snapshot schedules due to: %v", err)
			}
			logrus.Infof("started metrics collection for applicationcontrollers")
		}
		if enableMigrationController {
			for {
				isCRDRegistered := true
				crdName := fmt.Sprintf("%s.%s", stork_api.ClusterPairResourcePlural, stork.GroupName)
				if _, err := apiextensions.Instance().GetCRD(crdName, metav1.GetOptions{}); err != nil {
					logrus.Errorf("failed to retrive clusterpairs crds: %v", err)
					isCRDRegistered = false
				}
				crdName = fmt.Sprintf("%s.%s", stork_api.MigrationResourcePlural, stork.GroupName)
				if _, err := apiextensions.Instance().GetCRD(crdName, metav1.GetOptions{}); err != nil {
					logrus.Errorf("failed to retrive migrations crds: %v", err)
					isCRDRegistered = false
				}
				if isCRDRegistered {
					break
				} else {
					time.Sleep(waitInterval * time.Second)
				}
			}
			if err := storkops.Instance().WatchClusterPair("", watchclusterpairCR, metav1.ListOptions{}); err != nil {
				logrus.Errorf("failed to watch clusterpair due to: %v", err)
			}
			if err := storkops.Instance().WatchMigration("", watchmigrationCR, metav1.ListOptions{}); err != nil {
				logrus.Errorf("failed to watch migration due to: %v", err)
			}
			if err := storkops.Instance().WatchMigrationSchedule("", watchmigrationScheduleCR, metav1.ListOptions{}); err != nil {
				logrus.Errorf("failed to watch migration schedules due to: %v", err)
			}
			logrus.Infof("started metrics collection for migrations")
		}
	}()
	return nil
}
