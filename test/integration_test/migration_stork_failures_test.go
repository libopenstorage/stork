//go:build integrationtest
// +build integrationtest

package integrationtest

import (
	"fmt"
	"testing"
	"time"

	storkv1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/portworx/sched-ops/k8s/core"
	storkops "github.com/portworx/sched-ops/k8s/stork"
	"github.com/portworx/sched-ops/task"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

const (
	migrationAppKey = "mysql-1-pvc"
	migrationKey    = "mysql-migration"
	nsKey           = "delete-stork-pods"
	storkNamespace  = "kube-system"
)

var storkLabel = map[string]string{"name": "stork"}

func TestMigrationStorkFailures(t *testing.T) {
	// reset mock time before running any tests
	err := setMockTime(nil)
	require.NoError(t, err, "Error resetting mock time")

	logrus.Infof("Using stork volume driver: %s", volumeDriverName)
	logrus.Infof("Backup path being used: %s", backupLocationPath)

}

func deleteStorkPodsSourceDuringMigrationTest(t *testing.T) {
	deleteStorkPodsDuringMigrationTest(t, "source", false)
}

func deleteStorkPodsDestDuringMigrationTest(t *testing.T) {
	deleteStorkPodsDuringMigrationTest(t, "destination", true)
}

func deleteStorkPodsDuringMigrationTest(t *testing.T, clusterKey string, delStorkDest bool) {
	var err error
	includeResourcesFlag := true
	startApplicationsFlag := false

	err = setSourceKubeConfig()
	require.NoError(t, err, "failed to set kubeconfig to source cluster: %v", err)

	ctxs, err := schedulerDriver.Schedule(nsKey+"-"+clusterKey,
		scheduler.ScheduleOptions{AppKeys: []string{migrationAppKey}})
	require.NoError(t, err, "Error scheduling task")
	require.Equal(t, 1, len(ctxs), "Only one task should have started")

	err = schedulerDriver.WaitForRunning(ctxs[0], defaultWaitTimeout, defaultWaitInterval)
	require.NoError(t, err, "Error waiting for app to get to running state")

	// create, apply and validate cluster pair specs
	err = scheduleClusterPair(t, ctxs[0], false, true, defaultClusterPairDir, false)
	require.NoError(t, err, "Error scheduling cluster pair")

	// apply migration specs
	currMigNamespace := migrationAppKey + "-" + nsKey + "-" + clusterKey
	currMig, err := createMigration(t, migrationKey, currMigNamespace, "remoteclusterpair", currMigNamespace, &includeResourcesFlag, &startApplicationsFlag)
	require.NoError(t, err, "failed to create migration %s in namespace: %s", currMig.Name, currMig.Namespace)

	// Wait for migration to start
	err = waitForMigrationToStart(currMig.Name, currMig.Namespace, migrationRetryTimeout)
	require.NoError(t, err, "Migration %s failed to start in namespace: %s", currMig.Name, currMig.Namespace)

	if delStorkDest {
		// Change kubeconfig to dest and delete stork pods
		err = setDestinationKubeConfig()
		require.NoError(t, err, "failed to set kubeconfig to destination cluster for deleting stork pods.")
	}

	logrus.Infof("Migration is in progress. Deleting stork pods.")
	storkPods, err := core.Instance().GetPods(storkNamespace, storkLabel)
	require.NoError(t, err, "failed to get stork pods.")

	err = core.Instance().DeletePods(storkPods.Items, false)
	require.NoError(t, err, "failed to delete stork pods.")

	// Change kubeconfig back to source. Ensure migration is successful, this redundant when
	// we have not switched context to destination cluster
	err = setSourceKubeConfig()
	require.NoError(t, err, "failed to set kubeconfig to source cluster to check for migration.")

	//err = waitForMigrationToComplete(currMig.Name, currMig.Namespace, migrationRetryTimeout)
	err = storkops.Instance().ValidateMigration(currMig.Name, currMig.Namespace, migrationRetryTimeout, defaultWaitInterval)
	require.NoError(t, err, "Migration %s failed to start in namespace: %s", currMig.Name, currMig.Namespace)

	// Change kubeconfig to destination. Check stork pods are running, for sanity
	err = setDestinationKubeConfig()
	require.NoError(t, err, "failed to set kubeconfig to destination cluster for checking stork pods.")
	_, err = core.Instance().GetPods(storkNamespace, storkLabel)
	require.NoError(t, err, "failed to get stork pods after migration.")

	// Change kubeconfig back to source. Clean up
	err = setSourceKubeConfig()
	require.NoError(t, err, "failed to set kubeconfig to source cluster before cleanup.")
	destroyAndWait(t, ctxs)
	err = deleteAndwaitForMigrationDeletion(currMig.Name, currMig.Namespace, migrationRetryTimeout)
	require.NoError(t, err, "Migration %s failed to delete in namespace: %s", currMig.Name, currMig.Namespace)
}

func waitForMigrationToStart(name, namespace string, timeout time.Duration) error {
	getMigration := func() (interface{}, bool, error) {
		migration, err := storkops.Instance().GetMigration(name, namespace)
		if err != nil {
			return "", false, err
		}

		if migration.Status.Status != storkv1.MigrationStatusInProgress {
			return "", true, fmt.Errorf("Migration %s in %s has not started yet.Status: %s. Retrying ", name, namespace, migration.Status.Status)
		}
		return "", false, nil
	}
	_, err := task.DoRetryWithTimeout(getMigration, timeout, backupWaitInterval)
	return err
}

func deleteAndwaitForMigrationDeletion(name, namespace string, timeout time.Duration) error {
	logrus.Infof("Deleting migration: %s in namespace: %s", name, namespace)
	err := storkops.Instance().DeleteMigration(name, namespace)
	if err != nil {
		return fmt.Errorf("Failed to delete migration: %s in namespace: %s", name, namespace)
	}
	getMigration := func() (interface{}, bool, error) {
		migration, err := storkops.Instance().GetMigration(name, namespace)
		if err == nil {
			return "", true, fmt.Errorf("Migration %s in %s has not completed yet.Status: %s. Retrying ", name, namespace, migration.Status.Status)
		}
		return "", false, nil
	}
	_, err = task.DoRetryWithTimeout(getMigration, timeout, backupWaitInterval)
	return err
}
