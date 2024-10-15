//go:build integrationtest
// +build integrationtest

package integrationtest

import (
	"fmt"
	"testing"
	"time"

	storkv1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	storkops "github.com/libopenstorage/stork/pkg/crud/stork"
	"github.com/libopenstorage/stork/pkg/log"
	"github.com/portworx/sched-ops/k8s/core"
	"github.com/portworx/sched-ops/task"
	"github.com/pure-px/torpedo/drivers/scheduler"
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
	log.FailOnError(t, err, "Error resetting mock time")
	currentTestSuite = t.Name()

	log.InfoD("Using stork volume driver: %s", volumeDriverName)
	log.InfoD("Backup path being used: %s", backupLocationPath)
	setDefaultsForBackup(t)

	t.Run("deleteStorkPodsSourceDuringMigrationTest", deleteStorkPodsSourceDuringMigrationTest)
	t.Run("deleteStorkPodsDestDuringMigrationTest", deleteStorkPodsDestDuringMigrationTest)
}

func deleteStorkPodsSourceDuringMigrationTest(t *testing.T) {
	var testrailID, testResult = 51458, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	defer updateDashStats(t.Name(), &testResult)

	deleteStorkPodsDuringMigrationTest(t, "source", false)

	// If we are here then the test has passed
	testResult = testResultPass
	log.InfoD("Test status at end of %s test: %s", t.Name(), testResult)
}

func deleteStorkPodsDestDuringMigrationTest(t *testing.T) {
	var testrailID, testResult = 51459, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	defer updateDashStats(t.Name(), &testResult)

	deleteStorkPodsDuringMigrationTest(t, "destination", true)

	// If we are here then the test has passed
	testResult = testResultPass
	log.InfoD("Test status at end of %s test: %s", t.Name(), testResult)
}

func deleteStorkPodsDuringMigrationTest(t *testing.T, clusterKey string, delStorkDest bool) {
	var err error
	includeResourcesFlag := true
	includeVolumesFlag := true
	startApplicationsFlag := false

	err = setSourceKubeConfig()
	log.FailOnError(t, err, "failed to set kubeconfig to source cluster: %v", err)

	ctxs, err := schedulerDriver.Schedule(nsKey+"-"+clusterKey,
		scheduler.ScheduleOptions{AppKeys: []string{migrationAppKey}})
	log.FailOnError(t, err, "Error scheduling task")
	Dash.VerifyFatal(t, len(ctxs), 1, "Only one task should have started")

	err = schedulerDriver.WaitForRunning(ctxs[0], defaultWaitTimeout, defaultWaitInterval)
	log.FailOnError(t, err, "Error waiting for app to get to running state")

	// Schedule bidirectional or regular cluster pair based on the flag
	scheduleClusterPairGeneric(t, ctxs, migrationAppKey, nsKey+"-"+clusterKey, defaultClusterPairDir, "", false, true, false)

	// apply migration specs
	currMigNamespace := migrationAppKey + "-" + nsKey + "-" + clusterKey
	currMig, err := createMigration(t, migrationKey, currMigNamespace, "remoteclusterpair", currMigNamespace, &includeResourcesFlag, &includeVolumesFlag, &startApplicationsFlag)
	log.FailOnError(t, err, "failed to create migration %s in namespace: %s", currMig.Name, currMig.Namespace)

	// Wait for migration to start
	err = waitForMigrationToStart(currMig.Name, currMig.Namespace, migrationRetryTimeout)
	log.FailOnError(t, err, "Migration %s failed to start in namespace: %s", currMig.Name, currMig.Namespace)

	if delStorkDest {
		// Change kubeconfig to dest and delete stork pods
		err = setDestinationKubeConfig()
		log.FailOnError(t, err, "failed to set kubeconfig to destination cluster for deleting stork pods.")
	}

	log.InfoD("Migration is in progress. Deleting stork pods.")
	storkPods, err := core.Instance().GetPods(storkNamespace, storkLabel)
	log.FailOnError(t, err, "failed to get stork pods.")

	err = core.Instance().DeletePods(storkPods.Items, false)
	log.FailOnError(t, err, "failed to delete stork pods.")

	// Change kubeconfig back to source. Ensure migration is successful, this redundant when
	// we have not switched context to destination cluster
	err = setSourceKubeConfig()
	log.FailOnError(t, err, "failed to set kubeconfig to source cluster to check for migration.")

	//err = waitForMigrationToComplete(currMig.Name, currMig.Namespace, migrationRetryTimeout)
	err = storkops.Instance().ValidateMigration(currMig.Name, currMig.Namespace, migrationRetryTimeout, defaultWaitInterval)
	log.FailOnError(t, err, "Migration %s failed to start in namespace: %s", currMig.Name, currMig.Namespace)

	// Change kubeconfig to destination. Check stork pods are running, for sanity
	err = setDestinationKubeConfig()
	log.FailOnError(t, err, "failed to set kubeconfig to destination cluster for checking stork pods.")
	_, err = core.Instance().GetPods(storkNamespace, storkLabel)
	log.FailOnError(t, err, "failed to get stork pods after migration.")
	err = core.Instance().DeleteNamespace(currMigNamespace)
	log.FailOnError(t, err, "failed to delete namespace %s on destination cluster", currMigNamespace)

	// Change kubeconfig back to source. Clean up
	err = setSourceKubeConfig()
	log.FailOnError(t, err, "failed to set kubeconfig to source cluster before cleanup.")
	destroyAndWait(t, ctxs)
	err = deleteAndwaitForMigrationDeletion(currMig.Name, currMig.Namespace, migrationRetryTimeout)
	log.FailOnError(t, err, "Migration %s failed to delete in namespace: %s", currMig.Name, currMig.Namespace)
	err = core.Instance().DeleteNamespace(currMigNamespace)
	log.FailOnError(t, err, "failed to delete namespace %s on source cluster", currMigNamespace)
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
	log.InfoD("Deleting migration: %s in namespace: %s", name, namespace)
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
