//go:build integrationtest
// +build integrationtest

package integrationtest

import (
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/portworx/sched-ops/k8s/apps"
	"github.com/portworx/sched-ops/k8s/core"
	storkops "github.com/portworx/sched-ops/k8s/stork"
	"github.com/portworx/sched-ops/task"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/portworx/torpedo/drivers/scheduler/spec"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	apps_api "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"

	"github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
)

const (
	migrationRetryInterval = 10 * time.Second
	migrationRetryTimeout  = 5 * time.Minute
	rabbitmqNamespace      = "rabbitmq-operator-migration"
)

func TestMigration(t *testing.T) {
	// reset mock time before running any tests
	err := setMockTime(nil)
	require.NoError(t, err, "Error resetting mock time")

	logrus.Infof("Using stork volume driver: %s", volumeDriverName)
	logrus.Infof("Backup path being used: %s", backupLocationPath)

	setDefaultsForBackup(t)

	t.Run("testMigration", testMigration)
	t.Run("testMigrationFailoverFailback", testMigrationFailoverFailback)
	t.Run("deleteStorkPodsSourceDuringMigrationTest", deleteStorkPodsSourceDuringMigrationTest)
	t.Run("deleteStorkPodsDestDuringMigrationTest", deleteStorkPodsDestDuringMigrationTest)
}

func testMigration(t *testing.T) {
	// reset mock time before running any tests
	err := setMockTime(nil)
	require.NoError(t, err, "Error resetting mock time")

	err = setSourceKubeConfig()
	require.NoError(t, err, "failed to set kubeconfig to source cluster: %v", err)

	t.Run("deploymentTest", deploymentMigrationTest)
	t.Run("deploymentMigrationReverseTest", deploymentMigrationReverseTest)
	t.Run("statefulsetTest", statefulsetMigrationTest)
	t.Run("statefulsetStartAppFalseTest", statefulsetMigrationStartAppFalseTest)
	t.Run("statefulsetRuleTest", statefulsetMigrationRuleTest)
	t.Run("preExecRuleMissingTest", statefulsetMigrationRulePreExecMissingTest)
	t.Run("postExecRuleMissingTest", statefulsetMigrationRulePostExecMissingTest)
	t.Run("disallowedNamespaceTest", migrationDisallowedNamespaceTest)
	t.Run("failingPreExecRuleTest", migrationFailingPreExecRuleTest)
	t.Run("failingPostExecRuleTest", migrationFailingPostExecRuleTest)
	// TODO: waiting for https://portworx.atlassian.net/browse/STOR-281 to be resolved
	if authTokenConfigMap == "" {
		t.Run("labelSelectorTest", migrationLabelSelectorTest)
		t.Run("labelExcludeSelectorTest", migrationLabelExcludeSelectorTest)
		t.Run("intervalScheduleTest", migrationIntervalScheduleTest)
		t.Run("dailyScheduleTest", migrationDailyScheduleTest)
		t.Run("weeklyScheduleTest", migrationWeeklyScheduleTest)
		t.Run("monthlyScheduleTest", migrationMonthlyScheduleTest)
		t.Run("scheduleInvalidTest", migrationScheduleInvalidTest)
		t.Run("intervalScheduleCleanupTest", intervalScheduleCleanupTest)
	}
	t.Run("networkpolicyTest", networkPolicyMigrationTest)
	t.Run("endpointTest", endpointMigrationTest)
	t.Run("clusterPairFailuresTest", clusterPairFailuresTest)
	t.Run("scaleTest", migrationScaleTest)
	t.Run("pvcResizeTest", pvcResizeMigrationTest)
	t.Run("transformResourceTest", transformResourceTest)
	t.Run("suspendMigrationTest", suspendMigrationTest)
	t.Run("operatorMigrationMongoTest", operatorMigrationMongoTest)
	t.Run("operatorMigrationRabbitmqTest", operatorMigrationRabbitmqTest)
	t.Run("bidirectionalClusterPairTest", bidirectionalClusterPairTest)
	t.Run("unidirectionalClusterPairTest", unidirectionalClusterPairTest)
	t.Run("serviceAndServiceAccountUpdate", serviceAndServiceAccountUpdate)
	t.Run("namespaceLabelSelectorTest", namespaceLabelSelectorTest)

	err = setRemoteConfig("")
	require.NoError(t, err, "setting kubeconfig to default failed")
}

func triggerMigrationTest(
	t *testing.T,
	instanceID string,
	appKey string,
	additionalAppKeys []string,
	migrationAppKey string,
	migrationSuccessExpected bool,
	migrateAllAppsExpected bool,
	startAppsOnMigration bool,
	skipDestDeletion bool,
) {
	var err error
	// Reset config in case of error
	defer func() {
		err = setSourceKubeConfig()
		require.NoError(t, err, "Error resetting source config")
	}()

	ctxs, preMigrationCtx := triggerMigration(t, instanceID, appKey, additionalAppKeys, []string{migrationAppKey}, migrateAllAppsExpected, false, startAppsOnMigration, false, "", nil)

	validateAndDestroyMigration(t, ctxs, instanceID, appKey, preMigrationCtx, migrationSuccessExpected, startAppsOnMigration, migrateAllAppsExpected, false, skipDestDeletion, true)
}

func triggerMigration(
	t *testing.T,
	instanceID string,
	appKey string,
	additionalAppKeys []string,
	migrationAppKeys []string,
	migrateAllAppsExpected bool,
	skipStoragePair bool,
	startAppsOnMigration bool,
	pairReverse bool,
	projectIDMappings string,
	namespaceLabels map[string]string,
) ([]*scheduler.Context, *scheduler.Context) {
	ctxs, err := schedulerDriver.Schedule(instanceID,
		scheduler.ScheduleOptions{
			AppKeys: []string{appKey},
			Labels:  namespaceLabels,
		})
	require.NoError(t, err, "Error scheduling task")
	require.Equal(t, 1, len(ctxs), "Only one task should have started")

	err = schedulerDriver.WaitForRunning(ctxs[0], defaultWaitTimeout, defaultWaitInterval)
	require.NoError(t, err, "Error waiting for app to get to running state")

	preMigrationCtx := ctxs[0].DeepCopy()

	if len(additionalAppKeys) > 0 {
		err = schedulerDriver.AddTasks(ctxs[0],
			scheduler.ScheduleOptions{AppKeys: additionalAppKeys})
		require.NoError(t, err, "Error scheduling additional apps")
		err = schedulerDriver.WaitForRunning(ctxs[0], defaultWaitTimeout, defaultWaitInterval)
		require.NoError(t, err, "Error waiting for additional apps to get to running state")
	}

	if migrateAllAppsExpected {
		preMigrationCtx = ctxs[0].DeepCopy()
	}

	// Schedule bidirectional or regular cluster pair based on the flag
	scheduleClusterPairGeneric(t, ctxs, appKey, instanceID, defaultClusterPairDir, projectIDMappings, skipStoragePair, true, pairReverse)

	// apply migration specs
	err = schedulerDriver.AddTasks(ctxs[0],
		scheduler.ScheduleOptions{AppKeys: migrationAppKeys})
	require.NoError(t, err, "Error scheduling migration specs")

	return ctxs, preMigrationCtx
}

// validateMigrationSummary validats the migration summary
// currently we don't have an automated way to find out how many resources got deployed
// through torpedo specs. For ex. a statefulset can have an inline PVC and that should
// get counted as a resource in Migration, but torpedo won't count it as a separate resource
// in its context. The caller is expected provide the counts
func validateMigrationSummary(
	t *testing.T,
	preMigrationCtx *scheduler.Context,
	expectedResources uint64,
	expectedVolumes uint64,
	migrationName, namespace string,
) {
	if preMigrationCtx == nil {
		return
	}
	if preMigrationCtx.App == nil {
		return
	}
	migObj, err := storkops.Instance().GetMigration(migrationName, namespace)
	require.NoError(t, err, "get migration failed")
	require.NotNil(t, migObj.Status.Summary, "migration summary is nil")
	require.Equal(t, migObj.Status.Summary.NumberOfMigratedResources, expectedResources, "unexpected number of resources migrated")
	require.Equal(t, migObj.Status.Summary.NumberOfMigratedVolumes, expectedVolumes, "unexpected number of volumes migrated")
	require.Equal(t, migObj.Status.Summary.TotalNumberOfResources, expectedResources, "unexpected number of total resources")
	require.Equal(t, migObj.Status.Summary.TotalNumberOfVolumes, expectedVolumes, "unexpected number of total volumes")
	if expectedVolumes > 0 {
		require.True(t, migObj.Status.Summary.TotalBytesMigrated > 0, "expected bytes total to be non-zero")
	} else {
		require.True(t, migObj.Status.Summary.TotalBytesMigrated == 0, "expected bytes total to be zero")
	}
}

func validateAndDestroyMigration(
	t *testing.T,
	ctxs []*scheduler.Context,
	instanceID string,
	appKey string,
	preMigrationCtx *scheduler.Context,
	migrationSuccessExpected bool,
	startAppsOnMigration bool,
	migrateAllAppsExpected bool,
	skipAppDeletion bool,
	skipDestDeletion bool,
	blowNamespaces bool,
) {
	var err error
	timeout := defaultWaitTimeout
	if !migrationSuccessExpected {
		timeout = timeout / 5
	}

	allAppsCtx := ctxs[0].DeepCopy()
	err = schedulerDriver.WaitForRunning(ctxs[0], timeout, defaultWaitInterval)
	if migrationSuccessExpected {
		require.NoError(t, err, "Error waiting for migration to get to Ready state")

		// wait on cluster 2 for the app to be running
		funcWaitAndDelete := func() {
			if startAppsOnMigration {
				err = schedulerDriver.WaitForRunning(preMigrationCtx, defaultWaitTimeout, defaultWaitInterval)
				require.NoError(t, err, "Error waiting for pod to get to running state on remote cluster after migration")
				if !migrateAllAppsExpected {
					err = schedulerDriver.WaitForRunning(allAppsCtx, defaultWaitTimeout/2, defaultWaitInterval)
					require.Error(t, err, "All apps shouldn't have been migrated")
				}
			} else {
				err = schedulerDriver.WaitForRunning(preMigrationCtx, defaultWaitTimeout/4, defaultWaitInterval)
				require.Error(t, err, "Expected pods to NOT get to running state on remote cluster after migration")
			}

			/* Failing right now as SC's are not migrated
			* else {
				logrus.Infof("test only validating storage components as migration has startApplications disabled")
				err = schedulerDriver.InspectVolumes(preMigrationCtx, defaultWaitTimeout, defaultWaitInterval)
				require.NoError(t, err, "Error validating storage components on remote cluster after migration")
			}*/

			// Validate the migration summary
			// destroy mysql app on cluster 2
			if !skipAppDeletion && !skipDestDeletion {
				destroyAndWait(t, []*scheduler.Context{preMigrationCtx})
			}
		}
		executeOnDestination(t, funcWaitAndDelete)
	} else {
		require.Error(t, err, "Expected migration to fail")
	}

	// destroy app on cluster 1
	if !skipAppDeletion {
		destroyAndWait(t, ctxs)
	}

	if blowNamespaces {
		blowNamespacesForTest(t, instanceID, appKey, skipDestDeletion)
	}
}

func deploymentMigrationTest(t *testing.T) {
	var testrailID, testResult = 50803, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult)
	defer updateTestRail(&testResult, testrailID, runID)
	instanceID := "mysql-migration"
	appKey := "mysql-1-pvc"

	triggerMigrationTest(
		t,
		instanceID,
		appKey,
		nil,
		instanceID,
		true,
		true,
		true,
		false,
	)

	// If we are here then the test has passed
	testResult = testResultPass
	logrus.Infof("Test status at end of %s test: %s", t.Name(), testResult)
}

func deploymentMigrationReverseTest(t *testing.T) {
	var testrailID, testResult = 54210, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult)
	defer updateTestRail(&testResult, testrailID, runID)
	instanceID := "mysql-migration"
	appKey := "mysql-1-pvc"

	var err error

	ctxs, preMigrationCtx := triggerMigration(
		t,
		instanceID,
		appKey,
		nil,
		[]string{instanceID},
		true,
		false,
		true,
		false,
		"",
		nil,
	)

	// Cleanup up source
	validateAndDestroyMigration(t, ctxs, instanceID, appKey, preMigrationCtx, true, true, true, false, false, false)

	// Change kubeconfig to destination
	err = setDestinationKubeConfig()
	require.NoError(t, err, "failed to set kubeconfig to source cluster: %v", err)

	ctxsReverse, err := schedulerDriver.Schedule("mysql-migration",
		scheduler.ScheduleOptions{AppKeys: []string{"mysql-1-pvc"}})
	require.NoError(t, err, "Error scheduling task")
	require.Equal(t, 1, len(ctxsReverse), "Only one task should have started")

	err = schedulerDriver.WaitForRunning(ctxsReverse[0], defaultWaitTimeout, defaultWaitInterval)
	require.NoError(t, err, "Error waiting for app to get to running state")

	postMigrationCtx := ctxsReverse[0].DeepCopy()

	// create, apply and validate cluster pair specs on destination for non-bidirectional pair
	if !bidirectionalClusterpair && !unidirectionalClusterpair {
		err = scheduleClusterPair(ctxsReverse[0], false, false, "cluster-pair-reverse", "", true)
	} else if unidirectionalClusterpair {
		clusterPairNamespace := fmt.Sprintf("%s-%s", appKey, instanceID)
		err = setSourceKubeConfig()
		require.NoError(t, err, "Error setting remote config")
		err = core.Instance().DeleteSecret(remotePairName, clusterPairNamespace)
		require.NoError(t, err, "Error deleting secret")
		err = storkops.Instance().DeleteBackupLocation(remotePairName, clusterPairNamespace)
		require.NoError(t, err, "Error deleting backuplocation")
		err = setDestinationKubeConfig()
		require.NoError(t, err, "Error setting remote config")
		err = core.Instance().DeleteSecret(remotePairName, clusterPairNamespace)
		require.NoError(t, err, "Error deleting secret")
		err = storkops.Instance().DeleteBackupLocation(remotePairName, clusterPairNamespace)
		require.NoError(t, err, "Error deleting backuplocation")
		err = scheduleUnidirectionalClusterPair(remotePairName, clusterPairNamespace, "", defaultBackupLocation, defaultSecretName, false, true)
	}
	require.NoError(t, err, "Error scheduling cluster pair")

	// apply migration specs
	err = schedulerDriver.AddTasks(ctxsReverse[0],
		scheduler.ScheduleOptions{AppKeys: []string{"mysql-migration"}})
	require.NoError(t, err, "Error scheduling migration specs")

	err = schedulerDriver.WaitForRunning(ctxsReverse[0], defaultWaitTimeout, defaultWaitInterval)
	require.NoError(t, err, "Error waiting for migration to complete")

	destroyAndWait(t, ctxsReverse)

	// Cleanup up source
	err = setSourceKubeConfig()
	require.NoError(t, err, "failed to set kubeconfig to source cluster: %v", err)

	err = schedulerDriver.WaitForRunning(postMigrationCtx, defaultWaitTimeout, defaultWaitInterval)
	require.NoError(t, err, "Error waiting for migration to complete")

	//validateAndDestroyMigration(t, []*scheduler.Context{preMigrationCtx}, preMigrationCtx, true, true, true, true, false)

	destroyAndWait(t, []*scheduler.Context{postMigrationCtx})
	validateAndDestroyMigration(t, []*scheduler.Context{preMigrationCtx}, instanceID, appKey, preMigrationCtx, false, false, false, true, false, true)

	// If we are here then the test has passed
	testResult = testResultPass
	logrus.Infof("Test status at end of %s test: %s", t.Name(), testResult)
}

func statefulsetMigrationTest(t *testing.T) {
	var testrailID, testResult = 50804, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult)
	defer updateTestRail(&testResult, testrailID, runID)
	instanceID := "cassandra-migration"
	appKey := "cassandra"

	triggerMigrationTest(
		t,
		instanceID,
		appKey,
		nil,
		instanceID,
		true,
		true,
		true,
		false,
	)

	// If we are here then the test has passed
	testResult = testResultPass
	logrus.Infof("Test status at end of %s test: %s", t.Name(), testResult)
}

func statefulsetMigrationStartAppFalseTest(t *testing.T) {
	var testrailID, testResult = 86243, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult)
	defer updateTestRail(&testResult, testrailID, runID)
	instanceID := "cassandra-migration"
	appKey := "cassandra"

	triggerMigrationTest(
		t,
		instanceID,
		appKey,
		nil,
		"cassandra-migration-startapps-false",
		true,
		true,
		false,
		false,
	)

	// If we are here then the test has passed
	testResult = testResultPass
	logrus.Infof("Test status at end of %s test: %s", t.Name(), testResult)
}

func statefulsetMigrationRuleTest(t *testing.T) {
	var testrailID, testResult = 50805, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult)
	defer updateTestRail(&testResult, testrailID, runID)
	instanceID := "cassandra-migration-rule"
	appKey := "cassandra"

	triggerMigrationTest(
		t,
		instanceID,
		appKey,
		nil,
		"cassandra-migration-rule",
		true,
		true,
		true,
		false,
	)

	// If we are here then the test has passed
	testResult = testResultPass
	logrus.Infof("Test status at end of %s test: %s", t.Name(), testResult)
}

func statefulsetMigrationRulePreExecMissingTest(t *testing.T) {
	var testrailID, testResult = 50806, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult)
	defer updateTestRail(&testResult, testrailID, runID)
	instanceID := "migration-pre-exec-missing"
	appKey := "mysql-1-pvc"

	triggerMigrationTest(
		t,
		instanceID,
		appKey,
		nil,
		"mysql-migration-pre-exec-missing",
		false,
		true,
		true,
		true,
	)

	// If we are here then the test has passed
	testResult = testResultPass
	logrus.Infof("Test status at end of %s test: %s", t.Name(), testResult)
}
func statefulsetMigrationRulePostExecMissingTest(t *testing.T) {
	var testrailID, testResult = 50807, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult)
	defer updateTestRail(&testResult, testrailID, runID)
	instanceID := "migration-post-exec-missing"
	appKey := "mysql-1-pvc"

	triggerMigrationTest(
		t,
		instanceID,
		appKey,
		nil,
		"mysql-migration-post-exec-missing",
		false,
		true,
		true,
		true,
	)

	// If we are here then the test has passed
	testResult = testResultPass
	logrus.Infof("Test status at end of %s test: %s", t.Name(), testResult)
}

func migrationDisallowedNamespaceTest(t *testing.T) {
	var testrailID, testResult = 50808, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult)
	defer updateTestRail(&testResult, testrailID, runID)
	instanceID := "migration-disallowed-namespace"
	appKey := "mysql-1-pvc"

	triggerMigrationTest(
		t,
		instanceID,
		appKey,
		nil,
		"mysql-migration-disallowed-ns",
		false,
		true,
		true,
		true,
	)

	// If we are here then the test has passed
	testResult = testResultPass
	logrus.Infof("Test status at end of %s test: %s", t.Name(), testResult)
}

func migrationFailingPreExecRuleTest(t *testing.T) {
	var testrailID, testResult = 50809, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult)
	defer updateTestRail(&testResult, testrailID, runID)
	instanceID := "migration-failing-pre-exec-rule"
	appKey := "mysql-1-pvc"

	triggerMigrationTest(
		t,
		instanceID,
		appKey,
		nil,
		"mysql-migration-failing-pre-exec",
		false,
		true,
		true,
		true,
	)

	// If we are here then the test has passed
	testResult = testResultPass
	logrus.Infof("Test status at end of %s test: %s", t.Name(), testResult)
}

func migrationFailingPostExecRuleTest(t *testing.T) {
	var testrailID, testResult = 50810, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult)
	defer updateTestRail(&testResult, testrailID, runID)
	instanceID := "migration-failing-post-exec-rule"
	appKey := "mysql-1-pvc"

	triggerMigrationTest(
		t,
		instanceID,
		appKey,
		nil,
		"mysql-migration-failing-post-exec",
		false,
		true,
		true,
		true,
	)

	// If we are here then the test has passed
	testResult = testResultPass
	logrus.Infof("Test status at end of %s test: %s", t.Name(), testResult)
}

func migrationLabelSelectorTest(t *testing.T) {
	var testrailID, testResult = 50811, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult)
	defer updateTestRail(&testResult, testrailID, runID)
	instanceID := "migration-label-selector-test"
	appKey := "cassandra"

	triggerMigrationTest(
		t,
		instanceID,
		appKey,
		[]string{"mysql-1-pvc"},
		"label-selector-migration",
		true,
		false,
		true,
		false,
	)

	// If we are here then the test has passed
	testResult = testResultPass
	logrus.Infof("Test status at end of %s test: %s", t.Name(), testResult)
}

func migrationLabelExcludeSelectorTest(t *testing.T) {
	var testrailID, testResult = 86245, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult)
	defer updateTestRail(&testResult, testrailID, runID)
	instanceID := "migration-label-exclude-selector-test"
	appKey := "cassandra"

	triggerMigrationTest(
		t,
		instanceID,
		appKey,
		[]string{"mysql-1-pvc"},
		"label-exclude-selector-migration",
		true,
		false,
		true,
		false,
	)

	// If we are here then the test has passed
	testResult = testResultPass
	logrus.Infof("Test status at end of %s test: %s", t.Name(), testResult)
}

func namespaceLabelSelectorTest(t *testing.T) {
	var testrailID, testResult = 86244, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult)
	defer updateTestRail(&testResult, testrailID, runID)
	instanceID := "ns-selector-test"
	appKey := "cassandra"

	var err error
	defer func() {
		err = setSourceKubeConfig()
		require.NoError(t, err, "Error resetting source config")
	}()

	// Deploy mysql-1-pvc in namespace "mysql-1-pvc-ns-selector-test"
	ctxNs2, err := schedulerDriver.Schedule(instanceID,
		scheduler.ScheduleOptions{
			AppKeys: []string{"mysql-1-pvc"},
			Labels:  nil,
		})
	require.NoError(t, err, "Error scheduling task")
	require.Equal(t, 1, len(ctxNs2), "Only one task should have started")

	// Deploy cassandra in namespace "cassandra-ns-selector-test"
	// This namespace has label kubernetes.io/metadata.name=cassandra-ns-selector-test
	ctxs, preMigrationCtx := triggerMigration(
		t,
		instanceID,
		appKey,
		[]string{},
		[]string{"namespace-selector-migration"},
		false,
		false,
		true,
		false,
		"",
		nil)

	// Validate but do not destroy migration and apps
	validateAndDestroyMigration(
		t,
		ctxs,
		instanceID,
		appKey,
		preMigrationCtx,
		true,
		true,
		true,
		true,  //Skip Deleting App on Destination
		true,  //Skip Deleting App on Source
		false, //Don't delete namespaces yet
	)

	// Validate on destination cluster that mysql-1-pvc is not migrated as it doesn't have the required namespace labels
	err = setDestinationKubeConfig()
	require.NoError(t, err, "failed to set kubeconfig to source cluster: %v", err)

	err = schedulerDriver.WaitForRunning(ctxNs2[0], defaultWaitTimeout/16, defaultWaitInterval)
	require.Error(t, err, "Error waiting for pod to get to running state on remote cluster after migration")

	err = setSourceKubeConfig()
	require.NoError(t, err, "failed to set kubeconfig to source cluster: %v", err)

	// Destroy migration
	destroyAndWait(t, []*scheduler.Context{preMigrationCtx})

	// Destroy cassandra app context
	destroyAndWait(t, ctxs)

	// Destroy mysql-1-pvc app context
	destroyAndWait(t, ctxNs2)

	// If we are here then the test has passed
	testResult = testResultPass
	logrus.Infof("Test status at end of %s test: %s", t.Name(), testResult)
}

// migrationIntervalScheduleTest runs test for migrations with schedules that are
// intervals of time
func migrationIntervalScheduleTest(t *testing.T) {
	var testrailID, testResult = 50812, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult)
	defer updateTestRail(&testResult, testrailID, runID)
	instanceID := "mysql-migration-schedule-interval"
	appKey := "mysql-1-pvc"

	var err error
	// Reset config in case of error
	defer func() {
		err = setSourceKubeConfig()
		require.NoError(t, err, "Error resetting remote config")
	}()

	// the schedule interval for these specs it set to 5 minutes
	ctxs, preMigrationCtx := triggerMigration(
		t,
		instanceID,
		appKey,
		nil,
		[]string{"mysql-migration-schedule-interval"},
		true,
		false,
		false,
		false,
		"",
		nil,
	)

	// bump time of the world by 5 minutes
	mockNow := time.Now().Add(6 * time.Minute)
	err = setMockTime(&mockNow)
	require.NoError(t, err, "Error setting mock time")

	validateAndDestroyMigration(t, ctxs, instanceID, appKey, preMigrationCtx, true, false, true, false, false, true)

	// If we are here then the test has passed
	testResult = testResultPass
	logrus.Infof("Test status at end of %s test: %s", t.Name(), testResult)
}

// intervalScheduleCleanupTest runs test for migrations with schedules that are
// intervals of time, will try to perform cleanup of k8s resources which are deleted
// on source cluster
func intervalScheduleCleanupTest(t *testing.T) {
	var testrailID, testResult = 86246, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult)
	defer updateTestRail(&testResult, testrailID, runID)
	instanceID := "mysql-migration-schedule-interval"
	appKey := "mysql-1-pvc"

	var err error
	var name, namespace string
	var pvcs *v1.PersistentVolumeClaimList
	// Reset config in case of error
	defer func() {
		err = setSourceKubeConfig()
		require.NoError(t, err, "Error resetting remote config")
	}()
	err = setMockTime(nil)
	require.NoError(t, err, "Error resetting mock time")
	// the schedule interval for these specs it set to 5 minutes
	ctxs, preMigrationCtx := triggerMigration(
		t,
		instanceID,
		appKey,
		[]string{"cassandra"},
		[]string{"mysql-migration-schedule-interval"},
		true,
		false,
		false,
		false,
		"",
		nil,
	)

	validateMigration(t, "mysql-migration-schedule-interval", preMigrationCtx.GetID())

	// delete statefulset from source cluster
	for i, spec := range ctxs[0].App.SpecList {
		if obj, ok := spec.(*apps_api.StatefulSet); ok {
			name = obj.GetName()
			namespace = obj.GetNamespace()
			pvcs, err = apps.Instance().GetPVCsForStatefulSet(obj)
			require.NoError(t, err, "error getting pvcs for ss")
			err = apps.Instance().DeleteStatefulSet(name, namespace)
			require.NoError(t, err, "error deleting cassandra statefulset")
			err = apps.Instance().ValidateStatefulSet(obj, 1*time.Minute)
			require.NoError(t, err, "error deleting cassandra statefulset")
			ctxs[0].App.SpecList = append(ctxs[0].App.SpecList[:i], ctxs[0].App.SpecList[i+1:]...)
			break
		}
	}
	// remove statefulset from preMigrationCtx as well
	for i, spec := range preMigrationCtx.App.SpecList {
		if _, ok := spec.(*apps_api.StatefulSet); ok {
			preMigrationCtx.App.SpecList = append(preMigrationCtx.App.SpecList[:i],
				preMigrationCtx.App.SpecList[i+1:]...)
			break
		}
	}

	// delete pvcs
	for _, pvc := range pvcs.Items {
		err := core.Instance().DeletePersistentVolumeClaim(pvc.Name, pvc.Namespace)
		require.NoError(t, err, "Error deleting pvc")
	}

	// bump time of the world by 5 minutes
	mockNow := time.Now().Add(6 * time.Minute)
	err = setMockTime(&mockNow)
	require.NoError(t, err, "Error setting mock time")

	// verify app deleted on source cluster with second migration
	time.Sleep(1 * time.Minute)
	validateMigration(t, instanceID, preMigrationCtx.GetID())
	validateMigrationCleanup(t, name, namespace, pvcs)

	validateAndDestroyMigration(t, ctxs, instanceID, appKey, preMigrationCtx, true, false, true, false, false, true)

	// If we are here then the test has passed
	testResult = testResultPass
	logrus.Infof("Test status at end of %s test: %s", t.Name(), testResult)
}

func validateMigrationCleanup(t *testing.T, name, namespace string, pvcs *v1.PersistentVolumeClaimList) {
	// validate if statefulset got deleted on cluster2
	err := setDestinationKubeConfig()
	require.NoError(t, err, "failed to set kubeconfig to destination cluster: %v", err)

	// Verify if statefulset get delete
	_, err = apps.Instance().GetStatefulSet(name, namespace)
	require.Error(t, err, "expected ss:%v error not found", name)

	for _, pvc := range pvcs.Items {
		resp, err := core.Instance().GetPersistentVolumeClaim(pvc.Name, pvc.Namespace)
		if err == nil {
			require.NotNil(t, resp.DeletionTimestamp)
		} else {
			require.Error(t, err, "expected pvc to be deleted:%v", resp.Name)
		}
	}

	err = setSourceKubeConfig()
	require.NoError(t, err, "Error resetting remote config")
}

func validateMigration(t *testing.T, name, namespace string) {
	//  ensure only one migration has run
	migrationsMap, err := storkops.Instance().ValidateMigrationSchedule(
		name, namespace, defaultWaitTimeout, defaultWaitInterval)
	require.NoError(t, err, "error getting migration schedule")
	require.Len(t, migrationsMap, 1, "expected only one schedule type in migration map")

	migrationStatus := migrationsMap[v1alpha1.SchedulePolicyTypeInterval][0]
	// Independently validate the migration
	err = storkops.Instance().ValidateMigration(
		migrationStatus.Name, namespace, defaultWaitTimeout, defaultWaitInterval)
	require.NoError(t, err, "failed to validate migration")
}

func migrationDailyScheduleTest(t *testing.T) {
	var testrailID, testResult = 50813, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult)
	defer updateTestRail(&testResult, testrailID, runID)

	migrationScheduleTest(t, v1alpha1.SchedulePolicyTypeDaily, "mysql-migration-schedule-daily", "", -1)

	// If we are here then the test has passed
	testResult = testResultPass
	logrus.Infof("Test status at end of %s test: %s", t.Name(), testResult)
}

func migrationWeeklyScheduleTest(t *testing.T) {
	var testrailID, testResult = 50814, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult)
	defer updateTestRail(&testResult, testrailID, runID)

	migrationScheduleTest(t, v1alpha1.SchedulePolicyTypeWeekly, "mysql-migration-schedule-weekly", "Monday", -1)

	// If we are here then the test has passed
	testResult = testResultPass
	logrus.Infof("Test status at end of %s test: %s", t.Name(), testResult)
}

func migrationMonthlyScheduleTest(t *testing.T) {
	var testrailID, testResult = 50815, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult)
	defer updateTestRail(&testResult, testrailID, runID)

	migrationScheduleTest(t, v1alpha1.SchedulePolicyTypeMonthly, "mysql-migration-schedule-monthly", "", 11)

	// If we are here then the test has passed
	testResult = testResultPass
	logrus.Infof("Test status at end of %s test: %s", t.Name(), testResult)
}

func migrationScheduleInvalidTest(t *testing.T) {
	var testrailID, testResult = 50816, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult)
	defer updateTestRail(&testResult, testrailID, runID)
	instanceID := "mysql-migration-schedule-invalid"
	appKey := "mysql-1-pvc"

	migrationSchedules := []string{
		"mysql-migration-schedule-daily-invalid",
		"mysql-migration-schedule-weekly-invalid",
		"mysql-migration-schedule-monthly-invalid",
	}

	ctxs, preMigrationCtx := triggerMigration(
		t,
		instanceID,
		appKey,
		nil,
		migrationSchedules,
		true,
		false,
		false,
		false,
		"",
		nil,
	)

	namespace := preMigrationCtx.GetID()
	time.Sleep(90 * time.Second)

	// **** TEST ensure 0 migrations since the schedule is invalid. Also check events for invalid specs
	for _, migrationScheduleName := range migrationSchedules {
		migrationSchedule, err := storkops.Instance().GetMigrationSchedule(migrationScheduleName, namespace)
		require.NoError(t, err, fmt.Sprintf("failed to get migration schedule: [%s] %s", namespace, migrationScheduleName))
		require.Empty(t, migrationSchedule.Status.Items, "expected 0 items in migration schedule status")

		listOptions := meta_v1.ListOptions{
			FieldSelector: fields.OneTermEqualSelector("involvedObject.name", migrationScheduleName).String(),
			Watch:         false,
		}

		storkEvents, err := core.Instance().ListEvents(namespace, listOptions)
		require.NoError(t, err, "failed to list stork events")

		foundFailedEvent := false
		for _, ev := range storkEvents.Items {
			if ev.Reason == "Failed" && strings.Contains(ev.Message, "Error checking if migration should be triggered: Invalid") {
				foundFailedEvent = true
				break
			}
		}

		require.True(t, foundFailedEvent,
			fmt.Sprintf("failed to find an event for the migration schedule: [%s] %s for the invalid policy",
				namespace, migrationScheduleName))
	}

	destroyAndWait(t, ctxs)

	// If we are here then the test has passed
	testResult = testResultPass
	logrus.Infof("Test status at end of %s test: %s", t.Name(), testResult)
}

// NOTE: below test assumes all schedule policies used here  (interval, daily, weekly or monthly) have a
// trigger time of 12:05PM. Ensure the SchedulePolicy specs use that time.
func migrationScheduleTest(
	t *testing.T,
	scheduleType v1alpha1.SchedulePolicyType,
	migrationScheduleName string,
	scheduleDay string,
	scheduleDate int) {
	var err error
	// Reset config in case of error
	defer func() {
		err = setSourceKubeConfig()
		require.NoError(t, err, "Error resetting remote config")
	}()

	date := time.Now().Day()
	if scheduleDate > 0 {
		date = scheduleDate
	} else if len(scheduleDay) > 0 {
		// find next available date (e.g 17th) for the given day (e.g Monday)
		currTime := time.Now()
		found := false
		for i := 0; i < 7; i++ {
			if currTime.Weekday().String() == scheduleDay {
				date = currTime.Day()
				found = true
				break
			}

			currTime = currTime.Add(24 * time.Hour) // move to next day
		}

		require.True(t, found, fmt.Sprintf("failed to find date from given schedule day: %s", scheduleDay))
	}

	month := time.Now().Month()
	// Increment the month if we went to the next
	if date < time.Now().Day() {
		month++
	}
	// Increment the year if we went to the next
	year := time.Now().Year()
	if month < time.Now().Month() {
		year++
	}
	nextTrigger := time.Date(year, month, date, 12, 4, 0, 0, time.Local)
	// Set time 2 hours before the scheduled time so no migrations run
	mockNow := nextTrigger.Add(-2 * time.Hour)

	err = setMockTime(&mockNow)
	require.NoError(t, err, "Error setting mock time")

	ctxs, preMigrationCtx := triggerMigration(
		t,
		migrationScheduleName,
		"mysql-1-pvc",
		nil,
		[]string{migrationScheduleName},
		true,
		false,
		false,
		false,
		"",
		nil,
	)

	namespace := preMigrationCtx.GetID()
	failureErrString := fmt.Sprintf("basic validation of migration schedule: [%s] %s failed",
		namespace, migrationScheduleName)

	// **** TEST 1: ensure 0 migrations since we haven't reached the daily scheduled time
	migrationSchedule, err := storkops.Instance().GetMigrationSchedule(migrationScheduleName, namespace)
	require.NoError(t, err, "failed to get migration schedule")
	require.Empty(t, migrationSchedule.Status.Items, "expected 0 items in migration schedule status")

	// **** TEST 2: bump time one minute past the scheduled time of daily migration
	mockNow = nextTrigger.Add(1 * time.Minute)
	err = setMockTime(&mockNow)
	require.NoError(t, err, "Error setting mock time")

	//  ensure only one migration has run
	migrationsMap, err := storkops.Instance().ValidateMigrationSchedule(
		migrationScheduleName, namespace, defaultWaitTimeout, defaultWaitInterval)
	require.NoError(t, err, failureErrString)
	require.Len(t, migrationsMap, 1, "expected only one schedule type in migration map")

	migrations := migrationsMap[scheduleType]
	require.Len(t, migrations, 1, fmt.Sprintf("expected exactly one %v migration. Found: %d", scheduleType, len(migrations)))

	migrationStatus := migrations[0]

	// Independently validate the migration
	err = storkops.Instance().ValidateMigration(migrationStatus.Name, namespace, 1*time.Minute, defaultWaitInterval)
	require.NoError(t, err, "failed to validate first daily migration")

	// check creation time of the new migration
	firstMigrationCreationTime := migrationStatus.CreationTimestamp
	require.True(t, mockNow.Before(firstMigrationCreationTime.Time) || mockNow.Equal(firstMigrationCreationTime.Time),
		fmt.Sprintf("creation of migration: %v should have been after or equal to: %v",
			firstMigrationCreationTime, mockNow))

	// **** TEST 3 bump time by 2 more hours. Should not cause any new migrations
	mockNow = nextTrigger.Add(2 * time.Hour)
	err = setMockTime(&mockNow)
	require.NoError(t, err, "Error setting mock time")

	//  ensure no new migrations
	migrationsMap, err = storkops.Instance().ValidateMigrationSchedule(
		migrationScheduleName, namespace, defaultWaitTimeout, defaultWaitInterval)
	require.NoError(t, err, failureErrString)
	require.Len(t, migrationsMap, 1, "expected only one schedule type in migration map")

	migrations = migrationsMap[scheduleType]
	require.Len(t, migrations, 1, fmt.Sprintf("expected exactly one %v migration. Found: %d",
		scheduleType, len(migrations)))
	require.Equal(t, firstMigrationCreationTime, migrations[0].CreationTimestamp,
		"timestamps of first and most recent migrations don't match")

	// **** TEST 4 bump time by (1 day / 1 week / 1 month) + 5 minutes. Should cause one new migration
	switch scheduleType {
	case v1alpha1.SchedulePolicyTypeDaily:
		mockNow = nextTrigger.AddDate(0, 0, 1)
	case v1alpha1.SchedulePolicyTypeWeekly:
		mockNow = nextTrigger.AddDate(0, 0, 7)
	case v1alpha1.SchedulePolicyTypeMonthly:
		mockNow = nextTrigger.AddDate(0, 1, 0)
	default:
		t.Fatalf("this testcase only supports daily, weekly and monthly intervals")
	}
	mockNow = mockNow.Add(5 * time.Minute)
	err = setMockTime(&mockNow)
	require.NoError(t, err, "Error setting mock time")

	// Give time for new migration to trigger
	time.Sleep(time.Minute)

	for i := 0; i < 10; i++ {
		migrationsMap, err = storkops.Instance().ValidateMigrationSchedule(
			migrationScheduleName, namespace, defaultWaitTimeout, defaultWaitInterval)
		require.NoError(t, err, failureErrString)
		require.Len(t, migrationsMap, 1, "expected only one schedule type in migration map")

		migrations = migrationsMap[scheduleType]
		// If there are more than 1 migrations, the prune might still be in
		// progress, so retry after a short sleep
		if len(migrations) == 1 {
			break
		}
		time.Sleep(10 * time.Second)
	}
	require.Len(t, migrations, 1, fmt.Sprintf("expected exactly one %v migration. Found: %d", scheduleType, len(migrations)))

	migrationStatus = migrations[0]
	require.True(t, firstMigrationCreationTime.Time.Before(migrationStatus.CreationTimestamp.Time),
		fmt.Sprintf("creation of migration: %v should have been after: %v the first migration",
			firstMigrationCreationTime, migrationStatus.CreationTimestamp))

	// validate and destroy apps on both clusters
	validateAndDestroyMigration(t, ctxs, migrationScheduleName, "mysql-1-pvc", preMigrationCtx, true, false, true, false, false, false)

	// explicitly check if all child migrations of the schedule are deleted
	f := func() (interface{}, bool, error) {
		for _, migrations := range migrationsMap {
			for _, m := range migrations {
				_, err := storkops.Instance().GetMigration(m.Name, namespace)
				if err == nil {
					return "", true, fmt.Errorf("get on migration: %s should have failed", m.Name)
				}

				if !errors.IsNotFound(err) {
					logrus.Infof("unexpected err: %v when checking deleted migration: %s", err, m.Name)
					return "", true, err
				}
			}
		}

		//done all deleted
		return "", false, nil
	}

	_, err = task.DoRetryWithTimeout(f, defaultWaitTimeout, defaultWaitInterval)
	require.NoError(t, err, "migrations for schedules have not been deleted")
}

func migrationScaleTest(t *testing.T) {
	var testrailID, testResult = 86247, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult)
	defer updateTestRail(&testResult, testrailID, runID)
	instanceID := "mysql-migration"
	appKey := "mysql-1-pvc"

	triggerMigrationScaleTest(
		t,
		instanceID,
		appKey,
		true,
		true,
		true,
	)

	// If we are here then the test has passed
	testResult = testResultPass
	logrus.Infof("Test status at end of %s test: %s", t.Name(), testResult)
}

func triggerMigrationScaleTest(t *testing.T, migrationKey, migrationAppKey string, includeResourcesFlag, includeVolumesFlag, startApplicationsFlag bool) {
	var appCtxs []*scheduler.Context
	var ctxs []*scheduler.Context
	var allMigrations []*v1alpha1.Migration
	var err error
	instanceID := migrationKey
	appKey := migrationAppKey

	// Reset config in case of error
	defer func() {
		err = setRemoteConfig("")
		require.NoError(t, err, "Error resetting remote config")
	}()

	for i := 1; i <= migrationScaleCount; i++ {
		currCtxs, err := schedulerDriver.Schedule(migrationKey+"-"+strconv.Itoa(i),
			scheduler.ScheduleOptions{AppKeys: []string{migrationAppKey}})
		require.NoError(t, err, "Error scheduling task")
		require.Equal(t, 1, len(currCtxs), "Only one task should have started")

		err = schedulerDriver.WaitForRunning(currCtxs[0], defaultWaitTimeout, defaultWaitInterval)
		require.NoError(t, err, "Error waiting for app to get to running state")

		// Save context without the clusterpair object
		preMigrationCtx := currCtxs[0].DeepCopy()
		appCtxs = append(appCtxs, preMigrationCtx)

		// Schedule bidirectional or regular cluster pair based on the flag
		scheduleClusterPairGeneric(t, currCtxs, appKey, instanceID+"-"+strconv.Itoa(i), defaultClusterPairDir, projectIDMappings, false, true, false)
		ctxs = append(ctxs, currCtxs...)

		currMigNamespace := migrationAppKey + "-" + migrationKey + "-" + strconv.Itoa(i)
		currMig, err := createMigration(t, migrationKey, currMigNamespace, "remoteclusterpair", currMigNamespace, &includeResourcesFlag, &includeVolumesFlag, &startApplicationsFlag)
		require.NoError(t, err, "failed to create migration: %s in namespace %s", migrationKey, currMigNamespace)
		allMigrations = append(allMigrations, currMig)

	}
	err = WaitForMigration(allMigrations)
	require.NoError(t, err, "Error in scaled migrations")

	// Validate apps on destination
	if startApplicationsFlag {
		// wait on cluster 2 for the app to be running
		err = setDestinationKubeConfig()
		require.NoError(t, err, "failed to set kubeconfig to destination cluster: %v", err)

		for _, ctx := range appCtxs {
			err = schedulerDriver.WaitForRunning(ctx, defaultWaitTimeout, defaultWaitInterval)
			require.NoError(t, err, "Error waiting for app to get to running state on destination cluster")
		}
		// Delete all apps on destination cluster
		destroyAndWait(t, appCtxs)
	}

	err = setSourceKubeConfig()
	require.NoError(t, err, "failed to set kubeconfig to source cluster: %v", err)

	// Delete all apps and cluster pairs on source cluster
	destroyAndWait(t, ctxs)

	// Delete migrations
	err = deleteMigrations(allMigrations)
	require.NoError(t, err, "error in deleting migrations.")
}

func clusterPairFailuresTest(t *testing.T) {
	var testrailID, testResult = 86248, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult)
	defer updateTestRail(&testResult, testrailID, runID)

	ctxs, err := schedulerDriver.Schedule("cluster-pair-failures",
		scheduler.ScheduleOptions{AppKeys: []string{testKey}})
	require.NoError(t, err, "Error scheduling task")
	require.Equal(t, 1, len(ctxs), "Only one task should have started")

	err = schedulerDriver.WaitForRunning(ctxs[0], defaultWaitTimeout, defaultWaitInterval)
	require.NoError(t, err, "Error waiting for app to get to running state")

	var clusterPairCtx = &scheduler.Context{
		UID:             ctxs[0].UID,
		ScheduleOptions: scheduler.ScheduleOptions{Namespace: "cp-failure"},
		App: &spec.AppSpec{
			Key:      ctxs[0].App.Key,
			SpecList: []interface{}{},
		}}

	badTokenInfo, errPairing := volumeDriver.GetClusterPairingInfo(remoteFilePath, "", IsCloud(), false)
	require.NoError(t, errPairing, "Error writing to clusterpair.yml: %v")

	// Change token value to an incorrect token
	badTokenInfo[tokenKey] = "randomtoken"
	err = createClusterPair(badTokenInfo, false, true, defaultClusterPairDir, "")
	require.NoError(t, err, "Error creating cluster Spec: %v")

	err = schedulerDriver.RescanSpecs(specDir, volumeDriverName)
	require.NoError(t, err, "Unable to parse spec dir: %v")

	err = schedulerDriver.AddTasks(clusterPairCtx,
		scheduler.ScheduleOptions{AppKeys: []string{defaultClusterPairDir}})
	require.NoError(t, err, "Failed to schedule Cluster Pair Specs: %v")

	// We would like to exit early in case of negative tests
	err = schedulerDriver.WaitForRunning(clusterPairCtx, defaultWaitTimeout/10, defaultWaitInterval)
	require.Error(t, err, "Cluster pairing should have failed due to incorrect token")

	destroyAndWait(t, []*scheduler.Context{clusterPairCtx})

	badIPInfo, errPairing := volumeDriver.GetClusterPairingInfo(remoteFilePath, "", IsCloud(), false)
	require.NoError(t, errPairing, "Error writing to clusterpair.yml: %v")

	badIPInfo[clusterIP] = "0.0.0.0"

	err = createClusterPair(badIPInfo, false, true, defaultClusterPairDir, "")
	require.NoError(t, err, "Error creating cluster Spec: %v")

	err = schedulerDriver.RescanSpecs(specDir, volumeDriverName)
	require.NoError(t, err, "Unable to parse spec dir: %v")

	err = schedulerDriver.AddTasks(clusterPairCtx,
		scheduler.ScheduleOptions{AppKeys: []string{defaultClusterPairDir}})
	require.NoError(t, err, "Failed to schedule Cluster Pair Specs: %v")

	// We would like to exit early in case of negative tests
	err = schedulerDriver.WaitForRunning(clusterPairCtx, defaultWaitTimeout/10, defaultWaitInterval)
	require.Error(t, err, "Cluster pairing should have failed due to incorrect IP")

	destroyAndWait(t, []*scheduler.Context{clusterPairCtx})

	badPortInfo, errPairing := volumeDriver.GetClusterPairingInfo(remoteFilePath, "", IsCloud(), false)
	require.NoError(t, errPairing, "Error writing to clusterpair.yml: %v")

	badPortInfo[clusterPort] = "0000"

	err = createClusterPair(badPortInfo, false, true, defaultClusterPairDir, "")
	require.NoError(t, err, "Error creating cluster Spec: %v")

	err = schedulerDriver.RescanSpecs(specDir, volumeDriverName)
	require.NoError(t, err, "Unable to parse spec dir: %v")

	err = schedulerDriver.AddTasks(clusterPairCtx,
		scheduler.ScheduleOptions{AppKeys: []string{defaultClusterPairDir}})
	require.NoError(t, err, "Failed to schedule Cluster Pair Specs")

	// We would like to exit early in case of negative tests
	err = schedulerDriver.WaitForRunning(clusterPairCtx, defaultWaitTimeout/10, defaultWaitInterval)
	require.Error(t, err, "Cluster pairing should have failed due to incorrect port")

	destroyAndWait(t, []*scheduler.Context{clusterPairCtx})
	destroyAndWait(t, ctxs)

	// If we are here then the test has passed
	testResult = testResultPass
	logrus.Infof("Test status at end of %s test: %s", t.Name(), testResult)
}

func bidirectionalClusterPairTest(t *testing.T) {
	if bidirectionalClusterpair == false {
		t.Skipf("skipping %s test because bidirectional cluster pair flag has not been set", t.Name())
	}
	var testrailID, testResult = 86249, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult)
	defer updateTestRail(&testResult, testrailID, runID)

	clusterPairName := "birectional-cluster-pair"
	clusterPairNamespace := "bidirectional-clusterpair-ns"

	// Read the configmap secret-config in default namespace and based on the secret type , create the bidirectional pair
	configMap, err := core.Instance().GetConfigMap("secret-config", "default")
	if err != nil {
		require.NoError(t, err, "error getting configmap secret-config in defaule namespace: %v")
	}
	cmData := configMap.Data
	// Scheduler cluster pairs: source cluster --> destination cluster and destination cluster --> source cluster
	for location, secret := range cmData {
		logrus.Infof("Creating a bidirectional-pair using %s as objectstore.", location)
		err := scheduleBidirectionalClusterPair(clusterPairName, clusterPairNamespace, "", v1alpha1.BackupLocationType(location), secret)
		require.NoError(t, err, "failed to set bidirectional cluster pair: %v", err)

		err = setSourceKubeConfig()
		require.NoError(t, err, "failed to set kubeconfig to source cluster: %v", err)

		_, err = storkops.Instance().GetClusterPair(clusterPairName, clusterPairNamespace)
		require.NoError(t, err, "failed to get bidirectional cluster pair on source: %v", err)

		err = storkops.Instance().ValidateClusterPair(clusterPairName, clusterPairNamespace, defaultWaitTimeout, defaultWaitInterval)
		require.NoError(t, err, "failed to validate bidirectional cluster pair on source: %v", err)

		logrus.Infof("Successfully validated cluster pair %s in namespace %s on source cluster", clusterPairName, clusterPairNamespace)

		// Clean up on source cluster
		err = storkops.Instance().DeleteClusterPair(clusterPairName, clusterPairNamespace)
		require.NoError(t, err, "Error deleting clusterpair on source cluster")

		logrus.Infof("Successfully deleted cluster pair %s in namespace %s on source cluster", clusterPairName, clusterPairNamespace)

		// Clean up destination cluster while we are on it
		err = core.Instance().DeleteNamespace(clusterPairNamespace)
		require.NoError(t, err, "failed to delete namespace %s on destination cluster", clusterPairNamespace)

		err = setDestinationKubeConfig()
		require.NoError(t, err, "failed to set kubeconfig to destination cluster: %v", err)

		_, err = storkops.Instance().GetClusterPair(clusterPairName, clusterPairNamespace)
		require.NoError(t, err, "failed to get bidirectional cluster pair on destination: %v", err)

		err = storkops.Instance().ValidateClusterPair(clusterPairName, clusterPairNamespace, defaultWaitTimeout, defaultWaitInterval)
		require.NoError(t, err, "failed to validate bidirectional cluster pair on destination: %v", err)

		logrus.Infof("Successfully validated cluster pair %s in namespace %s on destination cluster", clusterPairName, clusterPairNamespace)

		// Clean up on destination cluster
		err = storkops.Instance().DeleteClusterPair(clusterPairName, clusterPairNamespace)
		require.NoError(t, err, "Error deleting clusterpair on destination cluster")

		// Clean up destination cluster while we are on it
		err = core.Instance().DeleteNamespace(clusterPairNamespace)
		require.NoError(t, err, "failed to delete namespace %s on destination cluster", clusterPairNamespace)

		logrus.Infof("Successfully deleted cluster pair %s in namespace %s on destination cluster", clusterPairName, clusterPairNamespace)

		err = setSourceKubeConfig()
		require.NoError(t, err, "failed to set kubeconfig to source cluster: %v", err)

		logrus.Infof("Successfully tested creating a bidirectional-pair using %s as objectstore.", location)
	}
	// If we are here then the test has passed
	testResult = testResultPass
	logrus.Infof("Test status at end of %s test: %s", t.Name(), testResult)
}

func unidirectionalClusterPairTest(t *testing.T) {
	if unidirectionalClusterpair == false {
		t.Skipf("skipping %s test because unidirectional cluster pair flag has not been set", t.Name())
	}
	var testrailID, testResult = 91979, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult)
	defer updateTestRail(&testResult, testrailID, runID)

	clusterPairName := "unirectional-cluster-pair"
	clusterPairNamespace := "unidirectional-clusterpair-ns"

	// Read the configmap secret-config in default namespace and based on the secret type , create the bidirectional pair
	configMap, err := core.Instance().GetConfigMap("secret-config", "default")
	if err != nil {
		require.NoError(t, err, "error getting configmap secret-config in defaule namespace: %v")
	}
	cmData := configMap.Data
	// Scheduler cluster pairs: source cluster --> destination cluster and destination cluster --> source cluster
	for location, secret := range cmData {
		logrus.Infof("Creating a unidirectional-pair using %s as objectstore.", location)
		err := scheduleUnidirectionalClusterPair(clusterPairName, clusterPairNamespace, "", v1alpha1.BackupLocationType(location), secret, true, false)
		require.NoError(t, err, "failed to set unidirectional cluster pair: %v", err)

		err = setSourceKubeConfig()
		require.NoError(t, err, "failed to set kubeconfig to source cluster: %v", err)

		_, err = storkops.Instance().GetClusterPair(clusterPairName, clusterPairNamespace)
		require.NoError(t, err, "failed to get unidirectional cluster pair on source: %v", err)

		err = storkops.Instance().ValidateClusterPair(clusterPairName, clusterPairNamespace, defaultWaitTimeout, defaultWaitInterval)
		require.NoError(t, err, "failed to validate unidirectional cluster pair on source: %v", err)

		logrus.Infof("Successfully validated cluster pair %s in namespace %s on source cluster", clusterPairName, clusterPairNamespace)

		err = setDestinationKubeConfig()
		require.NoError(t, err, "failed to set kubeconfig to destination cluster: %v", err)

		_, err = storkops.Instance().GetClusterPair(clusterPairName, clusterPairNamespace)
		require.Error(t, err, "clusterpair should not be created on destination cluster")

		err = core.Instance().DeleteNamespace(clusterPairNamespace)
		require.NoError(t, err, "failed to delete namespace %s on destination cluster", clusterPairNamespace)

		err = setSourceKubeConfig()
		require.NoError(t, err, "failed to set kubeconfig to source cluster: %v", err)

		// Clean up on source cluster
		err = storkops.Instance().DeleteClusterPair(clusterPairName, clusterPairNamespace)
		require.NoError(t, err, "Error deleting clusterpair on source cluster")

		logrus.Infof("Successfully deleted cluster pair %s in namespace %s on source cluster", clusterPairName, clusterPairNamespace)

		err = core.Instance().DeleteNamespace(clusterPairNamespace)
		require.NoError(t, err, "failed to delete namespace %s on source cluster", clusterPairNamespace)
	}
	// If we are here then the test has passed
	testResult = testResultPass
	logrus.Infof("Test status at end of %s test: %s", t.Name(), testResult)
}

func operatorMigrationMongoTest(t *testing.T) {
	var testrailID, testResult = 86250, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult)
	defer updateTestRail(&testResult, testrailID, runID)

	triggerMigrationTest(
		t,
		"migration",
		"mongo-operator",
		nil,
		"mongo-op-migration",
		true,
		true,
		true,
		true,
	)

	// If we are here then the test has passed
	testResult = testResultPass
	logrus.Infof("Test status at end of %s test: %s", t.Name(), testResult)
}

func operatorMigrationRabbitmqTest(t *testing.T) {
	var testrailID, testResult = 86251, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult)
	defer updateTestRail(&testResult, testrailID, runID)

	_, err := core.Instance().CreateNamespace(&v1.Namespace{
		ObjectMeta: meta_v1.ObjectMeta{
			Name: rabbitmqNamespace,
			Labels: map[string]string{
				"app.kubernetes.io/component": "rabbitmq-operator",
				"app.kubernetes.io/name":      "rabbitmq-system",
				"app.kubernetes.io/part-of":   "rabbitmq",
			},
		},
	})
	if !errors.IsAlreadyExists(err) {
		require.NoError(t, err, "failed to create namespace %s for rabbitmq", rabbitmqNamespace)
	}

	triggerMigrationTest(
		t,
		"migration",
		"rabbitmq-operator",
		nil,
		"rabbitmq-migration",
		true,
		true,
		true,
		true,
	)

	// If we are here then the test has passed
	testResult = testResultPass
	logrus.Infof("Test status at end of %s test: %s", t.Name(), testResult)
}

func createMigration(
	t *testing.T,
	name string,
	namespace string,
	clusterPair string,
	migrationNamespace string,
	includeResources *bool,
	includeVolumes *bool,
	startApplications *bool,
) (*v1alpha1.Migration, error) {

	migration := &v1alpha1.Migration{
		ObjectMeta: meta_v1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: v1alpha1.MigrationSpec{
			ClusterPair:       clusterPair,
			IncludeResources:  includeResources,
			IncludeVolumes:    includeVolumes,
			StartApplications: startApplications,
			Namespaces:        []string{migrationNamespace},
		},
	}
	if authTokenConfigMap != "" {
		err := addSecurityAnnotation(migration)
		if err != nil {
			return nil, err
		}
	}

	mig, err := storkops.Instance().CreateMigration(migration)
	return mig, err
}

func deleteMigrations(migrations []*v1alpha1.Migration) error {
	for _, mig := range migrations {
		err := storkops.Instance().DeleteMigration(mig.Name, mig.Namespace)
		if err != nil {
			return fmt.Errorf("Failed to delete migration %s in namespace %s. Error: %v", mig.Name, mig.Namespace, err)
		}
	}
	return nil
}

func WaitForMigration(migrationList []*v1alpha1.Migration) error {
	checkMigrations := func() (interface{}, bool, error) {
		isComplete := true
		for _, m := range migrationList {
			mig, err := storkops.Instance().GetMigration(m.Name, m.Namespace)
			if err != nil {
				return "", false, err
			}
			if mig.Status.Status != v1alpha1.MigrationStatusSuccessful {
				logrus.Infof("Migration %s in namespace %s is pending", m.Name, m.Namespace)
				isComplete = false
			}
		}
		if isComplete {
			return "", false, nil
		}
		return "", true, fmt.Errorf("some migrations are still pending")
	}
	_, err := task.DoRetryWithTimeout(checkMigrations, migrationRetryTimeout, migrationRetryInterval)
	return err
}

// pvcResizeMigrationTest validate migrated pvcs size get
// reflected correctly after change on source pvc
// it also make sure that sc param is kept for migrated pvcs
func pvcResizeMigrationTest(t *testing.T) {
	var testrailID, testResult = 86252, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult)
	defer updateTestRail(&testResult, testrailID, runID)
	instanceID := "mysql-migration-schedule-interval"
	appKey := "mysql-1-pvc"

	var err error
	var namespace string
	var pvcs *v1.PersistentVolumeClaimList
	// Reset config in case of error
	defer func() {
		err = setSourceKubeConfig()
		require.NoError(t, err, "Error resetting remote config")
	}()
	err = setMockTime(nil)
	require.NoError(t, err, "Error resetting mock time")
	// the schedule interval for these specs it set to 5 minutes
	ctxs, preMigrationCtx := triggerMigration(
		t,
		instanceID,
		appKey,
		[]string{"cassandra"},
		[]string{"mysql-migration-schedule-interval"},
		true,
		false,
		false,
		false,
		"",
		nil,
	)

	validateMigration(t, instanceID, preMigrationCtx.GetID())

	// Get pvc lists from source cluster
	for _, spec := range ctxs[0].App.SpecList {
		if obj, ok := spec.(*apps_api.StatefulSet); ok {
			namespace = obj.GetNamespace()
			pvcs, err = core.Instance().GetPersistentVolumeClaims(namespace, nil)
			require.NoError(t, err, "error retriving pvc list from %s namespace", namespace)
			break
		}
	}

	// resize pvcs
	for _, pvc := range pvcs.Items {
		cap := pvc.Status.Capacity[v1.ResourceName(v1.ResourceStorage)]
		currSize := cap.Value()
		pvc.Spec.Resources.Requests[v1.ResourceStorage] = *resource.NewQuantity(int64(currSize*2), resource.BinarySI)
		_, err := core.Instance().UpdatePersistentVolumeClaim(&pvc)
		require.NoError(t, err, "Error updating pvc: %s/%s", pvc.GetNamespace(), pvc.GetName())
	}
	logrus.Infof("Resized PVCs on source cluster")
	// bump time of the world by 5 minutes
	mockNow := time.Now().Add(6 * time.Minute)
	err = setMockTime(&mockNow)
	require.NoError(t, err, "Error setting mock time")

	time.Sleep(10 * time.Second)
	logrus.Infof("Trigger second migration")
	validateMigration(t, "mysql-migration-schedule-interval", preMigrationCtx.GetID())

	// Change kubeconfig to destination
	err = setDestinationKubeConfig()
	require.NoError(t, err, "failed to set kubeconfig to source cluster: %v", err)

	// verify resized pvcs
	for _, pvc := range pvcs.Items {
		cap := pvc.Status.Capacity[v1.ResourceName(v1.ResourceStorage)]
		pvcSize := cap.Value()

		migratedPvc, err := core.Instance().GetPersistentVolumeClaim(pvc.GetName(), pvc.GetNamespace())
		require.NoError(t, err, "error retriving pvc %s/%s", pvc.GetNamespace(), pvc.GetName())

		cap = migratedPvc.Status.Capacity[v1.ResourceName(v1.ResourceStorage)]
		migrSize := cap.Value()

		if migrSize != pvcSize*2 {
			resizeErr := fmt.Errorf("pvc %s/%s is not resized. Expected: %v, Current: %v", pvc.GetNamespace(), pvc.GetName(), pvcSize*2, migrSize)
			require.NoError(t, resizeErr, resizeErr)
		}
		srcSC, err := getStorageClassNameForPVC(&pvc)
		require.NoError(t, err, "error retriving sc for %s/%s", pvc.GetNamespace(), pvc.GetName())

		destSC, err := getStorageClassNameForPVC(migratedPvc)
		require.NoError(t, err, "error retriving sc for %s/%s", migratedPvc.GetNamespace(), migratedPvc.GetName())

		if srcSC != destSC {
			scErr := fmt.Errorf("migrated pvc storage class does not match")
			require.NoError(t, scErr, "SC Expected: %v, Current: %v", srcSC, destSC)
		}
	}

	logrus.Infof("Successfully verified migrated pvcs on destination cluster")

	err = setSourceKubeConfig()
	require.NoError(t, err, "failed to set kubeconfig to source cluster: %v", err)
	validateAndDestroyMigration(t, ctxs, instanceID, appKey, preMigrationCtx, true, false, true, false, false, true)

	// If we are here then the test has passed
	testResult = testResultPass
	logrus.Infof("Test status at end of %s test: %s", t.Name(), testResult)
}

func getStorageClassNameForPVC(pvc *v1.PersistentVolumeClaim) (string, error) {
	var scName string
	if pvc.Spec.StorageClassName != nil && len(*pvc.Spec.StorageClassName) > 0 {
		scName = *pvc.Spec.StorageClassName
	} else {
		scName = pvc.Annotations[v1.BetaStorageClassAnnotation]
	}

	if len(scName) == 0 {
		return "", fmt.Errorf("PVC: %s does not have a storage class", pvc.Name)
	}
	return scName, nil
}

func suspendMigrationTest(t *testing.T) {
	var err error
	var namespace string
	// Reset config in case of error
	defer func() {
		err = setSourceKubeConfig()
		require.NoError(t, err, "Error resetting remote config")
	}()
	err = setMockTime(nil)
	require.NoError(t, err, "Error resetting mock time")
	instanceID := "mysql-migration-schedule-interval-autosuspend"
	appKey := "mysql-1-pvc"

	// the schedule interval for these specs it set to 5 minutes
	ctxs, preMigrationCtx := triggerMigration(
		t,
		instanceID,
		appKey,
		[]string{"cassandra"},
		[]string{"mysql-migration-schedule-interval-autosuspend"},
		true,
		false,
		false,
		false,
		"",
		nil,
	)

	validateMigration(t, "mysql-migration-schedule-interval-autosuspend", preMigrationCtx.GetID())

	// Change kubeconfig to destination
	err = setDestinationKubeConfig()
	require.NoError(t, err, "failed to set kubeconfig to source cluster: %v", err)

	// start sts on dr cluster
	for _, spec := range ctxs[0].App.SpecList {
		if obj, ok := spec.(*apps_api.StatefulSet); ok {
			scale := int32(1)
			sts, err := apps.Instance().GetStatefulSet(obj.GetName(), obj.GetNamespace())
			require.NoError(t, err, "Error retriving sts: %s/%s", obj.GetNamespace(), obj.GetName())
			sts.Spec.Replicas = &scale
			namespace = obj.GetNamespace()
			_, err = apps.Instance().UpdateStatefulSet(sts)
			require.NoError(t, err, "Error updating sts: %s/%s", obj.GetNamespace(), obj.GetName())
			break
		}
	}

	// verify migration status on DR cluster
	migrSched, err := storkops.Instance().GetMigrationSchedule("mysql-migration-schedule-interval-autosuspend", namespace)
	require.NoError(t, err, "failed to retrive migration schedule: %v", err)

	if migrSched.Spec.Suspend != nil && !(*migrSched.Spec.Suspend) {
		// fail test
		suspendErr := fmt.Errorf("migrationschedule is not in suspended state on DR cluster: %s/%s", migrSched.GetName(), migrSched.GetNamespace())
		require.NoError(t, err, "Failed: %v", suspendErr)
	}

	// validate if migrationschedule is suspended on source cluster
	err = setSourceKubeConfig()
	require.NoError(t, err, "failed to set kubeconfig to source cluster: %v", err)

	migrSched, err = storkops.Instance().GetMigrationSchedule("mysql-migration-schedule-interval-autosuspend", namespace)
	require.NoError(t, err, "failed to retrive migration schedule: %v", err)

	if migrSched.Spec.Suspend != nil && !(*migrSched.Spec.Suspend) {
		// fail test
		suspendErr := fmt.Errorf("migrationschedule is not suspended on source cluster : %s/%s", migrSched.GetName(), migrSched.GetNamespace())
		require.NoError(t, err, "Failed: %v", suspendErr)
	}

	logrus.Infof("Successfully verified suspend migration case")

	validateAndDestroyMigration(t, ctxs, instanceID, appKey, preMigrationCtx, true, false, true, false, false, true)
}

func endpointMigrationTest(t *testing.T) {
	var testrailID, testResult = 86256, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult)
	defer updateTestRail(&testResult, testrailID, runID)
	instanceID := "endpoint-migration-schedule-interval"
	appKey := "endpoint"

	var err error
	namespace := "endpoint-migration-schedule-interval"
	// Reset config in case of error
	defer func() {
		err = setSourceKubeConfig()
		require.NoError(t, err, "Error resetting remote config")
	}()
	err = setMockTime(nil)
	require.NoError(t, err, "Error resetting mock time")
	// the schedule interval for these specs it set to 5 minutes
	ctxs, preMigrationCtx := triggerMigration(
		t,
		instanceID,
		appKey,
		[]string{},
		[]string{"endpoint-migration-schedule-interval"},
		true,
		false,
		false,
		false,
		"",
		nil,
	)

	validateMigration(t, "endpoint-migration-schedule-interval", preMigrationCtx.GetID())

	srcEndpoints, err := core.Instance().ListEndpoints(namespace, meta_v1.ListOptions{})
	require.NoError(t, err, "error retriving endpoints list from %s namespace", namespace)
	cnt := 0
	for _, endpoint := range srcEndpoints.Items {
		collect := false
		for _, subset := range endpoint.Subsets {
			for _, addr := range subset.Addresses {
				if addr.TargetRef != nil {
					collect = true
				}
			}
		}
		if !collect {
			cnt++
		}
	}
	logrus.Infof("endpoint on source cluster: %+v", srcEndpoints)

	// Change kubeconfig to destination
	err = setDestinationKubeConfig()
	require.NoError(t, err, "failed to set kubeconfig to source cluster: %v", err)

	destEndpoints, err := core.Instance().ListEndpoints(namespace, meta_v1.ListOptions{})
	require.NoError(t, err, "error retriving endpoints list from %s namespace", namespace)

	if len(destEndpoints.Items) != cnt {
		matchErr := fmt.Errorf("migrated endpoints does not match")
		require.NoError(t, matchErr, "Endpoints Expected: %v, Current: %v", srcEndpoints, destEndpoints)
	}

	logrus.Infof("Successfully verified migrated endpoints on destination cluster")

	err = setSourceKubeConfig()
	require.NoError(t, err, "failed to set kubeconfig to source cluster: %v", err)
	validateAndDestroyMigration(t, ctxs, instanceID, appKey, preMigrationCtx, true, false, true, false, false, true)

	// If we are here then the test has passed
	testResult = testResultPass
	logrus.Infof("Test status at end of %s test: %s", t.Name(), testResult)
}

// networkPolicyMigrationTest validate migrated network policy
func networkPolicyMigrationTest(t *testing.T) {
	var testrailID, testResult = 86257, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult)
	defer updateTestRail(&testResult, testrailID, runID)

	// validate default behaviour of network policy migration where policy which has CIDR set
	// will not be migrated
	validateNetworkPolicyMigration(t, false)
	logrus.Infof("Validating migration of all network policy with IncludeNetworkPolicyWithCIDR set to true")
	// validate behaviour of network policy migration where policy which has CIDR set
	// will be migrated using IncludeNetworkPolicyWithCIDR option in migration schedule
	validateNetworkPolicyMigration(t, true)

	// If we are here then the test has passed
	testResult = testResultPass
	logrus.Infof("Test status at end of %s test: %s", t.Name(), testResult)
}
func validateNetworkPolicyMigration(t *testing.T, all bool) {
	var err error
	namespace := "networkpolicy-networkpolicy-migration-schedule-interval"
	scheduleName := "networkpolicy-migration-schedule-interval"
	if all {
		namespace = "networkpolicy-networkpolicy-all-migration-schedule-interval"
		scheduleName = "networkpolicy-all-migration-schedule-interval"
	}
	// Reset config in case of error
	defer func() {
		err = setSourceKubeConfig()
		require.NoError(t, err, "Error resetting remote config")
	}()
	err = setMockTime(nil)
	require.NoError(t, err, "Error resetting mock time")
	// the schedule interval for these specs it set to 5 minutes
	ctxs, preMigrationCtx := triggerMigration(
		t,
		scheduleName,
		"networkpolicy",
		[]string{},
		[]string{scheduleName},
		true,
		false,
		false,
		false,
		"",
		nil,
	)

	validateMigration(t, scheduleName, preMigrationCtx.GetID())

	networkPolicies, err := core.Instance().ListNetworkPolicy(namespace, meta_v1.ListOptions{})
	require.NoError(t, err, "error retriving network policy list from %s namespace", namespace)
	cnt := 0
	for _, networkPolicy := range networkPolicies.Items {
		collect := true
		ingressRule := networkPolicy.Spec.Ingress
		for _, ingress := range ingressRule {
			for _, fromPolicyPeer := range ingress.From {
				ipBlock := fromPolicyPeer.IPBlock
				if ipBlock != nil && len(ipBlock.CIDR) != 0 {
					collect = false
				}
			}
		}
		egreeRule := networkPolicy.Spec.Egress
		for _, egress := range egreeRule {
			for _, networkPolicyPeer := range egress.To {
				ipBlock := networkPolicyPeer.IPBlock
				if ipBlock != nil && len(ipBlock.CIDR) != 0 {
					collect = false
				}
			}
		}
		if collect {
			cnt++
		}
	}
	if all {
		cnt = len(networkPolicies.Items)
	}
	logrus.Infof("No. of network policies which does not have CIDR set: %d", cnt)

	// Change kubeconfig to destination
	err = setDestinationKubeConfig()
	require.NoError(t, err, "failed to set kubeconfig to source cluster: %v", err)

	destNetworkPolicies, err := core.Instance().ListNetworkPolicy(namespace, meta_v1.ListOptions{})
	require.NoError(t, err, "error retriving network policy list from %s namespace", namespace)

	if len(destNetworkPolicies.Items) != cnt {
		matchErr := fmt.Errorf("migrated network poilcy does not match")
		logrus.Infof("Policy found on source cluster")
		for _, networkPolicy := range networkPolicies.Items {
			logrus.Infof("Network Policy: %s/%s", networkPolicy.Namespace, networkPolicy.Name)
		}
		logrus.Infof("Policy found on DR cluster")
		for _, networkPolicy := range destNetworkPolicies.Items {
			logrus.Infof("Network Policy: %s/%s", networkPolicy.Namespace, networkPolicy.Name)
		}
		require.NoError(t, matchErr, "NetworkPolicy Expected: %v, Actual: %v", cnt, len(destNetworkPolicies.Items))
	}

	logrus.Infof("Successfully verified migrated network policies on destination cluster")

	err = setSourceKubeConfig()
	require.NoError(t, err, "failed to set kubeconfig to source cluster: %v", err)
	validateAndDestroyMigration(t, ctxs, scheduleName, "networkpolicy", preMigrationCtx, true, false, true, false, false, true)
}

func transformResourceTest(t *testing.T) {
	var testrailID, testResult = 86253, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult)
	defer updateTestRail(&testResult, testrailID, runID)
	instanceID := "mysql-migration-transform-interval"
	appKey := "mysql-1-pvc"

	var err error
	// Reset config in case of error
	defer func() {
		err = setSourceKubeConfig()
		require.NoError(t, err, "Error resetting remote config")
	}()
	err = setMockTime(nil)
	require.NoError(t, err, "Error resetting mock time")
	// the schedule interval for these specs it set to 5 minutes
	ctxs, preMigrationCtx := triggerMigration(
		t,
		instanceID,
		appKey,
		[]string{"transform-service"},
		[]string{"mysql-migration-transform-interval"},
		true,
		false,
		false,
		false,
		"",
		nil,
	)
	namespace := "mysql-1-pvc-mysql-migration-transform-interval"
	validateMigration(t, "mysql-migration-transform-interval", preMigrationCtx.GetID())

	// expected service changes after transformation
	expectedServiceType := "LoadBalancer"
	expectedLabels := make(map[string]string)
	expectedLabels["handler"] = "project"
	expectedLabels["app"] = "mysql-2"

	// Change kubeconfig to destination
	err = setDestinationKubeConfig()
	require.NoError(t, err, "failed to set kubeconfig to source cluster: %v", err)

	// verify service resource migration on destination cluster
	// Verify if statefulset get delete
	svcs, err := core.Instance().ListServices(namespace, meta_v1.ListOptions{})
	require.NoError(t, err, "unable to query services in namespace : %s", namespace)

	for _, svc := range svcs.Items {
		if string(svc.Spec.Type) != expectedServiceType {
			matchErr := fmt.Errorf("transformed service type does not match")
			require.NoError(t, matchErr, "actual: %s, expected: %s", svc.Spec.Type, expectedServiceType)
		}
		labels := svc.GetLabels()
		for k, v := range expectedLabels {
			if val, ok := labels[k]; !ok || val != v {
				matchErr := fmt.Errorf("transformed service labels does not match")
				require.NoError(t, matchErr, "actual: %s, expected: %s", labels, expectedLabels)
			}
		}
	}
	err = setSourceKubeConfig()
	require.NoError(t, err, "failed to set kubeconfig to source cluster: %v", err)
	validateAndDestroyMigration(t, ctxs, instanceID, appKey, preMigrationCtx, true, false, true, false, true, true)

	// If we are here then the test has passed
	testResult = testResultPass
	logrus.Infof("Test status at end of %s test: %s", t.Name(), testResult)
}

// Testrail ID 85741 - https://portworx.testrail.net/index.php?/cases/view/85741
func serviceAndServiceAccountUpdate(t *testing.T) {
	var testrailID, testResult = 85741, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult)
	defer updateTestRail(&testResult, testrailID, runID)

	var err error
	// Reset config in case of error
	defer func() {
		err = setSourceKubeConfig()
		require.NoError(t, err, "Error resetting remote config")
	}()
	err = setMockTime(nil)
	require.NoError(t, err, "Error resetting mock time")
	instanceID := "update"
	appKey := "mysql-service-account"
	additionalAppKey := []string{}
	migrationAppKey := "mysql-service-acc-migration-schedule"
	migrateAllAppsExpected := true
	skipStoragePair := false
	startAppsOnMigration := false
	pairReverse := false
	autoMount := true
	testNamespace := fmt.Sprintf("%s-%s", appKey, instanceID)

	err = setMockTime(nil)
	require.NoError(t, err, "Error resetting mock time")

	_, preMigrationCtx := triggerMigration(t, instanceID, appKey, additionalAppKey, []string{migrationAppKey}, migrateAllAppsExpected, skipStoragePair, startAppsOnMigration, pairReverse, "", nil)

	// Validate intial migration
	validateMigration(t, migrationAppKey, preMigrationCtx.GetID())

	logrus.Infof("First migration completed successfully. Updating service account and service before next migration.")

	// Update service account's secret references, automountservicetoken, and service's ports
	serviceAccountSrc, err := core.Instance().GetServiceAccount(appKey, testNamespace)
	require.NoError(t, err, "Error getting service account on source")

	serviceAccountSrc.AutomountServiceAccountToken = &autoMount

	// Update existing secret ref name in the service account
	for i := 0; i < len(serviceAccountSrc.Secrets); i++ {
		serviceAccountSrc.Secrets[i].Name = serviceAccountSrc.Secrets[i].Name + "-test"
	}

	// Add new secret reference to service account
	objRef := v1.ObjectReference{
		Name:      "new",
		Namespace: testNamespace,
	}
	serviceAccountSrc.Secrets = append(serviceAccountSrc.Secrets, objRef)

	serviceAccountSrc, err = core.Instance().UpdateServiceAccount(serviceAccountSrc)
	require.NoError(t, err, "Error updating service account on source")
	require.NotNil(t, serviceAccountSrc)

	// After update do a get on service account to get values of secret references
	serviceAccountSrc, err = core.Instance().GetServiceAccount(appKey, testNamespace)
	require.NoError(t, err, "Error getting service account")
	require.NotNil(t, serviceAccountSrc)

	// Collect secret references on source cluster
	secretRefSrc := make(map[string]bool)
	for i := 0; i < len(serviceAccountSrc.Secrets); i++ {
		secretRefSrc[serviceAccountSrc.Secrets[i].Name] = true
	}

	mysqlService, err := core.Instance().GetService("mysql-service", testNamespace)
	require.NoError(t, err, "Error getting mysql service on source")
	require.NotNil(t, mysqlService)

	// Update ports on the service on source clusters
	var updatedPorts []int32
	for i := 0; i < len(mysqlService.Spec.Ports); i++ {
		mysqlService.Spec.Ports[i].Port = mysqlService.Spec.Ports[i].Port + 1
		updatedPorts = append(updatedPorts, mysqlService.Spec.Ports[i].Port)
	}

	mysqlService, err = core.Instance().UpdateService(mysqlService)
	require.NoError(t, err, "Error updating mysql service on source")
	require.NotNil(t, mysqlService)

	logrus.Infof("Waiting for next migration to trigger...")
	time.Sleep(5 * time.Minute)

	validateMigration(t, migrationAppKey, preMigrationCtx.GetID())
	logrus.Infof("Migration completed successfully, after updating service and service account.")

	// Verify service account update on migration
	err = setDestinationKubeConfig()
	serviceAccountDest, err := core.Instance().GetServiceAccount(appKey, testNamespace)
	require.NoError(t, err, "Error getting service account on destination")
	require.Equal(t, true, *serviceAccountDest.AutomountServiceAccountToken)

	// Delete service account on source and migrate again
	logrus.Infof("Deleting  service account on source cluster.")
	err = setSourceKubeConfig()
	require.NoError(t, err, "failed to set kubeconfig to source cluster: %v", err)

	err = core.Instance().DeleteServiceAccount(appKey, testNamespace)
	require.NoError(t, err, "Failed to delete service account on source")

	time.Sleep(5 * time.Minute)

	validateMigration(t, migrationAppKey, preMigrationCtx.GetID())
	logrus.Infof("Migration completed successfully, after deletion of service account.")

	// Validate service and service account on destination cluster, for updates made on the source cluster earlier
	err = setDestinationKubeConfig()
	require.NoError(t, err, "failed to set kubeconfig to destination cluster: %v", err)

	serviceAccountDest, err = core.Instance().GetServiceAccount(appKey, testNamespace)
	require.NoError(t, err, "service account NOT found on destination, should NOT have been deleted by migration")

	// Get secret references on destination and compare with source cluster
	secretRefDest := make(map[string]bool)
	for i := 0; i < len(serviceAccountDest.Secrets); i++ {
		secretRefDest[serviceAccountDest.Secrets[i].Name] = true
	}

	// First check that there are more secret references on destination
	require.GreaterOrEqual(t, len(secretRefDest), len(secretRefSrc),
		"number of secret references should be greater or equal on destination since we append")

	// Also check if all secret references from source are present on destination as well
	for sec := range secretRefSrc {
		_, ok := secretRefDest[sec]
		require.True(t, ok, "secret %s not present on destination service account", sec)
	}

	mysqlServiceDest, err := core.Instance().GetService("mysql-service", testNamespace)
	require.NoError(t, err, "Error getting mysql service on source")

	// Validate ports for service are updated on destination cluster
	for i := 0; i < len(mysqlServiceDest.Spec.Ports); i++ {
		require.Equal(t, updatedPorts[i], mysqlServiceDest.Spec.Ports[i].Port,
			"ports not updated for service on destination cluster, expected %d, found %d",
			updatedPorts[i], mysqlServiceDest.Spec.Ports[i].Port)
	}

	// Clean up destination cluster while we are on it
	err = core.Instance().DeleteNamespace(testNamespace)
	require.NoError(t, err, "failed to delete namespace %s on destination cluster")

	// Clean up source cluster
	err = setSourceKubeConfig()
	require.NoError(t, err, "failed to set kubeconfig to source cluster: %v", err)

	err = core.Instance().DeleteNamespace(testNamespace)
	require.NoError(t, err, "failed to delete namespace %s on destination cluster")

	err = setMockTime(nil)
	require.NoError(t, err, "Error resetting mock time")

	// If we are here then the test has passed
	testResult = testResultPass
	logrus.Infof("Test status at end of %s test: %s", t.Name(), testResult)
}

func scheduleClusterPairGeneric(t *testing.T, ctxs []*scheduler.Context,
	appKey, instanceID string,
	defaultClusterPairDir, projectIDMappings string,
	skipStoragePair, resetConfig, pairReverse bool) {
	var err error
	if bidirectionalClusterpair {
		clusterPairNamespace := fmt.Sprintf("%s-%s", appKey, instanceID)
		logrus.Info("Bidirectional flag is set, will create bidirectional cluster pair:")
		logrus.Infof("Name: %s", remotePairName)
		logrus.Infof("Namespace: %s", clusterPairNamespace)
		logrus.Infof("Backuplocation: %s", defaultBackupLocation)
		logrus.Infof("Secret name: %s", defaultSecretName)
		err = scheduleBidirectionalClusterPair(remotePairName, clusterPairNamespace, projectIDMappings, defaultBackupLocation, defaultSecretName)
		require.NoError(t, err, "failed to set bidirectional cluster pair: %v", err)
		err = setSourceKubeConfig()
		require.NoError(t, err, "failed to set kubeconfig to source cluster: %v", err)

	} else if unidirectionalClusterpair {
		clusterPairNamespace := fmt.Sprintf("%s-%s", appKey, instanceID)
		logrus.Info("Unidirectional flag is set, will create unidirectional cluster pair:")
		logrus.Infof("Name: %s", remotePairName)
		logrus.Infof("Namespace: %s", clusterPairNamespace)
		logrus.Infof("Backuplocation: %s", defaultBackupLocation)
		logrus.Infof("Secret name: %s", defaultSecretName)
		err = scheduleUnidirectionalClusterPair(remotePairName, clusterPairNamespace, projectIDMappings, defaultBackupLocation, defaultSecretName, true, pairReverse)
		require.NoError(t, err, "failed to set unidirectional cluster pair: %v", err)
	} else {
		// create, apply and validate cluster pair specs
		err = scheduleClusterPair(ctxs[0], skipStoragePair, true, defaultClusterPairDir, projectIDMappings, pairReverse)
		require.NoError(t, err, "Error scheduling cluster pair")
	}
}

func blowNamespacesForTest(t *testing.T, instanceID, appKey string, skipDestDeletion bool) {
	err := setSourceKubeConfig()
	require.NoError(t, err, "failed to set kubeconfig to source cluster: %v", err)

	namespace := fmt.Sprintf("%s-%s", appKey, instanceID)
	err = core.Instance().DeleteNamespace(namespace)
	require.NoError(t, err, "failed to delete namespace %s on destination cluster", namespace)

	// for negative cases the namespace is not even created on destination, so skip deleting it
	if skipDestDeletion {
		return
	}
	err = setDestinationKubeConfig()
	require.NoError(t, err, "failed to set kubeconfig to destination cluster: %v", err)

	err = core.Instance().DeleteNamespace(namespace)
	require.NoError(t, err, "failed to delete namespace %s on destination cluster", namespace)

	err = setSourceKubeConfig()
	require.NoError(t, err, "failed to set kubeconfig to destination cluster: %v", err)
}
