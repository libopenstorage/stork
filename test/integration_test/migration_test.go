//go:build integrationtest
// +build integrationtest

package integrationtest

import (
	"bytes"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/libopenstorage/stork/pkg/log"
	"github.com/libopenstorage/stork/pkg/storkctl"
	"github.com/portworx/sched-ops/k8s/apps"
	"github.com/portworx/sched-ops/k8s/core"
	storkops "github.com/portworx/sched-ops/k8s/stork"
	"github.com/portworx/sched-ops/task"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/portworx/torpedo/drivers/scheduler/spec"
	"github.com/portworx/torpedo/pkg/asyncdr"
	"github.com/spf13/cobra"
	apps_api "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"

	"github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	storkv1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
)

const (
	migrationRetryInterval = 10 * time.Second
	migrationRetryTimeout  = 5 * time.Minute
	rabbitmqNamespace      = "rabbitmq-operator-migration"
)

func TestMigration(t *testing.T) {
	// reset mock time before running any tests
	err := setMockTime(nil)
	log.FailOnError(t, err, "Error resetting mock time")

	log.InfoD("Using stork volume driver: %s", volumeDriverName)
	log.InfoD("Backup path being used: %s", backupLocationPath)
	setDefaultsForBackup(t)
	currentTestSuite = t.Name()

	t.Run("testMigration", testMigration)
	t.Run("testMigrationFailoverFailback", testMigrationFailoverFailback)
	t.Run("deleteStorkPodsSourceDuringMigrationTest", deleteStorkPodsSourceDuringMigrationTest)
	t.Run("deleteStorkPodsDestDuringMigrationTest", deleteStorkPodsDestDuringMigrationTest)
}

func testMigration(t *testing.T) {
	// reset mock time before running any tests
	err := setMockTime(nil)
	log.FailOnError(t, err, "Error resetting mock time")

	err = setSourceKubeConfig()
	log.FailOnError(t, err, "failed to set kubeconfig to source cluster: %v", err)

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
	t.Run("excludeResourceTypeDeploymentTest", excludeResourceTypeDeploymentTest)
	t.Run("excludeResourceTypePVCTest", excludeResourceTypePVCTest)
	t.Run("excludeMultipleResourceTypesTest", excludeMultipleResourceTypesTest)
	t.Run("excludeResourceTypesWithSelectorsTest", excludeResourceTypesWithSelectorsTest)
	t.Run("excludeNonExistingResourceTypesTest", excludeNonExistingResourceTypesTest)
	t.Run("transformCRResourceTest", transformCRResourceTest)

	err = setRemoteConfig("")
	log.FailOnError(t, err, "setting kubeconfig to default failed")
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
		log.FailOnError(t, err, "Error resetting source config")
	}()

	ctxs, preMigrationCtx := triggerMigration(t, instanceID, appKey, additionalAppKeys, []string{migrationAppKey}, migrateAllAppsExpected, false, startAppsOnMigration, false, "", nil)
	validateAndDestroyMigration(t, ctxs, instanceID, appKey, preMigrationCtx, migrationSuccessExpected, startAppsOnMigration, migrateAllAppsExpected, false, skipDestDeletion, true, nil, nil)
}

func scheduleAndRunTasks(
	t *testing.T,
	instanceID string,
	appKey string,
	additionalAppKeys []string,
	migrateAllAppsExpected bool,
	skipStoragePair bool,
	pairReverse bool,
	projectIDMappings string,
	namespaceLabels map[string]string,
) ([]*scheduler.Context, *scheduler.Context) {
	ctxs, err := schedulerDriver.Schedule(instanceID,
		scheduler.ScheduleOptions{
			AppKeys: []string{appKey},
			Labels:  namespaceLabels,
		})
	log.FailOnError(t, err, "Error scheduling task")

	Dash.VerifyFatal(t, 1, len(ctxs), "Only one task should have started")

	err = schedulerDriver.WaitForRunning(ctxs[0], defaultWaitTimeout, defaultWaitInterval)
	log.FailOnError(t, err, "Error waiting for app to get to running state")

	preMigrationCtx := ctxs[0].DeepCopy()

	if len(additionalAppKeys) > 0 {
		err = schedulerDriver.AddTasks(ctxs[0],
			scheduler.ScheduleOptions{AppKeys: additionalAppKeys})
		log.FailOnError(t, err, "Error scheduling additional apps")
		err = schedulerDriver.WaitForRunning(ctxs[0], defaultWaitTimeout, defaultWaitInterval)
		log.FailOnError(t, err, "Error waiting for additional apps to get to running state")
	}

	if migrateAllAppsExpected {
		preMigrationCtx = ctxs[0].DeepCopy()
	}

	// Schedule bidirectional or regular cluster pair based on the flag
	scheduleClusterPairGeneric(t, ctxs, appKey, instanceID, defaultClusterPairDir, projectIDMappings, skipStoragePair, true, pairReverse)
	return ctxs, preMigrationCtx
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
	ctxs, preMigrationCtx := scheduleAndRunTasks(t, instanceID, appKey, additionalAppKeys, migrateAllAppsExpected, skipStoragePair, pairReverse, projectIDMappings, namespaceLabels)

	// apply migration specs
	err := schedulerDriver.AddTasks(ctxs[0],
		scheduler.ScheduleOptions{AppKeys: migrationAppKeys})
	log.FailOnError(t, err, "Error scheduling migration specs")

	return ctxs, preMigrationCtx
}

func triggerMigrationSchedule(
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
	migrationScheduleArgs map[string]map[string]string,
	schedulePolicyArgs map[string]map[string]string,
) ([]*scheduler.Context, *scheduler.Context) {
	ctxs, preMigrationCtx := scheduleAndRunTasks(t, instanceID, appKey, additionalAppKeys, migrateAllAppsExpected, skipStoragePair, pairReverse, projectIDMappings, namespaceLabels)
	factory := storkctl.NewFactory()
	var outputBuffer bytes.Buffer
	cmd := storkctl.NewCommand(factory, os.Stdin, &outputBuffer, os.Stderr)
	migrationScheduleNamespace := fmt.Sprintf("%s-%s", appKey, instanceID)

	//Create schedulePolicies using storkCtl if any required
	for schedulePolicyName, customArgs := range schedulePolicyArgs {
		cmdArgs := []string{"create", "schedulepolicy", schedulePolicyName}
		executeStorkCtlCommand(t, cmd, cmdArgs, customArgs)
	}
	//Create migrationSchedules using storkCtl
	for _, migrationKey := range migrationAppKeys {
		cmdArgs := []string{"create", "migrationschedule", migrationKey, "-c", remotePairName,
			"--namespaces", migrationScheduleNamespace, "-n", migrationScheduleNamespace}
		executeStorkCtlCommand(t, cmd, cmdArgs, migrationScheduleArgs[migrationKey])
	}
	return ctxs, preMigrationCtx
}

func executeStorkCtlCommand(t *testing.T, cmd *cobra.Command, cmdArgs []string, customArgs map[string]string) {
	// add the custom args to the command
	if customArgs != nil {
		for key, value := range customArgs {
			cmdArgs = append(cmdArgs, "--"+key)
			if value != "" {
				cmdArgs = append(cmdArgs, value)
			}
		}
	}
	cmd.SetArgs(cmdArgs)
	//execute the command
	log.InfoD("The storkctl command being executed is %v", cmdArgs)
	log.FailOnError(t, cmd.Execute(), "Storkctl execution failed")
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
	log.FailOnError(t, err, "get migration failed")
	Dash.VerifyFatal(t, migObj.Status.Summary != nil, true, "migration summary is nil")
	Dash.VerifyFatal(t, migObj.Status.Summary.NumberOfMigratedResources, expectedResources, "unexpected number of resources migrated")
	Dash.VerifyFatal(t, migObj.Status.Summary.NumberOfMigratedVolumes, expectedVolumes, "unexpected number of volumes migrated")
	Dash.VerifyFatal(t, migObj.Status.Summary.TotalNumberOfResources, expectedResources, "unexpected number of total resources")
	Dash.VerifyFatal(t, migObj.Status.Summary.TotalNumberOfVolumes, expectedVolumes, "unexpected number of total volumes")
	if expectedVolumes > 0 {
		Dash.VerifyFatal(t, migObj.Status.Summary.TotalBytesMigrated > 0, true, "expected bytes total to be non-zero")
	} else {
		Dash.VerifyFatal(t, migObj.Status.Summary.TotalBytesMigrated == 0, true, "expected bytes total to be zero")
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
	migrationSchedulesToCleanup []string,
	schedulePoliciesToCleanup []string,
) {
	var err error
	timeout := defaultWaitTimeout
	if !migrationSuccessExpected {
		timeout = timeout / 5
	}
	migrationScheduleNamespace := fmt.Sprintf("%s-%s", appKey, instanceID)
	allAppsCtx := ctxs[0].DeepCopy()
	err = schedulerDriver.WaitForRunning(ctxs[0], timeout, defaultWaitInterval)
	for _, migrationSchedule := range migrationSchedulesToCleanup {
		// Need to Validate the migrationSchedules separately because they are created using storkctl
		// and not a part of the torpedo scheduler context
		_, err = storkops.Instance().ValidateMigrationSchedule(migrationSchedule, migrationScheduleNamespace, timeout, defaultWaitInterval)
	}
	if migrationSuccessExpected {
		// need to wait for migration to complete
		log.FailOnError(t, err, "Error waiting for migration to get to Ready state")

		// wait on cluster 2 for the app to be running
		funcWaitAndDelete := func() {
			if startAppsOnMigration {
				err = schedulerDriver.WaitForRunning(preMigrationCtx, defaultWaitTimeout, defaultWaitInterval)
				log.FailOnError(t, err, "Error waiting for pod to get to running state on remote cluster after migration")
				if !migrateAllAppsExpected {
					err = schedulerDriver.WaitForRunning(allAppsCtx, defaultWaitTimeout/2, defaultWaitInterval)
					log.FailOnNoError(t, err, "All apps shouldn't have been migrated")
				}
			} else {
				err = schedulerDriver.WaitForRunning(preMigrationCtx, defaultWaitTimeout/4, defaultWaitInterval)
				log.FailOnNoError(t, err, "Expected pods to NOT get to running state on remote cluster after migration")
			}

			/* Failing right now as SC's are not migrated
			* else {
				log.InfoD("test only validating storage components as migration has startApplications disabled")
				err = schedulerDriver.InspectVolumes(preMigrationCtx, defaultWaitTimeout, defaultWaitInterval)
				log.FailOnError(t, err, "Error validating storage components on remote cluster after migration")
			}*/

			// Validate the migration summary
			// destroy mysql app on cluster 2
			if !skipAppDeletion && !skipDestDeletion {
				destroyAndWait(t, []*scheduler.Context{preMigrationCtx})
			}
		}
		executeOnDestination(t, funcWaitAndDelete)
	} else {
		log.FailOnNoError(t, err, "Expected migration to fail")
	}

	// destroy app on cluster 1
	if !skipAppDeletion {
		destroyAndWait(t, ctxs)
		// migrationSchedules and schedulePolicies arguments are used to delete the created resources
		// as they are not part of the torpedo scheduler context
		for _, migrationSchedule := range migrationSchedulesToCleanup {
			DeleteAndWaitForMigrationScheduleDeletion(t, migrationSchedule, migrationScheduleNamespace)
		}
		for _, schedulePolicy := range schedulePoliciesToCleanup {
			DeleteAndWaitForSchedulePolicyDeletion(t, schedulePolicy)
		}
	}

	if blowNamespaces {
		blowNamespacesForTest(t, instanceID, appKey, skipDestDeletion)
	}
}

func deploymentMigrationTest(t *testing.T) {
	var testrailID, testResult = 50803, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	defer updateDashStats(t.Name(), &testResult)
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
	log.InfoD("Test status at end of %s test: %s", t.Name(), testResult)
}

func deploymentMigrationReverseTest(t *testing.T) {
	var testrailID, testResult = 54210, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	defer updateDashStats(t.Name(), &testResult)
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
	validateAndDestroyMigration(t, ctxs, instanceID, appKey, preMigrationCtx, true, true, true, false, false, false, nil, nil)

	// Change kubeconfig to destination
	err = setDestinationKubeConfig()
	log.FailOnError(t, err, "failed to set kubeconfig to source cluster: %v", err)

	ctxsReverse, err := schedulerDriver.Schedule("mysql-migration",
		scheduler.ScheduleOptions{AppKeys: []string{"mysql-1-pvc"}})
	log.FailOnError(t, err, "Error scheduling task")
	Dash.VerifyFatal(t, 1, len(ctxsReverse), "Only one task should have started")

	err = schedulerDriver.WaitForRunning(ctxsReverse[0], defaultWaitTimeout, defaultWaitInterval)
	log.FailOnError(t, err, "Error waiting for app to get to running state")

	postMigrationCtx := ctxsReverse[0].DeepCopy()

	// create, apply and validate cluster pair specs on destination for non-bidirectional pair
	if !bidirectionalClusterpair && !unidirectionalClusterpair {
		err = scheduleClusterPair(ctxsReverse[0], false, false, "cluster-pair-reverse", "", true)
	} else if unidirectionalClusterpair {
		clusterPairNamespace := fmt.Sprintf("%s-%s", appKey, instanceID)
		err = setSourceKubeConfig()
		log.FailOnError(t, err, "Error setting remote config")
		err = core.Instance().DeleteSecret(remotePairName, clusterPairNamespace)
		log.FailOnError(t, err, "Error deleting secret")
		err = storkops.Instance().DeleteBackupLocation(remotePairName, clusterPairNamespace)
		log.FailOnError(t, err, "Error deleting backuplocation")
		err = setDestinationKubeConfig()
		log.FailOnError(t, err, "Error setting remote config")
		err = core.Instance().DeleteSecret(remotePairName, clusterPairNamespace)
		log.FailOnError(t, err, "Error deleting secret")
		err = storkops.Instance().DeleteBackupLocation(remotePairName, clusterPairNamespace)
		log.FailOnError(t, err, "Error deleting backuplocation")
		err = scheduleUnidirectionalClusterPair(remotePairName, clusterPairNamespace, "", defaultBackupLocation, defaultSecretName, false, true)
	}
	log.FailOnError(t, err, "Error scheduling cluster pair")

	// apply migration specs
	err = schedulerDriver.AddTasks(ctxsReverse[0],
		scheduler.ScheduleOptions{AppKeys: []string{"mysql-migration"}})
	log.FailOnError(t, err, "Error scheduling migration specs")

	err = schedulerDriver.WaitForRunning(ctxsReverse[0], defaultWaitTimeout, defaultWaitInterval)
	log.FailOnError(t, err, "Error waiting for migration to complete")

	destroyAndWait(t, ctxsReverse)

	// Cleanup up source
	err = setSourceKubeConfig()
	log.FailOnError(t, err, "failed to set kubeconfig to source cluster: %v", err)

	err = schedulerDriver.WaitForRunning(postMigrationCtx, defaultWaitTimeout, defaultWaitInterval)
	log.FailOnError(t, err, "Error waiting for migration to complete")

	//validateAndDestroyMigration(t, []*scheduler.Context{preMigrationCtx}, preMigrationCtx, true, true, true, true, false)

	destroyAndWait(t, []*scheduler.Context{postMigrationCtx})
	validateAndDestroyMigration(t, []*scheduler.Context{preMigrationCtx}, instanceID, appKey, preMigrationCtx, false, false, false, true, false, true, nil, nil)

	// If we are here then the test has passed
	testResult = testResultPass
	log.InfoD("Test status at end of %s test: %s", t.Name(), testResult)
}

func statefulsetMigrationTest(t *testing.T) {
	var testrailID, testResult = 50804, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	defer updateDashStats(t.Name(), &testResult)
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
	log.InfoD("Test status at end of %s test: %s", t.Name(), testResult)
}

func statefulsetMigrationStartAppFalseTest(t *testing.T) {
	var testrailID, testResult = 86243, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	defer updateDashStats(t.Name(), &testResult)
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
	log.InfoD("Test status at end of %s test: %s", t.Name(), testResult)
}

func statefulsetMigrationRuleTest(t *testing.T) {
	var testrailID, testResult = 50805, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	defer updateDashStats(t.Name(), &testResult)
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
	log.InfoD("Test status at end of %s test: %s", t.Name(), testResult)
}

func statefulsetMigrationRulePreExecMissingTest(t *testing.T) {
	var testrailID, testResult = 50806, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	defer updateDashStats(t.Name(), &testResult)
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
		false,
	)

	// If we are here then the test has passed
	testResult = testResultPass
	log.InfoD("Test status at end of %s test: %s", t.Name(), testResult)
}
func statefulsetMigrationRulePostExecMissingTest(t *testing.T) {
	var testrailID, testResult = 50807, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	defer updateDashStats(t.Name(), &testResult)
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
		false,
	)

	// If we are here then the test has passed
	testResult = testResultPass
	log.InfoD("Test status at end of %s test: %s", t.Name(), testResult)
}

func migrationDisallowedNamespaceTest(t *testing.T) {
	var testrailID, testResult = 50808, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	defer updateDashStats(t.Name(), &testResult)
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
		false,
	)

	// If we are here then the test has passed
	testResult = testResultPass
	log.InfoD("Test status at end of %s test: %s", t.Name(), testResult)
}

func migrationFailingPreExecRuleTest(t *testing.T) {
	var testrailID, testResult = 50809, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	defer updateDashStats(t.Name(), &testResult)
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
		false,
	)

	// If we are here then the test has passed
	testResult = testResultPass
	log.InfoD("Test status at end of %s test: %s", t.Name(), testResult)
}

func migrationFailingPostExecRuleTest(t *testing.T) {
	var testrailID, testResult = 50810, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	defer updateDashStats(t.Name(), &testResult)
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
		false,
	)

	// If we are here then the test has passed
	testResult = testResultPass
	log.InfoD("Test status at end of %s test: %s", t.Name(), testResult)
}

func migrationLabelSelectorTest(t *testing.T) {
	var testrailID, testResult = 50811, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	defer updateDashStats(t.Name(), &testResult)
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
	log.InfoD("Test status at end of %s test: %s", t.Name(), testResult)
}

func migrationLabelExcludeSelectorTest(t *testing.T) {
	var testrailID, testResult = 86245, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	defer updateDashStats(t.Name(), &testResult)
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
	log.InfoD("Test status at end of %s test: %s", t.Name(), testResult)
}

func namespaceLabelSelectorTest(t *testing.T) {
	var testrailID, testResult = 86244, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	defer updateDashStats(t.Name(), &testResult)
	instanceID := "ns-selector-test"
	appKey := "cassandra"

	var err error
	defer func() {
		err = setSourceKubeConfig()
		log.FailOnError(t, err, "Error resetting source config")
	}()

	// Deploy mysql-1-pvc in namespace "mysql-1-pvc-ns-selector-test"
	ctxNs2, err := schedulerDriver.Schedule(instanceID,
		scheduler.ScheduleOptions{
			AppKeys: []string{"mysql-1-pvc"},
			Labels:  nil,
		})
	log.FailOnError(t, err, "Error scheduling task")
	Dash.VerifyFatal(t, 1, len(ctxNs2), "Only one task should have started")

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
		nil,
		nil,
	)

	// Validate on destination cluster that mysql-1-pvc is not migrated as it doesn't have the required namespace labels
	err = setDestinationKubeConfig()
	log.FailOnError(t, err, "failed to set kubeconfig to source cluster: %v", err)

	err = schedulerDriver.WaitForRunning(ctxNs2[0], defaultWaitTimeout/16, defaultWaitInterval)
	log.FailOnNoError(t, err, "Error waiting for pod to get to running state on remote cluster after migration")

	err = setSourceKubeConfig()
	log.FailOnError(t, err, "failed to set kubeconfig to source cluster: %v", err)

	// Destroy migration
	destroyAndWait(t, []*scheduler.Context{preMigrationCtx})

	// Destroy cassandra app context
	destroyAndWait(t, ctxs)

	// Destroy mysql-1-pvc app context
	destroyAndWait(t, ctxNs2)

	// If we are here then the test has passed
	testResult = testResultPass
	log.InfoD("Test status at end of %s test: %s", t.Name(), testResult)
}

// migrationIntervalScheduleTest runs test for migrations with schedules that are
// intervals of time
func migrationIntervalScheduleTest(t *testing.T) {
	var testrailID, testResult = 50812, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	defer updateDashStats(t.Name(), &testResult)
	instanceID := "mysql-migration-schedule-interval"
	appKey := "mysql-1-pvc"
	var err error
	// Reset config in case of error
	defer func() {
		err = setSourceKubeConfig()
		log.FailOnError(t, err, "Error resetting remote config")
	}()

	// we want to create the schedulePolicy and migrationSchedule using storkctl instead of scheduling apps using torpedo's scheduler
	// schedulePolicyArgs is a map of schedulePolicyName : {{flag1:value1,flag2:value2,....}}
	var schedulePolicyArgs = make(map[string]map[string]string)
	schedulePolicyArgs["migrate-every-5m"] = map[string]string{"policy-type": "Interval", "interval-minutes": "5"}

	// migrationScheduleArgs is a map of migrationScheduleName : {{flag1:value1,flag2:value2,....}}
	var migrationScheduleArgs = make(map[string]map[string]string)
	migrationScheduleArgs[instanceID] = map[string]string{
		"purge-deleted-resources": "",
		"schedule-policy-name":    "migrate-every-5m",
	}

	// the schedule interval for these specs it set to 5 minutes
	ctxs, preMigrationCtx := triggerMigrationSchedule(
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
		migrationScheduleArgs,
		schedulePolicyArgs,
	)

	// bump time of the world by 5 minutes
	mockNow := time.Now().Add(6 * time.Minute)
	err = setMockTime(&mockNow)
	log.FailOnError(t, err, "Error setting mock time")

	validateAndDestroyMigration(t, ctxs, instanceID, appKey, preMigrationCtx, true, false, true, false, false, true, []string{"mysql-migration-schedule-interval"}, []string{"migrate-every-5m"})

	// If we are here then the test has passed
	testResult = testResultPass
	log.InfoD("Test status at end of %s test: %s", t.Name(), testResult)
}

// intervalScheduleCleanupTest runs test for migrations with schedules that are
// intervals of time, will try to perform cleanup of k8s resources which are deleted
// on source cluster
func intervalScheduleCleanupTest(t *testing.T) {
	var testrailID, testResult = 86246, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	defer updateDashStats(t.Name(), &testResult)
	instanceID := "mysql-migration-schedule-interval"
	appKey := "mysql-1-pvc"

	var err error
	var name, namespace string
	var pvcs *v1.PersistentVolumeClaimList
	// Reset config in case of error
	defer func() {
		err = setSourceKubeConfig()
		log.FailOnError(t, err, "Error resetting remote config")
	}()
	err = setMockTime(nil)
	log.FailOnError(t, err, "Error resetting mock time")
	// the schedule interval for these specs it set to 5 minutes
	var migrationScheduleArgs = make(map[string]map[string]string)
	migrationScheduleArgs[instanceID] = map[string]string{
		"purge-deleted-resources": "",
		"schedule-policy-name":    "migrate-every-5m",
	}
	var schedulePolicyArgs = make(map[string]map[string]string)
	schedulePolicyArgs["migrate-every-5m"] = map[string]string{"policy-type": "Interval", "interval-minutes": "5"}
	ctxs, preMigrationCtx := triggerMigrationSchedule(
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
		migrationScheduleArgs,
		schedulePolicyArgs,
	)
	validateMigration(t, "mysql-migration-schedule-interval", preMigrationCtx.GetID())

	// delete statefulset from source cluster
	for i, spec := range ctxs[0].App.SpecList {
		if obj, ok := spec.(*apps_api.StatefulSet); ok {
			name = obj.GetName()
			namespace = obj.GetNamespace()
			pvcs, err = apps.Instance().GetPVCsForStatefulSet(obj)
			log.FailOnError(t, err, "error getting pvcs for ss")
			err = apps.Instance().DeleteStatefulSet(name, namespace)
			log.FailOnError(t, err, "error deleting cassandra statefulset")
			err = apps.Instance().ValidateStatefulSet(obj, 1*time.Minute)
			log.FailOnError(t, err, "error deleting cassandra statefulset")
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
		log.FailOnError(t, err, "Error deleting pvc")
	}

	// bump time of the world by 5 minutes
	mockNow := time.Now().Add(6 * time.Minute)
	err = setMockTime(&mockNow)
	log.FailOnError(t, err, "Error setting mock time")

	// verify app deleted on source cluster with second migration
	time.Sleep(1 * time.Minute)
	validateMigration(t, instanceID, preMigrationCtx.GetID())
	validateMigrationCleanup(t, name, namespace, pvcs)

	validateAndDestroyMigration(t, ctxs, instanceID, appKey, preMigrationCtx, true, false, true, false, false, true, []string{instanceID}, []string{"migrate-every-5m"})

	// If we are here then the test has passed
	testResult = testResultPass
	log.InfoD("Test status at end of %s test: %s", t.Name(), testResult)
}

func validateMigrationCleanup(t *testing.T, name, namespace string, pvcs *v1.PersistentVolumeClaimList) {
	// validate if statefulset got deleted on cluster2
	err := setDestinationKubeConfig()
	log.FailOnError(t, err, "failed to set kubeconfig to destination cluster: %v", err)

	// Verify if statefulset get delete
	_, err = apps.Instance().GetStatefulSet(name, namespace)
	log.FailOnNoError(t, err, "expected ss:%v error not found", name)

	for _, pvc := range pvcs.Items {
		resp, err := core.Instance().GetPersistentVolumeClaim(pvc.Name, pvc.Namespace)
		if err == nil {
			Dash.VerifyFatal(t, resp.DeletionTimestamp != nil, true, fmt.Sprintf("expected pvc to be deleted:%v", resp.Name))
		} else {
			log.FailOnNoError(t, err, "expected pvc to be deleted:%v", resp.Name)
		}
	}

	err = setSourceKubeConfig()
	log.FailOnError(t, err, "Error resetting remote config")
}

func validateMigration(t *testing.T, name, namespace string) {
	//  ensure only one migration has run
	migrationsMap, err := storkops.Instance().ValidateMigrationSchedule(
		name, namespace, defaultWaitTimeout, defaultWaitInterval)
	log.FailOnError(t, err, "error getting migration schedule")
	Dash.VerifyFatal(t, len(migrationsMap), 1, "expected only one schedule type in migration map")

	migrationStatus := migrationsMap[v1alpha1.SchedulePolicyTypeInterval][0]
	// Independently validate the migration
	err = storkops.Instance().ValidateMigration(
		migrationStatus.Name, namespace, defaultWaitTimeout, defaultWaitInterval)
	log.FailOnError(t, err, "failed to validate migration")
}

func migrationDailyScheduleTest(t *testing.T) {
	var testrailID, testResult = 50813, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	defer updateDashStats(t.Name(), &testResult)

	var schedulePolicyArgs = make(map[string]map[string]string)
	schedulePolicyArgs["migrate-every-daily"] = map[string]string{"policy-type": "Daily", "time": "12:04PM"}

	var migrationScheduleArgs = make(map[string]map[string]string)
	migrationScheduleArgs["mysql-migration-schedule-daily"] = map[string]string{
		"schedule-policy-name": "migrate-every-daily",
	}

	migrationScheduleTest(t, v1alpha1.SchedulePolicyTypeDaily, "mysql-migration-schedule-daily", "", -1, migrationScheduleArgs, schedulePolicyArgs)

	// If we are here then the test has passed
	testResult = testResultPass
	log.InfoD("Test status at end of %s test: %s", t.Name(), testResult)
}

func migrationWeeklyScheduleTest(t *testing.T) {
	var testrailID, testResult = 50814, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	defer updateDashStats(t.Name(), &testResult)

	var schedulePolicyArgs = make(map[string]map[string]string)
	schedulePolicyArgs["migrate-every-weekly"] = map[string]string{"policy-type": "Weekly", "time": "12:04PM", "day-of-week": "Monday"}

	var migrationScheduleArgs = make(map[string]map[string]string)
	migrationScheduleArgs["mysql-migration-schedule-weekly"] = map[string]string{
		"schedule-policy-name": "migrate-every-weekly",
	}

	migrationScheduleTest(t, v1alpha1.SchedulePolicyTypeWeekly, "mysql-migration-schedule-weekly", "Monday", -1, migrationScheduleArgs, schedulePolicyArgs)

	// If we are here then the test has passed
	testResult = testResultPass
	log.InfoD("Test status at end of %s test: %s", t.Name(), testResult)
}

func migrationMonthlyScheduleTest(t *testing.T) {
	var testrailID, testResult = 50815, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	defer updateDashStats(t.Name(), &testResult)

	var schedulePolicyArgs = make(map[string]map[string]string)
	schedulePolicyArgs["migrate-every-monthly"] = map[string]string{"policy-type": "Monthly", "time": "12:04PM", "date-of-month": "11"}

	var migrationScheduleArgs = make(map[string]map[string]string)
	migrationScheduleArgs["mysql-migration-schedule-monthly"] = map[string]string{
		"schedule-policy-name": "migrate-every-monthly",
	}

	migrationScheduleTest(t, v1alpha1.SchedulePolicyTypeMonthly, "mysql-migration-schedule-monthly", "", 11, migrationScheduleArgs, schedulePolicyArgs)

	// If we are here then the test has passed
	testResult = testResultPass
	log.InfoD("Test status at end of %s test: %s", t.Name(), testResult)
}

func migrationScheduleInvalidTest(t *testing.T) {
	var testrailID, testResult = 50816, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	defer updateDashStats(t.Name(), &testResult)
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
		log.FailOnError(t, err, fmt.Sprintf("failed to get migration schedule: [%s] %s", namespace, migrationScheduleName))
		Dash.VerifyFatal(t, len(migrationSchedule.Status.Items) == 0, true, "expected 0 items in migration schedule status")

		listOptions := meta_v1.ListOptions{
			FieldSelector: fields.OneTermEqualSelector("involvedObject.name", migrationScheduleName).String(),
			Watch:         false,
		}

		storkEvents, err := core.Instance().ListEvents(namespace, listOptions)
		log.FailOnError(t, err, "failed to list stork events")

		foundFailedEvent := false
		for _, ev := range storkEvents.Items {
			if ev.Reason == "Failed" && strings.Contains(ev.Message, "Error checking if migration should be triggered: Invalid") {
				foundFailedEvent = true
				break
			}
		}

		Dash.VerifyFatal(t, foundFailedEvent, true,
			fmt.Sprintf("failed to find an event for the migration schedule: [%s] %s for the invalid policy",
				namespace, migrationScheduleName))
	}

	destroyAndWait(t, ctxs)

	// If we are here then the test has passed
	testResult = testResultPass
	log.InfoD("Test status at end of %s test: %s", t.Name(), testResult)
}

// NOTE: below test assumes all schedule policies used here (daily, weekly or monthly) have a
// trigger time of 12:05PM. Ensure the SchedulePolicy specs use that time.
func migrationScheduleTest(
	t *testing.T,
	scheduleType v1alpha1.SchedulePolicyType,
	migrationScheduleName string,
	scheduleDay string,
	scheduleDate int,
	migrationScheduleCustomArgs map[string]map[string]string,
	schedulePolicyArgs map[string]map[string]string) {
	var err error
	// Reset config in case of error
	defer func() {
		err = setSourceKubeConfig()
		log.FailOnError(t, err, "Error resetting remote config")
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

		Dash.VerifyFatal(t, found, true, fmt.Sprintf("failed to find date from given schedule day: %s", scheduleDay))
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
	log.FailOnError(t, err, "Error setting mock time")

	ctxs, preMigrationCtx := triggerMigrationSchedule(
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
		migrationScheduleCustomArgs,
		schedulePolicyArgs,
	)

	namespace := preMigrationCtx.GetID()
	failureErrString := fmt.Sprintf("basic validation of migration schedule: [%s] %s failed",
		namespace, migrationScheduleName)

	// **** TEST 1: ensure 0 migrations since we haven't reached the daily scheduled time
	migrationSchedule, err := storkops.Instance().GetMigrationSchedule(migrationScheduleName, namespace)
	log.FailOnError(t, err, "failed to get migration schedule")
	Dash.VerifyFatal(t, len(migrationSchedule.Status.Items) == 0, true, "expected 0 items in migration schedule status")

	// **** TEST 2: bump time one minute past the scheduled time of daily migration
	mockNow = nextTrigger.Add(1 * time.Minute)
	err = setMockTime(&mockNow)
	log.FailOnError(t, err, "Error setting mock time")

	//  ensure only one migration has run
	migrationsMap, err := storkops.Instance().ValidateMigrationSchedule(
		migrationScheduleName, namespace, defaultWaitTimeout, defaultWaitInterval)
	log.FailOnError(t, err, failureErrString)
	Dash.VerifyFatal(t, len(migrationsMap), 1, "expected only one schedule type in migration map")

	migrations := migrationsMap[scheduleType]
	Dash.VerifyFatal(t, len(migrations), 1, fmt.Sprintf("expected exactly one %v migration. Found: %d", scheduleType, len(migrations)))

	migrationStatus := migrations[0]

	// Independently validate the migration
	err = storkops.Instance().ValidateMigration(migrationStatus.Name, namespace, 1*time.Minute, defaultWaitInterval)
	log.FailOnError(t, err, "failed to validate first daily migration")

	// check creation time of the new migration
	firstMigrationCreationTime := migrationStatus.CreationTimestamp
	Dash.VerifyFatal(t, mockNow.Before(firstMigrationCreationTime.Time) || mockNow.Equal(firstMigrationCreationTime.Time), true,
		fmt.Sprintf("creation of migration: %v should have been after or equal to: %v",
			firstMigrationCreationTime, mockNow))

	// **** TEST 3 bump time by 2 more hours. Should not cause any new migrations
	mockNow = nextTrigger.Add(2 * time.Hour)
	err = setMockTime(&mockNow)
	log.FailOnError(t, err, "Error setting mock time")

	//  ensure no new migrations
	migrationsMap, err = storkops.Instance().ValidateMigrationSchedule(
		migrationScheduleName, namespace, defaultWaitTimeout, defaultWaitInterval)
	log.FailOnError(t, err, failureErrString)
	Dash.VerifyFatal(t, len(migrationsMap), 1, "expected only one schedule type in migration map")

	migrations = migrationsMap[scheduleType]
	Dash.VerifyFatal(t, len(migrations), 1, fmt.Sprintf("expected exactly one %v migration. Found: %d",
		scheduleType, len(migrations)))
	Dash.VerifyFatal(t, firstMigrationCreationTime, migrations[0].CreationTimestamp,
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
	log.FailOnError(t, err, "Error setting mock time")

	// Give time for new migration to trigger
	time.Sleep(time.Minute)

	for i := 0; i < 10; i++ {
		migrationsMap, err = storkops.Instance().ValidateMigrationSchedule(
			migrationScheduleName, namespace, defaultWaitTimeout, defaultWaitInterval)
		log.FailOnError(t, err, failureErrString)
		Dash.VerifyFatal(t, len(migrationsMap), 1, "expected only one schedule type in migration map")

		migrations = migrationsMap[scheduleType]
		// If there are more than 1 migrations, the prune might still be in
		// progress, so retry after a short sleep
		if len(migrations) == 1 {
			break
		}
		time.Sleep(10 * time.Second)
	}
	Dash.VerifyFatal(t, len(migrations), 1, fmt.Sprintf("expected exactly one %v migration. Found: %d", scheduleType, len(migrations)))

	migrationStatus = migrations[0]
	Dash.VerifyFatal(t, firstMigrationCreationTime.Time.Before(migrationStatus.CreationTimestamp.Time), true,
		fmt.Sprintf("creation of migration: %v should have been after: %v the first migration",
			firstMigrationCreationTime, migrationStatus.CreationTimestamp))

	var migrationSchedulesToCleanup []string
	for key := range migrationScheduleCustomArgs {
		migrationSchedulesToCleanup = append(migrationSchedulesToCleanup, key)
	}

	var schedulePoliciesToCleanup []string
	for key := range schedulePolicyArgs {
		schedulePoliciesToCleanup = append(schedulePoliciesToCleanup, key)
	}

	// validate and destroy apps on both clusters
	validateAndDestroyMigration(t, ctxs, migrationScheduleName, "mysql-1-pvc", preMigrationCtx, true, false, true, false, false, false, migrationSchedulesToCleanup, schedulePoliciesToCleanup)

	// explicitly check if all child migrations of the schedule are deleted
	f := func() (interface{}, bool, error) {
		for _, migrations := range migrationsMap {
			for _, m := range migrations {
				_, err := storkops.Instance().GetMigration(m.Name, namespace)
				if err == nil {
					return "", true, fmt.Errorf("get on migration: %s should have failed", m.Name)
				}

				if !errors.IsNotFound(err) {
					log.InfoD("unexpected err: %v when checking deleted migration: %s", err, m.Name)
					return "", true, err
				}
			}
		}

		//done all deleted
		return "", false, nil
	}

	_, err = task.DoRetryWithTimeout(f, defaultWaitTimeout, defaultWaitInterval)
	log.FailOnError(t, err, "migrations for schedules have not been deleted")
}

func migrationScaleTest(t *testing.T) {
	var testrailID, testResult = 86247, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	defer updateDashStats(t.Name(), &testResult)
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
	log.InfoD("Test status at end of %s test: %s", t.Name(), testResult)
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
		log.FailOnError(t, err, "Error resetting remote config")
	}()

	for i := 1; i <= migrationScaleCount; i++ {
		currCtxs, err := schedulerDriver.Schedule(migrationKey+"-"+strconv.Itoa(i),
			scheduler.ScheduleOptions{AppKeys: []string{migrationAppKey}})
		log.FailOnError(t, err, "Error scheduling task")
		Dash.VerifyFatal(t, len(currCtxs), 1, "Only one task should have started")

		err = schedulerDriver.WaitForRunning(currCtxs[0], defaultWaitTimeout, defaultWaitInterval)
		log.FailOnError(t, err, "Error waiting for app to get to running state")

		// Save context without the clusterpair object
		preMigrationCtx := currCtxs[0].DeepCopy()
		appCtxs = append(appCtxs, preMigrationCtx)

		// Schedule bidirectional or regular cluster pair based on the flag
		scheduleClusterPairGeneric(t, currCtxs, appKey, instanceID+"-"+strconv.Itoa(i), defaultClusterPairDir, projectIDMappings, false, true, false)
		ctxs = append(ctxs, currCtxs...)

		currMigNamespace := migrationAppKey + "-" + migrationKey + "-" + strconv.Itoa(i)
		currMig, err := createMigration(t, migrationKey, currMigNamespace, "remoteclusterpair", currMigNamespace, &includeResourcesFlag, &includeVolumesFlag, &startApplicationsFlag)
		log.FailOnError(t, err, "failed to create migration: %s in namespace %s", migrationKey, currMigNamespace)
		allMigrations = append(allMigrations, currMig)

	}
	err = WaitForMigration(allMigrations)
	log.FailOnError(t, err, "Error in scaled migrations")

	// Validate apps on destination
	if startApplicationsFlag {
		// wait on cluster 2 for the app to be running
		err = setDestinationKubeConfig()
		log.FailOnError(t, err, "failed to set kubeconfig to destination cluster: %v", err)

		for _, ctx := range appCtxs {
			err = schedulerDriver.WaitForRunning(ctx, defaultWaitTimeout, defaultWaitInterval)
			log.FailOnError(t, err, "Error waiting for app to get to running state on destination cluster")
		}
		// Delete all apps on destination cluster
		destroyAndWait(t, appCtxs)
	}

	err = setSourceKubeConfig()
	log.FailOnError(t, err, "failed to set kubeconfig to source cluster: %v", err)

	// Delete all apps and cluster pairs on source cluster
	destroyAndWait(t, ctxs)

	// Delete migrations
	err = deleteMigrations(allMigrations)
	log.FailOnError(t, err, "error in deleting migrations.")
}

func clusterPairFailuresTest(t *testing.T) {
	var testrailID, testResult = 86248, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	defer updateDashStats(t.Name(), &testResult)

	ctxs, err := schedulerDriver.Schedule("cluster-pair-failures",
		scheduler.ScheduleOptions{AppKeys: []string{testKey}})
	log.FailOnError(t, err, "Error scheduling task")
	Dash.VerifyFatal(t, len(ctxs), 1, "Only one task should have started")

	err = schedulerDriver.WaitForRunning(ctxs[0], defaultWaitTimeout, defaultWaitInterval)
	log.FailOnError(t, err, "Error waiting for app to get to running state")

	var clusterPairCtx = &scheduler.Context{
		UID:             ctxs[0].UID,
		ScheduleOptions: scheduler.ScheduleOptions{Namespace: "cp-failure"},
		App: &spec.AppSpec{
			Key:      ctxs[0].App.Key,
			SpecList: []interface{}{},
		}}

	badTokenInfo, errPairing := volumeDriver.GetClusterPairingInfo(remoteFilePath, "", IsCloud(), false)
	log.FailOnError(t, errPairing, "Error writing to clusterpair.yml")

	// Change token value to an incorrect token
	badTokenInfo[tokenKey] = "randomtoken"
	err = createClusterPair(badTokenInfo, false, true, defaultClusterPairDir, "")
	log.FailOnError(t, err, "Error creating cluster Spec")

	err = schedulerDriver.RescanSpecs(specDir, volumeDriverName)
	log.FailOnError(t, err, "Unable to parse spec dir")

	err = schedulerDriver.AddTasks(clusterPairCtx,
		scheduler.ScheduleOptions{AppKeys: []string{defaultClusterPairDir}})
	log.FailOnError(t, err, "Failed to schedule Cluster Pair Specs")

	// We would like to exit early in case of negative tests
	err = schedulerDriver.WaitForRunning(clusterPairCtx, defaultWaitTimeout/10, defaultWaitInterval)
	log.FailOnNoError(t, err, "Cluster pairing should have failed due to incorrect token")

	destroyAndWait(t, []*scheduler.Context{clusterPairCtx})

	badIPInfo, errPairing := volumeDriver.GetClusterPairingInfo(remoteFilePath, "", IsCloud(), false)
	log.FailOnError(t, errPairing, "Error writing to clusterpair.yml")

	badIPInfo[clusterIP] = "0.0.0.0"

	err = createClusterPair(badIPInfo, false, true, defaultClusterPairDir, "")
	log.FailOnError(t, err, "Error creating cluster Spec")

	err = schedulerDriver.RescanSpecs(specDir, volumeDriverName)
	log.FailOnError(t, err, "Unable to parse spec dir")

	err = schedulerDriver.AddTasks(clusterPairCtx,
		scheduler.ScheduleOptions{AppKeys: []string{defaultClusterPairDir}})
	log.FailOnError(t, err, "Failed to schedule Cluster Pair Specs")

	// We would like to exit early in case of negative tests
	err = schedulerDriver.WaitForRunning(clusterPairCtx, defaultWaitTimeout/10, defaultWaitInterval)
	log.FailOnNoError(t, err, "Cluster pairing should have failed due to incorrect IP")

	destroyAndWait(t, []*scheduler.Context{clusterPairCtx})

	badPortInfo, errPairing := volumeDriver.GetClusterPairingInfo(remoteFilePath, "", IsCloud(), false)
	log.FailOnError(t, errPairing, "Error writing to clusterpair.yml")

	badPortInfo[clusterPort] = "0000"

	err = createClusterPair(badPortInfo, false, true, defaultClusterPairDir, "")
	log.FailOnError(t, err, "Error creating cluster Spec")

	err = schedulerDriver.RescanSpecs(specDir, volumeDriverName)
	log.FailOnError(t, err, "Unable to parse spec dir")

	err = schedulerDriver.AddTasks(clusterPairCtx,
		scheduler.ScheduleOptions{AppKeys: []string{defaultClusterPairDir}})
	log.FailOnError(t, err, "Failed to schedule Cluster Pair Specs")

	// We would like to exit early in case of negative tests
	err = schedulerDriver.WaitForRunning(clusterPairCtx, defaultWaitTimeout/10, defaultWaitInterval)
	log.FailOnNoError(t, err, "Cluster pairing should have failed due to incorrect port")

	destroyAndWait(t, []*scheduler.Context{clusterPairCtx})
	destroyAndWait(t, ctxs)

	// If we are here then the test has passed
	testResult = testResultPass
	log.InfoD("Test status at end of %s test: %s", t.Name(), testResult)
}

func bidirectionalClusterPairTest(t *testing.T) {
	if bidirectionalClusterpair == false {
		t.Skipf("skipping %s test because bidirectional cluster pair flag has not been set", t.Name())
	}
	var testrailID, testResult = 86249, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	defer updateDashStats(t.Name(), &testResult)

	clusterPairName := "birectional-cluster-pair"
	clusterPairNamespace := "bidirectional-clusterpair-ns"

	// Read the configmap secret-config in default namespace and based on the secret type , create the bidirectional pair
	configMap, err := core.Instance().GetConfigMap("secret-config", "default")
	if err != nil {
		log.FailOnError(t, err, "error getting configmap secret-config in defaule namespace")
	}
	cmData := configMap.Data
	// Scheduler cluster pairs: source cluster --> destination cluster and destination cluster --> source cluster
	for location, secret := range cmData {
		log.InfoD("Creating a bidirectional-pair using %s as objectstore.", location)
		err := scheduleBidirectionalClusterPair(clusterPairName, clusterPairNamespace, "", storkv1.BackupLocationType(location), secret, false)
		log.FailOnError(t, err, "failed to set bidirectional cluster pair: %v", err)

		err = setSourceKubeConfig()
		log.FailOnError(t, err, "failed to set kubeconfig to source cluster: %v", err)

		_, err = storkops.Instance().GetClusterPair(clusterPairName, clusterPairNamespace)
		log.FailOnError(t, err, "failed to get bidirectional cluster pair on source: %v", err)

		err = storkops.Instance().ValidateClusterPair(clusterPairName, clusterPairNamespace, defaultWaitTimeout, defaultWaitInterval)
		log.FailOnError(t, err, "failed to validate bidirectional cluster pair on source: %v", err)

		log.InfoD("Successfully validated cluster pair %s in namespace %s on source cluster", clusterPairName, clusterPairNamespace)

		// Clean up on source cluster
		err = storkops.Instance().DeleteClusterPair(clusterPairName, clusterPairNamespace)
		log.FailOnError(t, err, "Error deleting clusterpair on source cluster")

		log.InfoD("Successfully deleted cluster pair %s in namespace %s on source cluster", clusterPairName, clusterPairNamespace)

		// Clean up destination cluster while we are on it
		err = core.Instance().DeleteNamespace(clusterPairNamespace)
		log.FailOnError(t, err, "failed to delete namespace %s on destination cluster", clusterPairNamespace)

		err = setDestinationKubeConfig()
		log.FailOnError(t, err, "failed to set kubeconfig to destination cluster: %v", err)

		_, err = storkops.Instance().GetClusterPair(clusterPairName, clusterPairNamespace)
		log.FailOnError(t, err, "failed to get bidirectional cluster pair on destination: %v", err)

		err = storkops.Instance().ValidateClusterPair(clusterPairName, clusterPairNamespace, defaultWaitTimeout, defaultWaitInterval)
		log.FailOnError(t, err, "failed to validate bidirectional cluster pair on destination: %v", err)

		log.InfoD("Successfully validated cluster pair %s in namespace %s on destination cluster", clusterPairName, clusterPairNamespace)

		// Clean up on destination cluster
		err = storkops.Instance().DeleteClusterPair(clusterPairName, clusterPairNamespace)
		log.FailOnError(t, err, "Error deleting clusterpair on destination cluster")

		// Clean up destination cluster while we are on it
		err = core.Instance().DeleteNamespace(clusterPairNamespace)
		log.FailOnError(t, err, "failed to delete namespace %s on destination cluster", clusterPairNamespace)

		log.InfoD("Successfully deleted cluster pair %s in namespace %s on destination cluster", clusterPairName, clusterPairNamespace)

		err = setSourceKubeConfig()
		log.FailOnError(t, err, "failed to set kubeconfig to source cluster: %v", err)

		log.InfoD("Successfully tested creating a bidirectional-pair using %s as objectstore.", location)
	}
	// If we are here then the test has passed
	testResult = testResultPass
	log.InfoD("Test status at end of %s test: %s", t.Name(), testResult)
}

func unidirectionalClusterPairTest(t *testing.T) {
	if unidirectionalClusterpair == false {
		t.Skipf("skipping %s test because unidirectional cluster pair flag has not been set", t.Name())
	}
	var testrailID, testResult = 91979, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	defer updateDashStats(t.Name(), &testResult)

	clusterPairName := "unirectional-cluster-pair"
	clusterPairNamespace := "unidirectional-clusterpair-ns"

	// Read the configmap secret-config in default namespace and based on the secret type , create the bidirectional pair
	configMap, err := core.Instance().GetConfigMap("secret-config", "default")
	if err != nil {
		log.FailOnError(t, err, "error getting configmap secret-config in defaule namespace")
	}
	cmData := configMap.Data
	// Scheduler cluster pairs: source cluster --> destination cluster and destination cluster --> source cluster
	for location, secret := range cmData {
		log.InfoD("Creating a unidirectional-pair using %s as objectstore.", location)
		err := scheduleUnidirectionalClusterPair(clusterPairName, clusterPairNamespace, "", storkv1.BackupLocationType(location), secret, true, false)
		log.FailOnError(t, err, "failed to set unidirectional cluster pair: %v", err)

		err = setSourceKubeConfig()
		log.FailOnError(t, err, "failed to set kubeconfig to source cluster: %v", err)

		_, err = storkops.Instance().GetClusterPair(clusterPairName, clusterPairNamespace)
		log.FailOnError(t, err, "failed to get unidirectional cluster pair on source: %v", err)

		err = storkops.Instance().ValidateClusterPair(clusterPairName, clusterPairNamespace, defaultWaitTimeout, defaultWaitInterval)
		log.FailOnError(t, err, "failed to validate unidirectional cluster pair on source: %v", err)

		log.InfoD("Successfully validated cluster pair %s in namespace %s on source cluster", clusterPairName, clusterPairNamespace)

		err = setDestinationKubeConfig()
		log.FailOnError(t, err, "failed to set kubeconfig to destination cluster: %v", err)

		_, err = storkops.Instance().GetClusterPair(clusterPairName, clusterPairNamespace)
		log.FailOnNoError(t, err, "clusterpair should not be created on destination cluster")

		err = core.Instance().DeleteNamespace(clusterPairNamespace)
		log.FailOnError(t, err, "failed to delete namespace %s on destination cluster", clusterPairNamespace)

		err = setSourceKubeConfig()
		log.FailOnError(t, err, "failed to set kubeconfig to source cluster: %v", err)

		// Clean up on source cluster
		err = storkops.Instance().DeleteClusterPair(clusterPairName, clusterPairNamespace)
		log.FailOnError(t, err, "Error deleting clusterpair on source cluster")

		log.InfoD("Successfully deleted cluster pair %s in namespace %s on source cluster", clusterPairName, clusterPairNamespace)

		err = core.Instance().DeleteNamespace(clusterPairNamespace)
		log.FailOnError(t, err, "failed to delete namespace %s on source cluster", clusterPairNamespace)
	}
	// If we are here then the test has passed
	testResult = testResultPass
	log.InfoD("Test status at end of %s test: %s", t.Name(), testResult)
}

func operatorMigrationMongoTest(t *testing.T) {
	var testrailID, testResult = 86250, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	defer updateDashStats(t.Name(), &testResult)

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
	log.InfoD("Test status at end of %s test: %s", t.Name(), testResult)
}

func operatorMigrationRabbitmqTest(t *testing.T) {
	var testrailID, testResult = 86251, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	defer updateDashStats(t.Name(), &testResult)

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
		log.FailOnError(t, err, "failed to create namespace %s for rabbitmq", rabbitmqNamespace)
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
	log.InfoD("Test status at end of %s test: %s", t.Name(), testResult)
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
				log.InfoD("Migration %s in namespace %s is pending", m.Name, m.Namespace)
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
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	defer updateDashStats(t.Name(), &testResult)
	instanceID := "mysql-migration-schedule-interval"
	appKey := "mysql-1-pvc"

	var err error
	var namespace string
	var pvcs *v1.PersistentVolumeClaimList
	// Reset config in case of error
	defer func() {
		err = setSourceKubeConfig()
		log.FailOnError(t, err, "Error resetting remote config")
	}()
	err = setMockTime(nil)
	log.FailOnError(t, err, "Error resetting mock time")
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
			log.FailOnError(t, err, "error retrieving pvc list from %s namespace", namespace)
			break
		}
	}

	// resize pvcs
	for _, pvc := range pvcs.Items {
		cap := pvc.Status.Capacity[v1.ResourceName(v1.ResourceStorage)]
		currSize := cap.Value()
		pvc.Spec.Resources.Requests[v1.ResourceStorage] = *resource.NewQuantity(int64(currSize*2), resource.BinarySI)
		_, err := core.Instance().UpdatePersistentVolumeClaim(&pvc)
		log.FailOnError(t, err, "Error updating pvc: %s/%s", pvc.GetNamespace(), pvc.GetName())
	}
	log.InfoD("Resized PVCs on source cluster")
	// bump time of the world by 5 minutes
	mockNow := time.Now().Add(6 * time.Minute)
	err = setMockTime(&mockNow)
	log.FailOnError(t, err, "Error setting mock time")

	time.Sleep(10 * time.Second)
	log.InfoD("Trigger second migration")
	validateMigration(t, "mysql-migration-schedule-interval", preMigrationCtx.GetID())

	// Change kubeconfig to destination
	err = setDestinationKubeConfig()
	log.FailOnError(t, err, "failed to set kubeconfig to source cluster: %v", err)

	// verify resized pvcs
	for _, pvc := range pvcs.Items {
		cap := pvc.Status.Capacity[v1.ResourceName(v1.ResourceStorage)]
		pvcSize := cap.Value()

		migratedPvc, err := core.Instance().GetPersistentVolumeClaim(pvc.GetName(), pvc.GetNamespace())
		log.FailOnError(t, err, "error retrieving pvc %s/%s", pvc.GetNamespace(), pvc.GetName())

		cap = migratedPvc.Status.Capacity[v1.ResourceName(v1.ResourceStorage)]
		migrSize := cap.Value()

		if migrSize != pvcSize*2 {
			resizeErr := fmt.Errorf("pvc %s/%s is not resized. Expected: %v, Current: %v", pvc.GetNamespace(), pvc.GetName(), pvcSize*2, migrSize)
			log.FailOnError(t, resizeErr, "error resizing pvc %s/%s", pvc.GetNamespace(), pvc.GetName())
		}
		srcSC, err := getStorageClassNameForPVC(&pvc)
		log.FailOnError(t, err, "error retrieving sc for %s/%s", pvc.GetNamespace(), pvc.GetName())

		destSC, err := getStorageClassNameForPVC(migratedPvc)
		log.FailOnError(t, err, "error retrieving sc for %s/%s", migratedPvc.GetNamespace(), migratedPvc.GetName())

		if srcSC != destSC {
			scErr := fmt.Errorf("migrated pvc storage class does not match")
			log.FailOnError(t, scErr, "SC Expected: %v, Current: %v", srcSC, destSC)
		}
	}

	log.InfoD("Successfully verified migrated pvcs on destination cluster")

	err = setSourceKubeConfig()
	log.FailOnError(t, err, "failed to set kubeconfig to source cluster: %v", err)
	validateAndDestroyMigration(t, ctxs, instanceID, appKey, preMigrationCtx, true, false, true, false, false, true, nil, nil)

	// If we are here then the test has passed
	testResult = testResultPass
	log.InfoD("Test status at end of %s test: %s", t.Name(), testResult)
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
	var testResult = testResultFail
	// Reset config in case of error
	defer func() {
		err = setSourceKubeConfig()
		log.FailOnError(t, err, "Error resetting remote config")
	}()
	err = setMockTime(nil)
	log.FailOnError(t, err, "Error resetting mock time")
	defer updateDashStats(t.Name(), &testResult)
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
	log.FailOnError(t, err, "failed to set kubeconfig to source cluster: %v", err)

	// start sts on dr cluster
	for _, spec := range ctxs[0].App.SpecList {
		if obj, ok := spec.(*apps_api.StatefulSet); ok {
			scale := int32(1)
			sts, err := apps.Instance().GetStatefulSet(obj.GetName(), obj.GetNamespace())
			log.FailOnError(t, err, "Error retrieving sts: %s/%s", obj.GetNamespace(), obj.GetName())
			sts.Spec.Replicas = &scale
			namespace = obj.GetNamespace()
			_, err = apps.Instance().UpdateStatefulSet(sts)
			log.FailOnError(t, err, "Error updating sts: %s/%s", obj.GetNamespace(), obj.GetName())
			break
		}
	}

	// verify migration status on DR cluster
	migrSched, err := storkops.Instance().GetMigrationSchedule("mysql-migration-schedule-interval-autosuspend", namespace)
	log.FailOnError(t, err, "failed to retrive migration schedule: %v", err)

	if migrSched.Spec.Suspend != nil && !(*migrSched.Spec.Suspend) {
		// fail test
		suspendErr := fmt.Errorf("migrationschedule is not in suspended state on DR cluster: %s/%s", migrSched.GetName(), migrSched.GetNamespace())
		log.FailOnError(t, err, "Failed: %v", suspendErr)
	}

	// validate if migrationschedule is suspended on source cluster
	err = setSourceKubeConfig()
	log.FailOnError(t, err, "failed to set kubeconfig to source cluster: %v", err)

	migrSched, err = storkops.Instance().GetMigrationSchedule("mysql-migration-schedule-interval-autosuspend", namespace)
	log.FailOnError(t, err, "failed to retrive migration schedule: %v", err)

	if migrSched.Spec.Suspend != nil && !(*migrSched.Spec.Suspend) {
		// fail test
		suspendErr := fmt.Errorf("migrationschedule is not suspended on source cluster : %s/%s", migrSched.GetName(), migrSched.GetNamespace())
		log.FailOnError(t, err, "Failed: %v", suspendErr)
	}

	log.InfoD("Successfully verified suspend migration case")

	validateAndDestroyMigration(t, ctxs, instanceID, appKey, preMigrationCtx, true, false, true, false, false, true, nil, nil)
}

func endpointMigrationTest(t *testing.T) {
	var testrailID, testResult = 86256, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	defer updateDashStats(t.Name(), &testResult)
	instanceID := "endpoint-migration-schedule-interval"
	appKey := "endpoint"

	var err error
	namespace := "endpoint-migration-schedule-interval"
	// Reset config in case of error
	defer func() {
		err = setSourceKubeConfig()
		log.FailOnError(t, err, "Error resetting remote config")
	}()
	err = setMockTime(nil)
	log.FailOnError(t, err, "Error resetting mock time")
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
	log.FailOnError(t, err, "error retrieving endpoints list from %s namespace", namespace)
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
	log.InfoD("endpoint on source cluster: %+v", srcEndpoints)

	// Change kubeconfig to destination
	err = setDestinationKubeConfig()
	log.FailOnError(t, err, "failed to set kubeconfig to source cluster: %v", err)

	destEndpoints, err := core.Instance().ListEndpoints(namespace, meta_v1.ListOptions{})
	log.FailOnError(t, err, "error retrieving endpoints list from %s namespace", namespace)

	if len(destEndpoints.Items) != cnt {
		matchErr := fmt.Errorf("migrated endpoints does not match")
		log.FailOnError(t, matchErr, "Endpoints Expected: %v, Current: %v", srcEndpoints, destEndpoints)
	}

	log.InfoD("Successfully verified migrated endpoints on destination cluster")

	err = setSourceKubeConfig()
	log.FailOnError(t, err, "failed to set kubeconfig to source cluster: %v", err)
	validateAndDestroyMigration(t, ctxs, instanceID, appKey, preMigrationCtx, true, false, true, false, false, true, nil, nil)

	// If we are here then the test has passed
	testResult = testResultPass
	log.InfoD("Test status at end of %s test: %s", t.Name(), testResult)
}

// networkPolicyMigrationTest validate migrated network policy
func networkPolicyMigrationTest(t *testing.T) {
	var testrailID, testResult = 86257, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	defer updateDashStats(t.Name(), &testResult)

	// validate default behaviour of network policy migration where policy which has CIDR set
	// will not be migrated
	validateNetworkPolicyMigration(t, false)
	log.InfoD("Validating migration of all network policy with IncludeNetworkPolicyWithCIDR set to true")
	// validate behaviour of network policy migration where policy which has CIDR set
	// will be migrated using IncludeNetworkPolicyWithCIDR option in migration schedule
	validateNetworkPolicyMigration(t, true)

	// If we are here then the test has passed
	testResult = testResultPass
	log.InfoD("Test status at end of %s test: %s", t.Name(), testResult)
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
		log.FailOnError(t, err, "Error resetting remote config")
	}()
	err = setMockTime(nil)
	log.FailOnError(t, err, "Error resetting mock time")
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
	log.FailOnError(t, err, "error retrieving network policy list from %s namespace", namespace)
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
	log.InfoD("No. of network policies which does not have CIDR set: %d", cnt)

	// Change kubeconfig to destination
	err = setDestinationKubeConfig()
	log.FailOnError(t, err, "failed to set kubeconfig to source cluster: %v", err)

	destNetworkPolicies, err := core.Instance().ListNetworkPolicy(namespace, meta_v1.ListOptions{})
	log.FailOnError(t, err, "error retrieving network policy list from %s namespace", namespace)

	if len(destNetworkPolicies.Items) != cnt {
		matchErr := fmt.Errorf("migrated network poilcy does not match")
		log.InfoD("Policy found on source cluster")
		for _, networkPolicy := range networkPolicies.Items {
			log.InfoD("Network Policy: %s/%s", networkPolicy.Namespace, networkPolicy.Name)
		}
		log.InfoD("Policy found on DR cluster")
		for _, networkPolicy := range destNetworkPolicies.Items {
			log.InfoD("Network Policy: %s/%s", networkPolicy.Namespace, networkPolicy.Name)
		}
		log.FailOnError(t, matchErr, "NetworkPolicy Expected: %v, Actual: %v", cnt, len(destNetworkPolicies.Items))
	}

	log.InfoD("Successfully verified migrated network policies on destination cluster")

	err = setSourceKubeConfig()
	log.FailOnError(t, err, "failed to set kubeconfig to source cluster: %v", err)
	validateAndDestroyMigration(t, ctxs, scheduleName, "networkpolicy", preMigrationCtx, true, false, true, false, false, true, nil, nil)
}

func transformResourceTest(t *testing.T) {
	var testrailID, testResult = 86253, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	defer updateDashStats(t.Name(), &testResult)
	instanceID := "mysql-migration-transform-interval"
	appKey := "mysql-1-pvc"

	var err error
	// Reset config in case of error
	defer func() {
		err = setSourceKubeConfig()
		log.FailOnError(t, err, "Error resetting remote config")
	}()
	err = setMockTime(nil)
	log.FailOnError(t, err, "Error resetting mock time")
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
	log.FailOnError(t, err, "failed to set kubeconfig to source cluster: %v", err)

	// verify service resource migration on destination cluster
	// Verify if statefulset get delete
	svcs, err := core.Instance().ListServices(namespace, meta_v1.ListOptions{})
	log.FailOnError(t, err, "unable to query services in namespace : %s", namespace)

	for _, svc := range svcs.Items {
		if string(svc.Spec.Type) != expectedServiceType {
			matchErr := fmt.Errorf("transformed service type does not match")
			log.FailOnError(t, matchErr, "actual: %s, expected: %s", svc.Spec.Type, expectedServiceType)
		}
		labels := svc.GetLabels()
		for k, v := range expectedLabels {
			if val, ok := labels[k]; !ok || val != v {
				matchErr := fmt.Errorf("transformed service labels does not match")
				log.FailOnError(t, matchErr, "actual: %s, expected: %s", labels, expectedLabels)
			}
		}
	}
	err = setSourceKubeConfig()
	log.FailOnError(t, err, "failed to set kubeconfig to source cluster: %v", err)
	validateAndDestroyMigration(t, ctxs, instanceID, appKey, preMigrationCtx, true, false, true, false, true, true, nil, nil)

	// If we are here then the test has passed
	testResult = testResultPass
	log.InfoD("Test status at end of %s test: %s", t.Name(), testResult)
}

// transformCRResourceTest validates transformed CR resource
func transformCRResourceTest(t *testing.T) {
	var testrailID, testResult = 297495, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	defer updateDashStats(t.Name(), &testResult)

	instanceID := "mongodb-cr-transform"
	appKey := "transform-mongodb-cr"

	var err error
	// Reset config in case of error
	defer func() {
		err = setSourceKubeConfig()
		log.FailOnError(t, err, "Error resetting remote config")
	}()

	err = setSourceKubeConfig()
	log.FailOnError(t, err, "failed to set kubeconfig to source cluster: %v", err)

	appName := appNameMongo
	appPath := appPathMongo
	appData := asyncdr.GetAppData(appName)
	podsCreated, err := asyncdr.PrepareApp(appName, appPath)
	log.FailOnError(t, err, "Error creating pods")

	podsCreatedLen := len(podsCreated.Items)
	log.InfoD("podsCreatedLen: %d", podsCreatedLen)
	sourceClusterConfigPath, err := getClusterConfigPath(srcConfig)
	log.FailOnError(t, err, "Error getting source config path")

	destClusterConfigPath, err := getClusterConfigPath(destConfig)
	log.FailOnError(t, err, "Error getting destination config path")

	// validate CRD on source
	err = asyncdr.ValidateCRD(appData.ExpectedCrdList, sourceClusterConfigPath)
	log.FailOnError(t, err, "Error validating source crds")

	err = setSourceKubeConfig()
	log.FailOnError(t, err, "Error setting source kubeconfig")

	err = setMockTime(nil)
	log.FailOnError(t, err, "Error resetting mock time")

	err = scheduleBidirectionalClusterPair(clusterPairName, appData.Ns, "", storkv1.BackupLocationType(backupLocation), backupSecret, false)
	log.FailOnError(t, err, "Error creating cluster pair")

	err = setSourceKubeConfig()
	log.FailOnError(t, err, "Error setting source kubeconfig")

	// create resourcetransformation object
	ctxs, err := schedulerDriver.Schedule(instanceID,
		scheduler.ScheduleOptions{
			AppKeys:   []string{appKey},
			Namespace: appData.Ns,
		})
	log.FailOnError(t, err, "Error scheduling task")
	Dash.VerifyFatal(t, len(ctxs), 1, "Only one task should have started")

	err = schedulerDriver.WaitForRunning(ctxs[0], defaultWaitTimeout, defaultWaitInterval)
	log.FailOnError(t, err, "Error waiting for app to get to running state")

	// wait for 2 minutes in a loop for the status to become completed
	transformations := []string{"mongodb-cr-transform"}
	var transformation *storkv1.ResourceTransformation
	isResourceTransformationReady := false

	for i := 0; i < 8; i++ {
		transformation, err = storkops.Instance().GetResourceTransformation(transformations[0], appData.Ns)
		if err != nil {
			log.FailOnError(t, err, fmt.Sprintf("Error getting resource transformation %s in namespace %s", transformations[0], appData.Ns))
		}
		if transformation.Status.Status == storkv1.ResourceTransformationStatusReady {
			isResourceTransformationReady = true
			break
		}
		time.Sleep(15 * time.Second)
	}

	if !isResourceTransformationReady {
		log.FailOnError(t, err, "Resource transformation is not ready after 2 minutes, hence failing the test")
	}

	log.InfoD("Starting migration %s/%s with startApplication true", appData.Ns, migNamePref+appName)
	startApplications := true
	mig, err := asyncdr.CreateMigration(migNamePref+appName, appData.Ns, clusterPairName, appData.Ns, &includeVolumesFlag, &includeResourcesFlag, &startApplications, transformations)
	err = asyncdr.WaitForMigration([]*storkv1.Migration{mig})
	log.FailOnError(t, err, "Error waiting for migration")
	log.InfoD("Migration %s/%s completed successfully ", appData.Ns, migNamePref+appName)

	err = setDestinationKubeConfig()
	log.FailOnError(t, err, "Error setting dest kubeconfig")

	// validate CRD on destination
	err = asyncdr.ValidateCRD(appData.ExpectedCrdList, destClusterConfigPath)
	log.FailOnError(t, err, "Error validating destination crds")

	validated, err := validateCR(appName, appData.Ns, destClusterConfigPath)
	log.FailOnError(t, err, fmt.Sprintf("Error validating CR for app %s in namespace %s", appName, appData.Ns))
	Dash.VerifyFatal(t, validated, true, fmt.Sprintf("CR for app %s in namespace %s should have been present as startApplications is true", appName, appData.Ns))

	// verify that the transformations on CR have been applied
	err = validateCRWithTransformationValue(appName, appData.Ns, transformation, destClusterConfigPath)
	log.FailOnError(t, err, fmt.Sprintf("Error validating CR for app %s in namespace %s", appName, appData.Ns))

	// If we are here then the test has passed
	asyncdr.DeleteCRAndUninstallCRD(appData.OperatorName, appPath, appData.Ns)
	err = core.Instance().DeleteNamespace(appData.Ns)
	if err != nil {
		log.Error("Error deleting namespace %s in destination: %v\n", appData.Ns, err)
	}
	err = setSourceKubeConfig()
	log.FailOnError(t, err, "Error setting source kubeconfig")

	err = asyncdr.DeleteAndWaitForMigrationDeletion(mig.Name, mig.Namespace)
	log.FailOnError(t, err, "Error deleting migration")

	asyncdr.DeleteCRAndUninstallCRD(appData.OperatorName, appPath, appData.Ns)
	err = core.Instance().DeleteNamespace(appData.Ns)
	if err != nil {
		log.Error("Error deleting namespace %s in source: %v\n", appData.Ns, err)
	}
	time.Sleep(30 * time.Second)
	log.InfoD("Test %s ended", t.Name())
	// If we are here then the test has passed
	testResult = testResultPass
	log.InfoD("Test status at end of %s test: %s", t.Name(), testResult)
}

func excludeResourceTypeDeploymentTest(t *testing.T) {
	var testrailID, testResult = 93402, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	defer updateDashStats(t.Name(), &testResult)
	instanceID := "exclude-resourcetype-deployment"
	appKey := "mysql-1-pvc"
	pvcName := "mysql-data"
	namespace := fmt.Sprintf("%s-%s", appKey, instanceID)

	var err error
	defer func() {
		err = setSourceKubeConfig()
		log.FailOnError(t, err, "Error resetting source config")
	}()

	ctxs, preMigrationCtx := triggerMigration(
		t,
		instanceID,
		appKey,
		[]string{},
		[]string{instanceID},
		false,
		false,
		true, //Starting apps after migration
		false,
		"",
		nil)

	err = schedulerDriver.WaitForRunning(ctxs[0], defaultWaitTimeout/2, defaultWaitInterval)
	log.FailOnError(t, err, "Migration could not be completed")

	// Change kubeconfig to destination
	err = setDestinationKubeConfig()
	log.FailOnError(t, err, "failed to set kubeconfig to destination cluster: %v", err)

	destDeployments, err := apps.Instance().ListDeployments(namespace, meta_v1.ListOptions{})
	log.FailOnError(t, err, "error retrieving deployments from %s namespace", namespace)
	Dash.VerifyFatal(t, len(destDeployments.Items), 0, fmt.Sprintf("Expected no deployments in destination in %s namespace", namespace))
	destServices, err := core.Instance().ListServices(namespace, meta_v1.ListOptions{})
	log.FailOnError(t, err, "error retrieving services from %s namespace", namespace)
	Dash.VerifyFatal(t, len(destServices.Items), 1, fmt.Sprintf("Expected 1 service in destination in %s namespace", namespace))
	_, err = core.Instance().GetPersistentVolumeClaim(pvcName, namespace)
	log.FailOnError(t, err, "getting pvc %s in destination in %s namespace failed", pvcName, namespace)

	destroyAndWait(t, []*scheduler.Context{preMigrationCtx})

	err = setSourceKubeConfig()
	log.FailOnError(t, err, "failed to set kubeconfig to source cluster: %v", err)

	destroyAndWait(t, ctxs)

	blowNamespacesForTest(t, instanceID, appKey, false)
	// If we are here then the test has passed
	testResult = testResultPass
	log.InfoD("Test status at end of %s test: %s", t.Name(), testResult)
}

func excludeResourceTypePVCTest(t *testing.T) {
	var testrailID, testResult = 93403, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	defer updateDashStats(t.Name(), &testResult)
	instanceID := "exclude-resourcetype-pvc"
	appKey := "mysql-1-pvc"
	pvcName := "mysql-data"
	namespace := fmt.Sprintf("%s-%s", appKey, instanceID)

	var err error
	defer func() {
		err = setSourceKubeConfig()
		log.FailOnError(t, err, "Error resetting source config")
	}()

	ctxs, preMigrationCtx := triggerMigration(
		t,
		instanceID,
		appKey,
		[]string{},
		[]string{instanceID},
		false,
		false,
		true, //Starting apps after migration
		false,
		"",
		nil)

	err = schedulerDriver.WaitForRunning(ctxs[0], defaultWaitTimeout/2, defaultWaitInterval)
	log.FailOnError(t, err, "Migration could not be completed")

	// Change kubeconfig to destination
	err = setDestinationKubeConfig()
	log.FailOnError(t, err, "failed to set kubeconfig to destination cluster: %v", err)

	destDeployments, err := apps.Instance().ListDeployments(namespace, meta_v1.ListOptions{})
	log.FailOnError(t, err, "error retrieving deployments from %s namespace", namespace)
	Dash.VerifyFatal(t, len(destDeployments.Items), 1, fmt.Sprintf("Expected 1 deployment in destination in %s namespace", namespace))
	destServices, err := core.Instance().ListServices(namespace, meta_v1.ListOptions{})
	log.FailOnError(t, err, "error retrieving services from %s namespace", namespace)
	Dash.VerifyFatal(t, len(destServices.Items), 1, fmt.Sprintf("Expected 1 service in destination in %s namespace", namespace))
	_, err = core.Instance().GetPersistentVolumeClaim(pvcName, namespace)
	log.FailOnNoError(t, err, "getting pvc in destination in %s namespace should have failed", namespace)

	destroyAndWait(t, []*scheduler.Context{preMigrationCtx})

	err = setSourceKubeConfig()
	log.FailOnError(t, err, "failed to set kubeconfig to source cluster: %v", err)

	destroyAndWait(t, ctxs)

	blowNamespacesForTest(t, instanceID, appKey, false)
	// If we are here then the test has passed
	testResult = testResultPass
	log.InfoD("Test status at end of %s test: %s", t.Name(), testResult)
}

func excludeMultipleResourceTypesTest(t *testing.T) {
	var testrailID, testResult = 93404, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	defer updateDashStats(t.Name(), &testResult)
	instanceID := "exclude-multiple-resourcetypes-migration"
	appKey := "mysql-1-pvc"
	pvcName := "mysql-data"
	namespace := fmt.Sprintf("%s-%s", appKey, instanceID)

	var err error
	defer func() {
		err = setSourceKubeConfig()
		log.FailOnError(t, err, "Error resetting source config")
	}()

	ctxs, preMigrationCtx := triggerMigration(
		t,
		instanceID,
		appKey,
		[]string{},
		[]string{instanceID},
		false,
		false,
		true, //Starting apps after migration
		false,
		"",
		nil)

	err = schedulerDriver.WaitForRunning(ctxs[0], defaultWaitTimeout/2, defaultWaitInterval)
	log.FailOnError(t, err, "Migration could not be completed")

	// Change kubeconfig to destination
	err = setDestinationKubeConfig()
	log.FailOnError(t, err, "failed to set kubeconfig to destination cluster: %v", err)

	destDeployments, err := apps.Instance().ListDeployments(namespace, meta_v1.ListOptions{})
	log.FailOnError(t, err, "error retrieving deployments from %s namespace", namespace)
	Dash.VerifyFatal(t, len(destDeployments.Items), 0, fmt.Sprintf("Expected no deployments in destination in %s namespace", namespace))
	destServices, err := core.Instance().ListServices(namespace, meta_v1.ListOptions{})
	log.FailOnError(t, err, "error retrieving services from %s namespace", namespace)
	Dash.VerifyFatal(t, len(destServices.Items), 0, fmt.Sprintf("Expected no service in destination in %s namespace", namespace))
	_, err = core.Instance().GetPersistentVolumeClaim(pvcName, namespace)
	log.FailOnError(t, err, "getting pvc %s in destination in %s namespace failed", pvcName, namespace)

	destroyAndWait(t, []*scheduler.Context{preMigrationCtx})

	err = setSourceKubeConfig()
	log.FailOnError(t, err, "failed to set kubeconfig to source cluster: %v", err)

	destroyAndWait(t, ctxs)

	blowNamespacesForTest(t, instanceID, appKey, false)
	// If we are here then the test has passed
	testResult = testResultPass
	log.InfoD("Test status at end of %s test: %s", t.Name(), testResult)
}

func excludeResourceTypesWithSelectorsTest(t *testing.T) {
	var testrailID, testResult = 93466, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	defer updateDashStats(t.Name(), &testResult)
	instanceID := "exclude-resourcetype-selector"
	appKey := "mysql-1-pvc"
	pvcName := "mysql-data"
	namespace := fmt.Sprintf("%s-%s", appKey, instanceID)

	var err error
	defer func() {
		err = setSourceKubeConfig()
		log.FailOnError(t, err, "Error resetting source config")
	}()

	ctxs, preMigrationCtx := triggerMigration(
		t,
		instanceID,
		appKey,
		[]string{},
		[]string{instanceID},
		false,
		false,
		true, //Starting apps after migration
		false,
		"",
		nil)

	err = schedulerDriver.WaitForRunning(ctxs[0], defaultWaitTimeout/2, defaultWaitInterval)
	log.FailOnError(t, err, "Migration could not be completed")

	// Change kubeconfig to destination
	err = setDestinationKubeConfig()
	log.FailOnError(t, err, "failed to set kubeconfig to destination cluster: %v", err)

	destDeployments, err := apps.Instance().ListDeployments(namespace, meta_v1.ListOptions{})
	log.FailOnError(t, err, "error retrieving deployments from %s namespace", namespace)
	Dash.VerifyFatal(t, len(destDeployments.Items), 1, fmt.Sprintf("Expected 1 deployment in destination in %s namespace", namespace))
	destServices, err := core.Instance().ListServices(namespace, meta_v1.ListOptions{})
	log.FailOnError(t, err, "error retrieving services from %s namespace", namespace)
	Dash.VerifyFatal(t, len(destServices.Items), 0, fmt.Sprintf("Expected no service in destination in %s namespace", namespace))
	_, err = core.Instance().GetPersistentVolumeClaim(pvcName, namespace)
	log.FailOnError(t, err, "getting pvc %s in destination in %s namespace failed", pvcName, namespace)

	destroyAndWait(t, []*scheduler.Context{preMigrationCtx})

	err = setSourceKubeConfig()
	log.FailOnError(t, err, "failed to set kubeconfig to source cluster: %v", err)
	destroyAndWait(t, ctxs)

	blowNamespacesForTest(t, instanceID, appKey, false)
	// If we are here then the test has passed
	testResult = testResultPass
	log.InfoD("Test status at end of %s test: %s", t.Name(), testResult)
}

func excludeNonExistingResourceTypesTest(t *testing.T) {
	var testrailID, testResult = 93503, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	defer updateDashStats(t.Name(), &testResult)
	instanceID := "exclude-nonexisting-resourcetype-migration"
	appKey := "mysql-1-pvc"
	pvcName := "mysql-data"
	namespace := fmt.Sprintf("%s-%s", appKey, instanceID)

	var err error
	defer func() {
		err = setSourceKubeConfig()
		log.FailOnError(t, err, "Error resetting source config")
	}()

	ctxs, preMigrationCtx := triggerMigration(
		t,
		instanceID,
		appKey,
		[]string{},
		[]string{instanceID},
		false,
		false,
		true, //Starting apps after migration
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
		nil,
		nil,
	)

	// Change kubeconfig to destination
	err = setDestinationKubeConfig()
	log.FailOnError(t, err, "failed to set kubeconfig to destination cluster: %v", err)

	destDeployments, err := apps.Instance().ListDeployments(namespace, meta_v1.ListOptions{})
	log.FailOnError(t, err, "error retrieving deployments from %s namespace", namespace)
	Dash.VerifyFatal(t, len(destDeployments.Items), 1, fmt.Sprintf("Expected 1 deployment in destination in %s namespace", namespace))
	destStatefulSets, err := apps.Instance().ListStatefulSets(namespace, meta_v1.ListOptions{})
	log.FailOnError(t, err, "error retrieving statefulsets from %s namespace", namespace)
	Dash.VerifyFatal(t, len(destStatefulSets.Items), 0, fmt.Sprintf("Expected no statefulset in destination in %s namespace", namespace))
	destServices, err := core.Instance().ListServices(namespace, meta_v1.ListOptions{})
	log.FailOnError(t, err, "error retrieving services from %s namespace", namespace)
	Dash.VerifyFatal(t, len(destServices.Items), 1, fmt.Sprintf("Expected 1 service in destination in %s namespace", namespace))
	_, err = core.Instance().GetPersistentVolumeClaim(pvcName, namespace)
	log.FailOnError(t, err, "getting pvc %s in destination in %s namespace failed", pvcName, namespace)

	err = setSourceKubeConfig()
	log.FailOnError(t, err, "failed to set kubeconfig to source cluster: %v", err)
	validateAndDestroyMigration(
		t,
		ctxs,
		instanceID,
		appKey,
		preMigrationCtx,
		true,
		true,
		true,
		false,
		false,
		true,
		nil,
		nil,
	)

	// If we are here then the test has passed
	testResult = testResultPass
	log.InfoD("Test status at end of %s test: %s", t.Name(), testResult)
}

// Testrail ID 85741 - https://portworx.testrail.net/index.php?/cases/view/85741
func serviceAndServiceAccountUpdate(t *testing.T) {
	var testrailID, testResult = 85741, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	defer updateDashStats(t.Name(), &testResult)

	var err error
	// Reset config in case of error
	defer func() {
		err = setSourceKubeConfig()
		log.FailOnError(t, err, "Error resetting remote config")
	}()
	err = setMockTime(nil)
	log.FailOnError(t, err, "Error resetting mock time")
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
	log.FailOnError(t, err, "Error resetting mock time")

	_, preMigrationCtx := triggerMigration(t, instanceID, appKey, additionalAppKey, []string{migrationAppKey}, migrateAllAppsExpected, skipStoragePair, startAppsOnMigration, pairReverse, "", nil)

	// Validate intial migration
	validateMigration(t, migrationAppKey, preMigrationCtx.GetID())

	log.InfoD("First migration completed successfully. Updating service account and service before next migration.")

	// Update service account's secret references, automountservicetoken, and service's ports
	serviceAccountSrc, err := core.Instance().GetServiceAccount(appKey, testNamespace)
	log.FailOnError(t, err, "Error getting service account on source")

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
	log.FailOnError(t, err, "Error updating service account on source")

	// After update do a get on service account to get values of secret references
	serviceAccountSrc, err = core.Instance().GetServiceAccount(appKey, testNamespace)
	log.FailOnError(t, err, "Error getting service account on source")

	// Collect secret references on source cluster
	secretRefSrc := make(map[string]bool)
	for i := 0; i < len(serviceAccountSrc.Secrets); i++ {
		secretRefSrc[serviceAccountSrc.Secrets[i].Name] = true
	}

	mysqlService, err := core.Instance().GetService("mysql-service", testNamespace)
	log.FailOnError(t, err, "Error getting mysql service on source")

	// Update ports on the service on source clusters
	var updatedPorts []int32
	for i := 0; i < len(mysqlService.Spec.Ports); i++ {
		mysqlService.Spec.Ports[i].Port = mysqlService.Spec.Ports[i].Port + 1
		updatedPorts = append(updatedPorts, mysqlService.Spec.Ports[i].Port)
	}

	mysqlService, err = core.Instance().UpdateService(mysqlService)
	log.FailOnError(t, err, "Error updating mysql service on source")

	log.InfoD("Waiting for next migration to trigger...")
	time.Sleep(5 * time.Minute)

	validateMigration(t, migrationAppKey, preMigrationCtx.GetID())
	log.InfoD("Migration completed successfully, after updating service and service account.")

	// Verify service account update on migration
	err = setDestinationKubeConfig()
	serviceAccountDest, err := core.Instance().GetServiceAccount(appKey, testNamespace)
	log.FailOnError(t, err, "Error getting service account on destination")
	Dash.VerifyFatal(t, *serviceAccountDest.AutomountServiceAccountToken, true, "AutomountServiceAccountToken updated on destination")

	// Delete service account on source and migrate again
	log.InfoD("Deleting  service account on source cluster.")
	err = setSourceKubeConfig()
	log.FailOnError(t, err, "failed to set kubeconfig to source cluster: %v", err)

	err = core.Instance().DeleteServiceAccount(appKey, testNamespace)
	log.FailOnError(t, err, "Failed to delete service account on source")

	time.Sleep(5 * time.Minute)

	validateMigration(t, migrationAppKey, preMigrationCtx.GetID())
	log.InfoD("Migration completed successfully, after deletion of service account.")

	// Validate service and service account on destination cluster, for updates made on the source cluster earlier
	err = setDestinationKubeConfig()
	log.FailOnError(t, err, "failed to set kubeconfig to destination cluster: %v", err)

	serviceAccountDest, err = core.Instance().GetServiceAccount(appKey, testNamespace)
	log.FailOnError(t, err, "service account NOT found on destination, should NOT have been deleted by migration")

	// Get secret references on destination and compare with source cluster
	secretRefDest := make(map[string]bool)
	for i := 0; i < len(serviceAccountDest.Secrets); i++ {
		secretRefDest[serviceAccountDest.Secrets[i].Name] = true
	}

	// First check that there are more secret references on destination
	Dash.VerifyFatal(t, len(secretRefDest) >= len(secretRefSrc), true,
		"number of secret references should be greater or equal on destination since we append")

	// Also check if all secret references from source are present on destination as well
	for sec := range secretRefSrc {
		_, ok := secretRefDest[sec]
		Dash.VerifyFatal(t, ok, true, fmt.Sprintf("secret %s not present on destination service account", sec))
	}

	mysqlServiceDest, err := core.Instance().GetService("mysql-service", testNamespace)
	log.FailOnError(t, err, "Error getting mysql service on source")

	// Validate ports for service are updated on destination cluster
	for i := 0; i < len(mysqlServiceDest.Spec.Ports); i++ {
		Dash.VerifyFatal(t, updatedPorts[i], mysqlServiceDest.Spec.Ports[i].Port,
			fmt.Sprintf("ports not updated for service on destination cluster, expected %d, found %d",
				updatedPorts[i], mysqlServiceDest.Spec.Ports[i].Port))
	}

	// Clean up destination cluster while we are on it
	err = core.Instance().DeleteNamespace(testNamespace)
	log.FailOnError(t, err, "failed to delete namespace %s on destination cluster", testNamespace)

	// Clean up source cluster
	err = setSourceKubeConfig()
	log.FailOnError(t, err, "failed to set kubeconfig to source cluster: %v", err)

	err = core.Instance().DeleteNamespace(testNamespace)
	log.FailOnError(t, err, "failed to delete namespace %s on destination cluster", testNamespace)

	err = setMockTime(nil)
	log.FailOnError(t, err, "Error resetting mock time")

	// If we are here then the test has passed
	testResult = testResultPass
	log.InfoD("Test status at end of %s test: %s", t.Name(), testResult)
}

func scheduleClusterPairGeneric(t *testing.T, ctxs []*scheduler.Context,
	appKey, instanceID string,
	defaultClusterPairDir, projectIDMappings string,
	skipStoragePair, resetConfig, pairReverse bool) {
	var err error
	if bidirectionalClusterpair {
		clusterPairNamespace := fmt.Sprintf("%s-%s", appKey, instanceID)
		log.Info("Bidirectional flag is set, will create bidirectional cluster pair:")
		log.InfoD("Name: %s", remotePairName)
		log.InfoD("Namespace: %s", clusterPairNamespace)
		log.InfoD("Backuplocation: %s", defaultBackupLocation)
		log.InfoD("Secret name: %s", defaultSecretName)
		err = scheduleBidirectionalClusterPair(remotePairName, clusterPairNamespace, projectIDMappings, defaultBackupLocation, defaultSecretName, false)
		log.FailOnError(t, err, "failed to set bidirectional cluster pair: %v", err)
		err = setSourceKubeConfig()
		log.FailOnError(t, err, "failed to set kubeconfig to source cluster: %v", err)

	} else if unidirectionalClusterpair {
		clusterPairNamespace := fmt.Sprintf("%s-%s", appKey, instanceID)
		log.Info("Unidirectional flag is set, will create unidirectional cluster pair:")
		log.InfoD("Name: %s", remotePairName)
		log.InfoD("Namespace: %s", clusterPairNamespace)
		log.InfoD("Backuplocation: %s", defaultBackupLocation)
		log.InfoD("Secret name: %s", defaultSecretName)
		err = scheduleUnidirectionalClusterPair(remotePairName, clusterPairNamespace, projectIDMappings, defaultBackupLocation, defaultSecretName, true, pairReverse)
		log.FailOnError(t, err, "failed to set unidirectional cluster pair: %v", err)
	} else {
		// create, apply and validate cluster pair specs
		err = scheduleClusterPair(ctxs[0], skipStoragePair, true, defaultClusterPairDir, projectIDMappings, pairReverse)
		log.FailOnError(t, err, "Error scheduling cluster pair")
	}
}

func blowNamespacesForTest(t *testing.T, instanceID, appKey string, skipDestDeletion bool) {

	namespace := fmt.Sprintf("%s-%s", appKey, instanceID)
	blowNamespace(t, namespace, true)

	// for negative cases the namespace is not even created on destination, so skip deleting it
	if skipDestDeletion {
		return
	}

	blowNamespace(t, namespace, false)

	err := setSourceKubeConfig()
	log.FailOnError(t, err, "failed to set kubeconfig to destination cluster: %v", err)
}

func blowNamespaceForTest(t *testing.T, namespace string, skipDestDeletion bool) {

	blowNamespace(t, namespace, true)

	// for negative cases the namespace is not even created on destination, so skip deleting it
	if skipDestDeletion {
		return
	}

	blowNamespace(t, namespace, false)

	err := setSourceKubeConfig()
	log.FailOnError(t, err, "failed to set kubeconfig to destination cluster: %v", err)
}

func blowNamespace(t *testing.T, namespace string, source bool) {
	if source {
		err := setSourceKubeConfig()
		log.FailOnError(t, err, "failed to set kubeconfig to source cluster: %v", err)

		err = core.Instance().DeleteNamespace(namespace)
		if !errors.IsNotFound(err) {
			log.FailOnError(t, err, "failed to delete namespace %s on destination cluster", namespace)
		}
	} else {
		err := setDestinationKubeConfig()
		log.FailOnError(t, err, "failed to set kubeconfig to destination cluster: %v", err)

		err = core.Instance().DeleteNamespace(namespace)
		if !errors.IsNotFound(err) {
			log.FailOnError(t, err, "failed to delete namespace %s on destination cluster", namespace)
		}
	}
}
