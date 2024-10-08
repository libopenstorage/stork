//go:build integrationtest
// +build integrationtest

package integrationtest

import (
	"bytes"
	"fmt"
	"os"
	"testing"
	"time"

	storkv1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	storkops "github.com/libopenstorage/stork/pkg/crud/stork"
	"github.com/libopenstorage/stork/pkg/log"
	"github.com/libopenstorage/stork/pkg/storkctl"
	"github.com/portworx/sched-ops/k8s/apps"
	"github.com/portworx/torpedo/drivers/scheduler"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var (
	testClusterDomain, sourceClusterDomain, destClusterDomain string
)

func TestDRActions(t *testing.T) {
	// reset mock time before running any tests
	err := setMockTime(nil)
	log.FailOnError(t, err, "Error resetting mock time")
	currentTestSuite = t.Name()

	err = setSourceKubeConfig()
	log.FailOnError(t, err, "failed to set kubeconfig to source cluster: %v", err)

	log.InfoD("Using stork volume driver: %s", volumeDriverName)
	log.InfoD("Backup path being used: %s", backupLocationPath)
	setDefaultsForBackup(t)

	// get the destination kubeconfig from configmap in source cluster so that it can be passed to storkctl commands
	// since both the dr cli commands run in destination cluster
	destinationKubeConfigPath, err = getDestinationKubeConfigFile()
	log.FailOnError(t, err, "Error getting destination kubeconfig file")

	// Get the source kubeconfig from configmap.
	srcKubeConfigPath, err = getSourceKubeConfigFile()
	log.FailOnError(t, err, "Error getting source kubeconfig file")

	t.Run("testSyncDR", testSyncDR)
	t.Run("testAsyncDR", testAsyncDR)
}

func testSyncDR(t *testing.T) {
	testClusterDomain = os.Getenv(storkTestClusterDomainPrefix)
	sourceClusterDomain = fmt.Sprintf("%s1", testClusterDomain)
	destClusterDomain = fmt.Sprintf("%s2", testClusterDomain)

	t.Run("testDRActionMetroFailover", testDRActionMetroFailover)
	t.Run("testDRActionMetroFailback", testDRActionMetroFailback)
}

func testAsyncDR(t *testing.T) {
	// Async DR tests.
	t.Run("testDRActionFailoverMultipleNamespacesTest", testDRActionFailoverMultipleNamespacesTest)
	t.Run("testDRActionFailoverSubsetNamespacesTest", testDRActionFailoverSubsetNamespacesTest)
	t.Run("testDRActionFailoverWithMigrationRunningTest", testDRActionFailoverWithMigrationRunningTest)
	t.Run("testDRActionFailbackIntervalScheduleTest", testDRActionFailbackIntervalScheduleTest)
	t.Run("testDRActionFailbackDailyScheduleTest", testDRActionFailbackDailyScheduleTest)
	t.Run("testDRActionFailbackWeeklyScheduleTest", testDRActionFailbackWeeklyScheduleTest)
	t.Run("testDRActionFailbackMonthlyScheduleTest", testDRActionFailbackMonthlyScheduleTest)
	t.Run("testDRActionFailbackModifiedSecondaryReplicasTest", testDRActionFailbackModifiedSecondaryReplicasTest)
	t.Run("testDRActionFailbackSubsetNamespacesTest", testDRActionFailbackSubsetNamespacesTest)
}

// testDRActionFailoverMultipleNamespacesTest tests failover action for multiple namespaces
func testDRActionFailoverMultipleNamespacesTest(t *testing.T) {
	var testrailID, testResult = 297512, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	defer updateDashStats(t.Name(), &testResult)

	// Create the apps for migration
	// Start migrationschedule
	instanceID := "multi-ns-migration-schedule-interval"
	// appKeys is an array of appKeys that will be used to create the apps
	mysqlApp := "mysql-1-pvc"
	mysqlNamespace := fmt.Sprintf("%s-%s", mysqlApp, instanceID)
	elasticsearchApp := "elasticsearch"
	elasticsearchNamespace := fmt.Sprintf("%s-%s", elasticsearchApp, instanceID)
	appKeys := []string{mysqlApp, elasticsearchApp}
	var err error
	// Reset config in case of error
	defer func() {
		err = setSourceKubeConfig()
		log.FailOnError(t, err, "Error resetting remote config")
	}()

	// Schedule multiple apps
	var preMigrationCtxs []*scheduler.Context
	for _, appKey := range appKeys {
		ctxs, err := schedulerDriver.Schedule(instanceID,
			scheduler.ScheduleOptions{
				AppKeys: []string{appKey},
				Labels:  nil,
			})
		log.FailOnError(t, err, "Error scheduling task")
		Dash.VerifyFatal(t, 1, len(ctxs), "Only one task should have started")
		preMigrationCtxs = append(preMigrationCtxs, ctxs[0].DeepCopy())
	}

	for _, preMigrationCtx := range preMigrationCtxs {
		err = schedulerDriver.WaitForRunning(preMigrationCtx, defaultWaitTimeout, defaultWaitInterval)
		log.FailOnError(t, err, "Error waiting for app to get to running state")
	}

	sourceDeployments, err := apps.Instance().ListDeployments(mysqlNamespace, metav1.ListOptions{})
	log.FailOnError(t, err, "error retrieving deployments from %s namespace", mysqlNamespace)
	Dash.VerifyFatal(t, len(sourceDeployments.Items), 1, fmt.Sprintf("Expected 1 deployment in source in %s namespace", mysqlNamespace))
	sourceDeploymentReplicas := *sourceDeployments.Items[0].Spec.Replicas
	sourceStatefulsets, err := apps.Instance().ListStatefulSets(elasticsearchNamespace, metav1.ListOptions{})
	log.FailOnError(t, err, "error retrieving statefulsets from %s namespace", elasticsearchNamespace)
	Dash.VerifyFatal(t, len(sourceStatefulsets.Items), 1, fmt.Sprintf("Expected 1 statefulset in source in %s namespace", elasticsearchNamespace))
	sourceStatefulsetReplicas := *sourceStatefulsets.Items[0].Spec.Replicas

	// Create the clusterpair
	clusterPairNamespace := defaultAdminNamespace
	log.Info("Creating bidirectional cluster pair:")
	log.InfoD("Name: %s", remotePairName)
	log.InfoD("Namespace: %s", clusterPairNamespace)
	log.InfoD("Backuplocation: %s", defaultBackupLocation)
	log.InfoD("Secret name: %s", defaultSecretName)
	err = scheduleBidirectionalClusterPair(remotePairName, clusterPairNamespace, projectIDMappings, defaultBackupLocation, defaultSecretName, false)
	log.FailOnError(t, err, "failed to set bidirectional cluster pair: %v", err)
	err = setSourceKubeConfig()
	log.FailOnError(t, err, "failed to set kubeconfig to source cluster: %v", err)

	// Create the migration schedule
	// we want to create the schedulePolicy and migrationSchedule using storkctl instead of scheduling apps using torpedo's scheduler
	// schedulePolicyArgs is a map of schedulePolicyName : {{flag1:value1,flag2:value2,....}}
	schedulePolicyArgs := make(map[string]map[string]string)
	schedulePolicyArgs["migrate-every-5m"] = map[string]string{"policy-type": "Interval", "interval-minutes": "5"}

	// migrationScheduleArgs is a map of migrationScheduleName : {{flag1:value1,flag2:value2,....}}
	migrationScheduleArgs := make(map[string]map[string]string)
	migrationScheduleArgs[instanceID] = map[string]string{
		"purge-deleted-resources": "",
		"schedule-policy-name":    "migrate-every-5m",
	}

	//Create schedulePolicies using storkCtl if any required
	factory := storkctl.NewFactory()
	var outputBuffer bytes.Buffer
	cmd := storkctl.NewCommand(factory, os.Stdin, &outputBuffer, os.Stderr)
	for schedulePolicyName, customArgs := range schedulePolicyArgs {
		cmdArgs := []string{"create", "schedulepolicy", schedulePolicyName}
		executeStorkCtlCommand(t, cmd, cmdArgs, customArgs)
	}
	//Create migrationSchedules using storkCtl
	migrationScheduleName := "forward-migration-schedule-multiple-ns"
	namespacesValue := fmt.Sprintf("%s,%s", mysqlNamespace, elasticsearchNamespace)
	cmdArgs := []string{"create", "migrationschedule", migrationScheduleName, "-c", remotePairName,
		"--namespaces", namespacesValue, "-n", defaultAdminNamespace}
	executeStorkCtlCommand(t, cmd, cmdArgs, migrationScheduleArgs[instanceID])

	// bump time of the world by 6 minutes
	mockNow := time.Now().Add(6 * time.Minute)
	err = setMockTime(&mockNow)
	log.FailOnError(t, err, "Error setting mock time")
	defer func() {
		// Reset mocktime.
		err = setMockTime(nil)
		log.FailOnError(t, err, "Error resetting mock time")
	}()

	// Need to Validate the migrationSchedules separately because they are created using storkctl
	// and not a part of the torpedo scheduler context
	_, err = storkops.Instance().ValidateMigrationSchedule(migrationScheduleName, defaultAdminNamespace, defaultWaitTimeout, defaultWaitInterval)

	// Failover the application
	err = setDestinationKubeConfig()
	log.FailOnError(t, err, "failed to set kubeconfig to destination cluster: %v", err)

	failoverCmdArgs := map[string]string{
		"migration-reference": migrationScheduleName,
		"include-namespaces":  namespacesValue,
		"namespace":           defaultAdminNamespace,
	}
	drActionName, _ := createDRAction(t, defaultAdminNamespace, storkv1.ActionTypeFailover, migrationScheduleName, failoverCmdArgs)

	// Wait for failover action to complete
	waitTillActionComplete(t, storkv1.ActionTypeFailover, drActionName, defaultAdminNamespace)

	// Verify the application is running on the destination cluster
	destDeployments, err := apps.Instance().ListDeployments(mysqlNamespace, metav1.ListOptions{})
	log.FailOnError(t, err, "error retrieving deployments from %s namespace", mysqlNamespace)
	Dash.VerifyFatal(t, len(destDeployments.Items), 1, fmt.Sprintf("Expected 1 deployment in destination in %s namespace", mysqlNamespace))
	Dash.VerifyFatal(t, *destDeployments.Items[0].Spec.Replicas, sourceDeploymentReplicas, fmt.Sprintf("Expected %d replica in destination in %s namespace", sourceDeploymentReplicas, mysqlNamespace))

	destStatefulsets, err := apps.Instance().ListStatefulSets(elasticsearchNamespace, metav1.ListOptions{})
	log.FailOnError(t, err, "error retrieving statefulsets from %s namespace", elasticsearchNamespace)
	Dash.VerifyFatal(t, len(destStatefulsets.Items), 1, fmt.Sprintf("Expected 1 statefulset in destination in %s namespace", elasticsearchNamespace))
	Dash.VerifyFatal(t, *destStatefulsets.Items[0].Spec.Replicas, sourceStatefulsetReplicas, fmt.Sprintf("Expected %d replica in destination in %s namespace", sourceStatefulsetReplicas, elasticsearchNamespace))

	err = storkops.Instance().DeleteClusterPair(remotePairName, defaultAdminNamespace)
	log.FailOnError(t, err, "failed to delete clusterpair %s in namespace %s in destination: %v", remotePairName, defaultAdminNamespace, err)
	// Delete migrationschedule.
	DeleteAndWaitForMigrationScheduleDeletion(t, migrationScheduleName, defaultAdminNamespace)

	// Verify the application is not running on the source cluster
	err = setSourceKubeConfig()
	log.FailOnError(t, err, "failed to set kubeconfig to source cluster: %v", err)
	sourceDeployments, err = apps.Instance().ListDeployments(mysqlNamespace, metav1.ListOptions{})
	log.FailOnError(t, err, "error retrieving deployments from %s namespace", mysqlNamespace)
	Dash.VerifyFatal(t, len(sourceDeployments.Items), 1, fmt.Sprintf("Expected 1 deployment in source in %s namespace", mysqlNamespace))
	Dash.VerifyFatal(t, *sourceDeployments.Items[0].Spec.Replicas, 0, fmt.Sprintf("Expected 0 replica in source in deployment in %s namespace", mysqlNamespace))

	sourceStatefulsets, err = apps.Instance().ListStatefulSets(elasticsearchNamespace, metav1.ListOptions{})
	log.FailOnError(t, err, "error retrieving statefulsets from %s namespace", elasticsearchNamespace)
	Dash.VerifyFatal(t, len(sourceStatefulsets.Items), 1, fmt.Sprintf("Expected 1 statefulset in source in %s namespace", elasticsearchNamespace))
	Dash.VerifyFatal(t, *sourceStatefulsets.Items[0].Spec.Replicas, 0, fmt.Sprintf("Expected 0 replica in source in sts in %s namespace", elasticsearchNamespace))

	DeleteAndWaitForMigrationScheduleDeletion(t, migrationScheduleName, defaultAdminNamespace)
	for schedulePolicyName := range schedulePolicyArgs {
		cmdArgs := []string{"delete", "schedulepolicy", schedulePolicyName}
		executeStorkCtlCommand(t, cmd, cmdArgs, nil)
	}
	err = storkops.Instance().DeleteClusterPair(remotePairName, defaultAdminNamespace)
	log.FailOnError(t, err, "failed to delete clusterpair %s in namespace %s in source: %v", remotePairName, defaultAdminNamespace, err)

	// cleanup
	destroyAndWait(t, preMigrationCtxs)
	for _, ns := range []string{mysqlNamespace, elasticsearchNamespace} {
		blowNamespaceForTest(t, ns, false)
	}
	// If we are here then the test has passed
	testResult = testResultPass
	log.InfoD("Test status at end of %s test: %s", t.Name(), testResult)
}

// testDRActionFailoverSubsetNamespacesTest tests failover action for subset of namespaces
func testDRActionFailoverSubsetNamespacesTest(t *testing.T) {
	var testrailID, testResult = 297513, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	defer updateDashStats(t.Name(), &testResult)

	// Create the apps for migration
	// Start migrationschedule
	instanceID := "subset-ns-migration-schedule-interval"
	// appKeys is an array of appKeys that will be used to create the apps
	mysqlApp := "mysql-1-pvc"
	mysqlNamespace := fmt.Sprintf("%s-%s", mysqlApp, instanceID)
	elasticsearchApp := "elasticsearch"
	elasticsearchNamespace := fmt.Sprintf("%s-%s", elasticsearchApp, instanceID)
	appKeys := []string{mysqlApp, elasticsearchApp}
	var err error
	// Reset config in case of error
	defer func() {
		err = setSourceKubeConfig()
		log.FailOnError(t, err, "Error resetting remote config")
	}()

	// Schedule multiple apps
	var preMigrationCtxs []*scheduler.Context
	for _, appKey := range appKeys {
		ctxs, err := schedulerDriver.Schedule(instanceID,
			scheduler.ScheduleOptions{
				AppKeys: []string{appKey},
				Labels:  nil,
			})
		log.FailOnError(t, err, "Error scheduling task")
		Dash.VerifyFatal(t, 1, len(ctxs), "Only one task should have started")
		preMigrationCtxs = append(preMigrationCtxs, ctxs[0].DeepCopy())
	}

	for _, preMigrationCtx := range preMigrationCtxs {
		err = schedulerDriver.WaitForRunning(preMigrationCtx, defaultWaitTimeout, defaultWaitInterval)
		log.FailOnError(t, err, "Error waiting for app to get to running state")
	}

	sourceDeployments, err := apps.Instance().ListDeployments(mysqlNamespace, metav1.ListOptions{})
	log.FailOnError(t, err, "error retrieving deployments from %s namespace", mysqlNamespace)
	Dash.VerifyFatal(t, len(sourceDeployments.Items), 1, fmt.Sprintf("Expected 1 deployment in source in %s namespace", mysqlNamespace))
	sourceDeploymentReplicas := *sourceDeployments.Items[0].Spec.Replicas
	sourceStatefulsets, err := apps.Instance().ListStatefulSets(elasticsearchNamespace, metav1.ListOptions{})
	log.FailOnError(t, err, "error retrieving statefulsets from %s namespace", elasticsearchNamespace)
	Dash.VerifyFatal(t, len(sourceStatefulsets.Items), 1, fmt.Sprintf("Expected 1 statefulset in source in %s namespace", elasticsearchNamespace))
	sourceStatefulsetReplicas := *sourceStatefulsets.Items[0].Spec.Replicas

	// Create the clusterpair
	clusterPairNamespace := defaultAdminNamespace
	log.Info("Creating bidirectional cluster pair:")
	log.InfoD("Name: %s", remotePairName)
	log.InfoD("Namespace: %s", clusterPairNamespace)
	log.InfoD("Backuplocation: %s", defaultBackupLocation)
	log.InfoD("Secret name: %s", defaultSecretName)
	err = scheduleBidirectionalClusterPair(remotePairName, clusterPairNamespace, projectIDMappings, defaultBackupLocation, defaultSecretName, false)
	log.FailOnError(t, err, "failed to set bidirectional cluster pair: %v", err)
	err = setSourceKubeConfig()
	log.FailOnError(t, err, "failed to set kubeconfig to source cluster: %v", err)

	// Create the migration schedule
	// we want to create the schedulePolicy and migrationSchedule using storkctl instead of scheduling apps using torpedo's scheduler
	// schedulePolicyArgs is a map of schedulePolicyName : {{flag1:value1,flag2:value2,....}}
	schedulePolicyArgs := make(map[string]map[string]string)
	schedulePolicyArgs["migrate-every-5m"] = map[string]string{"policy-type": "Interval", "interval-minutes": "5"}

	// migrationScheduleArgs is a map of migrationScheduleName : {{flag1:value1,flag2:value2,....}}
	migrationScheduleArgs := make(map[string]map[string]string)
	migrationScheduleArgs[instanceID] = map[string]string{
		"purge-deleted-resources": "",
		"schedule-policy-name":    "migrate-every-5m",
	}

	//Create schedulePolicies using storkCtl if any required
	factory := storkctl.NewFactory()
	var outputBuffer bytes.Buffer
	cmd := storkctl.NewCommand(factory, os.Stdin, &outputBuffer, os.Stderr)
	for schedulePolicyName, customArgs := range schedulePolicyArgs {
		cmdArgs := []string{"create", "schedulepolicy", schedulePolicyName}
		executeStorkCtlCommand(t, cmd, cmdArgs, customArgs)
	}
	//Create migrationSchedules using storkCtl
	migrationScheduleName := "forward-migration-schedule-subset-ns"
	namespacesValue := fmt.Sprintf("%s,%s", mysqlNamespace, elasticsearchNamespace)
	cmdArgs := []string{"create", "migrationschedule", migrationScheduleName, "-c", remotePairName,
		"--namespaces", namespacesValue, "-n", defaultAdminNamespace}
	executeStorkCtlCommand(t, cmd, cmdArgs, migrationScheduleArgs[instanceID])

	// bump time of the world by 6 minutes
	mockNow := time.Now().Add(6 * time.Minute)
	err = setMockTime(&mockNow)
	log.FailOnError(t, err, "Error setting mock time")

	// Need to Validate the migrationSchedules separately because they are created using storkctl
	// and not a part of the torpedo scheduler context
	_, err = storkops.Instance().ValidateMigrationSchedule(migrationScheduleName, defaultAdminNamespace, defaultWaitTimeout, defaultWaitInterval)

	// Failover the application
	err = setDestinationKubeConfig()
	log.FailOnError(t, err, "failed to set kubeconfig to destination cluster: %v", err)

	failoverCmdArgs := map[string]string{
		"migration-reference": migrationScheduleName,
		"include-namespaces":  elasticsearchNamespace,
		"namespace":           defaultAdminNamespace,
	}
	drActionName, _ := createDRAction(t, defaultAdminNamespace, storkv1.ActionTypeFailover, migrationScheduleName, failoverCmdArgs)

	// Wait for failover action to complete
	waitTillActionComplete(t, storkv1.ActionTypeFailover, drActionName, defaultAdminNamespace)

	// Verify that only elasticsearch application is running on the destination cluster
	destDeployments, err := apps.Instance().ListDeployments(mysqlNamespace, metav1.ListOptions{})
	log.FailOnError(t, err, "error retrieving deployments from %s namespace", mysqlNamespace)
	Dash.VerifyFatal(t, len(destDeployments.Items), 1, fmt.Sprintf("Expected 1 deployment in destination in %s namespace", mysqlNamespace))
	Dash.VerifyFatal(t, *destDeployments.Items[0].Spec.Replicas, 0, fmt.Sprintf("Expected %d replica in destination in %s namespace", sourceDeploymentReplicas, mysqlNamespace))

	destStatefulsets, err := apps.Instance().ListStatefulSets(elasticsearchNamespace, metav1.ListOptions{})
	log.FailOnError(t, err, "error retrieving statefulsets from %s namespace", elasticsearchNamespace)
	Dash.VerifyFatal(t, len(destStatefulsets.Items), 1, fmt.Sprintf("Expected 1 statefulset in destination in %s namespace", elasticsearchNamespace))
	Dash.VerifyFatal(t, *destStatefulsets.Items[0].Spec.Replicas, sourceStatefulsetReplicas, fmt.Sprintf("Expected %d replica in destination in %s namespace", sourceStatefulsetReplicas, elasticsearchNamespace))

	// Verify that mysql application is running but not elasticsearch on the source cluster
	err = setSourceKubeConfig()
	log.FailOnError(t, err, "failed to set kubeconfig to source cluster: %v", err)
	sourceDeployments, err = apps.Instance().ListDeployments(mysqlNamespace, metav1.ListOptions{})
	log.FailOnError(t, err, "error retrieving deployments from %s namespace", mysqlNamespace)
	Dash.VerifyFatal(t, len(sourceDeployments.Items), 1, fmt.Sprintf("Expected 1 deployment in source in %s namespace", mysqlNamespace))
	Dash.VerifyFatal(t, *sourceDeployments.Items[0].Spec.Replicas, 1, fmt.Sprintf("Expected 0 replica in source in deployment in %s namespace", mysqlNamespace))

	sourceStatefulsets, err = apps.Instance().ListStatefulSets(elasticsearchNamespace, metav1.ListOptions{})
	log.FailOnError(t, err, "error retrieving statefulsets from %s namespace", elasticsearchNamespace)
	Dash.VerifyFatal(t, len(sourceStatefulsets.Items), 1, fmt.Sprintf("Expected 1 statefulset in source in %s namespace", elasticsearchNamespace))
	Dash.VerifyFatal(t, *sourceStatefulsets.Items[0].Spec.Replicas, 0, fmt.Sprintf("Expected 0 replica in source in sts in %s namespace", elasticsearchNamespace))

	// Failover #2 mysql ns
	err = setDestinationKubeConfig()
	log.FailOnError(t, err, "failed to set kubeconfig to destination cluster: %v", err)

	failoverCmdArgs = map[string]string{
		"migration-reference": migrationScheduleName,
		"include-namespaces":  mysqlNamespace,
		"namespace":           defaultAdminNamespace,
	}

	drActionName, _ = createDRAction(t, defaultAdminNamespace, storkv1.ActionTypeFailover, migrationScheduleName, failoverCmdArgs)

	// Wait for failover action to complete
	waitTillActionComplete(t, storkv1.ActionTypeFailover, drActionName, defaultAdminNamespace)

	// Verify that both mysql and elasticsearch applications are running on the destination cluster
	destDeployments, err = apps.Instance().ListDeployments(mysqlNamespace, metav1.ListOptions{})
	log.FailOnError(t, err, "error retrieving deployments from %s namespace", mysqlNamespace)
	Dash.VerifyFatal(t, len(destDeployments.Items), 1, fmt.Sprintf("Expected 1 deployment in destination in %s namespace", mysqlNamespace))
	Dash.VerifyFatal(t, *destDeployments.Items[0].Spec.Replicas, sourceDeploymentReplicas, fmt.Sprintf("Expected %d replica in destination in %s namespace", sourceDeploymentReplicas, mysqlNamespace))

	destStatefulsets, err = apps.Instance().ListStatefulSets(elasticsearchNamespace, metav1.ListOptions{})
	log.FailOnError(t, err, "error retrieving statefulsets from %s namespace", elasticsearchNamespace)
	Dash.VerifyFatal(t, len(destStatefulsets.Items), 1, fmt.Sprintf("Expected 1 statefulset in destination in %s namespace", elasticsearchNamespace))
	Dash.VerifyFatal(t, *destStatefulsets.Items[0].Spec.Replicas, sourceStatefulsetReplicas, fmt.Sprintf("Expected %d replica in destination in %s namespace", sourceStatefulsetReplicas, elasticsearchNamespace))

	err = storkops.Instance().DeleteClusterPair(remotePairName, defaultAdminNamespace)
	log.FailOnError(t, err, "failed to delete clusterpair %s in namespace %s in destination: %v", remotePairName, defaultAdminNamespace, err)
	DeleteAndWaitForMigrationScheduleDeletion(t, migrationScheduleName, defaultAdminNamespace)

	// Verify that both mysql and elasticsearch applications are not running on the source cluster
	err = setSourceKubeConfig()
	log.FailOnError(t, err, "failed to set kubeconfig to source cluster: %v", err)

	sourceDeployments, err = apps.Instance().ListDeployments(mysqlNamespace, metav1.ListOptions{})
	log.FailOnError(t, err, "error retrieving deployments from %s namespace", mysqlNamespace)
	Dash.VerifyFatal(t, len(sourceDeployments.Items), 1, fmt.Sprintf("Expected 1 deployment in source in %s namespace", mysqlNamespace))
	Dash.VerifyFatal(t, *sourceDeployments.Items[0].Spec.Replicas, 0, fmt.Sprintf("Expected 0 replica in source in deployment in %s namespace", mysqlNamespace))

	sourceStatefulsets, err = apps.Instance().ListStatefulSets(elasticsearchNamespace, metav1.ListOptions{})
	log.FailOnError(t, err, "error retrieving statefulsets from %s namespace", elasticsearchNamespace)
	Dash.VerifyFatal(t, len(sourceStatefulsets.Items), 1, fmt.Sprintf("Expected 1 statefulset in source in %s namespace", elasticsearchNamespace))
	Dash.VerifyFatal(t, *sourceStatefulsets.Items[0].Spec.Replicas, 0, fmt.Sprintf("Expected 0 replica in source in sts in %s namespace", elasticsearchNamespace))

	DeleteAndWaitForMigrationScheduleDeletion(t, migrationScheduleName, defaultAdminNamespace)

	for schedulePolicyName := range schedulePolicyArgs {
		cmdArgs := []string{"delete", "schedulepolicy", schedulePolicyName}
		executeStorkCtlCommand(t, cmd, cmdArgs, nil)
	}
	err = storkops.Instance().DeleteClusterPair(remotePairName, defaultAdminNamespace)
	log.FailOnError(t, err, "failed to delete clusterpair %s in namespace %s in source: %v", remotePairName, defaultAdminNamespace, err)

	// cleanup
	destroyAndWait(t, preMigrationCtxs)
	for _, ns := range []string{mysqlNamespace, elasticsearchNamespace} {
		blowNamespaceForTest(t, ns, false)
	}
	// If we are here then the test has passed
	testResult = testResultPass
	log.InfoD("Test status at end of %s test: %s", t.Name(), testResult)
}

// testDRActionFailoverWithMigrationRunningTest tests failover action when migration is running
func testDRActionFailoverWithMigrationRunningTest(t *testing.T) {
	var testrailID, testResult = 296306, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	defer updateDashStats(t.Name(), &testResult)

	// Create the apps for migration
	// Start migrationschedule
	instanceID := "migration-running"
	// appKeys is an array of appKeys that will be used to create the apps
	mysqlApp := "mysql-1-pvc"
	mysqlNamespace := fmt.Sprintf("%s-%s", mysqlApp, instanceID)
	var err error
	// Reset config in case of error
	defer func() {
		err = setSourceKubeConfig()
		log.FailOnError(t, err, "Error resetting remote config")
	}()

	ctxs, err := schedulerDriver.Schedule(instanceID,
		scheduler.ScheduleOptions{
			AppKeys: []string{mysqlApp},
			Labels:  nil,
		})
	log.FailOnError(t, err, "Error scheduling task")
	Dash.VerifyFatal(t, 1, len(ctxs), "Only one task should have started")

	err = schedulerDriver.WaitForRunning(ctxs[0], defaultWaitTimeout, defaultWaitInterval)
	log.FailOnError(t, err, "Error waiting for app to get to running state")

	sourceDeployments, err := apps.Instance().ListDeployments(mysqlNamespace, metav1.ListOptions{})
	log.FailOnError(t, err, "error retrieving deployments from %s namespace", mysqlNamespace)
	Dash.VerifyFatal(t, len(sourceDeployments.Items), 1, fmt.Sprintf("Expected 1 deployment in source in %s namespace", mysqlNamespace))
	sourceDeploymentReplicas := *sourceDeployments.Items[0].Spec.Replicas

	// Create the clusterpair
	clusterPairNamespace := mysqlNamespace
	log.Info("Creating bidirectional cluster pair:")
	log.InfoD("Name: %s", remotePairName)
	log.InfoD("Namespace: %s", clusterPairNamespace)
	log.InfoD("Backuplocation: %s", defaultBackupLocation)
	log.InfoD("Secret name: %s", defaultSecretName)
	err = scheduleBidirectionalClusterPair(remotePairName, clusterPairNamespace, projectIDMappings, defaultBackupLocation, defaultSecretName, false)
	log.FailOnError(t, err, "failed to set bidirectional cluster pair: %v", err)
	err = setSourceKubeConfig()
	log.FailOnError(t, err, "failed to set kubeconfig to source cluster: %v", err)

	// Create the migration schedule
	// we want to create the schedulePolicy and migrationSchedule using storkctl instead of scheduling apps using torpedo's scheduler
	// schedulePolicyArgs is a map of schedulePolicyName : {{flag1:value1,flag2:value2,....}}
	schedulePolicyArgs := make(map[string]map[string]string)
	schedulePolicyArgs["migrate-every-5m"] = map[string]string{"policy-type": "Interval", "interval-minutes": "5"}

	// migrationScheduleArgs is a map of migrationScheduleName : {{flag1:value1,flag2:value2,....}}
	migrationScheduleArgs := make(map[string]map[string]string)
	migrationScheduleArgs[instanceID] = map[string]string{
		"purge-deleted-resources": "",
		"schedule-policy-name":    "migrate-every-5m",
	}

	//Create schedulePolicies using storkCtl if any required
	factory := storkctl.NewFactory()
	var outputBuffer bytes.Buffer
	cmd := storkctl.NewCommand(factory, os.Stdin, &outputBuffer, os.Stderr)
	for schedulePolicyName, customArgs := range schedulePolicyArgs {
		cmdArgs := []string{"create", "schedulepolicy", schedulePolicyName}
		executeStorkCtlCommand(t, cmd, cmdArgs, customArgs)
	}
	//Create migrationSchedules using storkCtl
	migrationScheduleName := "forward-migration-schedule-running-migration"
	cmdArgs := []string{"create", "migrationschedule", migrationScheduleName, "-c", remotePairName,
		"--namespaces", mysqlNamespace, "-n", mysqlNamespace}
	executeStorkCtlCommand(t, cmd, cmdArgs, migrationScheduleArgs[instanceID])

	// bump time of the world by 1 minutes
	mockNow := time.Now().Add(1 * time.Minute)
	err = setMockTime(&mockNow)
	log.FailOnError(t, err, "Error setting mock time")
	time.Sleep(30 * time.Second)

	// Failover the application
	err = setDestinationKubeConfig()
	log.FailOnError(t, err, "failed to set kubeconfig to destination cluster: %v", err)

	failoverCmdArgs := map[string]string{
		"migration-reference": migrationScheduleName,
		"include-namespaces":  mysqlNamespace,
		"namespace":           mysqlNamespace,
	}
	drActionName, _ := createDRAction(t, mysqlNamespace, storkv1.ActionTypeFailover, migrationScheduleName, failoverCmdArgs)

	// Wait for failover action to complete
	waitTillActionComplete(t, storkv1.ActionTypeFailover, drActionName, mysqlNamespace)

	// Verify the application is running on the destination cluster
	destDeployments, err := apps.Instance().ListDeployments(mysqlNamespace, metav1.ListOptions{})
	log.FailOnError(t, err, "error retrieving deployments from %s namespace", mysqlNamespace)
	Dash.VerifyFatal(t, len(destDeployments.Items), 1, fmt.Sprintf("Expected 1 deployment in destination in %s namespace", mysqlNamespace))
	Dash.VerifyFatal(t, *destDeployments.Items[0].Spec.Replicas, sourceDeploymentReplicas, fmt.Sprintf("Expected %d replica in destination in %s namespace", sourceDeploymentReplicas, mysqlNamespace))

	err = storkops.Instance().DeleteClusterPair(remotePairName, mysqlNamespace)
	log.FailOnError(t, err, "failed to delete clusterpair %s in namespace %s in destination: %v", remotePairName, mysqlNamespace, err)

	// Verify the application is not running on the source cluster
	err = setSourceKubeConfig()
	log.FailOnError(t, err, "failed to set kubeconfig to source cluster: %v", err)
	sourceDeployments, err = apps.Instance().ListDeployments(mysqlNamespace, metav1.ListOptions{})
	log.FailOnError(t, err, "error retrieving deployments from %s namespace", mysqlNamespace)
	Dash.VerifyFatal(t, len(sourceDeployments.Items), 1, fmt.Sprintf("Expected 1 deployment in source in %s namespace", mysqlNamespace))
	Dash.VerifyFatal(t, *sourceDeployments.Items[0].Spec.Replicas, 0, fmt.Sprintf("Expected 0 replica in source in deployment in %s namespace", mysqlNamespace))

	DeleteAndWaitForMigrationScheduleDeletion(t, migrationScheduleName, mysqlNamespace)

	for schedulePolicyName := range schedulePolicyArgs {
		cmdArgs := []string{"delete", "schedulepolicy", schedulePolicyName}
		executeStorkCtlCommand(t, cmd, cmdArgs, nil)
	}

	err = storkops.Instance().DeleteClusterPair(remotePairName, mysqlNamespace)
	log.FailOnError(t, err, "failed to delete clusterpair %s in namespace %s in source: %v", remotePairName, mysqlNamespace, err)

	// cleanup
	destroyAndWait(t, ctxs)
	blowNamespaceForTest(t, mysqlNamespace, false)
	// If we are here then the test has passed
	testResult = testResultPass
	log.InfoD("Test status at end of %s test: %s", t.Name(), testResult)
}

func testDRActionFailbackIntervalScheduleTest(t *testing.T) {
	var testrailID, testResult = 296303, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	defer updateDashStats(t.Name(), &testResult)

	// Create the apps for migration
	// Start migrationschedule
	instanceID := "failback-interval"
	// appKeys is an array of appKeys that will be used to create the apps
	mysqlApp := "mysql-1-pvc"
	mysqlNamespace := fmt.Sprintf("%s-%s", mysqlApp, instanceID)
	var err error
	// Reset config in case of error
	defer func() {
		err = setSourceKubeConfig()
		log.FailOnError(t, err, "Error resetting remote config")
	}()

	schedulePolicyDestArgs := make(map[string]map[string]string)
	schedulePolicyDestArgs["migrate-interval-dr"] = map[string]string{"policy-type": "Interval"}
	failBackWithMigrationSchedulesWithDifferentPolicies(t, instanceID, mysqlApp, mysqlNamespace, schedulePolicyDestArgs)

	// If we are here then the test has passed
	testResult = testResultPass
	log.InfoD("Test status at end of %s test: %s", t.Name(), testResult)
}

func testDRActionFailbackDailyScheduleTest(t *testing.T) {
	var testrailID, testResult = 296300, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	defer updateDashStats(t.Name(), &testResult)

	// Create the apps for migration
	// Start migrationschedule
	instanceID := "failback-interval"
	// appKeys is an array of appKeys that will be used to create the apps
	mysqlApp := "mysql-1-pvc"
	mysqlNamespace := fmt.Sprintf("%s-%s", mysqlApp, instanceID)
	var err error
	// Reset config in case of error
	defer func() {
		err = setSourceKubeConfig()
		log.FailOnError(t, err, "Error resetting remote config")
	}()

	schedulePolicyDestArgs := make(map[string]map[string]string)
	schedulePolicyDestArgs["migrate-every-day-dr"] = map[string]string{"policy-type": "Daily"}
	failBackWithMigrationSchedulesWithDifferentPolicies(t, instanceID, mysqlApp, mysqlNamespace, schedulePolicyDestArgs)

	// If we are here then the test has passed
	testResult = testResultPass
	log.InfoD("Test status at end of %s test: %s", t.Name(), testResult)
}

func testDRActionFailbackWeeklyScheduleTest(t *testing.T) {
	var testrailID, testResult = 296301, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	defer updateDashStats(t.Name(), &testResult)

	// Create the apps for migration
	// Start migrationschedule
	instanceID := "failback-interval"
	// appKeys is an array of appKeys that will be used to create the apps
	mysqlApp := "mysql-1-pvc"
	mysqlNamespace := fmt.Sprintf("%s-%s", mysqlApp, instanceID)
	var err error
	// Reset config in case of error
	defer func() {
		err = setSourceKubeConfig()
		log.FailOnError(t, err, "Error resetting remote config")
	}()

	schedulePolicyDestArgs := make(map[string]map[string]string)
	schedulePolicyDestArgs["migrate-every-week-dr"] = map[string]string{"policy-type": "Weekly"}
	failBackWithMigrationSchedulesWithDifferentPolicies(t, instanceID, mysqlApp, mysqlNamespace, schedulePolicyDestArgs)

	// If we are here then the test has passed
	testResult = testResultPass
	log.InfoD("Test status at end of %s test: %s", t.Name(), testResult)
}

func testDRActionFailbackMonthlyScheduleTest(t *testing.T) {
	var testrailID, testResult = 296302, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	defer updateDashStats(t.Name(), &testResult)

	// Create the apps for migration
	instanceID := "failback-interval"
	// appKeys is an array of appKeys that will be used to create the apps
	mysqlApp := "mysql-1-pvc"
	mysqlNamespace := fmt.Sprintf("%s-%s", mysqlApp, instanceID)
	var err error
	// Reset config in case of error
	defer func() {
		err = setSourceKubeConfig()
		log.FailOnError(t, err, "Error resetting remote config")
	}()

	schedulePolicyDestArgs := make(map[string]map[string]string)
	schedulePolicyDestArgs["migrate-every-month-dr"] = map[string]string{"policy-type": "Monthly"}
	failBackWithMigrationSchedulesWithDifferentPolicies(t, instanceID, mysqlApp, mysqlNamespace, schedulePolicyDestArgs)

	// If we are here then the test has passed
	testResult = testResultPass
	log.InfoD("Test status at end of %s test: %s", t.Name(), testResult)
}

// testDRActionFailbackModifiedSecondaryReplicasTest tests performing failback of an app whose number of replicas have been modified in secondary after failover.
func testDRActionFailbackModifiedSecondaryReplicasTest(t *testing.T) {
	var testrailID, testResult = 257160, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	defer updateDashStats(t.Name(), &testResult)

	// Create the apps for migration.
	// Start migrationschedule
	instanceID := "failback-modified-secondary-replicas"
	elasticsearchApp := "elasticsearch"
	elasticsearchNamespace := fmt.Sprintf("%s-%s", elasticsearchApp, instanceID)
	appKeys := []string{elasticsearchApp}
	var err error
	// Reset config in case of error.
	defer func() {
		err = setSourceKubeConfig()
		log.FailOnError(t, err, "Error resetting remote config")
	}()

	// Schedule elasticsearch replicas.
	var preMigrationCtxs []*scheduler.Context
	for _, appKey := range appKeys {
		ctxs, err := schedulerDriver.Schedule(instanceID,
			scheduler.ScheduleOptions{
				AppKeys: []string{appKey},
				Labels:  nil,
			})
		log.FailOnError(t, err, "Error scheduling task")
		Dash.VerifyFatal(t, 1, len(ctxs), "Only one task should have started")
		preMigrationCtxs = append(preMigrationCtxs, ctxs[0].DeepCopy())
	}

	for _, preMigrationCtx := range preMigrationCtxs {
		err = schedulerDriver.WaitForRunning(preMigrationCtx, defaultWaitTimeout, defaultWaitInterval)
		log.FailOnError(t, err, "Error waiting for app to get to running state")
	}

	sourceStatefulsets, err := apps.Instance().ListStatefulSets(elasticsearchNamespace, metav1.ListOptions{})
	log.FailOnError(t, err, "error retrieving statefulsets from %s namespace", elasticsearchNamespace)
	Dash.VerifyFatal(t, len(sourceStatefulsets.Items), 1, fmt.Sprintf("Expected 1 statefulset in source in %s namespace", elasticsearchNamespace))
	sourceStatefulsetReplicas := *sourceStatefulsets.Items[0].Spec.Replicas

	// Create the clusterpair
	clusterPairNamespace := defaultAdminNamespace
	log.Info("Creating bidirectional cluster pair:")
	log.InfoD("Name: %s", remotePairName)
	log.InfoD("Namespace: %s", clusterPairNamespace)
	log.InfoD("Backuplocation: %s", defaultBackupLocation)
	log.InfoD("Secret name: %s", defaultSecretName)
	err = scheduleBidirectionalClusterPair(remotePairName, clusterPairNamespace, projectIDMappings, defaultBackupLocation, defaultSecretName, false)
	log.FailOnError(t, err, "failed to set bidirectional cluster pair: %v", err)
	err = setSourceKubeConfig()
	log.FailOnError(t, err, "failed to set kubeconfig to source cluster: %v", err)

	// Create the migration schedule.
	// We want to create the schedulePolicy and migrationSchedule using storkctl instead of scheduling apps using torpedo's scheduler.
	// schedulePolicyArgs is a map of schedulePolicyName : {{flag1:value1,flag2:value2,....}}.
	schedulePolicyArgs := make(map[string]map[string]string)
	schedulePolicyArgs["migrate-every-5m"] = map[string]string{"policy-type": "Interval", "interval-minutes": "5"}

	// migrationScheduleArgs is a map of migrationScheduleName : {{flag1:value1,flag2:value2,....}}
	migrationScheduleArgs := make(map[string]map[string]string)
	migrationScheduleArgs[instanceID] = map[string]string{
		"purge-deleted-resources": "",
		"schedule-policy-name":    "migrate-every-5m",
	}

	// Create schedulePolicies using storkCtl if any required.
	factory := storkctl.NewFactory()
	var outputBuffer bytes.Buffer
	cmd := storkctl.NewCommand(factory, os.Stdin, &outputBuffer, os.Stderr)
	for schedulePolicyName, customArgs := range schedulePolicyArgs {
		cmdArgs := []string{"create", "schedulepolicy", schedulePolicyName}
		executeStorkCtlCommand(t, cmd, cmdArgs, customArgs)
	}
	// Create migrationSchedules using storkCtl.
	migrationScheduleName := "failback-modified-secondary-replicas"
	cmdArgs := []string{"create", "migrationschedule", migrationScheduleName, "-c", remotePairName,
		"--namespaces", elasticsearchNamespace, "-n", defaultAdminNamespace}
	executeStorkCtlCommand(t, cmd, cmdArgs, migrationScheduleArgs[instanceID])

	// Bump time of the world by 6 minutes.
	mockNow := time.Now().Add(6 * time.Minute)
	err = setMockTime(&mockNow)
	log.FailOnError(t, err, "Error setting mock time")

	// Need to validate the migrationSchedules separately because they are created using storkctl
	// and not a part of the torpedo scheduler context.
	_, err = storkops.Instance().ValidateMigrationSchedule(migrationScheduleName, defaultAdminNamespace, defaultWaitTimeout, defaultWaitInterval)
	err = setMockTime(nil)
	log.FailOnError(t, err, "Error resetting mock time")

	// Failover the application.
	err = setDestinationKubeConfig()
	log.FailOnError(t, err, "failed to set kubeconfig to destination cluster: %v", err)

	failoverCmdArgs := map[string]string{
		"migration-reference": migrationScheduleName,
		"include-namespaces":  elasticsearchNamespace,
		"namespace":           defaultAdminNamespace,
	}
	drActionName, _ := createDRAction(t, defaultAdminNamespace, storkv1.ActionTypeFailover, migrationScheduleName, failoverCmdArgs)

	// Wait for failover action to complete.
	waitTillActionComplete(t, storkv1.ActionTypeFailover, drActionName, defaultAdminNamespace)

	// Verify the elasticsearch application is running on the destination cluster.
	destStatefulsets, err := apps.Instance().ListStatefulSets(elasticsearchNamespace, metav1.ListOptions{})
	log.FailOnError(t, err, "error retrieving statefulsets from %s namespace", elasticsearchNamespace)
	Dash.VerifyFatal(t, len(destStatefulsets.Items), 1, fmt.Sprintf("Expected 1 statefulset in destination in %s namespace", elasticsearchNamespace))
	Dash.VerifyFatal(t, *destStatefulsets.Items[0].Spec.Replicas, sourceStatefulsetReplicas, fmt.Sprintf("Expected %d replica in destination in %s namespace", sourceStatefulsetReplicas, elasticsearchNamespace))

	// Verify the elasticsearch application is not running on the source cluster.
	err = setSourceKubeConfig()
	log.FailOnError(t, err, "failed to set kubeconfig to source cluster: %v", err)
	sourceStatefulsets, err = apps.Instance().ListStatefulSets(elasticsearchNamespace, metav1.ListOptions{})
	log.FailOnError(t, err, "error retrieving statefulsets from %s namespace", elasticsearchNamespace)
	Dash.VerifyFatal(t, len(sourceStatefulsets.Items), 1, fmt.Sprintf("Expected 1 statefulset in source in %s namespace", elasticsearchNamespace))
	Dash.VerifyFatal(t, *sourceStatefulsets.Items[0].Spec.Replicas, 0, fmt.Sprintf("Expected 0 replica in source in sts in %s namespace", elasticsearchNamespace))

	///////////////////////////////////////////////////////////////////////////////////////
	// Scale the number of replicas of elasticsearch in destination cluster from 3 to 5 //
	//////////////////////////////////////////////////////////////////////////////////////

	err = setDestinationKubeConfig()
	log.FailOnError(t, err, "failed to set kubeconfig to destination cluster: %v", err)

	scaleMap, err := schedulerDriver.GetScaleFactorMap(preMigrationCtxs[0])
	log.FailOnError(t, err, "Error getting scale map")
	newScaleMap := make(map[string]int32, len(scaleMap))
	for name := range scaleMap {
		newScaleMap[name] = 5
	}
	err = schedulerDriver.ScaleApplication(preMigrationCtxs[0], newScaleMap)
	log.FailOnError(t, err, "Error while scaling up app")

	// Wait for new replicas to come up.
	err = schedulerDriver.WaitForRunning(preMigrationCtxs[0], defaultWaitTimeout, defaultWaitInterval)
	log.FailOnError(t, err, "Error waiting for app to get to running state")

	//////////////////////////////////////////////////////
	// Create reverse migrationschedule using storkCtl //
	////////////////////////////////////////////////////
	err = setDestinationKubeConfig()
	log.FailOnError(t, err, "failed to set kubeconfig to destination cluster: %v", err)

	// Initialize reversemigrationschedule and reverse schedule policy.
	reverseSchedulePolicyArgs := make(map[string]map[string]string)
	reverseSchedulePolicyArgs["reverse-migrate-every-5m"] = map[string]string{"policy-type": "Interval", "interval-minutes": "5"}
	reverseMigrationScheduleArgs := make(map[string]map[string]string)
	reverseMigrationScheduleArgs[instanceID] = map[string]string{
		"purge-deleted-resources": "",
		"schedule-policy-name":    "reverse-migrate-every-5m",
	}

	//Create schedulePolicies using storkCtl.
	cmd = storkctl.NewCommand(factory, os.Stdin, &outputBuffer, os.Stderr)
	for schedulePolicyName, customArgs := range reverseSchedulePolicyArgs {
		cmdArgs := []string{"create", "schedulepolicy", schedulePolicyName, "--kubeconfig", destinationKubeConfigPath}
		executeStorkCtlCommand(t, cmd, cmdArgs, customArgs)
	}
	log.Info("created schedule policy")

	// Create migration schedule.
	reverseMigrationScheduleName := "reverse-failback-modified-secondary-replicas"
	cmdArgs = []string{"create", "migrationschedule", reverseMigrationScheduleName, "-c", remotePairName,
		"--namespaces", elasticsearchNamespace, "-n", defaultAdminNamespace, "--kubeconfig", destinationKubeConfigPath}
	executeStorkCtlCommand(t, cmd, cmdArgs, reverseMigrationScheduleArgs[instanceID])

	// Bump time of the world by 6 minutes.
	mockNow = time.Now().Add(6 * time.Minute)
	err = setMockTime(&mockNow)
	log.FailOnError(t, err, "Error setting mock time")

	// Need to Validate the migrationSchedules separately because they are created using storkctl
	// and not a part of the torpedo scheduler context.
	_, err = storkops.Instance().ValidateMigrationSchedule(reverseMigrationScheduleName, defaultAdminNamespace, defaultWaitTimeout, defaultWaitInterval)

	log.Info("migration schedule created")
	failbackCmdArgs := map[string]string{
		"migration-reference": reverseMigrationScheduleName,
		"include-namespaces":  elasticsearchNamespace,
		"namespace":           defaultAdminNamespace,
	}
	drActionName, _ = createDRAction(t, defaultAdminNamespace, storkv1.ActionTypeFailback, reverseMigrationScheduleName, failbackCmdArgs)

	// Wait for failback action to complete.
	waitTillActionComplete(t, storkv1.ActionTypeFailback, drActionName, defaultAdminNamespace)
	err = setMockTime(nil)
	log.FailOnError(t, err, "Error resetting mock time")

	////////////////
	// Assertion //
	///////////////

	// Verify the elasticsearch application is not running on the destination cluster.
	destStatefulsets, err = apps.Instance().ListStatefulSets(elasticsearchNamespace, metav1.ListOptions{})
	log.FailOnError(t, err, "error retrieving statefulsets from %s namespace", elasticsearchNamespace)
	Dash.VerifyFatal(t, len(destStatefulsets.Items), 1, fmt.Sprintf("Expected 1 statefulset in destination in %s namespace", elasticsearchNamespace))
	Dash.VerifyFatal(t, *destStatefulsets.Items[0].Spec.Replicas, 0, fmt.Sprintf("Expected 0 replica in destination in sts in %s namespace", elasticsearchNamespace))

	// Verify the elasticsearch application is running on the source cluster.
	err = setSourceKubeConfig()
	log.FailOnError(t, err, "failed to set kubeconfig to source cluster: %v", err)
	expectedReplicas := int32(5)
	sourceStatefulsets, err = apps.Instance().ListStatefulSets(elasticsearchNamespace, metav1.ListOptions{})
	log.FailOnError(t, err, "error retrieving statefulsets from %s namespace", elasticsearchNamespace)
	Dash.VerifyFatal(t, len(sourceStatefulsets.Items), 1, fmt.Sprintf("Expected 1 statefulset in source in %s namespace", elasticsearchNamespace))
	Dash.VerifyFatal(t, *sourceStatefulsets.Items[0].Spec.Replicas, expectedReplicas, fmt.Sprintf("Expected %d replica in source in %s namespace", sourceStatefulsetReplicas, elasticsearchNamespace))

	// Verify that the migration schedule in destination cluster has been suspended.
	err = setDestinationKubeConfig()
	log.FailOnError(t, err, "failed to set kubeconfig to destination cluster: %v", err)
	migrSched, err := storkops.Instance().GetMigrationSchedule(reverseMigrationScheduleName, defaultAdminNamespace)
	log.FailOnError(t, err, "failed to retrieve migration schedule: %v", err)

	if migrSched.Spec.Suspend != nil && !(*migrSched.Spec.Suspend) {
		// Fail the test as the migration schedule is not yet suspended.
		suspendErr := fmt.Errorf("migrationschedule is not suspended on destination cluster : %s/%s", migrSched.GetName(), migrSched.GetNamespace())
		log.FailOnError(t, err, "Failed: %v", suspendErr)
	}
	log.InfoD("Successfully verified suspend migration in destination cluster")

	//////////////
	// Cleanup //
	/////////////

	// Delete destination cluster cluster-pair and migration schedule.
	err = storkops.Instance().DeleteClusterPair(remotePairName, defaultAdminNamespace)
	log.FailOnError(t, err, "failed to delete clusterpair %s in namespace %s in destination: %v", remotePairName, defaultAdminNamespace, err)
	DeleteAndWaitForMigrationScheduleDeletion(t, migrationScheduleName, defaultAdminNamespace)
	DeleteAndWaitForMigrationScheduleDeletion(t, reverseMigrationScheduleName, defaultAdminNamespace)
	for schedulePolicyName := range reverseSchedulePolicyArgs {
		err := storkops.Instance().DeleteSchedulePolicy(schedulePolicyName)
		log.FailOnError(t, err, "error deleting up schedule policy %s", schedulePolicyName)
	}
	// cleanup
	destroyAndWait(t, preMigrationCtxs)

	// Delete source cluster cluster-pair and migration schedule.
	err = setSourceKubeConfig()
	log.FailOnError(t, err, "failed to set kubeconfig to source cluster: %v", err)
	err = storkops.Instance().DeleteClusterPair(remotePairName, defaultAdminNamespace)
	log.FailOnError(t, err, "failed to delete clusterpair %s in namespace %s in source: %v", remotePairName, defaultAdminNamespace, err)
	DeleteAndWaitForMigrationScheduleDeletion(t, migrationScheduleName, defaultAdminNamespace)
	DeleteAndWaitForMigrationScheduleDeletion(t, reverseMigrationScheduleName, defaultAdminNamespace)
	for schedulePolicyName := range schedulePolicyArgs {
		err := storkops.Instance().DeleteSchedulePolicy(schedulePolicyName)
		log.FailOnError(t, err, "error deleting up schedule policy %s", schedulePolicyName)
	}
	// cleanup
	destroyAndWait(t, preMigrationCtxs)
	blowNamespaceForTest(t, elasticsearchNamespace, false)

	// If we are here then the test has passed.
	testResult = testResultPass
	log.InfoD("Test status at end of %s test: %s", t.Name(), testResult)
}

func testDRActionFailbackSubsetNamespacesTest(t *testing.T) {
	var testrailID, testResult = 297513, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	defer updateDashStats(t.Name(), &testResult)

	// Create the apps for migration
	// Start migrationschedule
	instanceID := "subset-ns-migration-schedule-interval"
	// appKeys is an array of appKeys that will be used to create the apps
	mysqlApp := "mysql-1-pvc"
	mysqlNamespace := fmt.Sprintf("%s-%s", mysqlApp, instanceID)
	elasticsearchApp := "elasticsearch"
	elasticsearchNamespace := fmt.Sprintf("%s-%s", elasticsearchApp, instanceID)
	appKeys := []string{mysqlApp, elasticsearchApp}
	var err error
	// Reset config in case of error
	defer func() {
		err = setSourceKubeConfig()
		log.FailOnError(t, err, "Error resetting remote config")
	}()

	// Schedule multiple apps
	var preMigrationCtxs []*scheduler.Context
	for _, appKey := range appKeys {
		ctxs, err := schedulerDriver.Schedule(instanceID,
			scheduler.ScheduleOptions{
				AppKeys: []string{appKey},
				Labels:  nil,
			})
		log.FailOnError(t, err, "Error scheduling task")
		Dash.VerifyFatal(t, 1, len(ctxs), "Only one task should have started")
		preMigrationCtxs = append(preMigrationCtxs, ctxs[0].DeepCopy())
	}

	for _, preMigrationCtx := range preMigrationCtxs {
		err = schedulerDriver.WaitForRunning(preMigrationCtx, defaultWaitTimeout, defaultWaitInterval)
		log.FailOnError(t, err, "Error waiting for app to get to running state")
	}

	sourceDeployments, err := apps.Instance().ListDeployments(mysqlNamespace, metav1.ListOptions{})
	log.FailOnError(t, err, "error retrieving deployments from %s namespace", mysqlNamespace)
	Dash.VerifyFatal(t, len(sourceDeployments.Items), 1, fmt.Sprintf("Expected 1 deployment in source in %s namespace", mysqlNamespace))
	sourceDeploymentReplicas := *sourceDeployments.Items[0].Spec.Replicas
	sourceStatefulsets, err := apps.Instance().ListStatefulSets(elasticsearchNamespace, metav1.ListOptions{})
	log.FailOnError(t, err, "error retrieving statefulsets from %s namespace", elasticsearchNamespace)
	Dash.VerifyFatal(t, len(sourceStatefulsets.Items), 1, fmt.Sprintf("Expected 1 statefulset in source in %s namespace", elasticsearchNamespace))
	sourceStatefulsetReplicas := *sourceStatefulsets.Items[0].Spec.Replicas

	// Create the clusterpair
	clusterPairNamespace := defaultAdminNamespace
	log.Info("Creating bidirectional cluster pair:")
	log.InfoD("Name: %s", remotePairName)
	log.InfoD("Namespace: %s", clusterPairNamespace)
	log.InfoD("Backuplocation: %s", defaultBackupLocation)
	log.InfoD("Secret name: %s", defaultSecretName)
	err = scheduleBidirectionalClusterPair(remotePairName, clusterPairNamespace, projectIDMappings, defaultBackupLocation, defaultSecretName, false)
	log.FailOnError(t, err, "failed to set bidirectional cluster pair: %v", err)
	err = setSourceKubeConfig()
	log.FailOnError(t, err, "failed to set kubeconfig to source cluster: %v", err)

	// Create the migration schedule
	// we want to create the schedulePolicy and migrationSchedule using storkctl instead of scheduling apps using torpedo's scheduler
	// schedulePolicyArgs is a map of schedulePolicyName : {{flag1:value1,flag2:value2,....}}
	schedulePolicyArgs := make(map[string]map[string]string)
	schedulePolicyArgs["migrate-every-5m"] = map[string]string{"policy-type": "Interval", "interval-minutes": "5"}

	// migrationScheduleArgs is a map of migrationScheduleName : {{flag1:value1,flag2:value2,....}}
	migrationScheduleArgs := make(map[string]map[string]string)
	migrationScheduleArgs[instanceID] = map[string]string{
		"purge-deleted-resources": "",
		"schedule-policy-name":    "migrate-every-5m",
	}

	//Create schedulePolicies using storkCtl if any required
	factory := storkctl.NewFactory()
	var outputBuffer bytes.Buffer
	cmd := storkctl.NewCommand(factory, os.Stdin, &outputBuffer, os.Stderr)
	for schedulePolicyName, customArgs := range schedulePolicyArgs {
		cmdArgs := []string{"create", "schedulepolicy", schedulePolicyName}
		executeStorkCtlCommand(t, cmd, cmdArgs, customArgs)
	}
	//Create migrationSchedules using storkCtl
	migrationScheduleName := "failback-migration-schedule-subset-ns"
	namespacesValue := fmt.Sprintf("%s,%s", mysqlNamespace, elasticsearchNamespace)
	cmdArgs := []string{"create", "migrationschedule", migrationScheduleName, "-c", remotePairName,
		"--namespaces", namespacesValue, "-n", defaultAdminNamespace}
	executeStorkCtlCommand(t, cmd, cmdArgs, migrationScheduleArgs[instanceID])

	// bump time of the world by 6 minutes
	mockNow := time.Now().Add(6 * time.Minute)
	err = setMockTime(&mockNow)
	log.FailOnError(t, err, "Error setting mock time")

	// Need to Validate the migrationSchedules separately because they are created using storkctl
	// and not a part of the torpedo scheduler context
	_, err = storkops.Instance().ValidateMigrationSchedule(migrationScheduleName, defaultAdminNamespace, defaultWaitTimeout, defaultWaitInterval)
	err = setMockTime(nil)
	log.FailOnError(t, err, "Error resetting mock time")

	// Failover the application
	err = setDestinationKubeConfig()
	log.FailOnError(t, err, "failed to set kubeconfig to destination cluster: %v", err)

	failoverCmdArgs := map[string]string{
		"migration-reference": migrationScheduleName,
		"include-namespaces":  namespacesValue,
		"namespace":           defaultAdminNamespace,
	}
	drActionName, _ := createDRAction(t, defaultAdminNamespace, storkv1.ActionTypeFailover, migrationScheduleName, failoverCmdArgs)

	// Wait for failover action to complete
	waitTillActionComplete(t, storkv1.ActionTypeFailover, drActionName, defaultAdminNamespace)

	// Verify that both mysql and elasticsearch applications are RUNNING on the DESTINATION cluster.
	destDeployments, err := apps.Instance().ListDeployments(mysqlNamespace, metav1.ListOptions{})
	log.FailOnError(t, err, "error retrieving deployments from %s namespace", mysqlNamespace)
	Dash.VerifyFatal(t, len(destDeployments.Items), 1, fmt.Sprintf("Expected 1 deployment in destination in %s namespace", mysqlNamespace))
	Dash.VerifyFatal(t, *destDeployments.Items[0].Spec.Replicas, sourceDeploymentReplicas, fmt.Sprintf("Expected %d replica in destination in %s namespace", sourceDeploymentReplicas, mysqlNamespace))

	destStatefulsets, err := apps.Instance().ListStatefulSets(elasticsearchNamespace, metav1.ListOptions{})
	log.FailOnError(t, err, "error retrieving statefulsets from %s namespace", elasticsearchNamespace)
	Dash.VerifyFatal(t, len(destStatefulsets.Items), 1, fmt.Sprintf("Expected 1 statefulset in destination in %s namespace", elasticsearchNamespace))
	Dash.VerifyFatal(t, *destStatefulsets.Items[0].Spec.Replicas, sourceStatefulsetReplicas, fmt.Sprintf("Expected %d replica in destination in %s namespace", sourceStatefulsetReplicas, elasticsearchNamespace))

	// Verify that both mysql and elasticsearch applications are NOT RUNNING on the SOURCE cluster.
	err = setSourceKubeConfig()
	log.FailOnError(t, err, "failed to set kubeconfig to source cluster: %v", err)

	sourceDeployments, err = apps.Instance().ListDeployments(mysqlNamespace, metav1.ListOptions{})
	log.FailOnError(t, err, "error retrieving deployments from %s namespace", mysqlNamespace)
	Dash.VerifyFatal(t, len(sourceDeployments.Items), 1, fmt.Sprintf("Expected 1 deployment in source in %s namespace", mysqlNamespace))
	Dash.VerifyFatal(t, *sourceDeployments.Items[0].Spec.Replicas, 0, fmt.Sprintf("Expected 0 replica in source in deployment in %s namespace", mysqlNamespace))

	sourceStatefulsets, err = apps.Instance().ListStatefulSets(elasticsearchNamespace, metav1.ListOptions{})
	log.FailOnError(t, err, "error retrieving statefulsets from %s namespace", elasticsearchNamespace)
	Dash.VerifyFatal(t, len(sourceStatefulsets.Items), 1, fmt.Sprintf("Expected 1 statefulset in source in %s namespace", elasticsearchNamespace))
	Dash.VerifyFatal(t, *sourceStatefulsets.Items[0].Spec.Replicas, 0, fmt.Sprintf("Expected 0 replica in source in sts in %s namespace", elasticsearchNamespace))

	//////////////////////////////////////////////////////
	// Create reverse migrationschedule using storkCtl //
	////////////////////////////////////////////////////

	err = setDestinationKubeConfig()
	log.FailOnError(t, err, "failed to set kubeconfig to destination cluster: %v", err)

	// Initialize reversemigrationschedule and reverse schedule policy.
	reverseSchedulePolicyArgs := make(map[string]map[string]string)
	reverseSchedulePolicyArgs["reverse-migrate-every-5m"] = map[string]string{"policy-type": "Interval", "interval-minutes": "5"}
	reverseMigrationScheduleArgs := make(map[string]map[string]string)
	reverseMigrationScheduleArgs[instanceID] = map[string]string{
		"purge-deleted-resources": "",
		"schedule-policy-name":    "reverse-migrate-every-5m",
	}

	// Create schedulePolicies using storkCtl.
	cmd = storkctl.NewCommand(factory, os.Stdin, &outputBuffer, os.Stderr)
	for schedulePolicyName, customArgs := range reverseSchedulePolicyArgs {
		cmdArgs := []string{"create", "schedulepolicy", schedulePolicyName, "--kubeconfig", destinationKubeConfigPath}
		executeStorkCtlCommand(t, cmd, cmdArgs, customArgs)
	}
	log.Info("created reverse schedule policy")

	// Create migration schedule.
	reverseMigrationScheduleName := "reverse-failback-migration-schedule-subset-ns"
	cmdArgs = []string{"create", "migrationschedule", reverseMigrationScheduleName, "-c", remotePairName,
		"--namespaces", elasticsearchNamespace, "-n", defaultAdminNamespace, "--kubeconfig", destinationKubeConfigPath}
	executeStorkCtlCommand(t, cmd, cmdArgs, reverseMigrationScheduleArgs[instanceID])

	// Bump time of the world by 6 minutes.
	mockNow = time.Now().Add(6 * time.Minute)
	err = setMockTime(&mockNow)
	log.FailOnError(t, err, "Error setting mock time")

	// Need to validate the migrationSchedules separately because they are created using storkctl
	// and not a part of the torpedo scheduler context.
	_, err = storkops.Instance().ValidateMigrationSchedule(reverseMigrationScheduleName, defaultAdminNamespace, defaultWaitTimeout, defaultWaitInterval)
	log.Info("reverse migration schedule created")

	failbackCmdArgs := map[string]string{
		"migration-reference": reverseMigrationScheduleName,
		"include-namespaces":  elasticsearchNamespace,
		"namespace":           defaultAdminNamespace,
	}
	drActionName, _ = createDRAction(t, defaultAdminNamespace, storkv1.ActionTypeFailback, reverseMigrationScheduleName, failbackCmdArgs)
	// Wait for failback action to complete.
	waitTillActionComplete(t, storkv1.ActionTypeFailback, drActionName, defaultAdminNamespace)
	err = setMockTime(nil)
	log.FailOnError(t, err, "Error resetting mock time")

	// Verify the elasticsearch application is not running on the destination cluster.
	destStatefulsets, err = apps.Instance().ListStatefulSets(elasticsearchNamespace, metav1.ListOptions{})
	log.FailOnError(t, err, "error retrieving statefulsets from %s namespace", elasticsearchNamespace)
	Dash.VerifyFatal(t, len(destStatefulsets.Items), 1, fmt.Sprintf("Expected 1 statefulset in destination in %s namespace", elasticsearchNamespace))
	Dash.VerifyFatal(t, *destStatefulsets.Items[0].Spec.Replicas, 0, fmt.Sprintf("Expected 0 replica in destination in sts in %s namespace", elasticsearchNamespace))

	// Verify the mysql application is running on the destination cluster.
	destDeployments, err = apps.Instance().ListDeployments(mysqlNamespace, metav1.ListOptions{})
	log.FailOnError(t, err, "error retrieving deployments from %s namespace", mysqlNamespace)
	Dash.VerifyFatal(t, len(destDeployments.Items), 1, fmt.Sprintf("Expected 1 deployment in destination in %s namespace", mysqlNamespace))
	Dash.VerifyFatal(t, *destDeployments.Items[0].Spec.Replicas, 1, fmt.Sprintf("Expected 1 replica in destination in deployment in %s namespace", mysqlNamespace))

	// Verify the elasticsearch application is running on the source cluster.
	err = setSourceKubeConfig()
	log.FailOnError(t, err, "failed to set kubeconfig to source cluster: %v", err)
	expectedReplicas := int32(3)
	sourceStatefulsets, err = apps.Instance().ListStatefulSets(elasticsearchNamespace, metav1.ListOptions{})
	log.FailOnError(t, err, "error retrieving statefulsets from %s namespace", elasticsearchNamespace)
	Dash.VerifyFatal(t, len(sourceStatefulsets.Items), 1, fmt.Sprintf("Expected 1 statefulset in source in %s namespace", elasticsearchNamespace))
	Dash.VerifyFatal(t, *sourceStatefulsets.Items[0].Spec.Replicas, expectedReplicas, fmt.Sprintf("Expected %d replica in source in %s namespace", sourceStatefulsetReplicas, elasticsearchNamespace))

	// Verify the mysql application is not running on the source cluster.
	destDeployments, err = apps.Instance().ListDeployments(mysqlNamespace, metav1.ListOptions{})
	log.FailOnError(t, err, "error retrieving deployments from %s namespace", mysqlNamespace)
	Dash.VerifyFatal(t, len(destDeployments.Items), 1, fmt.Sprintf("Expected 1 deployment in destination in %s namespace", mysqlNamespace))
	Dash.VerifyFatal(t, *destDeployments.Items[0].Spec.Replicas, 0, fmt.Sprintf("Expected 0 replica in destination in deployment in %s namespace", mysqlNamespace))

	// Verify that the migration schedule in destination cluster has been suspended.
	err = setDestinationKubeConfig()
	log.FailOnError(t, err, "failed to set kubeconfig to destination cluster: %v", err)
	migrSched, err := storkops.Instance().GetMigrationSchedule(reverseMigrationScheduleName, defaultAdminNamespace)
	log.FailOnError(t, err, "failed to retrieve migration schedule: %v", err)

	if migrSched.Spec.Suspend != nil && !(*migrSched.Spec.Suspend) {
		// Fail the test as the migration schedule is not yet suspended.
		suspendErr := fmt.Errorf("migrationschedule is not suspended on destination cluster : %s/%s", migrSched.GetName(), migrSched.GetNamespace())
		log.FailOnError(t, err, "Failed: %v", suspendErr)
	}
	log.InfoD("Successfully verified suspend migration in destination cluster")

	//////////////
	// Cleanup //
	/////////////

	// Delete destination cluster cluster-pair and migration schedule.
	err = storkops.Instance().DeleteClusterPair(remotePairName, defaultAdminNamespace)
	log.FailOnError(t, err, "failed to delete clusterpair %s in namespace %s in destination: %v", remotePairName, defaultAdminNamespace, err)
	DeleteAndWaitForMigrationScheduleDeletion(t, migrationScheduleName, defaultAdminNamespace)
	DeleteAndWaitForMigrationScheduleDeletion(t, reverseMigrationScheduleName, defaultAdminNamespace)
	for schedulePolicyName := range reverseSchedulePolicyArgs {
		err := storkops.Instance().DeleteSchedulePolicy(schedulePolicyName)
		log.FailOnError(t, err, "error deleting up schedule policy %s", schedulePolicyName)
	}
	// cleanup
	destroyAndWait(t, preMigrationCtxs)

	// Delete source cluster cluster-pair and migration schedule.
	err = setSourceKubeConfig()
	log.FailOnError(t, err, "failed to set kubeconfig to source cluster: %v", err)
	err = storkops.Instance().DeleteClusterPair(remotePairName, defaultAdminNamespace)
	log.FailOnError(t, err, "failed to delete clusterpair %s in namespace %s in source: %v", remotePairName, defaultAdminNamespace, err)
	DeleteAndWaitForMigrationScheduleDeletion(t, migrationScheduleName, defaultAdminNamespace)
	DeleteAndWaitForMigrationScheduleDeletion(t, reverseMigrationScheduleName, defaultAdminNamespace)
	for schedulePolicyName := range schedulePolicyArgs {
		err := storkops.Instance().DeleteSchedulePolicy(schedulePolicyName)
		log.FailOnError(t, err, "error deleting up schedule policy %s", schedulePolicyName)
	}
	// cleanup
	destroyAndWait(t, preMigrationCtxs)
	blowNamespaceForTest(t, elasticsearchNamespace, false)
	blowNamespaceForTest(t, mysqlNamespace, false)

	// If we are here then the test has passed
	testResult = testResultPass
	log.InfoD("Test status at end of %s test: %s", t.Name(), testResult)
}

// testDRActionMetroFailover tests the failover and failover status reporting for sync DR.
func testDRActionMetroFailover(t *testing.T) {
	// Create the apps for migration.
	// appKeys is an array of appKeys that will be used to create the apps.
	instanceID := "metro-failover-multi-ns-migration-schedule"
	mysqlApp := "mysql-1-pvc"
	mysqlNamespace := fmt.Sprintf("%s-%s", mysqlApp, instanceID)
	elasticsearchApp := "elasticsearch"
	elasticsearchNamespace := fmt.Sprintf("%s-%s", elasticsearchApp, instanceID)
	appKeys := []string{mysqlApp, elasticsearchApp}

	// Reset config in case of error
	defer func() {
		err := setSourceKubeConfig()
		log.FailOnError(t, err, "Error resetting remote config")
	}()

	// Schedule multiple apps on the source cluster.
	var preMigrationCtxs []*scheduler.Context
	for _, appKey := range appKeys {
		ctxs, err := schedulerDriver.Schedule(instanceID,
			scheduler.ScheduleOptions{
				AppKeys: []string{appKey},
				Labels:  nil,
			})
		log.FailOnError(t, err, "Error scheduling task")
		Dash.VerifyFatal(t, 1, len(ctxs), "Only one task should have started")
		preMigrationCtxs = append(preMigrationCtxs, ctxs[0].DeepCopy())
	}
	for _, preMigrationCtx := range preMigrationCtxs {
		err := schedulerDriver.WaitForRunning(preMigrationCtx, defaultWaitTimeout, defaultWaitInterval)
		log.FailOnError(t, err, "Error waiting for app to get to running state")
	}

	// Validate the successful deployment of apps on the source cluster.
	sourceDeployments, err := apps.Instance().ListDeployments(mysqlNamespace, metav1.ListOptions{})
	log.FailOnError(t, err, "error retrieving deployments from %s namespace", mysqlNamespace)
	Dash.VerifyFatal(t, len(sourceDeployments.Items), 1, fmt.Sprintf("Expected 1 deployment in source in %s namespace", mysqlNamespace))
	sourceDeploymentReplicas := *sourceDeployments.Items[0].Spec.Replicas
	sourceStatefulsets, err := apps.Instance().ListStatefulSets(elasticsearchNamespace, metav1.ListOptions{})
	log.FailOnError(t, err, "error retrieving statefulsets from %s namespace", elasticsearchNamespace)
	Dash.VerifyFatal(t, len(sourceStatefulsets.Items), 1, fmt.Sprintf("Expected 1 statefulset in source in %s namespace", elasticsearchNamespace))
	sourceStatefulsetReplicas := *sourceStatefulsets.Items[0].Spec.Replicas

	// Create the clusterpair.
	clusterPairNamespace := defaultAdminNamespace
	log.Info("Creating bidirectional cluster pair:")
	log.InfoD("Name: %s", remotePairName)
	log.InfoD("Namespace: %s", clusterPairNamespace)
	log.InfoD("Backuplocation: %s", defaultBackupLocation)
	log.InfoD("Secret name: %s", defaultSecretName)
	err = scheduleBidirectionalClusterPair(remotePairName, clusterPairNamespace, projectIDMappings, defaultBackupLocation, defaultSecretName, true)
	log.FailOnError(t, err, "failed to set bidirectional cluster pair: %v", err)
	err = setSourceKubeConfig()
	log.FailOnError(t, err, "failed to set kubeconfig to source cluster: %v", err)

	// Create the migration schedule
	// we want to create the schedulePolicy and migrationSchedule using storkctl instead of scheduling apps using torpedo's scheduler
	// schedulePolicyArgs is a map of schedulePolicyName : {{flag1:value1,flag2:value2,....}}
	schedulePolicyArgs := make(map[string]map[string]string)
	schedulePolicyArgs["migrate-every-5m"] = map[string]string{"policy-type": "Interval", "interval-minutes": "5"}

	// migrationScheduleArgs is a map of migrationScheduleName : {{flag1:value1,flag2:value2,....}}
	migrationScheduleArgs := make(map[string]map[string]string)
	migrationScheduleArgs[instanceID] = map[string]string{
		"purge-deleted-resources": "",
		"schedule-policy-name":    "migrate-every-5m",
	}

	//Create schedulePolicies using storkCtl if any required
	factory := storkctl.NewFactory()
	var outputBuffer bytes.Buffer
	cmd := storkctl.NewCommand(factory, os.Stdin, &outputBuffer, os.Stderr)
	for schedulePolicyName, customArgs := range schedulePolicyArgs {
		cmdArgs := []string{"create", "schedulepolicy", schedulePolicyName}
		executeStorkCtlCommand(t, cmd, cmdArgs, customArgs)
	}
	//Create migrationSchedules using storkCtl
	migrationScheduleName := "metro-dr-migration-schedule-multiple-ns"
	namespacesValue := fmt.Sprintf("%s,%s", mysqlNamespace, elasticsearchNamespace)
	cmdArgs := []string{"create", "migrationschedule", migrationScheduleName, "-c", remotePairName,
		"--namespaces", namespacesValue, "-n", defaultAdminNamespace}
	executeStorkCtlCommand(t, cmd, cmdArgs, migrationScheduleArgs[instanceID])

	// bump time of the world by 6 minutes
	mockNow := time.Now().Add(6 * time.Minute)
	err = setMockTime(&mockNow)
	log.FailOnError(t, err, "Error setting mock time")

	// Need to validate the migrationSchedules separately because they are created using storkctl
	// and not a part of the torpedo scheduler context.
	_, err = storkops.Instance().ValidateMigrationSchedule(migrationScheduleName, defaultAdminNamespace, defaultWaitTimeout, defaultWaitInterval)
	log.FailOnError(t, err, "failed to set validate migration schedule,error: %v", err)
	err = setMockTime(nil)
	log.FailOnError(t, err, "Error resetting mock time")

	// Failover the application
	err = setDestinationKubeConfig()
	log.FailOnError(t, err, "failed to set kubeconfig to destination cluster: %v", err)

	failoverCmdArgs := map[string]string{
		"migration-reference": migrationScheduleName,
		"include-namespaces":  namespacesValue,
		"namespace":           defaultAdminNamespace,
	}
	drActionName, _ := createDRAction(t, defaultAdminNamespace, storkv1.ActionTypeFailover, migrationScheduleName, failoverCmdArgs)

	// Wait for failover action to complete
	waitTillActionComplete(t, storkv1.ActionTypeFailover, drActionName, defaultAdminNamespace)

	// Verify the application is running on the destination cluster
	destDeployments, err := apps.Instance().ListDeployments(mysqlNamespace, metav1.ListOptions{})
	log.FailOnError(t, err, "error retrieving deployments from %s namespace", mysqlNamespace)
	Dash.VerifyFatal(t, len(destDeployments.Items), 1, fmt.Sprintf("Expected 1 deployment in destination in %s namespace", mysqlNamespace))
	Dash.VerifyFatal(t, *destDeployments.Items[0].Spec.Replicas, sourceDeploymentReplicas, fmt.Sprintf("Expected %d replica in destination in %s namespace", sourceDeploymentReplicas, mysqlNamespace))

	destStatefulsets, err := apps.Instance().ListStatefulSets(elasticsearchNamespace, metav1.ListOptions{})
	log.FailOnError(t, err, "error retrieving statefulsets from %s namespace", elasticsearchNamespace)
	Dash.VerifyFatal(t, len(destStatefulsets.Items), 1, fmt.Sprintf("Expected 1 statefulset in destination in %s namespace", elasticsearchNamespace))
	Dash.VerifyFatal(t, *destStatefulsets.Items[0].Spec.Replicas, sourceStatefulsetReplicas, fmt.Sprintf("Expected %d replica in destination in %s namespace", sourceStatefulsetReplicas, elasticsearchNamespace))

	DeleteAndWaitForMigrationScheduleDeletion(t, migrationScheduleName, defaultAdminNamespace)
	err = storkops.Instance().DeleteClusterPair(remotePairName, defaultAdminNamespace)
	log.FailOnError(t, err, "failed to delete clusterpair %s in namespace %s in destination: %v", remotePairName, defaultAdminNamespace, err)

	// Validate cluster domain status of source, destination and witness node.
	validateClusterDomainStatus(t, false, true)

	// Activate the clusterdomain again on source cluster in order to make it work for next test.
	cmdArgs = []string{"activate", "clusterdomain", "--all", "--kubeconfig", srcKubeConfigPath, "-n", defaultAdminNamespace}
	executeStorkCtlCommand(t, cmd, cmdArgs, nil)

	// Verify the application is not running on the source cluster
	err = setSourceKubeConfig()
	log.FailOnError(t, err, "failed to set kubeconfig to source cluster: %v", err)
	sourceDeployments, err = apps.Instance().ListDeployments(mysqlNamespace, metav1.ListOptions{})
	log.FailOnError(t, err, "error retrieving deployments from %s namespace", mysqlNamespace)
	Dash.VerifyFatal(t, len(sourceDeployments.Items), 1, fmt.Sprintf("Expected 1 deployment in source in %s namespace", mysqlNamespace))
	Dash.VerifyFatal(t, *sourceDeployments.Items[0].Spec.Replicas, 0, fmt.Sprintf("Expected 0 replica in source in deployment in %s namespace", mysqlNamespace))

	sourceStatefulsets, err = apps.Instance().ListStatefulSets(elasticsearchNamespace, metav1.ListOptions{})
	log.FailOnError(t, err, "error retrieving statefulsets from %s namespace", elasticsearchNamespace)
	Dash.VerifyFatal(t, len(sourceStatefulsets.Items), 1, fmt.Sprintf("Expected 1 statefulset in source in %s namespace", elasticsearchNamespace))
	Dash.VerifyFatal(t, *sourceStatefulsets.Items[0].Spec.Replicas, 0, fmt.Sprintf("Expected 0 replica in source in sts in %s namespace", elasticsearchNamespace))

	DeleteAndWaitForMigrationScheduleDeletion(t, migrationScheduleName, defaultAdminNamespace)
	for schedulePolicyName := range schedulePolicyArgs {
		err := storkops.Instance().DeleteSchedulePolicy(schedulePolicyName)
		log.FailOnError(t, err, "error deleting up schedule policy %s", schedulePolicyName)
	}
	err = storkops.Instance().DeleteClusterPair(remotePairName, defaultAdminNamespace)
	log.FailOnError(t, err, "failed to delete clusterpair %s in namespace %s in source: %v", remotePairName, defaultAdminNamespace, err)

	// cleanup
	destroyAndWait(t, preMigrationCtxs)
	for _, ns := range []string{mysqlNamespace, elasticsearchNamespace} {
		blowNamespaceForTest(t, ns, false)
	}
}

// testDRActionMetroFailback tests the failback and failback status reporting for sync DR.
func testDRActionMetroFailback(t *testing.T) {
	// Create the apps for migration.
	// appKeys is an array of appKeys that will be used to create the apps.
	instanceID := "metro-failback-multi-ns-migration-schedule"
	mysqlApp := "mysql-1-pvc"
	mysqlNamespace := fmt.Sprintf("%s-%s", mysqlApp, instanceID)
	elasticsearchApp := "elasticsearch"
	elasticsearchNamespace := fmt.Sprintf("%s-%s", elasticsearchApp, instanceID)
	appKeys := []string{mysqlApp, elasticsearchApp}

	// Reset config in case of error
	defer func() {
		err := setSourceKubeConfig()
		log.FailOnError(t, err, "Error resetting remote config")
	}()

	// Schedule multiple apps on the source cluster.
	var preMigrationCtxs []*scheduler.Context
	for _, appKey := range appKeys {
		ctxs, err := schedulerDriver.Schedule(instanceID,
			scheduler.ScheduleOptions{
				AppKeys: []string{appKey},
				Labels:  nil,
			})
		log.FailOnError(t, err, "Error scheduling task")
		Dash.VerifyFatal(t, 1, len(ctxs), "Only one task should have started")
		preMigrationCtxs = append(preMigrationCtxs, ctxs[0].DeepCopy())
	}
	for _, preMigrationCtx := range preMigrationCtxs {
		err := schedulerDriver.WaitForRunning(preMigrationCtx, defaultWaitTimeout, defaultWaitInterval)
		log.FailOnError(t, err, "Error waiting for app to get to running state")
	}

	// Validate the successful deployment of apps on the source cluster.
	sourceDeployments, err := apps.Instance().ListDeployments(mysqlNamespace, metav1.ListOptions{})
	log.FailOnError(t, err, "error retrieving deployments from %s namespace", mysqlNamespace)
	Dash.VerifyFatal(t, len(sourceDeployments.Items), 1, fmt.Sprintf("Expected 1 deployment in source in %s namespace", mysqlNamespace))
	sourceDeploymentReplicas := *sourceDeployments.Items[0].Spec.Replicas
	sourceStatefulsets, err := apps.Instance().ListStatefulSets(elasticsearchNamespace, metav1.ListOptions{})
	log.FailOnError(t, err, "error retrieving statefulsets from %s namespace", elasticsearchNamespace)
	Dash.VerifyFatal(t, len(sourceStatefulsets.Items), 1, fmt.Sprintf("Expected 1 statefulset in source in %s namespace", elasticsearchNamespace))
	sourceStatefulsetReplicas := *sourceStatefulsets.Items[0].Spec.Replicas

	/////////////////////////////////
	// Migrationschedule creation //
	///////////////////////////////

	// Create the clusterpair.
	clusterPairNamespace := defaultAdminNamespace
	log.Info("Creating bidirectional cluster pair:")
	log.InfoD("Name: %s", remotePairName)
	log.InfoD("Namespace: %s", clusterPairNamespace)
	log.InfoD("Backuplocation: %s", defaultBackupLocation)
	log.InfoD("Secret name: %s", defaultSecretName)
	err = scheduleBidirectionalClusterPair(remotePairName, clusterPairNamespace, projectIDMappings, defaultBackupLocation, defaultSecretName, true)
	log.FailOnError(t, err, "failed to set bidirectional cluster pair: %v", err)
	err = setSourceKubeConfig()
	log.FailOnError(t, err, "failed to set kubeconfig to source cluster: %v", err)

	// Create the migration schedule
	// we want to create the schedulePolicy and migrationSchedule using storkctl instead of scheduling apps using torpedo's scheduler
	// schedulePolicyArgs is a map of schedulePolicyName : {{flag1:value1,flag2:value2,....}}
	schedulePolicyArgs := make(map[string]map[string]string)
	schedulePolicyArgs["migrate-every-5m"] = map[string]string{"policy-type": "Interval", "interval-minutes": "5"}

	// migrationScheduleArgs is a map of migrationScheduleName : {{flag1:value1,flag2:value2,....}}
	migrationScheduleArgs := make(map[string]map[string]string)
	migrationScheduleArgs[instanceID] = map[string]string{
		"purge-deleted-resources": "",
		"schedule-policy-name":    "migrate-every-5m",
	}

	//Create schedulePolicies using storkCtl if any required
	factory := storkctl.NewFactory()
	var outputBuffer bytes.Buffer
	cmd := storkctl.NewCommand(factory, os.Stdin, &outputBuffer, os.Stderr)
	for schedulePolicyName, customArgs := range schedulePolicyArgs {
		cmdArgs := []string{"create", "schedulepolicy", schedulePolicyName}
		executeStorkCtlCommand(t, cmd, cmdArgs, customArgs)
	}
	//Create migrationSchedules using storkCtl
	migrationScheduleName := "metro-dr-migration-schedule-multiple-ns"
	namespacesValue := fmt.Sprintf("%s,%s", mysqlNamespace, elasticsearchNamespace)
	cmdArgs := []string{"create", "migrationschedule", migrationScheduleName, "-c", remotePairName,
		"--namespaces", namespacesValue, "-n", defaultAdminNamespace}
	executeStorkCtlCommand(t, cmd, cmdArgs, migrationScheduleArgs[instanceID])

	// bump time of the world by 6 minutes
	mockNow := time.Now().Add(6 * time.Minute)
	err = setMockTime(&mockNow)
	log.FailOnError(t, err, "Error setting mock time")

	// Need to validate the migrationSchedules separately because they are created using storkctl
	// and not a part of the torpedo scheduler context.
	_, err = storkops.Instance().ValidateMigrationSchedule(migrationScheduleName, defaultAdminNamespace, defaultWaitTimeout, defaultWaitInterval)
	log.FailOnError(t, err, "failed to set validate migration schedule,error: %v", err)
	err = setMockTime(nil)
	log.FailOnError(t, err, "Error resetting mock time")

	//////////////////////////////////////
	// Failover to destination cluster //
	////////////////////////////////////
	err = setDestinationKubeConfig()
	log.FailOnError(t, err, "failed to set kubeconfig to destination cluster: %v", err)

	// Failover the application to destination cluster.
	failoverCmdArgs := map[string]string{
		"migration-reference": migrationScheduleName,
		"include-namespaces":  namespacesValue,
		"namespace":           defaultAdminNamespace,
	}
	drActionName, _ := createDRAction(t, defaultAdminNamespace, storkv1.ActionTypeFailover, migrationScheduleName, failoverCmdArgs)

	// Wait for failover action to complete.
	waitTillActionComplete(t, storkv1.ActionTypeFailover, drActionName, defaultAdminNamespace)

	// Verify the application is running on the destination cluster
	destDeployments, err := apps.Instance().ListDeployments(mysqlNamespace, metav1.ListOptions{})
	log.FailOnError(t, err, "error retrieving deployments from %s namespace", mysqlNamespace)
	Dash.VerifyFatal(t, len(destDeployments.Items), 1, fmt.Sprintf("Expected 1 deployment in destination in %s namespace", mysqlNamespace))
	Dash.VerifyFatal(t, *destDeployments.Items[0].Spec.Replicas, sourceDeploymentReplicas, fmt.Sprintf("Expected %d replica in destination in %s namespace", sourceDeploymentReplicas, mysqlNamespace))

	destStatefulsets, err := apps.Instance().ListStatefulSets(elasticsearchNamespace, metav1.ListOptions{})
	log.FailOnError(t, err, "error retrieving statefulsets from %s namespace", elasticsearchNamespace)
	Dash.VerifyFatal(t, len(destStatefulsets.Items), 1, fmt.Sprintf("Expected 1 statefulset in destination in %s namespace", elasticsearchNamespace))
	Dash.VerifyFatal(t, *destStatefulsets.Items[0].Spec.Replicas, sourceStatefulsetReplicas, fmt.Sprintf("Expected %d replica in destination in %s namespace", sourceStatefulsetReplicas, elasticsearchNamespace))

	// Validate cluster domain status of source, destination and witness node.
	validateClusterDomainStatus(t, false, true)

	// Activate the clusterdomain again on source cluster in order to make it work for next test.
	cmdArgs = []string{"activate", "clusterdomain", "--all", "--kubeconfig", srcKubeConfigPath, "-n", defaultAdminNamespace}
	executeStorkCtlCommand(t, cmd, cmdArgs, nil)

	// Verify the application is not running on the source cluster
	err = setSourceKubeConfig()
	log.FailOnError(t, err, "failed to set kubeconfig to source cluster: %v", err)
	sourceDeployments, err = apps.Instance().ListDeployments(mysqlNamespace, metav1.ListOptions{})
	log.FailOnError(t, err, "error retrieving deployments from %s namespace", mysqlNamespace)
	Dash.VerifyFatal(t, len(sourceDeployments.Items), 1, fmt.Sprintf("Expected 1 deployment in source in %s namespace", mysqlNamespace))
	Dash.VerifyFatal(t, *sourceDeployments.Items[0].Spec.Replicas, 0, fmt.Sprintf("Expected 0 replica in source in deployment in %s namespace", mysqlNamespace))

	sourceStatefulsets, err = apps.Instance().ListStatefulSets(elasticsearchNamespace, metav1.ListOptions{})
	log.FailOnError(t, err, "error retrieving statefulsets from %s namespace", elasticsearchNamespace)
	Dash.VerifyFatal(t, len(sourceStatefulsets.Items), 1, fmt.Sprintf("Expected 1 statefulset in source in %s namespace", elasticsearchNamespace))
	Dash.VerifyFatal(t, *sourceStatefulsets.Items[0].Spec.Replicas, 0, fmt.Sprintf("Expected 0 replica in source in sts in %s namespace", elasticsearchNamespace))

	/////////////////////////////////////////////////////////
	// Failback all namespaces back to the source cluster //
	///////////////////////////////////////////////////////
	err = setDestinationKubeConfig()
	log.FailOnError(t, err, "failed to set kubeconfig to destination cluster: %v", err)

	// Create reverse migrationschedule using storkCtl.
	// Initialize reversemigrationschedule and reverse schedule policy.
	reverseSchedulePolicyArgs := make(map[string]map[string]string)
	reverseSchedulePolicyArgs["reverse-migrate-every-5m"] = map[string]string{"policy-type": "Interval", "interval-minutes": "5"}
	reverseMigrationScheduleArgs := make(map[string]map[string]string)
	reverseMigrationScheduleArgs[instanceID] = map[string]string{
		"purge-deleted-resources": "",
		"schedule-policy-name":    "reverse-migrate-every-5m",
	}

	// Create schedulePolicies using storkCtl.
	cmd = storkctl.NewCommand(factory, os.Stdin, &outputBuffer, os.Stderr)
	for schedulePolicyName, customArgs := range reverseSchedulePolicyArgs {
		cmdArgs := []string{"create", "schedulepolicy", schedulePolicyName, "--kubeconfig", destinationKubeConfigPath}
		executeStorkCtlCommand(t, cmd, cmdArgs, customArgs)
	}
	log.Info("created reverse schedule policy")

	// Create migration schedule.
	reverseMigrationScheduleName := "reverse-metro-dr-migration-schedule-multiple-ns"
	cmdArgs = []string{"create", "migrationschedule", reverseMigrationScheduleName, "-c", remotePairName,
		"--namespaces", namespacesValue, "-n", defaultAdminNamespace, "--kubeconfig", destinationKubeConfigPath}
	executeStorkCtlCommand(t, cmd, cmdArgs, reverseMigrationScheduleArgs[instanceID])

	// Bump time of the world by 6 minutes.
	mockNow = time.Now().Add(6 * time.Minute)
	err = setMockTime(&mockNow)
	log.FailOnError(t, err, "Error setting mock time")

	// Need to validate the migrationSchedules separately because they are created using storkctl
	// and not a part of the torpedo scheduler context.
	_, err = storkops.Instance().ValidateMigrationSchedule(reverseMigrationScheduleName, defaultAdminNamespace, defaultWaitTimeout, defaultWaitInterval)
	log.FailOnError(t, err, "failed to validate the reverse migration schedule")
	log.Info("created reverse migration schedule")

	failbackCmdArgs := map[string]string{
		"migration-reference": reverseMigrationScheduleName,
		"include-namespaces":  namespacesValue,
		"namespace":           defaultAdminNamespace,
	}
	drActionName, _ = createDRAction(t, defaultAdminNamespace, storkv1.ActionTypeFailback, reverseMigrationScheduleName, failbackCmdArgs)
	// Wait for failback action to complete.
	waitTillActionComplete(t, storkv1.ActionTypeFailback, drActionName, defaultAdminNamespace)
	err = setMockTime(nil)
	log.FailOnError(t, err, "Error resetting mock time")

	/////////////////////////
	// Failback assertion //
	///////////////////////

	// Verify the elasticsearch application is not running on the destination cluster.
	destStatefulsets, err = apps.Instance().ListStatefulSets(elasticsearchNamespace, metav1.ListOptions{})
	log.FailOnError(t, err, "error retrieving statefulsets from %s namespace", elasticsearchNamespace)
	Dash.VerifyFatal(t, len(destStatefulsets.Items), 1, fmt.Sprintf("Expected 1 statefulset in destination in %s namespace", elasticsearchNamespace))
	Dash.VerifyFatal(t, *destStatefulsets.Items[0].Spec.Replicas, 0, fmt.Sprintf("Expected 0 replica in destination in sts in %s namespace", elasticsearchNamespace))

	// Verify the mysql application is not running on the destination cluster.
	destDeployments, err = apps.Instance().ListDeployments(mysqlNamespace, metav1.ListOptions{})
	log.FailOnError(t, err, "error retrieving deployments from %s namespace", mysqlNamespace)
	Dash.VerifyFatal(t, len(destDeployments.Items), 1, fmt.Sprintf("Expected 1 deployment in destination in %s namespace", mysqlNamespace))
	Dash.VerifyFatal(t, *destDeployments.Items[0].Spec.Replicas, 0, fmt.Sprintf("Expected 0 replica in destination in deployment in %s namespace", mysqlNamespace))

	// Validate cluster domain status of source, destination and witness node.
	validateClusterDomainStatus(t, true, true)

	// Verify the elasticsearch application is running on the source cluster.
	err = setSourceKubeConfig()
	log.FailOnError(t, err, "failed to set kubeconfig to source cluster: %v", err)
	expectedReplicas := int32(3)
	sourceStatefulsets, err = apps.Instance().ListStatefulSets(elasticsearchNamespace, metav1.ListOptions{})
	log.FailOnError(t, err, "error retrieving statefulsets from %s namespace", elasticsearchNamespace)
	Dash.VerifyFatal(t, len(sourceStatefulsets.Items), 1, fmt.Sprintf("Expected 1 statefulset in source in %s namespace", elasticsearchNamespace))
	Dash.VerifyFatal(t, *sourceStatefulsets.Items[0].Spec.Replicas, expectedReplicas, fmt.Sprintf("Expected %d replica in source in %s namespace", sourceStatefulsetReplicas, elasticsearchNamespace))

	// Verify the mysql application is running on the source cluster.
	destDeployments, err = apps.Instance().ListDeployments(mysqlNamespace, metav1.ListOptions{})
	log.FailOnError(t, err, "error retrieving deployments from %s namespace", mysqlNamespace)
	Dash.VerifyFatal(t, len(destDeployments.Items), 1, fmt.Sprintf("Expected 1 deployment in destination in %s namespace", mysqlNamespace))
	Dash.VerifyFatal(t, *destDeployments.Items[0].Spec.Replicas, 1, fmt.Sprintf("Expected 1 replica in destination in deployment in %s namespace", mysqlNamespace))

	// Verify that the migration schedule in destination cluster has been suspended.
	err = setDestinationKubeConfig()
	log.FailOnError(t, err, "failed to set kubeconfig to destination cluster: %v", err)
	migrSched, err := storkops.Instance().GetMigrationSchedule(reverseMigrationScheduleName, defaultAdminNamespace)
	log.FailOnError(t, err, "failed to retrieve migration schedule: %v", err)

	if migrSched.Spec.Suspend != nil && !(*migrSched.Spec.Suspend) {
		// Fail the test as the migration schedule is not yet suspended.
		suspendErr := fmt.Errorf("migrationschedule is not suspended on destination cluster : %s/%s", migrSched.GetName(), migrSched.GetNamespace())
		log.FailOnError(t, err, "Failed: %v", suspendErr)
	}
	log.InfoD("Successfully verified suspend migration in destination cluster")

	//////////////
	// Cleanup //
	/////////////

	// Delete destination cluster cluster-pair and migration schedule.
	err = storkops.Instance().DeleteClusterPair(remotePairName, defaultAdminNamespace)
	log.FailOnError(t, err, "failed to delete clusterpair %s in namespace %s in destination: %v", remotePairName, defaultAdminNamespace, err)
	DeleteAndWaitForMigrationScheduleDeletion(t, migrationScheduleName, defaultAdminNamespace)
	DeleteAndWaitForMigrationScheduleDeletion(t, reverseMigrationScheduleName, defaultAdminNamespace)
	for schedulePolicyName := range reverseSchedulePolicyArgs {
		err := storkops.Instance().DeleteSchedulePolicy(schedulePolicyName)
		log.FailOnError(t, err, "error deleting up schedule policy %s", schedulePolicyName)
	}
	// cleanup
	destroyAndWait(t, preMigrationCtxs)

	// Delete source cluster cluster-pair and migration schedule.
	err = setSourceKubeConfig()
	log.FailOnError(t, err, "failed to set kubeconfig to source cluster: %v", err)
	err = storkops.Instance().DeleteClusterPair(remotePairName, defaultAdminNamespace)
	log.FailOnError(t, err, "failed to delete clusterpair %s in namespace %s in source: %v", remotePairName, defaultAdminNamespace, err)
	DeleteAndWaitForMigrationScheduleDeletion(t, migrationScheduleName, defaultAdminNamespace)
	DeleteAndWaitForMigrationScheduleDeletion(t, reverseMigrationScheduleName, defaultAdminNamespace)
	for schedulePolicyName := range schedulePolicyArgs {
		err := storkops.Instance().DeleteSchedulePolicy(schedulePolicyName)
		log.FailOnError(t, err, "error deleting up schedule policy %s", schedulePolicyName)
	}
	// cleanup
	destroyAndWait(t, preMigrationCtxs)
	blowNamespaceForTest(t, elasticsearchNamespace, false)
	blowNamespaceForTest(t, mysqlNamespace, false)
}

func failBackWithMigrationSchedulesWithDifferentPolicies(t *testing.T, instanceID string, appName string, appNamespace string, reverseSchedulePolicyArgs map[string]map[string]string) {
	ctxs, err := schedulerDriver.Schedule(instanceID,
		scheduler.ScheduleOptions{
			AppKeys: []string{appName},
			Labels:  nil,
		})
	log.FailOnError(t, err, "Error scheduling task")
	Dash.VerifyFatal(t, 1, len(ctxs), "Only one task should have started")

	err = schedulerDriver.WaitForRunning(ctxs[0], defaultWaitTimeout, defaultWaitInterval)
	log.FailOnError(t, err, "Error waiting for app to get to running state")

	sourceDeployments, err := apps.Instance().ListDeployments(appNamespace, metav1.ListOptions{})
	log.FailOnError(t, err, "error retrieving deployments from %s namespace", appNamespace)
	Dash.VerifyFatal(t, len(sourceDeployments.Items), 1, fmt.Sprintf("Expected 1 deployment in source in %s namespace", appNamespace))
	sourceDeploymentReplicas := *sourceDeployments.Items[0].Spec.Replicas

	// Create the clusterpair
	log.Info("Creating bidirectional cluster pair:")
	log.InfoD("Name: %s", remotePairName)
	log.InfoD("Namespace: %s", appNamespace)
	log.InfoD("Backuplocation: %s", defaultBackupLocation)
	log.InfoD("Secret name: %s", defaultSecretName)
	err = scheduleBidirectionalClusterPair(remotePairName, appNamespace, projectIDMappings, defaultBackupLocation, defaultSecretName, false)
	log.FailOnError(t, err, "failed to set bidirectional cluster pair: %v", err)

	err = setSourceKubeConfig()
	log.FailOnError(t, err, "failed to set kubeconfig to source cluster: %v", err)

	// Create the migration schedule
	// we want to create the schedulePolicy and migrationSchedule using storkctl instead of scheduling apps using torpedo's scheduler
	// schedulePolicyArgs is a map of schedulePolicyName : {{flag1:value1,flag2:value2,....}}
	schedulePolicyArgs := make(map[string]map[string]string)
	schedulePolicyArgs["migrate-every-10m"] = map[string]string{"policy-type": "Interval", "interval-minutes": "10"}

	// migrationScheduleArgs is a map of migrationScheduleName : {{flag1:value1,flag2:value2,....}}
	migrationScheduleArgs := make(map[string]map[string]string)
	migrationScheduleArgs[instanceID] = map[string]string{
		"purge-deleted-resources": "",
		"schedule-policy-name":    "migrate-every-10m",
	}

	//Create schedulePolicies using storkCtl if any required
	factory := storkctl.NewFactory()
	var outputBuffer bytes.Buffer
	cmd := storkctl.NewCommand(factory, os.Stdin, &outputBuffer, os.Stderr)
	for schedulePolicyName, customArgs := range schedulePolicyArgs {
		cmdArgs := []string{"create", "schedulepolicy", schedulePolicyName}
		executeStorkCtlCommand(t, cmd, cmdArgs, customArgs)
	}
	//Create migrationSchedules using storkCtl
	migrationScheduleName := "forward-migration-schedule-running-migration"
	cmdArgs := []string{"create", "migrationschedule", migrationScheduleName, "-c", remotePairName,
		"--namespaces", appNamespace, "-n", appNamespace}
	executeStorkCtlCommand(t, cmd, cmdArgs, migrationScheduleArgs[instanceID])

	time.Sleep(30 * time.Second)

	// Failover the application
	err = setDestinationKubeConfig()
	log.FailOnError(t, err, "failed to set kubeconfig to destination cluster: %v", err)

	failoverCmdArgs := map[string]string{
		"migration-reference": migrationScheduleName,
		"include-namespaces":  appNamespace,
		"namespace":           appNamespace,
	}
	drActionName, _ := createDRAction(t, appNamespace, storkv1.ActionTypeFailover, migrationScheduleName, failoverCmdArgs)

	// Wait for failover action to complete
	waitTillActionComplete(t, storkv1.ActionTypeFailover, drActionName, appNamespace)

	// Verify the application is running on the destination cluster
	destDeployments, err := apps.Instance().ListDeployments(appNamespace, metav1.ListOptions{})
	log.FailOnError(t, err, "error retrieving deployments from %s namespace", appNamespace)
	Dash.VerifyFatal(t, len(destDeployments.Items), 1, fmt.Sprintf("Expected 1 deployment in destination in %s namespace", appNamespace))
	Dash.VerifyFatal(t, *destDeployments.Items[0].Spec.Replicas, sourceDeploymentReplicas, fmt.Sprintf("Expected %d replica in destination in %s namespace", sourceDeploymentReplicas, appNamespace))

	// Verify the application is not running on the source cluster
	err = setSourceKubeConfig()
	log.FailOnError(t, err, "failed to set kubeconfig to source cluster: %v", err)
	sourceDeployments, err = apps.Instance().ListDeployments(appNamespace, metav1.ListOptions{})
	log.FailOnError(t, err, "error retrieving deployments from %s namespace", appNamespace)
	Dash.VerifyFatal(t, len(sourceDeployments.Items), 1, fmt.Sprintf("Expected 1 deployment in source in %s namespace", appNamespace))
	Dash.VerifyFatal(t, *sourceDeployments.Items[0].Spec.Replicas, 0, fmt.Sprintf("Expected 0 replica in source in deployment in %s namespace", appNamespace))

	// Failback the application
	err = setDestinationKubeConfig()
	log.FailOnError(t, err, "failed to set kubeconfig to destination cluster: %v", err)
	err = setMockTime(nil)
	log.FailOnError(t, err, "Error resetting mock time")

	// migrationScheduleArgs is a map of migrationScheduleName : {{flag1:value1,flag2:value2,....}}
	reverseMigrationScheduleArgs := make(map[string]map[string]string)
	reverseMigrationScheduleArgs[instanceID] = map[string]string{
		"purge-deleted-resources": "",
	}

	//Create schedulePolicies using storkCtl if any required
	cmd = storkctl.NewCommand(factory, os.Stdin, &outputBuffer, os.Stderr)
	var reverseSchedulePolicyName string
	var nextTriggerTime time.Time
	for schedulePolicyName, customArgs := range reverseSchedulePolicyArgs {
		nextTriggerTime = createSchedulePolicyWithMockedTime(t, schedulePolicyName, customArgs["policy-type"])
		reverseMigrationScheduleArgs[instanceID]["schedule-policy-name"] = schedulePolicyName
		// Will use the last schedulePolicyName as reverseSchedulePolicyName
		reverseSchedulePolicyName = schedulePolicyName
	}
	//Create migrationSchedules using storkCtl
	reverseMigrationScheduleName := "reverse-migration-schedule-running-migration"
	cmdArgs = []string{"create", "migrationschedule", reverseMigrationScheduleName, "-c", remotePairName,
		"--namespaces", appNamespace, "-n", appNamespace, "--kubeconfig", destinationKubeConfigPath}
	executeStorkCtlCommand(t, cmd, cmdArgs, reverseMigrationScheduleArgs[instanceID])

	log.InfoD("PolicyName: %s, Policy args: %v, ", reverseSchedulePolicyName, reverseSchedulePolicyArgs)

	err = setMockTime(&nextTriggerTime)
	log.FailOnError(t, err, "Error setting mock time")
	defer func() {
		err := setMockTime(nil)
		log.FailOnError(t, err, "Error resetting mock time")
	}()

	// Validate reverse migration schedule
	_, err = storkops.Instance().ValidateMigrationSchedule(reverseMigrationScheduleName, appNamespace, defaultWaitTimeout, defaultWaitInterval)
	log.FailOnError(t, err, "failed to validate reverse migration schedule for failback")

	failbackCmdArgs := map[string]string{
		"migration-reference": reverseMigrationScheduleName,
		"include-namespaces":  appNamespace,
		"namespace":           appNamespace,
	}
	failbackActionName, _ := createDRAction(t, appNamespace, storkv1.ActionTypeFailback, reverseMigrationScheduleName, failbackCmdArgs)

	// Wait for failover action to complete
	waitTillActionComplete(t, storkv1.ActionTypeFailback, failbackActionName, appNamespace)

	// Verify the application is running on the destination cluster
	destDeployments, err = apps.Instance().ListDeployments(appNamespace, metav1.ListOptions{})
	log.FailOnError(t, err, "error retrieving deployments from %s namespace", appNamespace)
	Dash.VerifyFatal(t, len(destDeployments.Items), 1, fmt.Sprintf("Expected 1 deployment in destination in %s namespace", appNamespace))
	Dash.VerifyFatal(t, *destDeployments.Items[0].Spec.Replicas, 0, fmt.Sprintf("Expected 0 replica in destination in %s namespace", appNamespace))

	DeleteAndWaitForMigrationScheduleDeletion(t, reverseMigrationScheduleName, appNamespace)

	for schedulePolicyName := range reverseSchedulePolicyArgs {
		err := storkops.Instance().DeleteSchedulePolicy(schedulePolicyName)
		log.FailOnError(t, err, "error deleting up schedule policy %s", schedulePolicyName)
	}
	err = storkops.Instance().DeleteClusterPair(remotePairName, appNamespace)
	log.FailOnError(t, err, "failed to delete clusterpair %s in namespace %s in destination: %v", remotePairName, appNamespace, err)

	// Reset mocktime in destination cluster
	err = setMockTime(nil)
	log.FailOnError(t, err, "Error resetting mock time")

	// Verify the application is not running on the source cluster
	err = setSourceKubeConfig()
	log.FailOnError(t, err, "failed to set kubeconfig to source cluster: %v", err)
	sourceDeployments, err = apps.Instance().ListDeployments(appNamespace, metav1.ListOptions{})
	log.FailOnError(t, err, "error retrieving deployments from %s namespace", appNamespace)
	Dash.VerifyFatal(t, len(sourceDeployments.Items), 1, fmt.Sprintf("Expected 1 deployment in source in %s namespace", appNamespace))
	Dash.VerifyFatal(t, *sourceDeployments.Items[0].Spec.Replicas, sourceDeploymentReplicas, fmt.Sprintf("Expected %d replica in source in deployment in %s namespace", sourceDeploymentReplicas, appNamespace))

	DeleteAndWaitForMigrationScheduleDeletion(t, migrationScheduleName, appNamespace)
	for schedulePolicyName := range schedulePolicyArgs {
		err := storkops.Instance().DeleteSchedulePolicy(schedulePolicyName)
		log.FailOnError(t, err, "error deleting up schedule policy %s", schedulePolicyName)
	}

	err = storkops.Instance().DeleteClusterPair(remotePairName, appNamespace)
	log.FailOnError(t, err, "failed to delete clusterpair %s in namespace %s in source: %v", remotePairName, appNamespace, err)

	// cleanup
	destroyAndWait(t, ctxs)
	blowNamespaceForTest(t, appNamespace, false)
}

// waitTillActionComplete waits for the DR action to complete
func waitTillActionComplete(t *testing.T, actionType storkv1.ActionType, actionName string, actionNamespace string) {
	// Wait for 15 mins for the action to complete
	for i := 0; i < 15; i++ {
		stage, status, info := getDRActionStatus(t, actionType, actionName, actionNamespace)
		if stage == "Completed" {
			if status == string(storkv1.ActionStatusSuccessful) {
				return
			} else if status == string(storkv1.ActionStatusFailed) {
				log.FailOnError(t, fmt.Errorf("DR action %s failed with status %s", actionName, status), "DR action failed expected status: %s, actual status: %s", storkv1.ActionStatusSuccessful, status)
			}
		}
		time.Sleep(1 * time.Minute)
		log.InfoD("Waiting for DR action to complete, action: %s, stage: %s, status: %s, info: %s", actionName, stage, status, info)
	}
	log.FailOnError(t, fmt.Errorf("DR action %s did not complete in 15 minutes", actionName), "DR action did not complete in 15 minutes")
}

func createSchedulePolicyWithMockedTime(t *testing.T, policyName string, policyType string) time.Time {
	retain := 5
	intervalMinutes := 15
	// Set first trigger 2 minutes from now
	scheduledTime := time.Now().Add(2 * time.Minute)

	var nextScheduledTime time.Time
	var schedPolicy *storkv1.SchedulePolicy
	switch policyType {
	case string(storkv1.SchedulePolicyTypeInterval):
		nextScheduledTime = scheduledTime.Add(time.Duration(intervalMinutes) * time.Minute)
		schedPolicy = &storkv1.SchedulePolicy{
			ObjectMeta: metav1.ObjectMeta{
				Name: policyName,
			},
			Policy: storkv1.SchedulePolicyItem{
				Interval: &storkv1.IntervalPolicy{
					Retain:          storkv1.Retain(retain),
					IntervalMinutes: 15,
				},
			}}
	case string(storkv1.SchedulePolicyTypeDaily):
		nextScheduledTime = scheduledTime.AddDate(0, 0, 1)
		schedPolicy = &storkv1.SchedulePolicy{
			ObjectMeta: metav1.ObjectMeta{
				Name: policyName,
			},
			Policy: storkv1.SchedulePolicyItem{
				Daily: &storkv1.DailyPolicy{
					Retain: storkv1.Retain(retain),
					Time:   scheduledTime.Format(time.Kitchen),
				},
			}}
	case string(storkv1.SchedulePolicyTypeWeekly):
		// Set first trigger 2 minutes from now
		nextScheduledTime = scheduledTime.AddDate(0, 0, 7)
		schedPolicy = &storkv1.SchedulePolicy{
			ObjectMeta: metav1.ObjectMeta{
				Name: policyName,
			},
			Policy: storkv1.SchedulePolicyItem{
				Weekly: &storkv1.WeeklyPolicy{
					Retain: storkv1.Retain(retain),
					Day:    scheduledTime.Weekday().String(),
					Time:   scheduledTime.Format(time.Kitchen),
				},
			}}
	case string(storkv1.SchedulePolicyTypeMonthly):
		nextScheduledTime = scheduledTime.AddDate(0, 1, 0)
		// Set the time to zero in case the date doesn't exist in the next month
		if nextScheduledTime.Day() != scheduledTime.Day() {
			nextScheduledTime = time.Time{}
		}
		schedPolicy = &storkv1.SchedulePolicy{
			ObjectMeta: metav1.ObjectMeta{
				Name: policyName,
			},
			Policy: storkv1.SchedulePolicyItem{
				Monthly: &storkv1.MonthlyPolicy{
					Retain: storkv1.Retain(retain),
					Date:   scheduledTime.Day(),
					Time:   scheduledTime.Format(time.Kitchen),
				},
			},
		}
	default:
		log.FailOnError(t, fmt.Errorf("invalid policy type %s", policyType), "Invalid policy type")
	}
	if authTokenConfigMap != "" {
		err := addSecurityAnnotation(schedPolicy)
		log.FailOnError(t, err, "error updating security annotations for schedule policy %s of type %v", policyName, policyType)
	}

	_, err := storkops.Instance().CreateSchedulePolicy(schedPolicy)
	log.FailOnError(t, err, "error creating schedule policy %s of type %s", policyName, policyType)
	log.InfoD("Created schedulepolicy %s of type %s at time %v on date %v",
		policyName, policyType, scheduledTime.Format(time.Kitchen), scheduledTime.Day())
	return nextScheduledTime
}
