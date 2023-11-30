//go:build integrationtest
// +build integrationtest

package integrationtest

import (
	"fmt"
	"strings"
	"testing"
	"time"

	storkapi "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/libopenstorage/stork/pkg/resourcecollector"
	"github.com/portworx/sched-ops/k8s/core"
	storkops "github.com/portworx/sched-ops/k8s/stork"
	"github.com/portworx/torpedo/pkg/asyncdr"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/clientcmd"
)

var (
	StashCRLabel = "stash-cr"
)

func TestStashStrategyMigration(t *testing.T) {
	// reset mock time before running any tests
	err := setMockTime(nil)
	require.NoError(t, err, "Error resetting mock time")

	logrus.Infof("Using stork volume driver: %s", volumeDriverName)
	logrus.Infof("Backup path being used: %s", backupLocationPath)

	setDefaultsForBackup(t)

	t.Run("testMigrationStashStrategyMongoDB", testMigrationStashStrategyMongoDB)
	t.Run("testMigrationStashStrategyKafka", testMigrationStashStrategyKafka)
	t.Run("testMigrationStashStrategyWithStartApplication", testMigrationStashStrategyWithStartApplication)
	t.Run("testMultipleTimesMigrationsWithStashStrategy", testMultipleTimesMigrationsWithStashStrategy)
	t.Run("testFailbackWithStashStrategy", testFailbackWithStashStrategy)
}

func testMigrationStashStrategyMongoDB(t *testing.T) {
	var testrailID, testResult = 64408114, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult)
	defer updateTestRail(&testResult, testrailID, runID)

	migrationStashStrategy(t, appNameMongo, appPathMongo)
	// If we are here then the test has passed
	time.Sleep(30 * time.Second)
	testResult = testResultPass
	logrus.Infof("Test status at end of %s test: %s", t.Name(), testResult)
}

func testMigrationStashStrategyKafka(t *testing.T) {
	var testrailID, testResult = 64408112, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult)
	defer updateTestRail(&testResult, testrailID, runID)

	migrationStashStrategy(t, appNameKafka, appPathKafka)
	// If we are here then the test has passed
	time.Sleep(30 * time.Second)
	testResult = testResultPass
	logrus.Infof("Test status at end of %s test: %s", t.Name(), testResult)
}

func migrationStashStrategy(t *testing.T, appName string, appPath string) {
	err := setSourceKubeConfig()
	require.NoError(t, err, "Failed to set kubeconfig to source cluster: %v", err)

	appData := asyncdr.GetAppData(appName)
	podsCreated, err := asyncdr.PrepareApp(appName, appPath)
	require.NoError(t, err, "Error creating pods")

	podsCreatedLen := len(podsCreated.Items)
	logrus.Infof("podsCreatedLen: %d", podsCreatedLen)
	sourceClusterConfigPath, err := getClusterConfigPath(srcConfig)
	require.NoError(t, err, "Error getting source config path")

	destClusterConfigPath, err := getClusterConfigPath(destConfig)
	require.NoError(t, err, "Error getting destination config path")

	// validate CRD on source
	err = asyncdr.ValidateCRD(appData.ExpectedCrdList, sourceClusterConfigPath)
	require.NoError(t, err, "Error validating source crds")

	// set stashstrategy
	err = setSourceKubeConfig()
	require.NoError(t, err, "Error setting source kubeconfig")

	err = scheduleBidirectionalClusterPair(clusterPairName, appData.Ns, "", storkapi.BackupLocationType(backupLocation), backupSecret)
	require.NoError(t, err, "Error creating cluster pair")

	err = setSourceKubeConfig()
	require.NoError(t, err, "Error setting source kubeconfig")

	suspendOptions := storkapi.SuspendOptions{}
	stashStrategy := storkapi.StashStrategy{StashCR: true}
	err = updateAppReg(appName, suspendOptions, stashStrategy)
	require.NoError(t, err, fmt.Sprintf("Error setting stash strategy in application registrations for app %s", appName))

	logrus.Infof("Starting migration %s/%s with startApplication false", appData.Ns, migNamePref+appName)
	startApplications := false
	mig, err := asyncdr.CreateMigration(migNamePref+appName, appData.Ns, clusterPairName, appData.Ns, &includeVolumesFlag, &includeResourcesFlag, &startApplications)
	require.NoError(t, err, "Error creating migration")
	err = asyncdr.WaitForMigration([]*storkapi.Migration{mig})
	require.NoError(t, err, "Error waiting for migration")
	logrus.Infof("Migration %s/%s completed successfully ", appData.Ns, migNamePref+appName)

	err = setDestinationKubeConfig()
	require.NoError(t, err, "Error setting dest kubeconfig")

	// validate CRD on destination
	err = asyncdr.ValidateCRD(appData.ExpectedCrdList, destClusterConfigPath)
	require.NoError(t, err, "Error validating destination crds")

	// validate that CR is not present as it is stashed
	validated, err := validateCR(appName, appData.Ns, destClusterConfigPath)
	require.NoError(t, err, fmt.Sprintf("Error validating CR for app %s in namespace %s", appName, appData.Ns))
	require.False(t, validated, fmt.Sprintf("CR for app %s in namespace %s should not have been present", appName, appData.Ns))

	// validate stashed cm in destination
	stashedCMCount := getStashedConfigMapCount(t, appData.Ns)
	operatorCRMap := getSupportedOperatorCRMapping()
	if _, ok := operatorCRMap[appName]; !ok {
		require.True(t, ok, fmt.Sprintf("app %s is not currently supported in test framework", appName))
	}
	expectedStashedCMCount := len(operatorCRMap[appName])
	require.Equal(t, expectedStashedCMCount, stashedCMCount, fmt.Sprintf("expected stashed configmap count %d got %d", expectedStashedCMCount, stashedCMCount))

	// activate app
	err = activateAppUsingStorkctl(appData.Ns, false)
	require.NoError(t, err, fmt.Sprintf("Error activating app in namespace %s", appData.Ns))

	err = setDestinationKubeConfig()
	require.NoError(t, err, "Error setting dest kubeconfig")

	logrus.Infof("Waiting for application pods to come up")
	time.Sleep(5 * time.Minute)
	logrus.Infof("Applications pods should be up by now")

	// validate that CR is present
	validated, err = validateCR(appName, appData.Ns, destClusterConfigPath)
	require.NoError(t, err, fmt.Sprintf("Error validating CR for app %s in namespace %s", appName, appData.Ns))
	require.True(t, validated, fmt.Sprintf("CR for app %s in namespace %s should have been present", appName, appData.Ns))

	podsMigrated, err := core.Instance().GetPods(appData.Ns, nil)
	require.NoError(t, err, "Error getting migrated pods")

	podsMigratedLen := len(podsMigrated.Items)
	require.Equal(t, podsCreatedLen, podsMigratedLen, "Pods migration failed as len of pods found on source doesn't match with pods found on destination")

	logrus.Infof("Delete destination and source namespaces")
	asyncdr.DeleteCRAndUninstallCRD(appData.OperatorName, appPath, appData.Ns)
	err = core.Instance().DeleteNamespace(appData.Ns)
	if err != nil {
		logrus.Errorf("Error deleting namespace %s in destination: %v\n", appData.Ns, err)
	}
	err = setSourceKubeConfig()
	require.NoError(t, err, "Error setting source kubeconfig")

	err = asyncdr.DeleteAndWaitForMigrationDeletion(mig.Name, mig.Namespace)
	require.NoError(t, err, "Error deleting migration")

	asyncdr.DeleteCRAndUninstallCRD(appData.OperatorName, appPath, appData.Ns)
	err = core.Instance().DeleteNamespace(appData.Ns)
	if err != nil {
		logrus.Errorf("Error deleting namespace %s in source: %v\n", appData.Ns, err)
	}
	logrus.Infof("Test %s ended", t.Name())
}

func updateAppReg(appName string, suspendOptions storkapi.SuspendOptions, stashStrategy storkapi.StashStrategy) error {
	operatorCRMap := getSupportedOperatorCRMapping()
	if _, ok := operatorCRMap[appName]; !ok {
		return fmt.Errorf("app %s is not currently supported in test framework", appName)
	}
	appResources := operatorCRMap[appName]

	for _, appResource := range appResources {
		appregName := strings.ToLower(appResource.Kind)
		reg, err := storkops.Instance().GetApplicationRegistration(appregName)
		if err != nil {
			return fmt.Errorf("error getting application registration %s: %v", appregName, err)
		}
		for i, resource := range reg.Resources {
			resource.SuspendOptions = suspendOptions
			resource.StashStrategy = stashStrategy
			reg.Resources[i] = resource
		}
		_, err = storkops.Instance().UpdateApplicationRegistration(reg)
		if err != nil {
			return fmt.Errorf("error setting stash strategy in application registration %s, %v", appregName, err)
		}
	}
	return nil
}

func validateCR(appName string, namespace string, kubeConfig string) (bool, error) {
	operatorCRMap := getSupportedOperatorCRMapping()
	if _, ok := operatorCRMap[appName]; !ok {
		return false, fmt.Errorf("app %s is not currently supported in test framework", appName)
	}
	resources := operatorCRMap[appName]

	config, err := clientcmd.BuildConfigFromFlags("", kubeConfig)
	if err != nil {
		return false, fmt.Errorf("error building config from kubeconfig failed: %v", err)
	}
	resourceCollector := resourcecollector.ResourceCollector{
		Driver: nil,
	}
	err = resourceCollector.Init(config)
	if err != nil {
		return false, fmt.Errorf("error initing resourcecollector while validating CR: %v", err)
	}

	validated := true
	for _, resource := range resources {
		objects, _, err := resourceCollector.GetResourcesForType(
			resource,
			nil,
			[]string{namespace},
			nil,
			nil,
			nil,
			false,
			resourcecollector.Options{},
		)
		if err != nil {
			return false, fmt.Errorf("error fetching objects while validating CR: %v", err)
		}
		logrus.Infof("number of resources for app %s in namespace %s is %d", appName, namespace, len(objects.Items))
		if len(objects.Items) != 1 {
			validated = false
		}
	}

	return validated, nil
}

func getStashedConfigMapCount(t *testing.T, namespace string) int {
	configMaps, err := core.Instance().ListConfigMap(namespace, metav1.ListOptions{LabelSelector: StashCRLabel})
	require.NoError(t, err, fmt.Sprintf("error listing configmaps in namespace %s", namespace))

	return len(configMaps.Items)
}

func getStashedCMUIDs(namespace string) (map[string]string, error) {
	nameUIDMap := make(map[string]string)

	configMaps, err := core.Instance().ListConfigMap(namespace, metav1.ListOptions{LabelSelector: StashCRLabel})
	if err != nil {
		return nameUIDMap, err
	}

	for _, configMap := range configMaps.Items {
		nameUIDMap[configMap.Name] = string(configMap.GetUID())
	}

	return nameUIDMap, nil
}

func testMigrationStashStrategyWithStartApplication(t *testing.T) {
	var testrailID, testResult = 64408118, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult)
	defer updateTestRail(&testResult, testrailID, runID)

	err := setSourceKubeConfig()
	require.NoError(t, err, "failed to set kubeconfig to source cluster: %v", err)

	appName := appNameMongo
	appPath := appPathMongo
	appData := asyncdr.GetAppData(appName)
	podsCreated, err := asyncdr.PrepareApp(appName, appPath)
	require.NoError(t, err, "Error creating pods")

	podsCreatedLen := len(podsCreated.Items)
	logrus.Infof("podsCreatedLen: %d", podsCreatedLen)
	sourceClusterConfigPath, err := getClusterConfigPath(srcConfig)
	require.NoError(t, err, "Error getting source config path")

	destClusterConfigPath, err := getClusterConfigPath(destConfig)
	require.NoError(t, err, "Error getting destination config path")

	// validate CRD on source
	err = asyncdr.ValidateCRD(appData.ExpectedCrdList, sourceClusterConfigPath)
	require.NoError(t, err, "Error validating source crds")

	err = setSourceKubeConfig()
	require.NoError(t, err, "Error setting source kubeconfig")

	err = scheduleBidirectionalClusterPair(clusterPairName, appData.Ns, "", storkapi.BackupLocationType(backupLocation), backupSecret)
	require.NoError(t, err, "Error creating cluster pair")

	// set stashstrategy
	err = setSourceKubeConfig()
	require.NoError(t, err, "Error setting source kubeconfig")

	suspendOptions := storkapi.SuspendOptions{}
	stashStrategy := storkapi.StashStrategy{StashCR: true}
	err = updateAppReg(appName, suspendOptions, stashStrategy)
	require.NoError(t, err, fmt.Sprintf("Error setting stash strategy in application registrations for app %s", appName))

	logrus.Infof("Starting migration %s/%s with startApplication true", appData.Ns, migNamePref+appName)
	startApplications := true
	mig, err := asyncdr.CreateMigration(migNamePref+appName, appData.Ns, clusterPairName, appData.Ns, &includeVolumesFlag, &includeResourcesFlag, &startApplications)
	require.NoError(t, err, "Error creating migration")
	err = asyncdr.WaitForMigration([]*storkapi.Migration{mig})
	require.NoError(t, err, "Error waiting for migration")
	logrus.Infof("Migration %s/%s completed successfully ", appData.Ns, migNamePref+appName)

	err = setDestinationKubeConfig()
	require.NoError(t, err, "Error setting dest kubeconfig")

	// validate CRD on destination
	err = asyncdr.ValidateCRD(appData.ExpectedCrdList, destClusterConfigPath)
	require.NoError(t, err, "Error validating destination crds")

	// validate that CR is not present as it is stashed
	validated, err := validateCR(appName, appData.Ns, destClusterConfigPath)
	require.NoError(t, err, fmt.Sprintf("Error validating CR for app %s in namespace %s", appName, appData.Ns))
	require.True(t, validated, fmt.Sprintf("CR for app %s in namespace %s should have been present as startApplications is true", appName, appData.Ns))

	// validate no stashed cm is present
	stashedCMCount := getStashedConfigMapCount(t, appData.Ns)
	expectedStashedCMCount := 0
	require.Equal(t, expectedStashedCMCount, stashedCMCount, fmt.Sprintf("expected stashed configmap count %d got %d", expectedStashedCMCount, stashedCMCount))

	podsMigrated, err := core.Instance().GetPods(appData.Ns, nil)
	require.NoError(t, err, "Error getting migrated pods")

	podsMigratedLen := len(podsMigrated.Items)
	require.Equal(t, podsCreatedLen, podsMigratedLen, "Pods migration failed as len of pods found on source doesn't match with pods found on destination")

	logrus.Infof("Delete destination and source namespaces")
	asyncdr.DeleteCRAndUninstallCRD(appData.OperatorName, appPath, appData.Ns)
	err = core.Instance().DeleteNamespace(appData.Ns)
	if err != nil {
		logrus.Errorf("Error deleting namespace %s in destination: %v\n", appData.Ns, err)
	}
	err = setSourceKubeConfig()
	require.NoError(t, err, "Error setting source kubeconfig")

	err = asyncdr.DeleteAndWaitForMigrationDeletion(mig.Name, mig.Namespace)
	require.NoError(t, err, "Error deleting migration")

	asyncdr.DeleteCRAndUninstallCRD(appData.OperatorName, appPath, appData.Ns)
	err = core.Instance().DeleteNamespace(appData.Ns)
	if err != nil {
		logrus.Errorf("Error deleting namespace %s in source: %v\n", appData.Ns, err)
	}
	time.Sleep(30 * time.Second)
	logrus.Infof("Test %s ended", t.Name())
	// If we are here then the test has passed
	testResult = testResultPass
	logrus.Infof("Test status at end of %s test: %s", t.Name(), testResult)
}

func testMultipleTimesMigrationsWithStashStrategy(t *testing.T) {
	testrailIDList := []int{66266865, 64408115}
	testResult := testResultFail
	for _, testrailID := range testrailIDList {
		runID := testrailSetupForTest(testrailID, &testResult)
		defer updateTestRail(&testResult, testrailID, runID)
	}

	err := setSourceKubeConfig()
	require.NoError(t, err, "failed to set kubeconfig to source cluster: %w", err)

	appName := appNameMongo
	appPath := appPathMongo
	appData := asyncdr.GetAppData(appName)
	podsCreated, err := asyncdr.PrepareApp(appName, appPath)
	require.NoError(t, err, "Error creating pods")

	podsCreatedLen := len(podsCreated.Items)
	logrus.Infof("podsCreatedLen: %d", podsCreatedLen)
	sourceClusterConfigPath, err := getClusterConfigPath(srcConfig)
	require.NoError(t, err, "Error getting source config path")

	destClusterConfigPath, err := getClusterConfigPath(destConfig)
	require.NoError(t, err, "Error getting destination config path")

	// validate CRD on source
	err = asyncdr.ValidateCRD(appData.ExpectedCrdList, sourceClusterConfigPath)
	require.NoError(t, err, "Error validating source crds")

	err = setSourceKubeConfig()
	require.NoError(t, err, "Error setting source kubeconfig")

	err = scheduleBidirectionalClusterPair(clusterPairName, appData.Ns, "", storkapi.BackupLocationType(backupLocation), backupSecret)
	require.NoError(t, err, "Error creating cluster pair")

	// set stashstrategy
	err = setSourceKubeConfig()
	require.NoError(t, err, "Error setting source kubeconfig")

	suspendOptions := storkapi.SuspendOptions{}
	stashStrategy := storkapi.StashStrategy{StashCR: true}
	err = updateAppReg(appName, suspendOptions, stashStrategy)
	require.NoError(t, err, fmt.Sprintf("Error setting stash strategy in application registrations for app %s", appName))

	startApplications := false
	firstMigrationName := fmt.Sprintf("%s%s-%d", migNamePref, appName, 1)
	logrus.Infof("Starting migration %s/%s with startApplication false, iteration number: 1", appData.Ns, firstMigrationName)
	mig1, err := asyncdr.CreateMigration(firstMigrationName, appData.Ns, clusterPairName, appData.Ns, &includeVolumesFlag, &includeResourcesFlag, &startApplications)
	require.NoError(t, err, "Error creating migration")
	err = asyncdr.WaitForMigration([]*storkapi.Migration{mig1})
	require.NoError(t, err, "Error waiting for migration")
	logrus.Infof("Migration %s/%s completed successfully ", appData.Ns, mig1.Name)

	// get the resource UID to verify if it is getting overwritten
	initialCMUIDs, err := getStashedCMUIDs(appData.Ns)
	require.NoError(t, err, "Error getting stashed configmap UIDs")

	// Do the migration again and verify the resource ID
	secondMigrationName := fmt.Sprintf("%s%s-%d", migNamePref, appName, 2)
	logrus.Infof("Starting migration %s/%s with startApplication false, iteration number: 2", appData.Ns, secondMigrationName)
	mig2, err := asyncdr.CreateMigration(secondMigrationName, appData.Ns, clusterPairName, appData.Ns, &includeVolumesFlag, &includeResourcesFlag, &startApplications)
	require.NoError(t, err, "Error creating migration")
	err = asyncdr.WaitForMigration([]*storkapi.Migration{mig2})
	require.NoError(t, err, "Error waiting for migration")
	logrus.Infof("Migration %s/%s completed successfully ", appData.Ns, mig2.Name)

	// get the resource UID to verify if it is getting overwritten
	finalCMUIDs, err := getStashedCMUIDs(appData.Ns)
	require.NoError(t, err, "Error getting stashed configmap UIDs")

	require.Equal(t, initialCMUIDs, finalCMUIDs, "stashed configmap UIDs are not matching")

	err = setDestinationKubeConfig()
	require.NoError(t, err, "Error setting dest kubeconfig")

	// validate CRD on destination
	err = asyncdr.ValidateCRD(appData.ExpectedCrdList, destClusterConfigPath)
	require.NoError(t, err, "Error validating destination crds")

	// validate that CR is not present as it is stashed
	validated, err := validateCR(appName, appData.Ns, destClusterConfigPath)
	require.NoError(t, err, fmt.Sprintf("Error validating CR for app %s in namespace %s", appName, appData.Ns))
	require.False(t, validated, fmt.Sprintf("CR for app %s in namespace %s should not have been present", appName, appData.Ns))

	// validate stashed cm in destination
	stashedCMCount := getStashedConfigMapCount(t, appData.Ns)
	operatorCRMap := getSupportedOperatorCRMapping()
	if _, ok := operatorCRMap[appName]; !ok {
		require.True(t, ok, fmt.Sprintf("app %s is not currently supported in test framework", appName))
	}
	expectedStashedCMCount := len(operatorCRMap[appName])
	require.Equal(t, expectedStashedCMCount, stashedCMCount, fmt.Sprintf("expected stashed configmap count %d got %d", expectedStashedCMCount, stashedCMCount))

	// activate app
	err = activateAppUsingStorkctl(appData.Ns, false)
	require.NoError(t, err, fmt.Sprintf("Error activating app in namespace %s", appData.Ns))

	err = setDestinationKubeConfig()
	require.NoError(t, err, "Error setting dest kubeconfig")

	logrus.Infof("Waiting for application pods to come up")
	time.Sleep(5 * time.Minute)
	logrus.Infof("Applications pods should be up by now")

	// validate that CR is present
	validated, err = validateCR(appName, appData.Ns, destClusterConfigPath)
	require.NoError(t, err, fmt.Sprintf("Error validating CR for app %s in namespace %s", appName, appData.Ns))
	require.True(t, validated, fmt.Sprintf("CR for app %s in namespace %s should have been present", appName, appData.Ns))

	podsMigrated, err := core.Instance().GetPods(appData.Ns, nil)
	require.NoError(t, err, "Error getting migrated pods")

	podsMigratedLen := len(podsMigrated.Items)
	require.Equal(t, podsCreatedLen, podsMigratedLen, "Pods migration failed as len of pods found on source doesn't match with pods found on destination")

	// deactivate app
	err = deactivateAppUsingStorkctl(appData.Ns, false)
	require.NoError(t, err, fmt.Sprintf("Error deactivating app in namespace %s", appData.Ns))

	logrus.Infof("Delete destination and source namespaces")
	asyncdr.DeleteCRAndUninstallCRD(appData.OperatorName, appPath, appData.Ns)
	err = core.Instance().DeleteNamespace(appData.Ns)
	if err != nil {
		logrus.Errorf("Error deleting namespace %s in destination: %v\n", appData.Ns, err)
	}
	err = setSourceKubeConfig()
	require.NoError(t, err, "Error setting source kubeconfig")

	migrations := []string{mig1.Name, mig2.Name}
	for _, migration := range migrations {
		err = asyncdr.DeleteAndWaitForMigrationDeletion(migration, mig1.Namespace)
		require.NoError(t, err, "Error deleting migration")
	}
	asyncdr.DeleteCRAndUninstallCRD(appData.OperatorName, appPath, appData.Ns)
	err = core.Instance().DeleteNamespace(appData.Ns)
	if err != nil {
		logrus.Errorf("Error deleting namespace %s in source: %v\n", appData.Ns, err)
	}
	logrus.Infof("Test %s ended", t.Name())
	time.Sleep(30 * time.Second)
	testResult = testResultPass
	logrus.Infof("Test status at end of %s tests: %s", t.Name(), testResult)
}

func testFailbackWithStashStrategy(t *testing.T) {
	var testrailID, testResult = 64408117, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult)
	defer updateTestRail(&testResult, testrailID, runID)

	err := setSourceKubeConfig()
	require.NoError(t, err, "failed to set kubeconfig to source cluster: %v", err)

	appName := appNameMongo
	appPath := appPathMongo
	appData := asyncdr.GetAppData(appName)
	podsCreated, err := asyncdr.PrepareApp(appName, appPath)
	require.NoError(t, err, "Error creating pods")

	podsCreatedLen := len(podsCreated.Items)
	logrus.Infof("podsCreatedLen: %d", podsCreatedLen)
	sourceClusterConfigPath, err := getClusterConfigPath(srcConfig)
	require.NoError(t, err, "Error getting source config path")

	destClusterConfigPath, err := getClusterConfigPath(destConfig)
	require.NoError(t, err, "Error getting destination config path")

	// validate CRD on source
	err = asyncdr.ValidateCRD(appData.ExpectedCrdList, sourceClusterConfigPath)
	require.NoError(t, err, "Error validating source crds")

	err = setSourceKubeConfig()
	require.NoError(t, err, "Error setting source kubeconfig")

	// creating migration and clusterpair in kube-system namespace as we have to delete the ns in source for failback
	migrationNamespace := "kube-system"
	err = scheduleBidirectionalClusterPair(clusterPairName, migrationNamespace, "", storkapi.BackupLocationType(backupLocation), backupSecret)
	require.NoError(t, err, "Error creating cluster pair")

	// set stashstrategy
	err = setSourceKubeConfig()
	require.NoError(t, err, "Error setting source kubeconfig")

	suspendOptions := storkapi.SuspendOptions{}
	stashStrategy := storkapi.StashStrategy{StashCR: true}
	err = updateAppReg(appName, suspendOptions, stashStrategy)
	require.NoError(t, err, fmt.Sprintf("Error setting stash strategy in application registrations for app %s", appName))

	logrus.Infof("Starting migration %s/%s with startApplication false", appData.Ns, migNamePref+appName)
	startApplications := false
	mig, err := asyncdr.CreateMigration(migNamePref+appName, migrationNamespace, clusterPairName, appData.Ns, &includeVolumesFlag, &includeResourcesFlag, &startApplications)
	require.NoError(t, err, "Error creating migration")
	err = asyncdr.WaitForMigration([]*storkapi.Migration{mig})
	require.NoError(t, err, "Error waiting for migration")
	logrus.Infof("Migration %s/%s completed successfully ", appData.Ns, migNamePref+appName)

	err = setDestinationKubeConfig()
	require.NoError(t, err, "Error setting dest kubeconfig")

	// validate stashed cm in destination
	stashedCMCount := getStashedConfigMapCount(t, appData.Ns)
	operatorCRMap := getSupportedOperatorCRMapping()
	if _, ok := operatorCRMap[appName]; !ok {
		require.True(t, ok, fmt.Sprintf("app %s is not currently supported in test framework", appName))
	}
	expectedStashedCMCount := len(operatorCRMap[appName])
	require.Equal(t, expectedStashedCMCount, stashedCMCount, fmt.Sprintf("expected stashed configmap count %d got %d", expectedStashedCMCount, stashedCMCount))

	// activate app
	err = activateAppUsingStorkctl(appData.Ns, false)
	require.NoError(t, err, fmt.Sprintf("Error activating app in namespace %s", appData.Ns))

	err = setDestinationKubeConfig()
	require.NoError(t, err, "Error setting dest kubeconfig")

	logrus.Infof("Waiting for application pods to come up")
	time.Sleep(5 * time.Minute)
	logrus.Infof("Applications pods should be up by now")

	// validate that CR is present
	validated, err := validateCR(appName, appData.Ns, destClusterConfigPath)
	require.NoError(t, err, fmt.Sprintf("Error validating CR for app %s in namespace %s in destination", appName, appData.Ns))
	require.True(t, validated, fmt.Sprintf("CR for app %s in namespace %s should have been present in destination", appName, appData.Ns))

	podsMigrated, err := core.Instance().GetPods(appData.Ns, nil)
	require.NoError(t, err, "Error getting migrated pods")

	podsMigratedLen := len(podsMigrated.Items)
	require.Equal(t, podsCreatedLen, podsMigratedLen, "Pods migration failed as len of pods found on source doesn't match with pods found on destination")

	logrus.Infof("Delete source app namespace before doing reverse migration")
	err = setSourceKubeConfig()
	require.NoError(t, err, "Error setting source kubeconfig")

	err = core.Instance().DeleteNamespace(appData.Ns)
	if err != nil {
		logrus.Errorf("Error deleting namespace %s in source: %v\n", appData.Ns, err)
	}
	time.Sleep(1 * time.Minute)

	err = setDestinationKubeConfig()
	require.NoError(t, err, "Error setting dest kubeconfig")
	err = updateAppReg(appName, suspendOptions, stashStrategy)
	require.NoError(t, err, fmt.Sprintf("Error setting stash strategy in application registrations for app %s in destination cluster", appName))

	revMigrationName := fmt.Sprintf("%s%s-%s", migNamePref, appName, "reverse")
	logrus.Infof("Starting reverse migration %s/%s with startApplication false", appData.Ns, revMigrationName)
	revmig, err := asyncdr.CreateMigration(revMigrationName, migrationNamespace, clusterPairName, appData.Ns, &includeVolumesFlag, &includeResourcesFlag, &startApplications)
	require.NoError(t, err, "Error creating migration")
	err = asyncdr.WaitForMigration([]*storkapi.Migration{revmig})
	require.NoError(t, err, "Error waiting for migration")
	logrus.Infof("Migration %s/%s completed successfully ", appData.Ns, revMigrationName)

	err = setSourceKubeConfig()
	require.NoError(t, err, "Error setting source kubeconfig")

	// validate CRD on source
	err = asyncdr.ValidateCRD(appData.ExpectedCrdList, sourceClusterConfigPath)
	require.NoError(t, err, "Error validating source crds")

	// validate that CR is not present as it is stashed
	validated, err = validateCR(appName, appData.Ns, sourceClusterConfigPath)
	require.NoError(t, err, fmt.Sprintf("Error validating CR for app %s in namespace %s in source", appName, appData.Ns))
	require.False(t, validated, fmt.Sprintf("CR for app %s in namespace %s should not have been present in source", appName, appData.Ns))

	// validate stashed cm in destination
	stashedCMCount = getStashedConfigMapCount(t, appData.Ns)
	expectedStashedCMCount = len(operatorCRMap[appName])
	require.Equal(t, expectedStashedCMCount, stashedCMCount, fmt.Sprintf("expected stashed configmap count %d got %d in source", expectedStashedCMCount, stashedCMCount))

	// activate app in source
	err = activateAppUsingStorkctl(appData.Ns, true)
	require.NoError(t, err, fmt.Sprintf("Error activating app in namespace %s", appData.Ns))

	err = setSourceKubeConfig()
	require.NoError(t, err, "Error setting source kubeconfig")

	logrus.Infof("Waiting for application pods to come up in source")
	time.Sleep(5 * time.Minute)
	logrus.Infof("Applications pods should be up by now in source")

	// validate that CR is present
	validated, err = validateCR(appName, appData.Ns, sourceClusterConfigPath)
	require.NoError(t, err, fmt.Sprintf("Error validating CR for app %s in namespace %s", appName, appData.Ns))
	require.True(t, validated, fmt.Sprintf("CR for app %s in namespace %s should have been present", appName, appData.Ns))

	podsMigrated, err = core.Instance().GetPods(appData.Ns, nil)
	require.NoError(t, err, "Error getting migrated pods")

	podsMigratedLen = len(podsMigrated.Items)
	require.Equal(t, podsCreatedLen, podsMigratedLen, "Pods migration failed as len of pods found on source doesn't match with pods found on destination")

	// cleanups
	err = asyncdr.DeleteAndWaitForMigrationDeletion(mig.Name, mig.Namespace)
	require.NoError(t, err, "Error deleting migration")

	asyncdr.DeleteCRAndUninstallCRD(appData.OperatorName, appPath, appData.Ns)
	err = core.Instance().DeleteNamespace(appData.Ns)
	if err != nil {
		logrus.Errorf("Error deleting namespace %s in destination: %v\n", appData.Ns, err)
	}

	err = setDestinationKubeConfig()
	require.NoError(t, err, "Error setting destination kubeconfig")

	err = asyncdr.DeleteAndWaitForMigrationDeletion(revmig.Name, revmig.Namespace)
	require.NoError(t, err, "Error deleting migration")

	asyncdr.DeleteCRAndUninstallCRD(appData.OperatorName, appPath, appData.Ns)
	err = core.Instance().DeleteNamespace(appData.Ns)
	if err != nil {
		logrus.Errorf("Error deleting namespace %s in destination: %v\n", appData.Ns, err)
	}

	err = setSourceKubeConfig()
	require.NoError(t, err, "Error setting source kubeconfig")

	time.Sleep(30 * time.Second)
	logrus.Infof("Test %s ended", t.Name())

	testResult = testResultPass
	logrus.Infof("Test status at end of %s tests: %s", t.Name(), testResult)
}
