//go:build integrationtest
// +build integrationtest

package integrationtest

import (
	"bytes"
	"fmt"
	storkv1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/libopenstorage/stork/pkg/storkctl"
	storkops "github.com/portworx/sched-ops/k8s/stork"
	"github.com/portworx/sched-ops/task"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/yaml"
	"os"
	"testing"
	"time"
)

const (
	syncDrClusterPair  = "automation-test-sync-cluster-pair"
	asyncDrClusterPair = "automation-test-async-cluster-pair"
	defaultNs          = "default"
)

func TestStorkCtlMigrationSchedule(t *testing.T) {
	err := setSourceKubeConfig()
	require.NoError(t, err, "failed to set kubeconfig to source cluster: %v", err)
	createClusterPairs(t)
	defer cleanUpClusterPairs(t)
	t.Run("createDefaultMigrationScheduleTest", createDefaultMigrationScheduleTest)
	t.Run("createCustomMigrationScheduleTest", createCustomMigrationScheduleTest)

	err = setRemoteConfig("")
	require.NoError(t, err, "setting kubeconfig to default failed")
}

func createDefaultMigrationScheduleTest(t *testing.T) {
	testrailID := 93398
	cmdArgs := map[string]string{
		"cluster-pair":    asyncDrClusterPair,
		"namespaces":      defaultNs,
		"annotations":     "openstorage.io/auth-secret-namespace=value1,openstorage.io/auth-secret-name=value2",
		"exclude-volumes": "",
	}
	createMigrationScheduleTest(t, testrailID, cmdArgs, "default-async-migration-schedule.yaml", "async")
}

func createCustomMigrationScheduleTest(t *testing.T) {
	testrailID := 93399
	cmdArgs := map[string]string{
		"cluster-pair":         syncDrClusterPair,
		"namespaces":           defaultNs,
		"interval":             "25",
		"disable-auto-suspend": "",
	}
	createMigrationScheduleTest(t, testrailID, cmdArgs, "custom-sync-migration-schedule.yaml", "sync")
}

func createMigrationScheduleTest(t *testing.T, testrailID int, args map[string]string, specFileName string, migrationMode string) {
	migrationScheduleName := "automation-test-migration-schedule"
	var testResult = testResultFail
	runID := testrailSetupForTest(testrailID, &testResult)
	defer updateTestRail(&testResult, testrailID, runID)
	defer migrationScheduleCleanup(t, migrationScheduleName)

	factory := storkctl.NewFactory()
	var outputBuffer bytes.Buffer
	cmd := storkctl.NewCommand(factory, os.Stdin, &outputBuffer, os.Stderr)
	cmdArgs := []string{"create", "migrationschedule", migrationScheduleName}
	//add the custom args to the command if any
	for key, value := range args {
		cmdArgs = append(cmdArgs, "--"+key)
		if value != "" {
			cmdArgs = append(cmdArgs, value)
		}
	}
	cmd.SetArgs(cmdArgs)
	//execute the command
	logrus.Infof("The storkctl command being executed is %v", cmdArgs)
	if err := cmd.Execute(); err != nil {
		logrus.Errorf("Storkctl execution failed: %v", err)
		return
	}
	// Get the captured output as a string
	actualOutput := outputBuffer.String()
	logrus.Infof("Actual output is: %s", actualOutput)
	expectedOutput := fmt.Sprintf("MigrationSchedule %v created successfully\n", migrationScheduleName)
	require.Equal(t, expectedOutput, actualOutput)

	//Validate the created resource
	specFile := "specs/storkctl-specs/migrationschedule/" + specFileName
	err := ValidateMigrationScheduleFromFile(t, specFile, migrationScheduleName, migrationMode)
	require.NoError(t, err, "Error validating the created resource")

	// If we are here then the test has passed
	testResult = testResultPass
	logrus.Infof("Test status at end of %s test: %s", t.Name(), testResult)
}

func ValidateMigrationScheduleFromFile(t *testing.T, specFilePath string, migrationScheduleName string, migrationMode string) error {
	data, err := getByteDataFromFile(specFilePath)
	if err != nil {
		return err
	}
	migrationSchedule := &storkv1.MigrationSchedule{}
	dec := yaml.NewYAMLOrJSONDecoder(bytes.NewReader([]byte(data)), len(data))
	if err := dec.Decode(&migrationSchedule); err != nil {
		return err
	}
	if err := ValidateMigrationSchedule(t, migrationSchedule, migrationScheduleName, migrationMode); err != nil {
		return err
	}
	return nil
}

func ValidateMigrationSchedule(t *testing.T, migrationSchedule *storkv1.MigrationSchedule, migrationScheduleName string, migrationMode string) error {
	//We want to validate if the created schedule policy resource matches our expectations
	actualMigrationSchedule, err := storkops.Instance().GetMigrationSchedule(migrationScheduleName, defaultNs)
	if err != nil {
		logrus.Errorf("Unable to get the created migration schedule")
		return err
	}
	logrus.Info("Trying to validate the migration schedule")
	if migrationSchedule.Annotations != nil || actualMigrationSchedule.Annotations != nil {
		require.Equal(t, migrationSchedule.Annotations, actualMigrationSchedule.Annotations, "MigrationSchedule Annotations mismatch")
	}
	logrus.Infof("include-volumes param actual :  %v, expected : %v", *actualMigrationSchedule.Spec.Template.Spec.IncludeVolumes, *migrationSchedule.Spec.Template.Spec.IncludeVolumes)
	require.Equal(t, migrationSchedule.Spec.SchedulePolicyName, actualMigrationSchedule.Spec.SchedulePolicyName, "MigrationSchedule Schedule Policy mismatch")
	require.Equal(t, migrationSchedule.Spec.AutoSuspend, actualMigrationSchedule.Spec.AutoSuspend, "MigrationSchedule AutoSuspend mismatch")
	require.Equal(t, migrationSchedule.Spec.Suspend, actualMigrationSchedule.Spec.Suspend, "MigrationSchedule Suspend mismatch")
	require.Equal(t, migrationSchedule.Spec.Template.Spec.IncludeVolumes, actualMigrationSchedule.Spec.Template.Spec.IncludeVolumes, "MigrationSchedule IncludeVolumes mismatch")
	require.Equal(t, migrationSchedule.Spec.Template.Spec.StartApplications, actualMigrationSchedule.Spec.Template.Spec.StartApplications, "MigrationSchedule StartApplications mismatch")
	require.Equal(t, migrationSchedule.Spec.Template.Spec.IncludeResources, actualMigrationSchedule.Spec.Template.Spec.IncludeResources, "MigrationSchedule IncludeResources mismatch")
	require.Equal(t, migrationSchedule.Spec.Template.Spec.ClusterPair, actualMigrationSchedule.Spec.Template.Spec.ClusterPair, "MigrationSchedule ClusterPair mismatch")
	require.Equal(t, migrationSchedule.Spec.Template.Spec.Namespaces, actualMigrationSchedule.Spec.Template.Spec.Namespaces, "MigrationSchedule Namespaces mismatch")
	return nil
}

// Create 2 clusterPairs one each for async-dr and sync-dr in the default namespace to be used by the migrationSchedules in this testSuite
func createClusterPairs(t *testing.T) {
	options := make(map[string]string)
	syncClusterPairObject := generateClusterPairObject(syncDrClusterPair, options)
	_, err := storkops.Instance().CreateClusterPair(syncClusterPairObject)
	require.NoError(t, err, "Error creating sync-dr cluster pair")
	options["option1"] = "value1"
	options["option2"] = "value2"
	asyncClusterPairObject := generateClusterPairObject(asyncDrClusterPair, options)
	_, err = storkops.Instance().CreateClusterPair(asyncClusterPairObject)
	require.NoError(t, err, "Error creating async-dr cluster pair")
}

func cleanUpClusterPairs(t *testing.T) {
	logrus.Info("CLeanup clusterpairs was called")
	err := storkops.Instance().DeleteClusterPair(syncDrClusterPair, defaultNs)
	require.NoError(t, err, "Error deleting sync-dr cluster pair")
	err = storkops.Instance().DeleteClusterPair(asyncDrClusterPair, "default")
	require.NoError(t, err, "Error deleting async-dr cluster pair")
}

func generateClusterPairObject(name string, options map[string]string) *storkv1.ClusterPair {
	return &storkv1.ClusterPair{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: defaultNs,
		},

		Spec: storkv1.ClusterPairSpec{
			Options: options,
		},
	}
}

func migrationScheduleCleanup(t *testing.T, migrationScheduleName string) {
	logrus.Info("Cleaning up created resources")
	// We need to delete migration schedule and also schedule policy if created.
	DeleteAndWaitForMigrationScheduleDeletion(t, migrationScheduleName, defaultNs)
	DeleteAndWaitForSchedulePolicyDeletion(t, migrationScheduleName)
}

func DeleteAndWaitForMigrationScheduleDeletion(t *testing.T, name string, namespace string) {
	err := storkops.Instance().DeleteMigrationSchedule(name, namespace)
	if err != nil {
		logrus.Errorf("Unable to delete migration schedule %s/%s", namespace, name)
	}
	f := func() (interface{}, bool, error) {
		logrus.Infof("Checking if migration schedule resource is successfully deleted")
		_, err := storkops.Instance().GetMigrationSchedule(name, namespace)
		if err == nil {
			return "", true, fmt.Errorf("get migration schedule : %s/%s should have failed", namespace, name)
		}
		if !errors.IsNotFound(err) {
			logrus.Infof("unexpected err: %v when checking deleted migration schedule: %s/%s", err, namespace, name)
			return "", true, err
		}
		//deletion done
		logrus.Infof("Migration Schedule %s/%s successfully deleted", namespace, name)
		return "", false, nil
	}
	_, err = task.DoRetryWithTimeout(f, defaultWaitTimeout, 2*time.Second)
	require.NoError(t, err, "Unable to delete migration schedule %s/%s", namespace, name)
}
