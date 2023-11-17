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
	"github.com/libopenstorage/stork/pkg/storkctl"
	"github.com/portworx/sched-ops/k8s/core"
	storkops "github.com/portworx/sched-ops/k8s/stork"
	"github.com/portworx/sched-ops/task"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/yaml"
)

const (
	syncDrClusterPair       = "automation-test-sync-cluster-pair"
	asyncDrClusterPair      = "automation-test-async-cluster-pair"
	syncDrAdminClusterPair  = "automation-test-sync-admin-cluster-pair"
	asyncDrAdminClusterPair = "automation-test-async-admin-cluster-pair"
	defaultNs               = "default"
	adminNs                 = "kube-system"
)

func TestStorkCtlMigrationSchedule(t *testing.T) {
	err := setSourceKubeConfig()
	require.NoError(t, err, "failed to set kubeconfig to source cluster: %v", err)
	createPrerequisiteResources(t)
	defer cleanUpPrerequisiteResources(t)
	t.Run("createDefaultAsyncMigrationScheduleTest", createDefaultAsyncMigrationScheduleTest)
	t.Run("createCustomAsyncMigrationScheduleTest", createCustomAsyncMigrationScheduleTest)
	t.Run("createDefaultSyncMigrationScheduleTest", createDefaultSyncMigrationScheduleTest)
	t.Run("createCustomSyncMigrationScheduleTest", createCustomSyncMigrationScheduleTest)
	err = setRemoteConfig("")
	require.NoError(t, err, "setting kubeconfig to default failed")
}

func createDefaultAsyncMigrationScheduleTest(t *testing.T) {
	testrailID := 93398
	cmdArgs := map[string]string{
		"cluster-pair": asyncDrClusterPair,
		"namespaces":   defaultNs,
	}
	createMigrationScheduleTest(t, testrailID, cmdArgs, "default-async-migration-schedule.yaml", defaultNs)
}

func createCustomAsyncMigrationScheduleTest(t *testing.T) {
	testrailID := 93399
	cmdArgs := map[string]string{
		"cluster-pair":         asyncDrAdminClusterPair,
		"namespaces":           defaultNs,
		"namespace-selectors":  "nsKey=value",
		"transform-spec":       "test-rt",
		"annotations":          "openstorage.io/auth-secret-namespace=value1,openstorage.io/auth-secret-name=value2",
		"exclude-volumes":      "",
		"interval":             "25",
		"disable-auto-suspend": "",
		"selectors":            "key1=value",
		"exclude-selectors":    "key2=value",
		"namespace":            adminNs,
	}
	createMigrationScheduleTest(t, testrailID, cmdArgs, "custom-async-migration-schedule.yaml", adminNs)
}

func createDefaultSyncMigrationScheduleTest(t *testing.T) {
	testrailID := 93468
	cmdArgs := map[string]string{
		"cluster-pair": syncDrClusterPair,
		"namespaces":   defaultNs,
	}
	createMigrationScheduleTest(t, testrailID, cmdArgs, "default-sync-migration-schedule.yaml", defaultNs)
}

func createCustomSyncMigrationScheduleTest(t *testing.T) {
	testrailID := 93469
	cmdArgs := map[string]string{
		"cluster-pair":                     syncDrClusterPair,
		"admin-cluster-pair":               syncDrAdminClusterPair,
		"namespaces":                       defaultNs,
		"schedule-policy-name":             "default-daily-policy",
		"pre-exec-rule":                    "preExec",
		"post-exec-rule":                   "postExec",
		"include-optional-resource-types":  "xJob",
		"ignore-owner-references-check":    "",
		"purge-deleted-resources":          "",
		"skip-service-update":              "",
		"include-network-policy-with-cidr": "",
		"disable-skip-deleted-namespaces":  "",
		"disable-auto-suspend":             "",
		"suspend":                          "",
		"start-applications":               "",
	}
	createMigrationScheduleTest(t, testrailID, cmdArgs, "custom-sync-migration-schedule.yaml", defaultNs)
}

func createMigrationScheduleTest(t *testing.T, testrailID int, args map[string]string,
	specFileName string, migrationScheduleNs string) {
	migrationScheduleName := "automation-test-migration-schedule"
	var testResult = testResultFail
	runID := testrailSetupForTest(testrailID, &testResult)
	defer updateTestRail(&testResult, testrailID, runID)
	defer migrationScheduleCleanup(t, migrationScheduleName, migrationScheduleNs)

	factory := storkctl.NewFactory()
	var outputBuffer bytes.Buffer
	cmd := storkctl.NewCommand(factory, os.Stdin, &outputBuffer, os.Stderr)
	cmdArgs := []string{"create", "migrationschedule", migrationScheduleName}
	// add the custom args to the command
	for key, value := range args {
		cmdArgs = append(cmdArgs, "--"+key)
		if value != "" {
			cmdArgs = append(cmdArgs, value)
		}
	}
	cmd.SetArgs(cmdArgs)
	// execute the command
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

	// Validate the created resource
	specFile := "specs/storkctl-specs/migrationschedule/" + specFileName
	err := ValidateMigrationScheduleFromFile(t, specFile, migrationScheduleName, migrationScheduleNs)
	require.NoError(t, err, "Error validating the created resource")

	// If we are here then the test has passed
	testResult = testResultPass
	logrus.Infof("Test status at end of %s test: %s", t.Name(), testResult)
}

func ValidateMigrationScheduleFromFile(t *testing.T, specFilePath string,
	migrationScheduleName string, migrationScheduleNs string) error {
	data, err := getByteDataFromFile(specFilePath)
	if err != nil {
		return err
	}
	migrationSchedule := &storkv1.MigrationSchedule{}
	dec := yaml.NewYAMLOrJSONDecoder(bytes.NewReader(data), len(data))
	if err := dec.Decode(&migrationSchedule); err != nil {
		return err
	}
	if err := ValidateMigrationSchedule(t, migrationSchedule, migrationScheduleName, migrationScheduleNs); err != nil {
		return err
	}
	return nil
}

func ValidateMigrationSchedule(t *testing.T, migrationSchedule *storkv1.MigrationSchedule,
	migrationScheduleName string, migrationScheduleNs string) error {
	// We want to validate if the created schedule policy resource matches our expectations
	actualMigrationSchedule, err := storkops.Instance().GetMigrationSchedule(migrationScheduleName, migrationScheduleNs)
	if err != nil {
		logrus.Errorf("Unable to get the created migration schedule")
		return err
	}
	logrus.Info("Trying to validate the migration schedule")
	if migrationSchedule.Annotations != nil || actualMigrationSchedule.Annotations != nil {
		require.Equal(t, migrationSchedule.Annotations, actualMigrationSchedule.Annotations, "MigrationSchedule Annotations mismatch")
	}
	require.Equal(t, migrationSchedule.Spec.SchedulePolicyName, actualMigrationSchedule.Spec.SchedulePolicyName, "MigrationSchedule Schedule Policy mismatch")
	require.Equal(t, migrationSchedule.Spec.AutoSuspend, actualMigrationSchedule.Spec.AutoSuspend, "MigrationSchedule AutoSuspend mismatch")
	require.Equal(t, migrationSchedule.Spec.Suspend, actualMigrationSchedule.Spec.Suspend, "MigrationSchedule Suspend mismatch")
	require.Equal(t, migrationSchedule.Spec.Template.Spec.IncludeVolumes, actualMigrationSchedule.Spec.Template.Spec.IncludeVolumes, "MigrationSchedule IncludeVolumes mismatch")
	require.Equal(t, migrationSchedule.Spec.Template.Spec.StartApplications, actualMigrationSchedule.Spec.Template.Spec.StartApplications, "MigrationSchedule StartApplications mismatch")
	require.Equal(t, migrationSchedule.Spec.Template.Spec.IncludeResources, actualMigrationSchedule.Spec.Template.Spec.IncludeResources, "MigrationSchedule IncludeResources mismatch")
	require.Equal(t, migrationSchedule.Spec.Template.Spec.ClusterPair, actualMigrationSchedule.Spec.Template.Spec.ClusterPair, "MigrationSchedule ClusterPair mismatch")
	require.Equal(t, migrationSchedule.Spec.Template.Spec.AdminClusterPair, actualMigrationSchedule.Spec.Template.Spec.AdminClusterPair, "MigrationSchedule AdminClusterPair mismatch")
	require.Equal(t, migrationSchedule.Spec.Template.Spec.Namespaces, actualMigrationSchedule.Spec.Template.Spec.Namespaces, "MigrationSchedule Namespaces mismatch")
	require.Equal(t, migrationSchedule.Spec.Template.Spec.NamespaceSelectors, actualMigrationSchedule.Spec.Template.Spec.NamespaceSelectors, "MigrationSchedule NamespaceSelectors mismatch")
	require.Equal(t, migrationSchedule.Spec.Template.Spec.Selectors, actualMigrationSchedule.Spec.Template.Spec.Selectors, "MigrationSchedule Selectors mismatch")
	require.Equal(t, migrationSchedule.Spec.Template.Spec.ExcludeSelectors, actualMigrationSchedule.Spec.Template.Spec.ExcludeSelectors, "MigrationSchedule ExcludeSelectors mismatch")
	require.Equal(t, migrationSchedule.Spec.Template.Spec.IgnoreOwnerReferencesCheck, actualMigrationSchedule.Spec.Template.Spec.IgnoreOwnerReferencesCheck, "MigrationSchedule IgnoreOwnerReferencesCheck mismatch")
	require.Equal(t, migrationSchedule.Spec.Template.Spec.IncludeOptionalResourceTypes, actualMigrationSchedule.Spec.Template.Spec.IncludeOptionalResourceTypes, "MigrationSchedule IncludeOptionalResourceTypes mismatch")
	require.Equal(t, migrationSchedule.Spec.Template.Spec.PostExecRule, actualMigrationSchedule.Spec.Template.Spec.PostExecRule, "MigrationSchedule PostExecRule mismatch")
	require.Equal(t, migrationSchedule.Spec.Template.Spec.PreExecRule, actualMigrationSchedule.Spec.Template.Spec.PreExecRule, "MigrationSchedule PreExecRule mismatch")
	require.Equal(t, migrationSchedule.Spec.Template.Spec.PurgeDeletedResources, actualMigrationSchedule.Spec.Template.Spec.PurgeDeletedResources, "MigrationSchedule PurgeDeletedResources mismatch")
	require.Equal(t, migrationSchedule.Spec.Template.Spec.SkipDeletedNamespaces, actualMigrationSchedule.Spec.Template.Spec.SkipDeletedNamespaces, "MigrationSchedule SkipDeletedNamespaces mismatch")
	require.Equal(t, migrationSchedule.Spec.Template.Spec.SkipServiceUpdate, actualMigrationSchedule.Spec.Template.Spec.SkipServiceUpdate, "MigrationSchedule SkipServiceUpdate mismatch")
	require.Equal(t, migrationSchedule.Spec.Template.Spec.StartApplications, actualMigrationSchedule.Spec.Template.Spec.StartApplications, "MigrationSchedule StartApplications mismatch")
	require.Equal(t, migrationSchedule.Spec.Template.Spec.TransformSpecs, actualMigrationSchedule.Spec.Template.Spec.TransformSpecs, "MigrationSchedule TransformSpecs mismatch")
	require.Equal(t, migrationSchedule.Spec.Template.Spec.IncludeNetworkPolicyWithCIDR, actualMigrationSchedule.Spec.Template.Spec.IncludeNetworkPolicyWithCIDR, "MigrationSchedule IncludeNetworkPolicyWithCIDR mismatch")
	return nil
}

func migrationScheduleCleanup(t *testing.T, migrationScheduleName string, migrationScheduleNs string) {
	logrus.Info("Cleaning up created resources")
	// We need to delete migration schedule and also schedule policy if created.
	DeleteAndWaitForMigrationScheduleDeletion(t, migrationScheduleName, migrationScheduleNs)
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

func createPrerequisiteResources(t *testing.T) {
	createClusterPairs(t)
	createNamespace(t)
	createResourceTransformation(t, defaultNs)
	createResourceTransformation(t, "test-ns")
}

// Create 2 clusterPairs each for async-dr and sync-dr in each of the default and admin namespaces
// to be used by the migrationSchedules in this testSuite
func createClusterPairs(t *testing.T) {
	options := make(map[string]string)
	syncClusterPairObject := generateClusterPairObject(syncDrClusterPair, defaultNs, options)
	syncAdminClusterPairObject := generateClusterPairObject(syncDrAdminClusterPair, adminNs, options)
	_, err := storkops.Instance().CreateClusterPair(syncClusterPairObject)
	require.NoError(t, err, "Error creating sync-dr cluster pair")
	_, err = storkops.Instance().CreateClusterPair(syncAdminClusterPairObject)
	require.NoError(t, err, "Error creating sync-dr admin cluster pair")
	options["option1"] = "value1"
	options["option2"] = "value2"
	asyncClusterPairObject := generateClusterPairObject(asyncDrClusterPair, defaultNs, options)
	asyncAdminClusterPairObject := generateClusterPairObject(asyncDrAdminClusterPair, adminNs, options)
	_, err = storkops.Instance().CreateClusterPair(asyncClusterPairObject)
	require.NoError(t, err, "Error creating async-dr cluster pair")
	_, err = storkops.Instance().CreateClusterPair(asyncAdminClusterPairObject)
	require.NoError(t, err, "Error creating async-dr admincluster pair")
}

func generateClusterPairObject(name string, namespace string, options map[string]string) *storkv1.ClusterPair {
	return &storkv1.ClusterPair{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},

		Spec: storkv1.ClusterPairSpec{
			Options: options,
		},
	}
}

func cleanUpPrerequisiteResources(t *testing.T) {
	logrus.Info("Cleanup prerequisite resources was called")
	err := storkops.Instance().DeleteClusterPair(syncDrClusterPair, defaultNs)
	require.NoError(t, err, "Error deleting sync-dr cluster pair")
	err = storkops.Instance().DeleteClusterPair(asyncDrClusterPair, defaultNs)
	require.NoError(t, err, "Error deleting async-dr cluster pair")
	err = storkops.Instance().DeleteClusterPair(asyncDrAdminClusterPair, adminNs)
	require.NoError(t, err, "Error deleting async-dr admin cluster pair")
	err = storkops.Instance().DeleteClusterPair(syncDrAdminClusterPair, adminNs)
	require.NoError(t, err, "Error deleting sync-dr admin cluster pair")
	err = storkops.Instance().DeleteResourceTransformation("test-rt", defaultNs)
	require.NoError(t, err, "Error deleting resource transformation test-rt")
	err = storkops.Instance().DeleteResourceTransformation("test-rt", "test-ns")
	require.NoError(t, err, "Error deleting resource transformation test-rt")
	err = core.Instance().DeleteNamespace("test-ns")
	require.NoError(t, err, "Error deleting namespace test-ns")
}

func createResourceTransformation(t *testing.T, namespace string) {
	_, err := storkops.Instance().CreateResourceTransformation(&storkv1.ResourceTransformation{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-rt",
			Namespace: namespace,
		},
		Spec: storkv1.ResourceTransformationSpec{
			Objects: []storkv1.TransformSpecs{
				{
					Resource: "/v1/Service",
					Paths: []storkv1.ResourcePaths{
						{
							Path:      "spec.type",
							Value:     "LoadBalancer",
							Type:      "string",
							Operation: "modify",
						},
					},
				}},
		},
	})
	require.NoError(t, err, "Error creating Resource Transformation")
}

func createNamespace(t *testing.T) {
	namespace := "test-ns"
	_, err := core.Instance().CreateNamespace(&v1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name:   namespace,
			Labels: map[string]string{"nsKey": "value"},
		},
	})
	require.NoError(t, err, "Error creating Namespace")
}
