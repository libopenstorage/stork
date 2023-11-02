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
	storkops "github.com/portworx/sched-ops/k8s/stork"
	"github.com/portworx/sched-ops/task"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/yaml"
)

func TestStorkCtlSchedulePolicy(t *testing.T) {
	err := setSourceKubeConfig()
	require.NoError(t, err, "failed to set kubeconfig to source cluster: %v", err)
	t.Run("createDefaultIntervalSchedulePolicyTest", createDefaultIntervalSchedulePolicyTest)
	t.Run("createDefaultDailySchedulePolicyTest", createDefaultDailySchedulePolicyTest)
	t.Run("createDefaultWeeklySchedulePolicyTest", createDefaultWeeklySchedulePolicyTest)
	t.Run("createDefaultMonthlySchedulePolicyTest", createDefaultMonthlySchedulePolicyTest)
	t.Run("createCustomIntervalSchedulePolicyTest", createCustomIntervalSchedulePolicyTest)
	t.Run("createCustomDailySchedulePolicyTest", createCustomDailySchedulePolicyTest)
	t.Run("createCustomWeeklySchedulePolicyTest", createCustomWeeklySchedulePolicyTest)
	t.Run("createCustomMonthlySchedulePolicyTest", createCustomMonthlySchedulePolicyTest)
	t.Run("deleteSchedulePolicyTest", deleteSchedulePolicyTest)
	err = setRemoteConfig("")
	require.NoError(t, err, "setting kubeconfig to default failed")
}

func createSchedulePolicyTest(t *testing.T, policyType string, args map[string]string, specFileName string, testrailID int) {
	schedulePolicyName := "automation-test-policy"
	var testResult = testResultFail
	runID := testrailSetupForTest(testrailID, &testResult)
	defer updateTestRail(&testResult, testrailID, runID)
	defer SchedulePolicyCleanup(t, schedulePolicyName)

	factory := storkctl.NewFactory()
	var outputBuffer bytes.Buffer
	cmd := storkctl.NewCommand(factory, os.Stdin, &outputBuffer, os.Stderr)
	cmdArgs := []string{"create", "schedulepolicy", schedulePolicyName, "-t", policyType}
	//add the custom args to the command if any
	for key, value := range args {
		cmdArgs = append(cmdArgs, "--"+key)
		cmdArgs = append(cmdArgs, value)
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
	logrus.Infof("Actual output is: %s\n", actualOutput)
	expectedOutput := fmt.Sprintf("Schedule policy %v created successfully\n", schedulePolicyName)
	require.Equal(t, expectedOutput, actualOutput)

	//Validate the created resource
	specFile := "specs/storkctl-specs/schedulepolicy/" + specFileName
	err := ValidateSchedulePolicyFromFile(t, specFile, schedulePolicyName)
	require.NoError(t, err, "Error validating the created resource")

	// If we are here then the test has passed
	testResult = testResultPass
	logrus.Infof("Test status at end of %s test: %s", t.Name(), testResult)
}

func deleteSchedulePolicyTest(t *testing.T) {
	var testrailID, testResult = 93195, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult)
	defer updateTestRail(&testResult, testrailID, runID)
	factory := storkctl.NewFactory()
	schedulePolicyName := "delete-test-policy"
	schedulePolicy := &storkv1.SchedulePolicy{
		ObjectMeta: meta.ObjectMeta{
			Name: schedulePolicyName,
		},
		Policy: storkv1.SchedulePolicyItem{
			Monthly: &storkv1.MonthlyPolicy{
				Date: 30,
				Time: "12:15PM",
			},
		},
	}
	_, err := storkops.Instance().CreateSchedulePolicy(schedulePolicy)
	if err != nil {
		logrus.Errorf("Unable to create schedule policy %v", schedulePolicyName)
	}

	var outputBuffer bytes.Buffer
	cmd := storkctl.NewCommand(factory, os.Stdin, &outputBuffer, os.Stderr)
	cmdArgs := []string{"delete", "schedulepolicy", schedulePolicyName}
	cmd.SetArgs(cmdArgs)
	logrus.Infof("The command being executed is %v", cmdArgs)
	if err := cmd.Execute(); err != nil {
		logrus.Errorf("Storkctl execution failed: %v", err)
		return
	}
	actualOutput := outputBuffer.String()
	logrus.Infof("Actual output is: %s", actualOutput)
	expectedOutput := fmt.Sprintf("Schedule policy %v deleted successfully\n", schedulePolicyName)
	require.Equal(t, expectedOutput, actualOutput)

	//validate that the schedule policy is actually deleted
	_, err = storkops.Instance().GetSchedulePolicy(schedulePolicyName)
	expectedErrorMsg := fmt.Sprintf("schedulepolicies.stork.libopenstorage.org \"%s\" not found", schedulePolicyName)
	require.Equal(t, expectedErrorMsg, err.Error())

	testResult = testResultPass
	logrus.Infof("Test status at end of %s test: %s", t.Name(), testResult)
}

func createDefaultIntervalSchedulePolicyTest(t *testing.T) {
	testrailID := 93187
	createSchedulePolicyTest(t, "Interval", nil, "interval-policy.yaml", testrailID)
}

func createDefaultDailySchedulePolicyTest(t *testing.T) {
	testrailID := 93188
	createSchedulePolicyTest(t, "Daily", nil, "daily-policy.yaml", testrailID)
}

func createDefaultWeeklySchedulePolicyTest(t *testing.T) {
	testrailID := 93189
	createSchedulePolicyTest(t, "Weekly", nil, "weekly-policy.yaml", testrailID)
}

func createDefaultMonthlySchedulePolicyTest(t *testing.T) {
	testrailID := 93190
	createSchedulePolicyTest(t, "Monthly", nil, "monthly-policy.yaml", testrailID)
}

func createCustomIntervalSchedulePolicyTest(t *testing.T) {
	testrailID := 93191
	args := map[string]string{
		"interval-minutes": "15",
		"retain":           "5",
	}
	createSchedulePolicyTest(t, "Interval", args, "custom-interval-policy.yaml", testrailID)
}

func createCustomDailySchedulePolicyTest(t *testing.T) {
	testrailID := 93192
	args := map[string]string{
		"force-full-snapshot-day": "Wednesday",
		"time":                    "4:00PM",
		"retain":                  "2",
	}
	createSchedulePolicyTest(t, "Daily", args, "custom-daily-policy.yaml", testrailID)
}

func createCustomWeeklySchedulePolicyTest(t *testing.T) {
	testrailID := 93193
	args := map[string]string{
		"day-of-week": "Friday",
		"time":        "2:00AM",
		"retain":      "3",
	}
	createSchedulePolicyTest(t, "Weekly", args, "custom-weekly-policy.yaml", testrailID)
}

func createCustomMonthlySchedulePolicyTest(t *testing.T) {
	testrailID := 93194
	args := map[string]string{
		"date-of-month": "15",
		"time":          "11:00AM",
		"retain":        "3",
	}
	createSchedulePolicyTest(t, "Monthly", args, "custom-monthly-policy.yaml", testrailID)
}

func ValidateSchedulePolicyFromFile(t *testing.T, specFile string, policyName string) error {
	data, err := getByteDataFromFile(specFile)
	if err != nil {
		return err
	}
	policy := &storkv1.SchedulePolicy{}
	dec := yaml.NewYAMLOrJSONDecoder(bytes.NewReader([]byte(data)), len(data))
	if err := dec.Decode(&policy); err != nil {
		return err
	}

	if err := ValidateSchedulePolicy(t, policy, policyName); err != nil {
		return err
	}
	return nil
}

func ValidateSchedulePolicy(t *testing.T, schedulePolicy *storkv1.SchedulePolicy, schedulePolicyName string) error {
	//We want to validate if the created schedule policy resource matches our expectations
	actualPolicy, err := storkops.Instance().GetSchedulePolicy(schedulePolicyName)
	if err != nil {
		logrus.Errorf("Unable to get the schedule policy")
		return err
	}
	logrus.Info("Trying to validate the created policy")
	//Validating schedulePolicy.Policy because schedulePolicy.metadata cannot be validated
	require.Equal(t, schedulePolicy.Policy, actualPolicy.Policy, "Created schedule policy doesn't match the expected specification")
	return nil
}

func SchedulePolicyCleanup(t *testing.T, policyName string) {
	logrus.Info("Cleaning up created resources")
	DeleteAndWaitForSchedulePolicyDeletion(t, policyName)
}

func DeleteAndWaitForSchedulePolicyDeletion(t *testing.T, name string) {
	err := storkops.Instance().DeleteSchedulePolicy(name)
	if err != nil {
		logrus.Errorf("Unable to delete schedule policy %s", name)
	}
	//check if the schedulePolicy is successfully deleted
	f := func() (interface{}, bool, error) {
		logrus.Infof("Checking if schedule policy resource is successfully deleted")
		_, err := storkops.Instance().GetSchedulePolicy(name)
		if err == nil {
			return "", true, fmt.Errorf("get schedule policy: %s should have failed", name)
		}
		if !errors.IsNotFound(err) {
			logrus.Infof("unexpected err: %v when checking deleted schedulePolicy: %s", err, name)
			return "", true, err
		}
		//deletion done
		logrus.Infof("Schedule policy %s successfully deleted", name)
		return "", false, nil
	}
	_, err = task.DoRetryWithTimeout(f, defaultWaitTimeout, 2*time.Second)
	require.NoError(t, err, "Unable to delete schedule policy %s", name)
}
