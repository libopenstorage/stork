//go:build integrationtest
// +build integrationtest

package integrationtest

import (
	"bufio"
	"bytes"
	"fmt"
	storkv1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/libopenstorage/stork/pkg/storkctl"
	storkops "github.com/portworx/sched-ops/k8s/stork"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"io"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/yaml"
	"os"
	"testing"
	"time"
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

func createSchedulePolicyTest(t *testing.T, policyType string, args map[string]string, specFileName string) {
	factory := storkctl.NewFactory()
	schedulePolicyName := "automation-test-policy"
	var outputBuffer bytes.Buffer
	defer SchedulePolicyCleanup(t, schedulePolicyName)
	cmd := storkctl.NewCommand(factory, os.Stdin, &outputBuffer, os.Stderr)
	cmdArgs := []string{"create", "schedulepolicy", schedulePolicyName, "-t", policyType}
	//add the custom args to the command if any
	for key, value := range args {
		cmdArgs = append(cmdArgs, "--"+key)
		cmdArgs = append(cmdArgs, value)
	}
	cmd.SetArgs(cmdArgs)
	//execute the command
	if err := cmd.Execute(); err != nil {
		logrus.Infof("Execute storkctl failed: %v", err)
		return
	}
	// Get the captured output as a string
	actualOutput := outputBuffer.String()
	fmt.Printf("Actual output is: %s\n", actualOutput)
	expectedOutput := fmt.Sprintf("Schedule policy %v created successfully\n", schedulePolicyName)
	require.Equal(t, expectedOutput, actualOutput)

	//Validate the created resource
	specFile := "specs/storkctl-specs/schedulepolicy/" + specFileName
	err := ValidateSchedulePolicyFromFile(t, specFile, schedulePolicyName)
	require.NoError(t, err, "Error trying to validate the created resource")

	// If we are here then the test has passed
	testResult := testResultPass
	logrus.Infof("Test status at end of %s test: %s", t.Name(), testResult)
}

func deleteSchedulePolicyTest(t *testing.T) {
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
		logrus.Errorf("unable to create schedule policy %v", schedulePolicyName)
	}

	var outputBuffer bytes.Buffer
	cmd := storkctl.NewCommand(factory, os.Stdin, &outputBuffer, os.Stderr)
	cmdArgs := []string{"delete", "schedulepolicy", schedulePolicyName}
	cmd.SetArgs(cmdArgs)
	if err := cmd.Execute(); err != nil {
		logrus.Infof("Execute storkctl failed: %v", err)
		return
	}
	actualOutput := outputBuffer.String()
	fmt.Printf("Actual output is: %s\n", actualOutput)
	expectedOutput := fmt.Sprintf("Schedule policy %v deleted successfully\n", schedulePolicyName)
	require.Equal(t, expectedOutput, actualOutput)

	//validate that the schedule policy is actually deleted
	_, err = storkops.Instance().GetSchedulePolicy(schedulePolicyName)
	expectedErrorMsg := fmt.Sprintf("schedulepolicies.stork.libopenstorage.org \"%s\" not found", schedulePolicyName)
	require.Equal(t, expectedErrorMsg, err.Error())

	testResult := testResultPass
	logrus.Infof("Test status at end of %s test: %s", t.Name(), testResult)

}

func createDefaultIntervalSchedulePolicyTest(t *testing.T) {
	createSchedulePolicyTest(t, "Interval", nil, "interval-policy.yaml")
}

func createDefaultDailySchedulePolicyTest(t *testing.T) {
	createSchedulePolicyTest(t, "Daily", nil, "daily-policy.yaml")
}

func createDefaultWeeklySchedulePolicyTest(t *testing.T) {
	createSchedulePolicyTest(t, "Weekly", nil, "weekly-policy.yaml")
}

func createDefaultMonthlySchedulePolicyTest(t *testing.T) {
	createSchedulePolicyTest(t, "Monthly", nil, "monthly-policy.yaml")
}

func createCustomIntervalSchedulePolicyTest(t *testing.T) {
	args := map[string]string{"interval-minutes": "15", "retain": "5"}
	createSchedulePolicyTest(t, "Interval", args, "custom-interval-policy.yaml")
}

func createCustomDailySchedulePolicyTest(t *testing.T) {
	args := map[string]string{"force-full-snapshot-day": "Wednesday", "time": "4:00PM", "retain": "2"}
	createSchedulePolicyTest(t, "Daily", args, "custom-daily-policy.yaml")
}

func createCustomWeeklySchedulePolicyTest(t *testing.T) {
	args := map[string]string{"day-of-week": "Friday", "time": "2:00AM", "retain": "3"}
	createSchedulePolicyTest(t, "Weekly", args, "custom-weekly-policy.yaml")
}

func createCustomMonthlySchedulePolicyTest(t *testing.T) {
	args := map[string]string{"date-of-month": "15", "time": "11:00AM", "retain": "3"}
	createSchedulePolicyTest(t, "Monthly", args, "custom-monthly-policy.yaml")
}

func ValidateSchedulePolicyFromFile(t *testing.T, specFile string, policyName string) error {
	if specFile == "" {
		return fmt.Errorf("empty file path")
	}
	file, err := os.Open(specFile)
	if err != nil {
		return fmt.Errorf("error opening file %v: %v", specFile, err)
	}
	data, err := io.ReadAll(bufio.NewReader(file))
	if err != nil {
		return fmt.Errorf("error reading file %v: %v", specFile, err)
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
		logrus.Errorf("unable to get schedule policy")
		return err
	}
	logrus.Info("Trying to validate if the created policy is per expectations")
	//Validating schedulePolicy.Policy because schedulePolicy.metadata cannot be validated
	require.Equal(t, schedulePolicy.Policy, actualPolicy.Policy, "Created schedule policy doesn't match the expected specification provided")
	return nil
}

func SchedulePolicyCleanup(t *testing.T, policyName string) {
	fmt.Println("Cleaning up created resources")
	err := storkops.Instance().DeleteSchedulePolicy(policyName)
	if err != nil {
		logrus.Errorf("unable to delete schedule policy %v", policyName)
	}
	// time to let deletion finish
	time.Sleep(time.Second * 2)
}
