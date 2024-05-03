package storkctlcli

import (
	"bytes"
	"fmt"
	"os"
	"time"

	storkv1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/libopenstorage/stork/pkg/storkctl"
	storkops "github.com/portworx/sched-ops/k8s/stork"
	"github.com/portworx/sched-ops/task"
	"github.com/sirupsen/logrus"

	"github.com/portworx/torpedo/pkg/aetosutil"
)

var dash *aetosutil.Dashboard

const (
	drPrefix = "automation-"
	actionRetryTimeout = 10 * time.Minute
	actionRetryInterval = 10 * time.Second
)

var (
	migSchedNs = "kube-system"
)

func ScheduleStorkctlMigrationSched(schedName, clusterPair, namespace string, extraArgs map[string]string) error {
	if namespace != "" {
		migSchedNs = namespace 
	}
	cmdArgs := map[string]string{
		"cluster-pair": clusterPair,
		"namespace":   migSchedNs,
	}
	err := createMigrationScheduleCli(schedName, cmdArgs, extraArgs)
	return err
}

func createMigrationScheduleCli(schedName string, cmdArgs map[string]string, extraArgs map[string]string) error {
	factory := storkctl.NewFactory()
	var outputBuffer bytes.Buffer
	cmd := storkctl.NewCommand(factory, os.Stdin, &outputBuffer, os.Stderr)
	migCmdArgs := []string{"create", "migrationschedule", schedName}
	// add the custom args to the command
	for key, value := range cmdArgs {
		migCmdArgs = append(migCmdArgs, "--"+key)
		if value != "" {
			migCmdArgs = append(migCmdArgs, value)
		}
	}
	if extraArgs != nil {
		for key, value := range extraArgs {
			migCmdArgs = append(migCmdArgs, "--"+key)
			if value != "" {
				migCmdArgs = append(migCmdArgs, value)
			}
		}
	}
	cmd.SetArgs(migCmdArgs)
	// execute the command
	logrus.Infof("The storkctl command being executed is %v", migCmdArgs)
	if err := cmd.Execute(); err != nil {
		if err != nil {
			return fmt.Errorf("Error in executing create migration schedule command: %v", err)
		}
	}
	return nil
}

func PerformFailoverOrFailback(action, namespace, migSchdRef string, skipSourceOp bool, extraArgs map[string]string) (error, string) {
	failoverFailbackCmdArgs := []string{"perform", action, "--migration-reference", migSchdRef, "--namespace", migSchedNs}
	if namespace != "" {
		migSchedNs = namespace 
	}

	factory := storkctl.NewFactory()
	var outputBuffer bytes.Buffer
	cmd := storkctl.NewCommand(factory, os.Stdin, &outputBuffer, os.Stderr)
	if skipSourceOp && action == "failover" {
		failoverFailbackCmdArgs = append(failoverFailbackCmdArgs, "--skip-source-operations")
	}
	if extraArgs != nil {
		for key, value := range extraArgs {
			failoverFailbackCmdArgs = append(failoverFailbackCmdArgs, "--"+key)
			if value != "" {
				failoverFailbackCmdArgs = append(failoverFailbackCmdArgs, value)
			}
		}
	}
	cmd.SetArgs(failoverFailbackCmdArgs)
	// execute the command
	logrus.Infof("The storkctl command being executed is %v", failoverFailbackCmdArgs)
	if err := cmd.Execute(); err != nil {
		if err != nil {
			return fmt.Errorf("Error in executing perform %v command: %v", action, err), ""
		}
	}
	// Get the captured output as a string
	actualOutput := outputBuffer.String()
	logrus.Infof("Actual output is: %s", actualOutput)
	return nil, actualOutput
}

func GetDRActionStatus(actionName, actionNamespace string) (string, string, error) {
	var action *storkv1.Action
	action, err := storkops.Instance().GetAction(actionName, actionNamespace)
	if err != nil {
		return "", "", err
	}
	return string(action.Status.Status), string(action.Status.Stage), nil
}

// WaitForMigration - waits until all migrations in the given list are successful
func WaitForActionSuccessful(actionName string, actionNamespace string, timeoutScale int) error {
	checkMigrations := func() (interface{}, bool, error) {
		isComplete := true
		status, stage, err := GetDRActionStatus(actionName, actionNamespace)
		if err != nil {
			return "", false, err
		}
		if status != "Successful" || stage != "Final" {
			isComplete = false
		}
		if isComplete {
			return "", false, nil
		}
		return "", true, fmt.Errorf("Action status is %v waiting for successful status", status)
	}
	actionTimeout := actionRetryTimeout * time.Duration(timeoutScale)
	_, err := task.DoRetryWithTimeout(checkMigrations, actionTimeout, actionRetryInterval)
	return err
}
