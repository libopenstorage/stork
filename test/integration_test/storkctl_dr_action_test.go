//go:build integrationtest
// +build integrationtest

package integrationtest

import (
	"bytes"
	"fmt"
	storkv1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/libopenstorage/stork/pkg/storkctl"
	storkops "github.com/portworx/sched-ops/k8s/stork"
	"os"
	"testing"
	"time"

	"github.com/libopenstorage/stork/pkg/log"
)

const (
	asyncDR string = "async-dr"
	syncDR  string = "sync-dr"
)

func TestStorkCtlActions(t *testing.T) {
	// running the actions in source cluster since we only need to create the action resource not execute it
	err := setSourceKubeConfig()
	log.FailOnError(t, err, "failed to set kubeconfig to source cluster: %v", err)
	mockNow := time.Date(2024, time.January, 1, 0, 0, 0, 0, time.Local)
	err = setMockTime(&mockNow)
	log.FailOnError(t, err, "Error setting mock time")
	currentTestSuite = t.Name()
	createClusterPairs(t)
	defer cleanUpClusterPairs(t)
	t.Run("createDefaultFailoverActionTest", createDefaultFailoverActionTest)
	err = setRemoteConfig("")
	log.FailOnError(t, err, "setting kubeconfig to default failed")
	err = setMockTime(nil)
	log.FailOnError(t, err, "Error resetting mock time")
}

func createDefaultFailoverActionTest(t *testing.T) {
	testrailId := 257169
	actionType := "failover"
	// the dummy migrationSchedule created migrates defaultNs and adminNs
	migrationScheduleName := "test-action-automation-migration-schedule"
	namespace := adminNs
	cmdArgs := map[string]string{
		"migration-reference": migrationScheduleName,
		"namespace":           namespace,
	}
	skipSourceOperations := false
	expectedAction := &storkv1.Action{
		Spec: storkv1.ActionSpec{
			ActionParameter: storkv1.ActionParameter{
				FailoverParameter: storkv1.FailoverParameter{
					FailoverNamespaces:         []string{adminNs, defaultNs},
					MigrationScheduleReference: migrationScheduleName,
					SkipSourceOperations:       &skipSourceOperations,
				},
			},
			ActionType: storkv1.ActionTypeFailover,
		},
	}
	createDRActionTest(t, testrailId, cmdArgs, expectedAction, migrationScheduleName, asyncDR, actionType, namespace)
}

func createDRActionTest(t *testing.T, testrailID int, args map[string]string, expectedAction *storkv1.Action ,migrationScheduleName string,
	clusterPairMode string, actionType string, namespace string) {
	var testResult = testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	defer migrationScheduleCleanup(t, migrationScheduleName, namespace)
	defer updateDashStats(t.Name(), &testResult)

	// Create the required migrationschedule in the given namespace
	createMigrationSchedule(t, migrationScheduleName, namespace, clusterPairMode)
	// Create the DR Action in the given namespace and verify output
	createDRAction(t, namespace, actionType, migrationScheduleName, args)
	failoverActionName := "failover-" + migrationScheduleName + "-2024-01-01-000000"
	// validate the action object created
	err := ValidateAction(t, expectedAction, failoverActionName, namespace)
	log.FailOnError(t, err, "Error validating the created resource")

	// If we are here then the test has passed
	testResult = testResultPass
	log.InfoD("Test status at end of %s test: %s", t.Name(), testResult)
}

func createDRAction(t *testing.T, namespace string, actionType string, migrationScheduleName string, customArgs map[string]string) {
	factory := storkctl.NewFactory()
	var outputBuffer bytes.Buffer
	cmd := storkctl.NewCommand(factory, os.Stdin, &outputBuffer, os.Stderr)
	failoverActionName := "failover-" + migrationScheduleName + "-2024-01-01-000000"
	cmdArgs := []string{"perform", actionType}
	executeStorkCtlCommand(t, cmd, cmdArgs, customArgs)
	// Get the captured output as a string
	actualOutput := outputBuffer.String()
	log.InfoD("Actual output is: %s", actualOutput)
	expectedOutput := fmt.Sprintf("Started failover for MigrationSchedule %s/%s\nTo check failover status use the command : `storkctl get failover %v -n %s`\n", namespace, migrationScheduleName, failoverActionName, namespace)
	Dash.VerifyFatal(t, expectedOutput, actualOutput, "Output mismatch")
}

func ValidateAction(t *testing.T, expectedAction *storkv1.Action, actionName string, actionNamespace string) error {
	actualAction, err := storkops.Instance().GetAction(actionName, actionNamespace)
	log.FailOnError(t, err, "Unable to get the created action resource")
	log.Info("Trying to validate the action resource")
	Dash.VerifyFatal(t, actualAction.Spec.ActionType, expectedAction.Spec.ActionType, "Action ActionType")
	Dash.VerifyFatal(t, actualAction.Spec.ActionParameter.FailoverParameter, expectedAction.Spec.ActionParameter.FailoverParameter, "Action FailoverParameter")
	Dash.VerifyFatal(t, actualAction.Spec.ActionParameter.FailbackParameter, expectedAction.Spec.ActionParameter.FailbackParameter, "Action FailbackParameter")
	return nil
}

func createMigrationSchedule(t *testing.T, migrationScheduleName string, namespace string, clusterPairMode string) {
	log.Info("Creating a test migrationSchedule resource")
	clusterPair := asyncDrAdminClusterPair
	if clusterPairMode == syncDR {
		clusterPair = syncDrAdminClusterPair
	}
	migrationScheduleObj := &storkv1.MigrationSchedule{
		Spec: storkv1.MigrationScheduleSpec{
			SchedulePolicyName: "default-migration-policy",
			Template: storkv1.MigrationTemplateSpec{
				Spec: storkv1.MigrationSpec{
					ClusterPair: clusterPair,
					Namespaces:  []string{defaultNs, adminNs},
				},
			},
		},
	}
	migrationScheduleObj.Annotations = map[string]string{"stork.libopenstorage.org/static-copy":"true"}
	migrationScheduleObj.Name = migrationScheduleName
	migrationScheduleObj.Namespace = namespace
	_, err := storkops.Instance().CreateMigrationSchedule(migrationScheduleObj)
	log.FailOnError(t, err, "Error creating test migrationSchedule")
}
