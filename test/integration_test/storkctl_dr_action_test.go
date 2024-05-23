//go:build integrationtest
// +build integrationtest

package integrationtest

import (
	"bytes"
	"fmt"
	"os"
	"strings"
	"testing"

	storkv1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/libopenstorage/stork/pkg/log"
	"github.com/libopenstorage/stork/pkg/storkctl"
	"github.com/libopenstorage/stork/pkg/utils"
	storkops "github.com/portworx/sched-ops/k8s/stork"
)

const (
	asyncDR string = "async-dr"
	syncDR  string = "sync-dr"
)

var destinationKubeConfigPath, srcKubeConfigPath string

func TestStorkCtlAction(t *testing.T) {
	err := setSourceKubeConfig()
	log.FailOnError(t, err, "failed to set kubeconfig to source cluster: %v", err)
	// get the destination kubeconfig from configmap in source cluster so that it can be passed to storkctl commands
	// since both the dr cli commands run in destination cluster
	destinationKubeConfigPath, err = utils.GetDestinationKubeConfigFile()
	log.FailOnError(t, err, "Error getting destination kubeconfig file")
	err = setDestinationKubeConfig()
	log.FailOnError(t, err, "failed to set kubeconfig to destination cluster: %v", err)
	currentTestSuite = t.Name()
	createClusterPairs(t)
	defer cleanUpClusterPairs(t)
	t.Run("createDefaultFailoverActionTest", createDefaultFailoverActionTest)
	t.Run("createFailoverActionWithIncludeNsTest", createFailoverActionWithIncludeNsTest)
	t.Run("createFailoverActionWithExcludeNsTest", createFailoverActionWithExcludeNsTest)
	t.Run("createFailoverActionWithSkipSourceOperationsTest", createFailoverActionWithSkipSourceOperationsTest)
	t.Run("createDefaultFailbackActionTest", createDefaultFailbackActionTest)
	t.Run("createFailbackActionWithIncludeNsTest", createFailbackActionWithIncludeNsTest)
	t.Run("createFailbackActionWithExcludeNsTest", createFailbackActionWithExcludeNsTest)
}

func createDefaultFailoverActionTest(t *testing.T) {
	testrailId := 257169
	actionType := storkv1.ActionTypeFailover
	// the dummy migrationSchedule created migrates defaultNs and adminNs
	migrationScheduleName := "test-action-automation-migration-schedule"
	namespace := adminNs
	cmdArgs := map[string]string{
		"migration-reference": migrationScheduleName,
		"namespace":           namespace,
	}
	expectedAction := generateDRActionObject(storkv1.ActionTypeFailover, migrationScheduleName, []string{defaultNs, adminNs}, false)
	createDRActionTest(t, testrailId, cmdArgs, expectedAction, migrationScheduleName, asyncDR, actionType, namespace)
}

func createFailoverActionWithIncludeNsTest(t *testing.T) {
	testrailId := 257170
	actionType := storkv1.ActionTypeFailover
	// the dummy migrationSchedule created migrates defaultNs and adminNs
	migrationScheduleName := "test-action-automation-migration-schedule"
	namespace := adminNs
	cmdArgs := map[string]string{
		"migration-reference": migrationScheduleName,
		"include-namespaces":  defaultNs,
		"namespace":           namespace,
	}
	expectedAction := generateDRActionObject(storkv1.ActionTypeFailover, migrationScheduleName, []string{defaultNs}, false)
	createDRActionTest(t, testrailId, cmdArgs, expectedAction, migrationScheduleName, asyncDR, actionType, namespace)
}

func createFailoverActionWithExcludeNsTest(t *testing.T) {
	testrailId := 296305
	actionType := storkv1.ActionTypeFailover
	// the dummy migrationSchedule created migrates defaultNs and adminNs
	migrationScheduleName := "test-action-automation-migration-schedule"
	namespace := adminNs
	cmdArgs := map[string]string{
		"migration-reference": migrationScheduleName,
		"exclude-namespaces":  adminNs,
		"namespace":           namespace,
	}
	expectedAction := generateDRActionObject(storkv1.ActionTypeFailover, migrationScheduleName, []string{defaultNs}, false)
	createDRActionTest(t, testrailId, cmdArgs, expectedAction, migrationScheduleName, asyncDR, actionType, namespace)
}

func createFailoverActionWithSkipSourceOperationsTest(t *testing.T) {
	testrailId := 257171
	actionType := storkv1.ActionTypeFailover
	// the dummy migrationSchedule created migrates defaultNs and adminNs
	migrationScheduleName := "test-action-automation-migration-schedule"
	namespace := adminNs
	cmdArgs := map[string]string{
		"migration-reference":    migrationScheduleName,
		"skip-source-operations": "",
		"namespace":              namespace,
	}
	expectedAction := generateDRActionObject(storkv1.ActionTypeFailover, migrationScheduleName, []string{defaultNs, adminNs}, true)
	createDRActionTest(t, testrailId, cmdArgs, expectedAction, migrationScheduleName, asyncDR, actionType, namespace)
}

func createDefaultFailbackActionTest(t *testing.T) {
	testrailId := 257158
	actionType := storkv1.ActionTypeFailback
	// the dummy migrationSchedule created migrates defaultNs and adminNs
	migrationScheduleName := "test-action-automation-migration-schedule"
	namespace := adminNs
	cmdArgs := map[string]string{
		"migration-reference": migrationScheduleName,
		"namespace":           namespace,
	}
	expectedAction := generateDRActionObject(storkv1.ActionTypeFailback, migrationScheduleName, []string{defaultNs, adminNs}, false)
	createDRActionTest(t, testrailId, cmdArgs, expectedAction, migrationScheduleName, asyncDR, actionType, namespace)
}

func createFailbackActionWithIncludeNsTest(t *testing.T) {
	testrailId := 257159
	actionType := storkv1.ActionTypeFailback
	// the dummy migrationSchedule created migrates defaultNs and adminNs
	migrationScheduleName := "test-action-automation-migration-schedule"
	namespace := adminNs
	cmdArgs := map[string]string{
		"migration-reference": migrationScheduleName,
		"include-namespaces":  defaultNs,
		"namespace":           namespace,
	}
	expectedAction := generateDRActionObject(storkv1.ActionTypeFailback, migrationScheduleName, []string{defaultNs}, false)
	createDRActionTest(t, testrailId, cmdArgs, expectedAction, migrationScheduleName, asyncDR, actionType, namespace)
}

func createFailbackActionWithExcludeNsTest(t *testing.T) {
	testrailId := 296304
	actionType := storkv1.ActionTypeFailback
	// the dummy migrationSchedule created migrates defaultNs and adminNs
	migrationScheduleName := "test-action-automation-migration-schedule"
	namespace := adminNs
	cmdArgs := map[string]string{
		"migration-reference": migrationScheduleName,
		"exclude-namespaces":  adminNs,
		"namespace":           namespace,
	}
	expectedAction := generateDRActionObject(storkv1.ActionTypeFailback, migrationScheduleName, []string{defaultNs}, false)
	createDRActionTest(t, testrailId, cmdArgs, expectedAction, migrationScheduleName, asyncDR, actionType, namespace)
}

func createDRActionTest(t *testing.T, testrailID int, args map[string]string, expectedAction *storkv1.Action, migrationScheduleName string,
	clusterPairMode string, actionType storkv1.ActionType, namespace string) {
	var testResult = testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	defer migrationScheduleCleanup(t, migrationScheduleName, namespace)
	defer updateDashStats(t.Name(), &testResult)

	// Create the required migrationschedule in the given namespace
	createMigrationSchedule(t, migrationScheduleName, namespace, clusterPairMode)
	// Create the DR Action in the given namespace and verify output
	drActionName, _ := createDRAction(t, namespace, actionType, migrationScheduleName, args)
	// validate the action object created
	err := ValidateAction(t, expectedAction, drActionName, namespace)
	log.FailOnError(t, err, "Error validating the created resource")

	// try to get the status as well
	_, _, _ = getDRActionStatus(t, actionType, drActionName, namespace)
	// Cleanup the created action resource
	err = storkops.Instance().DeleteAction(drActionName, namespace)
	log.FailOnError(t, err, "Error deleting the created action resource")
	// If we are here then the test has passed
	testResult = testResultPass
	log.InfoD("Test status at end of %s test: %s", t.Name(), testResult)
}

func createDRAction(t *testing.T, namespace string, actionType storkv1.ActionType, migrationScheduleName string, customArgs map[string]string) (string, []string) {
	factory := storkctl.NewFactory()
	var outputBuffer bytes.Buffer
	cmd := storkctl.NewCommand(factory, os.Stdin, &outputBuffer, os.Stderr)
	cmdArgs := []string{"perform", string(actionType), "--kubeconfig", destinationKubeConfigPath}
	executeStorkCtlCommand(t, cmd, cmdArgs, customArgs)
	// Get the captured output as a string
	actualOutput := outputBuffer.String()
	log.InfoD("Actual output is: %s", actualOutput)
	expectedOutput := fmt.Sprintf("Started %s for MigrationSchedule %s/%s", actionType, namespace, migrationScheduleName)
	// Break the actual output into lines
	output := strings.Split(actualOutput, "\n")
	// Check if the first line in output is as expected
	Dash.VerifyFatal(t, output[0], expectedOutput, "Action creation failed")
	// Extract the get status command from the output
	prefix := fmt.Sprintf("To check %s status use the command : `", actionType)
	getStatusCommand := strings.TrimSpace(strings.TrimPrefix(output[1], prefix))
	getStatusCommand = strings.TrimSuffix(getStatusCommand, "`")
	getStatusCmdArgs := strings.Split(getStatusCommand, " ")
	// Extract the action Name from the command args
	actionName := getStatusCmdArgs[3]
	// returning [1:] because 0th index is storkctl, which is not needed
	return actionName, getStatusCmdArgs[1:]
}

func getDRActionStatus(t *testing.T, actionType storkv1.ActionType, actionName string, actionNamespace string) (string, string, string) {
	factory := storkctl.NewFactory()
	var outputBuffer bytes.Buffer
	cmd := storkctl.NewCommand(factory, os.Stdin, &outputBuffer, os.Stderr)
	cmdArgs := []string{"get", string(actionType), actionName, "-n", actionNamespace, "--kubeconfig", destinationKubeConfigPath}
	executeStorkCtlCommand(t, cmd, cmdArgs, nil)
	// Get the captured output as a string
	actualOutput := outputBuffer.String()
	log.InfoD("Actual output is: %s", actualOutput)
	output := strings.Split(actualOutput, "\n")
	Dash.VerifyFatal(t, len(output), 3, "Action status command failed")
	// NAME                                                                   CREATED               STAGE       STATUS       MORE INFO
	// failback-reverse-elastic-2024-04-17-111052                             17 Apr 24 11:10 UTC   Completed   Successful   Scaled up Apps in : 1/1 namespaces

	// Extract the start index of each field
	startIndex := make(map[string]int)
	columns := []string{"NAME", "CREATED", "STAGE", "STATUS", "MORE INFO"}
	for _, column := range columns {
		startIndex[column] = strings.Index(output[0], column)
	}
	// Extract the value of each column
	name := strings.TrimSpace(output[1][startIndex["NAME"]:startIndex["CREATED"]])
	currentStage := strings.TrimSpace(output[1][startIndex["STAGE"]:startIndex["STATUS"]])
	currentStatus := strings.TrimSpace(output[1][startIndex["STATUS"]:startIndex["MORE INFO"]])
	moreInfo := strings.TrimSpace(output[1][startIndex["MORE INFO"]:])
	Dash.VerifyFatal(t, name, actionName, "Action Name")
	log.InfoD("Action %s status fields are: %s;%s;%s;", name, currentStage, currentStatus, moreInfo)
	return currentStage, currentStatus, moreInfo
}

func ValidateAction(t *testing.T, expectedAction *storkv1.Action, actionName string, actionNamespace string) error {
	actualAction, err := storkops.Instance().GetAction(actionName, actionNamespace)
	log.FailOnError(t, err, "Unable to get the created action resource")
	log.Info("Trying to validate the action resource")
	Dash.VerifyFatal(t, actualAction.Spec.ActionType, expectedAction.Spec.ActionType, "Action ActionType")
	Dash.VerifyFatal(t, actualAction.Spec.ActionParameter.FailoverParameter.FailoverNamespaces, expectedAction.Spec.ActionParameter.FailoverParameter.FailoverNamespaces, "Action FailoverParameter FailoverNamespaces")
	Dash.VerifyFatal(t, actualAction.Spec.ActionParameter.FailoverParameter.MigrationScheduleReference, expectedAction.Spec.ActionParameter.FailoverParameter.MigrationScheduleReference, "Action FailoverParameter MigrationScheduleReference")
	if expectedAction.Spec.ActionType == storkv1.ActionTypeFailover {
		Dash.VerifyFatal(t, *actualAction.Spec.ActionParameter.FailoverParameter.SkipSourceOperations, *expectedAction.Spec.ActionParameter.FailoverParameter.SkipSourceOperations, "Action FailoverParameter SkipSourceOperations")
	}
	Dash.VerifyFatal(t, actualAction.Spec.ActionParameter.FailbackParameter.FailbackNamespaces, expectedAction.Spec.ActionParameter.FailbackParameter.FailbackNamespaces, "Action FailbackParameter FailbackNamespaces")
	Dash.VerifyFatal(t, actualAction.Spec.ActionParameter.FailbackParameter.MigrationScheduleReference, expectedAction.Spec.ActionParameter.FailbackParameter.MigrationScheduleReference, "Action FailbackParameter MigrationScheduleReference")
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
	migrationScheduleObj.Annotations = map[string]string{"stork.libopenstorage.org/static-copy": "true"}
	migrationScheduleObj.Name = migrationScheduleName
	migrationScheduleObj.Namespace = namespace
	_, err := storkops.Instance().CreateMigrationSchedule(migrationScheduleObj)
	log.FailOnError(t, err, "Error creating test migrationSchedule")
}

func generateDRActionObject(actionType storkv1.ActionType, migrationScheduleName string, namespaces []string, skipSourceOperations bool) *storkv1.Action {
	action := storkv1.Action{
		Spec: storkv1.ActionSpec{
			ActionParameter: storkv1.ActionParameter{},
			ActionType:      actionType,
		},
	}
	if actionType == storkv1.ActionTypeFailback {
		action.Spec.ActionParameter.FailbackParameter = storkv1.FailbackParameter{
			FailbackNamespaces:         namespaces,
			MigrationScheduleReference: migrationScheduleName,
		}
	} else if actionType == storkv1.ActionTypeFailover {
		action.Spec.ActionParameter.FailoverParameter = storkv1.FailoverParameter{
			FailoverNamespaces:         namespaces,
			MigrationScheduleReference: migrationScheduleName,
			SkipSourceOperations:       &skipSourceOperations,
		}
	}
	return &action
}
