//go:build integrationtest
// +build integrationtest

package integrationtest

import (
	"bytes"
	"fmt"
	"os"
	"path"
	"strings"
	"testing"

	storkv1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/libopenstorage/stork/pkg/log"
	"github.com/libopenstorage/stork/pkg/storkctl"
	"github.com/portworx/sched-ops/k8s/core"
	storkops "github.com/portworx/sched-ops/k8s/stork"
)

const (
	asyncDR string = "async-dr"
	syncDR  string = "sync-dr"
)

var kubeConfigPath string

func TestStorkCtlActions(t *testing.T) {
	// running the actions in source cluster since we only need to create the action resource not execute it
	err := setSourceKubeConfig()
	log.FailOnError(t, err, "failed to set kubeconfig to source cluster: %v", err)
	kubeConfigPath , err = getDestinationKubeConfigFile()
	log.FailOnError(t, err, "Error getting destination kubeconfig file")
	err = setDestinationKubeConfig()
	log.FailOnError(t, err, "failed to set kubeconfig to destination cluster: %v", err)
	currentTestSuite = t.Name()
	createClusterPairs(t)
	defer cleanUpClusterPairs(t)
	t.Run("createDefaultFailoverActionTest", createDefaultFailoverActionTest)
}

func createDefaultFailoverActionTest(t *testing.T){
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
					FailoverNamespaces:         []string{defaultNs, adminNs},
					MigrationScheduleReference: migrationScheduleName,
					SkipSourceOperations:       &skipSourceOperations,
				},
			},
			ActionType: storkv1.ActionTypeFailover,
		},
	}
	createDRActionTest(t, testrailId, cmdArgs, expectedAction, migrationScheduleName, asyncDR, actionType, namespace)
}

func createDRActionTest(t *testing.T, testrailID int, args map[string]string, expectedAction *storkv1.Action, migrationScheduleName string,
	clusterPairMode string, actionType string, namespace string) {
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

func createDRAction(t *testing.T, namespace string, actionType string, migrationScheduleName string, customArgs map[string]string) (string, []string) {
	factory := storkctl.NewFactory()
	var outputBuffer bytes.Buffer
	cmd := storkctl.NewCommand(factory, os.Stdin, &outputBuffer, os.Stderr)
	cmdArgs := []string{"perform", actionType, "--kubeconfig", kubeConfigPath}
	executeStorkCtlCommand(t, cmd, cmdArgs, customArgs)
	// Get the captured output as a string
	actualOutput := outputBuffer.String()
	log.InfoD("Actual output is: %s", actualOutput)
	expectedOutput := fmt.Sprintf("Started failover for MigrationSchedule %s/%s", namespace, migrationScheduleName)
	// Break the actual output into lines
	output := strings.Split(actualOutput, "\n")
	// Check if the first line in output is as expected
	Dash.VerifyFatal(t, output[0], expectedOutput, "Action creation failed")
	// Extract the get status command from the output
	getStatusCommand := strings.TrimSpace(strings.TrimPrefix(output[1], "To check failover status use the command : `"))
	getStatusCommand = strings.TrimSuffix(getStatusCommand, "`")
	getStatusCmdArgs := strings.Split(getStatusCommand, " ")
	// Extract the action Name from the command args
	actionName := getStatusCmdArgs[3]
	// returning [1:] because 0th index is storkctl, which is not needed
	return actionName, getStatusCmdArgs[1:]
}

func getDRActionStatus(t *testing.T, actionType string, actionName string, actionNamespace string) (string, string, string){
	factory := storkctl.NewFactory()
	var outputBuffer bytes.Buffer
	cmd := storkctl.NewCommand(factory, os.Stdin, &outputBuffer, os.Stderr)
	cmdArgs := []string{"get", actionType, actionName, "-n", actionNamespace, "--kubeconfig", kubeConfigPath}
	executeStorkCtlCommand(t, cmd, cmdArgs, nil)
	// Get the captured output as a string
	actualOutput := outputBuffer.String()
	log.InfoD("Actual output is: %s", actualOutput)
	// Break the actual output into lines
	output := strings.Split(actualOutput, "\n")
	Dash.VerifyFatal(t, len(output), 3, "Action status command failed")
	// Extract the useful fields
	fields := strings.Fields(output[1])
    
    // Join the fields back together to form the columns
	// "failover-ms-4-2024-04-02-135725   02 Apr 24 13:57 UTC   Completed   Successful   Scaled up Apps in : 1/1 namespaces"
	// CREATED timestamp is index [1:5]
    name := fields[0]
    currentStage := fields[6]
	// status and moreInfo fields can be empty
	currentStatus := ""
	moreInfo := ""
	if len(fields) > 7 {
		currentStatus = fields[7]
	}
	if len(fields) > 8 {
		moreInfo = strings.Join(fields[8:], " ")
	}
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
	Dash.VerifyFatal(t, *actualAction.Spec.ActionParameter.FailoverParameter.SkipSourceOperations, *expectedAction.Spec.ActionParameter.FailoverParameter.SkipSourceOperations, "Action FailoverParameter SkipSourceOperations")
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

func getDestinationKubeConfigFile() (string, error) {
	destKubeconfigPath := path.Join("/tmp", "dest_kubeconfig")
	cm, err := core.Instance().GetConfigMap("destinationconfigmap", "kube-system")
	if err != nil {
		log.Error("Error reading config map: %v", err)
		return "", err
	}
	config := cm.Data["kubeconfig"]
	if len(config) == 0 {
		configErr := "Error reading kubeconfig: found empty remoteConfig in config map"
		return "", fmt.Errorf(configErr)
	}
	// dump to remoteFilePath
	err = os.WriteFile(destKubeconfigPath, []byte(config), 0644)
	return destKubeconfigPath, err
}
