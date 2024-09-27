//go:build unittest
// +build unittest

package storkctl

import (
	"fmt"
	"testing"

	storkv1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	storkops "github.com/libopenstorage/stork/pkg/crud/stork"
	"github.com/stretchr/testify/require"
)

func TestGetFailoverFailbackNoAction(t *testing.T) {
	cmdArgs := []string{"get", "failover"}
	expected := "No resources found.\n"
	var actionList storkv1.ActionList
	testCommon(t, cmdArgs, &actionList, expected, false)

	cmdArgs = []string{"get", "failback"}
	testCommon(t, cmdArgs, &actionList, expected, false)
}

func TestGetFailoverSuccessfulFailoverAction(t *testing.T) {
	defer resetTest()
	// create a failover action
	actionName := createFailoverActionAndVerify(t)
	// update the status of the action
	actionObj, err := storkops.Instance().GetAction(actionName, "kube-system")
	require.NoError(t, err, "Error getting action")
	actionObj.Status.Stage = storkv1.ActionStageFinal
	actionObj.Status.Status = storkv1.ActionStatusSuccessful
	actionObj.Status.Summary = &storkv1.ActionSummary{FailoverSummaryItem: []*storkv1.FailoverSummary{
		{
			Namespace: "ns1",
			Status:    storkv1.ActionStatusSuccessful,
			Reason:    "scaling up apps in namespace ns1 successful",
		},
		{
			Namespace: "ns2",
			Status:    storkv1.ActionStatusSuccessful,
			Reason:    "scaling up apps in namespace ns2 successful",
		},
	}}
	actionObj.Status.Reason = ""
	_, err = storkops.Instance().UpdateAction(actionObj)
	require.NoError(t, err, "Error updating action")
	cmdArgs := []string{"get", "failover", actionName, "-n", "kube-system"}
	expected := "NAME                                                CREATED   STAGE       STATUS       MORE INFO\n" +
		"failover-test-migrationschedule-2024-01-01-000000             Completed   Successful   Scaled up Apps in : 2/2 namespaces\n"
	testCommon(t, cmdArgs, nil, expected, false)
}

func TestGetFailbackSuccessfulFailbackAction(t *testing.T) {
	defer resetTest()
	// create a failback action
	actionName := createFailbackActionAndVerify(t)
	// update the status of the action
	actionObj, err := storkops.Instance().GetAction(actionName, "kube-system")
	require.NoError(t, err, "Error getting action")
	actionObj.Status.Stage = storkv1.ActionStageFinal
	actionObj.Status.Status = storkv1.ActionStatusSuccessful
	actionObj.Status.Summary = &storkv1.ActionSummary{FailbackSummaryItem: []*storkv1.FailbackSummary{
		{
			Namespace: "ns1",
			Status:    storkv1.ActionStatusSuccessful,
			Reason:    "scaling up apps in namespace ns1 successful",
		},
		{
			Namespace: "ns2",
			Status:    storkv1.ActionStatusSuccessful,
			Reason:    "scaling up apps in namespace ns2 successful",
		},
	}}
	actionObj.Status.Reason = ""
	_, err = storkops.Instance().UpdateAction(actionObj)
	require.NoError(t, err, "Error updating action")
	cmdArgs := []string{"get", "failback", actionName, "-n", "kube-system"}
	expected := "NAME                                                CREATED   STAGE       STATUS       MORE INFO\n" +
		"failback-test-migrationschedule-2024-01-01-000000             Completed   Successful   Scaled up Apps in : 2/2 namespaces\n"
	testCommon(t, cmdArgs, nil, expected, false)
}

func TestGetFailoverAllStages(t *testing.T) {
	defer resetTest()
	// create a failover action
	actionName := createFailoverActionAndVerify(t)
	// update the status of the action
	actionObj, err := storkops.Instance().GetAction(actionName, "kube-system")
	require.NoError(t, err, "Error getting action")

	cmdArgs := []string{"get", "failover", actionName, "-n", "kube-system"}
	// Initial stage
	actionObj.Status.Stage = storkv1.ActionStageInitial
	actionObj.Status.Status = storkv1.ActionStatusInProgress
	actionObj.Status.Reason = ""
	_, err = storkops.Instance().UpdateAction(actionObj)
	require.NoError(t, err, "Error updating action")
	expected := "NAME                                                CREATED   STAGE         STATUS        MORE INFO\n" +
		"failover-test-migrationschedule-2024-01-01-000000             Validations   In-Progress   \n"
	testCommon(t, cmdArgs, nil, expected, false)

	// ScaleDownSource stage
	actionObj.Status.Stage = storkv1.ActionStageScaleDownSource
	actionObj.Status.Status = storkv1.ActionStatusInProgress
	actionObj.Status.Reason = ""
	_, err = storkops.Instance().UpdateAction(actionObj)
	require.NoError(t, err, "Error updating action")
	expected = "NAME                                                CREATED   STAGE                    STATUS        MORE INFO\n" +
		"failover-test-migrationschedule-2024-01-01-000000             Scale Down (on source)   In-Progress   \n"
	testCommon(t, cmdArgs, nil, expected, false)

	// WaitAfterScaleDown stage
	actionObj.Status.Stage = storkv1.ActionStageWaitAfterScaleDown
	actionObj.Status.Status = storkv1.ActionStatusInProgress
	actionObj.Status.Reason = ""
	_, err = storkops.Instance().UpdateAction(actionObj)
	require.NoError(t, err, "Error updating action")
	expected = "NAME                                                CREATED   STAGE                                        STATUS        MORE INFO\n" +
		"failover-test-migrationschedule-2024-01-01-000000             Waiting for Apps to Scale Down (on source)   In-Progress   \n"
	testCommon(t, cmdArgs, nil, expected, false)

	// LastMileMigration stage
	actionObj.Status.Stage = storkv1.ActionStageLastMileMigration
	actionObj.Status.Status = storkv1.ActionStatusInProgress
	actionObj.Status.Reason = ""
	_, err = storkops.Instance().UpdateAction(actionObj)
	require.NoError(t, err, "Error updating action")
	expected = "NAME                                                CREATED   STAGE                                              STATUS        MORE INFO\n" +
		"failover-test-migrationschedule-2024-01-01-000000             Last Mile Migration (from source -> destination)   In-Progress   \n"
	testCommon(t, cmdArgs, nil, expected, false)

	// ScaleUpDestination stage
	actionObj.Status.Stage = storkv1.ActionStageScaleUpDestination
	actionObj.Status.Status = storkv1.ActionStatusSuccessful
	actionObj.Status.Reason = ""
	actionObj.Status.Summary = &storkv1.ActionSummary{FailoverSummaryItem: []*storkv1.FailoverSummary{
		{
			Namespace: "ns1",
			Status:    storkv1.ActionStatusSuccessful,
			Reason:    "scaling up apps in namespace ns1 successful",
		},
		{
			Namespace: "ns2",
			Status:    storkv1.ActionStatusFailed,
			Reason:    "scaling up apps in namespace ns2 failed",
		},
	}}
	_, err = storkops.Instance().UpdateAction(actionObj)
	require.NoError(t, err, "Error updating action")
	expected = "NAME                                                CREATED   STAGE                       STATUS       MORE INFO\n" +
		"failover-test-migrationschedule-2024-01-01-000000             Scale Up (on destination)   Successful   Scaled up Apps in : 1/2 namespaces\n"
	testCommon(t, cmdArgs, nil, expected, false)

	// ScaleUpSource i.e. Rollback stage
	actionObj.Status.Stage = storkv1.ActionStageScaleUpSource
	actionObj.Status.Status = storkv1.ActionStatusSuccessful
	actionObj.Status.Reason = "Failing failover operation as the last mile migration test-migrationschedule-lastmile-failover-353342c failed: status Failed"
	actionObj.Status.Summary = &storkv1.ActionSummary{FailoverSummaryItem: []*storkv1.FailoverSummary{
		{
			Namespace: "ns1",
			Status:    storkv1.ActionStatusRollbackSuccessful,
			Reason:    "scaling up apps in namespace ns1 successful",
		},
		{
			Namespace: "ns2",
			Status:    storkv1.ActionStatusRollbackSuccessful,
			Reason:    "scaling up apps in namespace ns2 successful",
		},
	}}
	_, err = storkops.Instance().UpdateAction(actionObj)
	require.NoError(t, err, "Error updating action")
	expected = "NAME                                                CREATED   STAGE                                 STATUS       MORE INFO\n" +
		"failover-test-migrationschedule-2024-01-01-000000             Rolling Back Scale Down (on source)   Successful   Rolled back Apps in : 2/2 namespaces ; Failing failover operation as the last mile migration test-migrationschedule-lastmile-failover-353342c failed: status Failed\n"
	testCommon(t, cmdArgs, nil, expected, false)
}

func TestGetFailbackAllStages(t *testing.T) {
	defer resetTest()
	// create a failback action
	actionName := createFailbackActionAndVerify(t)
	// update the status of the action
	actionObj, err := storkops.Instance().GetAction(actionName, "kube-system")
	require.NoError(t, err, "Error getting action")

	cmdArgs := []string{"get", "failback", actionName, "-n", "kube-system"}
	// Initial stage
	actionObj.Status.Stage = storkv1.ActionStageInitial
	actionObj.Status.Status = storkv1.ActionStatusInProgress
	actionObj.Status.Reason = ""
	_, err = storkops.Instance().UpdateAction(actionObj)
	require.NoError(t, err, "Error updating action")
	expected := "NAME                                                CREATED   STAGE         STATUS        MORE INFO\n" +
		"failback-test-migrationschedule-2024-01-01-000000             Validations   In-Progress   \n"
	testCommon(t, cmdArgs, nil, expected, false)

	// ScaleDownDestination stage
	actionObj.Status.Stage = storkv1.ActionStageScaleDownDestination
	actionObj.Status.Status = storkv1.ActionStatusInProgress
	actionObj.Status.Reason = ""
	_, err = storkops.Instance().UpdateAction(actionObj)
	require.NoError(t, err, "Error updating action")
	expected = "NAME                                                CREATED   STAGE                         STATUS        MORE INFO\n" +
		"failback-test-migrationschedule-2024-01-01-000000             Scale Down (on destination)   In-Progress   \n"
	testCommon(t, cmdArgs, nil, expected, false)

	// WaitAfterScaleDown stage
	actionObj.Status.Stage = storkv1.ActionStageWaitAfterScaleDown
	actionObj.Status.Status = storkv1.ActionStatusInProgress
	actionObj.Status.Reason = ""
	_, err = storkops.Instance().UpdateAction(actionObj)
	require.NoError(t, err, "Error updating action")
	expected = "NAME                                                CREATED   STAGE                                             STATUS        MORE INFO\n" +
		"failback-test-migrationschedule-2024-01-01-000000             Waiting for Apps to Scale Down (on destination)   In-Progress   \n"
	testCommon(t, cmdArgs, nil, expected, false)

	// LastMileMigration stage
	actionObj.Status.Stage = storkv1.ActionStageLastMileMigration
	actionObj.Status.Status = storkv1.ActionStatusInProgress
	actionObj.Status.Reason = ""
	_, err = storkops.Instance().UpdateAction(actionObj)
	require.NoError(t, err, "Error updating action")
	expected = "NAME                                                CREATED   STAGE                                              STATUS        MORE INFO\n" +
		"failback-test-migrationschedule-2024-01-01-000000             Last Mile Migration (from destination -> source)   In-Progress   \n"
	testCommon(t, cmdArgs, nil, expected, false)

	// ScaleUpSource stage
	actionObj.Status.Stage = storkv1.ActionStageScaleUpSource
	actionObj.Status.Status = storkv1.ActionStatusSuccessful
	actionObj.Status.Reason = ""
	actionObj.Status.Summary = &storkv1.ActionSummary{FailbackSummaryItem: []*storkv1.FailbackSummary{
		{
			Namespace: "ns1",
			Status:    storkv1.ActionStatusSuccessful,
			Reason:    "scaling up apps in namespace ns1 successful",
		},
		{
			Namespace: "ns2",
			Status:    storkv1.ActionStatusFailed,
			Reason:    "scaling up apps in namespace ns2 failed",
		},
	}}
	_, err = storkops.Instance().UpdateAction(actionObj)
	require.NoError(t, err, "Error updating action")
	expected = "NAME                                                CREATED   STAGE                  STATUS       MORE INFO\n" +
		"failback-test-migrationschedule-2024-01-01-000000             Scale Up (on source)   Successful   Scaled up Apps in : 1/2 namespaces\n"
	testCommon(t, cmdArgs, nil, expected, false)

	// ScaleUpDestination i.e. Rollback stage
	actionObj.Status.Stage = storkv1.ActionStageScaleUpDestination
	actionObj.Status.Status = storkv1.ActionStatusSuccessful
	actionObj.Status.Reason = "Failing failback operation as the last mile migration test-migrationschedule-lastmile-failback-353342c failed: status Failed"
	actionObj.Status.Summary = &storkv1.ActionSummary{FailbackSummaryItem: []*storkv1.FailbackSummary{
		{
			Namespace: "ns1",
			Status:    storkv1.ActionStatusRollbackSuccessful,
			Reason:    "scaling up apps in namespace ns1 successful",
		},
		{
			Namespace: "ns2",
			Status:    storkv1.ActionStatusRollbackSuccessful,
			Reason:    "scaling up apps in namespace ns2 successful",
		},
	}}
	_, err = storkops.Instance().UpdateAction(actionObj)
	require.NoError(t, err, "Error updating action")
	expected = "NAME                                                CREATED   STAGE                                      STATUS       MORE INFO\n" +
		"failback-test-migrationschedule-2024-01-01-000000             Rolling Back Scale Down (on destination)   Successful   Rolled back Apps in : 2/2 namespaces ; Failing failback operation as the last mile migration test-migrationschedule-lastmile-failback-353342c failed: status Failed\n"
	testCommon(t, cmdArgs, nil, expected, false)
}

func TestGetFailoverFailedFailoverAction(t *testing.T) {
	defer resetTest()
	// create a failover action
	actionName := createFailoverActionAndVerify(t)
	// update the status of the action
	actionObj, err := storkops.Instance().GetAction(actionName, "kube-system")
	require.NoError(t, err, "Error getting action")
	actionObj.Status.Stage = storkv1.ActionStageFinal
	actionObj.Status.Status = storkv1.ActionStatusFailed
	actionObj.Status.Summary = &storkv1.ActionSummary{FailoverSummaryItem: []*storkv1.FailoverSummary{
		{
			Namespace: "ns1",
			Status:    storkv1.ActionStatusRollbackSuccessful,
			Reason:    "scaling up apps in namespace ns1 successful",
		},
		{
			Namespace: "ns2",
			Status:    storkv1.ActionStatusRollbackFailed,
			Reason:    "scaling up apps in namespace ns2 failed",
		},
	}}
	actionObj.Status.Reason = "Failing failover operation as the last mile migration test-migrationschedule-lastmile-failover-353342c failed: status Failed"
	_, err = storkops.Instance().UpdateAction(actionObj)
	require.NoError(t, err, "Error updating action")
	cmdArgs := []string{"get", "failover", actionName, "-n", "kube-system"}
	expected := "NAME                                                CREATED   STAGE       STATUS   MORE INFO\n" +
		"failover-test-migrationschedule-2024-01-01-000000             Completed   Failed   Rolled back Apps in : 1/2 namespaces ; Failing failover operation as the last mile migration test-migrationschedule-lastmile-failover-353342c failed: status Failed\n"
	testCommon(t, cmdArgs, nil, expected, false)
}

func TestGetFailbackFailedFailbackAction(t *testing.T) {
	defer resetTest()
	// create a failback action
	actionName := createFailbackActionAndVerify(t)
	// update the status of the action
	actionObj, err := storkops.Instance().GetAction(actionName, "kube-system")
	require.NoError(t, err, "Error getting action")
	actionObj.Status.Stage = storkv1.ActionStageFinal
	actionObj.Status.Status = storkv1.ActionStatusFailed
	actionObj.Status.Summary = &storkv1.ActionSummary{FailbackSummaryItem: []*storkv1.FailbackSummary{
		{
			Namespace: "ns1",
			Status:    storkv1.ActionStatusRollbackSuccessful,
			Reason:    "scaling up apps in namespace ns1 successful",
		},
		{
			Namespace: "ns2",
			Status:    storkv1.ActionStatusRollbackFailed,
			Reason:    "scaling up apps in namespace ns2 failed",
		},
	}}
	actionObj.Status.Reason = "Failing failback operation as the last mile migration test-migrationschedule-lastmile-failback-353342c failed: status Failed"
	_, err = storkops.Instance().UpdateAction(actionObj)
	require.NoError(t, err, "Error updating action")
	cmdArgs := []string{"get", "failback", actionName, "-n", "kube-system"}
	expected := "NAME                                                CREATED   STAGE       STATUS   MORE INFO\n" +
		"failback-test-migrationschedule-2024-01-01-000000             Completed   Failed   Rolled back Apps in : 1/2 namespaces ; Failing failback operation as the last mile migration test-migrationschedule-lastmile-failback-353342c failed: status Failed\n"
	testCommon(t, cmdArgs, nil, expected, false)
}

func createFailoverActionAndVerify(t *testing.T) string {
	mockTheTime()
	failoverActionName := "failover-test-migrationschedule-2024-01-01-000000"
	createTestMigrationSchedule("test-migrationschedule", "default-migration-policy", "clusterPair1", []string{"ns1", "ns2"}, "kube-system", true, t)
	createClusterPair(t, "clusterPair1", "kube-system", "async-dr")
	cmdArgs := []string{"perform", "failover", "-m", "test-migrationschedule", "--skip-source-operations", "-n", "kube-system"}
	expected := fmt.Sprintf("Started failover for MigrationSchedule kube-system/test-migrationschedule\nTo check failover status use the command : `storkctl get failover %v -n kube-system`\n", failoverActionName)
	testCommon(t, cmdArgs, nil, expected, false)
	actionObj, err := storkops.Instance().GetAction(failoverActionName, "kube-system")
	require.NoError(t, err, "Error getting action")
	require.Equal(t, actionObj.Spec.ActionParameter.FailoverParameter.FailoverNamespaces, []string{"ns1", "ns2"})
	require.Equal(t, actionObj.Spec.ActionParameter.FailoverParameter.MigrationScheduleReference, "test-migrationschedule")
	require.Equal(t, *actionObj.Spec.ActionParameter.FailoverParameter.SkipSourceOperations, true)
	return failoverActionName
}

func createFailbackActionAndVerify(t *testing.T) string {
	mockTheTime()
	failbackActionName := "failback-test-migrationschedule-2024-01-01-000000"
	createTestMigrationSchedule("test-migrationschedule", "default-migration-policy", "clusterPair1", []string{"ns1", "ns2"}, "kube-system", true, t)
	createClusterPair(t, "clusterPair1", "kube-system", "async-dr")
	cmdArgs := []string{"perform", "failback", "-m", "test-migrationschedule", "-n", "kube-system"}
	expected := fmt.Sprintf("Started failback for MigrationSchedule kube-system/test-migrationschedule\nTo check failback status use the command : `storkctl get failback %v -n kube-system`\n", failbackActionName)
	testCommon(t, cmdArgs, nil, expected, false)
	actionObj, err := storkops.Instance().GetAction(failbackActionName, "kube-system")
	require.NoError(t, err, "Error getting action")
	require.Equal(t, actionObj.Spec.ActionParameter.FailbackParameter.FailbackNamespaces, []string{"ns1", "ns2"})
	require.Equal(t, actionObj.Spec.ActionParameter.FailbackParameter.MigrationScheduleReference, "test-migrationschedule")
	return failbackActionName
}
