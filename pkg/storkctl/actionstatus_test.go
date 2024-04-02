//go:build unittest
// +build unittest

package storkctl

import (
	"fmt"
	"testing"

	storkv1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	storkops "github.com/portworx/sched-ops/k8s/stork"
	"github.com/stretchr/testify/require"
)

func TestGetFailoverNoFailoverAction(t *testing.T) {
	cmdArgs := []string{"get", "failover"}
	expected := "No resources found.\n"
	var actionList storkv1.ActionList
	testCommon(t, cmdArgs, &actionList, expected, false)
}

func TestGetFailoverOneSuccessfulFailoverAction(t *testing.T) {
	defer resetTest()
	// create a failover action
	actionName := createFailoverActionAndVerify(t)
	// update the status pf the action
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
	expected := "NAME                                                CREATED   STAGE       STATUS       ADDITIONAL INFO\n" +
		"failover-test-migrationschedule-2024-01-01-000000             Completed   Successful   Scaled up Apps in : 2/2 namespaces\n"
	testCommon(t, cmdArgs, nil, expected, false)
}

func TestGetFailoverOneFailedFailoverAction(t *testing.T) {
	defer resetTest()
	// create a failover action
	actionName := createFailoverActionAndVerify(t)
	// update the status pf the action
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
	expected := "NAME                                                CREATED   STAGE       STATUS   ADDITIONAL INFO\n" +
		"failover-test-migrationschedule-2024-01-01-000000             Completed   Failed   Rolled back Apps in : 1/2 namespaces ; Failing failover operation as the last mile migration test-migrationschedule-lastmile-failover-353342c failed: status Failed\n"
	testCommon(t, cmdArgs, nil, expected, false)
}

func createFailoverActionAndVerify(t *testing.T) string {
	mockTheTime()
	failoverActionName := "failover-test-migrationschedule-2024-01-01-000000"
	createTestMigrationSchedule("test-migrationschedule", "default-migration-policy", "clusterPair1", []string{"ns1", "ns2"}, "kube-system", true, t)
	createClusterPair(t, "clusterPair1", "kube-system", "async-dr")
	cmdArgs := []string{"perform", "failover", "-m", "test-migrationschedule", "--skip-deactivate-source", "-n", "kube-system"}
	expected := fmt.Sprintf("Started failover for MigrationSchedule kube-system/test-migrationschedule\nTo check failover status use the command : `storkctl get failover %v -n kube-system`\n", failoverActionName)
	testCommon(t, cmdArgs, nil, expected, false)
	actionObj, err := storkops.Instance().GetAction(failoverActionName, "kube-system")
	require.NoError(t, err, "Error getting action")
	require.Equal(t, actionObj.Spec.ActionParameter.FailoverParameter.FailoverNamespaces, []string{"ns1", "ns2"})
	require.Equal(t, actionObj.Spec.ActionParameter.FailoverParameter.MigrationScheduleReference, "test-migrationschedule")
	require.Equal(t, *actionObj.Spec.ActionParameter.FailoverParameter.SkipDeactivateSource, true)
	return failoverActionName
}
