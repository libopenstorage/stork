//go:build unittest
// +build unittest

package storkctl

import (
	"fmt"
	"testing"
	"time"

	storkops "github.com/libopenstorage/stork/pkg/crud/stork"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/util/validation"
)

func TestPerformFailoverFailbackNoMigrationSchedule(t *testing.T) {
	cmdArgs := []string{"perform", "failover"}
	expected := "error: reference MigrationSchedule name needs to be provided for failover"
	testCommon(t, cmdArgs, nil, expected, true)

	cmdArgs = []string{"perform", "failback"}
	expected = "error: reference MigrationSchedule name needs to be provided for failback"
	testCommon(t, cmdArgs, nil, expected, true)
}

func TestPerformFailoverFailbackInvalidMigrationSchedule(t *testing.T) {
	cmdArgs := []string{"perform", "failover", "-m", "invalid-migr-sched"}
	expected := "error: unable to find the reference MigrationSchedule invalid-migr-sched in the default namespace"
	testCommon(t, cmdArgs, nil, expected, true)

	cmdArgs = []string{"perform", "failback", "-m", "invalid-migr-sched"}
	testCommon(t, cmdArgs, nil, expected, true)
}

func TestPerformFailoverIncorrectCluster(t *testing.T) {
	defer resetTest()
	// when ms doesn't have static copy annotation
	createTestMigrationSchedule("test-migrationschedule", "default-migration-policy", "clusterPair1", []string{"ns1", "ns2"}, "kube-system", false, t)
	createClusterPair(t, "clusterPair1", "kube-system", "async-dr")
	cmdArgs := []string{"perform", "failover", "-m", "test-migrationschedule", "--include-namespaces", "ns1", "-n", "kube-system"}
	expected := "error: ensure that `storkctl perform failover` is run in the cluster you want to failover to"
	testCommon(t, cmdArgs, nil, expected, true)
}

func TestPerformFailoverFailbackWithInvalidNamespaces(t *testing.T) {
	defer resetTest()
	createTestMigrationSchedule("test-migrationschedule", "default-migration-policy", "clusterPair1", []string{"ns1", "ns2"}, "kube-system", true, t)
	createClusterPair(t, "clusterPair1", "kube-system", "async-dr")
	cmdArgs := []string{"perform", "failover", "-m", "test-migrationschedule", "--include-namespaces", "ns1", "--exclude-namespaces", "ns2", "--skip-source-operations", "-n", "kube-system"}
	expected := "error: can provide only one of --include-namespaces or --exclude-namespaces values at once"
	testCommon(t, cmdArgs, nil, expected, true)

	cmdArgs = []string{"perform", "failback", "-m", "test-migrationschedule", "--include-namespaces", "ns1", "--exclude-namespaces", "ns2", "-n", "kube-system"}
	testCommon(t, cmdArgs, nil, expected, true)
}

func TestPerformFailoverFailbackWithInvalidIncludeNamespaceList(t *testing.T) {
	defer resetTest()
	createClusterPair(t, "clusterPair1", "ns1", "async-dr")
	createTestMigrationSchedule("test-migrationschedule", "default-migration-policy", "clusterPair1", []string{"ns1"}, "ns1", true, t)
	cmdArgs := []string{"perform", "failover", "-m", "test-migrationschedule", "--include-namespaces", "namespace1", "-n", "ns1"}
	expected := "error: provided namespaces [namespace1] are not a subset of the namespaces being migrated by the given MigrationSchedule"
	testCommon(t, cmdArgs, nil, expected, true)

	cmdArgs = []string{"perform", "failback", "-m", "test-migrationschedule", "--include-namespaces", "namespace1", "-n", "ns1"}
	testCommon(t, cmdArgs, nil, expected, true)
}

func TestPerformFailoverFailbackWithInvalidExcludeNamespaceList(t *testing.T) {
	defer resetTest()
	createClusterPair(t, "clusterPair1", "kube-system", "async-dr")
	createTestMigrationSchedule("test-migrationschedule", "default-migration-policy", "clusterPair1", []string{"ns1", "ns2"}, "kube-system", true, t)
	cmdArgs := []string{"perform", "failover", "-m", "test-migrationschedule", "--exclude-namespaces", "ns3", "-n", "kube-system"}
	expected := "error: provided namespaces [ns3] are not a subset of the namespaces being migrated by the given MigrationSchedule"
	testCommon(t, cmdArgs, nil, expected, true)

	cmdArgs = []string{"perform", "failback", "-m", "test-migrationschedule", "--exclude-namespaces", "ns3", "-n", "kube-system"}
	testCommon(t, cmdArgs, nil, expected, true)
}

func TestPerformFailoverFailbackMissingClusterPair(t *testing.T) {
	defer resetTest()
	mockTheTime()
	createTestMigrationSchedule("test-migrationschedule", "default-migration-policy", "clusterPair1", []string{"ns1", "ns2"}, "kube-system", true, t)
	cmdArgs := []string{"perform", "failover", "-m", "test-migrationschedule", "-n", "kube-system"}
	expected := "error: unable to find the ClusterPair clusterPair1 in the kube-system namespace"
	testCommon(t, cmdArgs, nil, expected, true)

	cmdArgs = []string{"perform", "failback", "-m", "test-migrationschedule", "-n", "kube-system"}
	testCommon(t, cmdArgs, nil, expected, true)

	//create clusterPair and try again
	createClusterPair(t, "clusterPair1", "kube-system", "async-dr")
	failoverActionName := "failover-test-migrationschedule-2024-01-01-000000"
	cmdArgs = []string{"perform", "failover", "-m", "test-migrationschedule", "-n", "kube-system"}
	expected = fmt.Sprintf("Started failover for MigrationSchedule kube-system/test-migrationschedule\nTo check failover status use the command : `storkctl get failover %v -n kube-system`\n", failoverActionName)
	testCommon(t, cmdArgs, nil, expected, false)
	actionObj, err := storkops.Instance().GetAction(failoverActionName, "kube-system")
	require.NoError(t, err, "Error getting action")
	require.Equal(t, actionObj.Spec.ActionParameter.FailoverParameter.FailoverNamespaces, []string{"ns1", "ns2"})
	require.Equal(t, actionObj.Spec.ActionParameter.FailoverParameter.MigrationScheduleReference, "test-migrationschedule")
	require.Equal(t, *actionObj.Spec.ActionParameter.FailoverParameter.SkipSourceOperations, false)

	failbackActionName := "failback-test-migrationschedule-2024-01-01-000000"
	cmdArgs = []string{"perform", "failback", "-m", "test-migrationschedule", "-n", "kube-system"}
	expected = fmt.Sprintf("Started failback for MigrationSchedule kube-system/test-migrationschedule\nTo check failback status use the command : `storkctl get failback %v -n kube-system`\n", failbackActionName)
	testCommon(t, cmdArgs, nil, expected, false)
	actionObj, err = storkops.Instance().GetAction(failbackActionName, "kube-system")
	require.NoError(t, err, "Error getting action")
	require.Equal(t, actionObj.Spec.ActionParameter.FailbackParameter.FailbackNamespaces, []string{"ns1", "ns2"})
	require.Equal(t, actionObj.Spec.ActionParameter.FailbackParameter.MigrationScheduleReference, "test-migrationschedule")

}

func TestPerformFailoverFailbackWithIncludeNamespaceList(t *testing.T) {
	defer resetTest()
	mockTheTime()
	createTestMigrationSchedule("test-migrationschedule", "default-migration-policy", "clusterPair1", []string{"ns1", "ns2"}, "kube-system", true, t)
	createClusterPair(t, "clusterPair1", "kube-system", "async-dr")
	cmdArgs := []string{"perform", "failover", "-m", "test-migrationschedule", "--include-namespaces", "ns1", "--skip-source-operations", "-n", "kube-system"}
	failoverActionName := "failover-test-migrationschedule-2024-01-01-000000"
	expected := fmt.Sprintf("Started failover for MigrationSchedule kube-system/test-migrationschedule\nTo check failover status use the command : `storkctl get failover %v -n kube-system`\n", failoverActionName)
	testCommon(t, cmdArgs, nil, expected, false)
	actionObj, err := storkops.Instance().GetAction(failoverActionName, "kube-system")
	require.NoError(t, err, "Error getting action")
	require.Equal(t, actionObj.Spec.ActionParameter.FailoverParameter.FailoverNamespaces, []string{"ns1"})
	require.Equal(t, actionObj.Spec.ActionParameter.FailoverParameter.MigrationScheduleReference, "test-migrationschedule")
	require.Equal(t, *actionObj.Spec.ActionParameter.FailoverParameter.SkipSourceOperations, true)

	failbackActionName := "failback-test-migrationschedule-2024-01-01-000000"
	cmdArgs = []string{"perform", "failback", "-m", "test-migrationschedule", "--include-namespaces", "ns1", "-n", "kube-system"}
	expected = fmt.Sprintf("Started failback for MigrationSchedule kube-system/test-migrationschedule\nTo check failback status use the command : `storkctl get failback %v -n kube-system`\n", failbackActionName)
	testCommon(t, cmdArgs, nil, expected, false)
	actionObj, err = storkops.Instance().GetAction(failbackActionName, "kube-system")
	require.NoError(t, err, "Error getting action")
	require.Equal(t, actionObj.Spec.ActionParameter.FailbackParameter.FailbackNamespaces, []string{"ns1"})
	require.Equal(t, actionObj.Spec.ActionParameter.FailbackParameter.MigrationScheduleReference, "test-migrationschedule")

}

func TestPerformFailoverFailbackWithExcludeNamespaceList(t *testing.T) {
	defer resetTest()
	mockTheTime()
	createClusterPair(t, "clusterPair1", "kube-system", "async-dr")
	createTestMigrationSchedule("test-migrationschedule", "default-migration-policy", "clusterPair1", []string{"ns1", "ns2"}, "kube-system", true, t)
	cmdArgs := []string{"perform", "failover", "-m", "test-migrationschedule", "--exclude-namespaces", "ns1", "-n", "kube-system"}
	failoverActionName := "failover-test-migrationschedule-2024-01-01-000000"
	expected := fmt.Sprintf("Started failover for MigrationSchedule kube-system/test-migrationschedule\nTo check failover status use the command : `storkctl get failover %v -n kube-system`\n", failoverActionName)
	testCommon(t, cmdArgs, nil, expected, false)
	actionObj, err := storkops.Instance().GetAction(failoverActionName, "kube-system")
	require.NoError(t, err, "Error getting action")
	require.Equal(t, actionObj.Spec.ActionParameter.FailoverParameter.FailoverNamespaces, []string{"ns2"})
	require.Equal(t, actionObj.Spec.ActionParameter.FailoverParameter.MigrationScheduleReference, "test-migrationschedule")
	require.Equal(t, *actionObj.Spec.ActionParameter.FailoverParameter.SkipSourceOperations, false)

	failbackActionName := "failback-test-migrationschedule-2024-01-01-000000"
	cmdArgs = []string{"perform", "failback", "-m", "test-migrationschedule", "--exclude-namespaces", "ns1", "-n", "kube-system"}
	expected = fmt.Sprintf("Started failback for MigrationSchedule kube-system/test-migrationschedule\nTo check failback status use the command : `storkctl get failback %v -n kube-system`\n", failbackActionName)
	testCommon(t, cmdArgs, nil, expected, false)
	actionObj, err = storkops.Instance().GetAction(failbackActionName, "kube-system")
	require.NoError(t, err, "Error getting action")
	require.Equal(t, actionObj.Spec.ActionParameter.FailbackParameter.FailbackNamespaces, []string{"ns2"})
	require.Equal(t, actionObj.Spec.ActionParameter.FailbackParameter.MigrationScheduleReference, "test-migrationschedule")
}

func TestFailoverFailbackActionNameTruncation(t *testing.T) {
	defer resetTest()
	mockTheTime()
	strWithLen253 := "utttaxvjedvxmjexmnapepwctfmjgydmidpcaantcarptudqufcpvideuttwbzgqxtexuceqnwnecxabuwaqzqypjxcvyubkwtpapziwkpdxzqfyjkyxnikfiauqvpktpkdurfzjrmyjzmhqhuqnpfjdnavfbkrrjqamtmjiphpdggcafmqugrmvqzwfchkukdeudbpxdzqqzeayjgcnphprurepjrqcwxbxrpnyhbdzkqveiukyzeghenfvp"
	truncatedStr := strWithLen253[:validation.DNS1123SubdomainMaxLength-len("failover")-len("2024-01-01-000000")-2]
	createTestMigrationSchedule(strWithLen253, "default-migration-policy", "clusterPair1", []string{"ns1", "ns2"}, "kube-system", true, t)
	createClusterPair(t, "clusterPair1", "kube-system", "async-dr")
	cmdArgs := []string{"perform", "failover", "-m", strWithLen253, "--exclude-namespaces", "ns1", "--skip-source-operations", "-n", "kube-system"}
	failoverActionName := fmt.Sprintf("failover-%v-2024-01-01-000000", truncatedStr)
	expected := fmt.Sprintf("Started failover for MigrationSchedule kube-system/%v\nTo check failover status use the command : `storkctl get failover %v -n kube-system`\n", strWithLen253, failoverActionName)
	testCommon(t, cmdArgs, nil, expected, false)

	failbackActionName := fmt.Sprintf("failback-%v-2024-01-01-000000", truncatedStr)
	cmdArgs = []string{"perform", "failback", "-m", strWithLen253, "--exclude-namespaces", "ns1", "-n", "kube-system"}
	expected = fmt.Sprintf("Started failback for MigrationSchedule kube-system/%v\nTo check failback status use the command : `storkctl get failback %v -n kube-system`\n", strWithLen253, failbackActionName)
	testCommon(t, cmdArgs, nil, expected, false)
}

func mockTheTime() {
	mockNow := time.Date(2024, time.January, 1, 0, 0, 0, 0, time.Local)
	setMockTime(&mockNow)
}
