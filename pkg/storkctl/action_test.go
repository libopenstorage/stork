//go:build unittest
// +build unittest

package storkctl

import (
	"fmt"
	"testing"
	"time"

	storkops "github.com/portworx/sched-ops/k8s/stork"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/util/validation"
)

func TestPerformFailoverNoMigrationSchedule(t *testing.T) {
	cmdArgs := []string{"perform", "failover"}
	expected := "error: reference MigrationSchedule name needs to be provided for failover"
	testCommon(t, cmdArgs, nil, expected, true)
}

func TestPerformFailoverInvalidMigrationSchedule(t *testing.T) {
	cmdArgs := []string{"perform", "failover", "-m", "invalid-migr-sched"}
	expected := "error: unable to find the reference MigrationSchedule invalid-migr-sched in the default namespace"
	testCommon(t, cmdArgs, nil, expected, true)
}

func TestPerformFailoverWithInvalidNamespaces(t *testing.T) {
	defer resetTest()
	createTestMigrationSchedule("test-migrationschedule", "default-migration-policy", "clusterPair1", []string{"ns1", "ns2"}, "kube-system", false, t)
	cmdArgs := []string{"perform", "failover", "-m", "test-migrationschedule", "--include-namespaces", "ns1", "--exclude-namespaces", "ns2", "--skip-deactivate-source", "-n", "kube-system"}
	expected := "error: can provide only one of --include-namespaces or --exclude-namespaces values at once"
	testCommon(t, cmdArgs, nil, expected, true)
}

func TestPerformFailoverWithInvalidIncludeNamespaceList(t *testing.T) {
	defer resetTest()
	createClusterPair(t, "clusterPair1", "ns1", "async-dr")
	createTestMigrationSchedule("test-migrationschedule", "default-migration-policy", "clusterPair1", []string{"ns1"}, "ns1", false, t)
	cmdArgs := []string{"perform", "failover", "-m", "test-migrationschedule", "--include-namespaces", "namespace1", "-n", "ns1"}
	expected := "error: provided namespaces [namespace1] are not a subset of the namespaces being migrated by the given MigrationSchedule"
	testCommon(t, cmdArgs, nil, expected, true)
}

func TestPerformFailoverWithInvalidExcludeNamespaceList(t *testing.T) {
	defer resetTest()
	createClusterPair(t, "clusterPair1", "kube-system", "async-dr")
	createTestMigrationSchedule("test-migrationschedule", "default-migration-policy", "clusterPair1", []string{"ns1", "ns2"}, "kube-system", false, t)
	cmdArgs := []string{"perform", "failover", "-m", "test-migrationschedule", "--exclude-namespaces", "ns3", "-n", "kube-system"}
	expected := "error: provided namespaces [ns3] are not a subset of the namespaces being migrated by the given MigrationSchedule"
	testCommon(t, cmdArgs, nil, expected, true)
}

func TestPerformFailoverMissingClusterPair(t *testing.T) {
	defer resetTest()
	mockTheTime()
	createTestMigrationSchedule("test-migrationschedule", "default-migration-policy", "clusterPair1", []string{"ns1", "ns2"}, "kube-system", false, t)
	cmdArgs := []string{"perform", "failover", "-m", "test-migrationschedule", "-n", "kube-system"}
	expected := "error: unable to find the cluster pair clusterPair1 in the kube-system namespace"
	testCommon(t, cmdArgs, nil, expected, true)

	//create clusterPair and try again
	createClusterPair(t, "clusterPair1", "kube-system", "async-dr")
	failoverActionName := "failover-test-migrationschedule-2024-01-01-000000"
	expected = fmt.Sprintf("Started failover for MigrationSchedule kube-system/test-migrationschedule\nTo check failover status use the command : `storkctl get failover %v -n kube-system`\n", failoverActionName)
	testCommon(t, cmdArgs, nil, expected, false)
	actionObj, err := storkops.Instance().GetAction(failoverActionName, "kube-system")
	require.NoError(t, err, "Error getting action")
	require.Equal(t, actionObj.Spec.ActionParameter.FailoverParameter.FailoverNamespaces, []string{"ns1", "ns2"})
	require.Equal(t, *actionObj.Spec.ActionParameter.FailoverParameter.SkipDeactivateSource, false)
}

func TestPerformFailoverWithIncludeNamespaceList(t *testing.T) {
	defer resetTest()
	mockTheTime()
	createTestMigrationSchedule("test-migrationschedule", "default-migration-policy", "clusterPair1", []string{"ns1", "ns2"}, "kube-system", false, t)
	cmdArgs := []string{"perform", "failover", "-m", "test-migrationschedule", "--include-namespaces", "ns1", "--skip-deactivate-source", "-n", "kube-system"}
	failoverActionName := "failover-test-migrationschedule-2024-01-01-000000"
	expected := fmt.Sprintf("Started failover for MigrationSchedule kube-system/test-migrationschedule\nTo check failover status use the command : `storkctl get failover %v -n kube-system`\n", failoverActionName)
	testCommon(t, cmdArgs, nil, expected, false)
	actionObj, err := storkops.Instance().GetAction(failoverActionName, "kube-system")
	require.NoError(t, err, "Error getting action")
	require.Equal(t, actionObj.Spec.ActionParameter.FailoverParameter.FailoverNamespaces, []string{"ns1"})
	require.Equal(t, *actionObj.Spec.ActionParameter.FailoverParameter.SkipDeactivateSource, true)
}

func TestPerformFailoverWithExcludeNamespaceList(t *testing.T) {
	defer resetTest()
	mockTheTime()
	createClusterPair(t, "clusterPair1", "kube-system", "async-dr")
	createTestMigrationSchedule("test-migrationschedule", "default-migration-policy", "clusterPair1", []string{"ns1", "ns2"}, "kube-system", false, t)
	cmdArgs := []string{"perform", "failover", "-m", "test-migrationschedule", "--exclude-namespaces", "ns1", "-n", "kube-system"}
	failoverActionName := "failover-test-migrationschedule-2024-01-01-000000"
	expected := fmt.Sprintf("Started failover for MigrationSchedule kube-system/test-migrationschedule\nTo check failover status use the command : `storkctl get failover %v -n kube-system`\n", failoverActionName)
	testCommon(t, cmdArgs, nil, expected, false)
	actionObj, err := storkops.Instance().GetAction(failoverActionName, "kube-system")
	require.NoError(t, err, "Error getting action")
	require.Equal(t, actionObj.Spec.ActionParameter.FailoverParameter.FailoverNamespaces, []string{"ns2"})
	require.Equal(t, *actionObj.Spec.ActionParameter.FailoverParameter.SkipDeactivateSource, false)
}

func TestFailoverActionNameTruncation(t *testing.T) {
	defer resetTest()
	mockTheTime()
	strWithLen253 := "utttaxvjedvxmjexmnapepwctfmjgydmidpcaantcarptudqufcpvideuttwbzgqxtexuceqnwnecxabuwaqzqypjxcvyubkwtpapziwkpdxzqfyjkyxnikfiauqvpktpkdurfzjrmyjzmhqhuqnpfjdnavfbkrrjqamtmjiphpdggcafmqugrmvqzwfchkukdeudbpxdzqqzeayjgcnphprurepjrqcwxbxrpnyhbdzkqveiukyzeghenfvp"
	truncatedStr := strWithLen253[:validation.DNS1123SubdomainMaxLength-len("failover")-len("2024-01-01-000000")-2]
	createTestMigrationSchedule(strWithLen253, "default-migration-policy", "clusterPair1", []string{"ns1", "ns2"}, "kube-system", false, t)
	cmdArgs := []string{"perform", "failover", "-m", strWithLen253, "--exclude-namespaces", "ns1", "--skip-deactivate-source", "-n", "kube-system"}
	failoverActionName := fmt.Sprintf("failover-%v-2024-01-01-000000", truncatedStr)
	expected := fmt.Sprintf("Started failover for MigrationSchedule kube-system/%v\nTo check failover status use the command : `storkctl get failover %v -n kube-system`\n", strWithLen253, failoverActionName)
	testCommon(t, cmdArgs, nil, expected, false)
}

func mockTheTime() {
	mockNow := time.Date(2024, time.January, 1, 0, 0, 0, 0, time.Local)
	setMockTime(&mockNow)
}
