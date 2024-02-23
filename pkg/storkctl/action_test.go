//go:build unittest
// +build unittest

package storkctl

import (
	"fmt"
	storkv1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	storkops "github.com/portworx/sched-ops/k8s/stork"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
	"time"
)

func TestPerformFailoverNoMigrationSchedule(t *testing.T) {
	cmdArgs := []string{"perform", "failover"}
	expected := "error: referenceMigrationSchedule name needs to be provided for failover"
	testCommon(t, cmdArgs, nil, expected, true)
}

func TestPerformFailoverInvalidMigrationSchedule(t *testing.T) {
	cmdArgs := []string{"perform", "failover", "-m", "invalid-migr-sched"}
	expected := "error: unable to find the referenceMigrationSchedule invalid-migr-sched in the default namespace"
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
	expected := "error: provided namespaces [namespace1] are not a subset of the namespaces being migrated by the given migrationSchedule"
	testCommon(t, cmdArgs, nil, expected, true)
}

func TestPerformFailoverWithInvalidExcludeNamespaceList(t *testing.T) {
	defer resetTest()
	createClusterPair(t, "clusterPair1", "kube-system", "async-dr")
	createTestMigrationSchedule("test-migrationschedule", "default-migration-policy", "clusterPair1", []string{"ns1", "ns2"}, "kube-system", false, t)
	cmdArgs := []string{"perform", "failover", "-m", "test-migrationschedule", "--exclude-namespaces", "ns3", "-n", "kube-system"}
	expected := "error: provided namespaces [ns3] are not a subset of the namespaces being migrated by the given migrationSchedule"
	testCommon(t, cmdArgs, nil, expected, true)
}

func TestPerformFailoverMissingClusterPair(t *testing.T) {
	defer resetTest()
	createTestMigrationSchedule("test-migrationschedule", "default-migration-policy", "clusterPair1", []string{"ns1", "ns2"}, "kube-system", false, t)
	cmdArgs := []string{"perform", "failover", "-m", "test-migrationschedule", "-n", "kube-system"}
	expected := "error: unable to find the cluster pair clusterPair1 in the kube-system namespace"
	testCommon(t, cmdArgs, nil, expected, true)

	//create clusterPair and try again
	createClusterPair(t, "clusterPair1", "kube-system", "async-dr")
	failoverActionName := mockGetActionName(storkv1.ActionTypeFailover, "test-migrationschedule")
	expected = fmt.Sprintf("Started failover for migrationSchedule kube-system/test-migrationschedule\nTo check failover status use the command : `storkctl get failover %v -n kube-system`\n", failoverActionName)
	testCommon(t, cmdArgs, nil, expected, false)
}

func TestPerformFailoverWithIncludeNamespaceList(t *testing.T) {
	defer resetTest()
	createTestMigrationSchedule("test-migrationschedule", "default-migration-policy", "clusterPair1", []string{"ns1", "ns2"}, "kube-system", false, t)
	cmdArgs := []string{"perform", "failover", "-m", "test-migrationschedule", "--include-namespaces", "ns1", "--skip-deactivate-source", "-n", "kube-system"}
	failoverActionName := mockGetActionName(storkv1.ActionTypeFailover, "test-migrationschedule")
	expected := fmt.Sprintf("Started failover for migrationSchedule kube-system/test-migrationschedule\nTo check failover status use the command : `storkctl get failover %v -n kube-system`\n", failoverActionName)
	testCommon(t, cmdArgs, nil, expected, false)
	actionObj, err := storkops.Instance().GetAction(failoverActionName, "kube-system")
	require.NoError(t, err, "Error getting action")
	require.Equal(t, actionObj.Spec.ActionParameter.FailoverParameter.FailoverNamespaces, []string{"ns1"})
	require.Equal(t, actionObj.Spec.ActionParameter.FailoverParameter.DeactivateSource, false)
}

func TestPerformFailoverWithExcludeNamespaceList(t *testing.T) {
	defer resetTest()
	createClusterPair(t, "clusterPair1", "kube-system", "async-dr")
	createTestMigrationSchedule("test-migrationschedule", "default-migration-policy", "clusterPair1", []string{"ns1", "ns2"}, "kube-system", false, t)
	cmdArgs := []string{"perform", "failover", "-m", "test-migrationschedule", "--exclude-namespaces", "ns1", "-n", "kube-system"}
	failoverActionName := mockGetActionName(storkv1.ActionTypeFailover, "test-migrationschedule")
	expected := fmt.Sprintf("Started failover for migrationSchedule kube-system/test-migrationschedule\nTo check failover status use the command : `storkctl get failover %v -n kube-system`\n", failoverActionName)
	testCommon(t, cmdArgs, nil, expected, false)
	actionObj, err := storkops.Instance().GetAction(failoverActionName, "kube-system")
	require.NoError(t, err, "Error getting action")
	require.Equal(t, actionObj.Spec.ActionParameter.FailoverParameter.FailoverNamespaces, []string{"ns2"})
}

func mockGetActionName(actionType storkv1.ActionType, referenceResourceName string) string {
	mockNow := time.Date(2024, time.January, 1, 0, 0, 0, 0, time.Local)
	setMockTime(&mockNow)
	return strings.Join([]string{string(actionType), referenceResourceName, GetCurrentTime().Format(nameTimeSuffixFormat)}, "-")
}
