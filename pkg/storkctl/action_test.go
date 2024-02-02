//go:build unittest
// +build unittest

package storkctl

import (
	storkv1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	storkops "github.com/portworx/sched-ops/k8s/stork"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestPerformFailoverNoMigrationSchedule(t *testing.T) {
	cmdArgs := []string{"perform", "failover"}
	expected := "error: reference MigrationSchedule name needs to be provided for failover"
	testCommon(t, cmdArgs, nil, expected, true)
}

func TestPerformFailoverInvalidMigrationSchedule(t *testing.T) {
	cmdArgs := []string{"perform", "failover", "-m", "invalid-migr-sched"}
	expected := "error: unable to find the reference MigrationSchedule in the given namespace"
	testCommon(t, cmdArgs, nil, expected, true)
}

func TestPerformFailoverWithInvalidNamespaceList(t *testing.T) {
	defer resetTest()
	createClusterPair(t, "clusterPair1", "default", "async-dr")
	cmdArgs := []string{"create", "migrationschedule", "-i", "15", "-c", "clusterPair1",
		"--namespaces", "default", "test-migrationschedule", "-n", "default"}
	expected := "MigrationSchedule test-migrationschedule created successfully\n"
	testCommon(t, cmdArgs, nil, expected, false)

	cmdArgs = []string{"perform", "failover", "-m", "test-migrationschedule", "--namespaces", "namespace1"}
	expected = "error: provided namespaces are invalid with respect to the referenceMigrationSchedule"
	testCommon(t, cmdArgs, nil, expected, true)
}

func TestPerformFailoverMissingClusterPair(t *testing.T) {
	defer resetTest()
	// Create migrationSchedule
	testms := storkv1.MigrationSchedule{
		Spec: storkv1.MigrationScheduleSpec{
			SchedulePolicyName: "default-migration-policy",
			Template: storkv1.MigrationTemplateSpec{
				Spec: storkv1.MigrationSpec{
					ClusterPair: "clusterPair1",
					Namespaces:  []string{"default"},
				},
			},
		},
	}
	testms.Namespace = "default"
	testms.Name = "test-migrationschedule"
	_, err := storkops.Instance().CreateMigrationSchedule(&testms)
	require.NoError(t, err, "Error creating migrationSchedule")

	cmdArgs := []string{"perform", "failover", "-m", "test-migrationschedule"}
	expected := "error: unable to find the cluster pair in the given namespace"
	testCommon(t, cmdArgs, nil, expected, true)

	//create clusterPair and try again
	createClusterPair(t, "clusterPair1", "default", "async-dr")
	expected = "Use the command `storkctl get failover failover-test-migrationschedule -n default` to get the status of the failover\n"
	testCommon(t, cmdArgs, nil, expected, false)
}
