// +build integrationtest

package integrationtest

import (
	"testing"

	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/stretchr/testify/require"
)

func testMigration(t *testing.T) {
	t.Run("deploymentTest", deploymentMigrationTest)
	t.Run("statefulsetTest", statefulsetMigrationTest)
	t.Run("statefulsetRuleTest", statefulsetMigrationRuleTest)
	t.Run("preExecRuleMissingTest", statefulsetMigrationRulePreExecMissingTest)
	t.Run("postExecRuleMissingTest", statefulsetMigrationRulePostExecMissingTest)
	t.Run("disallowedNamespaceTest", migrationDisallowedNamespaceTest)
	t.Run("failingPreExecRuleTest", migrationFailingPreExecRuleTest)
	t.Run("failingPostExecRuleTest", migrationFailingPostExecRuleTest)
}

func triggerMigrationTest(
	t *testing.T,
	instanceID string,
	appKey string,
	migrationAppKey string,
	migrationSuccessExpected bool,
) {
	var err error
	// schedule mysql app on cluster 1
	ctxs, err := schedulerDriver.Schedule(instanceID,
		scheduler.ScheduleOptions{AppKeys: []string{appKey}})
	require.NoError(t, err, "Error scheduling task")
	require.Equal(t, 1, len(ctxs), "Only one task should have started")

	err = schedulerDriver.WaitForRunning(ctxs[0], defaultWaitTimeout, defaultWaitInterval)
	require.NoError(t, err, "Error waiting for pod to get to running state")

	preMigrationCtx := ctxs[0].DeepCopy()

	// this is so mysql-1-pvc-namespace is created
	// create, apply and validate cluster pair specs
	err = scheduleClusterPair(ctxs[0])
	require.NoError(t, err, "Error scheduling cluster pair")

	// apply migration specs
	err = schedulerDriver.AddTasks(ctxs[0],
		scheduler.ScheduleOptions{AppKeys: []string{migrationAppKey}})
	require.NoError(t, err, "Error scheduling migration specs")

	err = schedulerDriver.WaitForRunning(ctxs[0], defaultWaitTimeout, defaultWaitInterval)
	if migrationSuccessExpected {
		require.NoError(t, err, "Error waiting for migration to get to Ready state")

		// wait on cluster 2 to get mysql pod running
		err = setRemoteConfig(remoteFilePath)
		require.NoError(t, err, "Error setting remote config")
		err = schedulerDriver.WaitForRunning(preMigrationCtx, defaultWaitTimeout, defaultWaitInterval)
		require.NoError(t, err, "Error waiting for pod to get to running state on remote cluster after migration")
		// destroy mysql app on cluster 2
		destroyAndWait(t, []*scheduler.Context{preMigrationCtx})
	} else {
		require.Error(t, err, "Expected migration to fail")
	}

	// destroy mysql app on cluster 1
	err = setRemoteConfig("")
	require.NoError(t, err, "Error setting remote config")
	destroyAndWait(t, ctxs)
}

func deploymentMigrationTest(t *testing.T) {
	triggerMigrationTest(
		t,
		"mysql-migration",
		"mysql-1-pvc",
		"mysql-migration",
		true,
	)
}

func statefulsetMigrationTest(t *testing.T) {
	triggerMigrationTest(
		t,
		"cassandra-migration",
		"cassandra",
		"cassandra-migration",
		true,
	)
}

func statefulsetMigrationRuleTest(t *testing.T) {
	triggerMigrationTest(
		t,
		"cassandra-migration-rule",
		"cassandra",
		"cassandra-migration-rule",
		true,
	)
}

func statefulsetMigrationRulePreExecMissingTest(t *testing.T) {
	triggerMigrationTest(
		t,
		"migration-pre-exec-missing",
		"mysql-1-pvc",
		"mysql-migration-pre-exec-missing",
		false,
	)
}
func statefulsetMigrationRulePostExecMissingTest(t *testing.T) {
	triggerMigrationTest(
		t,
		"migration-post-exec-missing",
		"mysql-1-pvc",
		"mysql-migration-post-exec-missing",
		false,
	)
}

func migrationDisallowedNamespaceTest(t *testing.T) {
	triggerMigrationTest(
		t,
		"migration-disallowed-namespace",
		"mysql-1-pvc",
		"mysql-migration-disallowed-ns",
		false,
	)
}

func migrationFailingPreExecRuleTest(t *testing.T) {
	triggerMigrationTest(
		t,
		"migration-failing-pre-exec-rule",
		"mysql-1-pvc",
		"mysql-migration-failing-pre-exec",
		false,
	)
}

func migrationFailingPostExecRuleTest(t *testing.T) {
	triggerMigrationTest(
		t,
		"migration-failing-post-exec-rule",
		"mysql-1-pvc",
		"mysql-migration-failing-post-exec",
		false,
	)
}
