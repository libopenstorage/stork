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
	t.Run("labelSelectorTest", migrationLabelSelectorTest)
}

func triggerMigrationTest(
	t *testing.T,
	instanceID string,
	appKey string,
	additionalAppKeys []string,
	migrationAppKey string,
	migrationSuccessExpected bool,
	migrateAllAppsExpected bool,
) {
	var err error
	// schedule the app on cluster 1
	ctxs, err := schedulerDriver.Schedule(instanceID,
		scheduler.ScheduleOptions{AppKeys: []string{appKey}})
	require.NoError(t, err, "Error scheduling task")
	require.Equal(t, 1, len(ctxs), "Only one task should have started")

	err = schedulerDriver.WaitForRunning(ctxs[0], defaultWaitTimeout, defaultWaitInterval)
	require.NoError(t, err, "Error waiting for app to get to running state")

	preMigrationCtx := ctxs[0].DeepCopy()

	if len(additionalAppKeys) > 0 {
		err = schedulerDriver.AddTasks(ctxs[0],
			scheduler.ScheduleOptions{AppKeys: additionalAppKeys})
		require.NoError(t, err, "Error scheduling additional apps")
		err = schedulerDriver.WaitForRunning(ctxs[0], defaultWaitTimeout, defaultWaitInterval)
		require.NoError(t, err, "Error waiting for additional apps to get to running state")
	}
	if migrateAllAppsExpected {
		preMigrationCtx = ctxs[0].DeepCopy()
	}
	allAppsCtx := ctxs[0].DeepCopy()
	// create, apply and validate cluster pair specs
	err = scheduleClusterPair(ctxs[0])
	require.NoError(t, err, "Error scheduling cluster pair")

	// apply migration specs
	err = schedulerDriver.AddTasks(ctxs[0],
		scheduler.ScheduleOptions{AppKeys: []string{migrationAppKey}})
	require.NoError(t, err, "Error scheduling migration specs")

	// Reset config in case of error
	defer func() {
		err = setRemoteConfig("")
		require.NoError(t, err, "Error resetting remote config")
	}()

	timeout := defaultWaitTimeout
	if !migrationSuccessExpected {
		timeout = timeout / 2
	}
	err = schedulerDriver.WaitForRunning(ctxs[0], timeout, defaultWaitInterval)
	if migrationSuccessExpected {
		require.NoError(t, err, "Error waiting for migration to get to Ready state")

		// wait on cluster 2 for the app to be running
		err = setRemoteConfig(remoteFilePath)
		require.NoError(t, err, "Error setting remote config")
		err = schedulerDriver.WaitForRunning(preMigrationCtx, defaultWaitTimeout, defaultWaitInterval)
		require.NoError(t, err, "Error waiting for pod to get to running state on remote cluster after migration")
		if !migrateAllAppsExpected {
			err = schedulerDriver.WaitForRunning(allAppsCtx, defaultWaitTimeout/2, defaultWaitInterval)
			require.Error(t, err, "All apps shouldn't have been migrated")
		}
		// destroy app on cluster 2
		destroyAndWait(t, []*scheduler.Context{preMigrationCtx})
	} else {
		require.Error(t, err, "Expected migration to fail")
	}

	// destroy app on cluster 1
	err = setRemoteConfig("")
	require.NoError(t, err, "Error resetting remote config")
	destroyAndWait(t, ctxs)
}

func deploymentMigrationTest(t *testing.T) {
	triggerMigrationTest(
		t,
		"mysql-migration",
		"mysql-1-pvc",
		nil,
		"mysql-migration",
		true,
		true,
	)
}

func statefulsetMigrationTest(t *testing.T) {
	triggerMigrationTest(
		t,
		"cassandra-migration",
		"cassandra",
		nil,
		"cassandra-migration",
		true,
		true,
	)
}

func statefulsetMigrationRuleTest(t *testing.T) {
	triggerMigrationTest(
		t,
		"cassandra-migration-rule",
		"cassandra",
		nil,
		"cassandra-migration-rule",
		true,
		true,
	)
}

func statefulsetMigrationRulePreExecMissingTest(t *testing.T) {
	triggerMigrationTest(
		t,
		"migration-pre-exec-missing",
		"mysql-1-pvc",
		nil,
		"mysql-migration-pre-exec-missing",
		false,
		true,
	)
}
func statefulsetMigrationRulePostExecMissingTest(t *testing.T) {
	triggerMigrationTest(
		t,
		"migration-post-exec-missing",
		"mysql-1-pvc",
		nil,
		"mysql-migration-post-exec-missing",
		false,
		true,
	)
}

func migrationDisallowedNamespaceTest(t *testing.T) {
	triggerMigrationTest(
		t,
		"migration-disallowed-namespace",
		"mysql-1-pvc",
		nil,
		"mysql-migration-disallowed-ns",
		false,
		true,
	)
}

func migrationFailingPreExecRuleTest(t *testing.T) {
	triggerMigrationTest(
		t,
		"migration-failing-pre-exec-rule",
		"mysql-1-pvc",
		nil,
		"mysql-migration-failing-pre-exec",
		false,
		true,
	)
}

func migrationFailingPostExecRuleTest(t *testing.T) {
	triggerMigrationTest(
		t,
		"migration-failing-post-exec-rule",
		"mysql-1-pvc",
		nil,
		"mysql-migration-failing-post-exec",
		false,
		true,
	)
}

func migrationLabelSelectorTest(t *testing.T) {
	triggerMigrationTest(
		t,
		"migration-label-selector-test",
		"cassandra",
		[]string{"mysql-1-pvc"},
		"label-selector-migration",
		true,
		false,
	)
}
