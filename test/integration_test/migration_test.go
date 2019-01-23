// +build integrationtest

package integrationtest

import (
	"testing"

	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/stretchr/testify/require"
)

func testMigration(t *testing.T) {
	t.Run("deploymentTest", deploymentMigrationTest)
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
