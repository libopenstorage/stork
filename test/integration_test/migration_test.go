// +build integrationtest

package integrationtest

import (
	"testing"

	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/stretchr/testify/require"
)

func testMigration(t *testing.T) {
	t.Run("sanityMigrationTest", sanityMigrationTest)
	//t.Run("wordpressMigrationTest", wordpressMigrationTest)
}

func sanityMigrationTest(t *testing.T) {
	var err error
	// schedule mysql app on cluster 1
	// we create namespace as <appKey> - integration-test(migrationNamespace)
	mysqlMigration := "mysql-1-pvc" + migrationNamespace
	mysqlCtxs, err := schedulerDriver.Schedule(migrationNamespace,
		scheduler.ScheduleOptions{AppKeys: []string{"mysql-1-pvc"}})
	require.NoError(t, err, "Error scheduling task")
	require.Equal(t, 1, len(mysqlCtxs), "Only one task should have started")

	err = schedulerDriver.WaitForRunning(mysqlCtxs[0], defaultWaitTimeout, defaultWaitInterval)
	require.NoError(t, err, "Error waiting for pod to get to running state")

	// this is so mysql-1-pvc-namespace is created
	// create, apply and validate cluster pair specs
	// create cluster pair in same namespace as mysql-1-pvc
	pairCtxs, err := scheduleClusterPair(mysqlMigration)
	require.NoError(t, err, "Error scheduling cluster pair")

	// apply migration specs
	// create migration in same namespace as mysql-1-pvc
	migrationCtxs, err := schedulerDriver.Schedule(mysqlMigration,
		scheduler.ScheduleOptions{AppKeys: []string{"mysql-migration"}})
	require.NoError(t, err, "Error scheduling migration specs")

	err = schedulerDriver.WaitForRunning(migrationCtxs[0], defaultWaitTimeout, defaultWaitInterval)
	require.NoError(t, err, "Error waiting for migration to get to Ready state")

	// wait on cluster 2 to get mysql pod running
	err = setRemoteConfig(remoteFilePath)
	require.NoError(t, err, "Error setting remote config")
	err = schedulerDriver.WaitForRunning(mysqlCtxs[0], defaultWaitTimeout, defaultWaitInterval)
	require.NoError(t, err, "Error waiting for pod to get to running state")
	// destroy mysql app on cluster 2
	destroyAndWait(t, mysqlCtxs)

	// destroy mysql app on cluster 1
	err = setRemoteConfig("")
	require.NoError(t, err, "Error setting remote config")
	destroyAndWait(t, mysqlCtxs)

	// destroy CRD objects
	destroyAndWait(t, migrationCtxs)
	destroyAndWait(t, pairCtxs)
}
