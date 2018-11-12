// +build integrationtest

package integrationtest

import (
	"testing"

	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

func testMigration(t *testing.T) {
	logrus.Info("Basic sanity tests for cloud migration")
	t.Run("sanityMigrationTest", sanityMigrationTest)
}

func sanityMigrationTest(t *testing.T) {
	var err error
	// create, apply and validate cluster pair specs
	pairCtxs, err := scheduleClusterPair()
	require.NoError(t, err, "Error scheduling cluster pair")

	// schedule mysql app on cluster 1
	mySQLCtxs, err := schedulerDriver.Schedule("singlemysql",
		scheduler.ScheduleOptions{AppKeys: []string{"mysql-1-pvc"}})
	require.NoError(t, err, "Error scheduling task")
	require.Equal(t, 1, len(mySQLCtxs), "Only one task should have started")

	err = schedulerDriver.WaitForRunning(mySQLCtxs[0], defaultWaitTimeout, defaultWaitInterval)
	require.NoError(t, err, "Error waiting for pod to get to running state")

	// apply migration specs
	migrsCtxs, err := schedulerDriver.Schedule("migration",
		scheduler.ScheduleOptions{AppKeys: []string{"migration"}})
	require.NoError(t, err, "Error scheduling migration specs")

	err = schedulerDriver.WaitForRunning(migrsCtxs[0], defaultWaitTimeout, defaultWaitInterval)
	require.NoError(t, err, "Error waiting for migration to get to Ready state")

	// wait on cluster 2 to get mysql pod running
	err = setRemoteConfig(remoteFilePath)
	require.NoError(t, err, "Error setting remote config")
	err = schedulerDriver.WaitForRunning(mySQLCtxs[0], defaultWaitTimeout, defaultWaitInterval)
	require.NoError(t, err, "Error waiting for pod to get to running state")
	// destroy mysql app on cluster 2
	destroyAndWait(t, mySQLCtxs)

	// destroy mysql app on cluster 1
	err = setRemoteConfig("")
	require.NoError(t, err, "Error setting remote config")
	destroyAndWait(t, mySQLCtxs)

	// destroy CRD objects
	destroyAndWait(t, migrsCtxs)
	destroyAndWait(t, pairCtxs)
}
