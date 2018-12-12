// +build integrationtest

package integrationtest

import (
	"testing"

	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

func testMigration(t *testing.T) {
	t.Run("appMigrationTest", appMigrationTest)
}

// migrate all apps specified in migrateAppTest
func appMigrationTest(t *testing.T) {

	for _, appName := range migrateAppTest {
		// we create namespace as <appKey> - integration-test(migrationNamespace)
		migrNamespace := appName + "-" + migrationNamespace
		// schedule mysql app on cluster 1
		appCtxs, err := schedulerDriver.Schedule(migrationNamespace,
			scheduler.ScheduleOptions{AppKeys: []string{appName}})
		require.NoError(t, err, "Error scheduling task")
		require.Equal(t, 1, len(appCtxs), "Only one task should have started")
		err = schedulerDriver.WaitForRunning(appCtxs[0], defaultWaitTimeout, defaultWaitInterval)
		require.NoError(t, err, "Error waiting for pod to get to running state")

		// this is so <appName>-namespace is created
		// create, apply and validate cluster pair specs
		// create cluster pair in same namespace as <migrNamespace>
		pairCtxs, err := scheduleClusterPair(migrNamespace)
		require.NoError(t, err, "Error scheduling cluster pair")

		// apply migration specs
		// create migration in same namespace as <migrNamespace>
		migrationCtxs, err := schedulerDriver.Schedule(migrNamespace,
			scheduler.ScheduleOptions{AppKeys: []string{appName + "-migration"}})
		require.NoError(t, err, "Error scheduling migration specs")
		err = schedulerDriver.WaitForRunning(migrationCtxs[0], defaultWaitTimeout, defaultWaitInterval)
		require.NoError(t, err, "Error waiting for migration to get to Ready state")

		// wait on cluster 2 to get app pods running
		err = setRemoteConfig(remoteFilePath)
		require.NoError(t, err, "Error setting remote config")
		err = schedulerDriver.WaitForRunning(appCtxs[0], defaultWaitTimeout, defaultWaitInterval)
		require.NoError(t, err, "Error waiting for pod to get to running state")
		// destroy app on cluster 2
		destroyAndWait(t, appCtxs)

		// destroy app on cluster 1
		err = setRemoteConfig("")
		require.NoError(t, err, "Error setting remote config")
		destroyAndWait(t, appCtxs)

		// destroy CRD objects from cluster 1
		destroyAndWait(t, migrationCtxs)
		destroyAndWait(t, pairCtxs)
	}
	logrus.Info("App Migration test PASSED")
}
