// +build integrationtest

package integrationtest

import (
	"testing"

	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

func testBasicCloudMigration(t *testing.T) {
	logrus.Info("Basic sanity tests for cloud migration")
	t.Run("sanityClusterPairTest", sanityClusterPairTest)
	t.Run("sanityMigrationTest", sanityMigrationTest)
}

func sanityClusterPairTest(t *testing.T) {
	var err error
	err = dumpRemoteKubeConfig(remoteConfig)
	require.NoError(t, err, "Error writing to clusterpair.yml: ")

	info, err := volumeDriver.GetClusterPairingInfo()
	require.NoError(t, err, "Error writing to clusterpair.yml: ")

	logrus.Infof("recieved %v", info)
	err = createClusterPair()
	require.NoError(t, err, "Error creating cluster Spec")

	err = schedulerDriver.RescanSpecs("./specs")
	require.NoError(t, err, "Unable to parse spec dir")

	_, err = schedulerDriver.Schedule("singlemysql",
		scheduler.ScheduleOptions{AppKeys: []string{"cluster-pair"}})
	require.NoError(t, err, "Error applying clusterpair")

	logrus.Info("Validated Cluster Pair Specs")
}

// apply cloudmigration spec and check status of migrated app
func sanityMigrationTest(t *testing.T) {
	//  schedule mysql app
	ctxs, err := schedulerDriver.Schedule("singlemysql",
		scheduler.ScheduleOptions{AppKeys: []string{"mysql-1-pvc"}})
	require.NoError(t, err, "Error scheduling task")
	require.Equal(t, 1, len(ctxs), "Only one task should have started")

	err = schedulerDriver.WaitForRunning(ctxs[0], defaultWaitTimeout, defaultWaitInterval)
	require.NoError(t, err, "Error waiting for pod to get to running state")

	// Apply cluster pair spec and check status
	_, err = schedulerDriver.Schedule("migrs",
		scheduler.ScheduleOptions{AppKeys: []string{"migration"}})
	require.NoError(t, err, "Error applying migration specs")

	// wait on cluster 2 to get mysql pod running
	err = setRemoteConfig(remoteFilePath)
	require.NoError(t, err, "Error setting remote config")

	err = schedulerDriver.WaitForRunning(ctxs[0], defaultWaitTimeout, defaultWaitInterval)
	require.NoError(t, err, "Error waiting for pod to get to running state")
	destroyAndWait(t, ctxs)

	// destroy mysql app on cluster 1
	err = setRemoteConfig("")
	require.NoError(t, err, "Error setting remote config")
	destroyAndWait(t, ctxs)
}
