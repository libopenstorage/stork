// +build integrationtest

package integrationtest

import (
	"testing"

	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/stretchr/testify/require"
)

func testSnapshot(t *testing.T) {
	t.Run("simpleSnapshotTest", simpleSnapshotTest)
}

func simpleSnapshotTest(t *testing.T) {
	ctxs, err := schedulerDriver.Schedule(generateInstanceID(t, ""),
		scheduler.ScheduleOptions{AppKeys: []string{"mysql-snap-restore"}})
	require.NoError(t, err, "Error scheduling task")
	require.Equal(t, 1, len(ctxs), "Only one task should have started")

	err = schedulerDriver.WaitForRunning(ctxs[0])
	require.NoError(t, err, "Error waiting for pod to get to running state")

	scheduledNodes, err := schedulerDriver.GetNodesForApp(ctxs[0])
	require.NoError(t, err, "Error getting node for app")
	require.Equal(t, 1, len(scheduledNodes), "App should be scheduled on one node")

	err = schedulerDriver.InspectVolumes(ctxs[0])
	require.NoError(t, err, "Error waiting for volumes")
	volumeNames := getVolumeNames(t, ctxs[0])
	require.Equal(t, 2, len(volumeNames), "Should only have two volumes")

	snapVolInfo, err := storkVolumeDriver.InspectVolume("mysql-snapshot")
	require.NoError(t, err, "Error getting snapshot volume")
	require.NotNil(t, snapVolInfo.ParentID, "ParentID is nil for snapshot")

	parentVolInfo, err := storkVolumeDriver.InspectVolume(snapVolInfo.ParentID)
	require.NoError(t, err, "Error getting snapshot parent volume")

	parentVolName := parentVolInfo.VolumeName
	var cloneVolName string

	found := false
	for _, volume := range volumeNames {
		if volume == parentVolName {
			found = true
		} else {
			cloneVolName = volume
		}
	}
	require.True(t, found, "Parent volume (%v) not found in list of volumes: %v", parentVolName, volumeNames)

	cloneVolInfo, err := storkVolumeDriver.InspectVolume(cloneVolName)
	require.NoError(t, err, "Error getting clone volume")
	require.Equal(t, snapVolInfo.VolumeID, cloneVolInfo.ParentID, "Clone volume does not have snapshot as parent")

	verifyScheduledNode(t, scheduledNodes[0], volumeNames)

	destroyAndWait(t, ctxs)
}
