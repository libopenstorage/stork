// +build integrationtest

package integrationtest

import (
	"fmt"
	"strings"
	"testing"

	crdv1 "github.com/kubernetes-incubator/external-storage/snapshot/pkg/apis/crd/v1"
	"github.com/portworx/sched-ops/k8s"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/stretchr/testify/require"
)

func testSnapshot(t *testing.T) {
	t.Run("simpleSnapshotTest", simpleSnapshotTest)
	t.Run("groupSnapshotTest", groupSnapshotTest)
	t.Run("cloudSnapshotTest", cloudSnapshotTest)
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
	require.Equal(t, 3, len(volumeNames), "Should only have two volumes and a snapshot")

	snapshotName := ""
	for _, volume := range volumeNames {
		if strings.HasPrefix(volume, "snapshot") {
			snapshotName = volume
		}
	}
	require.NotEmpty(t, snapshotName, "snapshot not found in list of volumes: %v", volumeNames)

	snapVolInfo, err := storkVolumeDriver.InspectVolume(snapshotName)
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
		} else if volume != snapVolInfo.VolumeName {
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

func groupSnapshotTest(t *testing.T) {
	ctxs, err := schedulerDriver.Schedule(generateInstanceID(t, ""),
		scheduler.ScheduleOptions{AppKeys: []string{"mysql-snap-group"}})
	require.NoError(t, err, "Error scheduling task")
	require.Len(t, ctxs, 1, "Only one task should have started")

	err = schedulerDriver.WaitForRunning(ctxs[0])
	require.NoError(t, err, "Error waiting for pod to get to running state")

	scheduledNodes, err := schedulerDriver.GetNodesForApp(ctxs[0])
	require.NoError(t, err, "Error getting node for app")
	require.Len(t, scheduledNodes, 1, "App should be scheduled on one node")

	err = schedulerDriver.InspectVolumes(ctxs[0])
	require.NoError(t, err, "Error waiting for volumes")

	allVolumeNames := getVolumeNames(t, ctxs[0])
	require.Len(t, allVolumeNames, 3, "Should only have two volumes and 1 snapshot")

	dataVolumesNames := make([]string, 0)
	for _, volume := range allVolumeNames {
		if !strings.HasPrefix(volume, "snapshot") {
			dataVolumesNames = append(dataVolumesNames, volume)
		}
	}

	require.Len(t, dataVolumesNames, 2, "should have only 2 data volumes")

	snaps, err := schedulerDriver.GetSnapshots(ctxs[0])
	require.NoError(t, err, "failed to get snapshots")
	require.Len(t, snaps, 1, "should have received exactly one snapshot")

	for _, snap := range snaps {
		s, err := k8s.Instance().GetSnapshot(snap.Name, snap.Namespace)
		require.NoError(t, err, "failed to query snapshot object")
		require.NotNil(t, s, "got nil snapshot object from k8s api")

		require.NotEmpty(t, s.Spec.SnapshotDataName, "snapshot object has empty snapshot data field")

		sData, err := k8s.Instance().GetSnapshotData(s.Spec.SnapshotDataName)
		require.NoError(t, err, "failed to query snapshot data object")

		snapType := sData.Spec.PortworxSnapshot.SnapshotType
		require.Equal(t, snapType, crdv1.PortworxSnapshotTypeLocal)

		require.NotEmpty(t, sData.Spec.PortworxSnapshot.SnapshotData, "group snapshot data has empty snapshot data name in portworx source field")

		childSnapDataNames := strings.Split(sData.Spec.PortworxSnapshot.SnapshotData, ",")
		require.Len(t, childSnapDataNames, 2, "should have exactly 2 child snapshots for the group snapshot")

		for _, childSnapDataName := range childSnapDataNames {
			childSnapData, err := k8s.Instance().GetSnapshotData(childSnapDataName)
			require.NoError(t, err, "failed to get volumeSnapshotdata object")

			childSnapID := childSnapData.Spec.PortworxSnapshot.SnapshotID
			require.NotEmpty(t, childSnapID, "got empty snapshot ID in volume snapshot data")

			snapVolInfo, err := storkVolumeDriver.InspectVolume(childSnapID)
			require.NoError(t, err, "Error getting snapshot volume")
			require.NotNil(t, snapVolInfo, fmt.Sprintf("got empty volume info for vol ID: %s", childSnapID))
			require.NotNil(t, snapVolInfo.ParentID, "ParentID is nil for snapshot")

			parentVolInfo, err := storkVolumeDriver.InspectVolume(snapVolInfo.ParentID)
			require.NoError(t, err, "Error getting snapshot parent volume")
			require.NotNil(t, parentVolInfo, fmt.Sprintf("got empty volume info for vol ID: %s", snapVolInfo.ParentID))

			// check if parent vol is correct
			found := false
			parentVolName := parentVolInfo.VolumeName
			for _, dataVol := range dataVolumesNames {
				if dataVol == parentVolName {
					found = true
					break
				}
			}
			require.True(t, found, "Parent volume (%s) not found in list of volumes: %v", parentVolName, dataVolumesNames)
		}
	}
	verifyScheduledNode(t, scheduledNodes[0], dataVolumesNames)
	destroyAndWait(t, ctxs)
}

func cloudSnapshotTest(t *testing.T) {
	ctxs, err := schedulerDriver.Schedule(generateInstanceID(t, ""),
		scheduler.ScheduleOptions{AppKeys: []string{"mysql-cloudsnap-restore"}})
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
	require.Equal(t, 3, len(volumeNames), "Should only have two volumes and a snapshot")

	dataVolumesNames := make([]string, 0)
	for _, volume := range volumeNames {
		if !strings.HasPrefix(volume, "snapshot") {
			dataVolumesNames = append(dataVolumesNames, volume)
		}
	}

	require.Len(t, dataVolumesNames, 2, "should have only 2 data volumes")

	snaps, err := schedulerDriver.GetSnapshots(ctxs[0])
	require.NoError(t, err, "failed to get snapshots")
	require.Len(t, snaps, 1, "should have received exactly one snapshot")

	for _, snap := range snaps {
		s, err := k8s.Instance().GetSnapshot(snap.Name, snap.Namespace)
		require.NoError(t, err, "failed to query snapshot object")
		require.NotNil(t, s, "got nil snapshot object from k8s api")

		require.NotEmpty(t, s.Spec.SnapshotDataName, "snapshot object has empty snapshot data field")

		sData, err := k8s.Instance().GetSnapshotData(s.Spec.SnapshotDataName)
		require.NoError(t, err, "failed to query snapshot data object")

		snapType := sData.Spec.PortworxSnapshot.SnapshotType
		require.Equal(t, snapType, crdv1.PortworxSnapshotTypeCloud)
	}

	verifyScheduledNode(t, scheduledNodes[0], dataVolumesNames)
	destroyAndWait(t, ctxs)
}
