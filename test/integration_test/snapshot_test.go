// +build integrationtest

package integrationtest

import (
	"fmt"
	"regexp"
	"testing"
	"time"

	crdv1 "github.com/kubernetes-incubator/external-storage/snapshot/pkg/apis/crd/v1"
	"github.com/portworx/sched-ops/k8s"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/util/wait"
)

var snapRuleFailRegex = regexp.MustCompile("^snapshot failed due to err.+(failed to validate snap rule|failed to run (pre|post)-snap rule).+")

func testSnapshot(t *testing.T) {
	t.Run("simpleSnapshotTest", simpleSnapshotTest)
	t.Run("cloudSnapshotTest", cloudSnapshotTest)
	t.Run("snapshotScaleTest", snapshotScaleTest)
	t.Run("groupSnapshotTest", groupSnapshotTest)
	t.Run("groupSnapshotScaleTest", groupSnapshotScaleTest)
}

func simpleSnapshotTest(t *testing.T) {
	ctx := createSnapshot(t, []string{"mysql-snap-restore"})
	verifySnapshot(t, ctx, "mysql-data", defaultWaitTimeout)
	destroyAndWait(t, ctx)
}

func verifyFailedSnapshot(snapName, snapNamespace string) error {
	failedSnapCheckBackoff := wait.Backoff{
		Duration: 5 * time.Second,
		Factor:   1,
		Steps:    24, // 2 minutes should be enough for the snap to fail
	}

	t := func() (bool, error) {
		snapObj, err := k8s.Instance().GetSnapshot(snapName, snapNamespace)
		if err != nil {
			return false, err
		}

		if snapObj.Status.Conditions == nil {
			return false, nil // conditions not yet populated
		}

		for _, cond := range snapObj.Status.Conditions {
			if cond.Type == crdv1.VolumeSnapshotConditionError {
				if snapRuleFailRegex.MatchString(cond.Message) {
					logrus.Infof("verified that snapshot has failed as expected due to: %s", cond.Message)
					return true, nil
				}
			}
		}

		return false, nil
	}

	return wait.ExponentialBackoff(failedSnapCheckBackoff, t)
}

func cloudSnapshotTest(t *testing.T) {
	ctxs, err := schedulerDriver.Schedule(generateInstanceID(t, ""),
		scheduler.ScheduleOptions{AppKeys: []string{"mysql-cloudsnap-restore"}})
	require.NoError(t, err, "Error scheduling task")
	require.Equal(t, 1, len(ctxs), "Only one task should have started")

	err = schedulerDriver.WaitForRunning(ctxs[0], defaultWaitTimeout, defaultWaitInterval)
	require.NoError(t, err, "Error waiting for pod to get to running state")

	scheduledNodes, err := schedulerDriver.GetNodesForApp(ctxs[0])
	require.NoError(t, err, "Error getting node for app")
	require.Equal(t, 1, len(scheduledNodes), "App should be scheduled on one node")

	err = schedulerDriver.InspectVolumes(ctxs[0], defaultWaitTimeout, defaultWaitInterval)
	require.NoError(t, err, "Error waiting for volumes")
	volumeNames := getVolumeNames(t, ctxs[0])
	require.Equal(t, 3, len(volumeNames), "Should only have two volumes and a snapshot")

	dataVolumesNames, dataVolumesInUse := parseDataVolumes(t, "mysql-data", ctxs[0])
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

	fmt.Printf("checking dataVolumesInUse: %v\n", dataVolumesInUse)
	verifyScheduledNode(t, scheduledNodes[0], dataVolumesInUse)
	destroyAndWait(t, ctxs)
}

func groupSnapshotTest(t *testing.T) {
	ctxsToDestroy := make([]*scheduler.Context, 0)
	// Positive tests
	ctxs := createGroupsnaps(t)
	ctxsToDestroy = append(ctxsToDestroy, ctxs...)

	for _, ctx := range ctxs {
		verifyGroupSnapshot(t, ctx, defaultWaitTimeout)
	}

	// Negative
	ctxs, err := schedulerDriver.Schedule(generateInstanceID(t, ""),
		scheduler.ScheduleOptions{AppKeys: []string{"mysql-snap-group-fail"}})
	require.NoError(t, err, "Error scheduling task")
	require.Len(t, ctxs, 1, "Only one task should have started")

	for _, ctx := range ctxs {
		err = schedulerDriver.WaitForRunning(ctx, defaultWaitTimeout, defaultWaitInterval)
		require.NoError(t, err, "Error waiting for pod to get to running state")

		snaps, err := schedulerDriver.GetSnapshots(ctx)
		require.Error(t, err, "expected to get error when fetching snapahots")
		require.Nil(t, snaps, "expected empty snapshots")
	}
	ctxsToDestroy = append(ctxsToDestroy, ctxs...)

	destroyAndWait(t, ctxsToDestroy)
}

func groupSnapshotScaleTest(t *testing.T) {
	allContexts := make([]*scheduler.Context, 0)
	// Triggers 2 snaps, so use half the count in the loop
	for i := 0; i < snapshotScaleCount/2; i++ {
		ctxs := createGroupsnaps(t)
		allContexts = append(allContexts, ctxs...)
	}

	timeout := defaultWaitTimeout
	// Increase the timeout if scale is more than or equal 10
	if snapshotScaleCount >= 10 {
		timeout *= time.Duration((snapshotScaleCount / 10) + 1)
	}

	for _, ctx := range allContexts {
		verifyGroupSnapshot(t, ctx, timeout)
	}

	destroyAndWait(t, allContexts)
}

func createGroupsnaps(t *testing.T) []*scheduler.Context {
	ctxs, err := schedulerDriver.Schedule(generateInstanceID(t, ""),
		scheduler.ScheduleOptions{AppKeys: []string{
			"mysql-localsnap-rule",  // tests local group snapshots with a pre exec rule
			"mysql-cloudsnap-group", // tests cloud group snapshots
		}})
	require.NoError(t, err, "Error scheduling task")
	require.Len(t, ctxs, 2, "Only one task should have started")

	return ctxs
}

func verifyGroupSnapshot(t *testing.T, ctx *scheduler.Context, waitTimeout time.Duration) {
	err := schedulerDriver.WaitForRunning(ctx, waitTimeout, defaultWaitInterval)
	require.NoError(t, err, "Error waiting for pod to get to running state")

	err = schedulerDriver.InspectVolumes(ctx, waitTimeout, defaultWaitInterval)
	require.NoError(t, err, "Error validating storage components")
}

func parseDataVolumes(
	t *testing.T,
	pvcInUseByTest string,
	ctx *scheduler.Context) ([]string, []string) {
	allVolumes, err := schedulerDriver.GetVolumes(ctx)
	require.NoError(t, err, "failed to get volumes")

	dataVolumesNames := make([]string, 0)
	dataVolumesInUse := make([]string, 0)
	for _, v := range allVolumes {
		pvc, err := k8s.Instance().GetPersistentVolumeClaim(v.Name, v.Namespace)
		require.NoError(t, err, "failed to get PVC")

		volName, err := k8s.Instance().GetVolumeForPersistentVolumeClaim(pvc)
		require.NoError(t, err, "failed to get PV name")
		dataVolumesNames = append(dataVolumesNames, volName)

		if pvc.GetName() == pvcInUseByTest {
			dataVolumesInUse = append(dataVolumesInUse, volName)
		}
	}

	require.Len(t, dataVolumesInUse, 1, "should have only 1 data volume in use")

	return dataVolumesNames, dataVolumesInUse
}

func createSnapshot(t *testing.T, appKeys []string) []*scheduler.Context {
	ctx, err := schedulerDriver.Schedule(generateInstanceID(t, ""),
		scheduler.ScheduleOptions{AppKeys: appKeys})
	require.NoError(t, err, "Error scheduling task")
	require.Equal(t, 1, len(ctx), "Only one task should have started")
	return ctx
}

func verifySnapshot(t *testing.T, ctxs []*scheduler.Context, pvcInUseByTest string, waitTimeout time.Duration) {
	err := schedulerDriver.WaitForRunning(ctxs[0], waitTimeout, defaultWaitInterval)
	require.NoError(t, err, "Error waiting for pod to get to running state")

	scheduledNodes, err := schedulerDriver.GetNodesForApp(ctxs[0])
	require.NoError(t, err, "Error getting node for app")
	require.Equal(t, 1, len(scheduledNodes), "App should be scheduled on one node")

	err = schedulerDriver.InspectVolumes(ctxs[0], waitTimeout, defaultWaitInterval)
	require.NoError(t, err, "Error waiting for volumes")
	volumeNames := getVolumeNames(t, ctxs[0])
	require.Equal(t, 3, len(volumeNames), "Should only have two volumes and a snapshot")

	dataVolumesNames, dataVolumesInUse := parseDataVolumes(t, pvcInUseByTest, ctxs[0])
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

		snapID := sData.Spec.PortworxSnapshot.SnapshotID
		require.NotEmpty(t, snapID, "got empty snapshot ID in volume snapshot data")

		snapVolInfo, err := storkVolumeDriver.InspectVolume(snapID)
		require.NoError(t, err, "Error getting snapshot volume")
		require.NotNil(t, snapVolInfo.ParentID, "ParentID is nil for snapshot")

		parentVolInfo, err := storkVolumeDriver.InspectVolume(snapVolInfo.ParentID)
		require.NoError(t, err, "Error getting snapshot parent volume")

		parentVolName := parentVolInfo.VolumeName
		var cloneVolName string

		found := false
		for _, volume := range dataVolumesNames {
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
	}

	verifyScheduledNode(t, scheduledNodes[0], dataVolumesInUse)
}

func snapshotScaleTest(t *testing.T) {
	ctxs := make([][]*scheduler.Context, snapshotScaleCount)
	for i := 0; i < snapshotScaleCount; i++ {
		ctxs[i] = createSnapshot(t, []string{"mysql-snap-restore"})
	}

	timeout := defaultWaitTimeout
	// Increase the timeout if scale is more than 10
	if snapshotScaleCount > 10 {
		timeout *= time.Duration((snapshotScaleCount / 10) + 1)
	}
	for i := 0; i < snapshotScaleCount; i++ {
		verifySnapshot(t, ctxs[i], "mysql-data", timeout)
	}
	for i := 0; i < snapshotScaleCount; i++ {
		destroyAndWait(t, ctxs[i])
	}
}
