// +build integrationtest

package integrationtest

import (
	"testing"
	"time"

	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/stretchr/testify/require"
)

func testSnapshotRestore(t *testing.T) {
	err := setSourceKubeConfig()
	require.NoError(t, err, "failed to set kubeconfig to source cluster: %v", err)

	t.Run("simpleSnapshotRestoreTest", simpleSnapshotRestoreTest)
	if !testing.Short() {
		t.Run("groupSnapshotRestoreTest", groupSnapshotRestoreTest)
		t.Run("cloudSnapshotRestoreTest", cloudSnapshotRestoreTest)
		t.Run("groupCloudSnapshotRestoreTest", groupCloudSnapshotRestoreTest)
	}

	err = setRemoteConfig("")
	require.NoError(t, err, "setting kubeconfig to default failed")
}

func createInPlaceRestore(t *testing.T, namespace string, appKeys []string) []*scheduler.Context {
	ctxs, err := schedulerDriver.Schedule(namespace,
		scheduler.ScheduleOptions{AppKeys: appKeys})
	require.NoError(t, err, "Error scheduling task")
	require.Equal(t, 1, len(ctxs), "Only one task should have started")
	return ctxs
}

func verifyInPlaceSnapshotRestore(t *testing.T, ctx []*scheduler.Context, startTime time.Time, timeout time.Duration) {
	err := schedulerDriver.WaitForRunning(ctx[0], timeout, defaultWaitInterval)
	require.NoError(t, err, "Error waiting for task")
	err = schedulerDriver.ValidateVolumeSnapshotRestore(ctx[0], startTime)
	require.NoError(t, err, "Error validating volumesnapshotRestore")
}

func simpleSnapshotRestoreTest(t *testing.T) {
	// foldername + name as namespaces
	snapCtx, err := schedulerDriver.Schedule("splocal",
		scheduler.ScheduleOptions{AppKeys: []string{"mysql-snap-restore"}})
	require.NoError(t, err, "Error scheduling task")
	require.Equal(t, 1, len(snapCtx), "Only one task should have started")
	verifySnapshot(t, snapCtx, "mysql-data", defaultWaitTimeout)
	startTime := time.Now()
	restoreCtx := createInPlaceRestore(t, "splocal", []string{"mysql-snap-inplace-restore"})
	verifyInPlaceSnapshotRestore(t, restoreCtx, startTime, defaultWaitTimeout)

	// cleanup test
	destroyAndWait(t, snapCtx)
	destroyAndWait(t, restoreCtx)
}

func groupSnapshotRestoreTest(t *testing.T) {
	snapCtx, err := schedulerDriver.Schedule("spgroup",
		scheduler.ScheduleOptions{AppKeys: []string{"mysql-2-pvc"}})
	require.NoError(t, err, "Error scheduling task")
	require.Len(t, snapCtx, 1, "Only one task should have started")
	verifyGroupSnapshot(t, snapCtx[0], defaultWaitTimeout)

	err = schedulerDriver.AddTasks(snapCtx[0],
		scheduler.ScheduleOptions{AppKeys: []string{"mysql-localsnap-group"}})
	require.NoError(t, err, "Error scheduling task")

	// restore local groupsnapshot
	startTime := time.Now()
	restoreCtx := createInPlaceRestore(t, "spgroup", []string{"mysql-snap-inplace-group-restore"})
	verifyInPlaceSnapshotRestore(t, restoreCtx, startTime, defaultWaitTimeout)

	// cleanup test
	destroyAndWait(t, snapCtx)
	destroyAndWait(t, restoreCtx)
}

func cloudSnapshotRestoreTest(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test: cloudSnapshotRestoreTest.")
	}
	snapCtx, err := schedulerDriver.Schedule("cslocal",
		scheduler.ScheduleOptions{AppKeys: []string{"mysql-cloudsnap-restore"}})
	require.NoError(t, err, "Error scheduling task")
	require.Equal(t, 1, len(snapCtx), "Only one task should have started")

	err = schedulerDriver.WaitForRunning(snapCtx[0], defaultWaitTimeout, defaultWaitInterval)
	require.NoError(t, err, "Error waiting for pod to get to running state")

	restoreCtx := createInPlaceRestore(t, "cslocal", []string{"mysql-snap-inplace-cloud-restore"})
	startTime := time.Now()
	verifyInPlaceSnapshotRestore(t, restoreCtx, startTime, defaultWaitTimeout)

	// cleanup test
	destroyAndWait(t, snapCtx)
	destroyAndWait(t, restoreCtx)
}

func groupCloudSnapshotRestoreTest(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test: groupCloudSnapshotRestoreTest.")
	}
	snapCtx, err := schedulerDriver.Schedule("csgroup",
		scheduler.ScheduleOptions{AppKeys: []string{"mysql-cloudsnap-group-restore"}})
	require.NoError(t, err, "Error scheduling task")
	require.Equal(t, 1, len(snapCtx), "Only one task should have started")

	verifyGroupSnapshot(t, snapCtx[0], defaultWaitTimeout)

	err = schedulerDriver.WaitForRunning(snapCtx[0], defaultWaitTimeout, defaultWaitInterval)
	require.NoError(t, err, "Error waiting for pod to get to running state")

	restoreCtx := createInPlaceRestore(t, "csgroup", []string{"mysql-groupsnap-inplace-cloud-restore"})
	startTime := time.Now()
	verifyInPlaceSnapshotRestore(t, restoreCtx, startTime, defaultWaitTimeout)

	// cleanup test
	destroyAndWait(t, snapCtx)
	destroyAndWait(t, restoreCtx)
}
