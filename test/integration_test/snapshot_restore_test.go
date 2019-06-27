// +build integrationtest

package integrationtest

import (
	"testing"
	"time"

	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/stretchr/testify/require"
)

func testSnapshotRestore(t *testing.T) {
	t.Run("simpleSnapshotRestoreTest", simpleSnapshotRestoreTest)
	/*t.Run("groupSnapshotRestoreTest", groupSnapshotRestoreTest)
	t.Run("cloudSnapshotRestoreTest", cloudSnapshotRestoreTest)
	t.Run("groupCloudSnapshotRestoreTest", groupCloudSnapshotRestoreTest)*/
}
func createInPlaceRestore(t *testing.T, appKeys []string) []*scheduler.Context {
	ctxs, err := schedulerDriver.Schedule("test",
		scheduler.ScheduleOptions{AppKeys: appKeys})
	require.NoError(t, err, "Error scheduling task")
	require.Equal(t, 1, len(ctxs), "Only one task should have started")
	return ctxs
}

func verifyInPlaceSnapshotRestore(t *testing.T, ctx []*scheduler.Context, startTime time.Time, timeout time.Duration) {
	err := schedulerDriver.WaitForRunning(ctx[0], timeout, defaultWaitInterval)
	// we shouldn't valid px specific things here just pass to torpedo validate inplacenspahostrestore
	// in torpedo inplace snapshot restore for localsnapshot check alerts with volume id may be bit hard low prio
	// in cloudsnaps check whether restore volume created and its ha level + alerts
	require.NoError(t, err, "Error waiting for task")
	err = schedulerDriver.ValidateVolumeSnapshotRestore(ctx[0], startTime)
	require.NoError(t, err, "Error validating volumesnapshotRestore")
}

func simpleSnapshotRestoreTest(t *testing.T) {
	// foldername + name as namespaces
	snapCtx, err := schedulerDriver.Schedule("test",
		scheduler.ScheduleOptions{AppKeys: []string{"mysql-snap-restore"}})
	require.NoError(t, err, "Error scheduling task")
	require.Equal(t, 1, len(snapCtx), "Only one task should have started")
	verifySnapshot(t, snapCtx, "mysql-data", defaultWaitTimeout)
	startTime := time.Now()
	restoreCtx := createInPlaceRestore(t, []string{"mysql-snap-inplace-restore"})
	verifyInPlaceSnapshotRestore(t, restoreCtx, startTime, defaultWaitTimeout)
}

func groupSnapshotRestoreTest(t *testing.T) {
	snapCtx := createGroupsnaps(t, []string{"mysql-group-snapshot-restore"})
	for _, ctx := range snapCtx {
		verifyGroupSnapshot(t, ctx, defaultWaitTimeout)
	}
	startTime := time.Now()
	restoreCtx := createInPlaceRestore(t, []string{"mysql-in-place-restore"})
	verifyInPlaceSnapshotRestore(t, restoreCtx, startTime, defaultWaitTimeout)
}

func cloudSnapshotRestoreTest(t *testing.T) {
	ctxs, err := schedulerDriver.Schedule("mysql-cloudsnap-restore",
		scheduler.ScheduleOptions{AppKeys: []string{"mysql-cloudsnap-restore"}})
	require.NoError(t, err, "Error scheduling task")
	require.Equal(t, 1, len(ctxs), "Only one task should have started")

	err = schedulerDriver.WaitForRunning(ctxs[0], defaultWaitTimeout, defaultWaitInterval)
	require.NoError(t, err, "Error waiting for pod to get to running state")

	restoreCtx := createInPlaceRestore(t, []string{"mysql-snap-inplace-restore"})
	startTime := time.Now()
	verifyInPlaceSnapshotRestore(t, restoreCtx, startTime, defaultWaitTimeout)
}

func groupCloudSnapshotRestoreTest(t *testing.T) {
	//TODO: add groupcloudsnap in place restore tests here
}
