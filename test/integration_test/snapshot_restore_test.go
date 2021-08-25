// +build integrationtest

package integrationtest

import (
	"fmt"
	"testing"
	"time"

	snapv1 "github.com/kubernetes-incubator/external-storage/snapshot/pkg/apis/crd/v1"
	storkv1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	k8sextops "github.com/portworx/sched-ops/k8s/externalstorage"
	storkops "github.com/portworx/sched-ops/k8s/stork"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func testSnapshotRestore(t *testing.T) {
	err := setSourceKubeConfig()
	require.NoError(t, err, "failed to set kubeconfig to source cluster: %v", err)

	t.Run("simpleSnapshotRestoreTest", simpleSnapshotRestoreTest)
	t.Run("inPlaceSnapshotRestoreDataTest", inPlaceSnapshotRestoreDataTest)
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
	verifySnapshot(t, snapCtx, "mysql-data", 3, 2, true, defaultWaitTimeout)
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

func inPlaceSnapshotRestoreDataTest(t *testing.T) {
	testnamespacePostfix := "snapvdbench"
	appCtx, err := schedulerDriver.Schedule(testnamespacePostfix,
		scheduler.ScheduleOptions{AppKeys: []string{"vdbench-repl-2-app"}})
	require.NoError(t, err, "Error scheduling vdbench app")
	err = schedulerDriver.WaitForRunning(appCtx[0], defaultWaitTimeout, defaultWaitInterval)
	require.NoError(t, err, "Error waiting for vdbench app")

	logrus.Infof("Waiting for vdbench to write some data to the PVC")
	time.Sleep(time.Second * 300)

	// Create snapshot directly using a private method instead of torpedo because
	// torped creates all specs at once and in this case we want to wait before
	// creating volumesnapshot as we need data to be written to the PVC first

	snapObj := &snapv1.VolumeSnapshot{
		Metadata: meta.ObjectMeta{
			Name:      "vdbench-snapshot",
			Namespace: fmt.Sprintf("vdbench-repl-2-app-%s", testnamespacePostfix),
		},
		Spec: snapv1.VolumeSnapshotSpec{
			PersistentVolumeClaimName: "vdbench-pvc",
		},
	}
	vdbenchSnap, err := k8sextops.Instance().CreateSnapshot(snapObj)
	require.NoError(t, err, "Error creating vdbench PVC snapshot")

	err = k8sextops.Instance().ValidateSnapshot(vdbenchSnap.Metadata.Name,
		vdbenchSnap.Metadata.Namespace,
		true,
		defaultWaitTimeout,
		defaultWaitInterval)
	require.NoError(t, err, "Failed to validate VolumeSnapshot: %v. Err: %v", vdbenchSnap.Metadata.Name, err)

	logrus.Infof("Writing some more data to the PVC before snapshot restore")
	time.Sleep(time.Second * 600)

	restoreObj := &storkv1.VolumeSnapshotRestore{
		Spec: storkv1.VolumeSnapshotRestoreSpec{
			SourceName:      "vdbench-snapshot",
			SourceNamespace: fmt.Sprintf("vdbench-repl-2-app-%s", testnamespacePostfix),
		},
	}
	restoreObj.Name = "vdbench-inplace-snapshot-restore"
	restoreObj.Namespace = fmt.Sprintf("vdbench-repl-2-app-%s", testnamespacePostfix)
	vdbenchRestore, err := storkops.Instance().CreateVolumeSnapshotRestore(restoreObj)
	require.NoError(t, err, "Error creating vdbench in-place restore")

	err = storkops.Instance().ValidateVolumeSnapshotRestore(vdbenchRestore.Name, vdbenchRestore.Namespace, 240*time.Second, defaultWaitInterval)
	require.NoError(t, err, "Error validating vdbench in-place restore")

	// cleanup test
	destroyAndWait(t, appCtx)

	err = storkops.Instance().DeleteVolumeSnapshotRestore(vdbenchRestore.Name, vdbenchRestore.Namespace)
	require.NoError(t, err, "Error deleting vdbench in-place restore")

	err = k8sextops.Instance().DeleteSnapshot(snapObj.Metadata.Name, snapObj.Metadata.Namespace)
	require.NoError(t, err, "Error deleting vdbench PVC snapshot")
}
