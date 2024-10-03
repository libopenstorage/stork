//go:build integrationtest
// +build integrationtest

package integrationtest

import (
	"fmt"
	"os"
	"testing"
	"time"

	snapv1 "github.com/kubernetes-incubator/external-storage/snapshot/pkg/apis/crd/v1"
	"github.com/libopenstorage/openstorage/pkg/auth/secrets"
	operator_util "github.com/libopenstorage/operator/drivers/storage/portworx/util"
	storkv1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/libopenstorage/stork/pkg/k8sutils"
	"github.com/libopenstorage/stork/pkg/log"
	k8sextops "github.com/portworx/sched-ops/k8s/externalstorage"
	storkops "github.com/portworx/sched-ops/k8s/stork"
	"github.com/portworx/torpedo/drivers/scheduler"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func testSnapshotRestore(t *testing.T) {
	err := setSourceKubeConfig()
	log.FailOnError(t, err, "failed to set kubeconfig to source cluster: %v", err)

	t.Run("simpleSnapshotRestoreTest", simpleSnapshotRestoreTest)
	t.Run("inPlaceSnapshotRestoreDataTest", inPlaceSnapshotRestoreDataTest)
	if !testing.Short() {
		t.Run("groupSnapshotRestoreTest", groupSnapshotRestoreTest)
		t.Run("cloudSnapshotRestoreTest", cloudSnapshotRestoreTest)
		t.Run("groupCloudSnapshotRestoreTest", groupCloudSnapshotRestoreTest)
	}

	err = setRemoteConfig("")
	log.FailOnError(t, err, "setting kubeconfig to default failed")
}

func createInPlaceRestore(t *testing.T, namespace string, appKeys []string) []*scheduler.Context {
	ctxs, err := schedulerDriver.Schedule(namespace,
		scheduler.ScheduleOptions{AppKeys: appKeys})
	log.FailOnError(t, err, "Error scheduling task")
	Dash.VerifyFatal(t, len(ctxs), 1, "Only one task should have started")
	return ctxs
}

func verifyInPlaceSnapshotRestore(t *testing.T, ctx []*scheduler.Context, startTime time.Time, timeout time.Duration) {
	err := schedulerDriver.WaitForRunning(ctx[0], timeout, defaultWaitInterval)
	log.FailOnError(t, err, "Error waiting for task")
	err = schedulerDriver.ValidateVolumeSnapshotRestore(ctx[0], startTime)
	log.FailOnError(t, err, "Error validating volumesnapshotRestore")
}

func simpleSnapshotRestoreTest(t *testing.T) {
	var testrailID, testResult = 50799, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	defer updateDashStats(t.Name(), &testResult)

	// foldername + name as namespaces
	snapCtx, err := schedulerDriver.Schedule("splocal",
		scheduler.ScheduleOptions{AppKeys: []string{"mysql-snap-restore"}})
	log.FailOnError(t, err, "Error scheduling task")
	Dash.VerifyFatal(t, len(snapCtx), 1, "Only one task should have started")
	verifySnapshot(t, snapCtx, "mysql-data", 3, 2, true, defaultWaitTimeout)
	startTime := time.Now()
	restoreCtx := createInPlaceRestore(t, "splocal", []string{"mysql-snap-inplace-restore"})
	verifyInPlaceSnapshotRestore(t, restoreCtx, startTime, defaultWaitTimeout)

	// cleanup test
	destroyAndWait(t, snapCtx)
	destroyAndWait(t, restoreCtx)

	// If we are here then the test has passed
	testResult = testResultPass
	log.InfoD("Test status at end of %s test: %s", t.Name(), testResult)
}

func groupSnapshotRestoreTest(t *testing.T) {
	var testrailID, testResult = 50800, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	defer updateDashStats(t.Name(), &testResult)

	snapCtx, err := schedulerDriver.Schedule("spgroup",
		scheduler.ScheduleOptions{AppKeys: []string{"mysql-2-pvc"}})
	log.FailOnError(t, err, "Error scheduling task")
	Dash.VerifyFatal(t, len(snapCtx), 1, "Only one task should have started")
	verifyGroupSnapshot(t, snapCtx[0], defaultWaitTimeout)

	err = schedulerDriver.AddTasks(snapCtx[0],
		scheduler.ScheduleOptions{AppKeys: []string{"mysql-localsnap-group"}})
	log.FailOnError(t, err, "Error scheduling task")

	// restore local groupsnapshot
	startTime := time.Now()
	restoreCtx := createInPlaceRestore(t, "spgroup", []string{"mysql-snap-inplace-group-restore"})
	verifyInPlaceSnapshotRestore(t, restoreCtx, startTime, defaultWaitTimeout)

	// cleanup test
	destroyAndWait(t, snapCtx)
	destroyAndWait(t, restoreCtx)

	// If we are here then the test has passed
	testResult = testResultPass
	log.InfoD("Test status at end of %s test: %s", t.Name(), testResult)
}

func cloudSnapshotRestoreTest(t *testing.T) {
	var testrailID, testResult = 50801, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	defer updateDashStats(t.Name(), &testResult)

	if testing.Short() {
		t.Skip("Skipping test: cloudSnapshotRestoreTest.")
	}
	snapCtx, err := schedulerDriver.Schedule("cslocal",
		scheduler.ScheduleOptions{AppKeys: []string{"mysql-cloudsnap-restore"}})
	log.FailOnError(t, err, "Error scheduling task")
	Dash.VerifyFatal(t, len(snapCtx), 1, "Only one task should have started")

	err = schedulerDriver.WaitForRunning(snapCtx[0], defaultWaitTimeout, defaultWaitInterval)
	log.FailOnError(t, err, "Error waiting for pod to get to running state")

	restoreCtx := createInPlaceRestore(t, "cslocal", []string{"mysql-snap-inplace-cloud-restore"})
	startTime := time.Now()
	verifyInPlaceSnapshotRestore(t, restoreCtx, startTime, defaultWaitTimeout)

	// cleanup test
	destroyAndWait(t, snapCtx)
	destroyAndWait(t, restoreCtx)

	// If we are here then the test has passed
	testResult = testResultPass
	log.InfoD("Test status at end of %s test: %s", t.Name(), testResult)
}

func groupCloudSnapshotRestoreTest(t *testing.T) {
	var testrailID, testResult = 50802, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	defer updateDashStats(t.Name(), &testResult)

	if testing.Short() {
		t.Skip("Skipping test: groupCloudSnapshotRestoreTest.")
	}
	snapCtx, err := schedulerDriver.Schedule("csgroup",
		scheduler.ScheduleOptions{AppKeys: []string{"mysql-cloudsnap-group-restore"}})
	log.FailOnError(t, err, "Error scheduling task")
	Dash.VerifyFatal(t, len(snapCtx), 1, "Only one task should have started")

	verifyGroupSnapshot(t, snapCtx[0], defaultWaitTimeout)

	err = schedulerDriver.WaitForRunning(snapCtx[0], defaultWaitTimeout, defaultWaitInterval)
	log.FailOnError(t, err, "Error waiting for pod to get to running state")

	restoreCtx := createInPlaceRestore(t, "csgroup", []string{"mysql-groupsnap-inplace-cloud-restore"})
	startTime := time.Now()
	verifyInPlaceSnapshotRestore(t, restoreCtx, startTime, defaultWaitTimeout)

	// cleanup test
	destroyAndWait(t, snapCtx)
	destroyAndWait(t, restoreCtx)

	// If we are here then the test has passed
	testResult = testResultPass
	log.InfoD("Test status at end of %s test: %s", t.Name(), testResult)
}

func inPlaceSnapshotRestoreDataTest(t *testing.T) {
	var testrailID, testResult = 86223, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	defer updateDashStats(t.Name(), &testResult)
	testnamespacePostfix := "snapvdbench"
	appCtx, err := schedulerDriver.Schedule(testnamespacePostfix,
		scheduler.ScheduleOptions{AppKeys: []string{"vdbench-repl-2-app"}})
	log.FailOnError(t, err, "Error scheduling vdbench app")
	err = schedulerDriver.WaitForRunning(appCtx[0], defaultWaitTimeout, defaultWaitInterval)
	log.FailOnError(t, err, "Error waiting for vdbench app")

	log.InfoD("Waiting for vdbench to write some data to the PVC")
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
	if authTokenConfigMap != "" {
		snapObj.Metadata.Annotations = make(map[string]string)
		snapObj.Metadata.Annotations[secrets.SecretNameKey] = operator_util.SecurityPXAdminTokenSecretName
		snapObj.Metadata.Annotations[secrets.SecretNamespaceKey] = os.Getenv(k8sutils.PxNamespaceEnvName)
	}
	vdbenchSnap, err := k8sextops.Instance().CreateSnapshot(snapObj)
	log.FailOnError(t, err, "Error creating vdbench PVC snapshot")

	err = k8sextops.Instance().ValidateSnapshot(vdbenchSnap.Metadata.Name,
		vdbenchSnap.Metadata.Namespace,
		true,
		defaultWaitTimeout,
		defaultWaitInterval)
	log.FailOnError(t, err, "Failed to validate VolumeSnapshot: %v. Err: %v", vdbenchSnap.Metadata.Name, err)

	log.InfoD("Writing some more data to the PVC before snapshot restore")
	time.Sleep(time.Second * 600)

	restoreObj := &storkv1.VolumeSnapshotRestore{
		Spec: storkv1.VolumeSnapshotRestoreSpec{
			SourceName:      "vdbench-snapshot",
			SourceNamespace: fmt.Sprintf("vdbench-repl-2-app-%s", testnamespacePostfix),
		},
	}
	restoreObj.Name = "vdbench-inplace-snapshot-restore"
	restoreObj.Namespace = fmt.Sprintf("vdbench-repl-2-app-%s", testnamespacePostfix)
	if authTokenConfigMap != "" {
		restoreObj.Annotations = make(map[string]string)
		restoreObj.Annotations[secrets.SecretNameKey] = operator_util.SecurityPXAdminTokenSecretName
		restoreObj.Annotations[secrets.SecretNamespaceKey] = os.Getenv(k8sutils.PxNamespaceEnvName)
	}
	vdbenchRestore, err := storkops.Instance().CreateVolumeSnapshotRestore(restoreObj)
	log.FailOnError(t, err, "Error creating vdbench in-place restore")

	err = storkops.Instance().ValidateVolumeSnapshotRestore(vdbenchRestore.Name, vdbenchRestore.Namespace, 240*time.Second, defaultWaitInterval)
	log.FailOnError(t, err, "Error validating vdbench in-place restore")

	// cleanup test
	destroyAndWait(t, appCtx)

	err = storkops.Instance().DeleteVolumeSnapshotRestore(vdbenchRestore.Name, vdbenchRestore.Namespace)
	log.FailOnError(t, err, "Error deleting vdbench in-place restore")

	err = k8sextops.Instance().DeleteSnapshot(snapObj.Metadata.Name, snapObj.Metadata.Namespace)
	log.FailOnError(t, err, "Error deleting vdbench PVC snapshot")

	// If we are here then the test has passed
	testResult = testResultPass
	log.InfoD("Test status at end of %s test: %s", t.Name(), testResult)
}
