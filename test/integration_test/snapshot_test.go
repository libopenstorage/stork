//go:build integrationtest
// +build integrationtest

package integrationtest

import (
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	crdv1 "github.com/kubernetes-incubator/external-storage/snapshot/pkg/apis/crd/v1"
	client "github.com/kubernetes-incubator/external-storage/snapshot/pkg/client"
	storkv1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/libopenstorage/stork/pkg/log"
	"github.com/portworx/sched-ops/k8s/core"
	"github.com/portworx/sched-ops/k8s/externalstorage"
	k8sextops "github.com/portworx/sched-ops/k8s/externalstorage"
	storkops "github.com/portworx/sched-ops/k8s/stork"
	"github.com/portworx/sched-ops/task"
	"github.com/portworx/torpedo/drivers/scheduler"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var storkStorageClass = "stork-snapshot-sc"

const (
	waitPvcBound         = 120 * time.Second
	waitPvcRetryInterval = 5 * time.Second

	snapshotScheduleRetryInterval = 10 * time.Second
	snapshotScheduleRetryTimeout  = 3 * time.Minute
)

func testSnapshot(t *testing.T) {
	err := setSourceKubeConfig()
	log.FailOnError(t, err, "failed to set kubeconfig to source cluster: %v", err)

	t.Run("simpleSnapshotTest", simpleSnapshotTest)
	t.Run("cloudSnapshotTest", cloudSnapshotTest)
	t.Run("snapshotScaleTest", snapshotScaleTest)
	t.Run("cloudSnapshotScaleTest", cloudSnapshotScaleTest)
	t.Run("customCacheSyncTimeoutSnapshotTest", customCacheSyncTimeoutSnapshotTest)

	if !testing.Short() {
		t.Run("groupSnapshotTest", groupSnapshotTest)
		t.Run("groupSnapshotScaleTest", groupSnapshotScaleTest)
	}
	t.Run("scheduleTests", snapshotScheduleTests)
	// TODO: waiting for https://portworx.atlassian.net/browse/STOR-281 to be resolved
	if authTokenConfigMap == "" {
		t.Run("storageclassTests", storageclassTests)
	}

	err = setRemoteConfig("")
	log.FailOnError(t, err, "setting kubeconfig to default failed")
}

func simpleSnapshotTest(t *testing.T) {
	var testrailID, testResult = 50792, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	defer updateDashStats(t.Name(), &testResult)

	ctx := createSnapshot(t, []string{"mysql-snap-restore"}, "simple-snap-restore")
	verifySnapshot(t, ctx, "mysql-data", 3, 2, true, defaultWaitTimeout)
	destroyAndWait(t, ctx)

	// If we are here then the test has passed
	testResult = testResultPass
	log.InfoD("Test status at end of %s test: %s", t.Name(), testResult)
}

func cloudSnapshotTest(t *testing.T) {
	var testrailID, testResult = 50793, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	defer updateDashStats(t.Name(), &testResult)

	ctxs, err := schedulerDriver.Schedule(generateInstanceID(t, ""),
		scheduler.ScheduleOptions{AppKeys: []string{"mysql-cloudsnap-restore"}})
	log.FailOnError(t, err, "Error scheduling task")
	Dash.VerifyFatal(t, 1, len(ctxs), "Only one task should have started")

	err = schedulerDriver.WaitForRunning(ctxs[0], defaultWaitTimeout, defaultWaitInterval)
	log.FailOnError(t, err, "Error waiting for pod to get to running state")

	scheduledNodes, err := schedulerDriver.GetNodesForApp(ctxs[0])
	log.FailOnError(t, err, "Error getting node for app")
	Dash.VerifyFatal(t, 1, len(scheduledNodes), "App should be scheduled on one node")

	err = schedulerDriver.ValidateVolumes(ctxs[0], defaultWaitTimeout, defaultWaitInterval, nil)
	log.FailOnError(t, err, "Error waiting for volumes")
	volumeNames := getVolumeNames(t, ctxs[0])
	Dash.VerifyFatal(t, 3, len(volumeNames), "Should only have two volumes and a snapshot")

	dataVolumesNames, dataVolumesInUse := parseDataVolumes(t, "mysql-data", ctxs[0])
	Dash.VerifyFatal(t, len(dataVolumesNames), 2, "should have only 2 data volumes")

	snaps, err := schedulerDriver.GetSnapshots(ctxs[0])
	log.FailOnError(t, err, "failed to get snapshots")
	Dash.VerifyFatal(t, len(snaps), 1, "should have received exactly one snapshot")

	for _, snap := range snaps {
		s, err := k8sextops.Instance().GetSnapshot(snap.Name, snap.Namespace)
		log.FailOnError(t, err, "failed to query snapshot object")
		Dash.VerifyFatal(t, s != nil, true, "got snapshot object from k8s api")

		Dash.VerifyFatal(t, s.Spec.SnapshotDataName != "", true, "snapshot object has snapshot data field")

		sData, err := k8sextops.Instance().GetSnapshotData(s.Spec.SnapshotDataName)
		log.FailOnError(t, err, "failed to query snapshot data object")

		snapType := sData.Spec.PortworxSnapshot.SnapshotType
		Dash.VerifyFatal(t, snapType, crdv1.PortworxSnapshotTypeCloud, "snapshot type is cloud")
	}

	log.InfoD("checking dataVolumesInUse: %v\n", dataVolumesInUse)
	verifyScheduledNode(t, scheduledNodes[0], dataVolumesInUse)
	destroyAndWait(t, ctxs)

	// If we are here then the test has passed
	testResult = testResultPass
	log.InfoD("Test status at end of %s test: %s", t.Name(), testResult)
}

func groupSnapshotTest(t *testing.T) {
	var testrailID, testResult = 50795, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	defer updateDashStats(t.Name(), &testResult)

	ctxsToDestroy := make([]*scheduler.Context, 0)
	// Positive tests
	ctxsPass := createGroupsnaps(t, []string{
		"mysql-localsnap-rule",  // tests local group snapshots with a pre exec rule
		"mysql-cloudsnap-group", // tests cloud group snapshots
		"group-cloud-snap-load", // volume is loaded while cloudsnap is being done
	})

	ctxsToDestroy = append(ctxsToDestroy, ctxsPass...)

	snapMap := map[string]int{
		"mysql-localsnap-rule":  2,
		"mysql-cloudsnap-group": 2,
		"group-cloud-snap-load": 3,
	}

	for _, ctx := range ctxsPass {
		verifyGroupSnapshot(t, ctx, groupSnapshotWaitTimeout)
	}

	// Negative
	ctxs, err := schedulerDriver.Schedule(generateInstanceID(t, ""),
		scheduler.ScheduleOptions{AppKeys: []string{"mysql-snap-group-fail"}})
	log.FailOnError(t, err, "Error scheduling task")
	Dash.VerifyFatal(t, len(ctxs), 1, "Only one task should have started")

	for _, ctx := range ctxs {
		err = schedulerDriver.WaitForRunning(ctx, defaultWaitTimeout, defaultWaitInterval)
		log.FailOnError(t, err, "Error waiting for pod to get to running state")

		snaps, err := schedulerDriver.GetSnapshots(ctx)
		log.FailOnNoError(t, err, "expected to get error when fetching snapshots")
		Dash.VerifyFatal(t, snaps == nil, true, "expected empty snapshots")
	}

	for _, ctx := range ctxsPass {
		err := schedulerDriver.WaitForRunning(ctx, defaultWaitTimeout, defaultWaitInterval)
		log.FailOnError(t, err, "Error waiting for pod to get to running state")

		snaps, err := schedulerDriver.GetSnapshots(ctx)
		log.FailOnError(t, err, fmt.Sprintf("Failed to get snapshots for %s.", ctx.App.Key))
		Dash.VerifyFatal(t, snapMap[ctx.App.Key], len(snaps), fmt.Sprintf("Only %d snapshots created for %s expected %d.", len(snaps), ctx.App.Key, snapMap[ctx.App.Key]))
		for _, snap := range snaps {
			restoredPvc, err := createRestorePvcForSnap(snap.Name, snap.Namespace)
			log.FailOnError(t, err, fmt.Sprintf("Failed to create pvc for restoring snapshot %s.", snap.Name))

			err = core.Instance().ValidatePersistentVolumeClaim(restoredPvc, waitPvcBound, waitPvcRetryInterval)
			log.FailOnError(t, err, fmt.Sprintf("PVC for restored snapshot %s not bound.", snap.Name))

			err = core.Instance().DeletePersistentVolumeClaim(restoredPvc.Name, restoredPvc.Namespace)
			log.FailOnError(t, err, fmt.Sprintf("Failed to delete PVC %s.", restoredPvc.Name))
		}
	}
	ctxsToDestroy = append(ctxsToDestroy, ctxs...)

	destroyAndWait(t, ctxsToDestroy)

	// If we are here then the test has passed
	testResult = testResultPass
	log.InfoD("Test status at end of %s test: %s", t.Name(), testResult)
}

func groupSnapshotScaleTest(t *testing.T) {
	var testrailID, testResult = 50796, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	defer updateDashStats(t.Name(), &testResult)

	allContexts := make([]*scheduler.Context, 0)
	// Triggers 2 snaps, so use half the count in the loop
	for i := 0; i < snapshotScaleCount/2; i++ {
		ctxs := createGroupsnaps(t, []string{
			"mysql-localsnap-rule",  // tests local group snapshots with a pre exec rule
			"mysql-cloudsnap-group", // tests cloud group snapshots
		})
		allContexts = append(allContexts, ctxs...)
	}

	timeout := groupSnapshotWaitTimeout
	// Increase the timeout if scale is more than or equal 10
	if snapshotScaleCount >= 10 {
		timeout *= time.Duration((snapshotScaleCount / 10) + 1)
	}

	for _, ctx := range allContexts {
		verifyGroupSnapshot(t, ctx, timeout)
	}

	destroyAndWait(t, allContexts)

	// If we are here then the test has passed
	testResult = testResultPass
	log.InfoD("Test status at end of %s test: %s", t.Name(), testResult)
}

func getSnapAnnotation(snapName string) map[string]string {
	snapAnnotation := make(map[string]string)
	snapAnnotation[client.SnapshotPVCAnnotation] = snapName
	return snapAnnotation
}

func createRestorePvcForSnap(snapName, snapNamespace string) (*v1.PersistentVolumeClaim, error) {
	restorePvc := &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: "restore-pvc-" + snapName,
			Namespace:    snapNamespace,
			Annotations:  getSnapAnnotation(snapName),
		},
		Spec: v1.PersistentVolumeClaimSpec{
			Resources: v1.ResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceName(v1.ResourceStorage): resource.MustParse("2Gi"),
				},
			},
			AccessModes:      []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce},
			StorageClassName: &storkStorageClass,
		},
	}

	if authTokenConfigMap != "" {
		err := addSecurityAnnotation(restorePvc)
		if err != nil {
			return nil, err
		}
	}

	pvc, err := core.Instance().CreatePersistentVolumeClaim(restorePvc)
	return pvc, err
}

func createGroupsnaps(t *testing.T, apps []string) []*scheduler.Context {
	ctxs, err := schedulerDriver.Schedule(generateInstanceID(t, ""),
		scheduler.ScheduleOptions{AppKeys: apps})
	log.FailOnError(t, err, "Error scheduling task")
	Dash.VerifyFatal(t, len(ctxs), len(apps), "Only one task should have started")

	return ctxs
}

func verifyGroupSnapshot(t *testing.T, ctx *scheduler.Context, waitTimeout time.Duration) {
	log.Info("Now verifying group volume snapshot")
	err := schedulerDriver.WaitForRunning(ctx, waitTimeout, defaultWaitInterval)
	log.FailOnError(t, err, fmt.Sprintf("Error waiting for app to get to running state in context: %s-%s", ctx.App.Key, ctx.UID))

	err = schedulerDriver.ValidateVolumes(ctx, waitTimeout, defaultWaitInterval, nil)
	log.FailOnError(t, err, fmt.Sprintf("Error validating storage components in context: %s-%s", ctx.App.Key, ctx.UID))
}

func parseDataVolumes(
	t *testing.T,
	pvcInUseByTest string,
	ctx *scheduler.Context) ([]string, []string) {
	allVolumes, err := schedulerDriver.GetVolumes(ctx)
	log.FailOnError(t, err, "failed to get volumes")

	dataVolumesNames := make([]string, 0)
	dataVolumesInUse := make([]string, 0)
	for _, v := range allVolumes {
		pvc, err := core.Instance().GetPersistentVolumeClaim(v.Name, v.Namespace)
		log.FailOnError(t, err, "failed to get PVC")

		volName, err := core.Instance().GetVolumeForPersistentVolumeClaim(pvc)
		log.FailOnError(t, err, "failed to get PV name")
		dataVolumesNames = append(dataVolumesNames, volName)

		if pvc.GetName() == pvcInUseByTest {
			dataVolumesInUse = append(dataVolumesInUse, volName)
		}
	}

	Dash.VerifyFatal(t, len(dataVolumesInUse), 1, "should have only 1 data volume in use")

	return dataVolumesNames, dataVolumesInUse
}

func createSnapshot(t *testing.T, appKeys []string, nsKey string) []*scheduler.Context {
	ctx, err := schedulerDriver.Schedule(nsKey,
		scheduler.ScheduleOptions{AppKeys: appKeys})
	log.FailOnError(t, err, "Error scheduling task")
	Dash.VerifyFatal(t, len(ctx), 1, "Only one task should have started")
	return ctx
}

func verifySnapshot(t *testing.T,
	ctxs []*scheduler.Context, pvcInUseByTest string,
	numExpectedVols,
	numExpectedDataVols int,
	verifyClone bool,
	waitTimeout time.Duration) {
	log.Info("Now verifying volume snapshot")
	err := schedulerDriver.WaitForRunning(ctxs[0], waitTimeout, defaultWaitInterval)
	log.FailOnError(t, err, fmt.Sprintf("Error waiting for app to get to running state in context: %s-%s", ctxs[0].App.Key, ctxs[0].UID))

	scheduledNodes, err := schedulerDriver.GetNodesForApp(ctxs[0])
	log.FailOnError(t, err, "Error getting node for app")
	Dash.VerifyFatal(t, len(scheduledNodes), 1, "App should be scheduled on one node")

	err = schedulerDriver.ValidateVolumes(ctxs[0], waitTimeout, defaultWaitInterval, nil)
	log.FailOnError(t, err, fmt.Sprintf("Error waiting for volumes in context: %s-%s", ctxs[0].App.Key, ctxs[0].UID))
	volumeNames := getVolumeNames(t, ctxs[0])
	Dash.VerifyFatal(t, numExpectedVols, len(volumeNames), "Should only have two volumes and a snapshot")

	dataVolumesNames, dataVolumesInUse := parseDataVolumes(t, pvcInUseByTest, ctxs[0])
	Dash.VerifyFatal(t, len(dataVolumesNames), numExpectedDataVols, "should have only 2 data volumes")

	snaps, err := schedulerDriver.GetSnapshots(ctxs[0])
	log.FailOnError(t, err, "failed to get snapshots")
	Dash.VerifyFatal(t, len(snaps), 1, "should have received exactly one snapshot")

	if externalTest {
		// TODO: Figure out a way to communicate with PX nodes from other cluster
		return
	}

	for _, snap := range snaps {
		s, err := k8sextops.Instance().GetSnapshot(snap.Name, snap.Namespace)
		log.FailOnError(t, err, "failed to query snapshot object")
		Dash.VerifyFatal(t, s != nil, true, "got snapshot object from k8s api")

		Dash.VerifyFatal(t, s.Spec.SnapshotDataName != "", true, "snapshot object has snapshot data field")

		sData, err := k8sextops.Instance().GetSnapshotData(s.Spec.SnapshotDataName)
		log.FailOnError(t, err, "failed to query snapshot data object")

		snapType := sData.Spec.PortworxSnapshot.SnapshotType
		Dash.VerifyFatal(t, snapType, crdv1.PortworxSnapshotTypeLocal, "snapshot type should be local")

		snapID := sData.Spec.PortworxSnapshot.SnapshotID
		Dash.VerifyFatal(t, snapID != "", true, "got empty snapshot ID in volume snapshot data")

		snapVolInfo, err := storkVolumeDriver.InspectVolume(snapID)
		log.FailOnError(t, err, "Error getting snapshot volume")
		Dash.VerifyFatal(t, snapVolInfo.ParentID != "", true, "ParentID is nil for snapshot")

		parentVolInfo, err := storkVolumeDriver.InspectVolume(snapVolInfo.ParentID)
		log.FailOnError(t, err, "Error getting snapshot parent volume")

		parentVolName := parentVolInfo.VolumeName

		if verifyClone {
			var cloneVolName string

			log.InfoD("ParentVolName: %s", parentVolName)
			log.InfoD("DataVolumeNames: %v", dataVolumesNames)

			found := false
			for _, volume := range dataVolumesNames {
				if volume == parentVolName {
					found = true
				} else if volume != snapVolInfo.VolumeName {
					cloneVolName = volume
				}
			}
			Dash.VerifyFatal(t, found, true, fmt.Sprintf("Parent volume (%v) not found in list of volumes: %v", parentVolName, volumeNames))

			cloneVolInfo, err := storkVolumeDriver.InspectVolume(cloneVolName)
			log.FailOnError(t, err, "Error getting clone volume")
			Dash.VerifyFatal(t, snapVolInfo.VolumeID, cloneVolInfo.ParentID, "Clone volume does not have snapshot as parent")
		}
	}

	verifyScheduledNode(t, scheduledNodes[0], dataVolumesInUse)
}

func verifyCloudSnapshot(t *testing.T, ctxs []*scheduler.Context, pvcInUseByTest string, waitTimeout time.Duration) {
	err := schedulerDriver.WaitForRunning(ctxs[0], defaultWaitTimeout, defaultWaitInterval)
	log.FailOnError(t, err, "Error waiting for pod to get to running state")

	scheduledNodes, err := schedulerDriver.GetNodesForApp(ctxs[0])
	log.FailOnError(t, err, "Error getting node for app")
	Dash.VerifyFatal(t, len(scheduledNodes), 1, "App should be scheduled on one node")

	err = schedulerDriver.ValidateVolumes(ctxs[0], defaultWaitTimeout, defaultWaitInterval, nil)
	log.FailOnError(t, err, "Error waiting for volumes")
	volumeNames := getVolumeNames(t, ctxs[0])
	Dash.VerifyFatal(t, len(volumeNames), 3, "Should only have two volumes and a snapshot")

	dataVolumesNames, dataVolumesInUse := parseDataVolumes(t, "mysql-data", ctxs[0])
	Dash.VerifyFatal(t, len(dataVolumesNames), 2, "should have only 2 data volumes")

	snaps, err := schedulerDriver.GetSnapshots(ctxs[0])
	log.FailOnError(t, err, "failed to get snapshots")
	Dash.VerifyFatal(t, len(snaps), 1, "should have received exactly one snapshot")

	for _, snap := range snaps {
		s, err := k8sextops.Instance().GetSnapshot(snap.Name, snap.Namespace)
		log.FailOnError(t, err, "failed to query snapshot object")
		Dash.VerifyFatal(t, s != nil, true, "got nil snapshot object from k8s api")

		Dash.VerifyFatal(t, s.Spec.SnapshotDataName != "", true, "snapshot object has empty snapshot data field")

		sData, err := k8sextops.Instance().GetSnapshotData(s.Spec.SnapshotDataName)
		log.FailOnError(t, err, "failed to query snapshot data object")

		snapType := sData.Spec.PortworxSnapshot.SnapshotType
		Dash.VerifyFatal(t, snapType, crdv1.PortworxSnapshotTypeCloud, "snapshot type should be cloud")
	}

	fmt.Printf("checking dataVolumesInUse: %v\n", dataVolumesInUse)
	verifyScheduledNode(t, scheduledNodes[0], dataVolumesInUse)
}

func snapshotScaleTest(t *testing.T) {
	ctxs := make([][]*scheduler.Context, snapshotScaleCount)
	for i := 0; i < snapshotScaleCount; i++ {
		ctxs[i] = createSnapshot(t, []string{"mysql-snap-restore"}, "snap-scale-"+strconv.Itoa(i))
	}

	timeout := defaultWaitTimeout
	// Increase the timeout if scale is more than 10
	if snapshotScaleCount > 10 {
		timeout *= time.Duration((snapshotScaleCount / 10) + 1)
	}
	for i := 0; i < snapshotScaleCount; i++ {
		verifySnapshot(t, ctxs[i], "mysql-data", 3, 2, true, timeout)
	}
	for i := 0; i < snapshotScaleCount; i++ {
		destroyAndWait(t, ctxs[i])
	}
}

func snapshotScheduleTests(t *testing.T) {
	err := setMockTime(nil)
	log.FailOnError(t, err, "Error resetting mock time")
	t.Run("intervalTest", intervalSnapshotScheduleTest)
	t.Run("intervalSnapshotScheduleWithSimilarLongNamesTest", intervalSnapshotScheduleWithSimilarLongNamesTest)
	t.Run("dailyTest", dailySnapshotScheduleTest)
	t.Run("weeklyTest", weeklySnapshotScheduleTest)
	t.Run("monthlyTest", monthlySnapshotScheduleTest)
	t.Run("invalidPolicyTest", invalidPolicySnapshotScheduleTest)
	t.Run("updateRetainValueTest", updateRetainValueTest)
}

func cloudSnapshotScaleTest(t *testing.T) {
	var testrailID, testResult = 86218, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	defer updateDashStats(t.Name(), &testResult)

	ctxs := make([][]*scheduler.Context, snapshotScaleCount)
	for i := 0; i < snapshotScaleCount; i++ {
		ctxs[i] = createSnapshot(t, []string{"mysql-cloudsnap-restore"}, "scale-"+strconv.Itoa(i))
	}

	timeout := defaultWaitTimeout
	// Increase the timeout if scale is more than 10
	if snapshotScaleCount > 10 {
		timeout *= time.Duration((snapshotScaleCount / 10) + 1)
	}

	for i := 0; i < snapshotScaleCount; i++ {
		verifyCloudSnapshot(t, ctxs[i], "mysql-data", timeout)
	}

	for i := 0; i < snapshotScaleCount; i++ {
		destroyAndWait(t, ctxs[i])
	}

	// If we are here then the test has passed
	testResult = testResultPass
	log.InfoD("Test status at end of %s test: %s", t.Name(), testResult)
}

func customCacheSyncTimeoutSnapshotTest(t *testing.T) {
	var testrailID, testResult = 301438, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	defer updateDashStats(t.Name(), &testResult)

	// Update stork with the argument.
	err := addStorkArgument("controller-cache-sync-timeout", "10")
	log.FailOnError(t, err, "Error adding stork argument")
	defer func() {
		err := removeStorkArgument("controller-cache-sync-timeout")
		log.FailOnError(t, err, "Error removing stork argument")
		err = waitForStorkDeployment()
		log.FailOnError(t, err, "Error waiting for new stork pods to come up")
	}()

	err = waitForStorkDeployment()
	log.FailOnError(t, err, "Error waiting for new stork pods to come up")

	ctx := createSnapshot(t, []string{"mysql-snap-restore"}, "simple-snap-restore")
	verifySnapshot(t, ctx, "mysql-data", 3, 2, true, defaultWaitTimeout)
	destroyAndWait(t, ctx)

	// If we are here then the test has passed
	testResult = testResultPass
	log.InfoD("Test status at end of %s test: %s", t.Name(), testResult)
}

func deletePolicyAndSnapshotSchedule(t *testing.T, namespace string, policyName string, snapshotScheduleName string) {
	err := storkops.Instance().DeleteSchedulePolicy(policyName)
	log.FailOnError(t, err, fmt.Sprintf("Error deleting schedule policy %v", policyName))

	err = storkops.Instance().DeleteSnapshotSchedule(snapshotScheduleName, namespace)
	log.FailOnError(t, err, fmt.Sprintf("Error deleting snapshot schedule %v from namespace %v",
		snapshotScheduleName, namespace))

	fn := func() (interface{}, bool, error) {
		snapshotList, err := k8sextops.Instance().ListSnapshots(namespace)
		if err != nil {
			return "", true, err
		}

		// Keep retrying if the snapshots are still present.
		if len(snapshotList.Items) > 0 {
			return "", true, fmt.Errorf("waiting for snapshots deletion in namespace %s,expected=0,got=%d", namespace, len(snapshotList.Items))
		}
		return len(snapshotList.Items), false, nil
	}
	snapshotItems, err := task.DoRetryWithTimeout(fn, defaultWaitTimeout, defaultWaitInterval)
	log.FailOnError(t, err, fmt.Sprintf("Error waiting for snapshots deletion in namespace: %v", namespace))
	Dash.VerifyFatal(t, snapshotItems, 0, fmt.Sprintf("All snapshots should have been deleted in namespace %v", namespace))
}

func intervalSnapshotScheduleTest(t *testing.T) {
	var testrailID, testResult = 50797, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	defer updateDashStats(t.Name(), &testResult)

	ctx := createApp(t, "interval-snap-sched-test")
	policyName := "intervalpolicy"
	retain := 2
	interval := 2
	schedPolicy := &storkv1.SchedulePolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name: policyName,
		},
		Policy: storkv1.SchedulePolicyItem{
			Interval: &storkv1.IntervalPolicy{
				Retain:          storkv1.Retain(retain),
				IntervalMinutes: interval,
			},
		}}

	if authTokenConfigMap != "" {
		err := addSecurityAnnotation(schedPolicy)
		log.FailOnError(t, err, "Error adding annotations to interval schedule policy")
	}

	_, err := storkops.Instance().CreateSchedulePolicy(schedPolicy)
	log.FailOnError(t, err, "Error creating interval schedule policy")
	log.InfoD("Created schedulepolicy %v with %v minute interval and retain at %v", policyName, interval, retain)

	scheduleName := "intervalscheduletest"
	namespace := ctx.GetID()
	snapSched := &storkv1.VolumeSnapshotSchedule{
		ObjectMeta: metav1.ObjectMeta{
			Name:      scheduleName,
			Namespace: namespace,
		},
		Spec: storkv1.VolumeSnapshotScheduleSpec{
			Template: storkv1.VolumeSnapshotTemplateSpec{
				Spec: crdv1.VolumeSnapshotSpec{
					PersistentVolumeClaimName: "mysql-data"},
			},
			SchedulePolicyName: policyName,
		},
	}

	if authTokenConfigMap != "" {
		err := addSecurityAnnotation(snapSched)
		log.FailOnError(t, err, "Error adding annotations to interval schedule policy")
	}

	_, err = storkops.Instance().CreateSnapshotSchedule(snapSched)
	log.FailOnError(t, err, "Error creating interval snapshot schedule")
	sleepTime := time.Duration((retain+1)*interval) * time.Minute
	log.InfoD("Created snapshotschedule %v in namespace %v, sleeping for %v for schedule to trigger",
		scheduleName, namespace, sleepTime)
	time.Sleep(sleepTime)

	snapStatuses, err := storkops.Instance().ValidateSnapshotSchedule("intervalscheduletest",
		namespace,
		snapshotScheduleRetryTimeout,
		snapshotScheduleRetryInterval)
	log.FailOnError(t, err, "Error validating interval snapshot schedule")
	Dash.VerifyFatal(t, len(snapStatuses), 1, "Should have snapshots for only one policy type")
	Dash.VerifyFatal(t, retain, len(snapStatuses[storkv1.SchedulePolicyTypeInterval]), fmt.Sprintf("Should have only %v snapshot for interval policy", retain))
	log.InfoD("Validated snapshotschedule %v", scheduleName)

	deletePolicyAndSnapshotSchedule(t, namespace, policyName, scheduleName)
	destroyAndWait(t, []*scheduler.Context{ctx})

	// If we are here then the test has passed
	testResult = testResultPass
	log.InfoD("Test status at end of %s test: %s", t.Name(), testResult)
}

func intervalSnapshotScheduleWithSimilarLongNamesTest(t *testing.T) {
	var testrailID, testResult = 297239, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	defer updateDashStats(t.Name(), &testResult)

	ctxs, err := schedulerDriver.Schedule(generateInstanceID(t, "scheduletest"),
		scheduler.ScheduleOptions{AppKeys: []string{"elasticsearch"}})
	log.FailOnError(t, err, "Error scheduling task")
	Dash.VerifyFatal(t, len(ctxs), 1, "Only one task should have started")

	err = schedulerDriver.WaitForRunning(ctxs[0], defaultWaitTimeout, defaultWaitInterval)
	log.FailOnError(t, err, "Error waiting for elasticsearch statefulset to get to running state")

	policyName := "intervalpolicy"
	retain := 1
	// Increasing the interval time as we have to validate multiple volumesnapshot schedules
	// so that chances of snapshot trigerring gets reduced for verfication of later volumesnashot schedules
	interval := 3
	schedPolicy := &storkv1.SchedulePolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name: policyName,
		},
		Policy: storkv1.SchedulePolicyItem{
			Interval: &storkv1.IntervalPolicy{
				Retain:          storkv1.Retain(retain),
				IntervalMinutes: interval,
			},
		}}

	if authTokenConfigMap != "" {
		err := addSecurityAnnotation(schedPolicy)
		log.FailOnError(t, err, "Error adding annotations to interval schedule policy")
	}

	_, err = storkops.Instance().CreateSchedulePolicy(schedPolicy)
	log.FailOnError(t, err, "Error creating interval schedule policy")
	log.InfoD("Created schedulepolicy %v with %v minute interval and retain at %v", policyName, interval, retain)

	namespace := ctxs[0].GetID()
	pvcList, err := core.Instance().GetPersistentVolumeClaims(namespace, nil)
	log.FailOnError(t, err, fmt.Sprintf("Error getting pvcs in namespace %s interval schedule policy", namespace))
	log.InfoD("Found %v pvcs in namespace %s", len(pvcList.Items), namespace)

	scheduleNames := make([]string, 0)
	for _, pvc := range pvcList.Items {
		scheduleName := fmt.Sprintf("intervalscheduletest-%s", pvc.Name)
		scheduleNames = append(scheduleNames, scheduleName)
		snapSched := &storkv1.VolumeSnapshotSchedule{
			ObjectMeta: metav1.ObjectMeta{
				Name:      scheduleName,
				Namespace: namespace,
			},
			Spec: storkv1.VolumeSnapshotScheduleSpec{
				Template: storkv1.VolumeSnapshotTemplateSpec{
					Spec: crdv1.VolumeSnapshotSpec{
						PersistentVolumeClaimName: pvc.Name},
				},
				SchedulePolicyName: policyName,
			},
		}

		if authTokenConfigMap != "" {
			err := addSecurityAnnotation(snapSched)
			log.FailOnError(t, err, "Error adding annotations to interval schedule policy")
		}

		_, err = storkops.Instance().CreateSnapshotSchedule(snapSched)
		log.FailOnError(t, err, "Error creating interval snapshot schedule")
		log.InfoD("Created snapshotschedule %v in namespace %v", scheduleName, namespace)
	}

	sleepTime := time.Duration(interval-1) * time.Minute
	log.InfoD("Sleeping for %v for schedule to trigger", sleepTime)
	time.Sleep(sleepTime)

	for _, scheduleName := range scheduleNames {
		snapStatuses, err := storkops.Instance().ValidateSnapshotSchedule(scheduleName,
			namespace,
			snapshotScheduleRetryTimeout,
			snapshotScheduleRetryInterval)
		log.FailOnError(t, err, "Error validating interval snapshot schedule")
		Dash.VerifyFatal(t, len(snapStatuses), 1, "Should have snapshots for only one policy type")
		Dash.VerifyFatal(t, len(snapStatuses[storkv1.SchedulePolicyTypeInterval]), retain, fmt.Sprintf("Should have only %v snapshot for interval policy", retain))
		log.InfoD("Validated snapshotschedule %v", scheduleName)
	}

	for _, scheduleName := range scheduleNames {
		err = storkops.Instance().DeleteSnapshotSchedule(scheduleName, namespace)
		log.FailOnError(t, err, fmt.Sprintf("Error deleting snapshot schedule %v from namespace %v", scheduleName, namespace))
	}
	err = storkops.Instance().DeleteSchedulePolicy(policyName)
	log.FailOnError(t, err, fmt.Sprintf("Error deleting schedule policy %v", policyName))

	destroyAndWait(t, ctxs)

	// If we are here then the test has passed
	testResult = testResultPass
	log.InfoD("Test status at end of %s test: %s", t.Name(), testResult)
}

func dailySnapshotScheduleTest(t *testing.T) {
	var testrailID, testResult = 86219, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	defer updateDashStats(t.Name(), &testResult)

	ctx := createApp(t, "daily-snap-sched-test")
	policyName := "dailypolicy"
	retain := 2
	// Set first trigger 2 minutes from now
	scheduledTime := time.Now().Add(2 * time.Minute)
	nextScheduledTime := scheduledTime.AddDate(0, 0, 1)
	schedPolicy := &storkv1.SchedulePolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name: policyName,
		},
		Policy: storkv1.SchedulePolicyItem{
			Daily: &storkv1.DailyPolicy{
				Retain: storkv1.Retain(retain),
				Time:   scheduledTime.Format(time.Kitchen),
			},
		}}

	if authTokenConfigMap != "" {
		err := addSecurityAnnotation(schedPolicy)
		log.FailOnError(t, err, "Error adding annotations to interval schedule policy")
	}

	_, err := storkops.Instance().CreateSchedulePolicy(schedPolicy)
	log.FailOnError(t, err, "Error creating daily schedule policy")
	log.InfoD("Created schedulepolicy %v at time %v and retain at %v",
		policyName, scheduledTime.Format(time.Kitchen), retain)

	scheduleName := "dailyscheduletest"
	namespace := ctx.GetID()
	snapSched := &storkv1.VolumeSnapshotSchedule{
		ObjectMeta: metav1.ObjectMeta{
			Name:      scheduleName,
			Namespace: namespace,
		},
		Spec: storkv1.VolumeSnapshotScheduleSpec{
			Template: storkv1.VolumeSnapshotTemplateSpec{
				Spec: crdv1.VolumeSnapshotSpec{
					PersistentVolumeClaimName: "mysql-data"},
			},
			SchedulePolicyName: policyName,
		},
	}

	if authTokenConfigMap != "" {
		err := addSecurityAnnotation(snapSched)
		log.FailOnError(t, err, "Error adding annotations to daily snapshot schedule")
	}

	_, err = storkops.Instance().CreateSnapshotSchedule(snapSched)
	log.FailOnError(t, err, "Error creating daily snapshot schedule")
	log.InfoD("Created snapshotschedule %v in namespace %v",
		scheduleName, namespace)
	commonSnapshotScheduleTests(t, scheduleName, policyName, namespace, nextScheduledTime, storkv1.SchedulePolicyTypeDaily)
	destroyAndWait(t, []*scheduler.Context{ctx})

	// If we are here then the test has passed
	testResult = testResultPass
	log.InfoD("Test status at end of %s test: %s", t.Name(), testResult)
}

func weeklySnapshotScheduleTest(t *testing.T) {
	var testrailID, testResult = 86220, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	defer updateDashStats(t.Name(), &testResult)

	ctx := createApp(t, "weekly-snap-sched-test")
	policyName := "weeklypolicy"
	retain := 2
	// Set first trigger 2 minutes from now
	scheduledTime := time.Now().Add(2 * time.Minute)
	nextScheduledTime := scheduledTime.AddDate(0, 0, 7)
	schedPolicy := &storkv1.SchedulePolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name: policyName,
		},
		Policy: storkv1.SchedulePolicyItem{
			Weekly: &storkv1.WeeklyPolicy{
				Retain: storkv1.Retain(retain),
				Day:    scheduledTime.Weekday().String(),
				Time:   scheduledTime.Format(time.Kitchen),
			},
		}}

	if authTokenConfigMap != "" {
		err := addSecurityAnnotation(schedPolicy)
		log.FailOnError(t, err, "Error adding annotations to weekly schedule policy")
	}

	_, err := storkops.Instance().CreateSchedulePolicy(schedPolicy)
	log.FailOnError(t, err, "Error creating weekly schedule policy")
	log.InfoD("Created schedulepolicy %v at time %v on day %v and retain at %v",
		policyName, scheduledTime.Format(time.Kitchen), scheduledTime.Weekday().String(), retain)

	scheduleName := "weeklyscheduletest"
	namespace := ctx.GetID()
	snapSched := &storkv1.VolumeSnapshotSchedule{
		ObjectMeta: metav1.ObjectMeta{
			Name:      scheduleName,
			Namespace: namespace,
		},
		Spec: storkv1.VolumeSnapshotScheduleSpec{
			Template: storkv1.VolumeSnapshotTemplateSpec{
				Spec: crdv1.VolumeSnapshotSpec{
					PersistentVolumeClaimName: "mysql-data"},
			},
			SchedulePolicyName: policyName,
		},
	}

	if authTokenConfigMap != "" {
		err := addSecurityAnnotation(snapSched)
		log.FailOnError(t, err, "Error adding annotations to weekly snapshot schedule")
	}

	_, err = storkops.Instance().CreateSnapshotSchedule(snapSched)
	log.FailOnError(t, err, "Error creating weekly snapshot schedule")
	log.InfoD("Created snapshotschedule %v in namespace %v",
		scheduleName, namespace)
	commonSnapshotScheduleTests(t, scheduleName, policyName, namespace, nextScheduledTime, storkv1.SchedulePolicyTypeWeekly)
	destroyAndWait(t, []*scheduler.Context{ctx})

	// If we are here then the test has passed
	testResult = testResultPass
	log.InfoD("Test status at end of %s test: %s", t.Name(), testResult)
}

func monthlySnapshotScheduleTest(t *testing.T) {
	var testrailID, testResult = 50786, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	defer updateDashStats(t.Name(), &testResult)

	ctx := createApp(t, "monthly-snap-sched-test")
	policyName := "monthlypolicy"
	retain := 2
	// Set first trigger 2 minutes from now
	scheduledTime := time.Now().Add(2 * time.Minute)
	nextScheduledTime := scheduledTime.AddDate(0, 1, 0)
	// Set the time to zero in case the date doesn't exist in the next month
	if nextScheduledTime.Day() != scheduledTime.Day() {
		nextScheduledTime = time.Time{}
	}
	schedPolicy := &storkv1.SchedulePolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name: policyName,
		},
		Policy: storkv1.SchedulePolicyItem{
			Monthly: &storkv1.MonthlyPolicy{
				Retain: storkv1.Retain(retain),
				Date:   scheduledTime.Day(),
				Time:   scheduledTime.Format(time.Kitchen),
			},
		}}

	if authTokenConfigMap != "" {
		err := addSecurityAnnotation(schedPolicy)
		log.FailOnError(t, err, "Error adding annotations to monthly schedule policy")
	}

	_, err := storkops.Instance().CreateSchedulePolicy(schedPolicy)
	log.FailOnError(t, err, "Error creating monthly schedule policy")
	log.InfoD("Created schedulepolicy %v at time %v on date %v and retain at %v",
		policyName, scheduledTime.Format(time.Kitchen), scheduledTime.Day(), retain)

	scheduleName := "monthlyscheduletest"
	namespace := ctx.GetID()
	snapSched := &storkv1.VolumeSnapshotSchedule{
		ObjectMeta: metav1.ObjectMeta{
			Name:      scheduleName,
			Namespace: namespace,
		},
		Spec: storkv1.VolumeSnapshotScheduleSpec{
			Template: storkv1.VolumeSnapshotTemplateSpec{
				Spec: crdv1.VolumeSnapshotSpec{
					PersistentVolumeClaimName: "mysql-data"},
			},
			SchedulePolicyName: policyName,
		},
	}

	if authTokenConfigMap != "" {
		err := addSecurityAnnotation(snapSched)
		log.FailOnError(t, err, "Error adding annotations to monthly snapshot schedule")
	}

	_, err = storkops.Instance().CreateSnapshotSchedule(snapSched)
	log.FailOnError(t, err, "Error creating monthly snapshot schedule")
	log.InfoD("Created snapshotschedule %v in namespace %v",
		scheduleName, namespace)
	commonSnapshotScheduleTests(t, scheduleName, policyName, namespace, nextScheduledTime, storkv1.SchedulePolicyTypeMonthly)
	destroyAndWait(t, []*scheduler.Context{ctx})

	// If we are here then the test has passed
	testResult = testResultPass
	log.InfoD("Test status at end of %s test: %s", t.Name(), testResult)
}

func commonSnapshotScheduleTests(
	t *testing.T,
	scheduleName string,
	policyName string,
	namespace string,
	nextTriggerTime time.Time,
	policyType storkv1.SchedulePolicyType) {
	// Make sure no snap gets created in the next minute
	_, err := storkops.Instance().ValidateSnapshotSchedule(scheduleName,
		namespace,
		1*time.Minute,
		snapshotScheduleRetryInterval)
	log.FailOnNoError(t, err, fmt.Sprintf("No snapshots should have been created for %v in namespace %v",
		scheduleName, namespace))
	sleepTime := time.Duration(1 * time.Minute)
	log.InfoD("Sleeping for %v for schedule to trigger",
		sleepTime)
	time.Sleep(sleepTime)

	snapStatuses, err := storkops.Instance().ValidateSnapshotSchedule(scheduleName,
		namespace,
		snapshotScheduleRetryTimeout,
		snapshotScheduleRetryInterval)
	log.FailOnError(t, err, "Error validating snapshot schedule")
	Dash.VerifyFatal(t, len(snapStatuses), 1, "Should have snapshots for only one policy type")
	Dash.VerifyFatal(t, len(snapStatuses[policyType]), 1, fmt.Sprintf("Should have only one snapshot for %v schedule", scheduleName))
	log.InfoD("Validated first snapshotschedule %v", scheduleName)

	// Now advance time to the next trigger if the next trigger is not zero
	if !nextTriggerTime.IsZero() {
		log.InfoD("Updating mock time to %v for next schedule", nextTriggerTime)
		err := setMockTime(&nextTriggerTime)
		log.FailOnError(t, err, "Error setting mock time")
		defer func() {
			err := setMockTime(nil)
			log.FailOnError(t, err, "Error resetting mock time")
		}()
		log.InfoD("Sleeping for 90 seconds for the schedule to get triggered")
		time.Sleep(90 * time.Second)
		snapStatuses, err := storkops.Instance().ValidateSnapshotSchedule(scheduleName,
			namespace,
			snapshotScheduleRetryTimeout,
			snapshotScheduleRetryInterval)
		log.FailOnError(t, err, "Error validating daily snapshot schedule")
		Dash.VerifyFatal(t, len(snapStatuses), 1, "Should have snapshots for only one policy type")
		Dash.VerifyFatal(t, len(snapStatuses[policyType]), 2, fmt.Sprintf("Should have 2 snapshots for %v schedule", scheduleName))
		log.InfoD("Validated second snapshotschedule %v", scheduleName)
	}
	deletePolicyAndSnapshotSchedule(t, namespace, policyName, scheduleName)
}

func invalidPolicySnapshotScheduleTest(t *testing.T) {
	var testrailID, testResult = 86222, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	defer updateDashStats(t.Name(), &testResult)

	ctx := createApp(t, "invalid-snap-sched-test")
	policyName := "invalidpolicy"
	scheduledTime := time.Now()
	retain := 2
	schedPolicy := &storkv1.SchedulePolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name: policyName,
		},
		Policy: storkv1.SchedulePolicyItem{
			Monthly: &storkv1.MonthlyPolicy{
				Retain: storkv1.Retain(retain),
				Date:   scheduledTime.Day(),
				Time:   "13:50PM",
			},
		}}

	if authTokenConfigMap != "" {
		err := addSecurityAnnotation(schedPolicy)
		log.FailOnError(t, err, "Error adding annotations to invalid schedule policy")
	}

	_, err := storkops.Instance().CreateSchedulePolicy(schedPolicy)
	log.FailOnError(t, err, "Error creating invalid schedule policy")
	log.InfoD("Created schedulepolicy %v at time %v on date %v and retain at %v",
		policyName, scheduledTime.Format(time.Kitchen), scheduledTime.Day(), retain)

	scheduleName := "invalidpolicyschedule"
	namespace := ctx.GetID()
	snapSched := &storkv1.VolumeSnapshotSchedule{
		ObjectMeta: metav1.ObjectMeta{
			Name:      scheduleName,
			Namespace: namespace,
		},
		Spec: storkv1.VolumeSnapshotScheduleSpec{
			Template: storkv1.VolumeSnapshotTemplateSpec{
				Spec: crdv1.VolumeSnapshotSpec{
					PersistentVolumeClaimName: "mysql-data"},
			},
			SchedulePolicyName: policyName,
		},
	}

	if authTokenConfigMap != "" {
		err := addSecurityAnnotation(snapSched)
		log.FailOnError(t, err, "Error adding annotations to invalid snapshot schedule")
	}

	_, err = storkops.Instance().CreateSnapshotSchedule(snapSched)
	log.FailOnError(t, err, "Error creating snapshot schedule with invalid policy")
	log.InfoD("Created snapshotschedule %v in namespace %v",
		scheduleName, namespace)
	_, err = storkops.Instance().ValidateSnapshotSchedule(scheduleName,
		namespace,
		3*time.Minute,
		snapshotScheduleRetryInterval)
	log.FailOnNoError(t, err, fmt.Sprintf("No snapshots should have been created for %v in namespace %v",
		scheduleName, namespace))
	deletePolicyAndSnapshotSchedule(t, namespace, policyName, scheduleName)
	destroyAndWait(t, []*scheduler.Context{ctx})

	// If we are here then the test has passed
	testResult = testResultPass
	log.InfoD("Test status at end of %s test: %s", t.Name(), testResult)
}

// updateRetainValueTest tests the creation/pruning of volumesnapshots while we modify the
// `Retain` field's value in the schedulepolicy.
func updateRetainValueTest(t *testing.T) {
	var testrailID, testResult = 300004, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	defer updateDashStats(t.Name(), &testResult)

	// Deploy a dummy application for taking snapshots.
	ctx := createApp(t, "retain-value-modify-test")

	////////////////////////////////////////////////////
	// Create schedulepolicy and snapshot schedule.
	////////////////////////////////////////////////////
	policyName := "intervalpolicy"
	scheduleName := "snapshotretainvaluetest"
	retain := 2
	interval := 1
	schedPolicy := &storkv1.SchedulePolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name: policyName,
		},
		Policy: storkv1.SchedulePolicyItem{
			Interval: &storkv1.IntervalPolicy{
				Retain:          storkv1.Retain(retain),
				IntervalMinutes: interval,
			},
		}}

	if authTokenConfigMap != "" {
		err := addSecurityAnnotation(schedPolicy)
		log.FailOnError(t, err, "Error adding annotations to interval schedule policy")
	}

	_, err := storkops.Instance().CreateSchedulePolicy(schedPolicy)
	log.FailOnError(t, err, "Error creating interval schedule policy")
	log.InfoD("Created schedulepolicy %v with %v minute interval and retain at %v", policyName, interval, retain)

	namespace := ctx.GetID()
	snapSched := &storkv1.VolumeSnapshotSchedule{
		ObjectMeta: metav1.ObjectMeta{
			Name:      scheduleName,
			Namespace: namespace,
		},
		Spec: storkv1.VolumeSnapshotScheduleSpec{
			Template: storkv1.VolumeSnapshotTemplateSpec{
				Spec: crdv1.VolumeSnapshotSpec{
					PersistentVolumeClaimName: "mysql-data"},
			},
			SchedulePolicyName: policyName,
		},
	}

	if authTokenConfigMap != "" {
		err := addSecurityAnnotation(snapSched)
		log.FailOnError(t, err, "Error adding annotations to interval schedule policy")
	}

	_, err = storkops.Instance().CreateSnapshotSchedule(snapSched)
	log.FailOnError(t, err, "Error creating interval snapshot schedule")

	//////////////////////////////////////////////////////////
	// Wait for creation of two volumesnapshots and validate
	// the count to match the retain value.
	/////////////////////////////////////////////////////////
	sleepTime := time.Duration((retain+1)*interval) * time.Minute
	log.InfoD("Created snapshotschedule %v in namespace %v, sleeping for %v for schedule to trigger",
		scheduleName, namespace, sleepTime)
	time.Sleep(sleepTime)

	snapStatuses, err := storkops.Instance().ValidateSnapshotSchedule(scheduleName,
		namespace,
		snapshotScheduleRetryTimeout,
		snapshotScheduleRetryInterval)
	log.FailOnError(t, err, "Error validating snapshot schedule")
	Dash.VerifyFatal(t, len(snapStatuses), 1, "Should have snapshots for only one policy type")
	Dash.VerifyFatal(t, retain, len(snapStatuses[storkv1.SchedulePolicyTypeInterval]), fmt.Sprintf("Should have only %v snapshot for interval policy", retain))
	log.InfoD("Validated snapshotschedule %s to have two volumesnapshots", scheduleName)

	// Verify from the cluster if there are expected number of snapshots.
	snapCount := 0
	snapshots, err := externalstorage.Instance().ListSnapshots(namespace)
	log.FailOnError(t, err, "Error listing snapshots")
	for _, snap := range snapshots.Items {
		if strings.Contains(snap.Metadata.Name, scheduleName) {
			snapCount++
		}
	}
	Dash.VerifyFatal(t, retain, snapCount, fmt.Sprintf("Should have only %v snapshots in the cluster", retain))

	////////////////////////////////////////////////////////
	// Increase the retain value to 3 and wait for another
	// snapshot to get created. Post this, wait for two three
	// cycles of snapshot creation and validate if three
	// snapshots are present.
	////////////////////////////////////////////////////////
	schedulePolicy, err := storkops.Instance().GetSchedulePolicy(policyName)
	log.FailOnError(t, err, "Failed to fetch the schedulepolicy")
	schedulePolicy.Policy.Interval.Retain = storkv1.Retain(3)
	_, err = storkops.Instance().UpdateSchedulePolicy(schedulePolicy)
	log.FailOnError(t, err, "Failed to update the schedulepolicy with retain value 3")

	// Wait for creation of one more snapshot.
	sleepTime = time.Duration(interval*3) * time.Minute
	time.Sleep(sleepTime)

	// Validate that three snapshots are present.
	snapStatuses, err = storkops.Instance().ValidateSnapshotSchedule(scheduleName,
		namespace,
		snapshotScheduleRetryTimeout,
		snapshotScheduleRetryInterval)
	log.FailOnError(t, err, "Error validating snapshot schedule")
	Dash.VerifyFatal(t, len(snapStatuses), 1, "Should have snapshots for only one policy type")
	Dash.VerifyFatal(t, 3, len(snapStatuses[storkv1.SchedulePolicyTypeInterval]), fmt.Sprintf("Should have only %v snapshot for interval policy", retain))
	log.InfoD("Validated snapshotschedule %s to have three volumesnapshots", scheduleName)

	////////////////////////////////////////////////////////
	// Reduce the retain value to 2 and validate if only
	// two snapshots remain post this change.
	////////////////////////////////////////////////////////
	schedulePolicy, err = storkops.Instance().GetSchedulePolicy(policyName)
	log.FailOnError(t, err, "Failed to fetch the schedulepolicy")
	schedulePolicy.Policy.Interval.Retain = storkv1.Retain(2)
	_, err = storkops.Instance().UpdateSchedulePolicy(schedulePolicy)
	log.FailOnError(t, err, "Failed to update the schedulepolicy with retain value 2")

	// Wait for pruning of one snapshot.
	sleepTime = time.Duration(interval*2) * time.Minute
	time.Sleep(sleepTime)

	// Validate that two snapshots are present since now the retain value of
	// the schedulepolicy is 2.
	snapStatuses, err = storkops.Instance().ValidateSnapshotSchedule(scheduleName,
		namespace,
		snapshotScheduleRetryTimeout,
		snapshotScheduleRetryInterval)
	log.FailOnError(t, err, "Error validating snapshot schedule")
	Dash.VerifyFatal(t, len(snapStatuses), 1, "Should have snapshots for only one policy type")
	Dash.VerifyFatal(t, 2, len(snapStatuses[storkv1.SchedulePolicyTypeInterval]), fmt.Sprintf("Should have only %v snapshot for interval policy", retain))
	log.InfoD("Validated snapshotschedule %s to have three volumesnapshots", scheduleName)
	// Verify from the cluster if there are expected number of snapshots.
	snapCount = 0
	snapshots, err = externalstorage.Instance().ListSnapshots(namespace)
	log.FailOnError(t, err, "Error listing snapshots")
	for _, snap := range snapshots.Items {
		if strings.Contains(snap.Metadata.Name, scheduleName) {
			snapCount++
		}
	}
	Dash.VerifyFatal(t, retain, snapCount, fmt.Sprintf("Should have only %v snapshots in the cluster", retain))

	deletePolicyAndSnapshotSchedule(t, namespace, policyName, scheduleName)
	destroyAndWait(t, []*scheduler.Context{ctx})

	// If we are here then the test has passed
	testResult = testResultPass
	log.InfoD("Test status at end of %s test: %s", t.Name(), testResult)
}

func storageclassTests(t *testing.T) {
	ctxs, err := schedulerDriver.Schedule("autosnaptest",
		scheduler.ScheduleOptions{AppKeys: []string{"snapshot-storageclass"}})
	log.FailOnError(t, err, "Error scheduling task")
	Dash.VerifyFatal(t, len(ctxs), 1, "Only one task should have started")

	namespace := ctxs[0].GetID()
	pvc := &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "autosnap",
			Namespace: namespace,
		},
	}
	err = core.Instance().ValidatePersistentVolumeClaim(pvc, waitPvcBound, waitPvcRetryInterval)
	log.FailOnError(t, err, fmt.Sprintf("PVC %v not bound.", pvc.Name))

	snapshotScheduleName := "autosnap-test-schedule"
	// Make sure snapshots get triggered
	_, err = storkops.Instance().ValidateSnapshotSchedule(snapshotScheduleName,
		namespace,
		3*time.Minute,
		snapshotScheduleRetryInterval)
	log.FailOnError(t, err, "Error validating snapshot schedule")
	// Destroy the PVC
	destroyAndWait(t, ctxs)

	// Make sure the snapshot schedule and snapshots are also deleted
	time.Sleep(10 * time.Second)
	snapshotScheduleList, err := storkops.Instance().ListSnapshotSchedules(namespace)
	log.FailOnError(t, err, fmt.Sprintf("Error getting list of snapshots schedules for namespace: %v", namespace))
	Dash.VerifyFatal(t, len(snapshotScheduleList.Items), 0, fmt.Sprintf("All snapshot schedules should have been deleted in namespace %v", namespace))
	snapshotList, err := k8sextops.Instance().ListSnapshots(namespace)
	log.FailOnError(t, err, fmt.Sprintf("Error getting list of snapshots for namespace: %v", namespace))
	Dash.VerifyFatal(t, len(snapshotList.Items), 0, fmt.Sprintf("All snapshots should have been deleted in namespace %v", namespace))
}
