//go:build integrationtest
// +build integrationtest

package integrationtest

import (
	"testing"
	"time"

	storkdriver "github.com/libopenstorage/stork/drivers/volume"
	"github.com/portworx/sched-ops/k8s/apps"
	"github.com/portworx/sched-ops/k8s/core"
	"github.com/portworx/sched-ops/k8s/storage"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/portworx/torpedo/pkg/log"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	apps_api "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	storage_api "k8s.io/api/storage/v1"
)

const (
	annotationStorageProvisioner = "volume.beta.kubernetes.io/storage-provisioner"

	maxPodBringupTime time.Duration = 2 * time.Minute
)

func TestExtender(t *testing.T) {
	dash.TestSetBegin(dash.TestSet)
	err := setSourceKubeConfig()
	require.NoError(t, err, "failed to set kubeconfig to source cluster: %v", err)

	t.Run("pvcOwnershipTest", pvcOwnershipTest)
	t.Run("noPVCTest", noPVCTest)
	t.Run("singlePVCTest", singlePVCTest)
	t.Run("statefulsetTest", statefulsetTest)
	t.Run("multiplePVCTest", multiplePVCTest)
	t.Run("driverNodeErrorTest", driverNodeErrorTest)
	t.Run("poolMaintenanceTest", poolMaintenanceTest)
	t.Run("antihyperconvergenceTest", antihyperconvergenceTest)
	t.Run("antihyperconvergenceTestPreferRemoteOnlyTest", antihyperconvergenceTestPreferRemoteOnlyTest)
	t.Run("preferRemoteNodeFalseHyperconvergenceTest", preferRemoteNodeFalseHyperconvergenceTest)
	t.Run("equalPodSpreadTest", equalPodSpreadTest)

	err = setRemoteConfig("")
	require.NoError(t, err, "setting kubeconfig to default failed")
}

func noPVCTest(t *testing.T) {
	dash.TestCaseBegin("Stork scheduler No PVC test", "Stork scheduler test for app with no PVC", "", nil)
	var testrailID, testResult = 50785, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult)
	defer updateTestRail(&testResult, testrailID, runID)

	log.InfoD("Deploy app with no PVC")
	ctxs, err := schedulerDriver.Schedule(generateInstanceID(t, "nopvctest"),
		scheduler.ScheduleOptions{AppKeys: []string{"mysql-nopvc"}})
	require.NoError(t, err, "Error scheduling task")
	require.Equal(t, 1, len(ctxs), "Only one task should have started")

	err = schedulerDriver.WaitForRunning(ctxs[0], defaultWaitTimeout, defaultWaitInterval)
	require.NoError(t, err, "Error waiting for pod to get to running state")

	destroyAndWait(t, ctxs)
	log.InfoD("Deleted app with no PVC")

	// If we are here then the test has passed
	testResult = testResultPass
	logrus.Infof("Test status at end of %s test: %s", t.Name(), testResult)
}

func singlePVCTest(t *testing.T) {
	dash.TestCaseBegin("Stork scheduler single PVC test", "Stork scheduler test for app with single PVC", "", nil)
	log.InfoD("")
	var testrailID, testResult = 50786, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult)
	defer updateTestRail(&testResult, testrailID, runID)

	ctxs, err := schedulerDriver.Schedule(generateInstanceID(t, "singlepvctest"),
		scheduler.ScheduleOptions{AppKeys: []string{"mysql-1-pvc"}})
	require.NoError(t, err, "Error scheduling task")
	require.Equal(t, 1, len(ctxs), "Only one task should have started")

	err = schedulerDriver.WaitForRunning(ctxs[0], defaultWaitTimeout, defaultWaitInterval)
	require.NoError(t, err, "Error waiting for pod to get to running state")

	scheduledNodes, err := schedulerDriver.GetNodesForApp(ctxs[0])
	require.NoError(t, err, "Error getting node for app")
	require.Equal(t, 1, len(scheduledNodes), "App should be scheduled on one node")
	log.InfoD("App with single PVC scheduled on one node")

	volumeNames := getVolumeNames(t, ctxs[0])
	require.Equal(t, 1, len(volumeNames), "Should only have one volume")

	verifyScheduledNode(t, scheduledNodes[0], volumeNames)

	destroyAndWait(t, ctxs)

	// If we are here then the test has passed
	testResult = testResultPass
	logrus.Infof("Test status at end of %s test: %s", t.Name(), testResult)
	log.InfoD("Deleted app with single PVC")
}

func statefulsetTest(t *testing.T) {
	dash.TestCaseBegin("Stateful set extender test", "Stork scheduler test with stateful set application", "", nil)
	var testrailID, testResult = 50787, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult)
	defer updateTestRail(&testResult, testrailID, runID)

	ctxs, err := schedulerDriver.Schedule(generateInstanceID(t, "sstest"),
		scheduler.ScheduleOptions{AppKeys: []string{"elasticsearch"}})
	require.NoError(t, err, "Error scheduling task")
	require.Equal(t, 1, len(ctxs), "Only one task should have started")

	err = schedulerDriver.WaitForRunning(ctxs[0], defaultWaitTimeout, defaultWaitInterval)
	require.NoError(t, err, "Error waiting for elasticsearch statefulset to get to running state")

	scheduledNodes, err := schedulerDriver.GetNodesForApp(ctxs[0])
	require.NoError(t, err, "Error getting node for app")
	// TODO: There is no qurantee that all pods will be scheduled on
	// different nodes, we should restrict pvc to repl1 and one px node
	// use preferLocalNode flag to ensure pod is getting scheduled on node
	// where replicas exist
	// require.Equal(t, 3, len(scheduledNodes), "App should be scheduled on one node")
	logrus.Infof("sts pod scheduled on %d nodes", len(scheduledNodes))
	// TODO: torpedo doesn't return correct volumes here
	volumeNames := getVolumeNames(t, ctxs[0])
	require.Equal(t, 3, len(volumeNames), "Should have 3 volumes")

	// TODO: Add verification for node where it was scheduled
	// torpedo doesn't return the pod->pvc mapping, so we can't validate that it
	// got scheduled on a prioritized node
	verifyScheduledNode(t, scheduledNodes[0], volumeNames)

	destroyAndWait(t, ctxs)

	// If we are here then the test has passed
	testResult = testResultPass
	logrus.Infof("Test status at end of %s test: %s", t.Name(), testResult)
}

func multiplePVCTest(t *testing.T) {
	dash.TestCaseBegin("Multiple PVC test", "Stork scheduler test with app using multiple PVCS", "", nil)
	var testrailID, testResult = 50788, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult)
	defer updateTestRail(&testResult, testrailID, runID)

	ctxs, err := schedulerDriver.Schedule(generateInstanceID(t, "multipvctest"),
		scheduler.ScheduleOptions{AppKeys: []string{"mysql-2-pvc"}})
	require.NoError(t, err, "Error scheduling task")
	require.Equal(t, 1, len(ctxs), "Only one task should have started")

	err = schedulerDriver.WaitForRunning(ctxs[0], defaultWaitTimeout, defaultWaitInterval)
	require.NoError(t, err, "Error waiting for pod to get to running state")

	scheduledNodes, err := schedulerDriver.GetNodesForApp(ctxs[0])
	require.NoError(t, err, "Error getting node for app")
	require.Equal(t, 1, len(scheduledNodes), "App should be scheduled on one node")

	volumeNames := getVolumeNames(t, ctxs[0])
	require.Equal(t, 2, len(volumeNames), "Should have two volumes")

	verifyScheduledNode(t, scheduledNodes[0], volumeNames)
	destroyAndWait(t, ctxs)

	// If we are here then the test has passed
	testResult = testResultPass
	logrus.Infof("Test status at end of %s test: %s", t.Name(), testResult)
}

func driverNodeErrorTest(t *testing.T) {
	dash.TestCaseBegin("Driver node error", "Induce error on driver node by stopping PX on the node", "", nil)
	var testrailID, testResult = 50789, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult)
	defer updateTestRail(&testResult, testrailID, runID)

	ctxs, err := schedulerDriver.Schedule(generateInstanceID(t, "drivererrtest"),
		scheduler.ScheduleOptions{AppKeys: []string{"mysql-1-pvc"}})
	require.NoError(t, err, "Error scheduling task")
	require.Equal(t, 1, len(ctxs), "Only one task should have started")

	err = schedulerDriver.WaitForRunning(ctxs[0], defaultWaitTimeout, defaultWaitInterval)
	require.NoError(t, err, "Error waiting for pod to get to running state")

	scheduledNodes, err := schedulerDriver.GetNodesForApp(ctxs[0])
	require.NoError(t, err, "Error getting node for app")
	require.Equal(t, 1, len(scheduledNodes), "App should be scheduled on one node")

	volumeNames := getVolumeNames(t, ctxs[0])
	require.Equal(t, 1, len(volumeNames), "Should have only one volume")

	verifyScheduledNode(t, scheduledNodes[0], volumeNames)

	time.Sleep(1 * time.Minute)

	log.InfoD("Stopping volume driver on node: %s", scheduledNodes[0])
	err = volumeDriver.StopDriver(scheduledNodes, false, nil)
	require.NoError(t, err, "Error stopping driver on scheduled Node %+v", scheduledNodes[0])
	stoppedNode := scheduledNodes[0]
	// node timeout bumped to 4 mins from stork 2.9.0
	// ref: https://github.com/libopenstorage/stork/pull/1028
	time.Sleep(5 * time.Minute)

	err = schedulerDriver.WaitForRunning(ctxs[0], defaultWaitTimeout, defaultWaitInterval)
	require.NoError(t, err, "Error waiting for pod to get to running state after deletion")

	scheduledNodes, err = schedulerDriver.GetNodesForApp(ctxs[0])
	require.NoError(t, err, "Error getting node for app")
	require.Equal(t, 1, len(scheduledNodes), "App should be scheduled on one node")
	require.NotEqual(t, stoppedNode.Name, scheduledNodes[0].Name, "Task restarted on stopped node")

	volumeNames = getVolumeNames(t, ctxs[0])
	require.Equal(t, 1, len(volumeNames), "Should have only one volume")

	verifyScheduledNode(t, scheduledNodes[0], volumeNames)

	log.InfoD("Starting volume driver on node: %s", stoppedNode)
	err = volumeDriver.StartDriver(stoppedNode)
	require.NoError(t, err, "Error starting driver on Node %+v", scheduledNodes[0])

	err = volumeDriver.WaitDriverUpOnNode(stoppedNode, defaultWaitTimeout)
	require.NoError(t, err, "Error waiting for Node to start %+v", scheduledNodes[0])
	log.InfoD("Verified volume driver is up on node: %s", stoppedNode)

	destroyAndWait(t, ctxs)

	// If we are here then the test has passed
	testResult = testResultPass
	logrus.Infof("Test status at end of %s test: %s", t.Name(), testResult)
}

func poolMaintenanceTest(t *testing.T) {
	dash.TestCaseBegin("Pool Maintenance", "Stork scheduling test with pool in maintenance mode", "", nil)
	var testrailID, testResult = 86080, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult)
	defer updateTestRail(&testResult, testrailID, runID)

	log.InfoD("Deploy App")
	ctxs, err := schedulerDriver.Schedule(generateInstanceID(t, "pool-test"),
		scheduler.ScheduleOptions{AppKeys: []string{"mysql-1-pvc"}})
	require.NoError(t, err, "Error scheduling task")
	require.Equal(t, 1, len(ctxs), "Only one task should have started")

	err = schedulerDriver.WaitForRunning(ctxs[0], defaultWaitTimeout, defaultWaitInterval)
	require.NoError(t, err, "Error waiting for pod to get to running state")

	scheduledNodes, err := schedulerDriver.GetNodesForApp(ctxs[0])
	require.NoError(t, err, "Error getting node for app")
	require.Equal(t, 1, len(scheduledNodes), "App should be scheduled on one node")

	volumeNames := getVolumeNames(t, ctxs[0])
	require.Equal(t, 1, len(volumeNames), "Should have only one volume")

	verifyScheduledNode(t, scheduledNodes[0], volumeNames)

	log.InfoD("Enter pool in maintenance mode on node: %s", scheduledNodes[0])
	err = volumeDriver.EnterPoolMaintenance(scheduledNodes[0])
	require.NoError(t, err, "Error entering pool maintenance mode on scheduled node %+v", scheduledNodes[0])
	poolMaintenanceNode := scheduledNodes[0]

	// Wait for node to go into maintenance mode
	time.Sleep(5 * time.Minute)

	// Delete the pods so that they get rescheduled
	pods, err := getPodsForApp(ctxs[0])
	require.NoError(t, err, "Failed to get pods for app")
	err = core.Instance().DeletePods(pods, false)
	require.NoError(t, err, "Error deleting the pods")

	err = schedulerDriver.WaitForRunning(ctxs[0], defaultWaitTimeout, defaultWaitInterval)
	require.NoError(t, err, "Error waiting for pod to get to running state after deletion")

	scheduledNodes, err = schedulerDriver.GetNodesForApp(ctxs[0])
	require.NoError(t, err, "Error getting node for app")
	require.Equal(t, 1, len(scheduledNodes), "App should be scheduled on one node")
	require.NotEqual(t, poolMaintenanceNode.Name, scheduledNodes[0].Name, "Pod should not be scheduled on node in PoolMaintenance state")

	err = volumeDriver.ExitPoolMaintenance(poolMaintenanceNode)
	require.NoError(t, err, "Error exiting pool maintenance mode on node %+v", scheduledNodes[0])

	err = volumeDriver.WaitDriverUpOnNode(poolMaintenanceNode, defaultWaitTimeout)
	require.NoError(t, err, "Error waiting for Node to start %+v", scheduledNodes[0])

	destroyAndWait(t, ctxs)

	// If we are here then the test has passed
	testResult = testResultPass
	logrus.Infof("Test status at end of %s test: %s", t.Name(), testResult)
}

func pvcOwnershipTest(t *testing.T) {
	dash.TestCaseBegin("PVC ownership", "Validating PVC ownership", "", nil)
	defer dash.TestCaseEnd()
	var testrailID, testResult = 50781, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult)
	defer updateTestRail(&testResult, testrailID, runID)

	log.InfoD("Schedule mysql app")
	ctxs, err := schedulerDriver.Schedule(generateInstanceID(t, "ownershiptest"),
		scheduler.ScheduleOptions{AppKeys: []string{"mysql-repl-1"}})
	require.NoError(t, err, "Error scheduling task")
	require.Equal(t, 1, len(ctxs), "Only one task should have started")

	err = schedulerDriver.WaitForRunning(ctxs[0], defaultWaitTimeout, defaultWaitInterval)
	require.NoError(t, err, "Error waiting for pod to get to running state")

	scheduledNodes, err := schedulerDriver.GetNodesForApp(ctxs[0])
	require.NoError(t, err, "Error getting node for app")
	require.Equal(t, 1, len(scheduledNodes), "App should be scheduled on one node")

	volumeNames := getVolumeNames(t, ctxs[0])
	require.Equal(t, 1, len(volumeNames), "Should have only one volume")

	verifyScheduledNode(t, scheduledNodes[0], volumeNames)

	for _, spec := range ctxs[0].App.SpecList {
		log.InfoD("Delete storage class.")
		if obj, ok := spec.(*storage_api.StorageClass); ok {
			err := storage.Instance().DeleteStorageClass(obj.Name)
			require.NoError(t, err, "Error deleting storage class for mysql.")
		}
		if obj, ok := spec.(*v1.PersistentVolumeClaim); ok {
			updatePVC, err := core.Instance().GetPersistentVolumeClaim(obj.Name, obj.Namespace)
			require.NoError(t, err, "Error getting persistent volume claim.")
			log.InfoD("Delete storage class annotation on PVC: %s", updatePVC.Name)
			delete(updatePVC.Annotations, annotationStorageProvisioner)
			_, err = core.Instance().UpdatePersistentVolumeClaim(updatePVC)
			require.NoError(t, err, "Error updating annotations in PVC.")
		}
	}

	log.InfoD("Stop volume driver on scheduled node: %s", scheduledNodes[0].Name)
	err = volumeDriver.StopDriver(scheduledNodes, false, nil)
	require.NoError(t, err, "Error stopping driver on scheduled Node %+v", scheduledNodes[0])
	// make sure to start driver if test failed
	defer func() {
		err = volumeDriver.StartDriver(scheduledNodes[0])
		require.NoError(t, err, "Error starting driver on scheduled Node %+v", scheduledNodes[0])
	}()
	// node timeout bumped to 4.5 mins from stork 2.9.0
	// ref: https://github.com/libopenstorage/stork/pull/1028
	// volumeDriver.StopDriver waits for 10 seconds for driver
	// to go down gracefully
	// lets wait for at least 2.5 mins for PX to go down
	time.Sleep(7 * time.Minute)

	var errUnscheduledPod bool
	for _, spec := range ctxs[0].App.SpecList {
		if obj, ok := spec.(*apps_api.Deployment); ok {
			if obj.Name == "mysql" {
				depPods, err := apps.Instance().GetDeploymentPods(obj)
				require.NoError(t, err, "Error getting pods for deployment ,mysql.")
				for _, pod := range depPods {
					for _, cond := range pod.Status.Conditions {
						if cond.Type == v1.PodScheduled && cond.Status == v1.ConditionFalse {
							log.InfoD("Unscheduled pod found: %s", pod.Name)
							errUnscheduledPod = true
						}
					}
				}
			}
		}
	}
	require.Equal(t, true, errUnscheduledPod, "Pod should not have been schedule.")

	err = volumeDriver.StartDriver(scheduledNodes[0])
	require.NoError(t, err, "Error starting driver on scheduled Node %+v", scheduledNodes[0])

	err = volumeDriver.WaitDriverUpOnNode(scheduledNodes[0], defaultWaitTimeout)
	require.NoError(t, err, "Volume driver is not up on Node %+v", scheduledNodes[0])

	destroyAndWait(t, ctxs)

	// If we are here then the test has passed
	testResult = testResultPass
	logrus.Infof("Test status at end of %s test: %s", t.Name(), testResult)
}

func antihyperconvergenceTest(t *testing.T) {
	dash.TestCaseBegin("Stork scheduler antihyperconvergence test", "validate antihyperconvergence for app with shared V4 SVC volume", "", nil)
	var testrailID, testResult = 85859, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult)
	defer updateTestRail(&testResult, testrailID, runID)

	log.InfoD("Schedule app")
	ctxs, err := schedulerDriver.Schedule("antihyperconvergencetest",
		scheduler.ScheduleOptions{
			AppKeys: []string{"test-sv4-svc-repl1"},
		})
	require.NoError(t, err, "Error scheduling task")
	require.Equal(t, 1, len(ctxs), "Only one task should have started")

	logrus.Infof("Waiting for all Pods to come online")
	err = schedulerDriver.WaitForRunning(ctxs[0], defaultWaitTimeout, defaultWaitInterval)
	require.NoError(t, err, "Error waiting for pod to get to running state")

	scheduledNodes, err := schedulerDriver.GetNodesForApp(ctxs[0])
	require.NoError(t, err, "Error getting node for app")
	require.Equal(t, 3, len(scheduledNodes), "App should be scheduled on 3 nodes")

	volumeNames := getVolumeNames(t, ctxs[0])
	require.Equal(t, 1, len(volumeNames), "Should have only one volume")

	logrus.Infof("Verifying Pods scheduling favors antihyperconvergence")
	verifyAntihyperconvergence(t, scheduledNodes, volumeNames)

	destroyAndWait(t, ctxs)

	// If we are here then the test has passed
	testResult = testResultPass
	logrus.Infof("Test status at end of %s test: %s", t.Name(), testResult)
}

func antihyperconvergenceTestPreferRemoteOnlyTest(t *testing.T) {
	log.InfoD("Verify anti-hyperconvergence with prefer remote node only option")
	var testrailID, testResult = 85860, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult)
	defer updateTestRail(&testResult, testrailID, runID)

	ctxs, err := schedulerDriver.Schedule("preferremoteonlytest",
		scheduler.ScheduleOptions{
			AppKeys: []string{"test-sv4-svc-repl1-prefer-remote-only"},
		})
	require.NoError(t, err, "Error scheduling task")
	require.Equal(t, 1, len(ctxs), "Only one task should have started")
	log.InfoD("App deployed")

	logrus.Infof("Waiting for all Pods to come online")
	err = schedulerDriver.WaitForRunning(ctxs[0], defaultWaitTimeout, defaultWaitInterval)
	require.NoError(t, err, "Error waiting for pod to get to running state")

	scheduledNodes, err := schedulerDriver.GetNodesForApp(ctxs[0])
	require.NoError(t, err, "Error getting node for app")
	require.Equal(t, 3, len(scheduledNodes), "App should be scheduled on 3 nodes")

	volumeNames := getVolumeNames(t, ctxs[0])
	require.Equal(t, 1, len(volumeNames), "Should have only one volume")

	logrus.Infof("Verifying Pods scheduling favors antihyperconvergence")
	verifyAntihyperconvergence(t, scheduledNodes, volumeNames)

	logrus.Infof("Cordon the nodes where application pods are running")
	for _, schedNode := range scheduledNodes {
		if schedNode.IsStorageDriverInstalled {
			err = core.Instance().CordonNode(schedNode.Name, defaultWaitTimeout, defaultWaitInterval)
			require.NoError(t, err, "Error cordorning k8s node for stork test pod")
		}
	}

	logrus.Infof("Delete application pods")
	for _, spec := range ctxs[0].App.SpecList {
		if obj, ok := spec.(*apps_api.Deployment); ok {
			depPods, err := apps.Instance().GetDeploymentPods(obj)
			require.NoError(t, err, "Error getting pods for deployment.")

			err = core.Instance().DeletePods(depPods, true)
			require.NoError(t, err, "Error delete deployment pods for deploymet.")
		}
	}

	logrus.Infof("Waiting for all Pods to come online")
	err = schedulerDriver.WaitForRunning(ctxs[0], maxPodBringupTime, defaultWaitInterval)
	require.Error(t, err, "Expected an error while scheduling the pod")

	destroyAndWait(t, ctxs)

	// Uncordon all the nodes after test
	nodeNameMap := node.GetNodesByName()
	for _, schedNode := range nodeNameMap {
		err = core.Instance().UnCordonNode(schedNode.Name, defaultWaitTimeout, defaultWaitInterval)
		require.NoError(t, err, "Error uncordorning k8s node for stork test pod")
	}

	// If we are here then the test has passed
	testResult = testResultPass
	logrus.Infof("Test status at end of %s test: %s", t.Name(), testResult)
}

func preferRemoteNodeFalseHyperconvergenceTest(t *testing.T) {
	dash.TestCaseBegin("Stork scheduler prefer remote node antihyperconvergence test", "validate antihyperconvergence with preferRemoteNodeOnly flag", "", nil)
	var testrailID, testResult = 92964, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult)
	defer updateTestRail(&testResult, testrailID, runID)

	ctxs, err := schedulerDriver.Schedule("preferremotenodefalsetest",
		scheduler.ScheduleOptions{
			AppKeys: []string{"test-sv4-svc-prefer-remote-node-false"},
		})
	require.NoError(t, err, "Error scheduling task")
	require.Equal(t, 1, len(ctxs), "Only one task should have started")

	logrus.Infof("Waiting for all Pods to come online")
	err = schedulerDriver.WaitForRunning(ctxs[0], defaultWaitTimeout, defaultWaitInterval)
	require.NoError(t, err, "Error waiting for pod to get to running state")

	scheduledNodes, err := schedulerDriver.GetNodesForApp(ctxs[0])
	require.NoError(t, err, "Error getting node for app")
	require.Equal(t, 3, len(scheduledNodes), "App should be scheduled on 3 nodes")

	volumeNames := getVolumeNames(t, ctxs[0])
	require.Equal(t, 1, len(volumeNames), "Should have only one volume")

	logrus.Infof("Verifying Pods scheduling favors hyperconvergence")
	verifyScheduledNodesMultipleReplicas(t, scheduledNodes, volumeNames)
}

func verifyAntihyperconvergence(t *testing.T, appNodes []node.Node, volumes []string) {
	driverNodes, err := storkVolumeDriver.GetNodes()
	require.NoError(t, err, "Unable to get driver nodes from stork volume driver")

	scores := make(map[string]int)
	idMap := make(map[string]*storkdriver.NodeInfo)
	for _, dNode := range driverNodes {
		scores[dNode.Hostname] = 100
		idMap[dNode.StorageID] = dNode
	}

	// Assign zero score to replica nodes for antihyperconvergence
	// Assign non zero score to non replica nodes for antihyperconvergence
	for _, vol := range volumes {
		volInfo, err := storkVolumeDriver.InspectVolume(vol)
		require.NoError(t, err, "Unable to inspect volume driver")
		for _, dataNode := range volInfo.DataNodes {
			hostname := idMap[dataNode].Hostname
			scores[hostname] = 0
		}
	}

	highScore := 0
	for _, score := range scores {
		if score > highScore {
			highScore = score
		}
	}

	for _, appNode := range appNodes {
		require.Equal(t, highScore, scores[appNode.Name], "Scheduled node does not have the highest score")
	}
	log.InfoD("Verified scheduled node has the highest score")
}

func equalPodSpreadTest(t *testing.T) {
	dash.TestCaseBegin("Stork scheduler equal pod spread test", "Verify equal pod spread is achieved using stork for an app", "", nil)
	var testrailID, testResult = 84664, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult)
	defer updateTestRail(&testResult, testrailID, runID)

	ctxs, err := schedulerDriver.Schedule("equal-pod-spread-test",
		scheduler.ScheduleOptions{
			AppKeys: []string{"postgres"},
		})
	require.NoError(t, err, "Error scheduling task")
	require.Equal(t, 1, len(ctxs), "Only one task should have started")

	logrus.Infof("Waiting for all Pods to come online")
	err = schedulerDriver.WaitForRunning(ctxs[0], defaultWaitTimeout, defaultWaitInterval)
	require.NoError(t, err, "Error waiting for pod to get to running state")

	volumeNames := getVolumeNames(t, ctxs[0])
	require.Equal(t, 3, len(volumeNames), "Should have only 3 volumes")

	scheduledNodes, err := schedulerDriver.GetNodesForApp(ctxs[0])
	require.NoError(t, err, "Error getting node for app")
	require.Equal(t, 3, len(scheduledNodes), "apps should have been deployed on 3 separate nodes, but got scheduled on only %d nodes",
		len(scheduledNodes))

	scheduledNodesMap := make(map[string]bool)
	for _, node := range scheduledNodes {
		if _, ok := scheduledNodesMap[node.Name]; !ok {
			scheduledNodesMap[node.Name] = true
		}
	}
	require.Equal(t, 3, len(scheduledNodesMap), "App should be scheduled on 3 nodes, pod spread not achieved.")

	logrus.Infof("Verifying that volume replicase are spread equally across worker nodes")
	log.InfoD("Pod spread verified")

	logrus.Info("Deleting apps created by the test")
	destroyAndWait(t, ctxs)

	// If we are here then the test has passed
	testResult = testResultPass
	logrus.Infof("Test status at end of %s test: %s", t.Name(), testResult)
}
