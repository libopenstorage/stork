//go:build integrationtest
// +build integrationtest

package integrationtest

import (
	"fmt"
	"testing"
	"time"

	storkdriver "github.com/libopenstorage/stork/drivers/volume"
	"github.com/libopenstorage/stork/pkg/log"
	"github.com/portworx/sched-ops/k8s/apps"
	"github.com/portworx/sched-ops/k8s/core"
	"github.com/portworx/sched-ops/k8s/storage"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/scheduler"
	apps_api "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	storage_api "k8s.io/api/storage/v1"
	"k8s.io/utils/strings/slices"
)

const (
	annotationStorageProvisioner = "volume.beta.kubernetes.io/storage-provisioner"
	dashProductName              = "stork"
	dashStatsType                = "stork-integration-test"

	maxPodBringupTime time.Duration = 2 * time.Minute
)

func TestExtender(t *testing.T) {
	err := setSourceKubeConfig()
	log.FailOnError(t, err, "failed to set kubeconfig to source cluster: %v", err)
	currentTestSuite = t.Name()

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
	t.Run("antihyperconvergenceAfterVolumeLabelUpdate", antihyperconvergenceAfterVolumeLabelUpdate)
	t.Run("equalPodSpreadTest", equalPodSpreadTest)

	err = setRemoteConfig("")
	log.FailOnError(t, err, "setting kubeconfig to default failed")
}

func noPVCTest(t *testing.T) {
	var testrailID, testResult = 50785, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	defer updateDashStats(t.Name(), &testResult)

	ctxs, err := schedulerDriver.Schedule(generateInstanceID(t, "nopvctest"),
		scheduler.ScheduleOptions{AppKeys: []string{"mysql-nopvc"}})
	log.FailOnError(t, err, "Error scheduling task")
	Dash.VerifyFatal(t, len(ctxs), 1, "Only one task should have started")

	err = schedulerDriver.WaitForRunning(ctxs[0], defaultWaitTimeout, defaultWaitInterval)
	log.FailOnError(t, err, "Error waiting for pod to get to running state")

	destroyAndWait(t, ctxs)

	// If we are here then the test has passed
	testResult = testResultPass
	log.InfoD("Test status at end of %s test: %s", t.Name(), testResult)
}

func singlePVCTest(t *testing.T) {
	var testrailID, testResult = 50786, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	defer updateDashStats(t.Name(), &testResult)

	ctxs, err := schedulerDriver.Schedule(generateInstanceID(t, "singlepvctest"),
		scheduler.ScheduleOptions{AppKeys: []string{"mysql-1-pvc"}})
	log.FailOnError(t, err, "Error scheduling task")
	Dash.VerifyFatal(t, len(ctxs), 1, "Only one task should have started")

	err = schedulerDriver.WaitForRunning(ctxs[0], defaultWaitTimeout, defaultWaitInterval)
	log.FailOnError(t, err, "Error waiting for pod to get to running state")

	scheduledNodes, err := schedulerDriver.GetNodesForApp(ctxs[0])
	log.FailOnError(t, err, "Error getting node for app")
	Dash.VerifyFatal(t, len(scheduledNodes), 1, "App should be scheduled on one node")

	volumeNames := getVolumeNames(t, ctxs[0])
	Dash.VerifyFatal(t, len(volumeNames), 1, "Should only have one volume")

	verifyScheduledNode(t, scheduledNodes[0], volumeNames)

	destroyAndWait(t, ctxs)

	// If we are here then the test has passed
	testResult = testResultPass
	log.InfoD("Test status at end of %s test: %s", t.Name(), testResult)
}

func statefulsetTest(t *testing.T) {
	var testrailID, testResult = 50787, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	defer updateDashStats(t.Name(), &testResult)

	ctxs, err := schedulerDriver.Schedule(generateInstanceID(t, "sstest"),
		scheduler.ScheduleOptions{AppKeys: []string{"elasticsearch"}})
	log.FailOnError(t, err, "Error scheduling task")
	Dash.VerifyFatal(t, len(ctxs), 1, "Only one task should have started")

	err = schedulerDriver.WaitForRunning(ctxs[0], defaultWaitTimeout, defaultWaitInterval)
	log.FailOnError(t, err, "Error waiting for elasticsearch statefulset to get to running state")

	scheduledNodes, err := schedulerDriver.GetNodesForApp(ctxs[0])
	log.FailOnError(t, err, "Error getting node for app")
	// TODO: There is no qurantee that all pods will be scheduled on
	// different nodes, we should restrict pvc to repl1 and one px node
	// use preferLocalNode flag to ensure pod is getting scheduled on node
	// where replicas exist
	log.InfoD("sts pod scheduled on %d nodes", len(scheduledNodes))
	// TODO: torpedo doesn't return correct volumes here
	volumeNames := getVolumeNames(t, ctxs[0])
	Dash.VerifyFatal(t, len(volumeNames), 3, "Should have 3 volumes")

	// TODO: Add verification for node where it was scheduled
	// torpedo doesn't return the pod->pvc mapping, so we can't validate that it
	// got scheduled on a prioritized node
	verifyScheduledNode(t, scheduledNodes[0], volumeNames)

	destroyAndWait(t, ctxs)

	// If we are here then the test has passed
	testResult = testResultPass
	log.InfoD("Test status at end of %s test: %s", t.Name(), testResult)
}

func multiplePVCTest(t *testing.T) {
	var testrailID, testResult = 50788, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	defer updateDashStats(t.Name(), &testResult)

	ctxs, err := schedulerDriver.Schedule(generateInstanceID(t, "multipvctest"),
		scheduler.ScheduleOptions{AppKeys: []string{"mysql-2-pvc"}})
	log.FailOnError(t, err, "Error scheduling task")
	Dash.VerifyFatal(t, len(ctxs), 1, "Only one task should have started")

	err = schedulerDriver.WaitForRunning(ctxs[0], defaultWaitTimeout, defaultWaitInterval)
	log.FailOnError(t, err, "Error waiting for pod to get to running state")

	scheduledNodes, err := schedulerDriver.GetNodesForApp(ctxs[0])
	log.FailOnError(t, err, "Error getting node for app")
	Dash.VerifyFatal(t, len(scheduledNodes), 1, "App should be scheduled on one node")

	volumeNames := getVolumeNames(t, ctxs[0])
	Dash.VerifyFatal(t, len(volumeNames), 2, "Should have two volumes")

	verifyScheduledNode(t, scheduledNodes[0], volumeNames)
	destroyAndWait(t, ctxs)

	// If we are here then the test has passed
	testResult = testResultPass
	log.InfoD("Test status at end of %s test: %s", t.Name(), testResult)
}

func driverNodeErrorTest(t *testing.T) {
	var testrailID, testResult = 50789, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	defer updateDashStats(t.Name(), &testResult)

	ctxs, err := schedulerDriver.Schedule(generateInstanceID(t, "drivererrtest"),
		scheduler.ScheduleOptions{AppKeys: []string{"mysql-1-pvc"}})
	log.FailOnError(t, err, "Error scheduling task")
	Dash.VerifyFatal(t, len(ctxs), 1, "Only one task should have started")

	err = schedulerDriver.WaitForRunning(ctxs[0], defaultWaitTimeout, defaultWaitInterval)
	log.FailOnError(t, err, "Error waiting for pod to get to running state")

	scheduledNodes, err := schedulerDriver.GetNodesForApp(ctxs[0])
	log.FailOnError(t, err, "Error getting node for app")
	Dash.VerifyFatal(t, len(scheduledNodes), 1, "App should be scheduled on one node")

	volumeNames := getVolumeNames(t, ctxs[0])
	Dash.VerifyFatal(t, len(volumeNames), 1, "Should have only one volume")

	verifyScheduledNode(t, scheduledNodes[0], volumeNames)

	time.Sleep(1 * time.Minute)

	err = volumeDriver.StopDriver(scheduledNodes, false, nil)
	log.FailOnError(t, err, "Error stopping driver on scheduled Node %+v", scheduledNodes[0])
	stoppedNode := scheduledNodes[0]
	// node timeout bumped to 4 mins from stork 2.9.0
	// ref: https://github.com/libopenstorage/stork/pull/1028
	time.Sleep(5 * time.Minute)

	err = schedulerDriver.WaitForRunning(ctxs[0], defaultWaitTimeout, defaultWaitInterval)
	log.FailOnError(t, err, "Error waiting for pod to get to running state after deletion")

	scheduledNodes, err = schedulerDriver.GetNodesForApp(ctxs[0])
	log.FailOnError(t, err, "Error getting node for app")
	Dash.VerifyFatal(t, 1, len(scheduledNodes), "App should be scheduled on one node")
	Dash.VerifyFatal(t, stoppedNode.Name != scheduledNodes[0].Name, true, "Task restarted on stopped node")

	volumeNames = getVolumeNames(t, ctxs[0])
	Dash.VerifyFatal(t, 1, len(volumeNames), "Should have only one volume")

	verifyScheduledNode(t, scheduledNodes[0], volumeNames)

	err = volumeDriver.StartDriver(stoppedNode)
	log.FailOnError(t, err, "Error starting driver on Node %+v", scheduledNodes[0])

	err = volumeDriver.WaitDriverUpOnNode(stoppedNode, defaultWaitTimeout)
	log.FailOnError(t, err, "Error waiting for Node to start %+v", scheduledNodes[0])

	destroyAndWait(t, ctxs)

	// If we are here then the test has passed
	testResult = testResultPass
	log.InfoD("Test status at end of %s test: %s", t.Name(), testResult)
}

func poolMaintenanceTest(t *testing.T) {
	var testrailID, testResult = 86080, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	defer updateDashStats(t.Name(), &testResult)

	ctxs, err := schedulerDriver.Schedule(generateInstanceID(t, "pool-test"),
		scheduler.ScheduleOptions{AppKeys: []string{"mysql-1-pvc"}})
	log.FailOnError(t, err, "Error scheduling task")
	Dash.VerifyFatal(t, 1, len(ctxs), "Only one task should have started")

	err = schedulerDriver.WaitForRunning(ctxs[0], defaultWaitTimeout, defaultWaitInterval)
	log.FailOnError(t, err, "Error waiting for pod to get to running state")

	scheduledNodes, err := schedulerDriver.GetNodesForApp(ctxs[0])
	log.FailOnError(t, err, "Error getting node for app")
	Dash.VerifyFatal(t, 1, len(scheduledNodes), "App should be scheduled on one node")

	volumeNames := getVolumeNames(t, ctxs[0])
	Dash.VerifyFatal(t, 1, len(volumeNames), "Should have only one volume")

	verifyScheduledNode(t, scheduledNodes[0], volumeNames)

	err = volumeDriver.EnterPoolMaintenance(scheduledNodes[0])
	log.FailOnError(t, err, "Error entering pool maintenance mode on scheduled node %+v", scheduledNodes[0])
	poolMaintenanceNode := scheduledNodes[0]

	// Wait for node to go into maintenance mode
	time.Sleep(5 * time.Minute)

	// Delete the pods so that they get rescheduled
	pods, err := getPodsForApp(ctxs[0])
	log.FailOnError(t, err, "Failed to get pods for app")
	err = core.Instance().DeletePods(pods, false)
	log.FailOnError(t, err, "Error deleting the pods")

	err = schedulerDriver.WaitForRunning(ctxs[0], defaultWaitTimeout, defaultWaitInterval)
	log.FailOnError(t, err, "Error waiting for pod to get to running state after deletion")

	scheduledNodes, err = schedulerDriver.GetNodesForApp(ctxs[0])
	log.FailOnError(t, err, "Error getting node for app")
	Dash.VerifyFatal(t, 1, len(scheduledNodes), "App should be scheduled on one node")
	Dash.VerifyFatal(t, poolMaintenanceNode.Name != scheduledNodes[0].Name, true, "Pod should not be scheduled on node in PoolMaintenance state")

	err = volumeDriver.ExitPoolMaintenance(poolMaintenanceNode)
	log.FailOnError(t, err, "Error exiting pool maintenance mode on node %+v", scheduledNodes[0])

	err = volumeDriver.WaitDriverUpOnNode(poolMaintenanceNode, defaultWaitTimeout)
	log.FailOnError(t, err, "Error waiting for Node to start %+v", scheduledNodes[0])

	destroyAndWait(t, ctxs)

	// If we are here then the test has passed
	testResult = testResultPass
	log.InfoD("Test status at end of %s test: %s", t.Name(), testResult)
}

func pvcOwnershipTest(t *testing.T) {
	var testrailID, testResult = 50781, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	defer updateDashStats(t.Name(), &testResult)

	ctxs, err := schedulerDriver.Schedule(generateInstanceID(t, "ownershiptest"),
		scheduler.ScheduleOptions{AppKeys: []string{"mysql-repl-1"}})
	log.FailOnError(t, err, "Error scheduling task")
	Dash.VerifyFatal(t, 1, len(ctxs), "Only one task should have started")

	err = schedulerDriver.WaitForRunning(ctxs[0], defaultWaitTimeout, defaultWaitInterval)
	log.FailOnError(t, err, "Error waiting for pod to get to running state")

	scheduledNodes, err := schedulerDriver.GetNodesForApp(ctxs[0])
	log.FailOnError(t, err, "Error getting node for app")
	Dash.VerifyFatal(t, 1, len(scheduledNodes), "App should be scheduled on one node")

	volumeNames := getVolumeNames(t, ctxs[0])
	Dash.VerifyFatal(t, 1, len(volumeNames), "Should have only one volume")

	verifyScheduledNode(t, scheduledNodes[0], volumeNames)

	for _, spec := range ctxs[0].App.SpecList {
		if obj, ok := spec.(*storage_api.StorageClass); ok {
			err := storage.Instance().DeleteStorageClass(obj.Name)
			log.FailOnError(t, err, "Error deleting storage class for mysql.")
		}
		if obj, ok := spec.(*v1.PersistentVolumeClaim); ok {
			updatePVC, err := core.Instance().GetPersistentVolumeClaim(obj.Name, obj.Namespace)
			log.FailOnError(t, err, "Error getting persistent volume claim.")
			delete(updatePVC.Annotations, annotationStorageProvisioner)
			_, err = core.Instance().UpdatePersistentVolumeClaim(updatePVC)
			log.FailOnError(t, err, "Error updating annotations in PVC.")
		}
	}

	err = volumeDriver.StopDriver(scheduledNodes, false, nil)
	log.FailOnError(t, err, "Error stopping driver on scheduled Node %+v", scheduledNodes[0])
	// make sure to start driver if test failed
	defer func() {
		err = volumeDriver.StartDriver(scheduledNodes[0])
		log.FailOnError(t, err, "Error starting driver on scheduled Node %+v", scheduledNodes[0])
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
				log.FailOnError(t, err, "Error getting pods for deployment ,mysql.")
				for _, pod := range depPods {
					for _, cond := range pod.Status.Conditions {
						if cond.Type == v1.PodScheduled && cond.Status == v1.ConditionFalse {
							errUnscheduledPod = true
						}
					}
				}
			}
		}
	}
	Dash.VerifyFatal(t, true, errUnscheduledPod, "Pod should not have been schedule.")

	err = volumeDriver.StartDriver(scheduledNodes[0])
	log.FailOnError(t, err, "Error starting driver on scheduled Node %+v", scheduledNodes[0])

	err = volumeDriver.WaitDriverUpOnNode(scheduledNodes[0], defaultWaitTimeout)
	log.FailOnError(t, err, "Volume driver is not up on Node %+v", scheduledNodes[0])

	destroyAndWait(t, ctxs)

	// If we are here then the test has passed
	testResult = testResultPass
	log.InfoD("Test status at end of %s test: %s", t.Name(), testResult)
}

func antihyperconvergenceTest(t *testing.T) {
	var testrailID, testResult = 85859, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	defer updateDashStats(t.Name(), &testResult)

	ctxs, err := schedulerDriver.Schedule("antihyperconvergencetest",
		scheduler.ScheduleOptions{
			AppKeys: []string{"test-sv4-svc-repl1"},
		})
	log.FailOnError(t, err, "Error scheduling task")
	Dash.VerifyFatal(t, 1, len(ctxs), "Only one task should have started")

	log.InfoD("Waiting for all Pods to come online")
	err = schedulerDriver.WaitForRunning(ctxs[0], defaultWaitTimeout, defaultWaitInterval)
	log.FailOnError(t, err, "Error waiting for pod to get to running state")

	scheduledNodes, err := schedulerDriver.GetNodesForApp(ctxs[0])
	log.FailOnError(t, err, "Error getting node for app")
	Dash.VerifyFatal(t, 3, len(scheduledNodes), "App should be scheduled on 3 nodes")

	volumeNames := getVolumeNames(t, ctxs[0])
	Dash.VerifyFatal(t, 1, len(volumeNames), "Should have only one volume")

	log.InfoD("Verifying Pods scheduling favors antihyperconvergence")
	verifyAntihyperconvergence(t, scheduledNodes, volumeNames)

	destroyAndWait(t, ctxs)

	// If we are here then the test has passed
	testResult = testResultPass
	log.InfoD("Test status at end of %s test: %s", t.Name(), testResult)
}

func antihyperconvergenceTestPreferRemoteOnlyTest(t *testing.T) {
	var testrailID, testResult = 85860, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	defer updateDashStats(t.Name(), &testResult)

	ctxs, err := schedulerDriver.Schedule("preferremoteonlytest",
		scheduler.ScheduleOptions{
			AppKeys: []string{"test-sv4-svc-repl1-prefer-remote-only"},
		})
	log.FailOnError(t, err, "Error scheduling task")
	Dash.VerifyFatal(t, 1, len(ctxs), "Only one task should have started")

	log.InfoD("Waiting for all Pods to come online")
	err = schedulerDriver.WaitForRunning(ctxs[0], defaultWaitTimeout, defaultWaitInterval)
	log.FailOnError(t, err, "Error waiting for pod to get to running state")

	scheduledNodes, err := schedulerDriver.GetNodesForApp(ctxs[0])
	log.FailOnError(t, err, "Error getting node for app")
	Dash.VerifyFatal(t, 3, len(scheduledNodes), "App should be scheduled on 3 nodes")

	volumeNames := getVolumeNames(t, ctxs[0])
	Dash.VerifyFatal(t, 1, len(volumeNames), "Should have only one volume")

	log.InfoD("Verifying Pods scheduling favors antihyperconvergence")
	verifyAntihyperconvergence(t, scheduledNodes, volumeNames)

	log.InfoD("Cordon the nodes where application pods are running")
	for _, schedNode := range scheduledNodes {
		if schedNode.IsStorageDriverInstalled {
			err = core.Instance().CordonNode(schedNode.Name, defaultWaitTimeout, defaultWaitInterval)
			log.FailOnError(t, err, "Error cordorning k8s node for stork test pod")
		}
	}

	log.InfoD("Delete application pods")
	for _, spec := range ctxs[0].App.SpecList {
		if obj, ok := spec.(*apps_api.Deployment); ok {
			depPods, err := apps.Instance().GetDeploymentPods(obj)
			log.FailOnError(t, err, "Error getting pods for deployment.")

			err = core.Instance().DeletePods(depPods, true)
			log.FailOnError(t, err, "Error delete deployment pods for deploymet.")
		}
	}

	log.InfoD("Waiting for all Pods to come online")
	err = schedulerDriver.WaitForRunning(ctxs[0], maxPodBringupTime, defaultWaitInterval)
	log.FailOnNoError(t, err, "Expected an error while scheduling the pod")

	destroyAndWait(t, ctxs)

	// Uncordon all the nodes after test
	nodeNameMap := node.GetNodesByName()
	for _, schedNode := range nodeNameMap {
		err = core.Instance().UnCordonNode(schedNode.Name, defaultWaitTimeout, defaultWaitInterval)
		log.FailOnError(t, err, "Error uncordorning k8s node for stork test pod")
	}

	// If we are here then the test has passed
	testResult = testResultPass
	log.InfoD("Test status at end of %s test: %s", t.Name(), testResult)
}

// This test has been added as part of fix for PWX-35513
func antihyperconvergenceAfterVolumeLabelUpdate(t *testing.T) {
	var testrailID, testResult = 94378, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	defer updateDashStats(t.Name(), &testResult)

	ctxs, err := schedulerDriver.Schedule("volumelabelupdate",
		scheduler.ScheduleOptions{
			AppKeys: []string{"test-sv4-replica1-prefer-remote-only"},
		})
	log.FailOnError(t, err, "Error scheduling task")
	Dash.VerifyFatal(t, 1, len(ctxs), "Only one task should have started")

	log.InfoD("Waiting for all Pods to come online")
	err = schedulerDriver.WaitForRunning(ctxs[0], defaultWaitTimeout, defaultWaitInterval)
	log.FailOnError(t, err, "Error waiting for pod to get to running state")

	scheduledNodes, err := schedulerDriver.GetNodesForApp(ctxs[0])
	log.FailOnError(t, err, "Error getting node for app")
	Dash.VerifyFatal(t, 1, len(scheduledNodes), "App should be scheduled on 1 nodes")

	volumeNames := getVolumeNames(t, ctxs[0])
	Dash.VerifyFatal(t, 1, len(volumeNames), "Should have only one volume")

	nodesWithoutVolumeData := getNodesWithoutVolumeData(t, volumeNames[0])

	// cordon the nodes without volume data
	for _, node := range nodesWithoutVolumeData {
		err = core.Instance().CordonNode(node, defaultWaitTimeout, defaultWaitInterval)
		log.FailOnError(t, err, "Error cordorning k8s node for stork test pod")
	}

	// update the label in px volume
	labels := map[string]string{"stork.libopenstorage.org/preferRemoteNodeOnly": "false"}
	updatePXVolumeLabel(t, volumeNames[0], labels)

	// restart the pod
	log.InfoD("Delete application pods")
	for _, spec := range ctxs[0].App.SpecList {
		if obj, ok := spec.(*apps_api.Deployment); ok {
			depPods, err := apps.Instance().GetDeploymentPods(obj)
			log.FailOnError(t, err, "Error getting pods for deployment.")

			err = core.Instance().DeletePods(depPods, true)
			log.FailOnError(t, err, "Error delete deployment pods for deploymet.")
		}
	}

	// check that pod can get scheduled
	log.InfoD("Waiting for all Pods to come online")
	err = schedulerDriver.WaitForRunning(ctxs[0], maxPodBringupTime, defaultWaitInterval)
	log.FailOnNoError(t, err, "Expected an error while scheduling the pod")

	destroyAndWait(t, ctxs)

	// uncordon the nodes
	for _, node := range nodesWithoutVolumeData {
		err = core.Instance().UnCordonNode(node, defaultWaitTimeout, defaultWaitInterval)
		log.FailOnError(t, err, "Error cordorning k8s node for stork test pod")
	}

	// If we are here then the test has passed
	testResult = testResultPass
	log.InfoD("Test status at end of %s test: %s", t.Name(), testResult)
}

func preferRemoteNodeFalseHyperconvergenceTest(t *testing.T) {
	var testrailID, testResult = 92964, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	defer updateDashStats(t.Name(), &testResult)

	ctxs, err := schedulerDriver.Schedule("preferremotenodefalsetest",
		scheduler.ScheduleOptions{
			AppKeys: []string{"test-sv4-svc-prefer-remote-node-false"},
		})
	log.FailOnError(t, err, "Error scheduling task")
	Dash.VerifyFatal(t, 1, len(ctxs), "Only one task should have started")

	log.InfoD("Waiting for all Pods to come online")
	err = schedulerDriver.WaitForRunning(ctxs[0], defaultWaitTimeout, defaultWaitInterval)
	log.FailOnError(t, err, "Error waiting for pod to get to running state")

	scheduledNodes, err := schedulerDriver.GetNodesForApp(ctxs[0])
	log.FailOnError(t, err, "Error getting node for app")
	Dash.VerifyFatal(t, 3, len(scheduledNodes), "App should be scheduled on 3 nodes")

	volumeNames := getVolumeNames(t, ctxs[0])
	Dash.VerifyFatal(t, 1, len(volumeNames), "Should have only one volume")

	log.InfoD("Verifying Pods scheduling favors hyperconvergence")
	verifyScheduledNodesMultipleReplicas(t, scheduledNodes, volumeNames)

	// If we are here then the test has passed
	testResult = testResultPass
	log.InfoD("Test status at end of %s test: %s", t.Name(), testResult)
}

func getNodesWithoutVolumeData(t *testing.T, volume string) []string {
	driverNodes, err := storkVolumeDriver.GetNodes()
	log.FailOnError(t, err, "Unable to get driver nodes from stork volume driver")

	idMap := make(map[string]*storkdriver.NodeInfo)
	for _, dNode := range driverNodes {
		idMap[dNode.StorageID] = dNode
	}

	volInfo, err := storkVolumeDriver.InspectVolume(volume)
	log.FailOnError(t, err, "Unable to inspect volume driver")

	nodes := node.GetWorkerNodes()
	nodesWithoutVolumeData := make([]string, 0)
	for _, node := range nodes {
		if !slices.Contains(volInfo.DataNodes, node.Name) {
			nodesWithoutVolumeData = append(nodesWithoutVolumeData, node.Name)
		}
	}
	return nodesWithoutVolumeData
}

func verifyAntihyperconvergence(t *testing.T, appNodes []node.Node, volumes []string) {
	driverNodes, err := storkVolumeDriver.GetNodes()
	log.FailOnError(t, err, "Unable to get driver nodes from stork volume driver")

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
		log.FailOnError(t, err, "Unable to inspect volume driver")
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
		Dash.VerifyFatal(t, highScore, scores[appNode.Name], "Scheduled node does not have the highest score")
	}
}

func equalPodSpreadTest(t *testing.T) {
	var testrailID, testResult = 84664, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	defer updateDashStats(t.Name(), &testResult)

	ctxs, err := schedulerDriver.Schedule("equal-pod-spread-test",
		scheduler.ScheduleOptions{
			AppKeys: []string{"postgres"},
		})
	log.FailOnError(t, err, "Error scheduling task")
	Dash.VerifyFatal(t, 1, len(ctxs), "Only one task should have started")

	log.InfoD("Waiting for all Pods to come online")
	err = schedulerDriver.WaitForRunning(ctxs[0], defaultWaitTimeout, defaultWaitInterval)
	log.FailOnError(t, err, "Error waiting for pod to get to running state")

	volumeNames := getVolumeNames(t, ctxs[0])
	Dash.VerifyFatal(t, 3, len(volumeNames), "Should have only 3 volumes")

	scheduledNodes, err := schedulerDriver.GetNodesForApp(ctxs[0])
	log.FailOnError(t, err, "Error getting node for app")
	Dash.VerifyFatal(t, 3, len(scheduledNodes), fmt.Sprintf("apps should have been deployed on 3 separate nodes, but got scheduled on only %d nodes",
		len(scheduledNodes)))

	scheduledNodesMap := make(map[string]bool)
	for _, node := range scheduledNodes {
		if _, ok := scheduledNodesMap[node.Name]; !ok {
			scheduledNodesMap[node.Name] = true
		}
	}
	Dash.VerifyFatal(t, 3, len(scheduledNodesMap), "App should be scheduled on 3 nodes, pod spread not achieved.")

	log.InfoD("Verifying that volume replicase are spread equally across worker nodes")

	log.Info("Deleting apps created by the test")
	destroyAndWait(t, ctxs)

	// If we are here then the test has passed
	testResult = testResultPass
	log.InfoD("Test status at end of %s test: %s", t.Name(), testResult)
}
