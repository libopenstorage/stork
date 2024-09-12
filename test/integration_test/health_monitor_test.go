//go:build integrationtest
// +build integrationtest

package integrationtest

import (
	"fmt"
	"math"
	"testing"
	"time"

	storkdriver "github.com/libopenstorage/stork/drivers/volume"
	"github.com/libopenstorage/stork/pkg/log"
	"github.com/portworx/sched-ops/k8s/apps"
	"github.com/portworx/sched-ops/k8s/core"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/scheduler"
	appsapi "k8s.io/api/apps/v1"
)

const (
	// node offline timeout just above 4.5 minutes
	// which is the max time stork could take to delete a app pod.
	nodeOfflineTimeout        = 295 * time.Second
	podDeleteBatchSize        = 5
	podBatchDeleteIntervalSec = 30
)

func TestHealthMonitor(t *testing.T) {
	err := setSourceKubeConfig()
	log.FailOnError(t, err, "failed to set kubeconfig to source cluster: %v", err)
	currentTestSuite = t.Name()

	t.Run("stopDriverTest", stopDriverTest)
	t.Run("stopKubeletTest", stopKubeletTest)
	t.Run("poolMaintenanceHealthTest", poolMaintenanceHealthTest)
	t.Run("healthCheckFixTest", healthCheckFixTest)
	t.Run("stopDriverCsiPodFailoverTest", stopDriverCsiPodFailoverTest)
	t.Run("stopDriverTestScale", stopDriverTestScale)

	err = setRemoteConfig("")
	log.FailOnError(t, err, "setting kubeconfig to default failed")
}

func stopDriverTest(t *testing.T) {

	log.InfoD("stop driver test")
	var testrailID, testResult = 50790, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	defer updateDashStats(t.Name(), &testResult)

	ctxs, err := schedulerDriver.Schedule(generateInstanceID(t, "stopdrivertest"),
		scheduler.ScheduleOptions{AppKeys: []string{"mysql-2-pvc"}})
	log.FailOnError(t, err, "Error scheduling task")
	Dash.VerifyFatal(t, 1, len(ctxs), "Only one task should have started")

	err = schedulerDriver.WaitForRunning(ctxs[0], defaultWaitTimeout, defaultWaitInterval)
	log.FailOnError(t, err, "Error waiting for pod to get to running state")

	scheduledNodes, err := schedulerDriver.GetNodesForApp(ctxs[0])
	log.FailOnError(t, err, "Error getting node for app")
	Dash.VerifyFatal(t, 1, len(scheduledNodes), "App should be scheduled on one node")

	volumeNames := getVolumeNames(t, ctxs[0])
	Dash.VerifyFatal(t, 2, len(volumeNames), "Should have two volumes")

	verifyScheduledNode(t, scheduledNodes[0], volumeNames)

	time.Sleep(1 * time.Minute)

	// Stop the driver and after 3 minutes verify that it moved to another node
	// where the volume is located
	err = volumeDriver.StopDriver(scheduledNodes, false, nil)
	log.FailOnError(t, err, "Error stopping driver on scheduled Node %+v", scheduledNodes[0])
	stoppedNode := scheduledNodes[0]

	time.Sleep(nodeOfflineTimeout)

	log.InfoD("Checking if pod got reschedule to online driver node ")
	err = schedulerDriver.WaitForRunning(ctxs[0], defaultWaitTimeout, defaultWaitInterval)
	log.FailOnError(t, err, "Error waiting for pod to get to running state after stopping driver")

	scheduledNodes, err = schedulerDriver.GetNodesForApp(ctxs[0])
	log.FailOnError(t, err, "Error getting node for app")
	Dash.VerifyFatal(t, 1, len(scheduledNodes), "App should be scheduled on one node")
	Dash.VerifyFatal(t, stoppedNode.Name != scheduledNodes[0].Name, true, "App scheduled on node with driver stopped")

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

func stopDriverTestScale(t *testing.T) {

	log.InfoD("stop driver test")
	var testrailID, testResult = 301327, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	defer updateDashStats(t.Name(), &testResult)

	totalApps := 40
	ctxList := make([]*scheduler.Context, 0)
	for i := 0; i < totalApps; i++ {
		ctxs, err := schedulerDriver.Schedule(generateShortInstanceID(t, "stopdrivertestscale"),
			scheduler.ScheduleOptions{AppKeys: []string{"busybox-1-pvc"}})
		log.FailOnError(t, err, "Error scheduling task")
		Dash.VerifyFatal(t, 1, len(ctxs), "Only one task should have started")
		ctxList = append(ctxList, ctxs[0])
	}
	Dash.VerifyFatal(t, totalApps, len(ctxList), "All apps should have been scheduled")

	for _, ctx := range ctxList {
		err := schedulerDriver.WaitForRunning(ctx, defaultWaitTimeout, defaultWaitInterval)
		log.FailOnError(t, err, "Error waiting for pod to get to running state")
	}

	// Lets get all the scheduled nodes for the apps as a map
	scheduledNodesAppCount := make(map[string]int)
	nodeNametoNodeMap := make(map[string]node.Node)
	for _, ctx := range ctxList {
		scheduledNodes, err := schedulerDriver.GetNodesForApp(ctx)
		log.FailOnError(t, err, "Error getting node for app")
		Dash.VerifyFatal(t, 1, len(scheduledNodes), "App should be scheduled on one node")
		nodeName := scheduledNodes[0].Name
		if _, ok := scheduledNodesAppCount[nodeName]; !ok {
			scheduledNodesAppCount[nodeName] = 0
			nodeNametoNodeMap[nodeName] = scheduledNodes[0]
		}
		scheduledNodesAppCount[nodeName]++
	}

	time.Sleep(1 * time.Minute)

	// Stop the driver and verify that it moved to another node
	var nodeToStopDriver string
	for nodeName, count := range scheduledNodesAppCount {
		if count > podDeleteBatchSize {
			nodeToStopDriver = nodeName
			break
		}
	}

	err := volumeDriver.StopDriver([]node.Node{nodeNametoNodeMap[nodeToStopDriver]}, false, nil)
	log.FailOnError(t, err, "Error stopping driver on scheduled Node %+v", nodeNametoNodeMap[nodeToStopDriver])

	// Add extra timeout as there is a delay in each pod deletion batches
	// Extra time to add is podBatchDeleteIntervalSec * (totalAppsScheduled on that node/podDeleteBatchSize)
	numberOfPodsInOfflineNode := scheduledNodesAppCount[nodeToStopDriver]
	additionalWait := int(math.Ceil(float64(numberOfPodsInOfflineNode)/float64(podDeleteBatchSize))) * podBatchDeleteIntervalSec

	log.InfoD("Waiting for %v seconds for all pods to get evicted from node %s", nodeOfflineTimeout.Seconds()+float64(additionalWait), nodeToStopDriver)

	time.Sleep(nodeOfflineTimeout + time.Duration(additionalWait)*time.Second)

	log.InfoD("Checking if all pods got rescheduled to online driver nodes")
	scheduledNodesAppCountAfter := make(map[string]int)
	for _, ctx := range ctxList {
		scheduledNodes, err := schedulerDriver.GetNodesForApp(ctx)
		log.FailOnError(t, err, "Error getting node for app")
		Dash.VerifyFatal(t, 1, len(scheduledNodes), "App should be scheduled on one node")
		nodeName := scheduledNodes[0].Name
		scheduledNodesAppCountAfter[nodeName]++
	}

	// Check that no app pods are running in offline node and all are evicted
	Dash.VerifyFatal(t, 0, scheduledNodesAppCountAfter[nodeToStopDriver], "App should not be scheduled on node with driver stopped")

	// Start the driver
	err = volumeDriver.StartDriver(nodeNametoNodeMap[nodeToStopDriver])
	log.FailOnError(t, err, "Error starting driver on Node %+v", nodeNametoNodeMap[nodeToStopDriver])

	err = volumeDriver.WaitDriverUpOnNode(nodeNametoNodeMap[nodeToStopDriver], defaultWaitTimeout)
	log.FailOnError(t, err, "Error waiting for Node to start %+v", nodeNametoNodeMap[nodeToStopDriver])

	destroyAndWait(t, ctxList)

	// If we are here then the test has passed
	testResult = testResultPass
	log.InfoD("Test status at end of %s test: %s", t.Name(), testResult)
}

func stopKubeletTest(t *testing.T) {
	log.InfoD("stop kubelet test")
	var testrailID, testResult = 50791, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	defer updateDashStats(t.Name(), &testResult)

	// Cordon node where the test is running. This is so that we don't end up stopping
	// kubelet on the node where the stork-test pod is running
	testPodNode := ""
	testPod, err := core.Instance().GetPodByName("stork-test", "kube-system")
	if err == nil { // if this hits an error, skip below logic to allow running tests outside a pod
		testPodNode = testPod.Spec.NodeName
		err = core.Instance().CordonNode(testPodNode, defaultWaitTimeout, defaultWaitInterval)
		log.FailOnError(t, err, "Error cordorning k8s node for stork test pod")
	}

	defer func() {
		if len(testPodNode) > 0 {
			err = core.Instance().UnCordonNode(testPodNode, defaultWaitTimeout, defaultWaitInterval)
			log.FailOnError(t, err, "Error uncordorning k8s node for stork test pod")
		}
	}()

	ctxs, err := schedulerDriver.Schedule(generateInstanceID(t, "stopkubelettest"),
		scheduler.ScheduleOptions{AppKeys: []string{"mysql-ss"}})
	log.FailOnError(t, err, "Error scheduling task")
	Dash.VerifyFatal(t, 1, len(ctxs), "Only one task should have started")

	err = schedulerDriver.WaitForRunning(ctxs[0], defaultWaitTimeout, defaultWaitInterval)
	log.FailOnError(t, err, "Error waiting for pod to get to running state")

	scheduledNodes, err := schedulerDriver.GetNodesForApp(ctxs[0])
	log.FailOnError(t, err, "Error getting node for app")
	Dash.VerifyFatal(t, 1, len(scheduledNodes), "App should be scheduled on one node")

	scheduledNode := scheduledNodes[0]
	err = schedulerDriver.StopSchedOnNode(scheduledNode)
	log.FailOnError(t, err, fmt.Sprintf("failed to stop scheduler on node: %s", scheduledNode.Name))

	defer func() {
		// restore scheduler
		err = schedulerDriver.StartSchedOnNode(scheduledNode)
		log.FailOnError(t, err, fmt.Sprintf("failed to start scheduler on node: %s", scheduledNode.Name))

	}()

	// wait for the scheduler daemon on node to stop and pod to get into unknown state
	time.Sleep(6 * time.Minute)

	err = schedulerDriver.WaitForRunning(ctxs[0], defaultWaitTimeout, defaultWaitInterval)
	log.FailOnError(t, err, "Error waiting for pod to get to running state")

	destroyAndWait(t, ctxs)

	// If we are here then the test has passed
	testResult = testResultPass
	log.InfoD("Test status at end of %s test: %s", t.Name(), testResult)

}

func poolMaintenanceHealthTest(t *testing.T) {
	var testrailID, testResult = 86081, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	defer updateDashStats(t.Name(), &testResult)

	ctxs, err := schedulerDriver.Schedule(generateInstanceID(t, "pool-health"),
		scheduler.ScheduleOptions{AppKeys: []string{"mysql-1-pvc"}})
	log.FailOnError(t, err, "Error scheduling task")
	Dash.VerifyFatal(t, 1, len(ctxs), "Only one task should have started")

	err = schedulerDriver.WaitForRunning(ctxs[0], defaultWaitTimeout, defaultWaitInterval)
	log.FailOnError(t, err, "Error waiting for pod to get to running state")

	scheduledNodesPre, err := schedulerDriver.GetNodesForApp(ctxs[0])
	log.FailOnError(t, err, "Error getting node for app")
	Dash.VerifyFatal(t, 1, len(scheduledNodesPre), "App should be scheduled on one node")

	err = volumeDriver.EnterPoolMaintenance(scheduledNodesPre[0])
	log.FailOnError(t, err, fmt.Sprintf("Error entering pool maintenance on scheduled Node %+v", scheduledNodesPre[0]))
	poolMaintenanceNode := scheduledNodesPre[0]

	// node timeout bumped to 4 mins from stork 2.9.0
	// ref: https://github.com/libopenstorage/stork/pull/1028
	time.Sleep(5 * time.Minute)

	// The pod should not be deleted from a node which is in pool maintenance state
	err = schedulerDriver.WaitForRunning(ctxs[0], defaultWaitTimeout, defaultWaitInterval)
	log.FailOnError(t, err, "Error waiting for pod to get to running state after deletion")

	scheduledNodesPost, err := schedulerDriver.GetNodesForApp(ctxs[0])
	log.FailOnError(t, err, "Error getting node for app")
	Dash.VerifyFatal(t, 1, len(scheduledNodesPost), "App should be scheduled on one node")
	Dash.VerifyFatal(t, poolMaintenanceNode.Name, scheduledNodesPost[0].Name, "Pod should not restarted on pool maintenance node")

	err = volumeDriver.ExitPoolMaintenance(poolMaintenanceNode)
	log.FailOnError(t, err, fmt.Sprintf("Error exiting pool maintenance on Node %+v", poolMaintenanceNode))

	err = volumeDriver.WaitDriverUpOnNode(poolMaintenanceNode, defaultWaitTimeout)
	log.FailOnError(t, err, fmt.Sprintf("Error waiting for Node to start %+v", poolMaintenanceNode))

	destroyAndWait(t, ctxs)

	// If we are here then the test has passed
	testResult = testResultPass
	log.InfoD("Test status at end of %s test: %s", t.Name(), testResult)
}

func healthCheckFixTest(t *testing.T) {
	var testrailID, testResult = 85900, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	defer updateDashStats(t.Name(), &testResult)

	// When a node's storage is offline stork should not bounce pods right away.
	// It now waits for a minute and checks again to see if the storage driver is still offline.
	// Bringing back node's storage within a minute should not affect anything
	ctxs, err := schedulerDriver.Schedule(generateInstanceID(t, "stopdrivertest"),
		scheduler.ScheduleOptions{AppKeys: []string{"mysql-1-pvc"}})
	log.FailOnError(t, err, "Error scheduling task")
	Dash.VerifyFatal(t, 1, len(ctxs), "Only one task should have started")

	err = schedulerDriver.WaitForRunning(ctxs[0], defaultWaitTimeout, defaultWaitInterval)
	log.FailOnError(t, err, "Error waiting for pod to get to running state")

	// Get uuid for the app
	preUIDList := make(map[string]string)
	postUIDList := make(map[string]string)
	for _, spec := range ctxs[0].App.SpecList {
		if dep, ok := spec.(*appsapi.Deployment); ok {
			depPods, err := apps.Instance().GetDeploymentPods(dep)
			log.FailOnError(t, err, "Error getting pods for deployment ,mysql.")
			for _, pod := range depPods {
				preUIDList[pod.Name] = string(pod.UID)
			}
		}
	}

	scheduledNodes, err := schedulerDriver.GetNodesForApp(ctxs[0])
	log.FailOnError(t, err, "Error getting node for app")
	Dash.VerifyFatal(t, 1, len(scheduledNodes), "App should be scheduled on one node")
	log.InfoD("Step: Completed scheduling app on node: %s", scheduledNodes[0].Name)

	volumeNames := getVolumeNames(t, ctxs[0])
	Dash.VerifyFatal(t, 1, len(volumeNames), "Should have one volume")

	verifyScheduledNode(t, scheduledNodes[0], volumeNames)

	// Stop the driver but bring it back in 30 seconds, verify that it has not moved to another node
	err = volumeDriver.StopDriver(scheduledNodes, false, nil)
	log.FailOnError(t, err, fmt.Sprintf("Error stopping driver on scheduled Node %+v", scheduledNodes[0]))
	stoppedNode := scheduledNodes[0]

	time.Sleep(30 * time.Second)

	// Start the driver
	err = volumeDriver.StartDriver(stoppedNode)
	log.FailOnError(t, err, fmt.Sprintf("Error starting driver on Node %+v", scheduledNodes[0]))

	err = volumeDriver.WaitDriverUpOnNode(stoppedNode, defaultWaitTimeout)
	log.FailOnError(t, err, fmt.Sprintf("Error waiting for Node to start %+v", scheduledNodes[0]))
	log.InfoD("Step: Started volume driver again on node: %s", scheduledNodes[0].Name)

	// Verify that app comes up on the same node
	scheduledNodesPostStop, err := schedulerDriver.GetNodesForApp(ctxs[0])
	log.FailOnError(t, err, "Error getting node for app")
	log.InfoD("Step: App scheduled on node after restart: %s", scheduledNodesPostStop[0].Name)

	Dash.VerifyFatal(t, 1, len(scheduledNodesPostStop), "App should be scheduled on one node")
	Dash.VerifyFatal(t, stoppedNode.Name, scheduledNodesPostStop[0].Name,
		"App scheduled on a different node after volume driver stopped for less than a minute")

	verifyScheduledNode(t, scheduledNodesPostStop[0], volumeNames)

	// verify the app has not restarted after volume driver was stopped, by comparing the start time
	for _, spec := range ctxs[0].App.SpecList {
		if dep, ok := spec.(*appsapi.Deployment); ok {
			depPods, err := apps.Instance().GetDeploymentPods(dep)
			log.FailOnError(t, err, "Error getting pods for deployment ,mysql.")
			for _, pod := range depPods {
				postUIDList[pod.Name] = string(pod.UID)
			}
		}
	}

	Dash.VerifyFatal(t, len(preUIDList), len(postUIDList), "Number of apps pre and post vol driver restart don't match")

	for pod := range preUIDList {
		Dash.VerifyFatal(t, preUIDList[pod], postUIDList[pod], "Uids of apps pre and post vol driver restart don't match")
	}

	destroyAndWait(t, ctxs)

	// If we are here then the test has passed
	testResult = testResultPass
	log.InfoD("Test status at end of %s test: %s", t.Name(), testResult)
}

func stopDriverCsiPodFailoverTest(t *testing.T) {
	var testrailID, testResult = 85901, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	defer updateDashStats(t.Name(), &testResult)

	// Verify CSI pods are running on online nodes
	log.InfoD("Checking if CSI pods are initially scheduled on online PX nodes")
	verifyCsiPodsRunningOnOnlineNode(t)

	// Get all csi pod instances
	csiPods, err := core.Instance().GetPods(storkNamespace, map[string]string{"app": "px-csi-driver"})
	log.FailOnError(t, err, "Failed to get csi pods")

	nodeNameMap := node.GetNodesByName()

	// To be enabled after KubeSchedulerConfiguration fix is integrated in operator
	// Get all nodes where CSI pods are running
	/*isCsiPodNode := make(map[string]bool)
	for _, csiPod := range csiPods.Items {
		isCsiPodNode[csiPod.Spec.NodeName] = true
	}

	Make sure to stop px on all the non csi nodes expect one
	nonCsiNodeAlreadyFound := false
	log.InfoD("Stopping PX on all non CSI pods except one for failover verification")
	for nodeName, schedNode := range nodeNameMap {
		if _, ok := isCsiPodNode[nodeName]; !ok {
			if schedNode.IsStorageDriverInstalled {
				if nonCsiNodeAlreadyFound {
					err = volumeDriver.StopDriver([]node.Node{schedNode}, false, nil)
					require.NoError(t, err, "Error stopping driver on node %+v", nodeNameMap[nodeName])
				} else {
					nonCsiNodeAlreadyFound = true
				}
			}
		}
	}*/

	podToFailover := csiPods.Items[0]
	nodeName := podToFailover.Spec.NodeName

	// Stop px one of of the csi nodes
	log.InfoD("Stopping PX on node = %v where px pod %v is running", nodeName, podToFailover.Name)
	err = volumeDriver.StopDriver([]node.Node{nodeNameMap[nodeName]}, false, nil)
	log.FailOnError(t, err, "Error stopping driver on scheduled Node %+v", nodeNameMap[podToFailover.Spec.NodeName])

	time.Sleep(nodeOfflineTimeout)

	// Verify CSI pods are running on online nodes after failover
	log.InfoD("Checking if all CSI pods are running on online PX nodes after failover")
	verifyCsiPodsRunningOnOnlineNode(t)

	err = volumeDriver.StartDriver(nodeNameMap[nodeName])
	log.FailOnError(t, err, "Error re-starting driver on Node %+v", nodeNameMap[podToFailover.Spec.NodeName])

	// If we are here then the test has passed
	testResult = testResultPass
	log.InfoD("Test status at end of %s test: %s", t.Name(), testResult)
}

func verifyCsiPodsRunningOnOnlineNode(t *testing.T) {
	csiPods, err := core.Instance().GetPods(storkNamespace, map[string]string{"app": "px-csi-driver"})
	log.FailOnError(t, err, "Failed to get csi pods after failover")

	driverNodes, err := storkVolumeDriver.GetNodes()
	log.FailOnError(t, err, "Error getting nodes from stork driver")

	for _, csiPod := range csiPods.Items {
		found := false
		for _, dNode := range driverNodes {
			if csiPod.Spec.NodeName == dNode.Hostname {
				Dash.VerifyFatal(t, dNode.Status, storkdriver.NodeOnline, fmt.Sprintf("CSI pod : %v scheduled on an offline node %v", csiPod.Name, dNode.Hostname))
				found = true
			}
		}
		Dash.VerifyFatal(t, true, found, fmt.Sprintf("CSI node not found in driver node list : %v", driverNodes))
	}
}
