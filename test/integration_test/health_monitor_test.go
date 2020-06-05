// +build integrationtest

package integrationtest

import (
	"fmt"
	"testing"
	"time"

	"github.com/portworx/sched-ops/k8s/apps"
	"github.com/portworx/sched-ops/k8s/core"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	appsapi "k8s.io/api/apps/v1"
)

func TestHealthMonitor(t *testing.T) {
	t.Run("stopDriverTest", stopDriverTest)
	t.Run("stopKubeletTest", stopKubeletTest)
	t.Run("healthCheckFixTest", healthCheckFixTest)
}

func stopDriverTest(t *testing.T) {
	ctxs, err := schedulerDriver.Schedule(generateInstanceID(t, "stopdrivertest"),
		scheduler.ScheduleOptions{AppKeys: []string{"mysql-1-pvc"}})
	require.NoError(t, err, "Error scheduling task")
	require.Equal(t, 1, len(ctxs), "Only one task should have started")

	err = schedulerDriver.WaitForRunning(ctxs[0], defaultWaitTimeout, defaultWaitInterval)
	require.NoError(t, err, "Error waiting for pod to get to running state")

	scheduledNodes, err := schedulerDriver.GetNodesForApp(ctxs[0])
	require.NoError(t, err, "Error getting node for app")
	require.Equal(t, 1, len(scheduledNodes), "App should be scheduled on one node")

	volumeNames := getVolumeNames(t, ctxs[0])
	require.Equal(t, 1, len(volumeNames), "Should have one volume")

	verifyScheduledNode(t, scheduledNodes[0], volumeNames)

	time.Sleep(1 * time.Minute)

	// Stop the driver and after 3 minutes verify that it moved to another node
	// where the volume is located
	err = volumeDriver.StopDriver(scheduledNodes, false, nil)
	require.NoError(t, err, "Error stopping driver on scheduled Node %+v", scheduledNodes[0])
	stoppedNode := scheduledNodes[0]

	time.Sleep(3 * time.Minute)

	err = schedulerDriver.WaitForRunning(ctxs[0], defaultWaitTimeout, defaultWaitInterval)
	require.NoError(t, err, "Error waiting for pod to get to running state after stopping driver")

	scheduledNodes, err = schedulerDriver.GetNodesForApp(ctxs[0])
	require.NoError(t, err, "Error getting node for app")
	require.Equal(t, 1, len(scheduledNodes), "App should be scheduled on one node")
	require.NotEqual(t, stoppedNode.Name, scheduledNodes[0].Name,
		"App scheduled on node with driver stopped")

	verifyScheduledNode(t, scheduledNodes[0], volumeNames)

	err = volumeDriver.StartDriver(stoppedNode)
	require.NoError(t, err, "Error starting driver on Node %+v", scheduledNodes[0])

	err = volumeDriver.WaitDriverUpOnNode(stoppedNode, defaultWaitTimeout)
	require.NoError(t, err, "Error waiting for Node to start %+v", scheduledNodes[0])

	destroyAndWait(t, ctxs)
}

func stopKubeletTest(t *testing.T) {
	// Cordon node where the test is running. This is so that we don't end up stopping
	// kubelet on the node where the stork-test pod is running
	testPodNode := ""
	testPod, err := core.Instance().GetPodByName("stork-test", "kube-system")
	if err == nil { // if this hits an error, skip below logic to allow running tests outside a pod
		testPodNode = testPod.Spec.NodeName
		err = core.Instance().CordonNode(testPodNode, defaultWaitTimeout, defaultWaitInterval)
		require.NoError(t, err, "Error cordorning k8s node for stork test pod")
	}

	defer func() {
		if len(testPodNode) > 0 {
			err = core.Instance().UnCordonNode(testPodNode, defaultWaitTimeout, defaultWaitInterval)
			require.NoError(t, err, "Error uncordorning k8s node for stork test pod")
		}
	}()

	ctxs, err := schedulerDriver.Schedule(generateInstanceID(t, "stopkubelettest"),
		scheduler.ScheduleOptions{AppKeys: []string{"mysql-ss"}})
	require.NoError(t, err, "Error scheduling task")
	require.Equal(t, 1, len(ctxs), "Only one task should have started")

	err = schedulerDriver.WaitForRunning(ctxs[0], defaultWaitTimeout, defaultWaitInterval)
	require.NoError(t, err, "Error waiting for pod to get to running state")

	scheduledNodes, err := schedulerDriver.GetNodesForApp(ctxs[0])
	require.NoError(t, err, "Error getting node for app")
	require.Equal(t, 1, len(scheduledNodes), "App should be scheduled on one node")

	scheduledNode := scheduledNodes[0]
	err = schedulerDriver.StopSchedOnNode(scheduledNode)
	require.NoError(t, err, fmt.Sprintf("failed to stop scheduler on node: %s", scheduledNode.Name))

	defer func() {
		// restore scheduler
		err = schedulerDriver.StartSchedOnNode(scheduledNode)
		require.NoError(t, err, fmt.Sprintf("failed to start scheduler on node: %s", scheduledNode.Name))

	}()

	// wait for the scheduler daemon on node to stop and pod to get into unknown state
	time.Sleep(6 * time.Minute)

	err = schedulerDriver.WaitForRunning(ctxs[0], defaultWaitTimeout, defaultWaitInterval)
	require.NoError(t, err, "Error waiting for pod to get to running state")

	destroyAndWait(t, ctxs)

}

func healthCheckFixTest(t *testing.T) {
	// When a node's storage is offline stork should not bounce pods right away.
	// It now waits for a minute and checks again to see if the storage driver is still offline.
	// Bringing back node's storage within a minute should not affect anything
	ctxs, err := schedulerDriver.Schedule(generateInstanceID(t, "stopdrivertest"),
		scheduler.ScheduleOptions{AppKeys: []string{"mysql-1-pvc"}})
	require.NoError(t, err, "Error scheduling task")
	require.Equal(t, 1, len(ctxs), "Only one task should have started")

	err = schedulerDriver.WaitForRunning(ctxs[0], defaultWaitTimeout, defaultWaitInterval)
	require.NoError(t, err, "Error waiting for pod to get to running state")

	// Get uuid for the app
	preUIDList := make(map[string]string)
	postUIDList := make(map[string]string)
	for _, spec := range ctxs[0].App.SpecList {
		if dep, ok := spec.(*appsapi.Deployment); ok {
			depPods, err := apps.Instance().GetDeploymentPods(dep)
			require.NoError(t, err, "Error getting pods for deployment ,mysql.")
			for _, pod := range depPods {
				preUIDList[pod.Name] = string(pod.UID)
			}
		}
	}

	scheduledNodes, err := schedulerDriver.GetNodesForApp(ctxs[0])
	require.NoError(t, err, "Error getting node for app")
	require.Equal(t, 1, len(scheduledNodes), "App should be scheduled on one node")
	logrus.Infof("Step: Completed scheduling app on node: %s", scheduledNodes[0].Name)

	volumeNames := getVolumeNames(t, ctxs[0])
	require.Equal(t, 1, len(volumeNames), "Should have one volume")

	verifyScheduledNode(t, scheduledNodes[0], volumeNames)

	// Stop the driver but bring it back in 30 seconds, verify that it has not moved to another node
	err = volumeDriver.StopDriver(scheduledNodes, false, nil)
	require.NoError(t, err, "Error stopping driver on scheduled Node %+v", scheduledNodes[0])
	stoppedNode := scheduledNodes[0]

	time.Sleep(30 * time.Second)

	// Start the driver
	err = volumeDriver.StartDriver(stoppedNode)
	require.NoError(t, err, "Error starting driver on Node %+v", scheduledNodes[0])

	err = volumeDriver.WaitDriverUpOnNode(stoppedNode, defaultWaitTimeout)
	require.NoError(t, err, "Error waiting for Node to start %+v", scheduledNodes[0])
	logrus.Infof("Step: Started volume driver again on node: %s", scheduledNodes[0].Name)

	// Verify that app comes up on the same node
	scheduledNodesPostStop, err := schedulerDriver.GetNodesForApp(ctxs[0])
	require.NoError(t, err, "Error getting node for app")
	logrus.Infof("Step: App scheduled on node after restart: %s", scheduledNodesPostStop[0].Name)

	require.Equal(t, 1, len(scheduledNodesPostStop), "App should be scheduled on one node")
	require.Equal(t, stoppedNode.Name, scheduledNodesPostStop[0].Name,
		"App scheduled on a different node after volume driver stopped for less than a minute")

	verifyScheduledNode(t, scheduledNodesPostStop[0], volumeNames)

	// verify the app has not restarted after volume driver was stopped, by comparing the start time
	for _, spec := range ctxs[0].App.SpecList {
		if dep, ok := spec.(*appsapi.Deployment); ok {
			depPods, err := apps.Instance().GetDeploymentPods(dep)
			require.NoError(t, err, "Error getting pods for deployment ,mysql.")
			for _, pod := range depPods {
				postUIDList[pod.Name] = string(pod.UID)
			}
		}
	}

	require.Equal(t, len(preUIDList), len(postUIDList), "Number of apps pre and post vol driver restart don't match")

	for pod := range preUIDList {
		require.Equal(t, preUIDList[pod], postUIDList[pod], "Uids of apps pre and post vol driver restart don't match")
	}

	destroyAndWait(t, ctxs)
}
