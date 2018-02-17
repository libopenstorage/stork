// +build integrationtest

package integrationtest

import (
	"testing"
	"time"

	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/stretchr/testify/require"
)

func testExtender(t *testing.T) {
	t.Run("noPVCTest", noPVCTest)
	t.Run("singlePVCTest", singlePVCTest)
	t.Run("multiplePVCTest", multiplePVCTest)
	t.Run("driverNodeErrorTest", driverNodeErrorTest)
}

func noPVCTest(t *testing.T) {
	ctxs, err := schedulerDriver.Schedule(generateInstanceID(t, "nopvctest"),
		scheduler.ScheduleOptions{AppKeys: []string{"mysql-nopvc"}})
	require.NoError(t, err, "Error scheduling task")
	require.Equal(t, 1, len(ctxs), "Only one task should have started")

	err = schedulerDriver.WaitForRunning(ctxs[0])
	require.NoError(t, err, "Error waiting for pod to get to running state")

	destroyAndWait(t, ctxs)
}

func singlePVCTest(t *testing.T) {
	ctxs, err := schedulerDriver.Schedule(generateInstanceID(t, "singlepvctest"),
		scheduler.ScheduleOptions{AppKeys: []string{"mysql-1-pvc"}})
	require.NoError(t, err, "Error scheduling task")
	require.Equal(t, 1, len(ctxs), "Only one task should have started")

	err = schedulerDriver.WaitForRunning(ctxs[0])
	require.NoError(t, err, "Error waiting for pod to get to running state")

	scheduledNodes, err := schedulerDriver.GetNodesForApp(ctxs[0])
	require.NoError(t, err, "Error getting node for app")
	require.Equal(t, 1, len(scheduledNodes), "App should be scheduled on one node")

	volumeNames := getVolumeNames(t, ctxs[0])
	require.Equal(t, 1, len(volumeNames), "Should only have one volume")

	verifyScheduledNode(t, scheduledNodes[0], volumeNames)

	destroyAndWait(t, ctxs)
}

func multiplePVCTest(t *testing.T) {
	ctxs, err := schedulerDriver.Schedule(generateInstanceID(t, "multipvctest"),
		scheduler.ScheduleOptions{AppKeys: []string{"mysql-2-pvc"}})
	require.NoError(t, err, "Error scheduling task")
	require.Equal(t, 1, len(ctxs), "Only one task should have started")

	err = schedulerDriver.WaitForRunning(ctxs[0])
	require.NoError(t, err, "Error waiting for pod to get to running state")

	scheduledNodes, err := schedulerDriver.GetNodesForApp(ctxs[0])
	require.NoError(t, err, "Error getting node for app")
	require.Equal(t, 1, len(scheduledNodes), "App should be scheduled on one node")

	volumeNames := getVolumeNames(t, ctxs[0])
	require.Equal(t, 2, len(volumeNames), "Should have two volumes")

	verifyScheduledNode(t, scheduledNodes[0], volumeNames)
	destroyAndWait(t, ctxs)
}

func driverNodeErrorTest(t *testing.T) {
	ctxs, err := schedulerDriver.Schedule(generateInstanceID(t, "drivererrtest"),
		scheduler.ScheduleOptions{AppKeys: []string{"mysql-1-pvc"}})
	require.NoError(t, err, "Error scheduling task")
	require.Equal(t, 1, len(ctxs), "Only one task should have started")

	err = schedulerDriver.WaitForRunning(ctxs[0])
	require.NoError(t, err, "Error waiting for pod to get to running state")

	scheduledNodes, err := schedulerDriver.GetNodesForApp(ctxs[0])
	require.NoError(t, err, "Error getting node for app")
	require.Equal(t, 1, len(scheduledNodes), "App should be scheduled on one node")

	volumeNames := getVolumeNames(t, ctxs[0])
	require.Equal(t, 1, len(volumeNames), "Should have only one volume")

	verifyScheduledNode(t, scheduledNodes[0], volumeNames)

	time.Sleep(1 * time.Minute)

	err = volumeDriver.StopDriver(scheduledNodes[0])
	require.NoError(t, err, "Error stopping driver on scheduled Node %+v", scheduledNodes[0])
	stoppedNode := scheduledNodes[0]

	time.Sleep(1 * time.Minute)
	err = schedulerDriver.DeleteTasks(ctxs[0])
	require.NoError(t, err, "Error deleting pod")
	time.Sleep(10 * time.Second)

	err = schedulerDriver.WaitForRunning(ctxs[0])
	require.NoError(t, err, "Error waiting for pod to get to running state after deletion")

	scheduledNodes, err = schedulerDriver.GetNodesForApp(ctxs[0])
	require.NoError(t, err, "Error getting node for app")
	require.Equal(t, 1, len(scheduledNodes), "App should be scheduled on one node")
	require.NotEqual(t, stoppedNode.Name, scheduledNodes[0].Name, "Task restarted on stopped node")

	volumeNames = getVolumeNames(t, ctxs[0])
	require.Equal(t, 1, len(volumeNames), "Should have only one volume")

	verifyScheduledNode(t, scheduledNodes[0], volumeNames)

	err = volumeDriver.StartDriver(stoppedNode)
	require.NoError(t, err, "Error starting driver on Node %+v", scheduledNodes[0])

	err = volumeDriver.WaitDriverUpOnNode(stoppedNode)
	require.NoError(t, err, "Error waiting for Node to start %+v", scheduledNodes[0])

	destroyAndWait(t, ctxs)
}
