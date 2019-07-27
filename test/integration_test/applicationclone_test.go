// +build integrationtest

package integrationtest

import (
	"testing"

	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/stretchr/testify/require"
)

func testApplicationClone(t *testing.T) {
	t.Run("simpleApplicationCloneTest", simpleApplicationCloneTest)
}

func simpleApplicationCloneTest(t *testing.T) {
	ctx := createApp(t, "app-clone-test")

	cloneAppCtx := ctx.DeepCopy()

	// Clone the app that was created and make sure the application clone task
	// succeeds
	cloneTaskCtx, err := schedulerDriver.Schedule("application-clone",
		scheduler.ScheduleOptions{AppKeys: []string{"application-clone"}})
	require.NoError(t, err, "Error scheduling app clone task")
	require.Equal(t, 1, len(cloneTaskCtx), "Only one task should have started")
	err = schedulerDriver.WaitForRunning(cloneTaskCtx[0], defaultWaitTimeout, defaultWaitInterval)
	require.NoError(t, err, "Error waiting for app clone task to get to running state")

	// Make sure the cloned app is running
	err = schedulerDriver.UpdateTasksID(cloneAppCtx, "app-clone-test-clone")
	require.NoError(t, err, "Error updating task id for app clone context")
	err = schedulerDriver.WaitForRunning(cloneAppCtx, defaultWaitTimeout, defaultWaitInterval)
	require.NoError(t, err, "Error waiting for cloned app to get to running state")

	// Destroy the clone task and cloned add
	destroyAndWait(t, []*scheduler.Context{cloneAppCtx, cloneTaskCtx[0]})

	// Destroy the original app
	err = schedulerDriver.UpdateTasksID(ctx, ctx.GetID())
	require.NoError(t, err, "Error update task id for app context")
	destroyAndWait(t, []*scheduler.Context{ctx})
}
