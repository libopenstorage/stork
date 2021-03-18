// +build integrationtest

package integrationtest

import (
	"testing"

	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/stretchr/testify/require"
)

func TestApplicationClone(t *testing.T) {
	err := setSourceKubeConfig()
	require.NoError(t, err, "failed to set kubeconfig to source cluster: %v", err)

	t.Run("deploymentTest", deploymentApplicationCloneTest)
	t.Run("statefulsetTest", statefulsetApplicationCloneTest)
	t.Run("statefulsetRuleTest", statefulsetApplicationCloneRuleTest)
	t.Run("preExecRuleMissingTest", applicationCloneRulePreExecMissingTest)
	t.Run("postExecRuleMissingTest", applicationCloneRulePostExecMissingTest)
	t.Run("disallowedNamespaceTest", applicationCloneDisallowedNamespaceTest)
	t.Run("failingPreExecRuleTest", applicationCloneFailingPreExecRuleTest)
	t.Run("failingPostExecRuleTest", applicationCloneFailingPostExecRuleTest)
	t.Run("labelSelectorTest", applicationCloneLabelSelectorTest)

	err = setRemoteConfig("")
	require.NoError(t, err, "setting kubeconfig to default failed")
}

func triggerApplicationCloneTest(
	t *testing.T,
	instanceID string,
	appKey string,
	additionalAppKeys []string,
	cloneAppKey string,
	cloneSuccessExpected bool,
	cloneAllAppsExpected bool,
	scaleDownAppAfterClone bool,
) {
	var scaleMap map[string]int32
	ctxs, err := schedulerDriver.Schedule(instanceID,
		scheduler.ScheduleOptions{AppKeys: []string{appKey}})
	require.NoError(t, err, "Error scheduling task")
	require.Equal(t, 1, len(ctxs), "Only one task should have started")

	err = schedulerDriver.WaitForRunning(ctxs[0], defaultWaitTimeout, defaultWaitInterval)
	require.NoError(t, err, "Error waiting for app to get to running state")

	cloneAppCtx := ctxs[0].DeepCopy()

	if len(additionalAppKeys) > 0 {
		err = schedulerDriver.AddTasks(ctxs[0],
			scheduler.ScheduleOptions{AppKeys: additionalAppKeys})
		require.NoError(t, err, "Error scheduling additional apps")
		err = schedulerDriver.WaitForRunning(ctxs[0], defaultWaitTimeout, defaultWaitInterval)
		require.NoError(t, err, "Error waiting for additional apps to get to running state")
	}

	if cloneAllAppsExpected {
		cloneAppCtx = ctxs[0].DeepCopy()
	}

	// Clone the app that was created and make sure the application clone task
	// succeeds
	cloneTaskCtx, err := schedulerDriver.Schedule("application-clone",
		scheduler.ScheduleOptions{AppKeys: []string{cloneAppKey}})
	require.NoError(t, err, "Error scheduling app clone task")
	require.Equal(t, 1, len(cloneTaskCtx), "Only one task should have started")
	timeout := defaultWaitTimeout
	if !cloneSuccessExpected {
		timeout = timeout / 4
	}

	err = schedulerDriver.WaitForRunning(cloneTaskCtx[0], timeout, defaultWaitInterval)
	if cloneSuccessExpected {
		require.NoError(t, err, "Error waiting for app clone task to get to running state")
		if scaleDownAppAfterClone {
			// After app has been cloned scale it down to 0, else cloned apps may start running
			scaleMap, err = schedulerDriver.GetScaleFactorMap(cloneAppCtx)
			require.NoError(t, err, "Error getting scale map")
			reducedScaleMap := make(map[string]int32, len(scaleMap))
			for name := range scaleMap {
				reducedScaleMap[name] = 0
			}
			err = schedulerDriver.ScaleApplication(cloneAppCtx, reducedScaleMap)
			require.NoError(t, err, "Error getting scaling down app")
		}

		// Make sure the cloned app is running
		err = schedulerDriver.UpdateTasksID(cloneAppCtx, appKey+"-"+instanceID+"-dest")
		require.NoError(t, err, "Error updating task id for app clone context")
		err = schedulerDriver.WaitForRunning(cloneAppCtx, defaultWaitTimeout, defaultWaitInterval)
		require.NoError(t, err, "Error waiting for cloned app to get to running state")

		// Scale up the original app
		if scaleDownAppAfterClone {
			err = schedulerDriver.ScaleApplication(cloneAppCtx, scaleMap)
			require.NoError(t, err, "Error getting scaling up app %v", cloneAppCtx)
			err = schedulerDriver.WaitForRunning(cloneAppCtx, defaultWaitTimeout, defaultWaitInterval)
			require.NoError(t, err, "Error waiting for app to get to running state")
		}

		// Destroy the clone task and cloned app
		require.NoError(t, err, "Error updating task id for app clone context")
		destroyAndWait(t, []*scheduler.Context{cloneAppCtx, cloneTaskCtx[0]})
	} else {
		require.Error(t, err, "Expected app clone task to fail")
		// Destroy the clone task
		destroyAndWait(t, cloneTaskCtx)
	}

	// Destroy the original app
	err = schedulerDriver.UpdateTasksID(ctxs[0], ctxs[0].GetID())
	require.NoError(t, err, "Error update task id for app context")
	destroyAndWait(t, ctxs)
}

func deploymentApplicationCloneTest(t *testing.T) {
	triggerApplicationCloneTest(
		t,
		"mysql-clone",
		"mysql-1-pvc",
		nil,
		"mysql-clone",
		true,
		true,
		false,
	)
}

func statefulsetApplicationCloneTest(t *testing.T) {
	triggerApplicationCloneTest(
		t,
		"cassandra-clone",
		"cassandra",
		nil,
		"cassandra-clone",
		true,
		true,
		true,
	)
}

func statefulsetApplicationCloneRuleTest(t *testing.T) {
	triggerApplicationCloneTest(
		t,
		"cassandra-clone-rule",
		"cassandra",
		nil,
		"cassandra-clone-rule",
		true,
		true,
		true,
	)
}

func applicationCloneRulePreExecMissingTest(t *testing.T) {
	triggerApplicationCloneTest(
		t,
		"applicationclone-pre-exec-missing",
		"mysql-1-pvc",
		nil,
		"mysql-clone-pre-exec-missing",
		false,
		true,
		false,
	)
}

func applicationCloneRulePostExecMissingTest(t *testing.T) {
	triggerApplicationCloneTest(
		t,
		"applicationclone-post-exec-missing",
		"mysql-1-pvc",
		nil,
		"mysql-clone-post-exec-missing",
		false,
		true,
		false,
	)
}

func applicationCloneDisallowedNamespaceTest(t *testing.T) {
	triggerApplicationCloneTest(
		t,
		"applicationclone-disallowed-namespace",
		"mysql-1-pvc",
		nil,
		"mysql-clone-disallowed-ns",
		false,
		true,
		false,
	)
}

func applicationCloneFailingPreExecRuleTest(t *testing.T) {
	triggerApplicationCloneTest(
		t,
		"applicationclone-failing-pre-exec-rule",
		"mysql-1-pvc",
		nil,
		"mysql-clone-failing-pre-exec",
		false,
		true,
		false,
	)
}

func applicationCloneFailingPostExecRuleTest(t *testing.T) {
	triggerApplicationCloneTest(
		t,
		"applicationclone-failing-post-exec-rule",
		"mysql-1-pvc",
		nil,
		"mysql-clone-failing-post-exec",
		false,
		true,
		false,
	)
}

func applicationCloneLabelSelectorTest(t *testing.T) {
	triggerApplicationCloneTest(
		t,
		"applicationclone-label-selector-test",
		"cassandra",
		[]string{"mysql-1-pvc"},
		"label-selector-applicationclone",
		true,
		false,
		false,
	)
}
