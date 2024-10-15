//go:build integrationtest
// +build integrationtest

package integrationtest

import (
	"testing"

	"github.com/libopenstorage/stork/pkg/log"
	"github.com/pure-px/torpedo/drivers/scheduler"
)

func TestApplicationClone(t *testing.T) {
	err := setSourceKubeConfig()
	log.FailOnError(t, err, "failed to set kubeconfig to source cluster: %v", err)
	currentTestSuite = t.Name()

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
	log.FailOnError(t, err, "setting kubeconfig to default failed")
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
	log.FailOnError(t, err, "Error scheduling task")
	Dash.VerifyFatal(t, 1, len(ctxs), "Only one task should have started")

	err = schedulerDriver.WaitForRunning(ctxs[0], defaultWaitTimeout, defaultWaitInterval)
	log.FailOnError(t, err, "Error waiting for app to get to running state")

	cloneAppCtx := ctxs[0].DeepCopy()

	if len(additionalAppKeys) > 0 {
		err = schedulerDriver.AddTasks(ctxs[0],
			scheduler.ScheduleOptions{AppKeys: additionalAppKeys})
		log.FailOnError(t, err, "Error scheduling additional apps")
		err = schedulerDriver.WaitForRunning(ctxs[0], defaultWaitTimeout, defaultWaitInterval)
		log.FailOnError(t, err, "Error waiting for additional apps to get to running state")
	}

	if cloneAllAppsExpected {
		cloneAppCtx = ctxs[0].DeepCopy()
	}

	// Clone the app that was created and make sure the application clone task
	// succeeds
	cloneTaskCtx, err := schedulerDriver.Schedule("application-clone",
		scheduler.ScheduleOptions{AppKeys: []string{cloneAppKey}})
	log.FailOnError(t, err, "Error scheduling app clone task")
	Dash.VerifyFatal(t, len(cloneTaskCtx), 1, "Only one task should have started")
	timeout := defaultWaitTimeout
	if !cloneSuccessExpected {
		timeout = timeout / 4
	}

	err = schedulerDriver.WaitForRunning(cloneTaskCtx[0], timeout, defaultWaitInterval)
	if cloneSuccessExpected {
		log.FailOnError(t, err, "Error waiting for app clone task to get to running state")
		if scaleDownAppAfterClone {
			// After app has been cloned scale it down to 0, else cloned apps may start running
			scaleMap, err = schedulerDriver.GetScaleFactorMap(cloneAppCtx)
			log.FailOnError(t, err, "Error getting scale map")
			reducedScaleMap := make(map[string]int32, len(scaleMap))
			for name := range scaleMap {
				reducedScaleMap[name] = 0
			}
			err = schedulerDriver.ScaleApplication(cloneAppCtx, reducedScaleMap)
			log.FailOnError(t, err, "Error getting scaling down app")
		}

		// Make sure the cloned app is running
		err = schedulerDriver.UpdateTasksID(cloneAppCtx, appKey+"-"+instanceID+"-dest")
		log.FailOnError(t, err, "Error updating task id for app clone context")
		err = schedulerDriver.WaitForRunning(cloneAppCtx, defaultWaitTimeout, defaultWaitInterval)
		log.FailOnError(t, err, "Error waiting for cloned app to get to running state")

		// Scale up the original app
		if scaleDownAppAfterClone {
			err = schedulerDriver.ScaleApplication(cloneAppCtx, scaleMap)
			log.FailOnError(t, err, "Error getting scaling up app %v", cloneAppCtx)
			err = schedulerDriver.WaitForRunning(cloneAppCtx, defaultWaitTimeout, defaultWaitInterval)
			log.FailOnError(t, err, "Error waiting for app to get to running state")
		}

		// Destroy the clone task and cloned app
		log.FailOnError(t, err, "Error updating task id for app clone context")
		destroyAndWait(t, []*scheduler.Context{cloneAppCtx, cloneTaskCtx[0]})
	} else {
		log.FailOnNoError(t, err, "Expected app clone task to fail")
		// Destroy the clone task
		destroyAndWait(t, cloneTaskCtx)
	}

	// Destroy the original app
	err = schedulerDriver.UpdateTasksID(ctxs[0], ctxs[0].GetID())
	log.FailOnError(t, err, "Error update task id for app context")
	destroyAndWait(t, ctxs)
}

func deploymentApplicationCloneTest(t *testing.T) {
	var testrailID, testResult = 50840, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	defer updateDashStats(t.Name(), &testResult)

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

	// If we are here then the test has passed
	testResult = testResultPass
	log.InfoD("Test status at end of %s test: %s", t.Name(), testResult)
}

func statefulsetApplicationCloneTest(t *testing.T) {
	var testrailID, testResult = 50841, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	defer updateDashStats(t.Name(), &testResult)

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

	// If we are here then the test has passed
	testResult = testResultPass
	log.InfoD("Test status at end of %s test: %s", t.Name(), testResult)
}

func statefulsetApplicationCloneRuleTest(t *testing.T) {
	var testrailID, testResult = 50842, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	defer updateDashStats(t.Name(), &testResult)

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

	// If we are here then the test has passed
	testResult = testResultPass
	log.InfoD("Test status at end of %s test: %s", t.Name(), testResult)
}

func applicationCloneRulePreExecMissingTest(t *testing.T) {
	var testrailID, testResult = 50843, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	defer updateDashStats(t.Name(), &testResult)

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

	// If we are here then the test has passed
	testResult = testResultPass
	log.InfoD("Test status at end of %s test: %s", t.Name(), testResult)
}

func applicationCloneRulePostExecMissingTest(t *testing.T) {
	var testrailID, testResult = 50844, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	defer updateDashStats(t.Name(), &testResult)

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

	// If we are here then the test has passed
	testResult = testResultPass
	log.InfoD("Test status at end of %s test: %s", t.Name(), testResult)
}

func applicationCloneDisallowedNamespaceTest(t *testing.T) {
	var testrailID, testResult = 50845, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	defer updateDashStats(t.Name(), &testResult)

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

	// If we are here then the test has passed
	testResult = testResultPass
	log.InfoD("Test status at end of %s test: %s", t.Name(), testResult)
}

func applicationCloneFailingPreExecRuleTest(t *testing.T) {
	var testrailID, testResult = 50846, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	defer updateDashStats(t.Name(), &testResult)

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

	// If we are here then the test has passed
	testResult = testResultPass
	log.InfoD("Test status at end of %s test: %s", t.Name(), testResult)
}

func applicationCloneFailingPostExecRuleTest(t *testing.T) {
	var testrailID, testResult = 50847, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	defer updateDashStats(t.Name(), &testResult)

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

	// If we are here then the test has passed
	testResult = testResultPass
	log.InfoD("Test status at end of %s test: %s", t.Name(), testResult)
}

func applicationCloneLabelSelectorTest(t *testing.T) {
	var testrailID, testResult = 50848, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	defer updateDashStats(t.Name(), &testResult)

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

	// If we are here then the test has passed
	testResult = testResultPass
	log.InfoD("Test status at end of %s test: %s", t.Name(), testResult)
}
