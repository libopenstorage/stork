//go:build integrationtest
// +build integrationtest

package integrationtest

import (
	"fmt"
	"testing"
	"time"

	"github.com/libopenstorage/stork/pkg/cmdexecutor"
	"github.com/libopenstorage/stork/pkg/log"
	"github.com/portworx/sched-ops/k8s/apps"
	"github.com/portworx/torpedo/drivers/scheduler"
	_ "github.com/portworx/torpedo/drivers/scheduler/k8s"
	"github.com/skyrings/skyring-common/tools/uuid"
	apps_api "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
)

func TestCommandExecutor(t *testing.T) {
	err := setSourceKubeConfig()
	log.FailOnError(t, err, "failed to set kubeconfig to source cluster: %v", err)
	currentTestSuite = t.Name()

	t.Run("cmdExecutorTest", cmdExecutorTest)

	err = setRemoteConfig("")
	log.FailOnError(t, err, "setting kubeconfig to default failed")
}

func cmdExecutorTest(t *testing.T) {
	var testrailID, testResult = 86261, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	defer updateDashStats(t.Name(), &testResult)

	err := setSourceKubeConfig()
	log.FailOnError(t, err, "failed to set kubeconfig to source cluster: %v", err)

	id, err := uuid.New()
	log.FailOnError(t, err, "failed to get uuid")

	ctxs, err := schedulerDriver.Schedule(id.String(), scheduler.ScheduleOptions{AppKeys: []string{"mysql-no-persistence"}})
	log.FailOnError(t, err, "Error scheduling task")
	Dash.VerifyFatal(t, len(ctxs), 1, "Only one task should have started")

	passCommands := []string{
		`uname -a && ${WAIT_CMD};`,
		`mysql --user=root --password=password -Bse 'flush tables with read lock;system ${WAIT_CMD};'`,
	}

	noWaitPlaceholderCmd := `uname -a;` // WAIT_CMD missing
	failCommands := []string{
		`no-such-command && ${WAIT_CMD};`, // run a non-existing command
		`mysql --user=root --password=badpassword -Bse 'flush tables with read lock;system ${WAIT_CMD};'`, // give incorrect mysql password
	}

	for _, ctx := range ctxs {
		err = schedulerDriver.WaitForRunning(ctx, defaultWaitTimeout, defaultWaitInterval)
		log.FailOnError(t, err, "Error waiting for pod to get to running state")

		pods, err := getContextPods(ctx)
		log.FailOnError(t, err, "failed to get pods for context")
		Dash.VerifyFatal(t, pods != nil, true, "Empty pods for context")

		// Positive test cases
		for _, testCmd := range passCommands {
			executors := startCommandInPods(t, testCmd, pods)

			for _, executor := range executors {
				err = executor.Wait(120 * time.Second)
				ns, name := executor.GetPod()
				log.FailOnError(t, err, fmt.Sprintf("failed to wait for command on pod: [%s] %s", ns, name))
			}
		}

		// Negative test cases
		for _, pod := range pods {
			executor := cmdexecutor.Init(pod.GetNamespace(), pod.GetName(), "", noWaitPlaceholderCmd, string(pod.GetUID()))
			errChan := make(chan error)
			err = executor.Start(errChan)
			log.FailOnNoError(t, err, "expected error from the start command API")
		}

		for _, testCmd := range failCommands {
			executors := startCommandInPods(t, testCmd, pods)

			for _, executor := range executors {
				err = executor.Wait(10 * time.Second)
				ns, name := executor.GetPod()
				log.FailOnNoError(t, err, fmt.Sprintf("expected error since command: %s should fail on pod: [%s] %s",
					executor.GetCommand(), ns, name))
			}
		}
	}

	destroyAndWait(t, ctxs)

	err = setRemoteConfig("")
	log.FailOnError(t, err, "setting kubeconfig to default failed")

	// If we are here then the test has passed
	testResult = testResultPass
	log.InfoD("Test status at end of %s test: %s", t.Name(), testResult)
}

func startCommandInPods(t *testing.T, command string, pods []v1.Pod) []cmdexecutor.Executor {
	executors := make([]cmdexecutor.Executor, 0)
	for _, pod := range pods {
		executor := cmdexecutor.Init(pod.GetNamespace(), pod.GetName(), "", command, string(pod.GetUID()))
		errChan := make(chan error)
		err := executor.Start(errChan)
		log.FailOnError(t, err, "failed to start async command")
		executors = append(executors, executor)
	}

	return executors
}

func getContextPods(ctx *scheduler.Context) ([]v1.Pod, error) {
	k8sOps := apps.Instance()
	var pods []v1.Pod

	for _, spec := range ctx.App.SpecList {
		if obj, ok := spec.(*apps_api.Deployment); ok {
			depPods, err := k8sOps.GetDeploymentPods(obj)
			if err != nil {
				return nil, err
			}
			pods = append(pods, depPods...)
		} else if obj, ok := spec.(*apps_api.StatefulSet); ok {
			ssPods, err := k8sOps.GetStatefulSetPods(obj)
			if err != nil {
				return nil, err
			}
			pods = append(pods, ssPods...)
		}
	}

	return pods, nil
}
