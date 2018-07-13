// +build integrationtest

package integrationtest

import (
	"fmt"
	"testing"

	"github.com/libopenstorage/stork/pkg/cmdexecutor"
	k8s_ops "github.com/portworx/sched-ops/k8s"
	"github.com/portworx/torpedo/drivers/scheduler"
	_ "github.com/portworx/torpedo/drivers/scheduler/k8s"
	"github.com/skyrings/skyring-common/tools/uuid"
	"github.com/stretchr/testify/require"
	apps_api "k8s.io/api/apps/v1beta2"
	"k8s.io/api/core/v1"
)

func asyncPodCommandTest(t *testing.T) {
	id, err := uuid.New()
	require.NoError(t, err, "failed to get uuid")

	ctxs, err := schedulerDriver.Schedule(id.String(), scheduler.ScheduleOptions{AppKeys: []string{"mysql-no-persistence"}})
	require.NoError(t, err, "Error scheduling task")
	require.Equal(t, 1, len(ctxs), "Only one task should have started")

	passCommands := []string{
		`uname -a && ${WAIT_CMD};`,
		`mysql --user=root --password=password -Bse 'flush tables with read lock;system ${WAIT_CMD};'`,
	}

	noWaitPlaceholderCmd := `uname -a;` // WAIT_CMD missing
	failCommands := []string{
		`no-such-command && ${WAIT_CMD};`,                                                                 // run a non-existing command
		`mysql --user=root --password=badpassword -Bse 'flush tables with read lock;system ${WAIT_CMD};'`, // give incorrect mysql password
	}

	for _, ctx := range ctxs {
		err = schedulerDriver.WaitForRunning(ctx)
		require.NoError(t, err, "Error waiting for pod to get to running state")

		pods, err := getContextPods(ctx)
		require.NoError(t, err, "failed to get pods for context")
		require.NotEmpty(t, pods, "got empty pods for context")

		// Positive test cases
		for _, testCmd := range passCommands {
			executors := startCommandInPods(t, testCmd, pods)

			for _, executor := range executors {
				err = executor.Wait(120)
				ns, name := executor.GetPod()
				require.NoError(t, err, fmt.Sprintf("failed to wait for command on pod: [%s] %s", ns, name))
			}
		}

		// Negative test cases
		for _, pod := range pods {
			executor := cmdexecutor.Init(pod.GetNamespace(), pod.GetName(), "", noWaitPlaceholderCmd, string(pod.GetUID()))
			errChan := make(chan error)
			err = executor.Start(errChan)
			require.Error(t, err, "expected error from the start command API")
		}

		for _, testCmd := range failCommands {
			executors := startCommandInPods(t, testCmd, pods)

			for _, executor := range executors {
				err = executor.Wait(10)
				ns, name := executor.GetPod()
				require.Error(t, err, fmt.Sprintf("expected error since command: %s should fail on pod: [%s] %s",
					executor.GetCommand(), ns, name))
			}
		}
	}

	destroyAndWait(t, ctxs)
}

func startCommandInPods(t *testing.T, command string, pods []v1.Pod) []cmdexecutor.Executor {
	executors := make([]cmdexecutor.Executor, 0)
	for _, pod := range pods {
		executor := cmdexecutor.Init(pod.GetNamespace(), pod.GetName(), "", command, string(pod.GetUID()))
		errChan := make(chan error)
		err := executor.Start(errChan)
		require.NoError(t, err, "failed to start async command")
		executors = append(executors, executor)
	}

	return executors
}

func getContextPods(ctx *scheduler.Context) ([]v1.Pod, error) {
	k8sOps := k8s_ops.Instance()
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
