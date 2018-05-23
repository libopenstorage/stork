package main

import (
	"fmt"
	"os"
	"testing"

	k8s_ops "github.com/portworx/sched-ops/k8s"
	"github.com/portworx/torpedo/drivers/scheduler"
	_ "github.com/portworx/torpedo/drivers/scheduler/k8s"
	"github.com/sirupsen/logrus"
	"github.com/skyrings/skyring-common/tools/uuid"
	"github.com/stretchr/testify/require"
	apps_api "k8s.io/api/apps/v1beta2"
	"k8s.io/api/core/v1"
)

const (
	schedulerDriverName = "k8s"
	volumeDriverName    = "pxd"
	nodeDriverName      = "ssh"
)

var schedulerDriver scheduler.Driver

func setup(t *testing.T) {
	var err error
	schedulerDriver, err = scheduler.Get(schedulerDriverName)
	require.NoError(t, err, "Error getting scheduler driver %v", schedulerDriverName)

	dir, err := os.Getwd()
	require.NoError(t, err, "failed to get current working dir")

	specDir := dir + "/specs"
	err = schedulerDriver.Init(specDir, volumeDriverName, nodeDriverName)
	require.NoError(t, err, "Error initializing scheduler driver %v with spec dir: %s", schedulerDriverName, specDir)
}

func TestAsyncPodCommand(t *testing.T) {
	kubeconfig := os.Getenv("KUBECONFIG")
	if len(kubeconfig) == 0 {
		t.Skip("KUBECONFIG not defined. Skipping pod async command test")
	}

	if passed := t.Run("setup", setup); !passed {
		t.FailNow()
	}

	logrus.Infof("Running async pod test using kubeconfig: %s", kubeconfig)

	id, _ := uuid.New()
	ctxs, err := schedulerDriver.Schedule(id.String(), scheduler.ScheduleOptions{AppKeys: []string{"mysql-basic"}})
	require.NoError(t, err, "Error scheduling task")
	require.Equal(t, 1, len(ctxs), "Only one task should have started")

	passCommands := []string{
		`uname -a && ${WAIT_CMD}`,
		`mysql --user=root --password=password -Bse 'flush tables with read lock;system ${WAIT_CMD}'`,
	}

	noWaitPlaceholderCmd := `uname -a;` // WAIT_CMD missing
	failCommands := []string{
		`no-such-command && ${WAIT_CMD}`,                                                                 // run a non-existing command
		`mysql --user=root --password=badpassword -Bse 'flush tables with read lock;system ${WAIT_CMD}'`, // give incorrect mysql password
	}

	for _, ctx := range ctxs {
		err = schedulerDriver.WaitForRunning(ctx)
		require.NoError(t, err, "Error waiting for pod to get to running state")

		pods, err := getContextPods(ctx)
		require.NoError(t, err, "failed to get pods for context")
		require.NotEmpty(t, pods, "got empty pods for context")

		// Positive test cases
		for _, testCmd := range passCommands {
			statusFileMap := startCommandInPods(t, testCmd, pods)

			for _, pod := range pods {
				statusFile := statusFileMap[pod.GetSelfLink()]
				err = checkFileExistsInPod(pod.GetNamespace(), pod.GetName(), "", statusFile, 120)
				require.NoError(t, err, fmt.Sprintf("failed to check status file: %s on pod", statusFile))
			}
		}

		// Negative test cases
		for _, pod := range pods {
			statusFile, err := startAsyncPodCommand(pod.GetNamespace(), pod.GetName(), "", noWaitPlaceholderCmd)
			require.Error(t, err, "expected error from the start command API")
			require.Empty(t, statusFile, "expected empty statusFile from start comamnd API")
		}

		for _, testCmd := range failCommands {
			statusFileMap := startCommandInPods(t, testCmd, pods)

			for _, pod := range pods {
				statusFile := statusFileMap[pod.GetSelfLink()]
				err = checkFileExistsInPod(pod.GetNamespace(), pod.GetName(), "", statusFile, 10)
				require.Error(t, err, fmt.Sprintf("expected error since status file: %s would not exist on pod", statusFile))
			}
		}
	}

	destroyAndWait(t, ctxs)
}

func startCommandInPods(t *testing.T, command string, pods []v1.Pod) map[string]string {
	statusFileMap := make(map[string]string, 0)
	for _, pod := range pods {
		statusFile, err := startAsyncPodCommand(pod.GetNamespace(), pod.GetName(), "", command)
		require.NoError(t, err, "failed to start async command")
		require.NotEmpty(t, statusFile, "got empty status file")
		statusFileMap[pod.GetSelfLink()] = statusFile
	}

	return statusFileMap
}

func destroyAndWait(t *testing.T, ctxs []*scheduler.Context) {
	for _, ctx := range ctxs {
		err := schedulerDriver.Destroy(ctx, nil)
		require.NoError(t, err, "Error destroying ctx: %+v", ctx)
		err = schedulerDriver.WaitForDestroy(ctx)
		require.NoError(t, err, "Error waiting for destroy of ctx: %+v", ctx)
	}
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
