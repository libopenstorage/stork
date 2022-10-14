//go:build integrationtest
// +build integrationtest

package integrationtest

import (
	"regexp"
	"strings"
	"testing"

	"github.com/portworx/sched-ops/k8s/core"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
)

// This test is named starting with "TestExtender" so that is runs as part of the TestExtender suite
func TestExtenderWebhookStatfs(t *testing.T) {
	// reset mock time before running any tests
	err := setMockTime(nil)
	require.NoError(t, err, "Error resetting mock time")

	logrus.Infof("Using stork volume driver: %s", volumeDriverName)

	err = setSourceKubeConfig()
	require.NoError(t, err, "failed to set kubeconfig to source cluster: %v", err)

	appKeys := []string{"virt-launcher-sim", "virt-launcher-sim-enc", "test-sv4-svc", "test-sv4-svc-enc"}

	ctxs, err := schedulerDriver.Schedule("webhook-statfs-test",
		scheduler.ScheduleOptions{
			AppKeys: appKeys,
		})
	require.NoError(t, err, "Error scheduling task")
	require.Equal(t, len(appKeys), len(ctxs))

	numWorkers := int32(len(node.GetWorkerNodes()))

	for _, appCtx := range ctxs {
		scaleMap, err := schedulerDriver.GetScaleFactorMap(appCtx)
		require.NoError(t, err, "Error getting scale map")
		newScaleMap := make(map[string]int32, len(scaleMap))
		for name := range scaleMap {
			newScaleMap[name] = numWorkers
		}
		err = schedulerDriver.ScaleApplication(appCtx, newScaleMap)
		require.NoError(t, err, "Error when scaling app to %d", numWorkers)
	}

	for _, appCtx := range ctxs {
		err = schedulerDriver.WaitForRunning(appCtx, defaultWaitTimeout, defaultWaitInterval)
		require.NoError(t, err, "Error waiting for app to get to running state")
	}

	for _, appCtx := range ctxs {
		validateStatfs(t, appCtx)
	}

	logrus.Infof("Destroying apps")
	destroyAndWait(t, ctxs)
}

func validateStatfs(t *testing.T, ctx *scheduler.Context) {
	// mount paths inside the app container
	sharedMountPath := "/shared-vol"
	localMountPath := "/local-vol"

	vols, err := schedulerDriver.GetVolumes(ctx)
	require.NoError(t, err, "failed to get volumes for context %s", ctx.App.Key)

	require.Equal(t, len(vols), 1)
	vol := vols[0]

	pods, err := core.Instance().GetPodsUsingPV(vol.ID)
	require.NoError(t, err, "failed to get pods for volume %v of context %s", vol.ID, ctx.App.Key)

	foundBindMount := false
	foundNFSMount := false
	for _, pod := range pods {
		logrus.Infof("validating statfs in pod %s in namespace %v", pod.Name, pod.Namespace)

		// Sharedv4 PX volume is mounted on path /sv4test. Check if it is nfs-mounted or bind-mounted.
		//
		// Bind mount sample output:
		//
		// $ kubectl exec -it virt-launcher-sim-dep-8476fffd8b-ds4l5 -c sv4test -- df -T /sv4test
		// Filesystem                     Type 1K-blocks  Used Available Use% Mounted on
		// /dev/pxd/pxd585819943023766088 ext4  51343840 54556  48651460   1% /sv4test
		//
		//
		// NFS mount sample output:
		//
		// $ kubetl exec -it virt-launcher-sim-dep-8476fffd8b-f7vbk -c sv4test -- df -T /sv4test
		// Filesystem                                          Type 1K-blocks  Used Available Use% Mounted on
		// 192.168.121.49:/var/lib/osd/pxns/585819943023766088 nfs   51344384 54272  48652288   1% /sv4test

		output := runCommandInSv4TestContainer(t, &pod, []string{"df", "-T", sharedMountPath})
		isBindMount := regexp.MustCompile(`pxd.*ext4`).MatchString(output)
		if !isBindMount {
			// must be an NFS mount
			require.True(t, regexp.MustCompile(`pxns.*nfs`).MatchString(output))
		}
		logrus.Infof("isBindMount=%v", isBindMount)

		// check statfs() on the PX volume
		output = runCommandInSv4TestContainer(t, &pod, []string{"stat", "--format=%T", "-f", sharedMountPath})
		output = strings.TrimSpace(output)
		logrus.Infof("statfs(%v)=%v", sharedMountPath, output)

		if isVirtLauncherContext(ctx) {
			// should always be "nfs"
			require.Equal(t, output, "nfs",
				"statfs() did not return 'nfs' for virt launcher pod %s/%s", pod.Namespace, pod.Name)
		} else if isBindMount {
			require.NotEqual(t, output, "nfs",
				"statfs() returned 'nfs' for bind-mounted pod %s/%s", pod.Namespace, pod.Name)
		} else {
			require.Equal(t, output, "nfs",
				"statfs() did not return 'nfs' for nfs-mounted pod %s/%s", pod.Namespace, pod.Name)
		}

		// Path /local-vol is an {emptydir} volume so it should never return nfs for the file system type
		output = runCommandInSv4TestContainer(t, &pod, []string{"stat", "--format=%T", "-f", localMountPath})
		output = strings.TrimSpace(output)
		logrus.Infof("statfs(%v)=%v", localMountPath, output)
		require.NotEqual(t, output, "nfs",
			"statfs() returned 'nfs' for local volume in pod %s/%s", pod.Namespace, pod.Name)

		if isBindMount {
			foundBindMount = true
		} else {
			foundNFSMount = true
		}
	}

	// Sanity check: since we run 1 pod on each of the worker nodes, we expect to find
	// 1 bind-mounted pod and at least 1 nfs-mounted pod.
	require.True(t, foundBindMount, "bind-mounted pod not found for context %s", ctx.App.Key)
	require.True(t, foundNFSMount, "nfs-mounted pod not found for context %s", ctx.App.Key)
	logrus.Infof("validated statfs for context %v", ctx.App.Key)
}

func runCommandInSv4TestContainer(t *testing.T, pod *corev1.Pod, cmd []string) string {
	container := "sv4test"
	output, err := core.Instance().RunCommandInPod(cmd, pod.Name, container, pod.Namespace)
	require.NoError(t, err,
		"failed to run command %v inside the pod %v/%v", cmd, pod.Namespace, pod.Name)
	return output
}

func isVirtLauncherContext(ctx *scheduler.Context) bool {
	return strings.HasPrefix(ctx.App.Key, "virt-launcher-sim")
}
