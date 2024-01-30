//go:build integrationtest
// +build integrationtest

package integrationtest

import (
	"bytes"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/portworx/sched-ops/k8s/core"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
)

const (
	statfsConfigMapName = "px-statfs"
	cmBinaryDataFileKey = "px_statfs.so"
	cmDataPreloadKey    = "ld.so.preload"
	cmDataSumKey        = "px_statfs.so.sha256"
)

// This test is named starting with "TestExtender" so that is runs as part of the TestExtender suite
func TestExtenderWebhookStatfs(t *testing.T) {
	var testResult = testResultFail
	// reset mock time before running any tests
	err := setMockTime(nil)
	require.NoError(t, err, "Error resetting mock time")
	currentTestSuite = t.Name()
	defer updateDashStats(t.Name(), &testResult)

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

	foundVirtLauncher := false
	for _, appCtx := range ctxs {
		validateStatfs(t, appCtx)

		if !foundVirtLauncher && isVirtLauncherContext(appCtx) {
			foundVirtLauncher = true
			validateConfigMapUpdate(t, appCtx)
		}
	}
	require.True(t, foundVirtLauncher)

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

// This test verifies that the webhook updates the px-statfs configMap, as neeed.
// Test steps:
//
// - Start with a "good" px-statfs configmap that the webhook has already created.
// - Make some dummy changes to the px-statfs configmap
// - Trigger the webhook by deleting one of the virt-launcher-sim pod (new pod should start)
// - Wait until the webhook has restored the original configMap
func validateConfigMapUpdate(t *testing.T, ctx *scheduler.Context) {
	require.True(t, isVirtLauncherContext(ctx))
	vols, err := schedulerDriver.GetVolumes(ctx)
	require.NoError(t, err, "Failed to get volumes for context %s", ctx.App.Key)

	require.Equal(t, len(vols), 1)
	vol := vols[0]

	testCases := []func(cm *corev1.ConfigMap){
		func(cm *corev1.ConfigMap) {
			// TestCase: value of "ld.so.preload" key in data is missing px-statfs.so
			// - change the contents of ld.so.preload file in the configmap
			cm.Data[cmDataPreloadKey] = "dummy-value"
		},
		func(cm *corev1.ConfigMap) {
			// TestCase: checksum mismatch
			cm.Data[cmDataSumKey] = "dummy-value"
			cm.BinaryData[cmBinaryDataFileKey] = updateBytesForTesting(cm.BinaryData[cmBinaryDataFileKey])
		},
		func(cm *corev1.ConfigMap) {
			// TestCase: no checksum
			delete(cm.Data, cmDataSumKey)
			cm.BinaryData[cmBinaryDataFileKey] = updateBytesForTesting(cm.BinaryData[cmBinaryDataFileKey])
		},
	}
	for i, updateFn := range testCases {
		logrus.Infof("Testing configmap update function %d", i)
		pods, err := core.Instance().GetPodsUsingPV(vol.ID)
		require.NoError(t, err, "Failed to get pods for volume %v of context %s", vol.ID, ctx.App.Key)

		require.True(t, len(pods) > 0)

		// pick a pod that is not terminating already
		var pod corev1.Pod
		for _, pod = range pods {
			if pod.DeletionTimestamp.IsZero() {
				logrus.Infof("Using pod %s/%s for the configMap update test", pod.Namespace, pod.Name)
				break
			}
		}

		logrus.Infof("Changing configMap in namespace %s", pod.Namespace)
		cm, err := core.Instance().GetConfigMap(statfsConfigMapName, pod.Namespace)
		require.NoError(t, err)

		// get the original golden values
		origPreload, exists := cm.Data[cmDataPreloadKey]
		require.True(t, exists)

		origSum, exists := cm.Data[cmDataSumKey]
		require.True(t, exists)

		origSOFileContents, exists := cm.BinaryData[cmBinaryDataFileKey]
		require.True(t, exists)

		// change something in the configmap and verify that the configMap gets restored to the original golden values,
		// when a new pod starts up
		updateFn(cm)
		_, err = core.Instance().UpdateConfigMap(cm)
		require.NoError(t, err)

		// verify that our changes went through
		require.False(t, configMapMatches(pod.Namespace, origPreload, origSum, origSOFileContents))

		// Delete a pod. When a new pod starts up, Stork webhook should detect the mismatch and update the configMap.
		err = core.Instance().DeletePod(pod.Name, pod.Namespace, false)
		require.NoError(t, err)

		require.Eventuallyf(t, func() bool {
			logrus.Infof("checking if the ConfigMap matches")
			return configMapMatches(pod.Namespace, origPreload, origSum, origSOFileContents)
		}, 3*time.Minute, 3*time.Second, "ConfigMap %s/%s did not get updated", pod.Namespace, statfsConfigMapName)
	}
	logrus.Infof("Validated configMap update in context %v", ctx.App.Key)
}

func configMapMatches(namespace, expectedPreload string, expectedSum string, expectedSOFileContents []byte) bool {
	cm, err := core.Instance().GetConfigMap(statfsConfigMapName, namespace)
	if err != nil {
		logrus.Infof("Failed to get configmap %s/%s: %v", namespace, statfsConfigMapName, err)
		return false
	}

	actualPreload := cm.Data[cmDataPreloadKey]
	if actualPreload != expectedPreload {
		logrus.Infof("ConfigMap.Data[%s] does not match: expected %s, actual %s",
			cmDataPreloadKey, expectedPreload, actualPreload)
		return false
	}

	actualSum := cm.Data[cmDataSumKey]
	if actualSum != expectedSum {
		logrus.Infof("ConfigMap.Data[%s] does not match: expected %s, actual %s", cmDataSumKey, expectedSum, actualSum)
		return false
	}

	// .so file contents should match
	if !bytes.Equal(expectedSOFileContents, cm.BinaryData[cmBinaryDataFileKey]) {
		logrus.Infof("ConfigMap.BinaryData[%s] does not match: expected %v, actual %v",
			cmBinaryDataFileKey, expectedSOFileContents, cm.BinaryData[cmBinaryDataFileKey])
		return false
	}
	return true
}

// Returns a new []byte with 1-byte difference from the original one
func updateBytesForTesting(origBytes []byte) []byte {
	updatedBytes := append([]byte(nil), origBytes...)
	updateIndex := len(updatedBytes) / 2
	origByte := updatedBytes[updateIndex]
	newByte := origByte ^ 1
	logrus.Infof("Updating byte at index %d from %d to %d", updateIndex, origByte, newByte)
	updatedBytes[updateIndex] = newByte
	return updatedBytes
}
