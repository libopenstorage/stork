//go:build integrationtest
// +build integrationtest

package integrationtest

import (
	"bytes"
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/libopenstorage/stork/pkg/log"
	"github.com/portworx/sched-ops/k8s/core"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/scheduler"
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
	log.FailOnError(t, err, "Error resetting mock time")
	currentTestSuite = t.Name()
	defer updateDashStats(t.Name(), &testResult)

	log.InfoD("Using stork volume driver: %s", volumeDriverName)

	err = setSourceKubeConfig()
	log.FailOnError(t, err, "failed to set kubeconfig to source cluster: %v", err)

	appKeys := []string{"virt-launcher-sim", "virt-launcher-sim-enc", "test-sv4-svc", "test-sv4-svc-enc"}

	ctxs, err := schedulerDriver.Schedule("webhook-statfs-test",
		scheduler.ScheduleOptions{
			AppKeys: appKeys,
		})
	log.FailOnError(t, err, "Error scheduling task")
	Dash.VerifyFatal(t, len(appKeys), len(ctxs), "Number of apps scheduled is equal to number of app keys")

	numWorkers := int32(len(node.GetWorkerNodes()))

	for _, appCtx := range ctxs {
		scaleMap, err := schedulerDriver.GetScaleFactorMap(appCtx)
		log.FailOnError(t, err, "Error getting scale map")
		newScaleMap := make(map[string]int32, len(scaleMap))
		for name := range scaleMap {
			newScaleMap[name] = numWorkers
		}
		err = schedulerDriver.ScaleApplication(appCtx, newScaleMap)
		log.FailOnError(t, err, "Error when scaling app to %d", numWorkers)
	}

	for _, appCtx := range ctxs {
		err = schedulerDriver.WaitForRunning(appCtx, defaultWaitTimeout, defaultWaitInterval)
		log.FailOnError(t, err, "Error waiting for app to get to running state")
	}

	foundVirtLauncher := false
	for _, appCtx := range ctxs {
		validateStatfs(t, appCtx)

		if !foundVirtLauncher && isVirtLauncherContext(appCtx) {
			foundVirtLauncher = true
			validateConfigMapUpdate(t, appCtx)
		}
	}
	Dash.VerifyFatal(t, foundVirtLauncher, true, "Virt launcher context not found")

	log.InfoD("Destroying apps")
	destroyAndWait(t, ctxs)
}

func validateStatfs(t *testing.T, ctx *scheduler.Context) {
	// mount paths inside the app container
	sharedMountPath := "/shared-vol"
	localMountPath := "/local-vol"

	vols, err := schedulerDriver.GetVolumes(ctx)
	log.FailOnError(t, err, "failed to get volumes for context %s", ctx.App.Key)

	Dash.VerifyFatal(t, len(vols), 1, "Number of volumes is 1")
	vol := vols[0]

	pods, err := core.Instance().GetPodsUsingPV(vol.ID)
	log.FailOnError(t, err, "failed to get pods for volume %v of context %s", vol.ID, ctx.App.Key)

	foundBindMount := false
	foundNFSMount := false
	for _, pod := range pods {
		log.InfoD("validating statfs in pod %s in namespace %v", pod.Name, pod.Namespace)

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
			Dash.VerifyFatal(t, regexp.MustCompile(`pxns.*nfs`).MatchString(output), true, fmt.Sprintf("statfs() did not return 'nfs' for pod %s/%s", pod.Namespace, pod.Name))
		}
		log.InfoD("isBindMount=%v", isBindMount)

		// check statfs() on the PX volume
		output = runCommandInSv4TestContainer(t, &pod, []string{"stat", "--format=%T", "-f", sharedMountPath})
		output = strings.TrimSpace(output)
		log.InfoD("statfs(%v)=%v", sharedMountPath, output)

		if isVirtLauncherContext(ctx) {
			// should always be "nfs"
			Dash.VerifyFatal(t, output, "nfs",
				fmt.Sprintf("statfs() did not return 'nfs' for virt launcher pod %s/%s", pod.Namespace, pod.Name))
		} else if isBindMount {
			Dash.VerifyFatal(t, output != "nfs", true,
				fmt.Sprintf("statfs() returned 'nfs' for bind-mounted pod %s/%s", pod.Namespace, pod.Name))
		} else {
			Dash.VerifyFatal(t, output, "nfs",
				fmt.Sprintf("statfs() did not return 'nfs' for nfs-mounted pod %s/%s", pod.Namespace, pod.Name))
		}

		// Path /local-vol is an {emptydir} volume so it should never return nfs for the file system type
		output = runCommandInSv4TestContainer(t, &pod, []string{"stat", "--format=%T", "-f", localMountPath})
		output = strings.TrimSpace(output)
		log.InfoD("statfs(%v)=%v", localMountPath, output)
		Dash.VerifyFatal(t, output != "nfs", true,
			fmt.Sprintf("statfs() returned 'nfs' for local volume in pod %s/%s", pod.Namespace, pod.Name))

		if isBindMount {
			foundBindMount = true
		} else {
			foundNFSMount = true
		}
	}

	// Sanity check: since we run 1 pod on each of the worker nodes, we expect to find
	// 1 bind-mounted pod and at least 1 nfs-mounted pod.
	Dash.VerifyFatal(t, foundBindMount, true, fmt.Sprintf("bind-mounted pod not found for context %s", ctx.App.Key))
	Dash.VerifyFatal(t, foundNFSMount, true, fmt.Sprintf("nfs-mounted pod not found for context %s", ctx.App.Key))
	log.InfoD("validated statfs for context %v", ctx.App.Key)
}

func runCommandInSv4TestContainer(t *testing.T, pod *corev1.Pod, cmd []string) string {
	container := "sv4test"
	output, err := core.Instance().RunCommandInPod(cmd, pod.Name, container, pod.Namespace)
	log.FailOnError(t, err,
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
	Dash.VerifyFatal(t, isVirtLauncherContext(ctx), true, "context is not for virt-launcher-sim pod")
	vols, err := schedulerDriver.GetVolumes(ctx)
	log.FailOnError(t, err, "Failed to get volumes for context %s", ctx.App.Key)

	Dash.VerifyFatal(t, len(vols), 1, fmt.Sprintf("expected 1 volume for context %s", ctx.App.Key))
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
		log.InfoD("Testing configmap update function %d", i)
		pods, err := core.Instance().GetPodsUsingPV(vol.ID)
		log.FailOnError(t, err, "Failed to get pods for volume %v of context %s", vol.ID, ctx.App.Key)

		Dash.VerifyFatal(t, len(pods) > 0, true, fmt.Sprintf("no pods found for volume %v of context %s", vol.ID, ctx.App.Key))

		// pick a pod that is not terminating already
		var pod corev1.Pod
		for _, pod = range pods {
			if pod.DeletionTimestamp.IsZero() {
				log.InfoD("Using pod %s/%s for the configMap update test", pod.Namespace, pod.Name)
				break
			}
		}

		log.InfoD("Changing configMap in namespace %s", pod.Namespace)
		cm, err := core.Instance().GetConfigMap(statfsConfigMapName, pod.Namespace)
		log.FailOnError(t, err, fmt.Sprintf("Failed to get configMap %s in namespace %s", statfsConfigMapName, pod.Namespace))

		// get the original golden values
		origPreload, exists := cm.Data[cmDataPreloadKey]
		Dash.VerifyFatal(t, exists, true, "ld.so.preload key not found in configMap")

		origSum, exists := cm.Data[cmDataSumKey]
		Dash.VerifyFatal(t, exists, true, "checksum key not found in configMap")

		origSOFileContents, exists := cm.BinaryData[cmBinaryDataFileKey]
		Dash.VerifyFatal(t, exists, true, "binary file key not found in configMap")

		// change something in the configmap and verify that the configMap gets restored to the original golden values,
		// when a new pod starts up
		updateFn(cm)
		_, err = core.Instance().UpdateConfigMap(cm)
		log.FailOnError(t, err, fmt.Sprintf("Failed to update configMap %s in namespace %s", statfsConfigMapName, pod.Namespace))

		// verify that our changes went through
		Dash.VerifyFatal(t, configMapMatches(pod.Namespace, origPreload, origSum, origSOFileContents), false, "configMap did not get updated")

		// Delete a pod. When a new pod starts up, Stork webhook should detect the mismatch and update the configMap.
		err = core.Instance().DeletePod(pod.Name, pod.Namespace, false)
		log.FailOnError(t, err, fmt.Sprintf("Failed to delete pod %s in namespace %s", pod.Name, pod.Namespace))

		require.Eventuallyf(t, func() bool {
			log.InfoD("checking if the ConfigMap matches")
			return configMapMatches(pod.Namespace, origPreload, origSum, origSOFileContents)
		}, 3*time.Minute, 3*time.Second, "ConfigMap %s/%s did not get updated", pod.Namespace, statfsConfigMapName)
	}
	log.InfoD("Validated configMap update in context %v", ctx.App.Key)
}

func configMapMatches(namespace, expectedPreload string, expectedSum string, expectedSOFileContents []byte) bool {
	cm, err := core.Instance().GetConfigMap(statfsConfigMapName, namespace)
	if err != nil {
		log.InfoD("Failed to get configmap %s/%s: %v", namespace, statfsConfigMapName, err)
		return false
	}

	actualPreload := cm.Data[cmDataPreloadKey]
	if actualPreload != expectedPreload {
		log.InfoD("ConfigMap.Data[%s] does not match: expected %s, actual %s",
			cmDataPreloadKey, expectedPreload, actualPreload)
		return false
	}

	actualSum := cm.Data[cmDataSumKey]
	if actualSum != expectedSum {
		log.InfoD("ConfigMap.Data[%s] does not match: expected %s, actual %s", cmDataSumKey, expectedSum, actualSum)
		return false
	}

	// .so file contents should match
	if !bytes.Equal(expectedSOFileContents, cm.BinaryData[cmBinaryDataFileKey]) {
		log.InfoD("ConfigMap.BinaryData[%s] does not match: expected %v, actual %v",
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
	log.InfoD("Updating byte at index %d from %d to %d", updateIndex, origByte, newByte)
	updatedBytes[updateIndex] = newByte
	return updatedBytes
}
