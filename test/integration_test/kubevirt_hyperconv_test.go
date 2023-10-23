//go:build integrationtest
// +build integrationtest

package integrationtest

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/libopenstorage/openstorage/api"
	"github.com/portworx/sched-ops/k8s/core"
	kubevirt "github.com/portworx/sched-ops/k8s/kubevirt-dynamic"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/portworx/torpedo/drivers/volume"
	"github.com/portworx/torpedo/pkg/log"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	cmdRetry      = 5 * time.Second
	cmdTimeout    = 1 * time.Minute
	mountTypeBind = "bind"
	mountTypeNFS  = "nfs"
)

// This test simulates OCP upgrade by live-migrating VM to a NON-replica node and
// then restarting PX on the node where volume is attached. It expects that the VM
// should end up with a bind-mount.
//
// PX would have performed a single live migration in this test.
func kubeVirtHyperConvOneLiveMigration(t *testing.T) {
	var testrailID, testResult = 93196, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult)
	defer updateTestRail(&testResult, testrailID, runID)
	instanceID := "kubevirt-hyperconv-one-live-migration"
	appKey := "kubevirt-fedora"
	deployedVMName := "test-vm-csi"

	ctxs := kubevirtVMDeployAndValidate(
		t,
		instanceID,
		appKey,
		deployedVMName,
	)

	allNodes := node.GetNodesByVoDriverNodeID()

	for _, appCtx := range ctxs {
		err := schedulerDriver.WaitForRunning(appCtx, defaultWaitTimeout, defaultWaitInterval)
		require.NoError(t, err, "Error waiting for app %s to get to running state", appCtx.App.Key)

		vols, err := schedulerDriver.GetVolumes(appCtx)
		require.NoError(t, err, "failed to get volumes for context %s", appCtx.App.Key)

		// this test expects only one volume in the VM
		require.Equal(t, len(vols), 1)
		vol := vols[0]

		apiVol, err := volumeDriver.InspectVolume(vol.ID)
		require.NoError(t, err, "Failed to inspect PV %s", vol.ID)

		attachedNode, err := volumeDriver.GetNodeForVolume(vol, cmdTimeout, cmdRetry)
		require.NoError(t, err)
		log.Infof("volume %s (%s) is attached to node %s", vol.ID, apiVol.Id, attachedNode.Name)

		// VMs should have a bind-mount initially
		vmPod, err := getVMPod(appCtx, vol)
		require.NoError(t, err)

		verifyVMProperties(t, appCtx, allNodes, vol, apiVol, attachedNode, vmPod,
			false, /* expectAttachedNodeChanged  */
			true,  /* expectBindMount */
			true /* expectReplicaNode */)

		vmiName, vmiUID, vmiPhase, transitionTime, err := getVMIDetails(vmPod)
		require.NoError(t, err)
		require.Equal(t, "Running", vmiPhase)

		// Simulate OCP node upgrade by:
		// 1. Live migrate the VM to another node
		// 2. Restart PX on the original node where the volume should still be attached

		// start a live migration and wait for it to finish
		startAndWaitForVMIMigration(t, vmPod.Namespace, vmiName)

		// VM should be using NFS mount after live migration and should be running on a non-replica node
		// No change to the volume attachment.
		vmPod, err = getVMPod(appCtx, vol)
		require.NoError(t, err)
		verifyVMProperties(t, appCtx, allNodes, vol, apiVol, attachedNode, vmPod,
			false, /* expectAttachedNodeChanged */
			false, /* expectBindMount */
			false /* expectReplicaNode */)

		// restart px on the original node
		err = volumeDriver.RestartDriver(*attachedNode, nil)
		require.NoError(t, err)

		// make sure that the volume attachment has moved
		attachedNodeAfterFailover, err := volumeDriver.GetNodeForVolume(vol, cmdTimeout, cmdRetry)
		require.NoError(t, err)
		require.NotEqual(t, attachedNode.Name, attachedNodeAfterFailover.Name, "attached node did not change after failover")

		// VM should use a bind-mount eventually
		verifyBindMountEventually(t, appCtx, vol, apiVol)

		// Verify that VM stayed up the whole time
		// get VMI, verify that the uid is the same, the phase is running, and phaseTransitionTime is the same
		vmPod, err = getVMPod(appCtx, vol)
		require.NoError(t, err)
		_, vmiUIDAfter, vmiPhaseAfter, transitionTimeAfter, err := getVMIDetails(vmPod)
		require.NoError(t, err)
		require.Equal(t, "Running", vmiPhaseAfter)
		require.Equal(t, vmiUID, vmiUIDAfter)
		require.Equal(t, transitionTime, transitionTimeAfter)
	}

	logrus.Infof("Destroying apps")
	destroyAndWait(t, ctxs)

	// If we are here then the test has passed
	testResult = testResultPass
	logrus.Infof("Test status at end of %s test: %s", t.Name(), testResult)
}

// This test simulates OCP upgrade by live-migrating VM to a *replica* node and
// then restarting PX on the node where volume is attached. It expects that the VM
// should end up with a bind-mount.
//
// PX would have performed two back-to-back live migrations in this test.
func TestKubeVirtHyperConvTwoLiveMigrations(t *testing.T) {
	var testrailID, testResult = 93197, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult)
	defer updateTestRail(&testResult, testrailID, runID)

	// reset mock time before running any tests
	err := setMockTime(nil)
	require.NoError(t, err, "Error resetting mock time")

	logrus.Infof("Using stork volume driver: %s", volumeDriverName)

	err = setSourceKubeConfig()
	require.NoError(t, err, "failed to set kubeconfig to source cluster: %v", err)

	appKeys := []string{"kubevirt"}

	ctxs, err := schedulerDriver.Schedule("kubevirt-hyperconv-two-live-migrations",
		scheduler.ScheduleOptions{
			AppKeys: appKeys,
		})
	require.NoError(t, err, "Error scheduling task")
	require.Equal(t, len(appKeys), len(ctxs))

	allNodes := node.GetNodesByVoDriverNodeID()

	for _, appCtx := range ctxs {
		err = schedulerDriver.WaitForRunning(appCtx, defaultWaitTimeout, defaultWaitInterval)
		require.NoError(t, err, "Error waiting for app %s to get to running state", appCtx.App.Key)

		// TODO: need to merge the torpedo PR #1747 and vendor it into the stork repo
		vols, err := schedulerDriver.GetVolumes(appCtx)
		require.NoError(t, err, "failed to get volumes for context %s", appCtx.App.Key)

		// this test expects only one volume in the VM
		require.Equal(t, len(vols), 1)
		vol := vols[0]

		apiVol, err := volumeDriver.InspectVolume(vol.ID)
		require.NoError(t, err, "Failed to inspect PV %s", vol.ID)

		attachedNode, err := volumeDriver.GetNodeForVolume(vol, cmdTimeout, cmdRetry)
		require.NoError(t, err)
		log.Infof("volume %s (%s) is attached to node %s", vol.ID, apiVol.Id, attachedNode.Name)

		// VMs should have a bind-mount initially
		vmPod, err := getVMPod(appCtx, vol)
		require.NoError(t, err)

		verifyVMProperties(t, appCtx, allNodes, vol, apiVol, attachedNode, vmPod,
			false, /* expectAttachedNodeChanged  */
			true,  /* expectBindMount */
			true /* expectReplicaNode */)

		// get VMI, verify running phase and note down UID, phase last transition time
		vmiName, vmiUID, vmiPhase, transitionTime, err := getVMIDetails(vmPod)
		require.NoError(t, err)
		require.Equal(t, "Running", vmiPhase)

		// Cordon off all non-replica nodes so that the next live migration moves the VM pod to a replica node
		cordonedNodes := cordonNonReplicaNodes(t, apiVol, allNodes)
		defer func() {
			for _, cordonedNode := range cordonedNodes {
				logrus.Infof("Uncordoning node %s", cordonedNode.Name)
				err := core.Instance().UnCordonNode(cordonedNode.Name, defaultWaitTimeout, defaultWaitInterval)
				log.Errorf("Failed to uncordon node %s: %v", cordonedNode.Name, err)
			}
		}()

		// Simulate OCP node upgrade by:
		// 1. Live migrate the VM to another node
		// 2. Restart PX on the original node where the volume should still be attached

		// start a live migration and wait for it to finish
		startAndWaitForVMIMigration(t, vmPod.Namespace, vmiName)

		// VM should be using NFS mount after live migration
		vmPod, err = getVMPod(appCtx, vol)
		require.NoError(t, err)
		verifyVMProperties(t, appCtx, allNodes, vol, apiVol, attachedNode, vmPod,
			false, /* expectAttachedNodeChanged  */
			false, /* expectBindMount */
			true /* expectReplicaNode */)

		for _, cordonedNode := range cordonedNodes {
			logrus.Infof("Uncordoning node %s", cordonedNode.Name)
			err := core.Instance().UnCordonNode(cordonedNode.Name, defaultWaitTimeout, defaultWaitInterval)
			require.NoError(t, err, "Failed to uncordon node %s: %v", cordonedNode.Name, err)
		}
		cordonedNodes = nil

		// restart px on the original node
		err = volumeDriver.RestartDriver(*attachedNode, nil)
		require.NoError(t, err)

		// make sure that the volume attachment has moved
		attachedNodeAfterFailover, err := volumeDriver.GetNodeForVolume(vol, cmdTimeout, cmdRetry)
		require.NoError(t, err)
		require.NotEqual(t, attachedNode.Name, attachedNodeAfterFailover.Name, "attached node did not change after failover")

		// VM should use a bind-mount eventually
		verifyBindMountEventually(t, appCtx, vol, apiVol)

		// Verify that VM stayed up the whole time
		// get VMI, verify that the uid is the same, the phase is running, and phaseTransitionTime is the same
		vmPodAfter, err := getVMPod(appCtx, vol)
		require.NoError(t, err)
		_, vmiUIDAfter, vmiPhaseAfter, transitionTimeAfter, err := getVMIDetails(vmPodAfter)
		require.NoError(t, err)
		require.Equal(t, "Running", vmiPhaseAfter)
		require.Equal(t, vmiUID, vmiUIDAfter)
		require.Equal(t, transitionTime, transitionTimeAfter)
	}

	logrus.Infof("Destroying apps")
	destroyAndWait(t, ctxs)

	// If we are here then the test has passed
	testResult = testResultPass
	logrus.Infof("Test status at end of %s test: %s", t.Name(), testResult)
}

func verifyVMProperties(
	t *testing.T, appCtx *scheduler.Context, allNodes map[string]node.Node, vol *volume.Volume, apiVol *api.Volume,
	previousAttachedNode *node.Node, vmPod *corev1.Pod, expectAttachedNodeChanged, expectBindMount, expectReplicaNode bool,
) {
	// verify attached node
	attachedNode, err := volumeDriver.GetNodeForVolume(vol, cmdTimeout, cmdRetry)
	require.NoError(t, err)
	if expectAttachedNodeChanged {
		require.NotEqual(t, previousAttachedNode.Name, attachedNode.Name, "attached node did not change")
	} else {
		require.Equal(t, previousAttachedNode.Name, attachedNode.Name, "attached node changed")
	}

	// verify replica node
	replicaNodeIDs := getReplicaNodeIDs(apiVol)
	podNamespacedName := fmt.Sprintf("%s/%s", vmPod.Namespace, vmPod.Name)
	var podNodeID string
	for nodeID, node := range allNodes {
		if vmPod.Spec.NodeName == node.Name {
			podNodeID = nodeID
			break
		}
	}
	require.NotEmpty(t, podNodeID, "could not find nodeID for node %s where pod %s is running",
		vmPod.Spec.NodeName, podNamespacedName)
	if expectReplicaNode {
		require.True(t, replicaNodeIDs[podNodeID], "pod is running on node %s (%s) which is not a replica node",
			vmPod.Spec.NodeName, podNodeID)
	} else {
		require.False(t, replicaNodeIDs[podNodeID], "pod is running on node %s (%s) which is a replica node",
			vmPod.Spec.NodeName, podNodeID)
	}

	// verify rootdisk mount type
	mountType, err := getVMRootDiskMountType(vmPod, apiVol)
	require.NoError(t, err)

	if expectBindMount {
		require.Equal(t, mountTypeBind, mountType, "root disk was not bind-mounted")
	} else {
		require.Equal(t, mountTypeNFS, mountType, "root disk was not nfs-mounted")
	}
	logrus.Infof("Verified root disk mount type %q for volume %s (%s)", mountType, vol.ID, apiVol.Id)
}

// getVMIDetails returns VMI name, UID, phase and time when VMI transitioned to that phase.
func getVMIDetails(vmPod *corev1.Pod) (string, string, string, time.Time, error) {
	podNamespacedName := fmt.Sprintf("%s/%s", vmPod.Namespace, vmPod.Name)
	// get VMI name from ownerRef
	var vmiRef *metav1.OwnerReference
	for _, ownerRef := range vmPod.OwnerReferences {
		if ownerRef.Kind == "VirtualMachineInstance" {
			vmiRef = &ownerRef
			break
		}
	}
	if vmiRef == nil {
		return "", "", "", time.Time{}, fmt.Errorf("did not find VMI ownerRef in pod %s", podNamespacedName)
	}
	vmi, err := kubevirt.Instance().GetVirtualMachineInstance(context.TODO(), vmPod.Namespace, vmiRef.Name)
	if err != nil {
		return "", "", "", time.Time{}, fmt.Errorf("failed to get VMI for pod %s", podNamespacedName)
	}

	var transitionTime time.Time
	for _, vmiPhaseTransition := range vmi.PhaseTransitions {
		if vmiPhaseTransition.Phase == vmi.Phase && vmiPhaseTransition.TransitionTime.After(transitionTime) {
			transitionTime = vmiPhaseTransition.TransitionTime
		}
	}
	if transitionTime.IsZero() {
		return "", "", "", time.Time{}, fmt.Errorf(
			"failed to determine when VMI for pod %s transitioned to phase %s", podNamespacedName, vmi.Phase)

	}
	return vmi.Name, vmi.UID, vmi.Phase, transitionTime, nil
}

// Get mount type (nfs or bind) of the root disk volume. This function assumes that the volume name inside
// the pod yaml is "rootdisk". e.g.
//
//	  volumes:
//	  - name: rootdisk
//	    persistentVolumeClaim:
//		  claimName: fedora-communist-toucan
func getVMRootDiskMountType(pod *corev1.Pod, apiVol *api.Volume) (string, error) {
	podNamespacedName := pod.Namespace + "/" + pod.Name
	logrus.Infof("checking the rootdisk mount type in pod %s", podNamespacedName)

	cmd := []string{"mount"}
	output, err := core.Instance().RunCommandInPod(cmd, pod.Name, "", pod.Namespace)
	if err != nil {
		return "", fmt.Errorf("failed to run command %v inside the pod %s", cmd, podNamespacedName)
	}
	var foundBindMount, foundNFSMount bool
	for _, line := range strings.Split(output, "\n") {
		// Sample output if the volume is bind-mounted:
		// $ kubectl exec -it virt-launcher-fedora-communist-toucan-jfw7n -- mount
		// ...
		// /dev/pxd/pxd365793461222635857 on /run/kubevirt-private/vmi-disks/rootdisk type ext4 (rw,relatime,seclabel,discard)
		// ...
		bindMountRE := fmt.Sprintf("/dev/pxd/pxd%s on .*rootdisk type (ext4|xfs)", apiVol.Id)
		if regexp.MustCompile(bindMountRE).MatchString(line) {
			if foundBindMount || foundNFSMount {
				return "", fmt.Errorf("multiple rootdisk mounts found: %s", output)
			}
			foundBindMount = true
			logrus.Infof("found root disk bind mounted for VM pod %s: %s", podNamespacedName, line)
		}

		// Sample output if the volume is nfs-mounted:
		// $ kubectl exec -it virt-launcher-fedora-communist-toucan-bqcrp -- mount
		// ...
		// 172.30.194.11:/var/lib/osd/pxns/365793461222635857 on /run/kubevirt-private/vmi-disks/rootdisk type nfs (...)
		// ...
		nfsMountRE := fmt.Sprintf(":/var/lib/osd/pxns/%s on .*rootdisk type nfs", apiVol.Id)
		if regexp.MustCompile(nfsMountRE).MatchString(line) {
			if foundBindMount || foundNFSMount {
				return "", fmt.Errorf("multiple rootdisk mounts found: %s", output)
			}
			foundNFSMount = true
			logrus.Infof("found root disk nfs mounted for VM pod %s: %s", podNamespacedName, line)
		}
	}
	if !foundBindMount && !foundNFSMount {
		return "", fmt.Errorf("no root disk mount in pod %s: %s", podNamespacedName, output)
	}
	if foundBindMount {
		return mountTypeBind, nil
	}
	return mountTypeNFS, nil
}

func getVMPod(appCtx *scheduler.Context, vol *volume.Volume) (*corev1.Pod, error) {
	pods, err := core.Instance().GetPodsUsingPV(vol.ID)
	if err != nil {
		return nil, fmt.Errorf("failed to get pods for volume %s of context %s: %w", vol.ID, appCtx.App.Key, err)
	}

	var found corev1.Pod
	for _, pod := range pods {
		if pod.Status.Phase == corev1.PodRunning {
			if found.Name != "" {
				// We don't expect this function to be called while live migration is in progress. Therefore,
				// there should be only one VM pod in the running state
				return nil, fmt.Errorf("more than 1 KubeVirt pods (%s, %s) are in running state for volume %s",
					found.Name, pod.Name, vol.ID)
			}
			found = pod
		}
	}
	if found.Name == "" {
		return nil, fmt.Errorf("failed to find a running pod for volume %s", vol.ID)
	}
	return &found, nil
}

func cordonNonReplicaNodes(t *testing.T, vol *api.Volume, allNodes map[string]node.Node) []*node.Node {
	replicaNodeIDs := getReplicaNodeIDs(vol)
	var cordonedNodes []*node.Node

	for nodeID, node := range allNodes {
		if replicaNodeIDs[nodeID] {
			continue
		}
		logrus.Infof("Cordoning non-replica node %s (%s)", node.Name, nodeID)
		err := core.Instance().CordonNode(node.Name, defaultWaitTimeout, defaultWaitInterval)
		require.NoError(t, err, "Failed to cordon node %s (%s)", node.Name, nodeID)
		cordonedNodes = append(cordonedNodes, &node)
	}
	return cordonedNodes
}

func getReplicaNodeIDs(vol *api.Volume) map[string]bool {
	replicaNodes := map[string]bool{}
	for _, replicaSet := range vol.ReplicaSets {
		for _, aNode := range replicaSet.Nodes {
			replicaNodes[aNode] = true
		}
	}
	return replicaNodes
}

func verifyBindMountEventually(t *testing.T, appCtx *scheduler.Context, vol *volume.Volume, apiVol *api.Volume) {
	require.Eventuallyf(t, func() bool {
		pod, err := getVMPod(appCtx, vol)
		if err != nil {
			// this is expected while the live migration is running
			logrus.Infof("Could not get VM pod for vol %s (%s) for context %s: %v",
				vol.ID, apiVol.Id, appCtx.App.Key, err)
		}
		mountType, err := getVMRootDiskMountType(pod, apiVol)
		if err != nil {
			logrus.Warnf("Failed to get mount type of vol %s (%s) for context %s: %v",
				vol.ID, apiVol.Id, appCtx.App.Key, err)
		}
		return mountType == mountTypeBind
	}, 5*time.Minute, 30*time.Second, "VM vol %s (%s) did not switch to a bind-mount", vol.ID, apiVol.Id)
}

func startAndWaitForVMIMigration(t *testing.T, vmiNamespace, vmiName string) {
	ctx := context.TODO()

	// start migration
	migration, err := kubevirt.Instance().CreateVirtualMachineInstanceMigration(ctx, vmiNamespace, vmiName)
	require.NoError(t, err)

	// wait for completion
	var migr *kubevirt.VirtualMachineInstanceMigration
	require.Eventuallyf(t, func() bool {
		migr, err = kubevirt.Instance().GetVirtualMachineInstanceMigration(ctx, vmiNamespace, migration.Name)
		if err != nil {
			logrus.Warnf("Failed to get migration %s/%s: %v", vmiNamespace, migration.Name, err)
			return false
		}
		if !migr.Completed {
			logrus.Warnf("VMI migration %s/%s is still not completed", vmiNamespace, migration.Name)
			return false
		}
		return true
	}, 10*time.Minute, 10*time.Second, "migration for VMI %s/%s is stuck", vmiNamespace, migration.Name)

	// verify that the migration was successful
	require.Equal(t, "Succeeded", migr.Phase)
	require.False(t, migr.Failed)
}
