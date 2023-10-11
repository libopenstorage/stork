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

type kubevirtTestState struct {
	appCtx                 *scheduler.Context
	allNodes               map[string]node.Node
	volume                 *volume.Volume
	apiVol                 *api.Volume
	attachedNode           *node.Node
	vmiName                string
	vmiUID                 string
	vmiPhase               string
	vmiPhaseTransitionTime time.Time
	vmPod                  *corev1.Pod
}

// This test simulates OCP upgrade by live-migrating VM to a NON-replica node and
// then restarting PX on the node where volume is attached. It expects that the VM
// should end up with a bind-mount (hyperconvergence). PX performs
// a single live migration in this test.
func kubeVirtHypercOneLiveMigration(t *testing.T) {
	var testrailID, testResult = 93196, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult)
	defer updateTestRail(&testResult, testrailID, runID)
	instanceID := "vm-one-lm"
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
		testState := &kubevirtTestState{
			appCtx:   appCtx,
			allNodes: allNodes,
		}
		verifyInitialVMI(t, testState)

		// Simulate OCP node upgrade by:
		// 1. Live migrate the VM to another node
		// 2. Restart PX on the original node where the volume should still be attached

		// start a live migration and wait for it to finish
		// vmPod changes after the live migration
		startAndWaitForVMIMigration(t, testState, false /* expectReplicaNode */)

		// restart px on the original node and make sure that the volume attachment has moved
		restartVolumeDriverAndWaitForAttachmentToMove(t, testState)

		// VM should use a bind-mount eventually
		log.Infof("Waiting for the VM to return to the hyperconverged state again")
		verifyBindMountEventually(t, testState)
	}
	logrus.Infof("Destroying apps")
	destroyAndWait(t, ctxs)
	// If we are here then the test has passed
	testResult = testResultPass
	logrus.Infof("Test status at end of %s test: %s", t.Name(), testResult)
}

// This test simulates OCP upgrade by live-migrating VM to a *replica* node and
// then restarting PX on the node where volume is attached. It expects that the VM
// should end up with a bind-mount (hyperconvergence). PX performs *two*
// back-to-back live migrations in this test.
func kubeVirtHypercTwoLiveMigrations(t *testing.T) {
	var testrailID, testResult = 93197, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult)
	defer updateTestRail(&testResult, testrailID, runID)
	instanceID := "vm-two-lm"
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
		testState := &kubevirtTestState{
			appCtx:   appCtx,
			allNodes: allNodes,
		}
		verifyInitialVMI(t, testState)

		// Cordon off all non-replica nodes so that the next live migration moves the VM pod to a replica node
		cordonedNodes := cordonNonReplicaNodes(t, testState.apiVol, allNodes)
		uncordonFunc := func() { uncordonNodes(cordonedNodes) }
		defer uncordonFunc()

		// Simulate OCP node upgrade by:
		// 1. Live migrate the VM to another node
		// 2. Restart PX on the original node where the volume should still be attached

		// start a live migration and wait for it to finish
		// vmPod changes after the live migration
		startAndWaitForVMIMigration(t, testState, true /* expectReplicaNode */)

		// restart px on the original node and make sure that the volume attachment has moved
		restartVolumeDriverAndWaitForAttachmentToMove(t, testState)

		// VM should use a bind-mount eventually
		log.Infof("Waiting for the VM to return to the hyperconverged state again")
		verifyBindMountEventually(t, testState)
		uncordonFunc()
		cordonedNodes = nil
	}
	logrus.Infof("Destroying apps")
	destroyAndWait(t, ctxs)
	// If we are here then the test has passed
	testResult = testResultPass
	logrus.Infof("Test status at end of %s test: %s", t.Name(), testResult)
}

func verifyInitialVMI(t *testing.T, testState *kubevirtTestState) {
	appCtx := testState.appCtx

	err := schedulerDriver.WaitForRunning(appCtx, defaultWaitTimeout, defaultWaitInterval)
	require.NoError(t, err, "Error waiting for app %s to get to running state", appCtx.App.Key)

	vols, err := schedulerDriver.GetVolumes(appCtx)
	require.NoError(t, err, "failed to get volumes for context %s", appCtx.App.Key)

	// this test expects only one volume in the VM
	require.Equal(t, len(vols), 1)
	testState.volume = vols[0]

	testState.apiVol, err = volumeDriver.InspectVolume(testState.volume.ID)
	require.NoError(t, err, "Failed to inspect PV %s", testState.volume.ID)

	testState.attachedNode, err = volumeDriver.GetNodeForVolume(testState.volume, cmdTimeout, cmdRetry)
	require.NoError(t, err)
	log.Infof("volume %s (%s) is attached to node %s",
		testState.volume.ID, testState.apiVol.Id, testState.attachedNode.Name)

	// VMs should have a bind-mount initially
	testState.vmPod, err = getVMPod(appCtx, testState.volume)
	require.NoError(t, err)

	testState.vmiName, err = getVMINameFromVMPod(testState.vmPod)
	require.NoError(t, err)
	testState.vmiUID, testState.vmiPhase, testState.vmiPhaseTransitionTime, err = getVMIDetails(
		testState.vmPod.Namespace, testState.vmiName)
	require.NoError(t, err)
	require.Equal(t, "Running", testState.vmiPhase)

	verifyVMProperties(t, testState,
		false, /* expectAttachedNodeChanged  */
		true,  /* expectBindMount */
		true /* expectReplicaNode */)
}

func startAndWaitForVMIMigration(t *testing.T, testState *kubevirtTestState, migrateToReplicaNode bool) {
	ctx := context.TODO()
	vmiNamespace := testState.vmPod.Namespace
	vmiName := testState.vmiName

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
		// wait until there is only one pod in the running state
		testState.vmPod, err = getVMPod(testState.appCtx, testState.volume)
		if err != nil {
			logrus.Warnf("Failed to get VM pod while waiting for live migration to finish for VMI %s/%s: %v",
				vmiNamespace, vmiName, err)
			return false
		}
		logrus.Infof("VMI was live migrated to pod %s/%s", vmiNamespace, testState.vmPod.Name)
		return true
	}, 10*time.Minute, 10*time.Second, "migration for VMI %s/%s is stuck", vmiNamespace, migration.Name)

	// verify that the migration was successful
	require.Equal(t, "Succeeded", migr.Phase)
	require.False(t, migr.Failed)

	verifyVMProperties(t, testState,
		false, /* expectAttachedNodeChanged (attached node should not change after live migration) */
		false, /* expectBindMount (test always live-migrates a bind-mounted VM, so the new mount should be NFS) */
		migrateToReplicaNode /* expectReplicaNode */)
}

func restartVolumeDriverAndWaitForAttachmentToMove(t *testing.T, testState *kubevirtTestState) {
	var err error
	var newAttachedNode *node.Node

	appCtx := testState.appCtx
	oldAttachedNode := testState.attachedNode
	vol := testState.volume
	apiVol := testState.apiVol

	// restart px on the original node
	log.Infof("Restarting volume driver on node %s", oldAttachedNode.Name)
	err = volumeDriver.RestartDriver(*oldAttachedNode, nil)
	require.NoError(t, err)

	require.Eventuallyf(t, func() bool {
		newAttachedNode, err = volumeDriver.GetNodeForVolume(vol, cmdTimeout, cmdRetry)
		if err != nil {
			logrus.Warnf("Failed to get the attached node for vol %s (%s) for context %s: %v",
				vol.ID, apiVol.Id, appCtx.App.Key, err)
			return false
		}
		log.Infof("New attached node for volume %s (%s) is %s", vol.ID, apiVol.Id, newAttachedNode.Name)
		return oldAttachedNode.Name != newAttachedNode.Name
	}, 5*time.Minute, 30*time.Second, "Attached node did not change from %s for VM vol %s (%s)",
		oldAttachedNode.Name, vol.ID, apiVol.Id)

	log.Infof("Volume attachment changed from node %s to node %s after failover",
		oldAttachedNode.Name, newAttachedNode.Name)
	testState.attachedNode = newAttachedNode

	// verify that vm stayed up
	verifyVMStayedUp(t, testState)
}

func verifyBindMountEventually(t *testing.T, testState *kubevirtTestState) {
	var err error

	require.Eventuallyf(t, func() bool {
		testState.vmPod, err = getVMPod(testState.appCtx, testState.volume)
		if err != nil {
			// this is expected while the live migration is running
			logrus.Infof("Could not get VM pod for vol %s (%s) for context %s: %v",
				testState.volume.ID, testState.apiVol.Id, testState.appCtx.App.Key, err)
			return false
		}
		mountType, err := getVMRootDiskMountType(testState.vmPod, testState.apiVol)
		if err != nil {
			logrus.Warnf("Failed to get mount type of vol %s (%s) for context %s: %v",
				testState.volume.ID, testState.apiVol.Id, testState.appCtx.App.Key, err)
			return false
		}
		return mountType == mountTypeBind
	}, 10*time.Minute, 30*time.Second,
		"VM vol %s (%s) did not switch to a bind-mount", testState.volume.ID, testState.apiVol.Id)

	// Verify that VM stayed up the whole time
	verifyVMStayedUp(t, testState)
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
		node := node
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

func uncordonNodes(cordonedNodes []*node.Node) {
	for _, cordonedNode := range cordonedNodes {
		logrus.Infof("Uncordoning node %s", cordonedNode.Name)
		err := core.Instance().UnCordonNode(cordonedNode.Name, defaultWaitTimeout, defaultWaitInterval)
		if err != nil {
			log.Errorf("Failed to uncordon node %s: %v", cordonedNode.Name, err)
		}
	}
}

func getVMPod(appCtx *scheduler.Context, vol *volume.Volume) (*corev1.Pod, error) {
	pods, err := core.Instance().GetPodsUsingPV(vol.ID)
	if err != nil {
		return nil, fmt.Errorf("failed to get pods for volume %s of context %s: %w", vol.ID, appCtx.App.Key, err)
	}

	var found corev1.Pod
	for _, pod := range pods {
		if pod.Labels["kubevirt.io"] == "virt-launcher" && pod.Status.Phase == corev1.PodRunning {
			if found.Name != "" {
				// there should be only one VM pod in the running state (otherwise live migration is in progress)
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

func verifyVMProperties(
	t *testing.T, testState *kubevirtTestState, expectAttachedNodeChanged, expectBindMount, expectReplicaNode bool,
) {
	var err error
	previousAttachedNode := testState.attachedNode
	vol := testState.volume
	apiVol := testState.apiVol
	vmPod := testState.vmPod
	podNamespacedName := fmt.Sprintf("%s/%s", vmPod.Namespace, vmPod.Name)

	// verify attached node
	testState.attachedNode, err = volumeDriver.GetNodeForVolume(vol, cmdTimeout, cmdRetry)
	require.NoError(t, err)
	if expectAttachedNodeChanged {
		require.NotEqual(t, previousAttachedNode.Name, testState.attachedNode.Name, "attached node did not change")
	} else {
		require.Equal(t, previousAttachedNode.Name, testState.attachedNode.Name, "attached node changed")
	}

	// verify replica node
	replicaNodeIDs := getReplicaNodeIDs(apiVol)
	var podNodeID string
	for nodeID, node := range testState.allNodes {
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

// getVMIDetails returns VMI UID, phase and time when VMI transitioned to that phase.
func getVMIDetails(vmiNamespace, vmiName string) (string, string, time.Time, error) {
	vmi, err := kubevirt.Instance().GetVirtualMachineInstance(context.TODO(), vmiNamespace, vmiName)
	if err != nil {
		return "", "", time.Time{}, fmt.Errorf("failed to get VMI for %s/%s", vmiNamespace, vmiName)
	}

	var transitionTime time.Time
	for _, vmiPhaseTransition := range vmi.PhaseTransitions {
		if vmiPhaseTransition.Phase == vmi.Phase && vmiPhaseTransition.TransitionTime.After(transitionTime) {
			transitionTime = vmiPhaseTransition.TransitionTime
		}
	}
	if transitionTime.IsZero() {
		return "", "", time.Time{}, fmt.Errorf(
			"failed to determine when VMI %s/%s transitioned to phase %s", vmiNamespace, vmiName, vmi.Phase)
	}
	return vmi.UID, vmi.Phase, transitionTime, nil
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

	// Sample output if the volume is bind-mounted:
	// $ kubectl exec -it virt-launcher-fedora-communist-toucan-jfw7n -- mount
	// ...
	// /dev/pxd/pxd365793461222635857 on /run/kubevirt-private/vmi-disks/rootdisk type ext4 (rw,relatime,seclabel,discard)
	// ...
	bindMountRE := regexp.MustCompile(fmt.Sprintf("/dev/pxd/pxd%s on .*rootdisk type (ext4|xfs)", apiVol.Id))

	// Sample output if the volume is nfs-mounted:
	// $ kubectl exec -it virt-launcher-fedora-communist-toucan-bqcrp -- mount
	// ...
	// 172.30.194.11:/var/lib/osd/pxns/365793461222635857 on /run/kubevirt-private/vmi-disks/rootdisk type nfs (...)
	// ...
	nfsMountRE := regexp.MustCompile(fmt.Sprintf(":/var/lib/osd/pxns/%s on .*rootdisk type nfs", apiVol.Id))

	cmd := []string{"mount"}
	output, err := core.Instance().RunCommandInPod(cmd, pod.Name, "", pod.Namespace)
	if err != nil {
		return "", fmt.Errorf("failed to run command %v inside the pod %s", cmd, podNamespacedName)
	}
	var foundBindMount, foundNFSMount bool
	for _, line := range strings.Split(output, "\n") {
		if bindMountRE.MatchString(line) {
			if foundBindMount || foundNFSMount {
				return "", fmt.Errorf("multiple rootdisk mounts found: %s", output)
			}
			foundBindMount = true
			logrus.Infof("found root disk bind mounted for VM pod %s: %s", podNamespacedName, line)
		}

		if nfsMountRE.MatchString(line) {
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

func verifyVMStayedUp(t *testing.T, testState *kubevirtTestState) {
	// If a VM is stopped and started again, a new VMI object gets created with the same name (i.e. the UID will change).
	// We are using that fact here to ensure that the VM did not stop during our test.
	vmiUIDAfter, vmiPhaseAfter, transitionTimeAfter, err := getVMIDetails(testState.vmPod.Namespace, testState.vmiName)
	require.NoError(t, err)
	require.Equal(t, "Running", vmiPhaseAfter)
	require.Equal(t, testState.vmiUID, vmiUIDAfter)
	require.Equal(t, testState.vmiPhaseTransitionTime, transitionTimeAfter)
}

// get VMI name from ownerRef of the virt-launcher pod
func getVMINameFromVMPod(vmPod *corev1.Pod) (string, error) {
	var vmiRef *metav1.OwnerReference
	for _, ownerRef := range vmPod.OwnerReferences {
		if ownerRef.Kind == "VirtualMachineInstance" {
			vmiRef = &ownerRef
			break
		}
	}
	if vmiRef == nil {
		return "", fmt.Errorf("did not find VMI ownerRef in pod %s/%s", vmPod.Namespace, vmPod.Name)
	}
	return vmiRef.Name, nil
}
