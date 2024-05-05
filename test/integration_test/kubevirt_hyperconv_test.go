//go:build integrationtest
// +build integrationtest

package integrationtest

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/libopenstorage/openstorage/api"
	operatorv1 "github.com/libopenstorage/operator/pkg/apis/core/v1"
	"github.com/libopenstorage/operator/pkg/constants"
	"github.com/libopenstorage/stork/pkg/log"
	"github.com/portworx/sched-ops/k8s/core"
	kubevirt "github.com/portworx/sched-ops/k8s/kubevirt"
	kubevirtdy "github.com/portworx/sched-ops/k8s/kubevirt-dynamic"
	"github.com/portworx/sched-ops/k8s/operator"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/portworx/torpedo/drivers/volume"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	kubevirtv1 "kubevirt.io/api/core/v1"
	cdiv1beta1 "kubevirt.io/containerized-data-importer-api/pkg/apis/core/v1beta1"
)

const (
	cmdRetry              = 5 * time.Second
	cmdTimeout            = 1 * time.Minute
	mountTypeBind         = "bind"
	mountTypeNFS          = "nfs"
	vpsVolAffinityLabel   = "vps.portworx.io/volume-affinity"
	pxUpdateTestLabel     = "kubevirt-update-px-test"
	stcAnnotationMiscArgs = "portworx.io/misc-args"
)

// pxUpdateValidationFailure represents a validation failure during PX update. It happens when
// one or more virt launcher pods are still running on a node where a Portworx pod is being deleted.
type pxUpdateValidationFailure struct {
	timestamp        string
	nodeName         string
	deletedPXPod     string
	deletionTime     string
	virtLauncherPods map[string]string
}

type pxUpdateValidatorData struct {
	sync.Mutex
	// map of nodeName to map of virtLauncherPod UID to pod namespace/name
	virtLauncherPodsByNode map[string]map[string]string
	// px update validation failures caught by the watcher
	failures []pxUpdateValidationFailure
}

var validatorData pxUpdateValidatorData

type failedMigration struct {
	migration *kubevirtv1.VirtualMachineInstanceMigration
	firstSeen time.Time
	vmStopped bool
}

type eventWatcherDataType struct {
	sync.Mutex
	events []corev1.Event
}

var eventWatcherData eventWatcherDataType

type unblockPXUpdateData struct {
	sync.Mutex
	stop             bool
	failedMigrations map[string]*failedMigration
}

type vmDisk struct {
	diskName             string
	pvcName              string
	storageClassName     string
	waitForFirstConsumer bool
	volume               *volume.Volume
	apiVol               *api.Volume
	attachedNode         *node.Node
}

func (d *vmDisk) String() string {
	volName := ""
	if d.volume != nil {
		volName = d.volume.ID
	}
	volID := ""
	if d.apiVol != nil {
		volID = d.apiVol.Id
	}
	return fmt.Sprintf("VM disk [%s, %s, %s]", d.diskName, volName, volID)
}

type hotPlugDisk struct {
	pvcName          string
	pvName           string
	storageClassName string
	apiVol           *api.Volume
}

func (d *hotPlugDisk) String() string {
	volID := ""
	if d.apiVol != nil {
		volID = d.apiVol.Id
	}
	return fmt.Sprintf("hotplug disk [%s, %s, %s]", d.pvcName, d.pvName, volID)
}

type kubevirtTestState struct {
	appCtx                 *scheduler.Context
	allNodes               map[string]node.Node
	vmDisks                []*vmDisk
	vmiName                string
	vmiUID                 string
	vmiPhase               string
	vmiPhaseTransitionTime time.Time
	vmPod                  *corev1.Pod
	vmUID                  string
	hotPlugDisks           []*hotPlugDisk
}

// This test simulates OCP upgrade by live-migrating VM to a NON-replica node and
// then restarting PX on the node where volume is attached. It expects that the VM
// should end up with a bind-mount (hyperconvergence). PX performs
// a single live migration in this test.
func kubeVirtHypercOneLiveMigration(t *testing.T) {
	var testrailID, testResult = 93196, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	instanceID := "one-live-migr"

	// Background watcher to capture events
	startEventWatcher(t)
	t.Cleanup(func() { logAndClearEvents(t) })

	ctxs := kubevirtVMScaledDeployAndValidate(
		t,
		instanceID,
		[]string{
			"kubevirt-fedora", "kubevirt-fedora-wait-first-consumer", "kubevirt-fedora-multi-disks-wffc",
			"kubevirt-windows-22k-server", "kubevirt-windows-22k-server-wait-first-consumer",
			"kubevirt-fedora-multiple-disks-datavol-only",
		},
		kubevirtScale,
	)
	allNodes := node.GetNodesByVoDriverNodeID()

	// Verify the initial state of the VMs before making any changes to the cluster.
	verifyInitialHyperconvergence(t, ctxs, allNodes)

	// Iterate over all VMs and simulate OCP node upgrade for each.
	for _, appCtx := range ctxs {
		// We need to gather the testState again because it may have changed during the previous iteration.
		testState := &kubevirtTestState{
			appCtx:   appCtx,
			allNodes: allNodes,
		}
		gatherInitialVMIInfo(t, testState)
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
		log.InfoD("Waiting for the VM to return to the hyperconverged state again")
		verifyBindMount(t, testState, false /*initialCheck*/)

		// Verify that VM stayed up the whole time
		verifyVMStayedUp(t, testState)
	}
	log.InfoD("Destroying apps")
	destroyAndWait(t, ctxs)
	// If we are here then the test has passed
	testResult = testResultPass
	log.InfoD("Test status at end of %s test: %s", t.Name(), testResult)
}

// This test simulates OCP upgrade by live-migrating VM to a *replica* node and
// then restarting PX on the node where volume is attached. It expects that the VM
// should end up with a bind-mount (hyperconvergence). PX performs *two*
// back-to-back live migrations in this test.
func kubeVirtHypercTwoLiveMigrations(t *testing.T) {
	var testrailID, testResult = 93197, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	instanceID := "two-live-migr"

	// Background watcher to capture events
	startEventWatcher(t)
	t.Cleanup(func() { logAndClearEvents(t) })

	ctxs := kubevirtVMScaledDeployAndValidate(
		t,
		instanceID,
		[]string{
			"kubevirt-fedora", "kubevirt-fedora-wait-first-consumer", "kubevirt-fedora-multi-disks-wffc",
			"kubevirt-windows-22k-server", "kubevirt-windows-22k-server-wait-first-consumer",
			"kubevirt-fedora-multiple-disks-datavol-only",
		},
		kubevirtScale,
	)
	allNodes := node.GetNodesByVoDriverNodeID()

	// Verify the initial state of the VMs before making any changes to the cluster.
	verifyInitialHyperconvergence(t, ctxs, allNodes)

	for _, appCtx := range ctxs {
		testState := &kubevirtTestState{
			appCtx:   appCtx,
			allNodes: allNodes,
		}

		gatherInitialVMIInfo(t, testState)
		verifyInitialVMI(t, testState)

		// Cordon off all non-replica nodes so that the next live migration moves the VM pod to a replica node.
		// verifyInitialVMI verifies that all vmDisks are using the same set of replica nodes. So, we can use
		// the firt vmDisk below.
		cordonedNodes := cordonNonReplicaNodes(t, testState.vmDisks[0].apiVol, allNodes)
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
		log.InfoD("Waiting for the VM to return to the hyperconverged state again")
		verifyBindMount(t, testState, false /*initialCheck*/)

		// Verify that VM stayed up the whole time
		verifyVMStayedUp(t, testState)

		uncordonFunc()
		cordonedNodes = nil
	}
	log.InfoD("Destroying apps")
	destroyAndWait(t, ctxs)
	// If we are here then the test has passed
	testResult = testResultPass
	log.InfoD("Test status at end of %s test: %s", t.Name(), testResult)
}

// Add hotplug disks to a running VM and verify that they are collocated.
func kubeVirtHypercHotPlugDiskCollocation(t *testing.T) {
	var testrailID, testResult = 257201, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	instanceID := "hotplug-colo"

	// Background watcher to capture events
	startEventWatcher(t)
	t.Cleanup(func() { logAndClearEvents(t) })

	ctxs := kubevirtVMScaledDeployAndValidate(
		t,
		instanceID,
		[]string{
			"kubevirt-fedora", "kubevirt-fedora-wait-first-consumer",
		},
		kubevirtScale,
	)
	allNodes := node.GetNodesByVoDriverNodeID()

	// Verify the initial state of the VMs before making any changes to the cluster.
	verifyInitialHyperconvergence(t, ctxs, allNodes)

	for _, appCtx := range ctxs {
		testState := &kubevirtTestState{
			appCtx:   appCtx,
			allNodes: allNodes,
		}
		gatherInitialVMIInfo(t, testState)
		verifyInitialVMI(t, testState)

		addAndVerifyHotPlugDisks(t, testState)
	}
	log.InfoD("Destroying apps")
	destroyAndWait(t, ctxs)
	// If we are here then the test has passed
	testResult = testResultPass
	log.InfoD("Test status at end of %s test: %s", t.Name(), testResult)
}

// Deploy VM with a special label on the PVCs to skip adding VPS during vol creation.
// Then, wait for the VPS fix job to collocate the volumes.
func kubeVirtHypercVPSFixJob(t *testing.T) {
	var testrailID, testResult = 257177, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	instanceID := "vps-fix-job"

	// Background watcher to capture events
	startEventWatcher(t)
	t.Cleanup(func() { logAndClearEvents(t) })

	ctxs := kubevirtVMScaledDeployAndValidate(
		t,
		instanceID,
		[]string{
			"kubevirt-fedora-no-vps",
		},
		kubevirtScale,
	)
	allNodes := node.GetNodesByVoDriverNodeID()

	// set the cluster option to reduce the wait time
	setFixVPSJobFrequency(t, allNodes)
	for _, appCtx := range ctxs {
		testState := &kubevirtTestState{
			appCtx:   appCtx,
			allNodes: allNodes,
		}
		gatherInitialVMIInfo(t, testState)

		require.Eventuallyf(t, func() bool {
			return checkVMDisksCollocation(testState)
		}, time.Hour, 5*time.Second, "vm disks were not collocated")
	}
	log.InfoD("Destroying apps")
	destroyAndWait(t, ctxs)
	// If we are here then the test has passed
	testResult = testResultPass
	log.InfoD("Test status at end of %s test: %s", t.Name(), testResult)
}

// This test simulates OCP upgrade by live-migrating all VMs from a node a NON-replica node and
// then restarting PX on the node where volume is attached.
// It expects that all the VMs from that node should end up with a bind-mount (hyperconvergence).
// PX performs a single live migration for each VM on the node.
// The above steps are repeated for all worker nodes in the cluster
func kubeVirtSimulateOCPUpgrade(t *testing.T) {
	var testrailID, testResult = 297265, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	instanceID := "ocp-upgrade"

	// Background watcher to capture events
	startEventWatcher(t)
	t.Cleanup(func() { logAndClearEvents(t) })

	ctxs := kubevirtVMScaledDeployAndValidate(
		t,
		instanceID,
		[]string{
			"kubevirt-fedora", "kubevirt-fedora-wait-first-consumer", "kubevirt-fedora-multi-disks-wffc",
			"kubevirt-windows-22k-server", "kubevirt-windows-22k-server-wait-first-consumer",
			"kubevirt-fedora-multiple-disks-datavol-only",
		},
		kubevirtScale,
	)
	allNodes := node.GetNodesByVoDriverNodeID()

	// Verify the initial state of the VMs before making any changes to the cluster.
	verifyInitialHyperconvergence(t, ctxs, allNodes)

	nodeList := node.GetNodesByName()

	for nodeName, currNode := range nodeList {
		log.InfoD("\nStart OCP upgrade simulation on node: %s", nodeName)
		testStatesNode := getTestStatesForNode(t, ctxs, nodeName, allNodes)

		if len(testStatesNode) == 0 {
			log.InfoD("No VMs are present on node: %s. Skipping this node", nodeName)
			continue
		}

		for _, testState := range testStatesNode {
			// start a live migration and wait for it to finish, to simulate node drain in OCP.
			// vmPod changes after the live migration. The function below also verifies that the attachedNode
			// has not changed.
			startAndWaitForVMIMigration(t, testState, false /* expectReplicaNode */)
		}

		log.InfoD("Restarting volume driver on node %s", nodeName)
		restartVolumeDriverAndWaitForReady(t, &currNode)

		for _, testState := range testStatesNode {
			waitForVolumeAttachmentsToMove(t, testState, &currNode)

			log.InfoD("Waiting for the VM %s to return to the hyperconverged state again", testState.vmiName)
			verifyBindMount(t, testState, false /*initialCheck*/)

			// Verify that VM stayed up the whole time
			verifyVMStayedUp(t, testState)
		}
		log.InfoD("\nCompleted upgrade simulation on node: %s", nodeName)
	}

	log.InfoD("Destroying apps")
	destroyAndWait(t, ctxs)
	// If we are here then the test has passed
	testResult = testResultPass
	log.InfoD("Test status at end of %s test: %s", t.Name(), testResult)
}

func kubeVirtUpdatePX(t *testing.T) {
	var err error
	var testrailID, testResult = 297915, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	instanceID := "update-px"

	// Background watcher to capture events
	startEventWatcher(t)
	t.Cleanup(func() { logAndClearEvents(t) })

	ctxs := kubevirtVMScaledDeployAndValidate(
		t,
		instanceID,
		[]string{
			"kubevirt-fedora", "kubevirt-fedora-wait-first-consumer", "kubevirt-fedora-multi-disks-wffc",
			"kubevirt-windows-22k-server", "kubevirt-windows-22k-server-wait-first-consumer",
			"kubevirt-fedora-multiple-disks-datavol-only",
		},
		kubevirtScale,
	)
	allNodes := node.GetNodesByVoDriverNodeID()

	testStates := map[string]*kubevirtTestState{}
	for _, appCtx := range ctxs {
		testState := &kubevirtTestState{
			appCtx:   appCtx,
			allNodes: allNodes,
		}
		gatherInitialVMIInfo(t, testState)
		testStates[appCtx.App.Key] = testState
	}

	// Background watcher to catch PX update moving forward on a node with VMs still running on it
	startPXUpdateValidator(t)

	startTime := time.Now()

	// Update StorageCluster object to trigger PX update and wait for update to finish
	updatePX(t)

	// Iterate over all contexts and verify that the VM stayed up the whole time
	for _, appCtx := range ctxs {
		testState := testStates[appCtx.App.Key]
		testState.vmPod, err = getVMPod(testState.appCtx, testState.vmDisks[0].volume)
		log.FailOnError(t, err, "Failed to get pods for context %s", testState.appCtx.App.Key)
		// This is fragile but verifyVMStayedUp requires that only testState.vmPod needs to be up-to-date
		// and we have updated the vmPod above. Other values in the testState are not used in verifyVMStayedUp.
		verifyVMStayedUp(t, testState)
	}

	// check if the validator caught any failures
	checkPXUpdateValidationFailures(t, startTime)

	log.InfoD("Destroying apps")
	destroyAndWait(t, ctxs)
	// If we are here then the test has passed
	testResult = testResultPass
	log.InfoD("Test status at end of %s test: %s", t.Name(), testResult)
}

func kubeVirtUpdatePXBlocked(t *testing.T) {
	var err error
	var testrailID, testResult = 297916, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	instanceID := "update-px-blocked"

	// Background watcher to capture events
	startEventWatcher(t)
	t.Cleanup(func() { logAndClearEvents(t) })

	k8sNodes, err := core.Instance().GetNodes()
	log.FailOnError(t, err, "Failed to get k8s nodes")

	// remove our test label from all nodes first
	for _, k8sNode := range k8sNodes.Items {
		addRemoveTestLabelOnNode(t, k8sNode.Name, false /*add*/)
	}

	allNodes := node.GetNodesByVoDriverNodeID()

	// VM spec used in this test has "nodeSelector" to make the VM run only on nodes with a test label.
	//
	//   nodeSelector:
	//     kubevirt-update-px-test: "true"
	//
	// We will add that label to one of the nodes so that the VM can be scheduled only on that one node.
	// This will make the live-migration to fail during PX update since the VM cannot be moved to another node.
	// We will then stop the VM after some time to unblock PX update.

	// Select a node to pin the VM to.
	nodeToPinVMTo := ""
	for _, n := range allNodes {
		nodeToPinVMTo = n.SchedulerNodeName
		break
	}
	addRemoveTestLabelOnNode(t, nodeToPinVMTo, true /*add*/)
	t.Cleanup(func() { _ = addRemoveTestLabelOnNodeHelper(nodeToPinVMTo, false /*add*/) })

	// start VM with nodeSelector to make it run only on the node with the label
	ctxs := kubevirtVMScaledDeployAndValidate(
		t,
		instanceID,
		[]string{
			"kubevirt-fedora-with-node-selector",
		},
		kubevirtScale,
	)

	// Background watcher to catch PX update moving forward on a node with VMs still running on it
	startPXUpdateValidator(t)

	// Background routine to keep unblocking PX update by stopping VMs for failed migrations
	unblockerData := unblockPXUpdateData{
		failedMigrations: map[string]*failedMigration{},
	}
	go unblockPXUpdate(t, &unblockerData)

	startTime := time.Now()

	// Update StorageCluster object to trigger PX update and wait for update to finish
	updatePX(t)

	// there should be at least one failed VM live-migration during PX update
	unblockerData.Lock()
	Dash.VerifyFatal(t, len(unblockerData.failedMigrations) > 0, true, "Failed migrations not found")
	unblockerData.stop = true
	unblockerData.Unlock()

	// check if the validator caught any failures
	checkPXUpdateValidationFailures(t, startTime)

	// verify that at least one FailedToEvictVM event was generated after time the test was started
	verifyWatcherSawEvent(t, "FailedToEvictVM", startTime)

	log.InfoD("Destroying apps")
	destroyAndWait(t, ctxs)
	// If we are here then the test has passed
	testResult = testResultPass
	log.InfoD("Test status at end of %s test: %s", t.Name(), testResult)
}

func gatherInitialVMIInfo(t *testing.T, testState *kubevirtTestState) {
	appCtx := testState.appCtx

	err := schedulerDriver.WaitForRunning(appCtx, defaultWaitTimeout, defaultWaitInterval)
	log.FailOnError(t, err, "Error waiting for app %s to get to running state", appCtx.App.Key)

	vols, err := schedulerDriver.GetVolumes(appCtx)
	log.FailOnError(t, err, "failed to get volumes for context %s", appCtx.App.Key)

	for _, vol := range vols {
		vmDisk := &vmDisk{volume: vol}
		testState.vmDisks = append(testState.vmDisks, vmDisk)

		vmDisk.apiVol, err = volumeDriver.InspectVolume(vol.ID)
		log.FailOnError(t, err, "Failed to inspect PV %s", vol.ID)

		vmDisk.attachedNode, err = volumeDriver.GetNodeForVolume(vol, cmdTimeout, cmdRetry)
		log.FailOnError(t, err, fmt.Sprintf("Failed to get node for volume %s of context %s", vol.ID, appCtx.App.Key))

		vmDisk.pvcName = vmDisk.apiVol.Locator.VolumeLabels["pvc"]
		Dash.VerifyFatal(t, vmDisk.pvcName != "", true, "PVC name found in volume labels")

		pvc, err := core.Instance().GetPersistentVolumeClaim(vmDisk.pvcName, appCtx.App.NameSpace)
		log.FailOnError(t, err, "Failed to get PVC %s/%s for volume %s of context %s",
			appCtx.App.NameSpace, vmDisk.pvcName, vol.ID, appCtx.App.Key)

		Dash.VerifyFatal(t, pvc.Spec.StorageClassName != nil, true, fmt.Sprintf("PVC %s/%s has no storageClassName", appCtx.App.NameSpace, vmDisk.pvcName))

		Dash.VerifyFatal(t, pvc.Spec.VolumeName != "", true, fmt.Sprintf("PVC %s/%s has no volumeName", appCtx.App.NameSpace, vmDisk.pvcName))
		vmDisk.storageClassName = *pvc.Spec.StorageClassName

		sc, err := core.Instance().GetStorageClassForPVC(pvc)
		log.FailOnError(t, err, "Failed to get storageClass for PVC %s/%s for volume %s of context %s",
			appCtx.App.NameSpace, vmDisk.pvcName, vol.ID, appCtx.App.Key)

		if sc.VolumeBindingMode != nil && *sc.VolumeBindingMode == storagev1.VolumeBindingWaitForFirstConsumer {
			vmDisk.waitForFirstConsumer = true
		}

		if testState.vmPod == nil {
			testState.vmPod, err = getVMPod(appCtx, vol)
			log.FailOnError(t, err, "Failed to get pods for context %s", appCtx.App.Key)
		}

		// Get the volume name inside the pod yaml e.g.
		//	  volumes:
		//	  - name: rootdisk
		//	    persistentVolumeClaim:
		//		  claimName: fedora-communist-toucan
		for _, vmVol := range testState.vmPod.Spec.Volumes {
			if vmVol.PersistentVolumeClaim != nil && vmVol.PersistentVolumeClaim.ClaimName == vmDisk.pvcName {
				vmDisk.diskName = vmVol.Name
				break
			}
		}
		Dash.VerifyFatal(t, vmDisk.diskName != "", true, fmt.Sprintf("Failed to find disk name for PVC %s", vmDisk.pvcName))
		log.InfoD("%s attached to node %s", vmDisk, vmDisk.attachedNode.Name)
	}

	testState.vmPod, err = getVMPod(testState.appCtx, testState.vmDisks[0].volume)
	log.FailOnError(t, err, "Failed to get pods for context %s", testState.appCtx.App.Key)

	testState.vmiName, err = getVMINameFromVMPod(testState.vmPod)
	log.FailOnError(t, err, "Failed to get VMI name for pod %s", testState.vmPod.Name)

	var ready bool
	ready, testState.vmiUID, testState.vmiPhase, testState.vmiPhaseTransitionTime, testState.vmUID, err = getVMIDetails(
		testState.vmPod.Namespace, testState.vmiName)
	log.FailOnError(t, err, "Failed to get VMI details for pod %s", testState.vmPod.Name)
	Dash.VerifyFatal(t, testState.vmiPhase, "Running", fmt.Sprintf("VMI %s is not in Running state", testState.vmiName))
	Dash.VerifyFatal(t, ready, true, fmt.Sprintf("VMI %s is not ready", testState.vmiName))
}

func verifyInitialVMI(t *testing.T, testState *kubevirtTestState) {
	// verify all volumes are using the same set of replica nodes
	Dash.VerifyFatal(t, checkVMDisksCollocation(testState), true, "vm disks are collocated")

	verifyDisksAttachedOnSameNode(t, testState)

	// VM should have a bind-mount initially
	verifyBindMount(t, testState, true /*initialCheck*/)
}

// check if all volumes are using the same set of replica nodes and have VPS label+rule
func checkVMDisksCollocation(testState *kubevirtTestState) bool {
	var err error
	var prevReplicaNodeIDs map[string]bool
	var prevDisk *vmDisk
	var prevVPSLabelVal string
	for _, vmDisk := range testState.vmDisks {
		// refresh the apiVol to get the current state of the replicas
		vmDisk.apiVol, err = volumeDriver.InspectVolume(vmDisk.volume.ID)
		if err != nil {
			log.Warn("Failed to inspect volume for %s: %v", vmDisk, err)
			return false
		}

		replicaNodeIDs := getReplicaNodeIDs(vmDisk.apiVol)
		if prevReplicaNodeIDs != nil {
			if !matchReplicaNodeIDs(prevReplicaNodeIDs, replicaNodeIDs) {
				log.Warn("%s and %s have replicas on different nodes", prevDisk, vmDisk)
				return false
			}
		} else {
			prevReplicaNodeIDs = replicaNodeIDs
			prevDisk = vmDisk
		}
		// verify that our vps label is set
		vpsLabelVal := vmDisk.apiVol.Spec.VolumeLabels[vpsVolAffinityLabel]
		if vpsLabelVal == "" {
			log.Warn("PX volume for %s does not have %s label", vmDisk, vpsVolAffinityLabel)
			return false
		}
		log.InfoD("Found label %s=%s on %s", vpsVolAffinityLabel, vpsLabelVal, vmDisk)
		if prevVPSLabelVal != "" && vpsLabelVal != prevVPSLabelVal {
			log.Warn("VPS label values (%s vs %s) don't match for %s and %s",
				prevVPSLabelVal, vpsLabelVal, prevDisk, vmDisk)
			return false
		}
		prevVPSLabelVal = vpsLabelVal
	}
	return true
}

func addAndVerifyHotPlugDisks(t *testing.T, testState *kubevirtTestState) {
	// add 3 disks with ownerRef in DataVolume. PX will deduce VM UID from that ownerref during preCreate.
	// This simulates how OCP web interface adds the hotplug disks.
	for i := 0; i < 3; i++ {
		dvName := fmt.Sprintf("hotplug-with-ownerref-%d", i)
		hpDisk := addHotPlugDisk(t, testState, dvName, true /*wantOwnerRefOnDV*/)

		testState.hotPlugDisks = append(testState.hotPlugDisks, hpDisk)

		// verify that the replicas are collocated
		verifyHotPlugDisk(t, testState, hpDisk, false /*waitForVPSFixJob*/)
	}

	waitForVPSFixJob := true
	if testState.vmDisks[0].waitForFirstConsumer {
		// If the storageClass is using waitForFirstConsumer, PX should deduce the VM UID during volume creation.
		waitForVPSFixJob = false
	}

	// add 3 disks without ownerRef in DataVolume.
	// If volumeBindingMode=waitForFirstConsumer, PX will deduce the VM UID from the hotplug pod whose
	// ownerRef points to the virt-launcher pod. This will happen during vol creation.
	//
	// If volumeBindingMode=immediate, PX will not deduce VM UID during vol creation. VPS fix job will
	// collocate the replicas post-creation.
	//
	startIndex := len(testState.hotPlugDisks)
	for i := 0; i < 3; i++ {
		dvName := fmt.Sprintf("hotplug-no-ownerref-%d", i)
		hpDisk := addHotPlugDisk(t, testState, dvName, false /*wantOwnerRefOnDV*/)
		testState.hotPlugDisks = append(testState.hotPlugDisks, hpDisk)
	}
	for i := startIndex; i < len(testState.hotPlugDisks); i++ {
		verifyHotPlugDisk(t, testState, testState.hotPlugDisks[i], waitForVPSFixJob)
	}
}

func addHotPlugDisk(t *testing.T, testState *kubevirtTestState, dvName string, wantOwnerRefOnDV bool) *hotPlugDisk {
	ctx := context.TODO()
	var volumeMode corev1.PersistentVolumeMode = corev1.PersistentVolumeFilesystem
	appCtx := testState.appCtx
	ns := appCtx.App.NameSpace

	kvCli := kubevirt.Instance().GetKubevirtClient()

	dv := &cdiv1beta1.DataVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: dvName,
		},
		Spec: cdiv1beta1.DataVolumeSpec{
			Source: &cdiv1beta1.DataVolumeSource{
				Blank: &cdiv1beta1.DataVolumeBlankImage{},
			},
			Storage: &cdiv1beta1.StorageSpec{
				StorageClassName: &testState.vmDisks[0].storageClassName,
				AccessModes: []corev1.PersistentVolumeAccessMode{
					corev1.ReadWriteMany,
				},
				VolumeMode: &volumeMode,
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceName(corev1.ResourceStorage): resource.MustParse("3Gi"),
					},
				},
			},
		},
	}
	if wantOwnerRefOnDV {
		dv.ObjectMeta.OwnerReferences = []metav1.OwnerReference{
			{
				APIVersion: "kubevirt.io/v1",
				Kind:       "VirtualMachine",
				Name:       testState.vmiName, // VM name is the same as VMI name
				UID:        types.UID(testState.vmUID),
			},
		}
	}
	log.InfoD("Creating a hotplug data volume %s/%s", ns, dvName)
	_, err := kvCli.CdiClient().CdiV1beta1().DataVolumes(ns).Create(ctx, dv, metav1.CreateOptions{})
	log.FailOnError(t, err, "Failed to create hotplug data volume %s/%s", ns, dvName)

	// add hotplug volume to the VMI
	opts := &kubevirtv1.AddVolumeOptions{
		Name: dvName,
		Disk: &kubevirtv1.Disk{
			DiskDevice: kubevirtv1.DiskDevice{
				Disk: &kubevirtv1.DiskTarget{Bus: kubevirtv1.DiskBusSCSI},
			},
		},
		VolumeSource: &kubevirtv1.HotplugVolumeSource{
			DataVolume: &kubevirtv1.DataVolumeSource{
				Name: dvName,
			},
		},
	}
	log.InfoD("Adding hotplug data volume %s/%s to VMI %s", ns, dvName, testState.vmiName)
	err = kvCli.VirtualMachineInstance(ns).AddVolume(ctx, testState.vmiName, opts)
	log.FailOnError(t, err, "Failed to add hotplug data volume %s/%s to VMI %s", ns, dvName, testState.vmiName)

	// wait until the PVC is bound
	var pvc *corev1.PersistentVolumeClaim
	require.Eventuallyf(t, func() bool {
		pvc, err = core.Instance().GetPersistentVolumeClaim(dvName, ns)
		if err != nil {
			log.Warn("Failed to get PVC for DataVolume %s/%s of context %s: %v",
				ns, appCtx.App.NameSpace, appCtx.App.Key, err)
			return false
		}
		if pvc.Status.Phase != corev1.ClaimBound {
			log.Warn("Waiting for PVC %s/%s phase to be %s; current value: %s",
				pvc.Namespace, pvc.Name, corev1.ClaimBound, pvc.Status.Phase)
			return false
		}
		log.InfoD("PVC %s/%s phase is %s", pvc.Namespace, pvc.Name, pvc.Status.Phase)
		return true
	}, 3*time.Minute, 5*time.Second, "PVC %s/%s did not become bound", ns, dvName)

	hpDisk := &hotPlugDisk{
		pvcName:          pvc.Name,
		pvName:           pvc.Spec.VolumeName,
		storageClassName: *pvc.Spec.StorageClassName,
	}
	hpDisk.apiVol, err = volumeDriver.InspectVolume(hpDisk.pvName)
	log.FailOnError(t, err, "Failed to inspect PV %s for %s", hpDisk.pvName, hpDisk)
	return hpDisk
}

func verifyHotPlugDisk(t *testing.T, testState *kubevirtTestState, hpDisk *hotPlugDisk, waitForVPSFixJob bool) {
	if !waitForVPSFixJob {
		Dash.VerifyFatal(t, isHotplugDiskCollocated(testState, hpDisk), true, fmt.Sprintf("%s was collocated", hpDisk))
		return
	}
	// set the cluster option to reduce the wait time
	setFixVPSJobFrequency(t, testState.allNodes)
	require.Eventuallyf(t, func() bool {
		return isHotplugDiskCollocated(testState, hpDisk)
	}, time.Hour, 5*time.Second, "%s was not collocated", hpDisk)
}

func setFixVPSJobFrequency(t *testing.T, allNodes map[string]node.Node) {
	var n node.Node
	for _, n = range allNodes {
		break
	}
	err := volumeDriver.SetClusterOpts(n, map[string]string{"--fix-vps-frequency-in-minutes": "1"})
	require.NoError(t, err)
}

func isHotplugDiskCollocated(testState *kubevirtTestState, hpDisk *hotPlugDisk) bool {
	var err error

	vmDisk := testState.vmDisks[0]
	vmDiskReplicas := getReplicaNodeIDs(vmDisk.apiVol)
	vmDiskLabelVal := vmDisk.apiVol.Spec.VolumeLabels[vpsVolAffinityLabel]

	// refresh the apiVol to get the current state of the replicas
	hpDisk.apiVol, err = volumeDriver.InspectVolume(hpDisk.pvName)
	if err != nil {
		log.Warn("Failed to inspect PV %s for %s: %v", hpDisk.pvName, hpDisk, err)
		return false
	}
	hpDiskReplicas := getReplicaNodeIDs(hpDisk.apiVol)
	if !matchReplicaNodeIDs(vmDiskReplicas, hpDiskReplicas) {
		log.Warn("%s and %s have replicas on different nodes", hpDisk, vmDisk)
		return false
	}
	// verify that our vps label is set
	hpDiskLabelVal := hpDisk.apiVol.Spec.VolumeLabels[vpsVolAffinityLabel]
	if hpDiskLabelVal == "" {
		log.Warn("PX volume for %s does not have %s label", hpDisk, vpsVolAffinityLabel)
		return false
	}
	if hpDiskLabelVal != vmDiskLabelVal {
		log.Warn("VPS label value %s for %s doesn't match with the label %s for %s",
			hpDiskLabelVal, hpDisk, vmDiskLabelVal, vmDisk)
		return false
	}
	log.InfoD("%s is collocated", hpDisk)
	return true
}

func startAndWaitForVMIMigration(t *testing.T, testState *kubevirtTestState, migrateToReplicaNode bool) {
	ctx := context.TODO()
	vmiNamespace := testState.vmPod.Namespace
	vmiName := testState.vmiName

	// start migration
	migration, err := kubevirtdy.Instance().CreateVirtualMachineInstanceMigration(ctx, vmiNamespace, vmiName)
	log.FailOnError(t, err, "Failed to create migration for VMI %s/%s", vmiNamespace, vmiName)

	// wait for completion
	var migr *kubevirtdy.VirtualMachineInstanceMigration
	require.Eventuallyf(t, func() bool {
		migr, err = kubevirtdy.Instance().GetVirtualMachineInstanceMigration(ctx, vmiNamespace, migration.Name)
		if err != nil {
			log.Warn("Failed to get migration %s/%s: %v", vmiNamespace, migration.Name, err)
			return false
		}
		if !migr.Completed {
			log.Warn("VMI migration %s/%s is still not completed", vmiNamespace, migration.Name)
			return false
		}
		// wait until there is only one pod in the running state
		testState.vmPod, err = getVMPod(testState.appCtx, testState.vmDisks[0].volume)
		if err != nil {
			log.Warn("Failed to get VM pod while waiting for live migration to finish for VMI %s/%s: %v",
				vmiNamespace, vmiName, err)
			return false
		}
		log.InfoD("VMI was live migrated to pod %s/%s", vmiNamespace, testState.vmPod.Name)
		return true
	}, 10*time.Minute, 10*time.Second, "migration for VMI %s/%s is stuck", vmiNamespace, migration.Name)

	// verify that the migration was successful
	Dash.VerifyFatal(t, migr.Phase, "Succeeded", "Migration succeeded")

	verifyVMProperties(t, testState,
		false, /* expectAttachedNodeChanged (attached node should not change after live migration) */
		false, /* expectBindMount (test always live-migrates a bind-mounted VM, so the new mount should be NFS) */
		migrateToReplicaNode /* expectReplicaNode */)
}

func restartVolumeDriverAndWaitForAttachmentToMove(t *testing.T, testState *kubevirtTestState) {
	verifyDisksAttachedOnSameNode(t, testState)
	attachedNode := testState.vmDisks[0].attachedNode
	log.InfoD("Restarting volume driver on node %s", attachedNode.Name)
	restartVolumeDriverAndWaitForReady(t, attachedNode)
	waitForVolumeAttachmentsToMove(t, testState, attachedNode)
}

// Verify that all VM disks are attached on the same node.
// This function assumes that the attachedNode has already been updated for each vmDisk in the testState.
// It does not inspect the volume again.
func verifyDisksAttachedOnSameNode(t *testing.T, testState *kubevirtTestState) {
	var firstDisk *vmDisk

	// we expect all volumes to be attached on the same node
	for _, vmDisk := range testState.vmDisks {
		if firstDisk == nil {
			firstDisk = vmDisk
		} else {
			Dash.VerifyFatal(t, firstDisk.attachedNode.Name, vmDisk.attachedNode.Name, "VM disks are attached on same nodes")
		}
	}
}

func verifyBindMount(t *testing.T, testState *kubevirtTestState, initialCheck bool) {
	for _, vmDisk := range testState.vmDisks {
		var err error

		isBindMounted := func() bool {
			testState.vmPod, err = getVMPod(testState.appCtx, vmDisk.volume)
			if err != nil {
				// this is expected while the live migration is running since there will be 2 VM pods
				log.InfoD("Could not get VM pod for %s for context %s: %v", vmDisk, testState.appCtx.App.Key, err)
				return false
			}
			log.InfoD("Verifying bind mount for %s", vmDisk)
			mountType, err := getVMDiskMountType(testState.vmPod, vmDisk)
			if err != nil {
				log.Warn("Failed to get mount type of %s for context %s: %v", vmDisk, testState.appCtx.App.Key, err)
				return false
			}
			if mountType != mountTypeBind {
				if !initialCheck {
					log.Warn("Waiting for %s for context %s to switch to bind-mount from %q",
						vmDisk, testState.appCtx.App.Key, mountType)
				}
				return false
			}
			return true
		}

		if initialCheck {
			// This is the first time after VM was provisioned. It should be bind-mounted immediately. No need to retry.
			Dash.VerifyFatal(t, isBindMounted(), true, fmt.Sprintf("Initial check for %s", vmDisk))
			continue
		}

		// Wait for PX to perform additional live migration/s to return the VM to a bind-mounted state.
		require.Eventuallyf(t, isBindMounted, 10*time.Minute, 30*time.Second, "%s did not switch to a bind-mount", vmDisk)
	}
}

func cordonNonReplicaNodes(t *testing.T, vol *api.Volume, allNodes map[string]node.Node) []*node.Node {
	replicaNodeIDs := getReplicaNodeIDs(vol)
	var cordonedNodes []*node.Node

	for nodeID, n := range allNodes {
		if replicaNodeIDs[nodeID] {
			continue
		}
		log.InfoD("Cordoning non-replica node %s (%s)", n.Name, nodeID)
		err := core.Instance().CordonNode(n.Name, defaultWaitTimeout, defaultWaitInterval)
		log.FailOnError(t, err, "Failed to cordon node %s (%s)", n.Name, nodeID)
		n := n
		cordonedNodes = append(cordonedNodes, &n)
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

func matchReplicaNodeIDs(left, right map[string]bool) bool {
	if len(left) != len(right) {
		return false
	}
	for replicaNodeID := range left {
		if !right[replicaNodeID] {
			return false
		}
	}
	return true
}

func uncordonNodes(cordonedNodes []*node.Node) {
	for _, cordonedNode := range cordonedNodes {
		log.InfoD("Uncordoning node %s", cordonedNode.Name)
		err := core.Instance().UnCordonNode(cordonedNode.Name, defaultWaitTimeout, defaultWaitInterval)
		if err != nil {
			log.Error("Failed to uncordon node %s: %v", cordonedNode.Name, err)
		}
	}
}

func isVirtLauncherPod(pod *corev1.Pod) bool {
	return pod.Labels["kubevirt.io"] == "virt-launcher"
}

func getVMPod(appCtx *scheduler.Context, vol *volume.Volume) (*corev1.Pod, error) {
	pods, err := core.Instance().GetPodsUsingPV(vol.ID)
	if err != nil {
		return nil, fmt.Errorf("failed to get pods for volume %s of context %s: %w", vol.ID, appCtx.App.Key, err)
	}

	var found corev1.Pod
	for _, pod := range pods {
		if isVirtLauncherPod(&pod) && pod.Status.Phase == corev1.PodRunning {
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

	vmPod := testState.vmPod
	podNamespacedName := fmt.Sprintf("%s/%s", vmPod.Namespace, vmPod.Name)

	var podNodeID string
	for nodeID, node := range testState.allNodes {
		if vmPod.Spec.NodeName == node.Name {
			podNodeID = nodeID
			break
		}
	}
	Dash.VerifyFatal(t, podNodeID != "", true, fmt.Sprintf("find nodeID for node %s where pod %s is running", vmPod.Spec.NodeName, podNamespacedName))

	for _, vmDisk := range testState.vmDisks {
		log.InfoD("Checking %s", vmDisk)
		previousAttachedNode := vmDisk.attachedNode

		// verify attached node
		vmDisk.attachedNode, err = volumeDriver.GetNodeForVolume(vmDisk.volume, cmdTimeout, cmdRetry)
		log.FailOnError(t, err, "Failed to get attached node for %s", vmDisk)
		if expectAttachedNodeChanged {
			Dash.VerifyFatal(t, previousAttachedNode.Name != vmDisk.attachedNode.Name, true, fmt.Sprintf("attached node changed for %s", vmDisk))
		} else {
			Dash.VerifyFatal(t, previousAttachedNode.Name, vmDisk.attachedNode.Name, fmt.Sprintf("attached node changed for %s", vmDisk))
		}

		// verify replica node
		replicaNodeIDs := getReplicaNodeIDs(vmDisk.apiVol)
		if expectReplicaNode {
			Dash.VerifyFatal(t, replicaNodeIDs[podNodeID], true, fmt.Sprintf("pod is running on node %s (%s) which is NOT a replica node for %s", vmPod.Spec.NodeName, podNodeID, vmDisk))
		} else {
			Dash.VerifyFatal(t, replicaNodeIDs[podNodeID], false, fmt.Sprintf("pod is running on node %s (%s) which is a replica node for %s", vmPod.Spec.NodeName, podNodeID, vmDisk))
		}

		// verify mount type
		mountType, err := getVMDiskMountType(vmPod, vmDisk)
		log.FailOnError(t, err, "Failed to get mount type for %s", vmDisk)

		if expectBindMount {
			Dash.VerifyFatal(t, mountTypeBind, mountType, fmt.Sprintf("%s was bind-mounted", vmDisk))
		} else {
			Dash.VerifyFatal(t, mountTypeNFS, mountType, fmt.Sprintf("%s was nfs-mounted", vmDisk))
		}
		log.InfoD("Verified mount type %q for %s", mountType, vmDisk)
	}
}

// getVMIDetails returns VMI UID, phase and time when VMI transitioned to that phase, and ownerVM's UID.
func getVMIDetails(vmiNamespace, vmiName string) (bool, string, string, time.Time, string, error) {
	vmi, err := kubevirtdy.Instance().GetVirtualMachineInstance(context.TODO(), vmiNamespace, vmiName)
	if err != nil {
		return false, "", "", time.Time{}, "", fmt.Errorf("failed to get VMI for %s/%s", vmiNamespace, vmiName)
	}

	var transitionTime time.Time
	for _, vmiPhaseTransition := range vmi.PhaseTransitions {
		if vmiPhaseTransition.Phase == vmi.Phase && vmiPhaseTransition.TransitionTime.After(transitionTime) {
			transitionTime = vmiPhaseTransition.TransitionTime
		}
	}
	if transitionTime.IsZero() {
		return false, "", "", time.Time{}, "", fmt.Errorf(
			"failed to determine when VMI %s/%s transitioned to phase %s", vmiNamespace, vmiName, vmi.Phase)
	}
	return vmi.Ready, vmi.UID, vmi.Phase, transitionTime, vmi.OwnerVMUID, nil
}

// Get mount type (nfs or bind) of the VM disk
func getVMDiskMountType(pod *corev1.Pod, vmDisk *vmDisk) (string, error) {
	podNamespacedName := pod.Namespace + "/" + pod.Name
	log.InfoD("Checking the mount type of %s in pod %s", vmDisk, podNamespacedName)

	// Sample output if the volume is bind-mounted: (vmDisk.diskName is "rootdisk" in this example)
	// $ kubectl exec -it virt-launcher-fedora-communist-toucan-jfw7n -- mount
	// ...
	// /dev/pxd/pxd365793461222635857 on /run/kubevirt-private/vmi-disks/rootdisk type ext4 (rw,relatime,seclabel,discard)
	// ...
	bindMountRE := regexp.MustCompile(fmt.Sprintf("/dev/pxd/pxd%s on .*%s type (ext4|xfs)",
		vmDisk.apiVol.Id, vmDisk.diskName))

	// Sample output if the volume is nfs-mounted: (vmDisk.diskName is "rootdisk" in this example)
	// $ kubectl exec -it virt-launcher-fedora-communist-toucan-bqcrp -- mount
	// ...
	// 172.30.194.11:/var/lib/osd/pxns/365793461222635857 on /run/kubevirt-private/vmi-disks/rootdisk type nfs (...)
	// ...
	nfsMountRE := regexp.MustCompile(fmt.Sprintf(":/var/lib/osd/pxns/%s on .*%s type nfs",
		vmDisk.apiVol.Id, vmDisk.diskName))

	cmd := []string{"mount"}
	output, err := core.Instance().RunCommandInPod(cmd, pod.Name, "", pod.Namespace)
	if err != nil {
		return "", fmt.Errorf("failed to run command %v inside the pod %s", cmd, podNamespacedName)
	}
	var foundBindMount, foundNFSMount bool
	for _, line := range strings.Split(output, "\n") {
		if bindMountRE.MatchString(line) {
			if foundBindMount || foundNFSMount {
				return "", fmt.Errorf("multiple mounts found for %s: %s", vmDisk, output)
			}
			foundBindMount = true
			log.InfoD("Found %s bind mounted for VM pod %s: %s", vmDisk, podNamespacedName, line)
		}

		if nfsMountRE.MatchString(line) {
			if foundBindMount || foundNFSMount {
				return "", fmt.Errorf("multiple mounts found for %s: %s", vmDisk, output)
			}
			foundNFSMount = true
			log.InfoD("Found %s nfs mounted for VM pod %s: %s", vmDisk, podNamespacedName, line)
		}
	}
	if !foundBindMount && !foundNFSMount {
		return "", fmt.Errorf("no mount for %s in pod %s: %s", vmDisk, podNamespacedName, output)
	}
	if foundBindMount {
		return mountTypeBind, nil
	}
	return mountTypeNFS, nil
}

func verifyVMStayedUp(t *testing.T, testState *kubevirtTestState) {
	// If a VM is stopped and started again, a new VMI object gets created with the same name (i.e. the UID will change).
	// We are using that fact here to ensure that the VM did not stop during our test.
	ready, vmiUIDAfter, vmiPhaseAfter, transitionTimeAfter, _, err := getVMIDetails(testState.vmPod.Namespace, testState.vmiName)
	log.FailOnError(t, err, "failed to get VMI details after the test")
	Dash.VerifyFatal(t, ready, true, "VMI ready")
	Dash.VerifyFatal(t, vmiPhaseAfter, "Running", "VMI phase running")
	Dash.VerifyFatal(t, testState.vmiUID, vmiUIDAfter, "VMI UID")
	Dash.VerifyFatal(t, testState.vmiPhaseTransitionTime, transitionTimeAfter, "transitionTimeAfter verified")
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

// returns a list of kubevirtTestState objects for given node
func getTestStatesForNode(t *testing.T, ctxs []*scheduler.Context, nodeName string, allNodes map[string]node.Node) []*kubevirtTestState {
	testStatesForNode := make([]*kubevirtTestState, 0)
	for _, appCtx := range ctxs {
		testState := &kubevirtTestState{
			appCtx:   appCtx,
			allNodes: allNodes,
		}
		gatherInitialVMIInfo(t, testState)

		// Add this testState to the list of testStates for this node
		if nodeName == testState.vmPod.Spec.NodeName {
			log.InfoD("Found VM %s on node: %s", testState.vmPod.Name, nodeName)
			verifyInitialVMI(t, testState)
			// verifyInitialVMI has verified that all volumes are attached on the same node. We still need to
			// verify that the volumes are attached on the node we are interested in.
			require.True(t, testState.vmDisks[0].attachedNode.Name == nodeName)
			testStatesForNode = append(testStatesForNode, testState)
		}
	}
	return testStatesForNode
}

// Waits for the VM volume attachments to move to a different node after sharedv4 service failover.
// Called after restarting the volume driver on the oldAttachedNode. Verifies that all volumes
// are attached to the same node after the failover.
func waitForVolumeAttachmentsToMove(t *testing.T, testState *kubevirtTestState, oldAttachedNode *node.Node) {
	var newAttachedNode *node.Node
	var firstDisk *vmDisk = testState.vmDisks[0]
	for _, vmDisk := range testState.vmDisks {
		var attachedNode *node.Node
		var err error

		require.Eventuallyf(t, func() bool {
			attachedNode, err = volumeDriver.GetNodeForVolume(vmDisk.volume, cmdTimeout, cmdRetry)
			if err != nil {
				log.Warn("Failed to get the attached node for %s for context %s: %v",
					vmDisk, testState.appCtx.App.Key, err)
				return false
			}
			log.InfoD("New attached node for %s is %s", vmDisk, attachedNode.Name)
			return oldAttachedNode.Name != attachedNode.Name
		}, 5*time.Minute, 30*time.Second, "Attached node did not change from %s for %s",
			oldAttachedNode.Name, vmDisk)

		log.InfoD("%s: attachment changed from node %s to node %s after failover",
			vmDisk, oldAttachedNode.Name, attachedNode.Name)

		vmDisk.attachedNode = attachedNode

		// Verify that all volume attachments move to the same replica node
		if newAttachedNode == nil {
			newAttachedNode = attachedNode
		} else {
			require.Equal(t, newAttachedNode.Name, attachedNode.Name,
				"vm disks [%s] and [%s] attached on different nodes after sharedv4 failover", firstDisk, vmDisk)
		}
	}
	// verify that vm stayed up
	verifyVMStayedUp(t, testState)
}

func restartVolumeDriverAndWaitForReady(t *testing.T, attachedNode *node.Node) {
	log.InfoD("Restarting volume driver on node %s", attachedNode.Name)
	err := volumeDriver.StopDriver([]node.Node{*attachedNode}, false, nil)
	require.NoError(t, err)

	err = volumeDriver.WaitDriverDownOnNode(*attachedNode)
	require.NoError(t, err)

	err = volumeDriver.StartDriver(*attachedNode)
	require.NoError(t, err)

	err = volumeDriver.WaitDriverUpOnNode(*attachedNode, 10*time.Minute)
	require.NoError(t, err)

	log.InfoD("Volume driver is up on node %s", attachedNode.Name)
}

// Verify the initial state of the VMs before making any changes to the cluster.
func verifyInitialHyperconvergence(t *testing.T, ctxs []*scheduler.Context, allNodes map[string]node.Node) {
	for _, appCtx := range ctxs {
		testState := &kubevirtTestState{
			appCtx:   appCtx,
			allNodes: allNodes,
		}
		gatherInitialVMIInfo(t, testState)
		verifyInitialVMI(t, testState)
	}
}

// Trigger PX rolling update and wait for it to be completed.
func updatePX(t *testing.T) {
	var err error

	// We need to update the StorageCluster to trigger a PX update. In addition to deleting the oci-mon pods,
	// we want the update to restart PX as well. We change the misc-args annotation to achieve this.
	stc := getSTC(t)
	miscArgs := stc.Annotations[stcAnnotationMiscArgs]
	parts := strings.Split(miscArgs, ",")
	currentTraceFileDiskUsage := -1
	for _, part := range parts {
		keyValPair := strings.Split(part, "=")
		if len(keyValPair) == 2 && keyValPair[0] == "--tracefile-diskusage" {
			currentTraceFileDiskUsage, err = strconv.Atoi(keyValPair[1])
			require.NoError(t, err)
			break
		}
	}
	if currentTraceFileDiskUsage >= 0 {
		newVal := strconv.Itoa(currentTraceFileDiskUsage + 1)
		stc.Annotations[stcAnnotationMiscArgs] = strings.Replace(miscArgs,
			"--tracefile-diskusage="+strconv.Itoa(currentTraceFileDiskUsage), "--tracefile-diskusage="+newVal, 1)
	} else {
		stc.Annotations[stcAnnotationMiscArgs] = miscArgs + ",--tracefile-diskusage=7"
	}
	log.InfoD("Updating StorageCluster misc-args to trigger PX update: %s", stc.Annotations[stcAnnotationMiscArgs])
	_, err = operator.Instance().UpdateStorageCluster(stc)
	require.NoError(t, err)

	startTime := time.Now()
	// Wait until the update is in progress
	log.InfoD("Waiting for PX update to start")
	waitForStorageClusterCondition(t, operatorv1.ClusterConditionTypeUpdate, operatorv1.ClusterConditionStatusInProgress,
		5*time.Minute)

	// Wait until the update is completed
	log.InfoD("Waiting for PX update to finish")
	waitForStorageClusterCondition(t, operatorv1.ClusterConditionTypeUpdate, operatorv1.ClusterConditionStatusCompleted,
		time.Hour)
	log.InfoD("PX update finished in %v", time.Since(startTime))
}

func getSTC(t *testing.T) *operatorv1.StorageCluster {
	stcs, err := operator.Instance().ListStorageClusters(pxNamespace)
	require.NoError(t, err)
	require.Equal(t, 1, len(stcs.Items))
	return &(*stcs).Items[0]
}

func waitForStorageClusterCondition(
	t *testing.T, conditionType operatorv1.ClusterConditionType,
	status operatorv1.ClusterConditionStatus, timeout time.Duration,
) {
	// Example:
	//    - lastTransitionTime: "2024-05-04T23:24:50Z"
	//      message: Portworx update in progress, 2 nodes remaining
	//      source: Portworx
	//      status: InProgress
	//      type: Update
	//
	require.Eventually(t, func() bool {
		stc := getSTC(t)
		for _, condition := range stc.Status.Conditions {
			if condition.Source == "Portworx" && condition.Type == conditionType {
				if condition.Status == status {
					return true
				} else {
					log.Info("Waiting for StorageCluster condition %q to change to %q from %q: %s",
						conditionType, status, condition.Status, condition.Message)
					return false
				}
			}
		}
		log.Info("Could not find Portworx condition %s in StorageClusterstatus: %v", conditionType, stc.Status.Conditions)
		return false
	}, timeout, 5*time.Second)
}

func isPortworxPod(pod *corev1.Pod) bool {
	return pod.Labels["name"] == "portworx"
}

func startPXUpdateValidator(t *testing.T) {
	validatorData.Lock()
	defer validatorData.Unlock()
	if validatorData.virtLauncherPodsByNode == nil {
		validatorData.virtLauncherPodsByNode = make(map[string]map[string]string)
		startPodWatcher(t)
	}
}

func startPodWatcher(t *testing.T) {
	// Define the function that will be called for each watch update. It watches only virt-launcher and Portworx
	// pods. It keeps track of virt-launcher pods by node and reports a failure if a Portworx pod is being deleted
	// while there are virt-launcher pods on the same node.
	fn := func(object runtime.Object) error {
		validatorData.Lock()
		defer validatorData.Unlock()
		pod, ok := object.(*corev1.Pod)
		if !ok {
			err := fmt.Errorf("invalid object type on pod watch: %v", object)
			return err
		}
		if !isVirtLauncherPod(pod) && !isPortworxPod(pod) {
			return nil
		}
		if pod.Spec.NodeName == "" {
			return nil
		}
		virtLauncherPodsByNode := validatorData.virtLauncherPodsByNode
		log.InfoD("Pod %s/%s (%s) is in phase %s", pod.Namespace, pod.Name, pod.UID, pod.Status.Phase)

		if virtLauncherPodsByNode[pod.Spec.NodeName] == nil {
			virtLauncherPodsByNode[pod.Spec.NodeName] = make(map[string]string)
		}
		if isVirtLauncherPod(pod) {
			if pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
				// terminal state; remove from the map
				delete(virtLauncherPodsByNode[pod.Spec.NodeName], string(pod.UID))
			} else {
				virtLauncherPodsByNode[pod.Spec.NodeName][string(pod.UID)] = pod.Namespace + "/" + pod.Name
			}
		} else if isPortworxPod(pod) {
			if pod.DeletionTimestamp != nil {
				// portworx pod is being deleted; there should not be any active virt-launcher pods on the node
				if len(virtLauncherPodsByNode[pod.Spec.NodeName]) > 0 {
					log.Error("Portworx pod %s/%s is being deleted but there are active virt-launcher pods on the node: %v",
						pod.Namespace, pod.Name, virtLauncherPodsByNode[pod.Spec.NodeName])
					virtLauncherPods := map[string]string{}
					for uid, nsName := range virtLauncherPodsByNode[pod.Spec.NodeName] {
						virtLauncherPods[uid] = nsName
					}
					validatorData.failures = append(validatorData.failures, pxUpdateValidationFailure{
						timestamp:        time.Now().UTC().Format(time.RFC3339),
						nodeName:         pod.Spec.NodeName,
						deletedPXPod:     pod.Namespace + "/" + pod.Name,
						deletionTime:     pod.DeletionTimestamp.Time.UTC().Format(time.RFC3339),
						virtLauncherPods: virtLauncherPods,
					})
				}
			}
		}
		return nil
	}
	err := core.Instance().WatchPods("", fn, metav1.ListOptions{})
	log.FailOnError(t, err, "Failed to watch pods")
}

// check if the validator caught any failures
func checkPXUpdateValidationFailures(t *testing.T, startTime time.Time) {
	validatorData.Lock()
	defer validatorData.Unlock()
	failures := []pxUpdateValidationFailure{}
	for _, failure := range validatorData.failures {
		ts, err := time.Parse(time.RFC3339, failure.timestamp)
		log.FailOnError(t, err, "Failed to parse timestamp %s", failure.timestamp)
		if !ts.After(startTime) {
			continue
		}
		log.Warn("PX pod %s on node %s was deleted at %v while the node still had VMs: %v",
			failure.deletedPXPod, failure.nodeName, failure.deletionTime, failure.virtLauncherPods)
		failures = append(failures, failure)
	}
	Dash.VerifyFatal(t, len(failures) == 0, true, fmt.Sprintf("Found validation failures: %v", failures))
}

// unblockPXUpdate looks for any failed live-migrations during PX update. It waits for some time and then
// stops the VM for the failed live-migration in order to unblock the PX update.
func unblockPXUpdate(t *testing.T, unblockerData *unblockPXUpdateData) {
	log.InfoD("Monitoring failed VMI migrations")
	for {
		unblockerData.Lock()
		if unblockerData.stop {
			unblockerData.Unlock()
			log.InfoD("Done Monitoring failed VMI migrations")
			return
		}
		stopVMsOfFailedMigrations(t, unblockerData.failedMigrations)
		unblockerData.Unlock()
		time.Sleep(5 * time.Second)
	}
}

func stopVMsOfFailedMigrations(t *testing.T, seen map[string]*failedMigration) {
	// stop VMs for the failed migrations after some time
	for _, migr := range seen {
		if !migr.vmStopped && time.Since(migr.firstSeen) > 2*time.Minute {
			stopVMForMigration(t, migr.migration)
			migr.vmStopped = true
		}
	}
	kvCli := kubevirt.Instance().GetKubevirtClient()

	// check for new failed migrations

	// get all migrations created by the operator
	migrs, err := kvCli.VirtualMachineInstanceMigration("").List(&metav1.ListOptions{
		LabelSelector: constants.OperatorLabelManagedByKey + "=" + constants.OperatorLabelManagedByValue,
	})
	log.FailOnError(t, err, "Failed to list migrations created by the operator")
	for _, migr := range migrs.Items {
		if migr.Status.Phase != "Failed" {
			continue
		}
		migr := migr
		key := fmt.Sprintf("%s/%s", migr.Namespace, migr.Name)
		if _, ok := seen[key]; !ok {
			seen[key] = &failedMigration{
				migration: &migr,
				firstSeen: time.Now(),
			}
			log.InfoD("Found failed migration %s on node %s", key,
				migr.Annotations[constants.OperatorPrefix+"/vmi-migration-source-node"])
		}
	}
}

func stopVMForMigration(t *testing.T, migr *kubevirtv1.VirtualMachineInstanceMigration) {
	kvCli := kubevirt.Instance().GetKubevirtClient()
	log.InfoD("Stopping VM %s/%s for failed migration %s", migr.Namespace, migr.Spec.VMIName, migr.Name)
	err := kvCli.VirtualMachine(migr.Namespace).Stop(migr.Spec.VMIName, &kubevirtv1.StopOptions{})
	log.FailOnError(t, err, "Failed to stop VM %s/%s for failed migration %s",
		migr.Namespace, migr.Spec.VMIName, migr.Name)
}

func startEventWatcher(t *testing.T) {
	fn := func(object runtime.Object) error {
		eventWatcherData.Lock()
		defer eventWatcherData.Unlock()
		event, ok := object.(*corev1.Event)
		if !ok {
			err := fmt.Errorf("invalid object type on event watch: %v", object)
			return err
		}
		if event.InvolvedObject.Kind == "StorageNode" || event.InvolvedObject.Kind == "StorageCluster" {
			eventWatcherData.events = append(eventWatcherData.events, *event)
		}
		return nil
	}
	eventWatcherData.Lock()
	defer eventWatcherData.Unlock()
	if eventWatcherData.events == nil {
		eventWatcherData.events = []corev1.Event{}
		log.Info("Watching events in namespace %q", pxNamespace)
		err := core.Instance().WatchEvents(pxNamespace, fn, metav1.ListOptions{})
		log.FailOnError(t, err, "Failed to watch events in namespace %q", pxNamespace)
	}
}

func logAndClearEvents(t *testing.T) {
	if !t.Failed() {
		return
	}
	eventWatcherData.Lock()
	defer func() {
		eventWatcherData.events = []corev1.Event{}
		eventWatcherData.Unlock()
	}()
	// convert eventWatcherData.events to json
	eventsJSON, err := json.Marshal(eventWatcherData.events)
	if err != nil {
		log.Warn("Failed to marshal events to json: %v", err)
		return
	}
	log.Info("Events generated during the test: %s", eventsJSON)
}

func verifyWatcherSawEvent(t *testing.T, reason string, afterTime time.Time) {
	found := false
	eventWatcherData.Lock()
	for _, event := range eventWatcherData.events {
		if event.Reason != reason {
			continue
		}
		var seriesLast time.Time
		if event.Series != nil {
			seriesLast = event.Series.LastObservedTime.Time
		}
		var maxTime time.Time
		for _, t := range []time.Time{event.EventTime.Time, event.FirstTimestamp.Time, event.LastTimestamp.Time,
			event.CreationTimestamp.Time, seriesLast} {
			if t.After(maxTime) {
				maxTime = t
			}
		}
		if maxTime.After(afterTime) {
			log.InfoD("Found %s event: %s", reason, event.Message)
			found = true
			break
		}
	}
	eventWatcherData.Unlock()
	Dash.VerifyFatal(t, found, true, fmt.Sprintf("Did not find %s event after %v", reason, afterTime))
}

func addRemoveTestLabelOnNode(t *testing.T, nodeName string, add bool) {
	for i := 0; i < 100; i++ {
		err := addRemoveTestLabelOnNodeHelper(nodeName, add)
		if err == nil {
			return
		}
		if !errors.IsConflict(err) {
			log.FailOnError(t, err, "addRemoveTestLabelOnNode")
		}
		log.Info("conflict error in addRemoveTestLabelOnNode (add=%v) for node %s in attempt %d", add, nodeName, i)
		time.Sleep(100 * time.Millisecond)
	}
}

func addRemoveTestLabelOnNodeHelper(nodeName string, add bool) error {
	n, err := core.Instance().GetNodeByName(nodeName)
	if err != nil {
		return fmt.Errorf("failed to get k8s node %s: %w", nodeName, err)
	}
	val, ok := n.Labels[pxUpdateTestLabel]
	if (add && val == "true") || (!add && !ok) {
		// update not needed
		return nil
	}
	if add {
		n.Labels[pxUpdateTestLabel] = "true"
	} else {
		delete(n.Labels, pxUpdateTestLabel)
	}
	log.InfoD("Updating (add=%v) label %s on node %s", add, pxUpdateTestLabel, nodeName)
	if _, err = core.Instance().UpdateNode(n); err != nil {
		return fmt.Errorf("failed to update (add=%v) %s label on node %s: %w", add, pxUpdateTestLabel, nodeName, err)
	}
	return nil
}
