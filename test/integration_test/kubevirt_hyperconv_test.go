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
	"unsafe"

	"github.com/libopenstorage/openstorage/api"
	"github.com/portworx/sched-ops/k8s/core"
	kubevirt "github.com/portworx/sched-ops/k8s/kubevirt"
	kubevirtdy "github.com/portworx/sched-ops/k8s/kubevirt-dynamic"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/portworx/torpedo/drivers/volume"
	"github.com/portworx/torpedo/pkg/log"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	kubevirtv1 "kubevirt.io/api/core/v1"
	"kubevirt.io/client-go/kubecli"
	cdiv1beta1 "kubevirt.io/containerized-data-importer-api/pkg/apis/core/v1beta1"
)

const (
	cmdRetry            = 5 * time.Second
	cmdTimeout          = 1 * time.Minute
	mountTypeBind       = "bind"
	mountTypeNFS        = "nfs"
	vpsVolAffinityLabel = "vps.portworx.io/volume-affinity"
)

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
	runID := testrailSetupForTest(testrailID, &testResult)
	defer updateTestRail(&testResult, testrailID, runID)
	instanceID := "one-live-migr"

	ctxs := kubevirtVMScaledDeployAndValidate(
		t,
		instanceID,
		[]string{
			"kubevirt-fedora", "kubevirt-fedora-wait-first-consumer", "kubevirt-fedora-multi-disks-wffc",
			"kubevirt-windows-22k-server", "kubevirt-windows-22k-server-wait-first-consumer",
		},
		kubevirtScale,
	)
	allNodes := node.GetNodesByVoDriverNodeID()

	for _, appCtx := range ctxs {
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
		log.Infof("Waiting for the VM to return to the hyperconverged state again")
		verifyBindMount(t, testState, false /*initialCheck*/)

		// Verify that VM stayed up the whole time
		verifyVMStayedUp(t, testState)
	}
	log.Infof("Destroying apps")
	destroyAndWait(t, ctxs)
	// If we are here then the test has passed
	testResult = testResultPass
	log.Infof("Test status at end of %s test: %s", t.Name(), testResult)
}

// This test simulates OCP upgrade by live-migrating VM to a *replica* node and
// then restarting PX on the node where volume is attached. It expects that the VM
// should end up with a bind-mount (hyperconvergence). PX performs *two*
// back-to-back live migrations in this test.
func kubeVirtHypercTwoLiveMigrations(t *testing.T) {
	var testrailID, testResult = 93197, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult)
	defer updateTestRail(&testResult, testrailID, runID)
	instanceID := "two-live-migr"

	ctxs := kubevirtVMScaledDeployAndValidate(
		t,
		instanceID,
		[]string{
			"kubevirt-fedora", "kubevirt-fedora-wait-first-consumer", "kubevirt-fedora-multi-disks-wffc",
			"kubevirt-windows-22k-server", "kubevirt-windows-22k-server-wait-first-consumer",
		},
		kubevirtScale,
	)
	allNodes := node.GetNodesByVoDriverNodeID()

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
		log.Infof("Waiting for the VM to return to the hyperconverged state again")
		verifyBindMount(t, testState, false /*initialCheck*/)

		// Verify that VM stayed up the whole time
		verifyVMStayedUp(t, testState)

		uncordonFunc()
		cordonedNodes = nil
	}
	log.Infof("Destroying apps")
	destroyAndWait(t, ctxs)
	// If we are here then the test has passed
	testResult = testResultPass
	log.Infof("Test status at end of %s test: %s", t.Name(), testResult)
}

// Add hotplug disks to a running VM and verify that they are collocated.
func kubeVirtHypercHotPlugDiskCollocation(t *testing.T) {
	var testrailID, testResult = 257201, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult)
	defer updateTestRail(&testResult, testrailID, runID)
	instanceID := "hotplug-colo"

	ctxs := kubevirtVMScaledDeployAndValidate(
		t,
		instanceID,
		[]string{
			"kubevirt-fedora", "kubevirt-fedora-wait-first-consumer",
		},
		kubevirtScale,
	)
	allNodes := node.GetNodesByVoDriverNodeID()

	for _, appCtx := range ctxs {
		testState := &kubevirtTestState{
			appCtx:   appCtx,
			allNodes: allNodes,
		}
		gatherInitialVMIInfo(t, testState)
		verifyInitialVMI(t, testState)

		addAndVerifyHotPlugDisks(t, testState)
	}
	log.Infof("Destroying apps")
	destroyAndWait(t, ctxs)
	// If we are here then the test has passed
	testResult = testResultPass
	log.Infof("Test status at end of %s test: %s", t.Name(), testResult)
}

// Deploy VM with a special label on the PVCs to skip adding VPS during vol creation.
// Then, wait for the VPS fix job to collocate the volumes.
func kubeVirtHypercVPSFixJob(t *testing.T) {
	var testrailID, testResult = 257177, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult)
	defer updateTestRail(&testResult, testrailID, runID)
	instanceID := "vps-fix-job"

	ctxs := kubevirtVMScaledDeployAndValidate(
		t,
		instanceID,
		[]string{
			"kubevirt-fedora-no-vps",
		},
		kubevirtScale,
	)
	allNodes := node.GetNodesByVoDriverNodeID()

	for _, appCtx := range ctxs {
		testState := &kubevirtTestState{
			appCtx:   appCtx,
			allNodes: allNodes,
		}
		gatherInitialVMIInfo(t, testState)

		// TODO: need to set the cluster option --fix-vps-frequency-in-minutes to reduce the wait time
		require.Eventuallyf(t, func() bool {
			return checkVMDisksCollocation(testState)
		}, time.Hour, 5*time.Second, "vm disks were not collocated")
	}
	log.Infof("Destroying apps")
	destroyAndWait(t, ctxs)
	// If we are here then the test has passed
	testResult = testResultPass
	log.Infof("Test status at end of %s test: %s", t.Name(), testResult)
}

func gatherInitialVMIInfo(t *testing.T, testState *kubevirtTestState) {
	appCtx := testState.appCtx

	err := schedulerDriver.WaitForRunning(appCtx, defaultWaitTimeout, defaultWaitInterval)
	require.NoError(t, err, "Error waiting for app %s to get to running state", appCtx.App.Key)

	vols, err := schedulerDriver.GetVolumes(appCtx)
	require.NoError(t, err, "failed to get volumes for context %s", appCtx.App.Key)

	logrus.Infof("RK=> Number of volumes for test state %s: %d", testState.vmiName, len(vols))

	for _, vol := range vols {
		vmDisk := &vmDisk{volume: vol}
		testState.vmDisks = append(testState.vmDisks, vmDisk)

		vmDisk.apiVol, err = volumeDriver.InspectVolume(vol.ID)
		require.NoError(t, err, "Failed to inspect PV %s", vol.ID)

		vmDisk.attachedNode, err = volumeDriver.GetNodeForVolume(vol, cmdTimeout, cmdRetry)
		require.NoError(t, err)

		vmDisk.pvcName = vmDisk.apiVol.Locator.VolumeLabels["pvc"]
		require.NotEmpty(t, vmDisk.pvcName)

		pvc, err := core.Instance().GetPersistentVolumeClaim(vmDisk.pvcName, appCtx.App.NameSpace)
		require.NoError(t, err, "Failed to get PVC %s/%s for volume %s of context %s",
			appCtx.App.NameSpace, vmDisk.pvcName, vol.ID, appCtx.App.Key)

		require.NotNil(t, pvc.Spec.StorageClassName)
		require.NotEmpty(t, *pvc.Spec.StorageClassName)
		vmDisk.storageClassName = *pvc.Spec.StorageClassName

		sc, err := core.Instance().GetStorageClassForPVC(pvc)
		require.NoError(t, err, "Failed to get storageClass for PVC %s/%s for volume %s of context %s",
			appCtx.App.NameSpace, vmDisk.pvcName, vol.ID, appCtx.App.Key)

		if sc.VolumeBindingMode != nil && *sc.VolumeBindingMode == storagev1.VolumeBindingWaitForFirstConsumer {
			vmDisk.waitForFirstConsumer = true
		}

		if testState.vmPod == nil {
			testState.vmPod, err = getVMPod(appCtx, vol)
			require.NoError(t, err)
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
		require.NotEmpty(t, vmDisk.diskName)
		log.Infof("%s attached to node %s", vmDisk, vmDisk.attachedNode.Name)
	}

	testState.vmPod, err = getVMPod(testState.appCtx, testState.vmDisks[0].volume)
	require.NoError(t, err)

	testState.vmiName, err = getVMINameFromVMPod(testState.vmPod)
	require.NoError(t, err)

	testState.vmiUID, testState.vmiPhase, testState.vmiPhaseTransitionTime, testState.vmUID, err = getVMIDetails(
		testState.vmPod.Namespace, testState.vmiName)
	require.NoError(t, err)
	require.Equal(t, "Running", testState.vmiPhase)
}

func verifyInitialVMI(t *testing.T, testState *kubevirtTestState) {
	// verify all volumes are using the same set of replica nodes
	require.True(t, checkVMDisksCollocation(testState), "vm disks are not collocated")

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
			log.Warnf("Failed to inspect volume for %s: %v", vmDisk, err)
			return false
		}

		replicaNodeIDs := getReplicaNodeIDs(vmDisk.apiVol)
		if prevReplicaNodeIDs != nil {
			if !matchReplicaNodeIDs(prevReplicaNodeIDs, replicaNodeIDs) {
				log.Warnf("%s and %s have replicas on different nodes", prevDisk, vmDisk)
				return false
			}
		} else {
			prevReplicaNodeIDs = replicaNodeIDs
			prevDisk = vmDisk
		}
		// verify that our vps label is set
		vpsLabelVal := vmDisk.apiVol.Spec.VolumeLabels[vpsVolAffinityLabel]
		if vpsLabelVal == "" {
			log.Warnf("PX volume for %s does not have %s label", vmDisk, vpsVolAffinityLabel)
			return false
		}
		log.Infof("Found label %s=%s on %s", vpsVolAffinityLabel, vpsLabelVal, vmDisk)
		if prevVPSLabelVal != "" && vpsLabelVal != prevVPSLabelVal {
			log.Warnf("VPS label values (%s vs %s) don't match for %s and %s",
				prevVPSLabelVal, vpsLabelVal, prevDisk, vmDisk)
			return false
		}
		prevVPSLabelVal = vpsLabelVal
	}
	return true
}

func getKubevirtClient(t *testing.T) kubecli.KubevirtClient {
	// TODO: use reflect to get kubevirt typed client until sched-ops is vendored into stork.
	// Currently, there are vendoring issues. When those issues are fixed, we can just use the following.
	//
	//	kvCli := kubevirt.Instance().GetKubevirtClient()

	const ptrSize = unsafe.Sizeof(new(int))

	kc := kubevirt.Instance().(*kubevirt.Client)
	_, err := kc.GetVersion() // for initClient()
	if err != nil {
		log.Warnf("kubevirt GetVersion failed: %v", err)
		// continue
	}

	kvCli := *(*kubecli.KubevirtClient)(unsafe.Pointer(uintptr(unsafe.Pointer(kc)) + uintptr(ptrSize)))

	require.NotNil(t, kvCli)
	return kvCli
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

	kvCli := getKubevirtClient(t)

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
	log.Infof("Creating a hotplug data volume %s/%s", ns, dvName)
	_, err := kvCli.CdiClient().CdiV1beta1().DataVolumes(ns).Create(ctx, dv, metav1.CreateOptions{})
	require.NoError(t, err)

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
	log.Infof("Adding hotplug data volume %s/%s to VMI %s", ns, dvName, testState.vmiName)
	err = kvCli.VirtualMachineInstance(ns).AddVolume(ctx, testState.vmiName, opts)
	require.NoError(t, err)

	// wait until the PVC is bound
	var pvc *corev1.PersistentVolumeClaim
	require.Eventuallyf(t, func() bool {
		pvc, err = core.Instance().GetPersistentVolumeClaim(dvName, ns)
		if err != nil {
			log.Warnf("Failed to get PVC for DataVolume %s/%s of context %s: %v",
				ns, appCtx.App.NameSpace, appCtx.App.Key, err)
			return false
		}
		if pvc.Status.Phase != corev1.ClaimBound {
			log.Warnf("Waiting for PVC %s/%s phase to be %s; current value: %s",
				pvc.Namespace, pvc.Name, corev1.ClaimBound, pvc.Status.Phase)
			return false
		}
		log.Infof("PVC %s/%s phase is %s", pvc.Namespace, pvc.Name, pvc.Status.Phase)
		return true
	}, 3*time.Minute, 5*time.Second, "PVC %s/%s did not become bound", ns, dvName)

	hpDisk := &hotPlugDisk{
		pvcName:          pvc.Name,
		pvName:           pvc.Spec.VolumeName,
		storageClassName: *pvc.Spec.StorageClassName,
	}
	hpDisk.apiVol, err = volumeDriver.InspectVolume(hpDisk.pvName)
	require.NoError(t, err, "Failed to inspect PV %s for %s", hpDisk.pvName, hpDisk)
	return hpDisk
}

func verifyHotPlugDisk(t *testing.T, testState *kubevirtTestState, hpDisk *hotPlugDisk, waitForVPSFixJob bool) {
	if !waitForVPSFixJob {
		require.True(t, isHotplugDiskCollocated(testState, hpDisk), "%s was not collocated", hpDisk)
		return
	}
	// TODO: need to set the cluster option --fix-vps-frequency-in-minutes to reduce the wait time
	require.Eventuallyf(t, func() bool {
		return isHotplugDiskCollocated(testState, hpDisk)
	}, time.Hour, 5*time.Second, "%s was not collocated", hpDisk)
}

func isHotplugDiskCollocated(testState *kubevirtTestState, hpDisk *hotPlugDisk) bool {
	var err error

	vmDisk := testState.vmDisks[0]
	vmDiskReplicas := getReplicaNodeIDs(vmDisk.apiVol)
	vmDiskLabelVal := vmDisk.apiVol.Spec.VolumeLabels[vpsVolAffinityLabel]

	// refresh the apiVol to get the current state of the replicas
	hpDisk.apiVol, err = volumeDriver.InspectVolume(hpDisk.pvName)
	if err != nil {
		log.Warnf("Failed to inspect PV %s for %s: %v", hpDisk.pvName, hpDisk, err)
		return false
	}
	hpDiskReplicas := getReplicaNodeIDs(hpDisk.apiVol)
	if !matchReplicaNodeIDs(vmDiskReplicas, hpDiskReplicas) {
		log.Warnf("%s and %s have replicas on different nodes", hpDisk, vmDisk)
		return false
	}
	// verify that our vps label is set
	hpDiskLabelVal := hpDisk.apiVol.Spec.VolumeLabels[vpsVolAffinityLabel]
	if hpDiskLabelVal == "" {
		log.Warnf("PX volume for %s does not have %s label", hpDisk, vpsVolAffinityLabel)
		return false
	}
	if hpDiskLabelVal != vmDiskLabelVal {
		log.Warnf("VPS label value %s for %s doesn't match with the label %s for %s",
			hpDiskLabelVal, hpDisk, vmDiskLabelVal, vmDisk)
		return false
	}
	log.Infof("%s is collocated", hpDisk)
	return true
}

func startAndWaitForVMIMigration(t *testing.T, testState *kubevirtTestState, migrateToReplicaNode bool) {
	ctx := context.TODO()
	vmiNamespace := testState.vmPod.Namespace
	vmiName := testState.vmiName

	// start migration
	migration, err := kubevirtdy.Instance().CreateVirtualMachineInstanceMigration(ctx, vmiNamespace, vmiName)
	require.NoError(t, err)

	// wait for completion
	var migr *kubevirtdy.VirtualMachineInstanceMigration
	require.Eventuallyf(t, func() bool {
		migr, err = kubevirtdy.Instance().GetVirtualMachineInstanceMigration(ctx, vmiNamespace, migration.Name)
		if err != nil {
			log.Warnf("Failed to get migration %s/%s: %v", vmiNamespace, migration.Name, err)
			return false
		}
		if !migr.Completed {
			log.Warnf("VMI migration %s/%s is still not completed", vmiNamespace, migration.Name)
			return false
		}
		// wait until there is only one pod in the running state
		testState.vmPod, err = getVMPod(testState.appCtx, testState.vmDisks[0].volume)
		if err != nil {
			log.Warnf("Failed to get VM pod while waiting for live migration to finish for VMI %s/%s: %v",
				vmiNamespace, vmiName, err)
			return false
		}
		log.Infof("VMI was live migrated to pod %s/%s", vmiNamespace, testState.vmPod.Name)
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
	var oldAttachedNode, newAttachedNode *node.Node
	var firstDisk *vmDisk

	// we expect all volumes to be attached on the same node
	for _, vmDisk := range testState.vmDisks {
		if firstDisk == nil {
			oldAttachedNode = vmDisk.attachedNode
			firstDisk = vmDisk
		} else {
			require.Equal(t, firstDisk.attachedNode.Name, vmDisk.attachedNode.Name,
				"vm disks [%s] and [%s] are attached on different nodes", firstDisk, vmDisk)
		}
	}

	// restart px on the original node
	log.Infof("Restarting volume driver on node %s", oldAttachedNode.Name)
	err = volumeDriver.RestartDriver(*oldAttachedNode, nil)
	require.NoError(t, err)

	for _, vmDisk := range testState.vmDisks {
		var attachedNode *node.Node

		require.Eventuallyf(t, func() bool {
			attachedNode, err = volumeDriver.GetNodeForVolume(vmDisk.volume, cmdTimeout, cmdRetry)
			if err != nil {
				log.Warnf("Failed to get the attached node for %s for context %s: %v",
					vmDisk, testState.appCtx.App.Key, err)
				return false
			}
			log.Infof("New attached node for %s is %s", vmDisk, attachedNode.Name)
			return oldAttachedNode.Name != attachedNode.Name
		}, 5*time.Minute, 30*time.Second, "Attached node did not change from %s for %s",
			oldAttachedNode.Name, vmDisk)

		log.Infof("%s: attachment changed from node %s to node %s after failover",
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

func verifyBindMount(t *testing.T, testState *kubevirtTestState, initialCheck bool) {
	for _, vmDisk := range testState.vmDisks {
		var err error

		isBindMounted := func() bool {
			testState.vmPod, err = getVMPod(testState.appCtx, vmDisk.volume)
			if err != nil {
				// this is expected while the live migration is running since there will be 2 VM pods
				log.Infof("Could not get VM pod for %s for context %s: %v", vmDisk, testState.appCtx.App.Key, err)
				return false
			}
			logrus.Infof("Verifying bind mount for %s", vmDisk)
			mountType, err := getVMDiskMountType(testState.vmPod, vmDisk)
			if err != nil {
				log.Warnf("Failed to get mount type of %s for context %s: %v", vmDisk, testState.appCtx.App.Key, err)
				return false
			}
			if mountType != mountTypeBind {
				if !initialCheck {
					log.Warnf("Waiting for %s for context %s to switch to bind-mount from %q",
						vmDisk, testState.appCtx.App.Key, mountType)
				}
				return false
			}
			return true
		}

		if initialCheck {
			// This is the first time after VM was provisioned. It should be bind-mounted immediately. No need to retry.
			require.True(t, isBindMounted(), "%s was not bind mounted", vmDisk)
			continue
		}

		// Wait for PX to perform additional live migration/s to return the VM to a bind-mounted state.
		require.Eventuallyf(t, isBindMounted, 10*time.Minute, 30*time.Second, "%s did not switch to a bind-mount", vmDisk)
	}
}

func cordonNonReplicaNodes(t *testing.T, vol *api.Volume, allNodes map[string]node.Node) []*node.Node {
	replicaNodeIDs := getReplicaNodeIDs(vol)
	var cordonedNodes []*node.Node

	for nodeID, node := range allNodes {
		if replicaNodeIDs[nodeID] {
			continue
		}
		log.Infof("Cordoning non-replica node %s (%s)", node.Name, nodeID)
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
		log.Infof("Uncordoning node %s", cordonedNode.Name)
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

	vmPod := testState.vmPod
	podNamespacedName := fmt.Sprintf("%s/%s", vmPod.Namespace, vmPod.Name)

	var podNodeID string
	for nodeID, node := range testState.allNodes {
		if vmPod.Spec.NodeName == node.Name {
			podNodeID = nodeID
			break
		}
	}
	require.NotEmpty(t, podNodeID, "could not find nodeID for node %s where pod %s is running",
		vmPod.Spec.NodeName, podNamespacedName)

	for _, vmDisk := range testState.vmDisks {
		logrus.Infof("Checking %s", vmDisk)
		previousAttachedNode := vmDisk.attachedNode

		// verify attached node
		vmDisk.attachedNode, err = volumeDriver.GetNodeForVolume(vmDisk.volume, cmdTimeout, cmdRetry)
		require.NoError(t, err)
		if expectAttachedNodeChanged {
			require.NotEqual(t, previousAttachedNode.Name, vmDisk.attachedNode.Name,
				"attached node did not change for %s", vmDisk)
		} else {
			require.Equal(t, previousAttachedNode.Name, vmDisk.attachedNode.Name,
				"attached node changed for %s", vmDisk)
		}

		// verify replica node
		replicaNodeIDs := getReplicaNodeIDs(vmDisk.apiVol)
		if expectReplicaNode {
			require.True(t, replicaNodeIDs[podNodeID],
				"pod is running on node %s (%s) which is not a replica node for %s",
				vmPod.Spec.NodeName, podNodeID, vmDisk)
		} else {
			require.False(t, replicaNodeIDs[podNodeID],
				"pod is running on node %s (%s) which is a replica node for %s",
				vmPod.Spec.NodeName, podNodeID, vmDisk)
		}

		// verify mount type
		mountType, err := getVMDiskMountType(vmPod, vmDisk)
		require.NoError(t, err)

		if expectBindMount {
			require.Equal(t, mountTypeBind, mountType, "%s was not bind-mounted", vmDisk)
		} else {
			require.Equal(t, mountTypeNFS, mountType, "%s was not nfs-mounted", vmDisk)
		}
		log.Infof("Verified mount type %q for %s", mountType, vmDisk)
	}
}

// getVMIDetails returns VMI UID, phase and time when VMI transitioned to that phase, and ownerVM's UID.
func getVMIDetails(vmiNamespace, vmiName string) (string, string, time.Time, string, error) {
	vmi, err := kubevirtdy.Instance().GetVirtualMachineInstance(context.TODO(), vmiNamespace, vmiName)
	if err != nil {
		return "", "", time.Time{}, "", fmt.Errorf("failed to get VMI for %s/%s", vmiNamespace, vmiName)
	}

	var transitionTime time.Time
	for _, vmiPhaseTransition := range vmi.PhaseTransitions {
		if vmiPhaseTransition.Phase == vmi.Phase && vmiPhaseTransition.TransitionTime.After(transitionTime) {
			transitionTime = vmiPhaseTransition.TransitionTime
		}
	}
	if transitionTime.IsZero() {
		return "", "", time.Time{}, "", fmt.Errorf(
			"failed to determine when VMI %s/%s transitioned to phase %s", vmiNamespace, vmiName, vmi.Phase)
	}
	return vmi.UID, vmi.Phase, transitionTime, vmi.OwnerVMUID, nil
}

// Get mount type (nfs or bind) of the VM disk
func getVMDiskMountType(pod *corev1.Pod, vmDisk *vmDisk) (string, error) {
	podNamespacedName := pod.Namespace + "/" + pod.Name
	log.Infof("Checking the mount type of %s in pod %s", vmDisk, podNamespacedName)

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
			log.Infof("Found %s bind mounted for VM pod %s: %s", vmDisk, podNamespacedName, line)
		}

		if nfsMountRE.MatchString(line) {
			if foundBindMount || foundNFSMount {
				return "", fmt.Errorf("multiple mounts found for %s: %s", vmDisk, output)
			}
			foundNFSMount = true
			log.Infof("Found %s nfs mounted for VM pod %s: %s", vmDisk, podNamespacedName, line)
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
	vmiUIDAfter, vmiPhaseAfter, transitionTimeAfter, _, err := getVMIDetails(testState.vmPod.Namespace, testState.vmiName)
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
