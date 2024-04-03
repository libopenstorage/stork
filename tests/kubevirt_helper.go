package tests

import (
	context1 "context"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/libopenstorage/openstorage/api"
	"github.com/portworx/sched-ops/k8s/core"
	"github.com/portworx/sched-ops/k8s/kubevirt"
	"github.com/portworx/sched-ops/task"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/portworx/torpedo/drivers/volume"
	"github.com/portworx/torpedo/pkg/log"

	kubevirtdy "github.com/portworx/sched-ops/k8s/kubevirt-dynamic"
	corev1 "k8s.io/api/core/v1"
	kubevirtv1 "kubevirt.io/api/core/v1"
)

const (
	mountTypeBind = "bind"
	mountTypeNFS  = "nfs"
)

var (
	defaultVmMountCheckTimeout       = 15 * time.Minute
	defaultVmMountCheckRetryInterval = 30 * time.Second
)

// AddDisksToKubevirtVM is a function which takes number of disks to add and adds them to the kubevirt VMs passed (Please provide size in Gi)
func AddDisksToKubevirtVM(virtualMachines []*scheduler.Context, numberOfDisks int, size string) (bool, error) {
	// before adding disks check how many disks are present in the VM
	for _, appCtx := range virtualMachines {
		vms, err := GetAllVMsFromScheduledContexts([]*scheduler.Context{appCtx})
		if err != nil {
			return false, err
		}
		for _, v := range vms {
			t := func() (interface{}, bool, error) {
				diskCountOutput, err := GetNumberOfDisksInVM(v)
				if err != nil {
					return nil, false, fmt.Errorf("failed to get number of disks in VM [%s] in namespace [%s]", v.Name, v.Namespace)
				}
				// Total disks will be numberOfVolumes plus the container disk
				if diskCountOutput == 0 {
					return nil, true, fmt.Errorf("expected number of disks in VM [%s] in namespace [%s] is [%d] but got [%d]", v.Name, v.Namespace, numberOfDisks+1, diskCountOutput)
				}
				return diskCountOutput, false, nil
			}
			d, err := task.DoRetryWithTimeout(t, 10*time.Minute, 30*time.Second)
			if err != nil {
				return false, err
			}
			diskCount := d.(int)
			log.InfoD("Number of disks in VM [%s] in namespace [%s] is [%d]", v.Name, v.Namespace, diskCount)

			// Before we add the pvc we need to get storage class of the pvc
			storageClass, err := GetStorageClassOfVmPVC(appCtx)
			if err != nil {
				return false, err
			}
			log.InfoD("Storage class of PVC attached to VM [%s] in namespace [%s] is [%s]", v.Name, v.Namespace, storageClass)

			// Add the disks to the VM
			pvcs, err := CreatePVCsForVM(v, 1, storageClass, size)

			if err != nil {
				return false, err
			}

			specListInterfaces := make([]interface{}, len(pvcs))
			for i, pvc := range pvcs {
				// Converting each PVC to interface for appending to SpecList
				specListInterfaces[i] = pvc
			}
			appCtx.App.SpecList = append(appCtx.App.SpecList, specListInterfaces...)

			err = AddPVCsToVirtualMachine(v, pvcs)
			if err != nil {
				return false, err
			}

			err = RestartKubevirtVM(v.Name, v.Namespace, true)
			if err != nil {
				return false, err
			}
			log.InfoD("Sleep for 5mins for vm to come up")
			time.Sleep(5 * time.Minute)

			//After adding the pvcs check the number of disks in the VM
			vms, err := GetAllVMsFromScheduledContexts([]*scheduler.Context{appCtx})
			if err != nil {
				return false, err
			}
			for _, v := range vms {
				t = func() (interface{}, bool, error) {
					diskCountOutput, err := GetNumberOfDisksInVM(v)
					if err != nil {
						return nil, false, fmt.Errorf("failed to get number of disks in VM [%s] in namespace [%s]", v.Name, v.Namespace)
					}
					// Total disks will be numberOfVolumes plus the container disk
					if diskCountOutput != numberOfDisks+diskCount {
						return nil, true, fmt.Errorf("expected number of disks in VM [%s] in namespace [%s] is [%d] but got [%d]", v.Name, v.Namespace, numberOfDisks+1, diskCountOutput)
					}
					return diskCountOutput, false, nil
				}
				d, err = task.DoRetryWithTimeout(t, 10*time.Minute, 30*time.Second)
				if err != nil {
					return false, err
				}
				if diskCount == d.(int) {
					return false, fmt.Errorf("number of disks in VM [%s] in namespace [%s] is same as before adding disks", v.Name, v.Namespace)
				}
				diskCount = d.(int)
				log.InfoD("Number of disks in VM [%s] in namespace [%s] is [%d]", v.Name, v.Namespace, diskCount)

			}
		}
	}
	return true, nil
}

// GetStorageClassOfVmPVC returns the storage class of pvc attached to the VM
func GetStorageClassOfVmPVC(vm *scheduler.Context) (string, error) {
	// Get the PVC object from the VM
	nameSpace := vm.App.NameSpace
	pvcs, err := core.Instance().GetPersistentVolumeClaims(nameSpace, nil)
	if err != nil {
		return "", err
	}

	// Get the PVCs attached to the VM
	for _, pvc := range pvcs.Items {
		ScName, err := core.Instance().GetStorageClassForPVC(&pvc)
		if err != nil {
			return "", err
		}
		return ScName.Name, nil
	}
	return "", fmt.Errorf("failed to get storage class of PVC attached to VM [%s] in namespace [%s]", vm.App.Key, vm.App.NameSpace)
}

// StartAndWaitForVMIMigration starts the VM migration and waits for the VM to be in running state in the new node
func StartAndWaitForVMIMigration(virtualMachineCtx *scheduler.Context, ctx context1.Context) error {

	vms, err := GetAllVMsFromScheduledContexts([]*scheduler.Context{virtualMachineCtx})
	if err != nil {
		return err
	}

	// Get the namespace and name of the VM
	if len(vms) == 0 {
		return fmt.Errorf("no VMs found for VM [%s] in namespace [%s]", virtualMachineCtx.App.Key, virtualMachineCtx.App.NameSpace)
	}
	vmiNamespace := vms[0].Namespace
	vmiName := vms[0].Name
	if len(vms) > 1 {
		return fmt.Errorf("more than 1 VMs found for VM [%s] in namespace [%s]", virtualMachineCtx.App.Key, virtualMachineCtx.App.NameSpace)
	}

	//Get the node where the vm is scheduled before the migration
	nodeName, err := GetNodeOfVM(vms[0])
	if err != nil {
		return err
	}
	log.Infof("VM [%s] in namespace [%s] is scheduled on node [%s]", vmiName, vmiNamespace, nodeName)

	// Start the VM migration
	migration, err := kubevirtdy.Instance().CreateVirtualMachineInstanceMigration(ctx, vmiNamespace, vmiName)
	if err != nil {
		return err
	}
	log.Infof("VM migration created for VM [%s] in namespace [%s]", vmiName, vmiNamespace)

	// wait for completion
	var migr *kubevirtdy.VirtualMachineInstanceMigration

	// get volumes from app context
	vols, err := Inst().S.GetVolumes(virtualMachineCtx)
	if err != nil {
		return err
	}

	t := func() (interface{}, bool, error) {
		migr, err = kubevirtdy.Instance().GetVirtualMachineInstanceMigration(ctx, vmiNamespace, migration.Name)
		if err != nil {
			return "", false, fmt.Errorf("failed to get migration for VM [%s] in namespace [%s]", vmiName, vmiNamespace)
		}
		if !migr.Completed {
			return "", true, fmt.Errorf("waiting for migration to complete for VM [%s] in namespace [%s]", vmiName, vmiNamespace)
		}

		// wait until there is only one pod in the running state
		//TODO https://purestorage.atlassian.net/browse/PTX-23166 - This is a temporary fix to get the pod of the VM
		testPod, err := GetVirtLauncherPodForVM(virtualMachineCtx, vols[0])
		if err != nil {
			return "", true, err
		}

		//Get the node where the vm is scheduled after the migration
		nodeNameAfterMigration := testPod.Spec.NodeName

		if nodeName == nodeNameAfterMigration {
			return "", false, fmt.Errorf("VM pod live migrated [%s] in namespace [%s] but is still on the same node [%s]", testPod.Name, testPod.Namespace, nodeName)
		}
		log.InfoD("VM pod live migrated to node: [%s]", nodeNameAfterMigration)
		return "", false, nil
	}
	_, err = task.DoRetryWithTimeout(t, defaultVmMountCheckTimeout, defaultVmMountCheckRetryInterval)
	if err != nil {
		return err
	}
	return nil
}

// GetVirtLauncherPodForVM returns the virt-launcher pod for the VM
func GetVirtLauncherPodForVM(virtualMachineCtx *scheduler.Context, vol *volume.Volume) (*corev1.Pod, error) {
	pods, err := core.Instance().GetPodsUsingPV(vol.ID)
	if err != nil {
		return nil, fmt.Errorf("failed to get pods for volume %s of context %s: %w", vol.ID, virtualMachineCtx.App.Key, err)
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

// IsVMBindMounted checks if the volumes are bind mounted to the VM
func IsVMBindMounted(virtualMachineCtx *scheduler.Context, wait bool) (bool, error) {
	vols, err := Inst().S.GetVolumes(virtualMachineCtx)
	if err != nil {
		return false, err
	}

	// Get the node where pod is scheduled
	vms, err := GetAllVMsFromScheduledContexts([]*scheduler.Context{virtualMachineCtx})
	if err != nil {
		return false, err
	}
	// TODO Add support for multiple vm per spec/Context : https://purestorage.atlassian.net/browse/PTX-23167
	if len(vms) != 1 {
		return false, fmt.Errorf("expected 1 VM for VM [%s] in namespace [%s] but got [%d]", virtualMachineCtx.App.Key, virtualMachineCtx.App.NameSpace, len(vms))
	}

	vm := vms[0]
	vmNodeName, err := GetNodeOfVM(vm)
	if err != nil {
		return false, err
	}
	log.Infof("VM [%s] is deployed on node [%s]", virtualMachineCtx.App.Key, vmNodeName)

	// Keep a track of replicaset and consider this as the source of truth
	volInspect, err := Inst().V.InspectVolume(vols[0].ID)
	if err != nil {
		return false, fmt.Errorf("failed to inspect volume [%s]: %w", vols[0].ID, err)
	}
	globalReplicSet := volInspect.ReplicaSets
	log.InfoD("Length of replicaset: %d", len(globalReplicSet))

	// The criteria to call the bind mount successful is to check if the replicaset of all volumes should be same and should be locally attached to the node
	// check if the replicaset values is same as the global replicaset
	// Here we are considering globalreplicaset to be source of truth for comparison

	for _, vol := range vols {
		err := IsVolumeBindMounted(virtualMachineCtx, vmNodeName, vol, wait)
		if err != nil {
			return false, err
		}
		err = AreVolumeReplicasCollocated(vol, globalReplicSet)
		if err != nil {
			return false, err
		}
	}
	log.Infof("Successfully verified bind mount for VM [%s] in namespace [%s]", virtualMachineCtx.App.Key, virtualMachineCtx.App.NameSpace)
	return true, nil
}

// GetNodeOfVM returns nodename on which VM is running
func GetNodeOfVM(virtualMachineCtx kubevirtv1.VirtualMachine) (string, error) {
	vmi, err := kubevirt.Instance().GetVirtualMachineInstance(context1.TODO(), virtualMachineCtx.Name, virtualMachineCtx.Namespace)
	if err != nil {
		return "", err
	}
	log.InfoD("NodeName: %s", vmi.Status.NodeName)
	return vmi.Status.NodeName, nil
}

// ReplicaSetsMatch verifies if the replicaset nodes are present in global replicaset
func ReplicaSetsMatch(replicaset []*api.ReplicaSet, globalReplicSet []*api.ReplicaSet) error {
	// Considering aggregation is 1
	if len(replicaset[0].Nodes) != len(globalReplicSet[0].Nodes) {
		return fmt.Errorf("the number of nodes in the replicaset is not same as the global replicaset")
	}

	replicasetNodes := make(map[string]bool)

	for _, rs := range replicaset {
		for _, rsNode := range rs.Nodes {
			replicasetNodes[rsNode] = true
		}
	}

	for _, grs := range globalReplicSet {
		for _, grsNode := range grs.Nodes {
			if _, ok := replicasetNodes[grsNode]; !ok {
				return fmt.Errorf("replicaset mismatch node not found in global replicaset")
			}
		}
	}
	log.Infof("Replicaset matches with global replicaset")
	return nil
}

// getVMDiskMountType Gets mount type (nfs or bind) of the VM disk
func getVMDiskMountType(pod *corev1.Pod, vmDisk *volume.Volume, diskName string) (string, error) {
	podNamespacedName := pod.Namespace + "/" + pod.Name
	log.Infof("Checking the mount type of %s in pod %s", vmDisk, podNamespacedName)

	// Sample output if the volume is bind-mounted: (vmDisk.diskName is "rootdisk" in this example)
	// $ kubectl exec -it virt-launcher-fedora-communist-toucan-jfw7n -- mount
	// ...
	// /dev/pxd/pxd365793461222635857 on /run/kubevirt-private/vmi-disks/rootdisk type ext4 (rw,relatime,seclabel,discard)
	// ...
	volInspect, err := Inst().V.InspectVolume(vmDisk.ID)
	if err != nil {
		return "", fmt.Errorf("failed to inspect volume %s: %v", vmDisk.ID, err)
	}

	bindMountRE := regexp.MustCompile(fmt.Sprintf("/dev/pxd/pxd%s on .*%s type (ext4|xfs)",
		volInspect.Id, diskName))

	// Sample output if the volume is nfs-mounted: (vmDisk.diskName is "rootdisk" in this example)
	// $ kubectl exec -it virt-launcher-fedora-communist-toucan-bqcrp -- mount
	// ...
	// 172.30.194.11:/var/lib/osd/pxns/365793461222635857 on /run/kubevirt-private/vmi-disks/rootdisk type nfs (...)
	// ...
	nfsMountRE := regexp.MustCompile(fmt.Sprintf(":/var/lib/osd/pxns/%s on .*%s type nfs",
		volInspect.Id, diskName))

	cmd := []string{"mount"}
	output, err := core.Instance().RunCommandInPod(cmd, pod.Name, "compute", pod.Namespace)
	if err != nil {
		return "", fmt.Errorf("failed to run command %v inside the pod %s, error: %v", cmd, podNamespacedName, err)
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

// IsVolumeBindMounted verifies if the volume is bind mounted on the VM pod or not
func IsVolumeBindMounted(virtualMachineCtx *scheduler.Context, vmNodeName string, vol *volume.Volume, wait bool) error {
	volInspect, err := Inst().V.InspectVolume(vol.ID)
	if err != nil {
		return fmt.Errorf("failed to inspect volume [%s]: %w", vol.ID, err)
	}
	nodeIpAttachedOn := volInspect.AttachedOn
	nodeNameAttachedOn, err := node.GetNodeByIP(nodeIpAttachedOn)
	if err != nil {
		return fmt.Errorf("failed to get node name by IP [%s]: %w", nodeIpAttachedOn, err)
	}
	log.Infof("Volume [%s] is attached on node [%s]", vol.ID, nodeNameAttachedOn.Name)
	if nodeNameAttachedOn.Name != vmNodeName {
		return fmt.Errorf("volume [%s] is attached on node [%s] instead of node [%s]", vol.ID, nodeNameAttachedOn.Name, vmNodeName)
	}

	isBindMounted := false
	t := func() (interface{}, bool, error) {
		vmPod, err := GetVirtLauncherPodForVM(virtualMachineCtx, vol)
		if err != nil {
			// this is expected while the live migration is running since there will be 2 VM pods
			log.Infof("Could not get VM pod for %s for context %s: %v", vol.Name, virtualMachineCtx.App.Key, err)
			return false, false, nil
		}
		log.Infof("Verifying bind mount for %s", vol)
		diskName := ""
		for _, vmVol := range vmPod.Spec.Volumes {
			pvcName := volInspect.Locator.VolumeLabels["pvc"]
			if vmVol.PersistentVolumeClaim != nil && vmVol.PersistentVolumeClaim.ClaimName == pvcName {
				diskName = vmVol.Name
				break
			}
		}
		mountType, err := getVMDiskMountType(vmPod, vol, diskName)
		if err != nil {
			log.Warnf("Failed to get mount type of %s for context %s: %v", vol, virtualMachineCtx.App.Key, err)
			return false, false, nil
		}
		log.Infof("Mount type of %s for context %s: %s", vol.Name, virtualMachineCtx.App.Key, mountType)
		if mountType != mountTypeBind {
			if wait {
				log.Warnf("Waiting for %s for context %s to switch to bind-mount from %q",
					vol.Name, virtualMachineCtx.App.Key, mountType)
			}
			return false, false, nil
		}
		isBindMounted = true
		return true, false, nil
	}
	if !wait {
		// initial check is done only once
		_, _, err := t()
		if err != nil {
			return err
		}
		if !isBindMounted {
			return fmt.Errorf("volume [%s] is not bind mounted", vol.ID)
		}
	} else {
		_, err = task.DoRetryWithTimeout(t, defaultVmMountCheckTimeout, defaultVmMountCheckRetryInterval)
		if err != nil {
			return err
		}
	}
	return nil
}

// AreVolumeReplicasCollocated verifies if the volume replicas are collocated on the same set of nodes
func AreVolumeReplicasCollocated(vol *volume.Volume, globalReplicSet []*api.ReplicaSet) error {
	// Check if volumes have replicas on the same set of nodes
	volInspect, err := Inst().V.InspectVolume(vol.ID)
	if err != nil {
		return fmt.Errorf("failed to inspect volume [%s]: %w", vol.ID, err)
	}

	replicaset := volInspect.ReplicaSets

	// check if the replicaset size is same as the global replicaset size
	if len(replicaset) != len(globalReplicSet) {
		return fmt.Errorf("replicaset count mismatch for volume [%s] and for volume [%s]", vol.ID, volInspect.Id)
	}
	err = ReplicaSetsMatch(replicaset, globalReplicSet)
	if err != nil {
		return fmt.Errorf("replicaset mismatch for volume [%s] and volume [%s]", vol.ID, volInspect.Id)
	}
	return nil
}
