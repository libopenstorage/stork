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
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubevirtv1 "kubevirt.io/api/core/v1"
)

const (
	mountTypeBind = "bind"
	mountTypeNFS  = "nfs"

	kubevirtTemplates                     = "kubevirt-templates"
	kubevirtTemplateNamespace             = "openshift-virtualization-os-images"
	kubevirtCDIStorageConditionAnnotation = "cdi.kubevirt.io/storage.condition.running.reason"
	kubevirtCDIStoragePodPhaseAnnotation  = "cdi.kubevirt.io/storage.pod.phase"
)

var (
	defaultVmMountCheckTimeout       = 15 * time.Minute
	defaultVmMountCheckRetryInterval = 30 * time.Second
	k8sKubevirt                      = kubevirt.Instance()
	importerPodCompletionTimeout     = 30 * time.Minute
	importerPodRetryInterval         = 20 * time.Second
)

// AddDisksToKubevirtVM is a function which takes number of disks to add and adds them to the kubevirt VMs passed (Please provide size in Gi)
func AddDisksToKubevirtVM(virtualMachines []*scheduler.Context, numberOfDisks int, size string) (bool, error) {
	// before adding disks check how many disks are present in the VM
	log.InfoD("create config map")
	CreateConfigMap()

	for _, appCtx := range virtualMachines {
		vms, err := GetAllVMsFromScheduledContexts([]*scheduler.Context{appCtx})
		if err != nil {
			return false, err
		}
		for _, v := range vms {
			t := func() (interface{}, bool, error) {
				diskCountOutput, err := GetNumberOfDisksInVMViaVirtLauncherPod(appCtx)
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
			log.InfoD("Sleep for 30 seconds for vm to come up")
			time.Sleep(30 * time.Second)

			//After adding the pvcs check the number of disks in the VM
			vms, err := GetAllVMsFromScheduledContexts([]*scheduler.Context{appCtx})
			if err != nil {
				return false, err
			}
			for _, v := range vms {
				t = func() (interface{}, bool, error) {
					diskCountOutput, err := GetNumberOfDisksInVMViaVirtLauncherPod(appCtx)
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

// RunCmdInVirtLauncherPod runs a command in the virt-launcher pod of the VM
func RunCmdInVirtLauncherPod(virtualMachineCtx *scheduler.Context, cmd []string) (string, error) {
	vols, err := Inst().S.GetVolumes(virtualMachineCtx)
	if err != nil {
		return "", err
	}

	// Check if the pod is in the 'Running' state before executing the command
	_, err = task.DoRetryWithTimeout(func() (interface{}, bool, error) {
		vmPod, err := GetVirtLauncherPodForVM(virtualMachineCtx, vols[0])
		if err != nil {
			return nil, true, fmt.Errorf("failed to get pod: %s", err)
		}
		if vmPod.Status.Phase != corev1.PodRunning {
			return nil, true, fmt.Errorf("pod %s is not in running state, current state: %s", vmPod.Name, vmPod.Status.Phase)
		}
		return nil, false, nil
	}, 10*time.Minute, 10*time.Second)

	vmPod, err := GetVirtLauncherPodForVM(virtualMachineCtx, vols[0])
	if err != nil {
		return "", err
	}

	output, err := core.Instance().RunCommandInPod(cmd, vmPod.Name, "compute", vmPod.Namespace)
	if err != nil {
		return "", err
	}
	log.InfoD("Output of command: %s", output)
	return core.Instance().RunCommandInPod(cmd, vmPod.Name, "compute", vmPod.Namespace)

}

// GetNumberOfDisksInVMViaVirtLauncherPod gets the number of disks in the VM via the virt-launcher pod
func GetNumberOfDisksInVMViaVirtLauncherPod(virtualMachineCtx *scheduler.Context) (int, error) {
	cmd := []string{"lsblk"}

	t := func() (interface{}, bool, error) {
		output, err := RunCmdInVirtLauncherPod(virtualMachineCtx, cmd)
		log.InfoD("Output of command: %s", output)
		if err != nil {
			return 0, false, err
		}
		// Splitting the output into lines
		lines := strings.Split(output, "\n")

		// Count of lines containing "/run/kubevirt-private/vmi-disks/" and "pxd"
		numberOfDisks := 0

		// Loop through each line
		for _, line := range lines {
			// Check if the line contains "/run/kubevirt-private/vmi-disks/" and "pxd"
			fmt.Println(line)
			if strings.Contains(line, "/run/kubevirt-private/vmi-disks/") && strings.Contains(line, "pxd") {
				// Increment count if both conditions are met
				numberOfDisks++
			}
		}

		if err != nil {
			return 0, false, err
		}
		if numberOfDisks == 0 {
			return 0, true, nil
		}
		return numberOfDisks, numberOfDisks == 0, nil
	}
	d, err := task.DoRetryWithTimeout(t, 10*time.Minute, 30*time.Second)
	if err != nil {
		return 0, err
	}
	return d.(int), nil
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
		if !(migr.Phase == "Succeeded") {
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
	var vmPod *corev1.Pod
	for _, vol := range vols {
		if vmPod == nil {
			vmPod, _ = GetVirtLauncherPodForVM(virtualMachineCtx, vol)
		}
		// Commenting this code till PWX-36842 is not fixed
		err := IsVolumeBindMounted(virtualMachineCtx, vmNodeName, vol, wait, vmPod)
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
func IsVolumeBindMounted(virtualMachineCtx *scheduler.Context, vmNodeName string, vol *volume.Volume, wait bool, vmPod *corev1.Pod) error {
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
		if vmPod == nil {
			vmPod, err = GetVirtLauncherPodForVM(virtualMachineCtx, vol)
			if err != nil {
				// this is expected while the live migration is running since there will be 2 VM pods
				log.Infof("Could not get VM pod for %s for context %s: %v", vol.Name, virtualMachineCtx.App.Key, err)
				return false, false, nil
			}
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

// CreateConfigMap creates configmap for vm creds
func CreateConfigMap() error {
	// Check if a config map named kubevirt-creds exist
	configMap, err := k8sCore.GetConfigMap("kubevirt-creds", "default")
	log.Infof("configMap: %v", configMap)
	log.Infof("err: %v", err)
	if err != nil {
		if errors.IsNotFound(err) {
			// ConfigMap does not exist, so create it
			configMap = &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "kubevirt-creds",
					Namespace: "default",
				},
				Data: map[string]string{
					"fio-vm-multi-disk": "ubuntu",
				},
			}
			_, err = k8sCore.CreateConfigMap(configMap)
			if err != nil {
				log.Infof("Failed to create config map kubevirt-creds: %v", err)
				return err
			}
			log.Infof("Created config map kubevirt-creds")
		} else {
			// An unexpected error occurred when retrieving the ConfigMap
			log.Infof("Failed to retrieve config map kubevirt-creds: %v", err)
			return err
		}
	} else {
		log.Infof("Config map kubevirt-creds already exists")
	}
	return nil
}

// WriteFilesAndStoreMD5InVM write few files in the VM and calculates the md5sum
func WriteFilesAndStoreMD5InVM(virtualMachines []*scheduler.Context, namespace string, fileCount int, maxFileSize int) error {
	log.Infof("Creating %d files and storing their MD5 checksums in namespace %s", fileCount, namespace)
	createFilesCmd := fmt.Sprintf("mkdir -p ~/testfiles && cd ~/testfiles && "+
		"rm -f ~/file_checksums.md5 && for i in $(seq 1 %d); do "+
		"head -c $(($RANDOM %% %d)) </dev/urandom >file$i; md5sum file$i >> ~/file_checksums.md5; done", fileCount, maxFileSize)
	for _, appCtx := range virtualMachines {
		vms, err := GetAllVMsFromScheduledContexts([]*scheduler.Context{appCtx})
		if err != nil {
			return err
		}
		for _, v := range vms {
			_, err := RunCmdInVM(v, createFilesCmd, context1.TODO())
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// ValidateFileIntegrityInVM validates the md5sum of files that we wrote in the VM
func ValidateFileIntegrityInVM(virtualMachines []*scheduler.Context, namespace string) error {
	log.Infof("Validating file integrity in namespace %s", namespace)
	validateFilesCmd := "cd ~/testfiles && md5sum -c ~/file_checksums.md5"
	for _, appCtx := range virtualMachines {
		vms, err := GetAllVMsFromScheduledContexts([]*scheduler.Context{appCtx})
		if err != nil {
			return err
		}
		for _, v := range vms {
			_, err := RunCmdInVM(v, validateFilesCmd, context1.TODO())
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// ListEvents lists all events in a namespace in logs.
func ListEvents(namespace string) error {
	eventList, err := k8sCore.ListEvents(namespace, metav1.ListOptions{})
	if err != nil {
		log.Infof("Failed to list events in namespace %s: %v", namespace, err)
		return err
	}
	log.Infof("Events in namespace %s:", namespace)
	for _, event := range eventList.Items {
		log.Infof("Time: %v, Event: %s, Type: %s, Reason: %s, Object: %s/%s, Message: %s",
			event.FirstTimestamp, event.Name, event.Type, event.Reason, event.InvolvedObject.Kind, event.InvolvedObject.Name, event.Message)
	}
	return nil
}

// HotAddPVCsToKubevirtVM hot adds disk to a running VM
func HotAddPVCsToKubevirtVM(virtualMachines []*scheduler.Context, numberOfDisks int, size string) error {
	for _, appCtx := range virtualMachines {
		vms, err := GetAllVMsFromScheduledContexts([]*scheduler.Context{appCtx})
		if err != nil {
			return fmt.Errorf("failed to get VMs from context: %w", err)
		}
		for _, v := range vms {
			diskCountOutput, err := GetNumberOfDisksInVMViaVirtLauncherPod(appCtx)
			if err != nil {
				return fmt.Errorf("failed to get number of disks in VM [%s] in namespace [%s]: %w", v.Name, v.Namespace, err)
			}
			log.Infof("Currently having %v number of disks in the VM", diskCountOutput)
			storageClass, err := GetStorageClassOfVmPVC(appCtx)
			if err != nil {
				return fmt.Errorf("failed to get storage class for VM [%s]: %w", v.Name, err)
			}
			pvcs, err := CreatePVCsForVM(v, numberOfDisks, storageClass, size)
			if err != nil {
				return fmt.Errorf("failed to create PVCs for VM [%s]: %w", v.Name, err)
			}

			vm, err := k8sKubevirt.GetVirtualMachine(v.Name, v.Namespace)
			if err != nil {
				return fmt.Errorf("failed to get VM [%s]: %w", v.Name, err)
			}

			for _, pvc := range pvcs {
				diskName := fmt.Sprintf("disk-%s", pvc.Name)
				// Appending the disk and volume definitions to the VM spec
				vm.Spec.Template.Spec.Domain.Devices.Disks = append(vm.Spec.Template.Spec.Domain.Devices.Disks, kubevirtv1.Disk{
					Name: diskName,
					DiskDevice: kubevirtv1.DiskDevice{
						Disk: &kubevirtv1.DiskTarget{
							Bus: "virtio",
						},
					},
				})

				vm.Spec.Template.Spec.Volumes = append(vm.Spec.Template.Spec.Volumes, kubevirtv1.Volume{
					Name: diskName,
					VolumeSource: kubevirtv1.VolumeSource{
						PersistentVolumeClaim: &kubevirtv1.PersistentVolumeClaimVolumeSource{
							PersistentVolumeClaimVolumeSource: corev1.PersistentVolumeClaimVolumeSource{
								ClaimName: pvc.Name,
							},
							Hotpluggable: true,
						},
					},
				})
			}

			_, err = k8sKubevirt.UpdateVirtualMachine(vm)
			if err != nil {
				return fmt.Errorf("failed to update VM [%s]: %w", vm.Name, err)
			}

			err = RestartKubevirtVM(v.Name, v.Namespace, true)
			if err != nil {
				return err
			}
			log.InfoD("Sleep for 5mins for vm to come up")
			time.Sleep(5 * time.Minute)
			NewDiskCountOutput, err := GetNumberOfDisksInVMViaVirtLauncherPod(appCtx)
			if err != nil {
				return fmt.Errorf("failed to get number of disks in VM [%s] in namespace [%s]", v.Name, v.Namespace)
			}
			if NewDiskCountOutput == diskCountOutput {
				log.Infof("Disk successfully added")
			} else {
				return fmt.Errorf("Disk cannot be added.")
			}
		}
	}
	return nil
}
func DeployVMTemplatesAndValidate() error {
	_, err := Inst().S.Schedule("",
		scheduler.ScheduleOptions{
			AppKeys:   []string{kubevirtTemplates},
			Namespace: kubevirtTemplateNamespace,
		})

	// if new templates are deployed, this function will wait for them to get imported else it will exit
	waitForCompletedAnnotations := func() (interface{}, bool, error) {
		// Loop through all PVCs and check for annotations that signify existing downloaded templates
		pvcTemplates, err := core.Instance().GetPersistentVolumeClaims(kubevirtTemplateNamespace, nil)
		if err != nil {
			return nil, true, fmt.Errorf("failed to get any PVCs in namespace: %s. Retrying.", kubevirtTemplateNamespace)
		}
		for _, pvc := range pvcTemplates.Items {
			if pvc.ObjectMeta.Annotations[kubevirtCDIStorageConditionAnnotation] != "Completed" {
				return nil, true, fmt.Errorf("storage condition is not completed on pvc %s. Status: %s. Retrying.",
					pvc.Name, pvc.ObjectMeta.Annotations[kubevirtCDIStorageConditionAnnotation])
			}
			if pvc.ObjectMeta.Annotations[kubevirtCDIStoragePodPhaseAnnotation] != "Succeeded" {
				return nil, true, fmt.Errorf("pod phase has not succeeded on pvc %s. Phase: %s. Retrying.",
					pvc.Name, pvc.ObjectMeta.Annotations[kubevirtCDIStoragePodPhaseAnnotation])
			}
		}
		log.Infof("All templates are downloaded.")
		return "", false, nil
	}
	_, err = task.DoRetryWithTimeout(waitForCompletedAnnotations, importerPodCompletionTimeout, importerPodRetryInterval)
	return err
}
