package kubevirt

import (
	context1 "context"
	"fmt"
	"github.com/onsi/ginkgo/v2"
	"github.com/portworx/sched-ops/k8s/core"
	"github.com/portworx/sched-ops/k8s/kubevirt"
	"github.com/portworx/sched-ops/task"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/portworx/torpedo/pkg/log"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubevirtv1 "kubevirt.io/api/core/v1"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	sshPodName      = "ssh-pod"
	sshPodNamespace = "ssh-pod-namespace"
)

var (
	defaultCmdTimeout       = 5 * time.Minute
	defaultCmdRetryInterval = 30 * time.Second
	vmStartStopTimeout      = 5 * time.Minute
	vmStartStopRetryTime    = 30 * time.Second
)

// AddDisksToKubevirtVM is a function which takes number of disks to add and adds them to the kubevirt VMs passed (Please provide size in Gi)
func AddDisksToKubevirtVM(virtualMachines []*scheduler.Context, numberOfDisks int, size string, driver node.Driver) (bool, error) {
	// before adding disks check how many disks are present in the VM
	for _, appCtx := range virtualMachines {
		vms, err := GetAllVMsFromScheduledContexts([]*scheduler.Context{appCtx})
		if err != nil {
			return false, err
		}
		for _, v := range vms {
			t := func() (interface{}, bool, error) {
				diskCountOutput, err := GetNumberOfDisksInVM(v, driver)
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
					diskCountOutput, err := GetNumberOfDisksInVM(v, driver)
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

// GetAllVMsFromScheduledContexts returns all the Kubevirt VMs in the scheduled contexts
func GetAllVMsFromScheduledContexts(scheduledContexts []*scheduler.Context) ([]kubevirtv1.VirtualMachine, error) {
	var vms []kubevirtv1.VirtualMachine

	// Get only unique namespaces
	uniqueNamespaces := make(map[string]bool)
	for _, scheduledContext := range scheduledContexts {
		uniqueNamespaces[scheduledContext.ScheduleOptions.Namespace] = true
	}
	namespaces := make([]string, 0, len(uniqueNamespaces))
	for namespace := range uniqueNamespaces {
		namespaces = append(namespaces, namespace)
	}

	// Get VMs from the unique namespaces
	for _, n := range namespaces {
		vmList, err := GetAllVMsInNamespace(n)
		if err != nil {
			return nil, err
		}
		vms = append(vms, vmList...)
	}
	return vms, nil
}

// GetAllVMsInNamespace returns all the Kubevirt VMs in the given namespace
func GetAllVMsInNamespace(namespace string) ([]kubevirtv1.VirtualMachine, error) {
	k8sKubevirt := kubevirt.Instance()
	vms, err := k8sKubevirt.ListVirtualMachines(namespace)
	if err != nil {
		return nil, err
	}
	return vms.Items, nil

}

// GetNumberOfDisksInVM returns the number of disks in the VM
func GetNumberOfDisksInVM(vm kubevirtv1.VirtualMachine, n node.Driver) (int, error) {
	cmdForDiskCount := "lsblk -d | grep disk | wc -l"
	diskCountOutput, err := RunCmdInVM(vm, cmdForDiskCount, context1.TODO(), n)
	if err != nil {
		return 0, fmt.Errorf("failed to get disk count in VM: %v", err)
	}
	// trim diskCountOutput and convert to int
	diskCount, err := strconv.Atoi(strings.TrimSpace(diskCountOutput))
	if err != nil {
		return 0, fmt.Errorf("failed to convert disk count to int: %v", err)
	}
	return diskCount, nil
}

// RunCmdInVM runs a command in the VM by SSHing into it
func RunCmdInVM(vm kubevirtv1.VirtualMachine, cmd string, ctx context1.Context, driver node.Driver) (string, error) {
	var username string
	var password string
	// Kubevirt client
	k8sKubevirt := kubevirt.Instance()
	k8sCore := core.Instance()

	// Getting IP of the VM
	t := func() (interface{}, bool, error) {
		vmInstance, err := k8sKubevirt.GetVirtualMachineInstance(ctx, vm.Name, vm.Namespace)
		if err != nil {
			return "", false, err
		}
		if len(vmInstance.Status.Interfaces) == 0 {
			return "", true, fmt.Errorf("no interfaces found in the VM [%s] in namespace [%s]", vm.Name, vm.Namespace)
		}
		return "", false, nil

	}
	// Need to retry because in case of restarts that is a window where the Interface is not available
	_, err := task.DoRetryWithTimeout(t, 5*time.Minute, 30*time.Second)
	vmInstance, err := k8sKubevirt.GetVirtualMachineInstance(ctx, vm.Name, vm.Namespace)
	if err != nil {
		return "", err
	}
	ipAddress := vmInstance.Status.Interfaces[0].IP
	log.Infof("VM Name - %s", vm.Name)
	log.Infof("IP Address - %s", ipAddress)

	// Getting username of the VM
	// Username has to be added as a label to the VMI Spec
	if u, ok := vmInstance.Labels["username"]; !ok {
		return "", fmt.Errorf("username not found in the labels of the vmi spec")
	} else {
		log.Infof("Username - %s", u)
		username = u
	}

	// Get password of the VM
	// Password has to be added as a value in the ConfigMap named kubevirt-creds whose key the name of the VM
	cm, err := core.Instance().GetConfigMap("kubevirt-creds", "default")
	if err != nil {
		return "", err
	}
	if p, ok := cm.Data[vm.Name]; !ok {
		return "", fmt.Errorf("password not found in the configmap [%s/%s] for vm - [%s]", "default", "kubevirt-creds", vm.Name)
	} else {
		log.Infof("Password - %s", p)
		password = p

	}

	// SSH command to be executed
	sshCmd := fmt.Sprintf("sshpass -p '%s' ssh -o StrictHostKeyChecking=no %s@%s %s", password, username, ipAddress, cmd)
	log.Infof("SSH Command - %s", sshCmd)

	// If the cluster provider is openshift then we are creating ssh pod and running the command in it
	if os.Getenv("CLUSTER_PROVIDER") == "openshift" {
		log.Infof("Cluster is openshift hence creating the SSH Pod")
		err = initSSHPod(sshPodNamespace)
		if err != nil {
			return "", err
		}

		testCmdArgs := getSSHCommandArgs(username, password, ipAddress, "hostname")
		cmdArgs := getSSHCommandArgs(username, password, ipAddress, cmd)

		// To check if the ssh server is up and running
		t := func() (interface{}, bool, error) {
			output, err := k8sCore.RunCommandInPod(testCmdArgs, sshPodName, "ssh-container", sshPodNamespace)
			if err != nil {
				log.Infof("Error encountered")
				if isConnectionError(err.Error()) {
					log.Infof("Test connection output - \n%s", output)
					return "", true, err
				} else {
					return "", false, err
				}
			}
			log.Infof("Test connection success output - \n%s", output)
			return "", false, nil
		}
		_, err = task.DoRetryWithTimeout(t, 10*time.Minute, 30*time.Second)
		if err != nil {
			return "", err
		}

		// Executing the actual command
		output, err := k8sCore.RunCommandInPod(cmdArgs, sshPodName, "ssh-container", sshPodNamespace)
		if err != nil {
			return output, err
		}
		log.Infof("Output of cmd %s - \n%s", cmd, output)
		return output, nil
	} else {
		workerNode := node.GetWorkerNodes()[0]
		t := func() (interface{}, bool, error) {
			output, err := driver.RunCommand(workerNode, sshCmd, node.ConnectionOpts{
				Timeout:         defaultCmdTimeout,
				TimeBeforeRetry: defaultCmdRetryInterval,
				Sudo:            true,
			})
			if err != nil {
				log.Infof("Error encountered")
				if isConnectionError(err.Error()) {
					log.Infof("Output of cmd %s - \n%s", cmd, output)
					return "", true, err
				} else {
					return output, false, err
				}
			}
			log.Infof("Output of cmd %s - \n%s", cmd, output)
			return output, false, nil
		}
		commandOutput, err := task.DoRetryWithTimeout(t, 10*time.Minute, 30*time.Second)
		return commandOutput.(string), err
	}
}

// initSSHPod creates a pod with ssh server installed and running along with sshpass utility
func initSSHPod(namespace string) error {
	var p *corev1.Pod
	var err error
	k8sCore := core.Instance()
	// Check if namespace exists
	_, err = k8sCore.GetNamespace(namespace)
	if err != nil {
		// Namespace doesn't exist, create it
		log.Infof("Namespace %s does not exist. Creating...", namespace)
		ns := &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{
				Name: namespace,
			},
		}
		if _, err = k8sCore.CreateNamespace(ns); err != nil {
			log.Errorf("Failed to create namespace %s: %v", namespace, err)
			return err
		}
		log.Infof("Namespace %s created successfully", namespace)
	}
	if p, err = k8sCore.GetPodByName(sshPodName, namespace); p == nil {
		sshPodSpec := &corev1.Pod{
			TypeMeta: metav1.TypeMeta{
				Kind:       "Pod",
				APIVersion: "v1",
			},
			ObjectMeta: metav1.ObjectMeta{
				Name:      sshPodName,
				Namespace: namespace,
			},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{
					{
						Name:  "ssh-container",
						Image: "ubuntu:latest",
						Command: []string{
							"/bin/bash",
							"-c",
						},
						Args: []string{
							"apt-get update && apt-get install -y openssh-server sshpass && service ssh start && echo 'root:toor' | chpasswd && sleep infinity",
						},
					},
				},
			},
		}
		log.Infof("Creating ssh pod")
		_, err = k8sCore.CreatePod(sshPodSpec)
		if err != nil {
			log.Errorf("An Error Occured while creating %v", err)
			return err
		}
		t := func() (interface{}, bool, error) {
			pod, err := k8sCore.GetPodByName(sshPodName, namespace)
			if err != nil {
				return "", false, err
			}
			if !k8sCore.IsPodRunning(*pod) {
				return "", true, fmt.Errorf("waiting for pod %s to be in running state", sshPodName)
			}
			// Adding static sleep to let the ssh server start
			time.Sleep(30 * time.Second)
			log.Infof("ssh pod creation complete")
			return "", false, nil
		}
		_, err = task.DoRetryWithTimeout(t, 5*time.Minute, 30*time.Second)
		return err
	}
	return err
}

func getSSHCommandArgs(username, password, ipAddress, cmd string) []string {
	return []string{"sshpass", "-p", password, "ssh", "-o", "StrictHostKeyChecking=no", fmt.Sprintf("%s@%s", username, ipAddress), cmd}
}

// isConnectionError checks if the error message is a connection error
func isConnectionError(errorMessage string) bool {
	return strings.Contains(errorMessage, "Connection refused") || strings.Contains(errorMessage, "Host is unreachable") ||
		strings.Contains(errorMessage, "No route to host")
}

// RestartKubevirtVM restarts the kubevirt VM
func RestartKubevirtVM(name string, namespace string, waitForCompletion bool) error {
	k8sKubevirt := kubevirt.Instance()
	vm, err := k8sKubevirt.GetVirtualMachine(name, namespace)
	if err != nil {
		return err
	}
	err = k8sKubevirt.RestartVirtualMachine(vm)
	if err != nil {
		return err
	}
	if waitForCompletion {
		t := func() (interface{}, bool, error) {
			vm, err = k8sKubevirt.GetVirtualMachine(name, namespace)
			if err != nil {
				return "", false, fmt.Errorf("unable to get virtual machine [%s] in namespace [%s]\nerror - %s", name, namespace, err.Error())
			}
			if vm.Status.PrintableStatus != kubevirtv1.VirtualMachineStatusRunning {
				return "", true, fmt.Errorf("virtual machine [%s] in namespace [%s] is in %s state, waiting to be in %s state", name, namespace, vm.Status.PrintableStatus, kubevirtv1.VirtualMachineStatusRunning)
			}
			log.InfoD("virtual machine [%s] in namespace [%s] is in %s state", name, namespace, vm.Status.PrintableStatus)
			return "", false, nil
		}
		_, err = DoRetryWithTimeoutWithGinkgoRecover(t, vmStartStopTimeout, vmStartStopRetryTime)
		if err != nil {
			return err
		}
	}
	return nil
}

// DoRetryWithTimeoutWithGinkgoRecover calls `task.DoRetryWithTimeout` along with `ginkgo.GinkgoRecover()`, to be used in callbacks with panics or ginkgo assertions
func DoRetryWithTimeoutWithGinkgoRecover(taskFunc func() (interface{}, bool, error), timeout, timeBeforeRetry time.Duration) (interface{}, error) {
	taskFuncWithGinkgoRecover := func() (interface{}, bool, error) {
		defer ginkgo.GinkgoRecover()
		return taskFunc()
	}
	return task.DoRetryWithTimeout(taskFuncWithGinkgoRecover, timeout, timeBeforeRetry)
}

// CreatePVCsForVM creates PVCs for the VM
func CreatePVCsForVM(vm kubevirtv1.VirtualMachine, numberOfPVCs int, storageClassName, resourceStorage string) ([]*corev1.PersistentVolumeClaim, error) {
	pvcs := make([]*corev1.PersistentVolumeClaim, 0)
	for i := 0; i < numberOfPVCs; i++ {
		pvcName := fmt.Sprintf("%s-%s-%d", "pvc-new", vm.Name, i)
		pvc, err := core.Instance().CreatePersistentVolumeClaim(&corev1.PersistentVolumeClaim{
			TypeMeta: metav1.TypeMeta{
				Kind: "PersistentVolumeClaim",
			},
			ObjectMeta: metav1.ObjectMeta{
				Name:      pvcName,
				Namespace: vm.Namespace,
			},
			Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany},
				StorageClassName: &storageClassName,
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceStorage: resource.MustParse(resourceStorage),
					},
				},
			},
		})
		if err != nil {
			return nil, err
		}
		// adding kind to pvc object
		pvc.Kind = "PersistentVolumeClaim"
		pvcs = append(pvcs, pvc)
	}
	return pvcs, nil
}

// AddPVCsToVirtualMachine adds PVCs to virtual machine
func AddPVCsToVirtualMachine(vm kubevirtv1.VirtualMachine, pvcs []*corev1.PersistentVolumeClaim) error {
	k8sKubevirt := kubevirt.Instance()
	var volumes []kubevirtv1.Volume
	var disks []kubevirtv1.Disk
	for i, pvc := range pvcs {
		log.Infof("Adding PVC [%s] to VM [%s]", pvc.Name, vm.Name)
		volumes = append(volumes, kubevirtv1.Volume{
			Name: fmt.Sprintf("%s-%d", "datavolume-additional", i),
			VolumeSource: kubevirtv1.VolumeSource{
				PersistentVolumeClaim: &kubevirtv1.PersistentVolumeClaimVolumeSource{
					PersistentVolumeClaimVolumeSource: corev1.PersistentVolumeClaimVolumeSource{
						ClaimName: pvc.Name,
					},
					Hotpluggable: false,
				},
			},
		})
		disks = append(disks, kubevirtv1.Disk{
			Name:       fmt.Sprintf("%s-%d", "datavolume-additional", i),
			DiskDevice: kubevirtv1.DiskDevice{Disk: &kubevirtv1.DiskTarget{Bus: kubevirtv1.DiskBusVirtio}},
		})

	}
	vm.Spec.Template.Spec.Volumes = append(vm.Spec.Template.Spec.Volumes, volumes...)

	vm.Spec.Template.Spec.Domain.Devices.Disks = append(vm.Spec.Template.Spec.Domain.Devices.Disks, disks...)

	vmUpdate, err := k8sKubevirt.UpdateVirtualMachine(&vm)
	if err != nil {
		return err
	}
	log.Infof("[%d] volumes added to VM [%s]", len(pvcs), vmUpdate.Name)

	return nil
}
