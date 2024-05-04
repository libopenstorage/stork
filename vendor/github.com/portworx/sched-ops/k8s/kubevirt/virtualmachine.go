package kubevirt

import (
	"context"
	"fmt"
	"time"

	"github.com/portworx/sched-ops/task"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/apimachinery/pkg/api/errors"
	k8smetav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubevirtv1 "kubevirt.io/api/core/v1"
)

// VirtualMachineOps is an interface to perform kubevirt virtualMachine operations
type VirtualMachineOps interface {
	// CreateVirtualMachine calls VirtualMachine create client method
	CreateVirtualMachine(*kubevirtv1.VirtualMachine) (*kubevirtv1.VirtualMachine, error)
	// ListVirtualMachines List Kubevirt VirtualMachine in given namespace
	ListVirtualMachines(namespace string) (*kubevirtv1.VirtualMachineList, error)
	// BatchListVirtualMachines List Kubevirt VirtualMachine in given namespace in batches
	BatchListVirtualMachines(namespace string, listOptions *k8smetav1.ListOptions) (*kubevirtv1.VirtualMachineList, error)
	// ValidateVirtualMachineRunning check if VirtualMachine is running, if not
	// start VirtualMachine and wait for it get started.
	ValidateVirtualMachineRunning(string, string, time.Duration, time.Duration) error
	// DeleteVirtualMachine Delete VirtualMachine CR
	DeleteVirtualMachine(string, string) error
	// GetVirtualMachine Get updated VirtualMachine from client matching name and namespace
	GetVirtualMachine(string, string) (*kubevirtv1.VirtualMachine, error)
	// StartVirtualMachine Start VirtualMachine
	StartVirtualMachine(*kubevirtv1.VirtualMachine) error
	// StopVirtualMachine Stop VirtualMachine
	StopVirtualMachine(*kubevirtv1.VirtualMachine) error
	// RestartVirtualMachine restarts VirtualMachine
	RestartVirtualMachine(*kubevirtv1.VirtualMachine) error
	// GetVMDataVolumes returns DataVolumes used by the VM
	GetVMDataVolumes(vm *kubevirtv1.VirtualMachine) []string
	// GetVMPersistentVolumeClaims returns persistentVolumeClaim names used by the VMs
	GetVMPersistentVolumeClaims(vm *kubevirtv1.VirtualMachine) []string
	// GetVMSecrets returns references to secrets in all supported formats of VM configs
	GetVMSecrets(vm *kubevirtv1.VirtualMachine) []string
	// GetVMConfigMaps returns ConfigMaps referenced in the VirtualMachine.
	GetVMConfigMaps(*kubevirtv1.VirtualMachine) []string
	//IsVirtualMachineRunning returns true if virtualMachine is in running state
	IsVirtualMachineRunning(*kubevirtv1.VirtualMachine) bool
	// UpdateVirtualMachine updates existing Kubevirt VirtualMachine
	UpdateVirtualMachine(*kubevirtv1.VirtualMachine) (*kubevirtv1.VirtualMachine, error)
	// IsKubevirtCRDInstalled to check if virtualmachine crd is installed.
	IsKubevirtCRDInstalled() error
}

var (
	// VirtualMachineCRD
	VirtualMachineCRD = "virtualmachines.kubevirt.io"
	// VirtualMachineCRDError error return code for virtualMachine CRD
	VirtualMachineCRDError = status.Error(codes.FailedPrecondition, "Kubevirt CRD not configured on the cluster.")
)

// IsKubevirtCRDInstalled will check if virtualmachines.kubevirt.io CRD exists.
func (c *Client) IsKubevirtCRDInstalled() error {
	_, err := c.kubevirt.ExtensionsClient().ApiextensionsV1().CustomResourceDefinitions().Get(context.TODO(), VirtualMachineCRD, k8smetav1.GetOptions{})
	if err != nil {
		if errors.IsNotFound(err) {
			return VirtualMachineCRDError
		}
	}
	return err
}

// ListVirtualMachines List Kubevirt VirtualMachine in given namespace
func (c *Client) ListVirtualMachines(namespace string) (*kubevirtv1.VirtualMachineList, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	if err := c.IsKubevirtCRDInstalled(); err != nil {
		return nil, err
	}
	return c.kubevirt.VirtualMachine(namespace).List(&k8smetav1.ListOptions{})
}

// BatchListVirtualMachines List Kubevirt VirtualMachine in given namespace
func (c *Client) BatchListVirtualMachines(namespace string, listOptions *k8smetav1.ListOptions) (*kubevirtv1.VirtualMachineList, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}

	if err := c.IsKubevirtCRDInstalled(); err != nil {
		return nil, err
	}

	return c.kubevirt.VirtualMachine(namespace).List(listOptions)
}

// CreateVirtualMachine calls VirtualMachine create client method
func (c *Client) CreateVirtualMachine(vm *kubevirtv1.VirtualMachine) (*kubevirtv1.VirtualMachine, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	if err := c.IsKubevirtCRDInstalled(); err != nil {
		return nil, err
	}
	return c.kubevirt.VirtualMachine(vm.GetNamespace()).Create(vm)
}

// GetVirtualMachine Get updated VirtualMachine from client matching name and namespace
func (c *Client) GetVirtualMachine(name string, namespace string) (*kubevirtv1.VirtualMachine, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	if err := c.IsKubevirtCRDInstalled(); err != nil {
		return nil, err
	}

	return c.kubevirt.VirtualMachine(namespace).Get(name, &k8smetav1.GetOptions{})
}

// DeleteVirtualMachine Delete VirtualMachine CR
func (c *Client) DeleteVirtualMachine(name, namespace string) error {
	if err := c.initClient(); err != nil {
		return err
	}

	return c.kubevirt.VirtualMachine(namespace).Delete(name, &k8smetav1.DeleteOptions{})
}

// ValidateVirtualMachineRunning check if VirtualMachine is running, if not
// start VirtualMachine and wait for it get started.
func (c *Client) ValidateVirtualMachineRunning(name, namespace string, timeout, retryInterval time.Duration) error {
	if err := c.initClient(); err != nil {
		return err
	}
	vm, err := c.GetVirtualMachine(name, namespace)
	if err != nil {
		return fmt.Errorf("failed to get Virtual Machine")
	}

	// Start the VirtualMachine if its not Started yet
	if vm.Status.PrintableStatus == kubevirtv1.VirtualMachineStatusStopped ||
		vm.Status.PrintableStatus == kubevirtv1.VirtualMachineStatusStopping {
		if err = c.StartVirtualMachine(vm); err != nil {
			return fmt.Errorf("Failed to start VirtualMachine %v", err)
		}
	}

	t := func() (interface{}, bool, error) {

		vm, err = c.GetVirtualMachine(name, namespace)
		if err != nil {
			return "", false, fmt.Errorf("failed to get Virtual Machine")
		}

		if vm.Status.PrintableStatus == kubevirtv1.VirtualMachineStatusRunning {
			return "", false, nil
		}
		return "", true, fmt.Errorf("Virtual Machine not in running state: %v", vm.Status.PrintableStatus)

	}
	if _, err := task.DoRetryWithTimeout(t, timeout, retryInterval); err != nil {
		return err
	}
	return nil

}

// StartVirtualMachine Start VirtualMachine
func (c *Client) StartVirtualMachine(vm *kubevirtv1.VirtualMachine) error {

	if err := c.initClient(); err != nil {
		return err
	}

	return c.kubevirt.VirtualMachine(vm.GetNamespace()).Start(vm.GetName(), &kubevirtv1.StartOptions{})
}

// StopVirtualMachine Stop VirtualMachine
func (c *Client) StopVirtualMachine(vm *kubevirtv1.VirtualMachine) error {
	if err := c.initClient(); err != nil {
		return err
	}

	return c.kubevirt.VirtualMachine(vm.GetNamespace()).Stop(vm.GetName(), &kubevirtv1.StopOptions{})
}

// RestartVirtualMachine restarts VirtualMachine
func (c *Client) RestartVirtualMachine(vm *kubevirtv1.VirtualMachine) error {
	if err := c.initClient(); err != nil {
		return err
	}
	return c.kubevirt.VirtualMachine(vm.GetNamespace()).Restart(vm.GetName(), &kubevirtv1.RestartOptions{})
}

// IsVirtualMachineRunning returns true if virtualMachine is in running state
func (c *Client) IsVirtualMachineRunning(vm *kubevirtv1.VirtualMachine) bool {
	if err := c.initClient(); err != nil {
		return false
	}
	if vm.Status.PrintableStatus == kubevirtv1.VirtualMachineStatusRunning {
		return true
	}
	return false

}

// GetVMDataVolumes returns DataVolumes used by the VM
func (c *Client) GetVMDataVolumes(vm *kubevirtv1.VirtualMachine) []string {
	volList := vm.Spec.Template.Spec.Volumes
	dvList := make([]string, 0)
	for _, vol := range volList {
		if vol.VolumeSource.DataVolume != nil {
			dvList = append(dvList, vol.VolumeSource.DataVolume.Name)
		}
	}
	return dvList
}

// GetVMPersistentVolumeClaims returns persistentVolumeClaim names used by the VMs
func (c *Client) GetVMPersistentVolumeClaims(vm *kubevirtv1.VirtualMachine) []string {
	volList := vm.Spec.Template.Spec.Volumes
	PVCList := make([]string, 0)
	for _, vol := range volList {
		if vol.VolumeSource.PersistentVolumeClaim != nil {
			PVCList = append(PVCList, vol.VolumeSource.PersistentVolumeClaim.ClaimName)
		}
	}
	return PVCList
}

// GetVMSecrets returns references to secrets in all supported formats of VM configs
func (c *Client) GetVMSecrets(vm *kubevirtv1.VirtualMachine) []string {
	volList := vm.Spec.Template.Spec.Volumes
	secretList := make([]string, 0)
	for _, vol := range volList {
		// secret as VolumeType
		if vol.VolumeSource.Secret != nil {
			secretList = append(secretList, vol.Secret.SecretName)
		}
		// Secret reference as sysprep
		if vol.VolumeSource.Sysprep != nil {
			if vol.VolumeSource.Sysprep.Secret != nil {
				secretList = append(secretList, vol.VolumeSource.Sysprep.Secret.Name)
			}
		}
		if vol.VolumeSource.CloudInitNoCloud != nil {
			cloudInitNoCloud := vol.VolumeSource.CloudInitNoCloud
			// secret as NetworkDataSecretRef
			if cloudInitNoCloud.NetworkDataSecretRef != nil {
				secretList = append(secretList, cloudInitNoCloud.NetworkDataSecretRef.Name)
			}
			// secret as UserDataSecretRef
			if cloudInitNoCloud.UserDataSecretRef != nil {
				secretList = append(secretList, cloudInitNoCloud.UserDataSecretRef.Name)
			}
		}
		if vol.VolumeSource.CloudInitConfigDrive != nil {
			cloudInitConfigDrive := vol.VolumeSource.CloudInitConfigDrive
			// Secret from configDrive for NetworkData
			if cloudInitConfigDrive.NetworkDataSecretRef != nil {
				secretList = append(secretList, cloudInitConfigDrive.NetworkDataSecretRef.Name)
			}
			// Secret from confifDrive aka Ignition
			if cloudInitConfigDrive.UserDataSecretRef != nil {
				secretList = append(secretList, cloudInitConfigDrive.UserDataSecretRef.Name)
			}

		}
	}
	return secretList
}

// GetVMConfigMaps returns ConfigMaps referenced in the VirtualMachine.
func (c *Client) GetVMConfigMaps(vm *kubevirtv1.VirtualMachine) []string {
	volList := vm.Spec.Template.Spec.Volumes
	configMaps := make([]string, 0)
	for _, vol := range volList {
		// ConfigMap as volumeType
		if vol.ConfigMap != nil {
			configMaps = append(configMaps, vol.ConfigMap.Name)
		}
		// configMap reference in sysprep
		if vol.VolumeSource.Sysprep != nil {
			if vol.VolumeSource.Sysprep.ConfigMap != nil {
				configMaps = append(configMaps, vol.VolumeSource.Sysprep.ConfigMap.Name)
			}
		}

	}
	return configMaps
}

// UpdateVirtualMachine updates existing Kubevirt VirtualMachine
func (c *Client) UpdateVirtualMachine(vm *kubevirtv1.VirtualMachine) (*kubevirtv1.VirtualMachine, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}

	return c.kubevirt.VirtualMachine(vm.GetNamespace()).Update(vm)
}
