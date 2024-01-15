package resourcecollector

import (
	"fmt"
	"strings"

	storkapi "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	kubevirtv1 "kubevirt.io/api/core/v1"
)

const (
	// PxBackupObjectType_virtualMachine px-backuo ObjectType virtualMachine
	PxBackupObjectType_virtualMachine = "VirtualMachine"
	//VMPodSelectorLabel is the kubvirt label key for virt-launcher pod of the VM
	VMPodSelectorLabel = "vm.kubevirt.io/name"
	//VMFreezeCmd is the template for VM freeze command
	VMFreezeCmd = "/usr/bin/virt-freezer --freeze --name %s --namespace %s"
	//VMUnFreezeCmd is the template for VM unfreeze command
	VMUnFreezeCmd = "/usr/bin/virt-freezer --unfreeze --name %s --namespace %s"
)

// IsVirtualMachineRunning returns true if virtualMachine is in running state
func IsVirtualMachineRunning(vm kubevirtv1.VirtualMachine) bool {

	return  vm.Status.PrintableStatus == kubevirtv1.VirtualMachineStatusRunning
}

// GetVMDataVolumes returns DataVolumes used by the VM
func GetVMDataVolumes(vm kubevirtv1.VirtualMachine) []string {

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
func GetVMPersistentVolumeClaims(vm kubevirtv1.VirtualMachine) []string {

	volList := vm.Spec.Template.Spec.Volumes
	PVCList := make([]string, 0)
	for _, vol := range volList {
		if vol.VolumeSource.PersistentVolumeClaim != nil {
			PVCList = append(PVCList, vol.VolumeSource.PersistentVolumeClaim.ClaimName)
		}
		// Add DataVolume name to the PVC list
		if vol.VolumeSource.DataVolume != nil {
			PVCList = append(PVCList, vol.VolumeSource.DataVolume.Name)
		}
	}
	return PVCList
}

// GetVMSecrets returns references to secrets in all supported formats of VM configs
func GetVMSecrets(vm kubevirtv1.VirtualMachine) []string {

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
			// Secret from configDrive aka Ignition
			if cloudInitConfigDrive.UserDataSecretRef != nil {
				secretList = append(secretList, cloudInitConfigDrive.UserDataSecretRef.Name)
			}

		}
	}
	return secretList
}

// GetVMConfigMaps returns ConfigMaps referenced in the VirtualMachine.
func GetVMConfigMaps(vm kubevirtv1.VirtualMachine) []string {

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

// IsVMAgentConnected returns true if Running VM has qemu-guest-agent running in the guest OS
func IsVMAgentConnected(vm kubevirtv1.VirtualMachine) bool {

	if IsVirtualMachineRunning(vm) {
		for _, cond := range vm.Status.Conditions {
			if cond.Type == "AgentConnected" {
				return true
			}
		}
	}
	return false
}

// GetVMFreezeRule returns freeze Rule action for given VM
func GetVMFreezeRule(vm kubevirtv1.VirtualMachine) string {

	return fmt.Sprintf(VMFreezeCmd, vm.GetName(), vm.GetNamespace())
}

// GetVMUnFreezeRule returns unfreeze Rule action for given VM
func GetVMUnFreezeRule(vm kubevirtv1.VirtualMachine) string {

	return fmt.Sprintf(VMUnFreezeCmd, vm.GetName(), vm.GetNamespace())
}

// GetVMPodLabel return podSelector label for the given VM
func GetVMPodLabel(vm kubevirtv1.VirtualMachine) map[string]string {
	return map[string]string{
		VMPodSelectorLabel: vm.GetName(),
	}
}

// Transform dataVolume: to persistentVolumeClaim:
func transformDataVolume(input map[string]interface{}, name string) map[string]interface{} {

	output := make(map[string]interface{})

	for key, val := range input {
		if key == name {
			output["claimName"] = val
			continue
		}
		output[key] = val
	}
	return output
}

// Utility function for virtualMachine template parsing logic
func parseMap(input map[string]interface{}, output map[string]interface{}, is_key_present *bool) {

	for mKey, mVal := range input {
		switch mKey {
		case "macAddress":
			*is_key_present = true
			continue
		case "dataVolume":
			*is_key_present = true
			switch typeval := mVal.(type) {
			case map[string]interface{}:
				output["persistentVolumeClaim"] = transformDataVolume(typeval, "name")
				continue
			}
		}
		output[mKey] = mVal
	}
}

// Path parsing function for virtualMachine template.
func transformPath(object runtime.Unstructured, path []string) error {

	content := object.UnstructuredContent()
	data, _, err := unstructured.NestedSlice(content, path...)
	if err != nil {
		return err
	}
	found := false
	newDeviceFields := make([]interface{}, 0, 1)
	for _, val := range data {
		switch typeval := val.(type) {
		case map[string]interface{}:
			newMap := make(map[string]interface{})
			parseMap(typeval, newMap, &found)
			newDeviceFields = append(newDeviceFields, newMap)
		}
	}
	// Update only if the desired path,value is found in the template for transformation
	// Ignore otherwise.
	if found {
		unstructured.RemoveNestedField(content, path...)
		if err := unstructured.SetNestedSlice(content, newDeviceFields, path...); err != nil {
			return err
		}
	}
	return nil

}

// This should be called from restore workflow for any resource transform for
// VM resources.
func (r *ResourceCollector) prepareVirtualMachineForApply(
	object runtime.Unstructured,
	namespaces []string,
) error {

	// Remove macAddress as it creates macAddress conflict while restore
	// to the same cluster to different namespace while the source VM is
	// up and running.
	path := "spec.template.spec.domain.devices.interfaces"
	if err := transformPath(object, strings.Split(path, ".")); err != nil {
		return err
	}

	// Transform dataVolume to associated PVC for dataVolumeTemplate configurations
	path = "spec.template.spec.volumes"
	err := transformPath(object, strings.Split(path, "."))

	return err

}

func (r *ResourceCollector) prepareVirtualMachineForCollection(
	object runtime.Unstructured,
	namespaces []string,
) error {
	// VM object has mutually exclusive field to start/stop VM's
	// lets remove spec.running to avoid gettting blocked by
	// webhook controller
	content := object.UnstructuredContent()
	path := []string{"spec", "running"}
	unstructured.RemoveNestedField(content, path...)
	path = []string{"spec", "runStrategy"}
	if err := unstructured.SetNestedField(content, "Always", path...); err != nil {
		return err
	}
	// since dr handles volume migration datavolumetemplates
	// is not necessary
	path = []string{"spec", "dataVolumeTemplates"}
	unstructured.RemoveNestedField(content, path...)
	return nil
}

// getObjectInfoFromVM returns map with resource to VM mapping
func getObjecInfoFromVM(
	resourceNames []string, vm kubevirtv1.VirtualMachine,
	objectKind string,
	objectMap map[storkapi.ObjectInfo]kubevirtv1.VirtualMachine,
) map[storkapi.ObjectInfo]kubevirtv1.VirtualMachine {

	for _, resource := range resourceNames {
		info := storkapi.ObjectInfo{
			GroupVersionKind: metav1.GroupVersionKind{
				Group:   "core",
				Version: "v1",
				Kind:    objectKind,
			},
			Name:      resource,
			Namespace: vm.Namespace,
		}
		objectMap[info] = vm
	}
	return objectMap
}

// GetVMResourcesObjectMap creates a map of VM resources to VM from backupObject
func GetVMResourcesObjectMap(
	objects []runtime.Unstructured,
	includeObjectMap map[storkapi.ObjectInfo]bool,
) (map[storkapi.ObjectInfo]kubevirtv1.VirtualMachine, error) {
	objectsMap := make(map[storkapi.ObjectInfo]kubevirtv1.VirtualMachine)
	for _, o := range objects {
		objectType, err := meta.TypeAccessor(o)
		if err != nil {
			return objectsMap, err
		}
		if objectType.GetKind() == "VirtualMachine" {
			var vm kubevirtv1.VirtualMachine
			err := runtime.DefaultUnstructuredConverter.FromUnstructured(o.UnstructuredContent(), &vm)
			if err != nil {
				return objectsMap, nil
			}
			vmInfo := storkapi.ObjectInfo{
				GroupVersionKind: metav1.GroupVersionKind{
					Group:   "kubevirt.io",
					Version: "v1",
					Kind:    "VirtualMachine",
				},
				Name:      vm.Name,
				Namespace: vm.Namespace,
			}
			// Skip the resource if vm is not included in the includeResource.
			if !includeObjectMap[vmInfo] {
				continue
			}
			objectsMap = getObjecInfoFromVM(GetVMPersistentVolumeClaims(vm), vm, "PersistentVolumeClaim", objectsMap)
			objectsMap = getObjecInfoFromVM(GetVMConfigMaps(vm), vm, "ConfigMap", objectsMap)
			objectsMap = getObjecInfoFromVM(GetVMSecrets(vm), vm, "Secret", objectsMap)

		}
	}
	return objectsMap, nil
}
