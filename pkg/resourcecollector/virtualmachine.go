package resourcecollector

import (
	"context"
	"fmt"
	"strings"

	storkapi "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	kubevirtops "github.com/portworx/sched-ops/k8s/kubevirt"
	"github.com/sirupsen/logrus"
	k8s_errors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	kubevirtv1 "kubevirt.io/api/core/v1"
)

const (
	// PxBackupObjectType_virtualMachine px-backup ObjectType virtualMachine
	PxBackupObjectType_virtualMachine = "VirtualMachine"
	//VMPodSelectorLabel is the kubvirt label key for virt-launcher pod of the VM
	VMPodSelectorLabel = "vm.kubevirt.io/name"
	//VMFreezeCmd is the template for VM freeze command
	VMFreezeCmd = "/usr/bin/virt-freezer --freeze --name %s --namespace %s"
	//VMUnFreezeCmd is the template for VM unfreeze command
	VMUnFreezeCmd = "/usr/bin/virt-freezer --unfreeze --name %s --namespace %s"
	// VMContainerName is the name of the container to use for freeze/thaw
	VMContainerName             = "compute"
	VMPodSelectorCreatedByLabel = "kubevirt.io/created-by"
)

// IsVirtualMachineRunning returns true if virtualMachine is in running state
func IsVirtualMachineRunning(vm kubevirtv1.VirtualMachine) bool {

	return vm.Status.PrintableStatus == kubevirtv1.VirtualMachineStatusRunning
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
	kv := kubevirtops.Instance()
	podSelectorLabel := make(map[string]string)
	vmi, err := kv.GetVirtualMachineInstance(context.TODO(), vm.Name, vm.Namespace)
	if err != nil {
		// We will never come here if VM is up and running. However, if we do get error here,
		// we will log the error and move on. created-by label is only required for more than
		// one VMs with same name but different namespace. Hence, we will skip this field and
		// let rule executor handle the error if applicable.
		logrus.Errorf("error fetching VMI for vm %v(%v) while creating podLabelSelector:%v", vm.Name, vm.Namespace, err)
	} else {
		podSelectorLabel[VMPodSelectorCreatedByLabel] = string(vmi.GetUID())
	}
	podSelectorLabel[VMPodSelectorLabel] = vm.GetName()
	return podSelectorLabel
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

// GetVMResourcesFromResourceObject returns ObjectInfo array with resources for each of
// the VirtualMachines specified in restore.Spec.IncludeResources
func GetVMResourcesFromResourceObject(objects []runtime.Unstructured,
	includeObjectMap map[storkapi.ObjectInfo]bool,
) ([]storkapi.ObjectInfo, error) {

	includeVMObjects := make([]storkapi.ObjectInfo, 0)
	for _, o := range objects {
		objectType, err := meta.TypeAccessor(o)
		if err != nil {
			return includeVMObjects, err
		}
		if objectType.GetKind() == "VirtualMachine" {
			var vm kubevirtv1.VirtualMachine
			err := runtime.DefaultUnstructuredConverter.FromUnstructured(o.UnstructuredContent(), &vm)
			if err != nil {
				return includeVMObjects, nil
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
			resourcesInfoList := GetObjectInfoFromVMResources(vm)
			if len(resourcesInfoList) != 0 {
				// ConfigMaps and Secrets may be referred by more than one VM
				// try include only one entry per namespace.
				for _, info := range resourcesInfoList {
					if !includeObjectMap[info] {
						includeObjectMap[info] = true
						includeVMObjects = append(includeVMObjects, info)
					}
				}
			}
		}
	}
	return includeVMObjects, nil
}

// GetObjectInfoFromVMResources helper function to getch resources of given VirtualMachine
func GetObjectInfoFromVMResources(vm kubevirtv1.VirtualMachine) []storkapi.ObjectInfo {

	vmResourceInfoList := make([]storkapi.ObjectInfo, 0)

	// get PVCs of the VM
	volumeObjectInfoList := getObjectInfo(GetVMPersistentVolumeClaims(vm), vm.Namespace, "PersistentVolumeClaim")
	vmResourceInfoList = append(vmResourceInfoList, volumeObjectInfoList...)

	// get configMaps of the VM
	configMapObjectInfoList := getObjectInfo(GetVMConfigMaps(vm), vm.Namespace, "ConfigMap")
	vmResourceInfoList = append(vmResourceInfoList, configMapObjectInfoList...)

	// getSecret references of the VM
	secretObjectInfoList := getObjectInfo(GetVMSecrets(vm), vm.Namespace, "Secret")
	vmResourceInfoList = append(vmResourceInfoList, secretObjectInfoList...)

	return vmResourceInfoList

}

// getObjectInfo helper function that returns ObjectInfo object from resourcesName and namespace
func getObjectInfo(resourceNames []string, namespace string, objectKind string) []storkapi.ObjectInfo {
	objectInfoList := make([]storkapi.ObjectInfo, 0)
	for _, resource := range resourceNames {
		info := storkapi.ObjectInfo{
			GroupVersionKind: metav1.GroupVersionKind{
				Group:   "core",
				Version: "v1",
				Kind:    objectKind,
			},
			Name:      resource,
			Namespace: namespace,
		}
		objectInfoList = append(objectInfoList, info)
	}
	return objectInfoList

}

// GetRuleItemWithVMAction return PreRuleItem and PostRuleItem with freeze/thaw rule for the given VM
// This function is also used by torpedo for testing.
func GetRuleItemWithVMAction(vm kubevirtv1.VirtualMachine) (storkapi.RuleItem, storkapi.RuleItem) {

	vmPodSelector := GetVMPodLabel(vm)
	vmfreezeAction := GetVMFreezeRule(vm)
	vmUnFreezeAction := GetVMUnFreezeRule(vm)

	//create RuleItem from podselector and freeze/unfreeze actions
	preRuleItem := getRuleItemWithAction(vmPodSelector, vmfreezeAction)
	postRuleItem := getRuleItemWithAction(vmPodSelector, vmUnFreezeAction)

	return preRuleItem, postRuleItem
}

// getRuleItemWithAction helper function that returns RuleItem from podSelector and action string
func getRuleItemWithAction(podSelector map[string]string, ruleAction string) storkapi.RuleItem {
	var actions []storkapi.RuleAction

	action := storkapi.RuleAction{
		Type:  storkapi.RuleActionCommand,
		Value: ruleAction,
	}
	actions = append(actions, action)
	ruleInfoItem := storkapi.RuleItem{
		PodSelector: podSelector,
		Actions:     actions,
		Container:   VMContainerName,
	}
	return ruleInfoItem
}
func getVmsFromNamespaceList(namespaces []string, objectMap map[storkapi.ObjectInfo]bool) (
	[]kubevirtv1.VirtualMachine,
	error) {

	if len(namespaces) == 0 {
		return nil, nil
	}
	kv := kubevirtops.Instance()

	listOptions := &metav1.ListOptions{
		Limit: 500,
	}
	VmList := make([]kubevirtv1.VirtualMachine, 0)
	for _, ns := range namespaces {
		if IsNsPresentInIncludeResource(objectMap, ns) {
			//We don't need to fetch resources from this ns
			// priority is for includeResource. We already
			// added those list to includeResources
			continue
		}
		for {
			blist, err := kv.BatchListVirtualMachines(ns, listOptions)
			if err != nil {
				return nil, fmt.Errorf("error fetching list VM for ns %v: %v", ns, err)
			}
			VmList = append(VmList, blist.Items...)
			if blist.Continue == "" {
				break
			} else {
				listOptions.Continue = blist.Continue
			}
		}
	}
	return VmList, nil
}

func getVmsFromIncludeResourceList(includeResources []storkapi.ObjectInfo) (
	[]kubevirtv1.VirtualMachine,
	map[storkapi.ObjectInfo]bool,
	error) {

	objectMap := make(map[storkapi.ObjectInfo]bool)
	if len(includeResources) == 0 {
		return nil, objectMap, nil
	}

	vmList := make([]kubevirtv1.VirtualMachine, 0)
	kv := kubevirtops.Instance()
	for _, resourceInfo := range includeResources {
		if resourceInfo.Kind != "VirtualMachine" {
			logrus.Debugf("found nonVM entry for VM BackupObject type in includeResources name:%v, kind:%v, ignoring..", resourceInfo.Name, resourceInfo.Kind)
			continue
		}
		vm, err := kv.GetVirtualMachine(resourceInfo.Name, resourceInfo.Namespace)
		if err != nil {
			// if we could not fetch the VM specified, we error out.
			// we continue with rest of the list if the VM is not found.
			// If none of the VMs in the includeResource is found,
			// we will still proceed. This will cause empty resource backup.
			if k8s_errors.IsNotFound(err) {
				objectMap[resourceInfo] = true
				continue
			}
			logrus.Errorf("error fetching VM from includeResource list %v(%v)", resourceInfo.Name, resourceInfo.Namespace)
			return nil, objectMap, err
		}
		objectMap[resourceInfo] = true
		vmList = append(vmList, *vm)
	}
	return vmList, objectMap, nil
}

// GetVMIncludeListFromBackup returns all the VMs from various filters,
func GetVMIncludeListFromBackup(backup *storkapi.ApplicationBackup) (
	[]kubevirtv1.VirtualMachine,
	map[storkapi.ObjectInfo]bool,
	error) {

	// First Fetch VM List from backup.Spec.IncludeResources
	vmIncList, objectMap, err := getVmsFromIncludeResourceList(backup.Spec.IncludeResources)
	if err != nil {
		return nil, nil, err
	}
	// Second fetch VM List from Namespace and Namespace label list
	vmNsList, err := getVmsFromNamespaceList(backup.Spec.Namespaces, objectMap)
	if err != nil {
		return nil, nil, err
	}
	vmIncList = append(vmIncList, vmNsList...)
	return vmIncList, objectMap, nil
}

// GetVMIncludeResourceInfoList returns VMs and VM resources in IncludeResource format.
// Also returns preExec and postExec rule Item with freeze/thaw rule for each of the filter VMs
func GetVMIncludeResourceInfoList(vmList []kubevirtv1.VirtualMachine, objectMap map[storkapi.ObjectInfo]bool, nsMap map[string]bool, skipAutoVMRule bool) (
	[]storkapi.ObjectInfo,
	map[storkapi.ObjectInfo]bool,
	[]storkapi.RuleItem,
	[]storkapi.RuleItem,

) {

	freezeRulesItems := make([]storkapi.RuleItem, 0)
	unFreezeRulesItems := make([]storkapi.RuleItem, 0)
	vmResourceInfoList := make([]storkapi.ObjectInfo, 0)
	for _, vm := range vmList {
		addRule := false
		resourceInfoList := GetObjectInfoFromVMResources(vm)
		vmInfo := storkapi.ObjectInfo{
			GroupVersionKind: metav1.GroupVersionKind{
				Group:   "kubevirt.io",
				Version: "v1",
				Kind:    "VirtualMachine",
			},
			Name:      vm.Name,
			Namespace: vm.Namespace,
		}
		if !nsMap[vm.Namespace] {
			nsMap[vm.Namespace] = true
		}
		// We would have mapped vmInfo specified in backup.Spec.IncludeResource but,
		// VMList from namespaces would not have been. Map them here.
		if !objectMap[vmInfo] {
			objectMap[vmInfo] = true
			vmResourceInfoList = append(vmResourceInfoList, vmInfo)
		}

		for _, info := range resourceInfoList {
			if !objectMap[info] {
				objectMap[info] = true
				vmResourceInfoList = append(vmResourceInfoList, info)
			}
			if info.Kind == "PersistentVolumeClaim" {
				addRule = true
			}
		}

		if skipAutoVMRule {
			// Auto Rules are disabled hence skip it.
			continue
		}
		if !IsVirtualMachineRunning(vm) {
			// Skip auto rules for non running VMs.
			continue
		}
		if !IsVMAgentConnected(vm) {
			// qemu-guest-agent is not running, skip autoRules for this VM
			logrus.Warnf("skipping freeze/thaw rules for VirtualMachine %v(%v) reason : Agent Not Connected",
				vm.Name, vm.Namespace)
			continue
		}
		// Only add rule if there are PVCs associated to the VM.
		if addRule {
			preRuleItem, postRuleItem := GetRuleItemWithVMAction(vm)
			freezeRulesItems = append(freezeRulesItems, preRuleItem)
			unFreezeRulesItems = append(unFreezeRulesItems, postRuleItem)
		} else {
			logrus.Debugf("skipping freeze/thaw rules for VirtualMachine %v(%v) reason: No PVCs attached to the VM",
				vm.Name, vm.Namespace)
		}
	}
	return vmResourceInfoList, objectMap, freezeRulesItems, unFreezeRulesItems

}
