package resourcecollector

import (
	"strings"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
)

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
