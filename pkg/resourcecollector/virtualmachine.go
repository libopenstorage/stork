package resourcecollector

import (
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
)

/*
 * Parsing logic to fetch the given key; Ex: "macAddress" from interface slice
 */
func (r *ResourceCollector) parseMap(input map[string]interface{}, output map[string]interface{}, key string, is_key_present *bool) {
	for mKey, mVal := range input {
		if mKey == key {
			*is_key_present = true
			continue
		}
		output[mKey] = mVal

	}
}

/*
 * This function removes macAddress element from the interface list
 */
func (r *ResourceCollector) removeMac(object runtime.Unstructured) error {

	content := object.UnstructuredContent()
	path := []string{"spec", "template", "spec", "domain", "devices", "interfaces"}
	data, _, er := unstructured.NestedSlice(content, path...)
	if er != nil {
		return er
	}
	remove := false
	interf := make([]interface{}, 0, 1)
	for _, val := range data {
		switch typeval := val.(type) {
		case map[string]interface{}:
			b := make(map[string]interface{})
			r.parseMap(typeval, b, "macAddress", &remove)
			interf = append(interf, b)
		}
	}

	// We will not change if given key was not found in the content.
	if remove {
		unstructured.RemoveNestedField(content, path...)
		if err := unstructured.SetNestedSlice(content, interf, path...); err != nil {
			return err
		}
	}
	return nil

}

/*
 * This should be called from restore workflow for any resource transform for
 * VM resources.
 */
func (r *ResourceCollector) prepareVirtualMachineForApply(
	object runtime.Unstructured,
	namespaces []string,
) error {

	// Lets remove macAddress as it creates macAddress conflict while restore
	// to the same cluster to different namespace while the source VM is up and
	// running.
	err := r.removeMac(object)
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
