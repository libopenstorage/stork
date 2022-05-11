package resourcecollector

import (
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
)

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
