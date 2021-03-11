package resourcecollector

import (
	"fmt"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
)

func (r *ResourceCollector) serviceToBeCollected(
	object runtime.Unstructured,
) (bool, error) {
	metadata, err := meta.Accessor(object)
	if err != nil {
		return false, err
	}

	// Don't migrate the kubernetes service
	if metadata.GetName() == "kubernetes" {
		return false, nil
	}
	return true, nil
}

func (r *ResourceCollector) prepareServiceResourceForCollection(
	object runtime.Unstructured,
) error {
	var service v1.Service
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(object.UnstructuredContent(), &service); err != nil {
		return fmt.Errorf("error converting to service: %v", err)
	}
	// Reset the clusterIP if it is set
	if service.Spec.ClusterIP != "None" {
		err := unstructured.SetNestedField(object.UnstructuredContent(), "", "spec", "clusterIP")
		if err != nil {
			return err
		}
	}
	// Reset the clusterIps to nil
	err := unstructured.SetNestedField(object.UnstructuredContent(), nil, "spec", "clusterIPs")
	if err != nil {
		return err
	}
	// Reset the loadBalancerIP
	return unstructured.SetNestedField(object.UnstructuredContent(), "", "spec", "loadBalancerIP")
}

func (r *ResourceCollector) updateService(
	object runtime.Unstructured,
) error {
	var service v1.Service
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(object.UnstructuredContent(), &service); err != nil {
		return err
	}

	if service.Spec.Type == v1.ServiceTypeNodePort {
		for i := range service.Spec.Ports {
			service.Spec.Ports[i].NodePort = 0
		}
		o, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&service)
		if err != nil {
			return err
		}
		object.SetUnstructuredContent(o)
	}
	return nil
}
