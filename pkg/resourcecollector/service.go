package resourcecollector

import (
	"github.com/heptio/ark/pkg/util/collections"
	"k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
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
	spec, err := collections.GetMap(object.UnstructuredContent(), "spec")
	if err != nil {
		return err
	}
	// Don't delete clusterIP for headless services
	if ip, err := collections.GetString(spec, "clusterIP"); err == nil && ip != "None" {
		delete(spec, "clusterIP")
	}
	delete(spec, "loadBalancerIP")

	return nil
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
