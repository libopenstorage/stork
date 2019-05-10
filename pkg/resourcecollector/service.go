package resourcecollector

import (
	"github.com/heptio/ark/pkg/util/collections"
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
