package resourcecollector

import (
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
)

func (r *ResourceCollector) ingressToBeCollected(
	object runtime.Unstructured,
) (bool, error) {
	metadata, err := meta.Accessor(object)
	if err != nil {
		return false, err
	}

	return metadata.GetNamespace() != "", nil
}
