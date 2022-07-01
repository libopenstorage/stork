package resourcecollector

import (
	"fmt"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

func (r *ResourceCollector) endpointsToBeCollected(
	object runtime.Unstructured,
) (bool, error) {
	var endpoint v1.Endpoints
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(object.UnstructuredContent(), &endpoint); err != nil {
		return false, fmt.Errorf("error converting to endpoint: %v", err)
	}

	if endpoint.Annotations != nil {
		if _, ok := endpoint.Annotations["last_applied_annotation"]; ok {
			return true, nil
		}
		if _, ok := endpoint.Annotations["collect_resource"]; ok {
			return true, nil
		}
	}

	return false, nil
}
