package resourcecollector

import (
	"fmt"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

const (
	headlessService = "service.kubernetes.io/headless"
)

func (r *ResourceCollector) endpointsToBeCollected(
	object runtime.Unstructured,
) (bool, error) {
	var endpoint v1.Endpoints
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(object.UnstructuredContent(), &endpoint); err != nil {
		return false, fmt.Errorf("error converting to endpoint: %v", err)
	}

	if endpoint.Annotations != nil {
		// collect endpoint which has include-resources annotation applied
		if _, ok := endpoint.Annotations[IncludeResources]; ok {
			return true, nil
		}
	}
	// skip endpoints which has selector set
	for _, subset := range endpoint.Subsets {
		for _, addr := range subset.Addresses {
			if addr.TargetRef != nil {
				return false, nil
			}
		}
	}
	if endpoint.Annotations != nil {
		// collect manually created endpoint resources
		if _, ok := endpoint.Annotations[v1.LastAppliedConfigAnnotation]; !ok {
			return false, nil
		}
	}
	if endpoint.Labels != nil {
		// skip collecting endpointfs for headless service
		// https://kubernetes.io/docs/reference/labels-annotations-taints/#servicekubernetesioheadless
		if _, ok := endpoint.Labels[headlessService]; ok {
			return false, nil
		}

	}
	return true, nil
}
