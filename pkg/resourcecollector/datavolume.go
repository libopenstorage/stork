package resourcecollector

import "k8s.io/apimachinery/pkg/runtime"

func (r *ResourceCollector) dataVolumesToBeCollected(
	object runtime.Unstructured,
) (bool, error) {
	return false, nil
}
