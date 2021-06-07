package resourcecollector

import (
	"strings"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
)

func (r *ResourceCollector) resourceQuotaToBeCollected(
	object runtime.Unstructured,
) (bool, error) {
	metadata, err := meta.Accessor(object)
	if err != nil {
		return false, err
	}
	// Excluding gke-resource-quotas as this is created per ns on GKE
	excludeResourceQuota := []string{"gke-resource-quotas"}

	for _, name := range excludeResourceQuota {
		if strings.Compare(metadata.GetName(), name) == 0 {
			return false, nil
		}
	}
	return true, nil
}
