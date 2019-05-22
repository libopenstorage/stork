package resourcecollector

import (
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
)

func (r *ResourceCollector) serviceAccountToBeCollected(
	object runtime.Unstructured,
) (bool, error) {
	metadata, err := meta.Accessor(object)
	if err != nil {
		return false, err
	}

	// Don't migrate the default service account
	name := metadata.GetName()
	return (name != "default" && name != "builder" && name != "deployer"), nil
}
