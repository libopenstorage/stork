package resourcecollector

import (
	"strings"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
)

func (r *ResourceCollector) roleBindingToBeCollected(
	object runtime.Unstructured,
) (bool, error) {
	metadata, err := meta.Accessor(object)
	if err != nil {
		return false, err
	}

	name := metadata.GetName()
	return !strings.HasPrefix(name, "system:"), nil
}

func (r *ResourceCollector) roleToBeCollected(
	object runtime.Unstructured,
) (bool, error) {
	metadata, err := meta.Accessor(object)
	if err != nil {
		return false, err
	}

	name := metadata.GetName()
	return !strings.HasPrefix(name, "system:"), nil
}
