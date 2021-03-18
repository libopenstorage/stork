package resourcecollector

import (
	"strings"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
)

func (r *ResourceCollector) configmapToBeCollected(
	object runtime.Unstructured,
) (bool, error) {
	metadata, err := meta.Accessor(object)
	if err != nil {
		return false, err
	}
	// excludeConfigMap is a array of configmap that should not be
	// included in GetResources API as they are controlled by some controller,
	// "kube-root-ca.crt" is a configmap that is created in every namespace by
	// kube-controller-manager from k8s 1.20.0 onwards.
	excludeConfigMap := []string{"kube-root-ca.crt"}

	for _, name := range excludeConfigMap {
		ret := strings.Compare(metadata.GetName(), name)
		if ret == 0 {
			return false, nil
		}
	}
	return true, nil
}
