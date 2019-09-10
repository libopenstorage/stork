package resourcecollector

import (
	"fmt"

	v1 "k8s.io/api/core/v1"
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
	var serviceAccount v1.ServiceAccount
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(object.UnstructuredContent(), &serviceAccount); err != nil {
		return false, fmt.Errorf("error converting to serviceAccount: %v", err)
	}
	if name == "default" && len(serviceAccount.ImagePullSecrets) > 0 {
		return true, nil
	}
	return (name != "default" && name != "builder" && name != "deployer"), nil
}
