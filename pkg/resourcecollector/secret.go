package resourcecollector

import (
	"github.com/sirupsen/logrus"
	"k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

func (r *ResourceCollector) secretToBeCollected(
	object runtime.Unstructured,
) (bool, error) {
	var secret v1.Secret
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(object.UnstructuredContent(), &secret); err != nil {
		logrus.Errorf("Error converting Secret object %v: %v", object, err)
		return false, err
	}
	// Don't collect secret which store the default service account token
	if secret.Type == v1.SecretTypeServiceAccountToken {
		if accountName, ok := secret.Annotations[v1.ServiceAccountNameKey]; ok && accountName == "default" {
			return false, nil
		}
	}
	return true, nil

}
