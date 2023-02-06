package resourcecollector

import (
	"strings"

	"github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

const (
	serviceAccountUIDKey = "kubernetes.io/service-account.uid"
)

func (r *ResourceCollector) secretToBeCollected(
	object runtime.Unstructured,
) (bool, error) {
	var secret v1.Secret
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(object.UnstructuredContent(), &secret); err != nil {
		logrus.Errorf("Error converting Secret object %v: %v", object, err)
		return false, err
	}

	autoCreatedPrefixes := []string{"builder-dockercfg-", "builder-token-", "default-dockercfg-", "default-token-", "deployer-dockercfg-", "deployer-token-"}
	for _, prefix := range autoCreatedPrefixes {
		if strings.HasPrefix(secret.Name, prefix) {
			return false, nil
		}
	}
	return true, nil

}

func (r *ResourceCollector) prepareSecretForApply(
	object runtime.Unstructured,
) error {
	var secret v1.Secret
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(object.UnstructuredContent(), &secret); err != nil {
		logrus.Errorf("Error converting Secret object %v: %v", object, err)
		return err
	}
	// Reset the " kubernetes.io/service-account.uid" annotation,
	// so that it will update the uid of the newly created SA after restoring
	if secret.Annotations != nil {
		if _, ok := secret.Annotations[serviceAccountUIDKey]; ok {
			secret.Annotations[serviceAccountUIDKey] = ""
			// Reset the secret token data to empty, so that new service account token will be updated by k8s, during restore.
			if secret.Data["token"] != nil {
				secret.Data["token"] = nil
			}
		}
	}
	o, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&secret)
	if err != nil {
		return err
	}
	object.SetUnstructuredContent(o)
	return err
}
