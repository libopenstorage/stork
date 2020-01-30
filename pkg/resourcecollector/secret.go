package resourcecollector

import (
	"strings"

	"github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
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

	autoCreatedPrefixes := []string{"builder-dockercfg-", "builder-token-", "default-dockercfg-", "default-token-", "deployer-dockercfg-", "deployer-token-"}
	for _, prefix := range autoCreatedPrefixes {
		if strings.HasPrefix(secret.Name, prefix) {
			return false, nil
		}
	}
	return true, nil

}
