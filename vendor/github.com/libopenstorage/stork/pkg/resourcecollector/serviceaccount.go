package resourcecollector

import (
	"fmt"
	"strings"

	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
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
	if name == "default" || name == "builder" || name == "deployer" {
		if len(serviceAccount.ImagePullSecrets) > 0 {
			for _, imagePullSecret := range serviceAccount.ImagePullSecrets {
				if !strings.HasPrefix(imagePullSecret.Name, name+"-dockercfg") {
					return true, nil
				}
			}
		}
		return false, nil
	}
	return true, nil
}

func (r *ResourceCollector) mergeAndUpdateServiceAccount(
	object runtime.Unstructured,
) error {
	var newSA v1.ServiceAccount
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(object.UnstructuredContent(), &newSA); err != nil {
		return err
	}

	currentSA, err := r.coreOps.GetServiceAccount(newSA.Name, newSA.Namespace)
	if err != nil {
		if apierrors.IsNotFound(err) {
			_, err = r.coreOps.CreateServiceAccount(&newSA)
		}
		return err
	}
	imagePullSecrets := sets.NewString()
	for _, secret := range currentSA.ImagePullSecrets {
		imagePullSecrets.Insert(secret.Name)
	}
	for _, secret := range newSA.ImagePullSecrets {
		if !imagePullSecrets.Has(secret.Name) {
			currentSA.ImagePullSecrets = append(currentSA.ImagePullSecrets, secret)
		}
	}

	secrets := sets.NewString()
	for _, secret := range currentSA.Secrets {
		secrets.Insert(secret.Name)
	}
	for _, secret := range newSA.Secrets {
		if !imagePullSecrets.Has(secret.Name) {
			currentSA.Secrets = append(currentSA.Secrets, secret)
		}
	}
	// merge annotation for SA
	if newSA.Annotations != nil {
		if currentSA.Annotations == nil {
			currentSA.Annotations = make(map[string]string)
		}
		for k, v := range newSA.Annotations {
			currentSA.Annotations[k] = v
		}
	}
	_, err = r.coreOps.UpdateServiceAccount(currentSA)
	return err
}
