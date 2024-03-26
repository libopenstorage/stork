package resourceutils

import (
	"context"
	"fmt"
	"strings"

	"github.com/libopenstorage/stork/pkg/resourcecollector"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	k8sdynamic "k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
)

func GetGVKsRelevantForScaling() []metav1.GroupVersionKind {
	return []metav1.GroupVersionKind{
		{Group: "apps", Version: "v1", Kind: "StatefulSet"},
		{Group: "apps", Version: "v1", Kind: "Deployment"},
		{Group: "apps", Version: "v1", Kind: "ReplicaSet"},
		{Group: "apps.openshift.io", Version: "v1", Kind: "DeploymentConfig"},
		{Group: "ibp.com", Version: "v1alpha1", Kind: "IBPPeer"},
		{Group: "ibp.com", Version: "v1alpha1", Kind: "IBPCA"},
		{Group: "ibp.com", Version: "v1alpha1", Kind: "IBPOrderer"},
		{Group: "ibp.com", Version: "v1alpha1", Kind: "IBPConsole"},
	}
}

var cronJobGVK = metav1.GroupVersionKind{Group: "batch", Version: "v1", Kind: "CronJob"}

func GenerateDynamicClientForGVK(resourceType metav1.GroupVersionKind, namespace string, config *rest.Config) (k8sdynamic.ResourceInterface, error) {
	var client k8sdynamic.ResourceInterface
	ruleset := resourcecollector.GetDefaultRuleSet()
	configClient, err := k8sdynamic.NewForConfig(config)
	if err != nil {
		return nil, err
	}
	gvk := schema.FromAPIVersionAndKind(resourceType.Group+"/"+resourceType.Version, resourceType.Kind)
	resourceInterface := configClient.Resource(gvk.GroupVersion().WithResource(ruleset.Pluralize(strings.ToLower(gvk.Kind))))
	client = resourceInterface.Namespace(namespace)
	return client, nil
}

func ListResourcesByGVK(resourceType metav1.GroupVersionKind, namespace string, config *rest.Config) (*unstructured.UnstructuredList, k8sdynamic.ResourceInterface, error) {
	//Setup dynamic client to get resources based on gvk
	client, err := GenerateDynamicClientForGVK(resourceType, namespace, config)
	if err != nil {
		return nil, nil, err
	}
	opts := &metav1.ListOptions{
		TypeMeta: metav1.TypeMeta{
			Kind:       resourceType.Kind,
			APIVersion: resourceType.Group + "/" + resourceType.Version},
	}
	objects, err := client.List(context.TODO(), *opts)
	if err != nil && !errors.IsNotFound(err) {
		return nil, nil, fmt.Errorf("error listing the resources of the type: %v in namespace: %v -> %v", strings.ToLower(resourceType.String()), namespace, err)
	}
	return objects, client, nil
}
