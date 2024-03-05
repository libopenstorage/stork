package resourceutils

import (
	"context"
	"strconv"
	"strings"

	migration "github.com/libopenstorage/stork/pkg/migration/controllers"
	"github.com/libopenstorage/stork/pkg/resourcecollector"
	storkops "github.com/portworx/sched-ops/k8s/stork"
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
		{Group: "batch", Version: "v1", Kind: "CronJob"},
		{Group: "kubevirt.io", Version: "v1", Kind: "VirtualMachine"},
		{Group: "ibp.com", Version: "v1alpha1", Kind: "IBPPeer"},
		{Group: "ibp.com", Version: "v1alpha1", Kind: "IBPCA"},
		{Group: "ibp.com", Version: "v1alpha1", Kind: "IBPOrderer"},
		{Group: "ibp.com", Version: "v1alpha1", Kind: "IBPConsole"},
	}
}

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

func ListResourcesByGVK(resourceType metav1.GroupVersionKind, namespace string, config *rest.Config) (*unstructured.UnstructuredList, error) {
	//Setup dynamic client to get resources based on gvk
	client, err := GenerateDynamicClientForGVK(resourceType, namespace, config)
	if err != nil {
		return nil, err
	}
	opts := &metav1.ListOptions{
		TypeMeta: metav1.TypeMeta{
			Kind:       resourceType.Kind,
			APIVersion: resourceType.Group + "/" + resourceType.Version},
	}
	objects, err := client.List(context.TODO(), *opts)
	if err != nil && !errors.IsNotFound(err) {
		return nil, err
	}
	return objects, nil
}

func GetResourcesBeingActivated(activationNamespaces []string, config *rest.Config) (map[string]map[metav1.GroupVersionKind]map[string]string, error) {
	storkClient, err := storkops.NewForConfig(config)
	if err != nil {
		return nil, err
	}
	crdList, err := storkClient.ListApplicationRegistrations()
	if err != nil {
		return nil, err
	}
	resourceReplicaMap := make(map[string]map[metav1.GroupVersionKind]map[string]string)
	for _, ns := range activationNamespaces {
		//Initialise namespace -> resource kind maps
		resourceReplicaMap[ns] = make(map[metav1.GroupVersionKind]map[string]string)

		// 1. ApplicationResources
		for _, gvk := range GetGVKsRelevantForScaling() {
			//Initialise namespace/resourceKind -> resources/replicas maps
			resourceReplicaMap[ns][gvk] = make(map[string]string)
			resources, err := ListResourcesByGVK(gvk, ns, config)
			if err != nil {
				return nil, err
			}
			for _, object := range resources.Items {
				content := object.UnstructuredContent()
				annotations, found, err := unstructured.NestedStringMap(content, "metadata", "annotations")
				if err != nil {
					return nil, err
				}
				if !found {
					continue
				}
				//Only if replica annotation exists, we will be activating this resource
				if val, present := annotations[migration.StorkMigrationReplicasAnnotation]; present {
					resourceReplicaMap[ns][gvk][object.GetName()] = val
				}
			}
		}

		// 2. CRD Resources
		for _, applicationRegistration := range crdList.Items {
			for _, applicationResource := range applicationRegistration.Resources {
				gvk := applicationResource.GroupVersionKind
				//Initialise namespace/resourceKind -> resources/replicas maps
				resourceReplicaMap[ns][gvk] = make(map[string]string)
				resources, err := ListResourcesByGVK(gvk, ns, config)
				if err != nil {
					return nil, err
				}
				for _, object := range resources.Items {
					content := object.UnstructuredContent()
					annotations, found, err := unstructured.NestedStringMap(content, "metadata", "annotations")
					if err != nil {
						return nil, err
					}
					if !found {
						continue
					}
					//Only if replica annotation exists, we will be activating this resource
					if applicationResource.SuspendOptions.Path != "" {
						applicationResource.NestedSuspendOptions = append(applicationResource.NestedSuspendOptions, applicationResource.SuspendOptions)
					}
					if len(applicationResource.NestedSuspendOptions) == 0 {
						continue
					}
					for _, suspend := range applicationResource.NestedSuspendOptions {
						specPath := strings.Split(suspend.Path, ".")
						if len(specPath) > 1 {
							if suspend.Type == "int" || suspend.Type == "string" {
								if val, present := annotations[migration.StorkAnnotationPrefix+suspend.Path]; present {
									resourceReplicaMap[ns][gvk][object.GetName()] = val
								} else if val, present := annotations[migration.StorkMigrationCRDActivateAnnotation]; present {
									resourceReplicaMap[ns][gvk][object.GetName()] = val
								}
							} else if suspend.Type == "bool" {
								// bool type crd resources are always activated
								val, present, err := unstructured.NestedBool(content, specPath...)
								if err != nil {
									return nil, err
								}
								if !present {
									suspendVal, _ := strconv.ParseBool(suspend.Value)
									val = !suspendVal
								}
								resourceReplicaMap[ns][gvk][object.GetName()] = strconv.FormatBool(val)
							}
						}
					}
				}
			}
		}
	}
	return resourceReplicaMap, nil
}
