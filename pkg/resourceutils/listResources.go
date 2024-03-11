package resourceutils

import (
	"context"
	"fmt"
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
			resources, _, err := ListResourcesByGVK(gvk, ns, config)
			if err != nil {
				return nil, err
			}
			if resources == nil {
				continue
			}
			for _, object := range resources.Items {
				content := object.UnstructuredContent()
				replicaSetGVK := metav1.GroupVersionKind{Group: "apps", Version: "v1", Kind: "ReplicaSet"}
				if gvk == replicaSetGVK {
					ownerReferences, _, err := unstructured.NestedSlice(content, "metadata", "ownerReferences")
					if err != nil {
						return nil, fmt.Errorf("error getting the ownerReferences for the replicaSet: %v/%v -> %v", object.GetNamespace(), object.GetName(), err)
					}
					if ownerReferences != nil {
						// only collect replicaSets without owners
						continue
					}
				}
				annotations, found, err := unstructured.NestedStringMap(content, "metadata", "annotations")
				if err != nil {
					return nil, fmt.Errorf("error getting the annotations of the resource %v %v/%v: %v", strings.ToLower(gvk.Kind), object.GetNamespace(), object.GetName(), err)
				}
				if !found {
					continue
				}
				// only if replica annotation exists, we will be activating this resource
				if val, present := annotations[migration.StorkMigrationReplicasAnnotation]; present {
					resourceReplicaMap[ns][gvk][object.GetName()] = val
				}
			}
		}

		// 2. CronJobs
		resourceReplicaMap[ns][cronJobGVK] = make(map[string]string)
		resources, _, err := ListResourcesByGVK(cronJobGVK, ns, config)
		if err != nil {
			return nil, err
		}
		if resources != nil {
			for _, object := range resources.Items {
				content := object.UnstructuredContent()
				annotations, found, err := unstructured.NestedStringMap(content, "metadata", "annotations")
				if err != nil {
					return nil, fmt.Errorf("error getting the annotations of the resource %v %v/%v: %v", strings.ToLower(cronJobGVK.Kind), object.GetNamespace(), object.GetName(), err)
				}
				if !found {
					continue
				}
				// pick only if stork-migrated annotation exists in the resource
				if val, present := annotations[migration.StorkMigrationAnnotation]; present {
					resourceReplicaMap[ns][cronJobGVK][object.GetName()] = val
				}
			}
		}

		// 3. CRD Resources
		for _, applicationRegistration := range crdList.Items {
			for _, crd := range applicationRegistration.Resources {
				gvk := crd.GroupVersionKind
				// Initialise namespace/resourceKind -> resources/replicas maps
				resourceReplicaMap[ns][gvk] = make(map[string]string)
				resources, _, err := ListResourcesByGVK(gvk, ns, config)
				if err != nil {
					return nil, err
				}
				if resources == nil {
					continue
				}
				for _, object := range resources.Items {
					content := object.UnstructuredContent()
					annotations, found, err := unstructured.NestedStringMap(content, "metadata", "annotations")
					if err != nil {
						return nil, fmt.Errorf("error getting the annotations of the resource %v %v/%v: %v", strings.ToLower(gvk.Kind), object.GetNamespace(), object.GetName(), err)
					}
					if !found {
						continue
					}
					// only if replica annotation exists, we will be activating this resource
					if crd.SuspendOptions.Path != "" {
						crd.NestedSuspendOptions = append(crd.NestedSuspendOptions, crd.SuspendOptions)
					}
					if len(crd.NestedSuspendOptions) == 0 {
						continue
					}
					for _, suspend := range crd.NestedSuspendOptions {
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
								suspendVal, _ := strconv.ParseBool(suspend.Value)
								val := !suspendVal
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
