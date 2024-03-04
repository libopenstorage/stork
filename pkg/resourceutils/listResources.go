package resourceutils

import (
	"context"
	"fmt"
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

var statefulSetGVK = metav1.GroupVersionKind{Group: "apps", Version: "v1", Kind: "StatefulSet"}
var deploymentGVK = metav1.GroupVersionKind{Group: "apps", Version: "v1", Kind: "Deployment"}
var replicaSetGVK = metav1.GroupVersionKind{Group: "apps", Version: "v1", Kind: "ReplicaSet"}
var deploymentConfigGVK = metav1.GroupVersionKind{Group: "apps.openshift.io", Version: "v1", Kind: "DeploymentConfig"}
var cronJobGVK = metav1.GroupVersionKind{Group: "batch", Version: "v1", Kind: "CronJob"}
var virtualMachineGVK = metav1.GroupVersionKind{Group: "kubevirt.io", Version: "v1", Kind: "VirtualMachine"}
var ibpPeerGVK = metav1.GroupVersionKind{Group: "ibp.com", Version: "v1alpha1", Kind: "IBPPeer"}
var ibpcaGVK = metav1.GroupVersionKind{Group: "ibp.com", Version: "v1alpha1", Kind: "IBPCA"}
var ibpConsoleGVK = metav1.GroupVersionKind{Group: "ibp.com", Version: "v1alpha1", Kind: "IBPOrderer"}
var ibpOrdererGVK = metav1.GroupVersionKind{Group: "ibp.com", Version: "v1alpha1", Kind: "IBPConsole"}

func getRelevantGVKs() []metav1.GroupVersionKind {
	return []metav1.GroupVersionKind{statefulSetGVK, deploymentGVK, replicaSetGVK, deploymentConfigGVK, cronJobGVK, virtualMachineGVK, ibpPeerGVK, ibpcaGVK, ibpOrdererGVK, ibpConsoleGVK}
}

func ListResourcesByGVK(namespace string, config *rest.Config, resourceType metav1.GroupVersionKind) (*unstructured.UnstructuredList, error) {
	//Setup dynamic client to get resources based on gvk
	var client k8sdynamic.ResourceInterface
	ruleset := resourcecollector.GetDefaultRuleSet()
	configClient, err := k8sdynamic.NewForConfig(config)
	if err != nil {
		return nil, err
	}
	opts := &metav1.ListOptions{
		TypeMeta: metav1.TypeMeta{
			Kind:       resourceType.Kind,
			APIVersion: resourceType.Group + "/" + resourceType.Version},
	}
	gvk := schema.FromAPIVersionAndKind(opts.APIVersion, opts.Kind)
	resourceInterface := configClient.Resource(gvk.GroupVersion().WithResource(ruleset.Pluralize(strings.ToLower(gvk.Kind))))
	client = resourceInterface.Namespace(namespace)
	objects, err := client.List(context.TODO(), *opts)
	if err != nil && !errors.IsNotFound(err) {
		return nil, err
	}
	for _, obj := range objects.Items {
		fmt.Println(obj.GetName())
	}
	return objects, nil
}

func GenerateDynamicClientForGVK(resourceType metav1.GroupVersionKind, namespace string, config *rest.Config) (k8sdynamic.ResourceInterface, error) {
	var client k8sdynamic.ResourceInterface
	ruleset := resourcecollector.GetDefaultRuleSet()
	configClient, err := k8sdynamic.NewForConfig(config)
	if err != nil {
		return nil, err
	}
	opts := &metav1.UpdateOptions{
		TypeMeta: metav1.TypeMeta{
			Kind:       resourceType.Kind,
			APIVersion: resourceType.Group + "/" + resourceType.Version},
	}
	gvk := schema.FromAPIVersionAndKind(opts.APIVersion, opts.Kind)
	resourceInterface := configClient.Resource(gvk.GroupVersion().WithResource(ruleset.Pluralize(strings.ToLower(gvk.Kind))))
	client = resourceInterface.Namespace(namespace)
	return client, nil
}

func GetResourcesBeingActivated(activationNamespaces []string, config *rest.Config) (map[string]map[metav1.GroupVersionKind]map[string]string, error) {
	//TODO: Set correct config for storkops
	crdList, err := storkops.Instance().ListApplicationRegistrations()
	if err != nil {
		return nil, err
	}
	resourceReplicaMap := make(map[string]map[metav1.GroupVersionKind]map[string]string)
	for _, ns := range activationNamespaces {
		//Initialise namespace -> resource kind maps
		resourceReplicaMap[ns] = make(map[metav1.GroupVersionKind]map[string]string)

		// 1. ApplicationResources
		for _, gvk := range getRelevantGVKs() {
			//Initialise namespace/resourceKind -> resources/replicas maps
			resourceReplicaMap[ns][gvk] = make(map[string]string)
			resources, err := ListResourcesByGVK(ns, config, gvk)
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
				resources, err := ListResourcesByGVK(ns, config, gvk)
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
							}
							// for suspend.Type == "bool" we update irrespective of annotation presence
						}
					}
				}
			}
		}
	}
	return resourceReplicaMap, nil
}
