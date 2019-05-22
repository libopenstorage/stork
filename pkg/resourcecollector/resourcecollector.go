package resourcecollector

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/heptio/ark/pkg/discovery"
	"github.com/heptio/ark/pkg/util/collections"
	"github.com/libopenstorage/stork/drivers/volume"
	"github.com/portworx/sched-ops/k8s"
	"github.com/sirupsen/logrus"
	rbacv1 "k8s.io/api/rbac/v1"
	apiextensionsclient "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/kubernetes/pkg/registry/core/service/portallocator"
)

const (
	// Annotation to use when the resource shouldn't be collected
	skipResourceAnnotation = "stork.libopenstorage.ord/skipresource"
)

// ResourceCollector is used to collect and process unstructured objects in namespaces and using label selectors
type ResourceCollector struct {
	Driver           volume.Driver
	discoveryHelper  discovery.Helper
	dynamicInterface dynamic.Interface
}

// Init initializes the resource collector
func (r *ResourceCollector) Init() error {
	config, err := rest.InClusterConfig()
	if err != nil {
		return fmt.Errorf("error getting cluster config: %v", err)
	}

	aeclient, err := apiextensionsclient.NewForConfig(config)
	if err != nil {
		return fmt.Errorf("error getting apiextension client, %v", err)
	}

	discoveryClient := aeclient.Discovery()
	r.discoveryHelper, err = discovery.NewHelper(discoveryClient, logrus.New())
	if err != nil {
		return err
	}
	err = r.discoveryHelper.Refresh()
	if err != nil {
		return err
	}
	r.dynamicInterface, err = dynamic.NewForConfig(config)
	if err != nil {
		return err
	}
	return nil
}

func resourceToBeCollected(resource metav1.APIResource) bool {
	switch resource.Kind {
	case "PersistentVolumeClaim",
		"PersistentVolume",
		"Deployment",
		"DeploymentConfig",
		"StatefulSet",
		"ConfigMap",
		"Service",
		"Secret",
		"DaemonSet",
		"ServiceAccount",
		"Role",
		"RoleBinding",
		"ClusterRole",
		"ClusterRoleBinding",
		"ImageStream",
		"Ingress",
		"Route":
		return true
	default:
		return false
	}
}

// GetResources gets all the resources in the given list of namespaces which match the labelSelectors
func (r *ResourceCollector) GetResources(namespaces []string, labelSelectors map[string]string) ([]runtime.Unstructured, error) {
	err := r.discoveryHelper.Refresh()
	if err != nil {
		return nil, err
	}
	allObjects := make([]runtime.Unstructured, 0)

	// Map to prevent collection of duplicate objects
	resourceMap := make(map[types.UID]bool)

	crbs, err := k8s.Instance().ListClusterRoleBindings()
	if err != nil {
		return nil, err
	}

	for _, group := range r.discoveryHelper.Resources() {
		groupVersion, err := schema.ParseGroupVersion(group.GroupVersion)
		if err != nil {
			return nil, err
		}

		for _, resource := range group.APIResources {
			if !resourceToBeCollected(resource) {
				continue
			}

			for _, ns := range namespaces {
				var dynamicClient dynamic.ResourceInterface
				if !resource.Namespaced {
					dynamicClient = r.dynamicInterface.Resource(groupVersion.WithResource(resource.Name))
				} else {
					dynamicClient = r.dynamicInterface.Resource(groupVersion.WithResource(resource.Name)).Namespace(ns)
				}

				var selectors string
				// PVs don't get the labels from their PVCs, so don't use the label selector
				// Also skip for some other resources that aren't necessarily tied to an application
				switch resource.Kind {
				case "PersistentVolume",
					"ClusterRoleBinding",
					"ClusterRole",
					"ServiceAccount":
				default:
					selectors = labels.Set(labelSelectors).String()
				}
				objectsList, err := dynamicClient.List(metav1.ListOptions{
					LabelSelector: selectors,
				})
				if err != nil {
					return nil, err
				}
				objects, err := meta.ExtractList(objectsList)
				if err != nil {
					return nil, err
				}
				for _, o := range objects {
					runtimeObject, ok := o.(runtime.Unstructured)
					if !ok {
						return nil, fmt.Errorf("error casting object: %v", o)
					}

					collect, err := r.objectToBeCollected(labelSelectors, resourceMap, runtimeObject, crbs, ns)
					if err != nil {
						return nil, fmt.Errorf("error processing object %v: %v", runtimeObject, err)
					}
					if !collect {
						continue
					}
					metadata, err := meta.Accessor(runtimeObject)
					if err != nil {
						return nil, err
					}
					allObjects = append(allObjects, runtimeObject)
					resourceMap[metadata.GetUID()] = true
				}
			}
		}
	}

	err = r.prepareResourcesForCollection(allObjects, namespaces)
	if err != nil {
		return nil, err
	}
	return allObjects, nil
}

// Returns whether an object should be collected or not for the requested
// namespace
func (r *ResourceCollector) objectToBeCollected(
	labelSelectors map[string]string,
	resourceMap map[types.UID]bool,
	object runtime.Unstructured,
	crbs *rbacv1.ClusterRoleBindingList,
	namespace string,
) (bool, error) {
	metadata, err := meta.Accessor(object)
	if err != nil {
		return false, err
	}

	if value, present := metadata.GetAnnotations()[skipResourceAnnotation]; present {
		if skip, err := strconv.ParseBool(value); err == nil && skip {
			return false, err
		}
	}

	// Skip if we've already processed this object
	if _, ok := resourceMap[metadata.GetUID()]; ok {
		return false, nil
	}

	objectType, err := meta.TypeAccessor(object)
	if err != nil {
		return false, err
	}

	switch objectType.GetKind() {
	case "Service":
		return r.serviceToBeCollected(object)
	case "PersistentVolumeClaim":
		return r.pvcToBeCollected(object, namespace)
	case "PersistentVolume":
		return r.pvToBeCollected(labelSelectors, object, namespace)
	case "ClusterRoleBinding":
		return r.clusterRoleBindingToBeCollected(labelSelectors, object, namespace)
	case "ClusterRole":
		return r.clusterRoleToBeCollected(labelSelectors, object, crbs, namespace)
	case "ServiceAccount":
		return r.serviceAccountToBeCollected(object)
	case "Secret":
		return r.secretToBeCollected(object)
	}

	return true, nil
}

func (r *ResourceCollector) prepareResourcesForCollection(
	objects []runtime.Unstructured,
	namespaces []string,
) error {
	for _, o := range objects {
		metadata, err := meta.Accessor(o)
		if err != nil {
			return err
		}

		switch o.GetObjectKind().GroupVersionKind().Kind {
		case "PersistentVolume":
			err := r.preparePVResourceForCollection(o)
			if err != nil {
				return fmt.Errorf("error preparing PV resource %v: %v", metadata.GetName(), err)
			}
		case "Service":
			err := r.prepareServiceResourceForCollection(o)
			if err != nil {
				return fmt.Errorf("error preparing Service resource %v/%v: %v", metadata.GetNamespace(), metadata.GetName(), err)
			}
		case "ClusterRoleBinding":
			err := r.prepareClusterRoleBindingForCollection(o, namespaces)
			if err != nil {
				return fmt.Errorf("error preparing ClusterRoleBindings resource %v: %v", metadata.GetName(), err)
			}
		}

		content := o.UnstructuredContent()
		// Status shouldn't be retained when collecting resources
		delete(content, "status")
		metadataMap, err := collections.GetMap(content, "metadata")
		if err != nil {
			return fmt.Errorf("error getting metadata for resource %v: %v", metadata.GetName(), err)
		}
		// Remove all metadata except some well-known ones
		for key := range metadataMap {
			switch key {
			case "name", "namespace", "labels", "annotations":
			default:
				delete(metadataMap, key)
			}
		}
	}
	return nil
}

func (r *ResourceCollector) prepareResourceForApply(
	object runtime.Unstructured,
	namespaceMappings map[string]string,
	pvNameMappings map[string]string,
) error {
	objectType, err := meta.TypeAccessor(object)
	if err != nil {
		return err
	}

	metadata, err := meta.Accessor(object)
	if err != nil {
		return err
	}
	if metadata.GetNamespace() != "" {
		// Update the namepsace of the object, will be no-op for clustered resources
		metadata.SetNamespace(namespaceMappings[metadata.GetNamespace()])
	}

	switch objectType.GetKind() {
	case "PersistentVolume":
		return r.preparePVResourceForApply(object, pvNameMappings)
	case "PersistentVolumeClaim":
		return r.preparePVCResourceForApply(object, pvNameMappings)
	case "ClusterRoleBinding":
		err := r.prepareClusterRoleBindingForApply(object, namespaceMappings)
		if err != nil {
			return err
		}

	}
	return nil
}

func (r *ResourceCollector) mergeSupportedForResource(
	resourceName string,
) bool {
	switch resourceName {
	case "ClusterRoleBindings":
		return true
	}
	return false
}

func (r *ResourceCollector) mergeAndUpdateResource(
	object runtime.Unstructured,
) error {
	objectType, err := meta.TypeAccessor(object)
	if err != nil {
		return err
	}

	switch objectType.GetKind() {
	case "ClusterRoleBinding":
		return r.mergeAndUpdateClusterRoleBinding(object)
	}
	return nil
}

// ApplyResource applies a given resource using the provided client interface
func (r *ResourceCollector) ApplyResource(
	dynamicInterface dynamic.Interface,
	object *unstructured.Unstructured,
	pvNameMappings map[string]string,
	namespaceMappings map[string]string,
	deleteIfPresent bool,
) error {
	metadata, err := meta.Accessor(object)
	if err != nil {
		return err
	}
	objectType, err := meta.TypeAccessor(object)
	if err != nil {
		return err
	}
	resource := &metav1.APIResource{
		Name:       strings.ToLower(objectType.GetKind()) + "s",
		Namespaced: len(metadata.GetNamespace()) > 0,
	}

	destNamespace := ""
	if resource.Namespaced {
		destNamespace = namespaceMappings[metadata.GetNamespace()]
	}
	dynamicClient := dynamicInterface.Resource(
		object.GetObjectKind().GroupVersionKind().GroupVersion().WithResource(resource.Name)).Namespace(destNamespace)

	err = r.prepareResourceForApply(object, namespaceMappings, pvNameMappings)
	if err != nil {
		return err
	}

	_, err = dynamicClient.Create(object)
	if err != nil && (apierrors.IsAlreadyExists(err) || strings.Contains(err.Error(), portallocator.ErrAllocated.Error())) {
		if r.mergeSupportedForResource(resource.Name) {
			err := r.mergeAndUpdateResource(object)
			if err != nil {
				return err
			}
		} else if deleteIfPresent {
			// Delete the resource if it already exists on the destination
			// cluster and try creating again
			err = dynamicClient.Delete(metadata.GetName(), &metav1.DeleteOptions{})
			if err == nil {
				_, err = dynamicClient.Create(object)
			} else {
				return err
			}
		}
	}

	return err
}
