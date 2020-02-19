package resourcecollector

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/heptio/ark/pkg/discovery"
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
	restclient "k8s.io/client-go/rest"
	"k8s.io/kubernetes/pkg/registry/core/service/portallocator"
)

const (
	// Annotation to use when the resource shouldn't be collected
	skipResourceAnnotation = "stork.libopenstorage.org/skipresource"
	deletedMaxRetries      = 12
	deletedRetryInterval   = 10 * time.Second
)

// ResourceCollector is used to collect and process unstructured objects in namespaces and using label selectors
type ResourceCollector struct {
	Driver           volume.Driver
	discoveryHelper  discovery.Helper
	dynamicInterface dynamic.Interface
	k8sOps           k8s.Ops
}

// Init initializes the resource collector
func (r *ResourceCollector) Init(config *restclient.Config) error {
	var err error
	if config == nil {
		config, err = restclient.InClusterConfig()
		if err != nil {
			return fmt.Errorf("error getting cluster config: %v", err)
		}
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

	r.dynamicInterface, err = dynamic.NewForConfig(config)
	if err != nil {
		return err
	}

	// reset k8s instance to given cluster config
	r.k8sOps, err = k8s.NewInstanceFromRestConfig(config)
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
		"Route",
		"Template",
		"IBPCA",
		"IBPConsole",
		"IBPPeer",
		"IBPOrderer",
		"CronJob":
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

	crbs, err := r.k8sOps.ListClusterRoleBindings()
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

	allObjects, err = r.pruneOwnedResources(allObjects, resourceMap)
	if err != nil {
		return nil, err
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
	case "Role":
		return r.roleToBeCollected(object)
	case "RoleBinding":
		return r.roleBindingToBeCollected(object)
	case "Ingress":
		return r.ingressToBeCollected(object)
	}

	return true, nil
}

// Prune objects that are owned by a CRD if we are also collecting the CR
// since they will be recreated by the operator when the CR is created too.
// For objects that have an ownerRef but we aren't collecting it's owner
// remove the ownerRef so that the object doesn't get automatically deleted
// when created
func (r *ResourceCollector) pruneOwnedResources(
	objects []runtime.Unstructured,
	resourceMap map[types.UID]bool,
) ([]runtime.Unstructured, error) {
	updatedObjects := make([]runtime.Unstructured, 0)
	for _, o := range objects {
		metadata, err := meta.Accessor(o)
		if err != nil {
			return nil, err
		}

		collect := true

		objectType, err := meta.TypeAccessor(o)
		if err != nil {
			return nil, err
		}

		// Don't check for PVCs, we always want to collect them
		if objectType.GetKind() != "PersistentVolumeClaim" {
			owners := metadata.GetOwnerReferences()
			if len(owners) != 0 {
				for _, owner := range owners {
					// We don't collect pods, there might be come leader
					// election objects that could have pods as the owner, so
					// don't collect those objects
					if owner.Kind == "Pod" {
						collect = false
						break
					}

					// Skip object if we are already collecting its owner
					if _, exists := resourceMap[owner.UID]; exists {
						collect = false
						break
					}
				}
				// If the owner isn't being collected delete the owner reference
				metadata.SetOwnerReferences(nil)
			}
		}
		if collect {
			updatedObjects = append(updatedObjects, o)
		}
	}

	return updatedObjects, nil

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
		metadataMap := content["metadata"].(map[string]interface{})
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

// PrepareResourceForApply prepares the resource for apply including update
// namespace and any PV name updates. Should be called before DeleteResources
// and ApplyResource
func (r *ResourceCollector) PrepareResourceForApply(
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
		// Update the namespace of the object, will be no-op for clustered resources
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
	object runtime.Unstructured,
) bool {
	objectType, err := meta.TypeAccessor(object)
	if err != nil {
		return false
	}
	switch objectType.GetKind() {
	case "ClusterRoleBinding":
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
	object runtime.Unstructured,
) error {
	dynamicClient, err := r.getDynamicClient(dynamicInterface, object)
	if err != nil {
		return err
	}
	_, err = dynamicClient.Create(object.(*unstructured.Unstructured))
	if err != nil {
		if apierrors.IsAlreadyExists(err) || strings.Contains(err.Error(), portallocator.ErrAllocated.Error()) {
			if r.mergeSupportedForResource(object) {
				return r.mergeAndUpdateResource(object)
			} else if strings.Contains(err.Error(), portallocator.ErrAllocated.Error()) {
				err = r.updateService(object)
				if err != nil {
					return err
				}
			} else {
				return err
			}
			_, err = dynamicClient.Create(object.(*unstructured.Unstructured))
			return err
		}
	}

	return err
}

// DeleteResources deletes given resources using the provided client interface
func (r *ResourceCollector) DeleteResources(
	dynamicInterface dynamic.Interface,
	objects []runtime.Unstructured,
) error {
	// First delete all the objects
	for _, object := range objects {
		// Don't delete objects that support merging
		if r.mergeSupportedForResource(object) {
			continue
		}

		metadata, err := meta.Accessor(object)
		if err != nil {
			return err
		}

		dynamicClient, err := r.getDynamicClient(dynamicInterface, object)
		if err != nil {
			return err
		}

		// Delete the resource if it already exists on the destination
		// cluster and try creating again
		err = dynamicClient.Delete(metadata.GetName(), &metav1.DeleteOptions{})
		if err != nil && !apierrors.IsNotFound(err) {
			return err
		}
	}

	// Then wait for them to actually be deleted
	for _, object := range objects {
		// Objects that support merging aren't deleted
		if r.mergeSupportedForResource(object) {
			continue
		}

		metadata, err := meta.Accessor(object)
		if err != nil {
			return err
		}

		dynamicClient, err := r.getDynamicClient(dynamicInterface, object)
		if err != nil {
			return err
		}

		// Wait for up to 2 minutes for the object to be deleted
		for i := 0; i < deletedMaxRetries; i++ {
			_, err = dynamicClient.Get(metadata.GetName(), metav1.GetOptions{})
			if err != nil && apierrors.IsNotFound(err) {
				break
			}
			logrus.Warnf("Object %v still present, retrying in %v", metadata.GetName(), deletedRetryInterval)
			time.Sleep(deletedRetryInterval)
		}
	}
	return nil
}

func (r *ResourceCollector) getDynamicClient(
	dynamicInterface dynamic.Interface,
	object runtime.Unstructured,
) (dynamic.ResourceInterface, error) {
	metadata, err := meta.Accessor(object)
	if err != nil {
		return nil, err
	}
	objectType, err := meta.TypeAccessor(object)
	if err != nil {
		return nil, err
	}
	resource := &metav1.APIResource{
		Name:       strings.ToLower(objectType.GetKind()) + "s",
		Namespaced: len(metadata.GetNamespace()) > 0,
	}

	destNamespace := ""
	if resource.Namespaced {
		destNamespace = metadata.GetNamespace()
	}
	return dynamicInterface.Resource(
		object.GetObjectKind().GroupVersionKind().GroupVersion().WithResource(resource.Name)).Namespace(destNamespace), nil
}
