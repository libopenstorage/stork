package resourceutils

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	migration "github.com/libopenstorage/stork/pkg/migration/controllers"
	"github.com/libopenstorage/stork/pkg/resourcecollector"
	"github.com/portworx/sched-ops/k8s/core"
	storkops "github.com/portworx/sched-ops/k8s/stork"
	log "github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	k8sdynamic "k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
)

func ScaleUpResourcesInNamespace(namespace string, dryRun bool, config *rest.Config) (map[metav1.GroupVersionKind]map[string]string, error) {
	resourceReplicaMap := make(map[metav1.GroupVersionKind]map[string]string)
	// 1. ApplicationResources
	err := processActivatingApplicationResources(resourceReplicaMap, namespace, dryRun, config)
	if err != nil {
		return nil, err
	}
	// 2. CronJobs
	err = processActivatingCronJobs(resourceReplicaMap, namespace, dryRun, config)
	if err != nil {
		return nil, err
	}
	// 3. CRD Resources
	err = processActivatingCRDResources(resourceReplicaMap, namespace, dryRun, config)
	if err != nil {
		return nil, err
	}
	//4. Stash CM resources
	if !dryRun {
		// call this function only during actual scaleUp scenario
		err = scaleUpStashCMResources(namespace, config)
		if err != nil {
			return nil, err
		}
	}
	return resourceReplicaMap, nil
}

func processActivatingApplicationResources(resourceReplicaMap map[metav1.GroupVersionKind]map[string]string, ns string, dryRun bool, config *rest.Config) error {
	for _, gvk := range GetGVKsRelevantForScaling() {
		//Initialise resourceKind -> resources/replicas maps
		resourceReplicaMap[gvk] = make(map[string]string)
		resources, dynamicClient, err := ListResourcesByGVK(gvk, ns, config)
		if err != nil {
			return err
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
					return fmt.Errorf("error getting the ownerReferences for the replicaSet: %v/%v -> %v", object.GetNamespace(), object.GetName(), err)
				}
				if ownerReferences != nil {
					// only collect replicaSets without owners
					continue
				}
			}
			annotations, found, err := unstructured.NestedStringMap(content, "metadata", "annotations")
			if err != nil {
				return fmt.Errorf("error getting the annotations of the resource %v %v/%v: %v", strings.ToLower(gvk.Kind), object.GetNamespace(), object.GetName(), err)
			}
			if !found {
				continue
			}
			// only if replica annotation exists, we will be activating this resource
			if val, present := annotations[migration.StorkMigrationReplicasAnnotation]; present {
				if !dryRun {
					// actual activation i.e. scaling up of resources is controlled by the dryRun flag
					err := scaleUpApplicationResources(dynamicClient, object, val, gvk)
					if err != nil {
						return err
					}
				}
				resourceReplicaMap[gvk][object.GetName()] = val
			}
		}
	}
	return nil
}

func processActivatingCronJobs(resourceReplicaMap map[metav1.GroupVersionKind]map[string]string, ns string, dryRun bool, config *rest.Config) error {
	resourceReplicaMap[cronJobGVK] = make(map[string]string)
	resources, dynamicClient, err := ListResourcesByGVK(cronJobGVK, ns, config)
	if err != nil {
		return err
	}
	if resources != nil {
		for _, object := range resources.Items {
			content := object.UnstructuredContent()
			annotations, found, err := unstructured.NestedStringMap(content, "metadata", "annotations")
			if err != nil {
				return fmt.Errorf("error getting the annotations of the resource %v %v/%v: %v", strings.ToLower(cronJobGVK.Kind), object.GetNamespace(), object.GetName(), err)
			}
			if !found {
				continue
			}
			// pick only if stork-migrated annotation exists in the resource
			if val, present := annotations[migration.StorkMigrationAnnotation]; present {
				if !dryRun {
					err := scaleCronJob(dynamicClient, object, false)
					if err != nil {
						return err
					}
				}
				resourceReplicaMap[cronJobGVK][object.GetName()] = val
			}
		}
	}
	return nil
}

func processActivatingCRDResources(resourceReplicaMap map[metav1.GroupVersionKind]map[string]string, ns string, dryRun bool, config *rest.Config) error {
	storkClient, err := storkops.NewForConfig(config)
	if err != nil {
		return err
	}
	crdList, err := storkClient.ListApplicationRegistrations()
	if err != nil {
		return err
	}
	for _, applicationRegistration := range crdList.Items {
		for _, crd := range applicationRegistration.Resources {
			gvk := crd.GroupVersionKind
			// Initialise namespace/resourceKind -> resources/replicas maps
			resourceReplicaMap[gvk] = make(map[string]string)
			resources, dynamicClient, err := ListResourcesByGVK(gvk, ns, config)
			if err != nil {
				return err
			}
			if resources == nil {
				continue
			}
			for _, object := range resources.Items {
				content := object.UnstructuredContent()
				annotations, found, err := unstructured.NestedStringMap(content, "metadata", "annotations")
				if err != nil {
					return fmt.Errorf("error getting the annotations of the resource %v %v/%v: %v", strings.ToLower(gvk.Kind), object.GetNamespace(), object.GetName(), err)
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
						var replicas interface{}
						if suspend.Type == "int" || suspend.Type == "string" {
							var suspendAnnotationValue string
							if val, present := annotations[migration.StorkAnnotationPrefix+suspend.Path]; present {
								suspendAnnotationValue = strings.Split(val, ",")[0]
								resourceReplicaMap[gvk][object.GetName()] = suspendAnnotationValue
								replicas = suspendAnnotationValue
								if suspend.Type == "int" {
									parsedReplicas, err := strconv.Atoi(suspendAnnotationValue)
									if err != nil {
										return fmt.Errorf("error parsing replicas for %v %v/%v: %v", strings.ToLower(crd.Kind), object.GetNamespace(), object.GetName(), err)
									}
									replicas = int64(parsedReplicas)
								}
							} else if val, present := annotations[migration.StorkMigrationCRDActivateAnnotation]; present {
								suspendAnnotationValue = val
								replicas = suspendAnnotationValue
								if suspend.Type == "int" {
									parsedReplicas, err := strconv.Atoi(suspendAnnotationValue)
									if err != nil {
										return fmt.Errorf("error parsing replicas for %v %v/%v: %v", strings.ToLower(crd.Kind), object.GetNamespace(), object.GetName(), err)
									}
									replicas = int64(parsedReplicas)
								}
							} else {
								// Neither of the 2 relevant annotations found. So we will not be activating this resource
								continue
							}
							if !dryRun {
								err := scaleUpCRDResources(dynamicClient, object, crd.Kind, suspend, replicas)
								if err != nil {
									return err
								}
							}
							resourceReplicaMap[gvk][object.GetName()] = suspendAnnotationValue
						} else if suspend.Type == "bool" {
							suspendVal, err := strconv.ParseBool(suspend.Value)
							if err != nil {
								return fmt.Errorf("invalid suspend.Value provided for resource Kind %v : %v", crd.Kind, err)
							}
							// pick only if stork-migrated annotation exists in the resource
							if _, present := annotations[migration.StorkMigrationAnnotation]; present {
								// since we want to activate the resource we will take opposite of the suspend value
								replicas = !suspendVal
								if !dryRun {
									err := scaleUpCRDResources(dynamicClient, object, crd.Kind, suspend, replicas)
									if err != nil {
										return err
									}
								}
								resourceReplicaMap[gvk][object.GetName()] = strconv.FormatBool(!suspendVal)
							}
						}
					}
				}
			}
		}
	}
	return nil
}

func scaleUpApplicationResources(dynamicClient k8sdynamic.ResourceInterface, object unstructured.Unstructured, replicaAnnotationValue string, resourceType metav1.GroupVersionKind) error {
	parsedReplicas, err := strconv.Atoi(replicaAnnotationValue)
	if err != nil {
		return fmt.Errorf("error parsing replicas annotation value for %v %v/%v: %v", strings.ToLower(resourceType.Kind), object.GetNamespace(), object.GetName(), err)
	}
	content := object.UnstructuredContent()
	err = unstructured.SetNestedField(content, int64(parsedReplicas), "spec", "replicas")
	if err != nil {
		return err
	}
	opts := &metav1.UpdateOptions{
		TypeMeta: metav1.TypeMeta{
			Kind:       resourceType.Kind,
			APIVersion: resourceType.Group + "/" + resourceType.Version},
	}
	_, err = dynamicClient.Update(context.TODO(), &object, *opts, "")
	if err != nil {
		return fmt.Errorf("unable to scale up resource %v %v/%v: %v", strings.ToLower(resourceType.Kind), object.GetNamespace(), object.GetName(), err)
	}
	log.Infof("successfully updated resource %v %v/%v \n", strings.ToLower(resourceType.Kind), object.GetNamespace(), object.GetName())
	return nil
}

func scaleUpCRDResources(dynamicClient k8sdynamic.ResourceInterface, object unstructured.Unstructured, resourceKind string, suspend v1alpha1.SuspendOptions, replicaAnnotationValue interface{}) error {
	specPath := strings.Split(suspend.Path, ".")
	err := unstructured.SetNestedField(object.Object, replicaAnnotationValue, specPath...)
	if err != nil {
		return fmt.Errorf("error updating \"%v\" for %v %v/%v to %v : %v", suspend.Path, strings.ToLower(resourceKind), object.GetNamespace(), object.GetName(), replicaAnnotationValue, err)
	}
	_, err = dynamicClient.Update(context.TODO(), &object, metav1.UpdateOptions{}, "")
	if err != nil {
		return fmt.Errorf("error updating CR %v %v/%v: %v", strings.ToLower(resourceKind), object.GetNamespace(), object.GetName(), err)
	}
	log.Infof("Updated CR for %v %v/%v", strings.ToLower(resourceKind), object.GetNamespace(), object.GetName())
	return nil
}

func scaleUpStashCMResources(namespace string, config *rest.Config) error {
	pvcToPVCOwnerMapping := make(map[string][]metav1.OwnerReference)
	// List the configmaps with label "stash-cr" enabled
	coreClient, err := core.NewForConfig(config)
	if err != nil {
		return err
	}
	dynamicClient, err := k8sdynamic.NewForConfig(config)
	if err != nil {
		return err
	}
	configMaps, err := coreClient.ListConfigMap(namespace, metav1.ListOptions{LabelSelector: StashCRLabel})
	if err != nil {
		return err
	}
	ruleset := resourcecollector.GetDefaultRuleSet()
	// Create the CRs in the same namespace if those don't exist
	for _, configMap := range configMaps.Items {
		objBytes := []byte(configMap.Data[StashedCMCRKey])
		unstructuredObj := &unstructured.Unstructured{}
		err := unstructuredObj.UnmarshalJSON(objBytes)
		if err != nil {
			return fmt.Errorf("error converting string to Unstructured object: %s", err.Error())
		}
		resource := &metav1.APIResource{
			Name:       ruleset.Pluralize(strings.ToLower(unstructuredObj.GetKind())),
			Namespaced: len(unstructuredObj.GetNamespace()) > 0,
		}
		var resourceClient k8sdynamic.ResourceInterface
		if resource.Namespaced {
			resourceClient = dynamicClient.Resource(
				unstructuredObj.GetObjectKind().GroupVersionKind().GroupVersion().WithResource(resource.Name)).Namespace(unstructuredObj.GetNamespace())
		} else {
			resourceClient = dynamicClient.Resource(
				unstructuredObj.GetObjectKind().GroupVersionKind().GroupVersion().WithResource(resource.Name))
		}
		// Create the CR
		_, err = resourceClient.Create(context.TODO(), unstructuredObj, metav1.CreateOptions{})
		if err != nil && !errors.IsAlreadyExists(err) {
			if errors.IsAlreadyExists(err) {
				log.Warnf("error creating resource %s/%s from stashed configmap : %v as it already exists\n", unstructuredObj.GetKind(), unstructuredObj.GetName(), err)
			} else {
				return fmt.Errorf("error creating resource %s/%s from stashed configmap : %v", unstructuredObj.GetKind(), unstructuredObj.GetName(), err)
			}
		}
		// Get the CR
		newResourceUnstructured, err := resourceClient.Get(context.TODO(), unstructuredObj.GetName(), metav1.GetOptions{})
		if err != nil {
			return err
		}
		// Get the owner-references stored for PVCs and modify it to new resource's UID.
		ownedPVCs := configMap.Data[StashedCMOwnedPVCKey]
		nestedMap := make(map[string]metav1.OwnerReference)
		err = json.Unmarshal([]byte(ownedPVCs), &nestedMap)
		if err != nil {
			return err
		}
		for pvcName, ownerReference := range nestedMap {
			ownerReference.UID = newResourceUnstructured.GetUID()
			pvcToPVCOwnerMapping[pvcName] = append(pvcToPVCOwnerMapping[pvcName], ownerReference)
		}
		log.Infof("Successfully created the CRs from the stashed configmaps %s/%s", unstructuredObj.GetKind(), unstructuredObj.GetName())
	}
	// Find PVCs in the namespace which are owned by previous CR, need to modify the owner reference to new CRs for those
	pvcList, err := coreClient.GetPersistentVolumeClaims(namespace, nil)
	if err != nil {
		return err
	}
	for _, pvc := range pvcList.Items {
		if newOwnerReferences, ok := pvcToPVCOwnerMapping[pvc.GetName()]; ok {
			ownerReferences := pvc.GetOwnerReferences()
			ownerReferences = append(ownerReferences, newOwnerReferences...)
			pvc.SetOwnerReferences(ownerReferences)
			_, err = coreClient.UpdatePersistentVolumeClaim(&pvc)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
