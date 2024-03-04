package resourceutils

import (
	"context"
	"fmt"
	"github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"strconv"
	"strings"

	migration "github.com/libopenstorage/stork/pkg/migration/controllers"
	storkops "github.com/portworx/sched-ops/k8s/stork"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	k8sdynamic "k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
)

func ScaleDownGivenResources(namespaces []string, resourceMap map[string]map[metav1.GroupVersionKind]map[string]string, config *rest.Config) error {
	//TODO: Set correct config for storkops
	crdList, err := storkops.Instance().ListApplicationRegistrations()
	if err != nil {
		return err
	}

	for _, ns := range namespaces {
		// 1. ApplicationResources
		for _, gvk := range getRelevantGVKs() {
			resources, err := ListResourcesByGVK(ns, config, gvk)
			if err != nil {
				return err
			}
			dynamicClient, err := GenerateDynamicClientForGVK(gvk, ns, config)
			if err != nil {
				return err
			}
			for _, object := range resources.Items {
				if _, present := resourceMap[ns][gvk][object.GetName()]; present {
					err := scaleDownApplicationResource(object, dynamicClient, gvk)
					if err != nil {
						return err
					}
				}
			}
		}
		// 2. CRD Resources
		for _, applicationRegistration := range crdList.Items {
			for _, applicationResource := range applicationRegistration.Resources {
				gvk := applicationResource.GroupVersionKind
				dynamicClient, err := GenerateDynamicClientForGVK(gvk, ns, config)
				resources, err := ListResourcesByGVK(ns, config, gvk)
				if err != nil {
					return err
				}
				for _, object := range resources.Items {
					if _, present := resourceMap[ns][gvk][object.GetName()]; present {
						err := ScaleDownCRDResource(object, applicationResource, dynamicClient)
						if err != nil {
							return err
						}
					}
				}
			}
		}
	}
	return nil
}

func scaleDownApplicationResource(object unstructured.Unstructured, dynamicClient k8sdynamic.ResourceInterface, resourceType metav1.GroupVersionKind) error {
	content := object.UnstructuredContent()
	currentReplicas, found, err := unstructured.NestedInt64(content, "spec", "replicas")
	if err != nil {
		return err
	}
	if !found {
		currentReplicas = 1
	}
	annotations, found, err := unstructured.NestedStringMap(content, "metadata", "annotations")
	if err != nil {
		return err
	}
	if !found {
		annotations = make(map[string]string)
	}
	migrationReplicasAnnotationValue, replicaAnnotationIsPresent := annotations[migration.StorkMigrationReplicasAnnotation]
	if currentReplicas == 0 && replicaAnnotationIsPresent {
		//If the actual replica count is 0 and migrationReplicas annotation is more than 0, then don't override the migrationReplicaAnnotation value.
		migrationReplicas, err := strconv.ParseInt(migrationReplicasAnnotationValue, 10, 64)
		if err != nil {
			return err
		}
		if migrationReplicas > 0 {
			currentReplicas = migrationReplicas
		}
	}
	//we set the actual replica count to 0 to scale down the resource on target cluster
	err = unstructured.SetNestedField(content, int64(0), "spec", "replicas")
	if err != nil {
		return err
	}
	annotations[migration.StorkMigrationReplicasAnnotation] = strconv.FormatInt(currentReplicas, 10)
	err = unstructured.SetNestedStringMap(content, annotations, "metadata", "annotations")
	if err != nil {
		return err
	}
	_, err = dynamicClient.Update(context.TODO(), &object, metav1.UpdateOptions{}, "")
	if err != nil {
		return fmt.Errorf("unable to update resource %v %v/%v: %v", strings.ToLower(resourceType.String()), object.GetNamespace(), object.GetName(), err)
	}
	return nil
}

func ScaleDownCRDResource(object unstructured.Unstructured, applicationResource v1alpha1.ApplicationResource, dynamicClient k8sdynamic.ResourceInterface) error {
	content := object.UnstructuredContent()
	annotations, found, err := unstructured.NestedStringMap(content, "metadata", "annotations")
	if err != nil {
		return err
	}
	if !found {
		annotations = make(map[string]string)
	}
	if applicationResource.SuspendOptions.Path != "" {
		applicationResource.NestedSuspendOptions = append(applicationResource.NestedSuspendOptions, applicationResource.SuspendOptions)
	}
	if len(applicationResource.NestedSuspendOptions) == 0 {
		return nil
	}
	for _, suspend := range applicationResource.NestedSuspendOptions {
		specPath := strings.Split(suspend.Path, ".")
		if len(specPath) > 1 {
			var val string
			SuspendAnnotationValue, suspendAnnotationIsPresent := annotations[migration.StorkAnnotationPrefix+suspend.Path]
			var disableVersion interface{}
			if suspend.Type == "int" {
				currentValue, found, err := unstructured.NestedInt64(content, specPath...)
				if err != nil || !found {
					return fmt.Errorf("unable to find suspend path, err: %v", err)
				}
				disableVersion = int64(0)
				if currentValue == 0 && suspendAnnotationIsPresent {
					//suspendAnnotation has value set as {currVal + "," + suspend.Value}
					//we need to extract only the currVal from it
					annotationValue := strings.Split(SuspendAnnotationValue, ",")[0]
					intValue, err := strconv.ParseInt(annotationValue, 10, 64)
					if err != nil {
						return err
					}
					if intValue > 0 {
						//If the actual suspend path value is 0 and suspend annotation value is more than 0,
						//then don't override the annotation.
						currentValue = intValue
					}
				}
				val = fmt.Sprintf("%v", currentValue)
			} else if suspend.Type == "string" {
				currentValue, _, err := unstructured.NestedString(content, specPath...)
				if err != nil {
					return fmt.Errorf("unable to find suspend path, err: %v", err)
				}
				disableVersion = suspend.Value
				if currentValue == suspend.Value && suspendAnnotationIsPresent {
					annotationValue := strings.Split(SuspendAnnotationValue, ",")[0]
					if annotationValue != "" {
						//If the actual value is equal to suspend.Value and suspend path annotation value is not an empty string,
						//then don't override the annotation
						currentValue = annotationValue
					}
				}
				val = currentValue
			} else {
				return fmt.Errorf("invalid type %v to suspend cr", suspend.Type)
			}
			//scale down the CRD resource by setting the suspendPath value to disableVersion
			if err := unstructured.SetNestedField(content, disableVersion, specPath...); err != nil {
				return err
			}

			// path : activate/deactivate value
			annotations[migration.StorkAnnotationPrefix+suspend.Path] = val + "," + suspend.Value
		}
	}
	err = unstructured.SetNestedStringMap(content, annotations, "metadata", "annotations")
	if err != nil {
		return err
	}
	_, err = dynamicClient.Update(context.TODO(), &object, metav1.UpdateOptions{}, "")
	if err != nil {
		return fmt.Errorf("unable to update resource %v %v/%v: %v", strings.ToLower(applicationResource.GroupVersionKind.String()), object.GetNamespace(), object.GetName(), err)
	}
	return nil
}
