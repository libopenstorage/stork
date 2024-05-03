package resourceutils

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	migration "github.com/libopenstorage/stork/pkg/migration/controllers"
	"github.com/portworx/sched-ops/k8s/core"
	storkops "github.com/portworx/sched-ops/k8s/stork"
	log "github.com/sirupsen/logrus"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	k8sdynamic "k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
)

func ScaleDownGivenResources(namespaces []string, resourceMap map[string]map[metav1.GroupVersionKind]map[string]string, config *rest.Config) error {
	storkClient, err := storkops.NewForConfig(config)
	if err != nil {
		return err
	}
	crdList, err := storkClient.ListApplicationRegistrations()
	if err != nil {
		return err
	}

	for _, ns := range namespaces {
		// 1. ApplicationResources
		for _, gvk := range GetGVKsRelevantForScaling() {
			resources, dynamicClient, err := ListResourcesByGVK(gvk, ns, config)
			if err != nil {
				return err
			}
			if resources == nil {
				continue
			}
			for _, object := range resources.Items {
				if _, present := resourceMap[ns][gvk][object.GetName()]; present {
					err := scaleDownApplicationResource(dynamicClient, object, gvk)
					if err != nil {
						return err
					}
				}
			}
		}

		// 2. CronJobs
		resources, dynamicClient, err := ListResourcesByGVK(cronJobGVK, ns, config)
		if err != nil {
			return err
		}
		if resources != nil {
			for _, object := range resources.Items {
				if _, present := resourceMap[ns][cronJobGVK][object.GetName()]; present {
					err := scaleCronJob(dynamicClient, object, true)
					if err != nil {
						return err
					}
				}
			}
		}

		// 3. CRD Resources
		for _, applicationRegistration := range crdList.Items {
			for _, crd := range applicationRegistration.Resources {
				gvk := crd.GroupVersionKind
				resources, dynamicClient, err := ListResourcesByGVK(gvk, ns, config)
				if err != nil {
					return err
				}
				if resources == nil {
					continue
				}
				for _, object := range resources.Items {
					if _, present := resourceMap[ns][gvk][object.GetName()]; present {
						err := scaleDownCRDResource(dynamicClient, object, crd, config)
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

func scaleDownApplicationResource(dynamicClient k8sdynamic.ResourceInterface, object unstructured.Unstructured, resourceType metav1.GroupVersionKind) error {
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
	opts := &metav1.UpdateOptions{
		TypeMeta: metav1.TypeMeta{
			Kind:       resourceType.Kind,
			APIVersion: resourceType.Group + "/" + resourceType.Version},
	}
	_, err = dynamicClient.Update(context.TODO(), &object, *opts, "")
	if err != nil {
		return fmt.Errorf("unable to update resource %v %v/%v: %v", strings.ToLower(resourceType.Kind), object.GetNamespace(), object.GetName(), err)
	}
	log.Infof("successfully updated resource %v %v/%v \n", strings.ToLower(resourceType.Kind), object.GetNamespace(), object.GetName())
	return nil
}

func scaleCronJob(dynamicClient k8sdynamic.ResourceInterface, object unstructured.Unstructured, suspendValue bool) error {
	content := object.UnstructuredContent()
	err := unstructured.SetNestedField(content, suspendValue, "spec", "suspend")
	if err != nil {
		return err
	}
	opts := &metav1.UpdateOptions{
		TypeMeta: metav1.TypeMeta{
			Kind:       cronJobGVK.Kind,
			APIVersion: cronJobGVK.Group + "/" + cronJobGVK.Version},
	}
	_, err = dynamicClient.Update(context.TODO(), &object, *opts, "")
	if err != nil {
		return fmt.Errorf("unable to update resource %v %v/%v: %v", strings.ToLower(cronJobGVK.Kind), object.GetNamespace(), object.GetName(), err)
	}
	log.Infof("successfully updated resource %v %v/%v \n", strings.ToLower(cronJobGVK.Kind), object.GetNamespace(), object.GetName())
	return nil
}

func scaleDownCRDResource(dynamicClient k8sdynamic.ResourceInterface, object unstructured.Unstructured, crd v1alpha1.ApplicationResource, config *rest.Config) error {
	content := object.UnstructuredContent()
	annotations, found, err := unstructured.NestedStringMap(content, "metadata", "annotations")
	if err != nil {
		return err
	}
	if !found {
		annotations = make(map[string]string)
	}
	if crd.SuspendOptions.Path != "" {
		crd.NestedSuspendOptions = append(crd.NestedSuspendOptions, crd.SuspendOptions)
	}
	if len(crd.NestedSuspendOptions) == 0 {
		return nil
	}
	for _, suspend := range crd.NestedSuspendOptions {
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
					// suspendAnnotation has value set as {currVal + "," + suspend.Value}
					// we need to extract only the currVal from it
					annotationValue := strings.Split(SuspendAnnotationValue, ",")[0]
					intValue, err := strconv.ParseInt(annotationValue, 10, 64)
					if err != nil {
						return err
					}
					if intValue > 0 {
						// If the actual value is 0 and suspend annotation value is more than 0,
						// then don't override the annotation.
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
						// If the actual value is equal to suspend.Value (i.e. 0 value) and suspend path annotation value is not an empty string,
						// then don't override the annotation
						currentValue = annotationValue
					}
				}
				val = currentValue
			} else if suspend.Type == "bool" {
				if val, err := strconv.ParseBool(suspend.Value); err != nil {
					disableVersion = true
				} else {
					log.Warnf("failed to parse suspend.Value %v. going ahead with default bool disable value of true", suspend.Value)
					disableVersion = val
				}
			} else {
				return fmt.Errorf("invalid type %v to suspend cr", suspend.Type)
			}
			// scale down the CRD resource by setting the suspendPath value to disableVersion
			if err := unstructured.SetNestedField(content, disableVersion, specPath...); err != nil {
				return err
			}
			annotations[migration.StorkAnnotationPrefix+suspend.Path] = val + "," + suspend.Value
		}
	}
	err = unstructured.SetNestedStringMap(content, annotations, "metadata", "annotations")
	if err != nil {
		return err
	}
	opts := &metav1.UpdateOptions{
		TypeMeta: metav1.TypeMeta{
			Kind:       crd.Kind,
			APIVersion: crd.Group + "/" + crd.Version},
	}
	_, err = dynamicClient.Update(context.TODO(), &object, *opts, "")
	if err != nil {
		return fmt.Errorf("unable to update resource %v %v/%v: %v", strings.ToLower(crd.Kind), object.GetNamespace(), object.GetName(), err)
	}
	log.Infof("successfully updated resource %v %v/%v \n", strings.ToLower(crd.Kind), object.GetNamespace(), object.GetName())

	// Delete pods corresponding to the CRD as well
	if crd.PodsPath == "" {
		return nil
	}
	podPath := strings.Split(crd.PodsPath, ".")
	pods, found, err := unstructured.NestedStringSlice(object.Object, podPath...)
	if err != nil {
		return fmt.Errorf("error getting pods for %v %v/%v : %v", strings.ToLower(crd.Kind), object.GetNamespace(), object.GetName(), err)
	}
	if !found {
		return nil
	}
	coreClient, err := core.NewForConfig(config)
	if err != nil {
		return err
	}
	for _, pod := range pods {
		err = coreClient.DeletePod(object.GetNamespace(), pod, true)
		if err != nil {
			return fmt.Errorf("error deleting pod %v for %v %v/%v : %v", pod, strings.ToLower(crd.Kind), object.GetNamespace(), object.GetName(), err)
		}
	}
	return nil
}
