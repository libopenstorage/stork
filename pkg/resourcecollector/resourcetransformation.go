package resourcecollector

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	stork_api "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	storkops "github.com/portworx/sched-ops/k8s/stork"
	"github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func GetGVK(Data []byte) (schema.GroupVersionKind, error) {
	metadata := []metav1.TypeMeta{}
	if err := json.Unmarshal(Data, &metadata); err != nil {
		return schema.GroupVersionKind{}, err
	}
	gVersion, err := schema.ParseGroupVersion(metadata[0].APIVersion)
	if err != nil {
		return schema.GroupVersionKind{}, err
	}
	return gVersion.WithKind(metadata[0].Kind), nil
}

func IsObjectLabelsMatched(content map[string]interface{}, selector map[string]string) bool {
	logrus.Info("Starting IsObjectLabelsMatched")
	objectLabels, _, err := unstructured.NestedStringMap(content, "metadata", "labels")
	if err != nil {
		logrus.Error("error finding the metadata.labels from the unstructed content: ", content)
	}
	logrus.Infof("ObjectLabels %v, selector %v,output %v", objectLabels, selector, labels.SelectorFromSet(selector).Matches(labels.Set(objectLabels)))
	return labels.SelectorFromSet(selector).Matches(labels.Set(objectLabels))
	// labels.Equals(objectLabels, selector)
}

// GetGVKToTransformSpecMapper {GVK: []TransformSpecs{}}
func GetGVKToTransformSpecMapper(transformName string, namespace string) (map[schema.GroupVersionKind][]stork_api.TransformSpecs, error) {
	logrus.Info("Starting GetGVKToTransformSpecMapper")
	gvkResourceTransform := make(map[schema.GroupVersionKind][]stork_api.TransformSpecs)
	if transformName == "" || namespace == "" {
		logrus.Errorf("empty name received for resource transformation/namespace")
		return gvkResourceTransform, fmt.Errorf("empty name received for resource transformation/namespace")
	}

	resp, err := storkops.Instance().GetResourceTransformation(transformName, namespace)
	if err != nil {
		// current namespace does not have any transform CR
		// skip it from map
		if errors.IsNotFound(err) {
			logrus.Errorf("resource not found for resource transformation/namespace: %s,%s", transformName, namespace)
			return gvkResourceTransform, fmt.Errorf("resource not found for resource transformation/namespace: %s,%s", transformName, namespace)
		}
		logrus.Errorf("unable to get resource transfomration specs %s/%s, err: %v", namespace, transformName, err)
		return nil, fmt.Errorf("unable to get resource transfomration specs %s/%s, err: %v", namespace, transformName, err)
	}

	logrus.Infof("resp: %+v", *resp)

	for _, spec := range resp.Spec.Objects {
		if gvk, err := getGVK(spec.Resource); err != nil {
			logrus.Error("Error getGVK: ", err)
			return nil, err
		} else {
			logrus.Infof("update gvkResourceTransform for gvk %v with spec %v", gvk, spec)
			gvkResourceTransform[gvk] = append(gvkResourceTransform[gvk], spec)
		}
	}
	logrus.Info("Completed GetGVKToTransformSpecMapper")
	return gvkResourceTransform, nil
}

// Since we collect all resources from required migration namespace at once
// getResourcePatch creates map of namespace: {kind: []resourceinfo{}}
// to get transform spec for matching resources
func GetResourcePatch(transformName string, namespaces []string) (map[string]stork_api.KindResourceTransform, error) {
	// namespace- Kind:TransformSpec map for faster lookup
	patch := make(map[string]stork_api.KindResourceTransform)
	if transformName == "" {
		logrus.Error("Empty name received for resource transformation")
		return patch, nil
	}
	for _, namespace := range namespaces {
		resp, err := storkops.Instance().GetResourceTransformation(transformName, namespace)
		if err != nil {
			// current namespace does not have any transform CR
			// skip it from map
			if errors.IsNotFound(err) {
				continue
			}
			logrus.Errorf("Unable to get resource transfomration specs %s/%s, err: %v", namespace, transformName, err)
			return nil, err
		}
		resMap := make(map[string][]stork_api.TransformResourceInfo)
		for _, resource := range resp.Status.Resources {
			resMap[resource.Kind] = append(resMap[resource.Kind], *resource)
		}
		patch[namespace] = resMap
	}
	return patch, nil
}

// this method transform object as per resource transformation specified in each namespaces
func TransformResources(
	object runtime.Unstructured,
	resPatch []stork_api.TransformResourceInfo,
	objName, objNamespace string,
) error {
	for _, patch := range resPatch {
		if patch.Name == objName && patch.Namespace == objNamespace {
			if err := TransformObject(object, patch.Specs); err != nil {
				logrus.Errorf("Unable to Transform Object %s/%s, on resource kind: %s, err: %v", patch.Kind, patch.Namespace, patch.Name, err)
			}
		}
	}
	return nil
}

func TransformObject(object runtime.Unstructured, spec stork_api.TransformSpecs) error {
	logrus.Info("Started TransformObject")
	content := object.UnstructuredContent()
	for _, path := range spec.Paths {
		switch path.Operation {
		case stork_api.AddResourcePath:
			value, err := getNewValueForPath(path.Value, path.Type)
			if err != nil {
				logrus.Errorf("Unable to parse the Value for the type %s specified, path %s on resource: %s, err: %v", path.Type, path, spec.Resource, err)
				return err
			}
			if path.Type == stork_api.KeyPairResourceType {
				updateMap := value.(map[string]string)
				err := unstructured.SetNestedStringMap(content, updateMap, strings.Split(path.Path, ".")...)
				if err != nil {
					logrus.Errorf("Unable to apply patch path %s on resource: %s, err: %v", path, spec.Resource, err)
					return err
				}
			} else if path.Type == stork_api.SliceResourceType {
				updateSlice := value.([]string)
				err := unstructured.SetNestedStringSlice(content, updateSlice, strings.Split(path.Path, ".")...)
				if err != nil {
					logrus.Errorf("Unable to apply patch path %s on resource: %s, err: %v", path, spec.Resource, err)
					return err
				}
			} else {
				err := unstructured.SetNestedField(content, value, strings.Split(path.Path, ".")...)
				if err != nil {
					logrus.Errorf("Unable to perform operation %s on path %s on resource: %s,  err: %v", path.Operation, path, spec.Resource, err)
					return err
				}
			}

		case stork_api.DeleteResourcePath:
			unstructured.RemoveNestedField(content, strings.Split(path.Path, ".")...)
			logrus.Debugf("Removed patch path %s on resource: %s", path, spec.Resource)

		case stork_api.ModifyResourcePathValue:
			var value interface{}
			if path.Type == stork_api.KeyPairResourceType {
				currMap, _, err := unstructured.NestedMap(content, strings.Split(path.Path, ".")...)
				if err != nil || len(currMap) == 0 {
					return fmt.Errorf("unable to find spec path, err: %v", err)
				}
				mapList := strings.Split(path.Value, ",")
				for _, val := range mapList {
					keyPair := strings.Split(val, ":")
					if len(keyPair) != 2 {
						return fmt.Errorf("invalid keypair value format :%s", keyPair)
					}
					currMap[keyPair[0]] = keyPair[1]
				}
				value = currMap
			} else if path.Type == stork_api.SliceResourceType {
				currList, _, err := unstructured.NestedSlice(content, strings.Split(path.Path, ".")...)
				if err != nil {
					return fmt.Errorf("unable to find spec path, err: %v", err)
				}
				arrList := strings.Split(path.Value, ",")
				for _, val := range arrList {
					currList = append(currList, val)
				}
				value = currList
			} else {
				var err error
				value, err = getNewValueForPath(path.Value, path.Type)
				if err != nil {
					logrus.Errorf("Unable to parse the Value for the type %s specified, path %s on resource: %s,  err: %v", path.Type, path, spec.Resource, err)
					return err
				}
			}
			err := unstructured.SetNestedField(content, value, strings.Split(path.Path, ".")...)
			if err != nil {
				logrus.Errorf("Unable to perform operation %s on path %s on resource: %s,  err: %v", path.Operation, path, spec.Resource, err)
				return err
			}
		}
	}
	// lets add annotation saying this resource has been transformed by migration/restore
	// controller before applying
	// set migration annotations
	annotations, found, err := unstructured.NestedStringMap(content, "metadata", "annotations")
	if err != nil {
		return err
	}
	if !found {
		annotations = make(map[string]string)
	}
	annotations[TransformedResourceName] = "true"
	if err := unstructured.SetNestedStringMap(content, annotations, "metadata", "annotations"); err != nil {
		return err
	}
	object.SetUnstructuredContent(content)
	logrus.Infof("Updated resource %s with patch , object: %v", spec.Resource, object)
	return nil
}

func getNewValueForPath(oldVal string, valType stork_api.ResourceTransformationValueType) (interface{}, error) {
	var updatedValue interface{}
	var err error

	switch valType {
	case stork_api.KeyPairResourceType:
		//TODO: here we can accept map[string]interface{}, so we can use SetNestedField instead of SetNestedStringMap
		newVal := make(map[string]string)
		mapList := strings.Split(oldVal, ",")
		for _, val := range mapList {
			keyPair := strings.Split(val, ":")
			newVal[keyPair[0]] = keyPair[1]
		}
		updatedValue = newVal
	case stork_api.SliceResourceType:
		// TODO: here we can accept []interface{}{}, so we can use SetNestedField instead of SetNestedStringSlice
		newVal := []string{}
		arrList := strings.Split(oldVal, ",")
		newVal = append(newVal, arrList...)
		updatedValue = newVal
	case stork_api.IntResourceType:
		updatedValue, err = strconv.ParseInt(oldVal, 10, 64)
	case stork_api.BoolResourceType:
		updatedValue, err = strconv.ParseBool(oldVal)
	default:
		updatedValue = oldVal
	}
	return updatedValue, err
}

// return group,version,kind from give resource type
func getGVK(resource string) (schema.GroupVersionKind, error) {
	gvk := strings.Split(resource, "/")
	if len(gvk) != 3 {
		return schema.GroupVersionKind{}, fmt.Errorf("invalid resource kind :%s", resource)
	}
	return schema.GroupVersionKind{Group: gvk[0], Version: gvk[1], Kind: gvk[2]}, nil
}
