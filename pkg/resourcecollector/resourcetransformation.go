package resourcecollector

import (
	"fmt"
	"strings"

	stork_api "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	storkops "github.com/portworx/sched-ops/k8s/stork"
	"github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
)

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
			content := object.UnstructuredContent()
			for _, path := range patch.Specs.Paths {
				switch path.Operation {
				case stork_api.AddResourcePath:
					value := getNewValueForPath(path.Value, string(path.Type))
					if path.Type == stork_api.KeyPairResourceType {
						updateMap := value.(map[string]string)
						err := unstructured.SetNestedStringMap(content, updateMap, strings.Split(path.Path, ".")...)
						if err != nil {
							logrus.Errorf("Unable to apply patch path %s on resource kind: %s/,%s/%s,  err: %v", path, patch.Kind, patch.Namespace, patch.Name, err)
							return err
						}
					} else if path.Type == stork_api.SliceResourceType {
						err := unstructured.SetNestedField(content, value, strings.Split(path.Path, ".")...)
						if err != nil {
							logrus.Errorf("Unable to apply patch path %s on resource kind: %s/,%s/%s,  err: %v", path, patch.Kind, patch.Namespace, patch.Name, err)
							return err
						}
					} else {
						err := unstructured.SetNestedField(content, value, strings.Split(path.Path, ".")...)
						if err != nil {
							logrus.Errorf("Unable to perform operation %s on path %s on resource kind: %s/,%s/%s,  err: %v", path.Operation, path, patch.Kind, patch.Namespace, patch.Name, err)
							return err
						}
					}

				case stork_api.DeleteResourcePath:
					unstructured.RemoveNestedField(content, strings.Split(path.Path, ".")...)
					logrus.Debugf("Removed patch path %s on resource kind: %s/,%s/%s", path, patch.Kind, patch.Namespace, patch.Name)

				case stork_api.ModifyResourcePathValue:
					var value interface{}
					if path.Type == stork_api.KeyPairResourceType {
						currMap, _, err := unstructured.NestedMap(content, strings.Split(path.Path, ".")...)
						if err != nil {
							return fmt.Errorf("unable to find suspend path, err: %v", err)
						}
						mapList := strings.Split(path.Value, ",")
						for _, val := range mapList {
							keyPair := strings.Split(val, ":")
							currMap[keyPair[0]] = keyPair[1]
						}
						value = currMap
					} else if path.Type == stork_api.SliceResourceType {
						currList, _, err := unstructured.NestedSlice(content, strings.Split(path.Path, ".")...)
						if err != nil {
							return fmt.Errorf("unable to find suspend path, err: %v", err)
						}
						arrList := strings.Split(path.Value, ",")
						for _, val := range arrList {
							currList = append(currList, val)
						}
						value = currList
					} else {
						value = path.Value
					}
					err := unstructured.SetNestedField(content, value, strings.Split(path.Path, ".")...)
					if err != nil {
						logrus.Errorf("Unable to perform operation %s on path %s on resource kind: %s/,%s/%s,  err: %v", path.Operation, path, patch.Kind, patch.Namespace, patch.Name, err)
						return err
					}
				}
			}
			object.SetUnstructuredContent(content)
			logrus.Infof("Updated resource of kind %v with patch , resource: %v", patch.Kind, object)
		}
	}
	return nil
}

func getNewValueForPath(oldVal, valType string) interface{} {
	var updatedValue interface{}
	if valType == string(stork_api.KeyPairResourceType) {
		newVal := make(map[string]string)
		mapList := strings.Split(oldVal, ",")
		for _, val := range mapList {
			keyPair := strings.Split(val, ":")
			newVal[keyPair[0]] = keyPair[1]
		}
		updatedValue = newVal
	} else if valType == string(stork_api.SliceResourceType) {
		newVal := []string{}
		arrList := strings.Split(oldVal, ",")
		newVal = append(newVal, arrList...)
		updatedValue = newVal
	} else {
		updatedValue = oldVal
	}
	return updatedValue
}
