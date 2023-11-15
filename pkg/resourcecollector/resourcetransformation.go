package resourcecollector

import (
	"fmt"
	"regexp"
	"strconv"
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
				logrus.Debugf("TransformResources: UnstructuredContent: %v", content)
				logrus.Debugf("TransformResources: path %v, operation %v", path.Path, path.Operation)
				switch path.Operation {
				case stork_api.AddResourcePath:
					value, err := getNewValueForPath(path.Value, path.Type)
					if err != nil {
						logrus.Errorf("Unable to parse the Value for the type %s specified, path %s on resource kind: %s/,%s/%s,  err: %v", path.Type, path, patch.Kind, patch.Namespace, patch.Name, err)
						return err
					}
					if path.Type == stork_api.KeyPairResourceType {
						updateMap := value.(map[string]string)
						err := SetNestedStringMap(content, updateMap, path.Path)
						if err != nil {
							logrus.Errorf("Unable to apply patch path %s on resource kind: %s/,%s/%s,  err: %v", path, patch.Kind, patch.Namespace, patch.Name, err)
							return err
						}
					} else if path.Type == stork_api.SliceResourceType {
						updateSlice := value.([]string)
						err := SetNestedStringSlice(content, updateSlice, path.Path)
						if err != nil {
							logrus.Errorf("Unable to apply patch path %s on resource kind: %s/,%s/%s,  err: %v", path, patch.Kind, patch.Namespace, patch.Name, err)
							return err
						}
					} else {
						err := SetNestedField(content, value, path.Path)
						if err != nil {
							logrus.Errorf("Unable to perform operation %s on path %s on resource kind: %s/,%s/%s,  err: %v", path.Operation, path, patch.Kind, patch.Namespace, patch.Name, err)
							return err
						}
					}

				case stork_api.DeleteResourcePath:
					RemoveNestedField(content, strings.Split(path.Path, ".")...)
					logrus.Debugf("Removed patch path %s on resource kind: %s/,%s/%s", path, patch.Kind, patch.Namespace, patch.Name)

				case stork_api.ModifyResourcePathValue:
					var value interface{}
					if path.Type == stork_api.KeyPairResourceType {
						currMap, _, err := NestedMap(content, strings.Split(path.Path, ".")...)
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
						currList, _, err := NestedSlice(content, strings.Split(path.Path, ".")...)
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
							logrus.Errorf("Unable to parse the Value for the type %s specified, path %s on resource kind: %s/,%s/%s,  err: %v", path.Type, path, patch.Kind, patch.Namespace, patch.Name, err)
							return err
						}
					}
					err := SetNestedField(content, value, path.Path)
					if err != nil {
						logrus.Errorf("Unable to perform operation %s on path %s on resource kind: %s/,%s/%s,  err: %v", path.Operation, path, patch.Kind, patch.Namespace, patch.Name, err)
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
			logrus.Infof("Updated resource of kind %v with patch , resource: %v", patch.Kind, object)
		}
	}
	return nil
}

func getNewValueForPath(oldVal string, valType stork_api.ResourceTransformationValueType) (interface{}, error) {
	logrus.Debugf("oldVal %s,valType %v", oldVal, valType)
	var updatedValue interface{}
	var err error

	switch valType {
	case stork_api.KeyPairResourceType:
		//TODO: here we can accept map[string]interface{} because inside it is getting changed
		newVal := make(map[string]string)
		mapList := strings.Split(oldVal, ",")
		for _, val := range mapList {
			keyPair := strings.Split(val, ":")
			newVal[keyPair[0]] = keyPair[1]
		}
		updatedValue = newVal
	case stork_api.SliceResourceType:
		// TODO: SetNestedStringslice will slove the issue
		newVal := []string{}
		arrList := strings.Split(oldVal, ",")
		newVal = append(newVal, arrList...)
		updatedValue = newVal
	case stork_api.IntResourceType:
		updatedValue, err = strconv.ParseInt(oldVal, 10, 64)
	case stork_api.BoolResourceType:
		updatedValue, err = strconv.ParseBool(oldVal)
	case stork_api.StringResourceType:
		updatedValue = oldVal
	}
	return updatedValue, err
}

func jsonPath(fields []string) string {
	return "." + strings.Join(fields, ".")
}

var pathRegexpWithanArray = regexp.MustCompile(`^.+\[[0-9]+\](\.[a-zA-Z_/][a-zA-Z0-9_/]*)+$`)

func NestedSlice(obj map[string]interface{}, fields ...string) ([]interface{}, bool, error) {
	if !pathRegexpWithanArray.MatchString(strings.Join(fields, ".")) {
		return unstructured.NestedSlice(obj, fields...)
	}

	val, found, err := NestedFieldNoCopy(obj, fields...)
	if !found || err != nil {
		return nil, found, err
	}
	_, ok := val.([]interface{})
	if !ok {
		return nil, false, fmt.Errorf("%v accessor error: %v is of the type %T, expected []interface{}", jsonPath(fields), val, val)
	}
	return runtime.DeepCopyJSONValue(val).([]interface{}), true, nil
}

func NestedMap(obj map[string]interface{}, fields ...string) (map[string]interface{}, bool, error) {
	if !pathRegexpWithanArray.MatchString(strings.Join(fields, ".")) {
		return unstructured.NestedMap(obj, fields...)
	}

	m, found, err := nestedMapNoCopy(obj, fields...)
	if !found || err != nil {
		return nil, found, err
	}
	return runtime.DeepCopyJSON(m), true, nil
}

func nestedMapNoCopy(obj map[string]interface{}, fields ...string) (map[string]interface{}, bool, error) {
	val, found, err := NestedFieldNoCopy(obj, fields...)
	if !found || err != nil {
		return nil, found, err
	}
	m, ok := val.(map[string]interface{})
	if !ok {
		return nil, false, fmt.Errorf("%v accessor error: %v is of the type %T, expected map[string]interface{}", jsonPath(fields), val, val)
	}
	return m, true, nil
}

func NestedFieldNoCopy(obj map[string]interface{}, fields ...string) (interface{}, bool, error) {
	var val interface{} = obj

	for i, field := range fields {
		if val == nil {
			return nil, false, nil
		}
		if m, ok := val.(map[string]interface{}); ok {
			var err error
			val, ok, err = getValueFromMapKey(m, field)
			if !ok || err != nil {
				return nil, false, err
			}
		} else {
			return nil, false, fmt.Errorf("%v accessor error: %v is of the type %T, expected map[string]interface{}", jsonPath(fields[:i+1]), val, val)
		}
	}
	return val, true, nil
}

func SetNestedStringSlice(obj map[string]interface{}, value []string, path string) error {
	if !pathRegexpWithanArray.MatchString(path) {
		return unstructured.SetNestedStringSlice(obj, value, strings.Split(path, ".")...)
	}

	m := make([]interface{}, 0, len(value)) // convert []string into []interface{}
	for _, v := range value {
		m = append(m, v)
	}
	return setNestedFieldNoCopy(obj, m, strings.Split(path, ".")...)
}

func SetNestedStringMap(obj map[string]interface{}, value map[string]string, path string) error {
	if !pathRegexpWithanArray.MatchString(path) {
		return unstructured.SetNestedStringMap(obj, value, strings.Split(path, ".")...)
	}
	m := make(map[string]interface{}, len(value)) // convert map[string]string into map[string]interface{}
	for k, v := range value {
		m[k] = v
	}
	return setNestedFieldNoCopy(obj, m, strings.Split(path, ".")...)
}

func SetNestedField(obj map[string]interface{}, value interface{}, path string) error {
	if !pathRegexpWithanArray.MatchString(path) {
		return unstructured.SetNestedField(obj, value, strings.Split(path, ".")...)
	}
	return setNestedFieldNoCopy(obj, runtime.DeepCopyJSONValue(value), strings.Split(path, ".")...)
}

func setNestedFieldNoCopy(obj map[string]interface{}, value interface{}, fields ...string) error {
	m := obj

	for index, field := range fields[:len(fields)-1] {
		if val, ok, err := getValueFromMapKey(m, field); err != nil {
			return err
		} else if ok {
			if valMap, ok := val.(map[string]interface{}); ok {
				m = valMap
			} else {
				return fmt.Errorf("value cannot be set because %v is not a map[string]interface{}", jsonPath(fields[:index+1]))
			}
		} else {
			newVal := make(map[string]interface{})
			if err := setMapKeyWithValue(m, newVal, field); err != nil {
				return err
			}
			m = newVal
		}
	}
	m[fields[len(fields)-1]] = value
	return nil
}

func RemoveNestedField(obj map[string]interface{}, fields ...string) error {
	if !pathRegexpWithanArray.MatchString(strings.Join(fields, ".")) {
		unstructured.RemoveNestedField(obj, fields...)
		return nil
	}
	m := obj
	for _, field := range fields[:len(fields)-1] {
		if val, ok, err := getValueFromMapKey(m, field); err != nil {
			return err
		} else if ok {
			if valMap, ok := val.(map[string]interface{}); ok {
				m = valMap
			} else {
				return nil
			}
		} else {
			return nil
		}
	}
	delete(m, fields[len(fields)-1])
	return nil
}

var indexDelimeter = func(c rune) bool {
	return c == '[' || c == ']'
}

func setMapKeyWithValue(m, newVal map[string]interface{}, field string) error {
	// check if an array index exists in the field
	parts := strings.FieldsFunc(field, indexDelimeter)
	if len(parts) != 2 {
		m[field] = newVal
		return nil
	}

	// if the parts[0] is not an array send an error
	arr := m[parts[0]]
	value, ok := arr.([]interface{})
	if !ok {
		return fmt.Errorf("value cannot be set because %v is not a []interface{}", arr)
	}

	// append the newVal to the existing array
	value = append(value, newVal)
	m[parts[0]] = value
	return nil
}

func getValueFromMapKey(m map[string]interface{}, field string) (interface{}, bool, error) {
	// check if an array index exists in the field
	parts := strings.FieldsFunc(field, indexDelimeter)
	if len(parts) != 2 {
		value, ok := m[field]
		return value, ok, nil
	}

	// if the parts[0] is not an array send an error
	arr := m[parts[0]]
	value, ok := arr.([]interface{})
	if !ok {
		return nil, false, fmt.Errorf("value cannot be set because %v is not a []interface{}", arr)
	}

	// Convert the array index to int
	var arrIndex int
	_, err := fmt.Sscanf(parts[1], "%d", &arrIndex)
	if err != nil {
		return nil, false, err
	}

	// send the approriate array object
	if arrIndex < len(value) {
		return value[arrIndex], true, nil
	} else if arrIndex > len(value) {
		return nil, false, fmt.Errorf("value cannot be set because index %d is out of range in array %v with length %d", arrIndex, arr, len(value))
	}
	return nil, false, nil
}
