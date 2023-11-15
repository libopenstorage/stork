package resourcecollector

import (
	"fmt"
	"regexp"
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
						value = path.Value
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

// pathRegexpWithanArray is an regualr expression to validate an index exists in the path
var pathRegexpWithanArray = regexp.MustCompile(`^.+\[[0-9]+\].*$`)

func jsonPath(fields []string) string {
	return "." + strings.Join(fields, ".")
}

// NestedSlice is wrapper around unstructured.NestedSlice function
// if the path doesn't consists of an index the call is transferred to unstructured.NestedSlice
// else it uses the same logic but includes changes to support the array index
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

// NestedMap is wrapper around unstructured.NestedMap function
// if the path doesn't consists of an index the call is transferred to unstructured.NestedMap
// else it uses the same logic but includes changes to support the array index
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

// SetNestedStringSlice is wrapper around unstructured.SetNestedStringSlice function
// if the path doesn't consists of an index the call is transferred to unstructured.SetNestedStringSlice
// else it uses the same logic but includes changes to support the array index
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

// SetNestedStringMap is wrapper around unstructured.SetNestedStringMap function
// if the path doesn't consists of an index the call is transferred to unstructured.SetNestedStringMap
// else it uses the same logic but includes changes to support the array index
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

// SetNestedField is wrapper around unstructured.SetNestedField function
// if the path doesn't consists of an index the call is transferred to unstructured.SetNestedField
// else it uses the same logic but includes changes to support the array index
func SetNestedField(obj map[string]interface{}, value interface{}, path string) error {
	if !pathRegexpWithanArray.MatchString(path) {
		return unstructured.SetNestedField(obj, value, strings.Split(path, ".")...)
	}
	return setNestedFieldNoCopy(obj, runtime.DeepCopyJSONValue(value), strings.Split(path, ".")...)
}

// Here instead of m[field] we were using the getValueFromMapKey in case if the field as array index.
// while assigning a value we use setMapKeyWithValue.
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
	return setMapKeyWithValue(m, value, fields[len(fields)-1])
}

// RemoveNestedField is wrapper around unstructured.RemoveNestedField function
// if the path doesn't consists of an index the call is transferred to unstructured.RemoveNestedField
// else it uses the same logic but includes changes to support the array index
//
// Here instead of m[field] we were using the getValueFromMapKey in case if the field as array index
// deleteMapKey to remove a key from the map.
func RemoveNestedField(obj map[string]interface{}, fields ...string) error {
	if !pathRegexpWithanArray.MatchString(strings.Join(fields, ".")) {
		unstructured.RemoveNestedField(obj, fields...)
		return nil
	}
	m := obj
	for _, field := range fields[:len(fields)-1] {
		if val, ok, err := getValueFromMapKey(m, field); err != nil {
			logrus.Errorf("Error while get value from map witk key[%v] :%v", field, err)
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

	deleteMapKey(m, fields[len(fields)-1])
	return nil
}

var indexDelimeter = func(c rune) bool {
	return c == '[' || c == ']'
}

// deleteMapKey is to delete the key entry from the map m.
// if the field contains an array index then the approriate array element is deleted.
// Example: containers[3]
func deleteMapKey(m map[string]interface{}, field string) {
	// check if an array index exists in the field
	parts := strings.FieldsFunc(field, indexDelimeter)
	// Example: Here the parts is []string{conatiners, 3}
	// if the length of the parts is not equal to 2 then the field is not holding the index.
	if len(parts) != 2 {
		delete(m, field)
		return
	}

	// Validate the first part of the field.
	// if the parts[0] is not an array send an error.
	// Example: containers should hold a type []interface{}
	arr := m[parts[0]]
	arrValue, ok := arr.([]interface{})
	if !ok {
		logrus.Errorf("value cannot be set because %v is not a []interface{}", arr)
		return
	}

	// Convert the array index to int.
	// Example: Here the second part string "3" is converted to int to use it as Index.
	var arrIndex int
	_, err := fmt.Sscanf(parts[1], "%d", &arrIndex)
	if err != nil {
		logrus.Errorf("Error while parsing the array[%v] index :%v ", parts[0], err)
		return
	}

	// If the index exists remove the appropriate array item from the list.
	if arrIndex < len(arrValue) {
		arrValue = append(arrValue[:arrIndex], arrValue[arrIndex+1:]...)
		m[parts[0]] = arrValue
		return
	}
}

// setMapKeyWithValue is to assign the value to the map m with key field.
// if the field contains an array index then the approriate array element is updated.
// Example: containers[3]
func setMapKeyWithValue(m map[string]interface{}, value interface{}, field string) error {
	// check if an array index exists in the field
	parts := strings.FieldsFunc(field, indexDelimeter)
	// Example: Here the parts is []string{conatiners, 3}
	// if the length of the parts is not equal to 2 then the field is not holding the index.
	if len(parts) != 2 {
		m[field] = value
		return nil
	}

	// Validate the first part of the field.
	// if the parts[0] is not an array send an error.
	// Example: containers should hold a type []interface{}
	arr := m[parts[0]]
	arrValue, ok := arr.([]interface{})
	if !ok {
		return fmt.Errorf("value cannot be set because %v is not a []interface{}", arr)
	}

	// Convert the array index to int.
	// Example: Here the second part string "3" is converted to int to use it as Index.
	var arrIndex int
	_, err := fmt.Sscanf(parts[1], "%d", &arrIndex)
	if err != nil {
		return err
	}

	// update the approriate array element.
	// Example: If the 3 is lessthan the length of the array appropriate array element is updated.
	if arrIndex < len(arrValue) {
		arrValue[arrIndex] = value
	} else if arrIndex > len(arrValue) {
		// Example: If the 3 is greather the length of the array an error is return for out of range in the array.
		return fmt.Errorf("value cannot be set because index %d is out of range in array %v with length %d", arrIndex, arr, len(arrValue))
	} else {
		// Example: If the 3 is equal to the length of the array.
		// append the value to the existing array
		arrValue = append(arrValue, value)
	}
	// finally update the actual data structure m
	m[parts[0]] = arrValue
	return nil
}

// getValueFromMapKey is to retrive the value for the map m with key field. Here the field may even contain the array index.
// Example: containers[3]
func getValueFromMapKey(m map[string]interface{}, field string) (interface{}, bool, error) {
	// check if an array index exists in the field.
	parts := strings.FieldsFunc(field, indexDelimeter)
	// Example: Here the parts is []string{conatiners, 3}
	// if the length of the parts is not equal to 2 then the field is not holding the index.
	if len(parts) != 2 {
		value, ok := m[field]
		return value, ok, nil
	}

	// Validate the first part of the field.
	// if the parts[0] is not an array send an error.
	// Example: containers should hold a type []interface{}
	arr := m[parts[0]]
	value, ok := arr.([]interface{})
	if !ok {
		return nil, false, fmt.Errorf("value cannot be set because %v is not a []interface{}", arr)
	}

	// Convert the array index to int.
	// Example: Here the second part string "3" is converted to int to use it as Index.
	var arrIndex int
	_, err := fmt.Sscanf(parts[1], "%d", &arrIndex)
	if err != nil {
		return nil, false, err
	}

	// send the approriate array element.
	// Example: If the 3 is lessthan the length of the array appropriate array element is returned.
	if arrIndex < len(value) {
		return value[arrIndex], true, nil
	} else if arrIndex > len(value) {
		// Example: If the 3 is greather the length of the array an error is return for out of range in the array.
		return nil, false, fmt.Errorf("value cannot be set because index %d is out of range in array %v with length %d", arrIndex, arr, len(value))
	}
	// Example: If the 3 is equal to the length of the array nil error and exists=false is returned.
	return nil, false, nil
}
