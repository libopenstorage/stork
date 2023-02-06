package v1alpha1

import (
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	// ResourceTransformationResourceName is name for "ResourceTransformation" resource
	ResourceTransformationResourceName = "resourcetransformation"
	// ResourceTransformationResourcePlural is plural for "ResourceTransformation" resource
	ResourceTransformationResourcePlural = "resourcetransformations"
)

// ResourceTransformationOperationType is type of operation supported for
// resource transformation
type ResourceTransformationOperationType string

const (
	// AddPathValue is used to add path+value in specified resource spec
	// if path+value already exist this operation will override value
	// at given path
	AddResourcePath ResourceTransformationOperationType = "add"
	// ModifyResourcePathValue is used to merge value at speficied resource path
	// in case of a slice, entries will be appended.
	// in case of a keypair, entries will be merged
	ModifyResourcePathValue ResourceTransformationOperationType = "modify"
	// DeletePath from resource specification
	DeleteResourcePath ResourceTransformationOperationType = "delete"
	// JsonResourcePatch will patch json in given resource spec
	JsonResourcePatch ResourceTransformationOperationType = "jsonpatch"
)

// ResourceTransformationValueType is types of value supported on
// path in resource specs
type ResourceTransformationValueType string

type KindResourceTransform map[string][]TransformResourceInfo

const (
	// IntResourceType is to update integer value to specified resource path
	IntResourceType ResourceTransformationValueType = "int"
	// StringResourceType is to update string value to specified resource path
	StringResourceType ResourceTransformationValueType = "string"
	// BoolResourceType is to update boolean value to specified resource path
	BoolResourceType ResourceTransformationValueType = "bool"
	// SliceResourceType is to update slice value to specified resource path
	SliceResourceType ResourceTransformationValueType = "slice"
	// KeyPairResourceType is to update keypair value to specified resource path
	KeyPairResourceType ResourceTransformationValueType = "keypair"
)

// ResourceTransformationStatsusType is status of resource transformation CR
type ResourceTransformationStatusType string

const (
	// ResourceTransformationStatusInitial represents initial state of resource
	// transformation CR
	ResourceTransformationStatusInitial ResourceTransformationStatusType = ""
	// ResourceTransformationStatusInProgress represents dry run in progress state
	// of resource transformation
	ResourceTransformationStatusInProgress ResourceTransformationStatusType = "InProgress"
	// ResourceTransformationStatusReady represents ready state of resource
	// transformation CR
	ResourceTransformationStatusReady ResourceTransformationStatusType = "Ready"
	// ResourceTransformationStatusFailed represents dry-run failed state of resource
	// transformation CR
	ResourceTransformationStatusFailed ResourceTransformationStatusType = "Failed"
)

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ResourceTransformation represents a ResourceTransformation CR object
type ResourceTransformation struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              ResourceTransformationSpec   `json:"specs"`
	Status            ResourceTransformationStatus `json:"status"`
}

type ResourceTransformationStatus struct {
	Status    ResourceTransformationStatusType `json:"status"`
	Resources []*TransformResourceInfo         `json:"resources"`
}

// TransformResourceInfo is the info of resources selected
// for transformation
type TransformResourceInfo struct {
	Name                  string `json:"name"`
	Namespace             string `json:"namespace"`
	meta.GroupVersionKind `json:",inline"`
	Status                ResourceTransformationStatusType `json:"status"`
	Reason                string                           `json:"reason"`
	Specs                 TransformSpecs                   `json:"specs"`
}

// ResourceTransformationSpec is used to update k8s resources
// before migration/restore
type ResourceTransformationSpec struct {
	Objects []TransformSpecs `json:"transformSpecs"`
}

// TransformSpecs specifies the patch to update selected resource
// before migration/restore
type TransformSpecs struct {
	// Resource is GroupVersionKind for k8s resources
	// should be in format `group/version/kind"
	Resource string `json:"resource"`
	// Selectors label selector to filter out resource for
	// patching
	Selectors map[string]string `json:"selectors"`
	// Paths collection of resource path to update
	Paths []ResourcePaths `json:"paths"`
}

type TransformSpecPatch struct {
	GVK map[string]PatchStruct
}
type PatchStruct struct {
	// namespace - resource in namespace
	Resources map[string]TransformResourceInfo
}

// ResourcePaths specifies the patch to modify resource
// before migration/restore
type ResourcePaths struct {
	// Path k8s resource for operation
	Path string `json:"path"`
	// Value for given k8s path
	Value string `json:"value"`
	// Type of value specified int/bool/string/slice/keypair
	Type ResourceTransformationValueType `json:"type"`
	// Operation to be performed on path
	// add/modify/delete/replace/jsonPatch
	Operation ResourceTransformationOperationType `json:"operation"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ResourceTransformationList is a list of ResourceTransformations
type ResourceTransformationList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ResourceTransformation `json:"items"`
}
