/*
PDS API

Portworx Data Services API Server

API version: 1.0.0
*/

// Code generated by OpenAPI Generator (https://openapi-generator.tech); DO NOT EDIT.

package pds

import (
	"encoding/json"
)

// ControllersPaginatedTenantProjects struct for ControllersPaginatedTenantProjects
type ControllersPaginatedTenantProjects struct {
	Data []ModelsProject `json:"data,omitempty"`
	Pagination *ConstraintPagination `json:"pagination,omitempty"`
}

// NewControllersPaginatedTenantProjects instantiates a new ControllersPaginatedTenantProjects object
// This constructor will assign default values to properties that have it defined,
// and makes sure properties required by API are set, but the set of arguments
// will change when the set of required properties is changed
func NewControllersPaginatedTenantProjects() *ControllersPaginatedTenantProjects {
	this := ControllersPaginatedTenantProjects{}
	return &this
}

// NewControllersPaginatedTenantProjectsWithDefaults instantiates a new ControllersPaginatedTenantProjects object
// This constructor will only assign default values to properties that have it defined,
// but it doesn't guarantee that properties required by API are set
func NewControllersPaginatedTenantProjectsWithDefaults() *ControllersPaginatedTenantProjects {
	this := ControllersPaginatedTenantProjects{}
	return &this
}

// GetData returns the Data field value if set, zero value otherwise.
func (o *ControllersPaginatedTenantProjects) GetData() []ModelsProject {
	if o == nil || o.Data == nil {
		var ret []ModelsProject
		return ret
	}
	return o.Data
}

// GetDataOk returns a tuple with the Data field value if set, nil otherwise
// and a boolean to check if the value has been set.
func (o *ControllersPaginatedTenantProjects) GetDataOk() ([]ModelsProject, bool) {
	if o == nil || o.Data == nil {
		return nil, false
	}
	return o.Data, true
}

// HasData returns a boolean if a field has been set.
func (o *ControllersPaginatedTenantProjects) HasData() bool {
	if o != nil && o.Data != nil {
		return true
	}

	return false
}

// SetData gets a reference to the given []ModelsProject and assigns it to the Data field.
func (o *ControllersPaginatedTenantProjects) SetData(v []ModelsProject) {
	o.Data = v
}

// GetPagination returns the Pagination field value if set, zero value otherwise.
func (o *ControllersPaginatedTenantProjects) GetPagination() ConstraintPagination {
	if o == nil || o.Pagination == nil {
		var ret ConstraintPagination
		return ret
	}
	return *o.Pagination
}

// GetPaginationOk returns a tuple with the Pagination field value if set, nil otherwise
// and a boolean to check if the value has been set.
func (o *ControllersPaginatedTenantProjects) GetPaginationOk() (*ConstraintPagination, bool) {
	if o == nil || o.Pagination == nil {
		return nil, false
	}
	return o.Pagination, true
}

// HasPagination returns a boolean if a field has been set.
func (o *ControllersPaginatedTenantProjects) HasPagination() bool {
	if o != nil && o.Pagination != nil {
		return true
	}

	return false
}

// SetPagination gets a reference to the given ConstraintPagination and assigns it to the Pagination field.
func (o *ControllersPaginatedTenantProjects) SetPagination(v ConstraintPagination) {
	o.Pagination = &v
}

func (o ControllersPaginatedTenantProjects) MarshalJSON() ([]byte, error) {
	toSerialize := map[string]interface{}{}
	if o.Data != nil {
		toSerialize["data"] = o.Data
	}
	if o.Pagination != nil {
		toSerialize["pagination"] = o.Pagination
	}
	return json.Marshal(toSerialize)
}

type NullableControllersPaginatedTenantProjects struct {
	value *ControllersPaginatedTenantProjects
	isSet bool
}

func (v NullableControllersPaginatedTenantProjects) Get() *ControllersPaginatedTenantProjects {
	return v.value
}

func (v *NullableControllersPaginatedTenantProjects) Set(val *ControllersPaginatedTenantProjects) {
	v.value = val
	v.isSet = true
}

func (v NullableControllersPaginatedTenantProjects) IsSet() bool {
	return v.isSet
}

func (v *NullableControllersPaginatedTenantProjects) Unset() {
	v.value = nil
	v.isSet = false
}

func NewNullableControllersPaginatedTenantProjects(val *ControllersPaginatedTenantProjects) *NullableControllersPaginatedTenantProjects {
	return &NullableControllersPaginatedTenantProjects{value: val, isSet: true}
}

func (v NullableControllersPaginatedTenantProjects) MarshalJSON() ([]byte, error) {
	return json.Marshal(v.value)
}

func (v *NullableControllersPaginatedTenantProjects) UnmarshalJSON(src []byte) error {
	v.isSet = true
	return json.Unmarshal(src, &v.value)
}

