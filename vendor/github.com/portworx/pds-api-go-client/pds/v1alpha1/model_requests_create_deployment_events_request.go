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

// RequestsCreateDeploymentEventsRequest struct for RequestsCreateDeploymentEventsRequest
type RequestsCreateDeploymentEventsRequest struct {
	Events []RequestsDeploymentEvent `json:"events"`
}

// NewRequestsCreateDeploymentEventsRequest instantiates a new RequestsCreateDeploymentEventsRequest object
// This constructor will assign default values to properties that have it defined,
// and makes sure properties required by API are set, but the set of arguments
// will change when the set of required properties is changed
func NewRequestsCreateDeploymentEventsRequest(events []RequestsDeploymentEvent) *RequestsCreateDeploymentEventsRequest {
	this := RequestsCreateDeploymentEventsRequest{}
	this.Events = events
	return &this
}

// NewRequestsCreateDeploymentEventsRequestWithDefaults instantiates a new RequestsCreateDeploymentEventsRequest object
// This constructor will only assign default values to properties that have it defined,
// but it doesn't guarantee that properties required by API are set
func NewRequestsCreateDeploymentEventsRequestWithDefaults() *RequestsCreateDeploymentEventsRequest {
	this := RequestsCreateDeploymentEventsRequest{}
	return &this
}

// GetEvents returns the Events field value
func (o *RequestsCreateDeploymentEventsRequest) GetEvents() []RequestsDeploymentEvent {
	if o == nil {
		var ret []RequestsDeploymentEvent
		return ret
	}

	return o.Events
}

// GetEventsOk returns a tuple with the Events field value
// and a boolean to check if the value has been set.
func (o *RequestsCreateDeploymentEventsRequest) GetEventsOk() ([]RequestsDeploymentEvent, bool) {
	if o == nil  {
		return nil, false
	}
	return o.Events, true
}

// SetEvents sets field value
func (o *RequestsCreateDeploymentEventsRequest) SetEvents(v []RequestsDeploymentEvent) {
	o.Events = v
}

func (o RequestsCreateDeploymentEventsRequest) MarshalJSON() ([]byte, error) {
	toSerialize := map[string]interface{}{}
	if true {
		toSerialize["events"] = o.Events
	}
	return json.Marshal(toSerialize)
}

type NullableRequestsCreateDeploymentEventsRequest struct {
	value *RequestsCreateDeploymentEventsRequest
	isSet bool
}

func (v NullableRequestsCreateDeploymentEventsRequest) Get() *RequestsCreateDeploymentEventsRequest {
	return v.value
}

func (v *NullableRequestsCreateDeploymentEventsRequest) Set(val *RequestsCreateDeploymentEventsRequest) {
	v.value = val
	v.isSet = true
}

func (v NullableRequestsCreateDeploymentEventsRequest) IsSet() bool {
	return v.isSet
}

func (v *NullableRequestsCreateDeploymentEventsRequest) Unset() {
	v.value = nil
	v.isSet = false
}

func NewNullableRequestsCreateDeploymentEventsRequest(val *RequestsCreateDeploymentEventsRequest) *NullableRequestsCreateDeploymentEventsRequest {
	return &NullableRequestsCreateDeploymentEventsRequest{value: val, isSet: true}
}

func (v NullableRequestsCreateDeploymentEventsRequest) MarshalJSON() ([]byte, error) {
	return json.Marshal(v.value)
}

func (v *NullableRequestsCreateDeploymentEventsRequest) UnmarshalJSON(src []byte) error {
	v.isSet = true
	return json.Unmarshal(src, &v.value)
}

