// A REST client to interact with LINSTOR's REST API
// Copyright (C) LINBIT HA-Solutions GmbH
// All Rights Reserved.
// Author: Roland Kammerer <roland.kammerer@linbit.com>
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package client

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"

	"github.com/google/go-querystring/query"

	"github.com/LINBIT/golinstor/clonestatus"
	"github.com/LINBIT/golinstor/devicelayerkind"
)

// ResourceDefinitionService is a struct for the client pointer
type ResourceDefinitionService struct {
	client *Client
}

// ResourceDefinition is a struct to store the information about a resource-definition
type ResourceDefinition struct {
	Name string `json:"name,omitempty"`
	// External name can be used to have native resource names. If you need to store a non Linstor compatible resource name use this field and Linstor will generate a compatible name.
	ExternalName string `json:"external_name,omitempty"`
	// A string to string property map.
	Props     map[string]string         `json:"props,omitempty"`
	Flags     []string                  `json:"flags,omitempty"`
	LayerData []ResourceDefinitionLayer `json:"layer_data,omitempty"`
	// unique object id
	Uuid string `json:"uuid,omitempty"`
	// name of the linked resource group, if there is a link
	ResourceGroupName string `json:"resource_group_name,omitempty"`
}

// ResourceDefinitionCreate is a struct for holding the data needed to create a resource-defintion
type ResourceDefinitionCreate struct {
	// drbd port for resources
	DrbdPort int32 `json:"drbd_port,omitempty"`
	// drbd resource secret
	DrbdSecret         string             `json:"drbd_secret,omitempty"`
	DrbdTransportType  string             `json:"drbd_transport_type,omitempty"`
	ResourceDefinition ResourceDefinition `json:"resource_definition"`
}

// ResourceDefinitionLayer is a struct for the storing the layertype of a resource-defintion
type ResourceDefinitionLayer struct {
	Type devicelayerkind.DeviceLayerKind                                 `json:"type,omitempty"`
	Data OneOfDrbdResourceDefinitionLayerOpenflexResourceDefinitionLayer `json:"data,omitempty"`
}

// DrbdResourceDefinitionLayer is a struct which contains the information about the layertype of a resource-definition on drbd level
type DrbdResourceDefinitionLayer struct {
	ResourceNameSuffix string `json:"resource_name_suffix,omitempty"`
	PeerSlots          int32  `json:"peer_slots,omitempty"`
	AlStripes          int64  `json:"al_stripes,omitempty"`
	// used drbd port for this resource
	Port          int32  `json:"port,omitempty"`
	TransportType string `json:"transport_type,omitempty"`
	// drbd resource secret
	Secret string `json:"secret,omitempty"`
	Down   bool   `json:"down,omitempty"`
}

// VolumeDefinitionCreate is a struct used for creating volume-definitions
type VolumeDefinitionCreate struct {
	VolumeDefinition VolumeDefinition `json:"volume_definition"`
	DrbdMinorNumber  int32            `json:"drbd_minor_number,omitempty"`
}

// VolumeDefinition is a struct which is used to store volume-definition properties
type VolumeDefinition struct {
	VolumeNumber int32 `json:"volume_number,omitempty"`
	// Size of the volume in Kibi.
	SizeKib uint64 `json:"size_kib"`
	// A string to string property map.
	Props     map[string]string       `json:"props,omitempty"`
	Flags     []string                `json:"flags,omitempty"`
	LayerData []VolumeDefinitionLayer `json:"layer_data,omitempty"`
	// unique object id
	Uuid string `json:"uuid,omitempty"`
}

type VolumeDefinitionModify struct {
	SizeKib uint64 `json:"size_kib,omitempty"`
	GenericPropsModify
	// To add a flag just specify the flag name, to remove a flag prepend it with a '-'.  Flags:   * GROSS_SIZE
	Flags []string `json:"flags,omitempty"`
}

// VolumeDefinitionLayer is a struct for the layer-type of a volume-definition
type VolumeDefinitionLayer struct {
	Type devicelayerkind.DeviceLayerKind `json:"type"`
	Data OneOfDrbdVolumeDefinition       `json:"data,omitempty"`
}

// DrbdVolumeDefinition is a struct containing volume-definition on drbd level
type DrbdVolumeDefinition struct {
	ResourceNameSuffix string `json:"resource_name_suffix,omitempty"`
	VolumeNumber       int32  `json:"volume_number,omitempty"`
	MinorNumber        int32  `json:"minor_number,omitempty"`
}

type ResourceDefinitionCloneRequest struct {
	Name         string `json:"name,omitempty"`
	ExternalName string `json:"external_name,omitempty"`
}

type ResourceDefinitionCloneStarted struct {
	// Path for clone status
	Location string `json:"location"`
	// name of the source resource
	SourceName string `json:"source_name"`
	// name of the clone resource
	CloneName string       `json:"clone_name"`
	Messages  *[]ApiCallRc `json:"messages,omitempty"`
}

type ResourceDefinitionCloneStatus struct {
	Status clonestatus.CloneStatus `json:"status"`
}

// custom code

type ResourceDefinitionWithVolumeDefinition struct {
	ResourceDefinition
	VolumeDefinitions []VolumeDefinition `json:"volume_definitions,omitempty"`
}

type RDGetAllRequest struct {
	// ResourceDefinitions filters the returned resource definitions by the given names
	ResourceDefinitions   []string `url:"resource_definitions,omitempty"`
	// Props filters the returned resource definitions on their property values (uses key=value syntax)
	Props                 []string `url:"props,omitempty"`
	Offset                int      `url:"offset,omitempty"`
	Limit                 int      `url:"offset,omitempty"`
	// WithVolumeDefinitions, if set to true, LINSTOR will also include volume definitions in the response.
	WithVolumeDefinitions bool     `url:"with_volume_definitions,omitempty"`
}

// ResourceDefinitionProvider acts as an abstraction for a
// ResourceDefinitionService. It can be swapped out for another
// ResourceDefinitionService implementation, for example for testing.
type ResourceDefinitionProvider interface {
	// GetAll lists all resource-definitions
	GetAll(ctx context.Context, request RDGetAllRequest) ([]ResourceDefinitionWithVolumeDefinition, error)
	// Get return information about a resource-defintion
	Get(ctx context.Context, resDefName string, opts ...*ListOpts) (ResourceDefinition, error)
	// Create adds a new resource-definition
	Create(ctx context.Context, resDef ResourceDefinitionCreate) error
	// Modify allows to modify a resource-definition
	Modify(ctx context.Context, resDefName string, props GenericPropsModify) error
	// Delete completely deletes a resource-definition
	Delete(ctx context.Context, resDefName string) error
	// GetVolumeDefinitions returns all volume-definitions of a resource-definition
	GetVolumeDefinitions(ctx context.Context, resDefName string, opts ...*ListOpts) ([]VolumeDefinition, error)
	// GetVolumeDefinition shows the properties of a specific volume-definition
	GetVolumeDefinition(ctx context.Context, resDefName string, volNr int, opts ...*ListOpts) (VolumeDefinition, error)
	// CreateVolumeDefinition adds a volume-definition to a resource-definition. Only the size is required.
	CreateVolumeDefinition(ctx context.Context, resDefName string, volDef VolumeDefinitionCreate) error
	// ModifyVolumeDefinition give the abilty to modify a specific volume-definition
	ModifyVolumeDefinition(ctx context.Context, resDefName string, volNr int, props VolumeDefinitionModify) error
	// DeleteVolumeDefinition deletes a specific volume-definition
	DeleteVolumeDefinition(ctx context.Context, resDefName string, volNr int) error
	// GetPropsInfos gets meta information about the properties that can be
	// set on a resource definition.
	GetPropsInfos(ctx context.Context, opts ...*ListOpts) ([]PropsInfo, error)
	// GetDRBDProxyPropsInfos gets meta information about the properties
	// that can be set on a resource definition for drbd proxy.
	GetDRBDProxyPropsInfos(ctx context.Context, resDefName string, opts ...*ListOpts) ([]PropsInfo, error)
	// AttachExternalFile adds an external file to the resource definition. This
	// means that the file will be deployed to every node the resource is deployed on.
	AttachExternalFile(ctx context.Context, resDefName string, filePath string) error
	// DetachExternalFile removes a binding between an external file and a resource definition.
	// This means that the file will no longer be deployed on every node the resource
	// is deployed on.
	DetachExternalFile(ctx context.Context, resDefName string, filePath string) error
	// Clone starts cloning a resource definition and all resources using a method optimized for the storage driver.
	Clone(ctx context.Context, srcResDef string, request ResourceDefinitionCloneRequest) (ResourceDefinitionCloneStarted, error)
	// CloneStatus fetches the current status of a clone operation started by Clone.
	CloneStatus(ctx context.Context, srcResDef, targetResDef string) (ResourceDefinitionCloneStatus, error)
}

var _ ResourceDefinitionProvider = &ResourceDefinitionService{}

// resourceDefinitionLayerIn is a struct for resource-definitions
type resourceDefinitionLayerIn struct {
	Type devicelayerkind.DeviceLayerKind `json:"type,omitempty"`
	Data json.RawMessage                 `json:"data,omitempty"`
}

// UnmarshalJSON is needed for the unmarshal interface for ResourceDefinitionLayer types
func (rd *ResourceDefinitionLayer) UnmarshalJSON(b []byte) error {
	var rdIn resourceDefinitionLayerIn
	if err := json.Unmarshal(b, &rdIn); err != nil {
		return err
	}

	rd.Type = rdIn.Type
	switch rd.Type {
	case devicelayerkind.Drbd:
		dst := new(DrbdResourceDefinitionLayer)
		if rdIn.Data != nil {
			if err := json.Unmarshal(rdIn.Data, &dst); err != nil {
				return err
			}
		}
		rd.Data = dst
	case devicelayerkind.Openflex:
		dst := new(OpenflexResourceDefinitionLayer)
		if rdIn.Data != nil {
			if err := json.Unmarshal(rdIn.Data, &dst); err != nil {
				return err
			}
		}
		rd.Data = dst
	case devicelayerkind.Luks, devicelayerkind.Storage, devicelayerkind.Nvme, devicelayerkind.Writecache, devicelayerkind.Cache, devicelayerkind.Exos: // valid types, but do not set data
	default:
		return fmt.Errorf("'%+v' is not a valid type to Unmarshal", rd.Type)
	}

	return nil
}

// OneOfDrbdResourceDefinitionLayerOpenflexResourceDefinitionLayer is used to limit to these layer types
type OneOfDrbdResourceDefinitionLayerOpenflexResourceDefinitionLayer interface {
	isOneOfDrbdResourceDefinitionLayerOpenflexResourceDefinitionLayer()
}

// Function used if resource-definition-layertype is correct
func (d *DrbdResourceDefinitionLayer) isOneOfDrbdResourceDefinitionLayerOpenflexResourceDefinitionLayer() {
}

//volumeDefinitionLayerIn is a struct for volume-defintion-layers
type volumeDefinitionLayerIn struct {
	Type devicelayerkind.DeviceLayerKind `json:"type,omitempty"`
	Data json.RawMessage                 `json:"data,omitempty"`
}

// UnmarshalJSON is needed for the unmarshal interface for VolumeDefinitionLayer types
func (vd *VolumeDefinitionLayer) UnmarshalJSON(b []byte) error {
	var vdIn volumeDefinitionLayerIn
	if err := json.Unmarshal(b, &vdIn); err != nil {
		return err
	}

	vd.Type = vdIn.Type
	switch vd.Type {
	case devicelayerkind.Drbd:
		dst := new(DrbdVolumeDefinition)
		if vdIn.Data != nil {
			if err := json.Unmarshal(vdIn.Data, &dst); err != nil {
				return err
			}
		}
		vd.Data = dst
	case devicelayerkind.Luks, devicelayerkind.Storage, devicelayerkind.Nvme, devicelayerkind.Writecache, devicelayerkind.Openflex, devicelayerkind.Cache, devicelayerkind.Exos: // valid types, but do not set data
	default:
		return fmt.Errorf("'%+v' is not a valid type to Unmarshal", vd.Type)
	}

	return nil
}

// OneOfDrbdVolumeDefinition is used to prevent other layertypes than drbd-volume-defintion
type OneOfDrbdVolumeDefinition interface {
	isOneOfDrbdVolumeDefinition()
}

// Function used if volume-defintion-layertype is correct
func (d *DrbdVolumeDefinition) isOneOfDrbdVolumeDefinition() {}

// GetAll lists all resource-definitions
func (n *ResourceDefinitionService) GetAll(ctx context.Context, request RDGetAllRequest) ([]ResourceDefinitionWithVolumeDefinition, error) {
	val, err := query.Values(request)
	if err != nil {
		return nil, err
	}

	var resDefs []ResourceDefinitionWithVolumeDefinition
	_, err = n.client.doGET(ctx, "/v1/resource-definitions?"+val.Encode(), &resDefs)
	return resDefs, err
}

// Get return information about a resource-defintion
func (n *ResourceDefinitionService) Get(ctx context.Context, resDefName string, opts ...*ListOpts) (ResourceDefinition, error) {
	var resDef ResourceDefinition
	_, err := n.client.doGET(ctx, "/v1/resource-definitions/"+resDefName, &resDef, opts...)
	return resDef, err
}

// Create adds a new resource-definition
func (n *ResourceDefinitionService) Create(ctx context.Context, resDef ResourceDefinitionCreate) error {
	_, err := n.client.doPOST(ctx, "/v1/resource-definitions", resDef)
	return err
}

// Modify allows to modify a resource-definition
func (n *ResourceDefinitionService) Modify(ctx context.Context, resDefName string, props GenericPropsModify) error {
	_, err := n.client.doPUT(ctx, "/v1/resource-definitions/"+resDefName, props)
	return err
}

// Delete completely deletes a resource-definition
func (n *ResourceDefinitionService) Delete(ctx context.Context, resDefName string) error {
	_, err := n.client.doDELETE(ctx, "/v1/resource-definitions/"+resDefName, nil)
	return err
}

// GetVolumeDefinitions returns all volume-definitions of a resource-definition
func (n *ResourceDefinitionService) GetVolumeDefinitions(ctx context.Context, resDefName string, opts ...*ListOpts) ([]VolumeDefinition, error) {
	var volDefs []VolumeDefinition
	_, err := n.client.doGET(ctx, "/v1/resource-definitions/"+resDefName+"/volume-definitions", &volDefs, opts...)
	return volDefs, err
}

// GetVolumeDefinition shows the properties of a specific volume-definition
func (n *ResourceDefinitionService) GetVolumeDefinition(ctx context.Context, resDefName string, volNr int, opts ...*ListOpts) (VolumeDefinition, error) {
	var volDef VolumeDefinition
	_, err := n.client.doGET(ctx, "/v1/resource-definitions/"+resDefName+"/volume-definitions/"+strconv.Itoa(volNr), &volDef, opts...)
	return volDef, err
}

// CreateVolumeDefinition adds a volume-definition to a resource-definition. Only the size is required.
func (n *ResourceDefinitionService) CreateVolumeDefinition(ctx context.Context, resDefName string, volDef VolumeDefinitionCreate) error {
	_, err := n.client.doPOST(ctx, "/v1/resource-definitions/"+resDefName+"/volume-definitions", volDef)
	return err
}

// ModifyVolumeDefinition give the abilty to modify a specific volume-definition
func (n *ResourceDefinitionService) ModifyVolumeDefinition(ctx context.Context, resDefName string, volNr int, props VolumeDefinitionModify) error {
	_, err := n.client.doPUT(ctx, "/v1/resource-definitions/"+resDefName+"/volume-definitions/"+strconv.Itoa(volNr), props)
	return err
}

// DeleteVolumeDefinition deletes a specific volume-definition
func (n *ResourceDefinitionService) DeleteVolumeDefinition(ctx context.Context, resDefName string, volNr int) error {
	_, err := n.client.doDELETE(ctx, "/v1/resource-definitions/"+resDefName+"/volume-definitions/"+strconv.Itoa(volNr), nil)
	return err
}

// GetPropsInfos gets meta information about the properties that can be set on
// a resource definition.
func (n *ResourceDefinitionService) GetPropsInfos(ctx context.Context, opts ...*ListOpts) ([]PropsInfo, error) {
	var infos []PropsInfo
	_, err := n.client.doGET(ctx, "/v1/resource-definitions/properties/info", &infos, opts...)
	return infos, err
}

// GetDRBDProxyPropsInfos gets meta information about the properties that can
// be set on a resource definition for drbd proxy.
func (n *ResourceDefinitionService) GetDRBDProxyPropsInfos(ctx context.Context, resDefName string, opts ...*ListOpts) ([]PropsInfo, error) {
	var infos []PropsInfo
	_, err := n.client.doGET(ctx, "/v1/resource-definitions/"+resDefName+"/drbd-proxy/properties/info", &infos, opts...)
	return infos, err
}

// AttachExternalFile adds an external file to the resource definition. This
// means that the file will be deployed to every node the resource is deployed on.
func (n *ResourceDefinitionService) AttachExternalFile(ctx context.Context, resDefName string, filePath string) error {
	_, err := n.client.doPOST(ctx, "/v1/resource-definitions/"+resDefName+"/files/"+url.QueryEscape(filePath), nil)
	return err
}

// DetachExternalFile removes a binding between an external file and a resource definition.
// This means that the file will no longer be deployed on every node the resource
// is deployed on.
func (n *ResourceDefinitionService) DetachExternalFile(ctx context.Context, resDefName string, filePath string) error {
	_, err := n.client.doDELETE(ctx, "/v1/resource-definitions/"+resDefName+"/files/"+url.QueryEscape(filePath), nil)
	return err
}

// Clone starts cloning a resource definition and all resources using a method optimized for the storage driver.
func (n *ResourceDefinitionService) Clone(ctx context.Context, srcResDef string, request ResourceDefinitionCloneRequest) (ResourceDefinitionCloneStarted, error) {
	var resp ResourceDefinitionCloneStarted

	req, err := n.client.newRequest("POST", "/v1/resource-definitions/"+srcResDef+"/clone", request)
	if err != nil {
		return ResourceDefinitionCloneStarted{}, err
	}

	_, err = n.client.do(ctx, req, &resp)
	if err != nil {
		return ResourceDefinitionCloneStarted{}, err
	}

	return resp, nil
}

// CloneStatus fetches the current status of a clone operation started by Clone.
func (n *ResourceDefinitionService) CloneStatus(ctx context.Context, srcResDef, targetResDef string) (ResourceDefinitionCloneStatus, error) {
	var status ResourceDefinitionCloneStatus
	_, err := n.client.doGET(ctx, "/v1/resource-definitions/"+srcResDef+"/clone/"+targetResDef, &status)
	return status, err
}
