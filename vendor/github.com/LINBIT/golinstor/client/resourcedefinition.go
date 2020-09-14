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
	"strconv"
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
	Type LayerType                                                       `json:"type,omitempty"`
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

// LayerType initialized as string
type LayerType string

// List of LayerType
const (
	DRBD       LayerType = "DRBD"
	LUKS       LayerType = "LUKS"
	STORAGE    LayerType = "STORAGE"
	NVME       LayerType = "NVME"
	WRITECACHE LayerType = "WRITECACHE"
	CACHE      LayerType = "CACHE"
	OPENFLEX   LayerType = "OPENFLEX"
)

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
	Type LayerType                 `json:"type"`
	Data OneOfDrbdVolumeDefinition `json:"data,omitempty"`
}

// DrbdVolumeDefinition is a struct containing volume-definition on drbd level
type DrbdVolumeDefinition struct {
	ResourceNameSuffix string `json:"resource_name_suffix,omitempty"`
	VolumeNumber       int32  `json:"volume_number,omitempty"`
	MinorNumber        int32  `json:"minor_number,omitempty"`
}

// custom code

// resourceDefinitionLayerIn is a struct for resource-definitions
type resourceDefinitionLayerIn struct {
	Type LayerType       `json:"type,omitempty"`
	Data json.RawMessage `json:"data,omitempty"`
}

// UnmarshalJSON is needed for the unmarshal interface for ResourceDefinitionLayer types
func (rd *ResourceDefinitionLayer) UnmarshalJSON(b []byte) error {
	var rdIn resourceDefinitionLayerIn
	if err := json.Unmarshal(b, &rdIn); err != nil {
		return err
	}

	rd.Type = rdIn.Type
	switch rd.Type {
	case DRBD:
		dst := new(DrbdResourceDefinitionLayer)
		if rdIn.Data != nil {
			if err := json.Unmarshal(rdIn.Data, &dst); err != nil {
				return err
			}
		}
		rd.Data = dst
	case OPENFLEX:
		dst := new(OpenflexResourceDefinitionLayer)
		if rdIn.Data != nil {
			if err := json.Unmarshal(rdIn.Data, &dst); err != nil {
				return err
			}
		}
		rd.Data = dst
	case LUKS, STORAGE, NVME, WRITECACHE, CACHE: // valid types, but do not set data
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
	Type LayerType       `json:"type,omitempty"`
	Data json.RawMessage `json:"data,omitempty"`
}

// UnmarshalJSON is needed for the unmarshal interface for VolumeDefinitionLayer types
func (vd *VolumeDefinitionLayer) UnmarshalJSON(b []byte) error {
	var vdIn volumeDefinitionLayerIn
	if err := json.Unmarshal(b, &vdIn); err != nil {
		return err
	}

	vd.Type = vdIn.Type
	switch vd.Type {
	case DRBD:
		dst := new(DrbdVolumeDefinition)
		if vdIn.Data != nil {
			if err := json.Unmarshal(vdIn.Data, &dst); err != nil {
				return err
			}
		}
		vd.Data = dst
	case LUKS, STORAGE, NVME, WRITECACHE: // valid types, but do not set data
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
func (n *ResourceDefinitionService) GetAll(ctx context.Context, opts ...*ListOpts) ([]ResourceDefinition, error) {
	var resDefs []ResourceDefinition
	_, err := n.client.doGET(ctx, "/v1/resource-definitions", &resDefs, opts...)
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
