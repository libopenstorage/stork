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

	"github.com/google/go-querystring/query"

	"github.com/LINBIT/golinstor/devicelayerkind"
	"github.com/LINBIT/golinstor/snapshotshipstatus"
)

// ResourceService is a struct which contains the pointer of the client
type ResourceService struct {
	client *Client
}

// copy & paste from generated code

// Resource is a struct which holds the information of a resource
type Resource struct {
	Name     string `json:"name,omitempty"`
	NodeName string `json:"node_name,omitempty"`
	// A string to string property map.
	Props       map[string]string `json:"props,omitempty"`
	Flags       []string          `json:"flags,omitempty"`
	LayerObject ResourceLayer     `json:"layer_object,omitempty"`
	State       ResourceState     `json:"state,omitempty"`
	// unique object id
	Uuid string `json:"uuid,omitempty"`
	// milliseconds since unix epoch in UTC
	CreateTimestamp *TimeStampMs `json:"create_timestamp,omitempty"`
}

type ResourceWithVolumes struct {
	Resource
	// milliseconds since unix epoch in UTC
	CreateTimestamp *TimeStampMs `json:"create_timestamp,omitempty"`
	Volumes         []Volume     `json:"volumes,omitempty"`
	// shared space name of the data storage pool of the first volume of
	// the resource or empty if data storage pool is not shared
	SharedName string `json:"shared_name,omitempty"`
}

type ResourceDefinitionModify struct {
	// drbd port for resources
	DrbdPort int32 `json:"drbd_port,omitempty"`
	// drbd peer slot number
	DrbdPeerSlots int32                             `json:"drbd_peer_slots,omitempty"`
	LayerStack    []devicelayerkind.DeviceLayerKind `json:"layer_stack,omitempty"`
	// change resource group to the given group name
	ResourceGroup string `json:"resource_group,omitempty"`
	GenericPropsModify
}

// ResourceCreate is a struct where the properties of a resource are stored to create it
type ResourceCreate struct {
	Resource   Resource                          `json:"resource,omitempty"`
	LayerList  []devicelayerkind.DeviceLayerKind `json:"layer_list,omitempty"`
	DrbdNodeId int32                             `json:"drbd_node_id,omitempty"`
}

// ResourceLayer is a struct to store layer-information abour a resource
type ResourceLayer struct {
	Children           []ResourceLayer                 `json:"children,omitempty"`
	ResourceNameSuffix string                          `json:"resource_name_suffix,omitempty"`
	Type               devicelayerkind.DeviceLayerKind `json:"type,omitempty"`
	Drbd               DrbdResource                    `json:"drbd,omitempty"`
	Luks               LuksResource                    `json:"luks,omitempty"`
	Storage            StorageResource                 `json:"storage,omitempty"`
	Nvme               NvmeResource                    `json:"nvme,omitempty"`
	Openflex           OpenflexResource                `json:"openflex,omitempty"`
	Writecache         WritecacheResource              `json:"writecache,omitempty"`
	Cache              CacheResource                   `json:"cache,omitempty"`
	BCache             BCacheResource                  `json:"bcache,omitempty"`
}

type WritecacheResource struct {
	WritecacheVolumes []WritecacheVolume `json:"writecache_volumes,omitempty"`
}

type WritecacheVolume struct {
	VolumeNumber int32 `json:"volume_number,omitempty"`
	// block device path
	DevicePath string `json:"device_path,omitempty"`
	// block device path used as cache device
	DevicePathCache  string `json:"device_path_cache,omitempty"`
	AllocatedSizeKib int64  `json:"allocated_size_kib,omitempty"`
	UsableSizeKib    int64  `json:"usable_size_kib,omitempty"`
	// String describing current volume state
	DiskState string `json:"disk_state,omitempty"`
}

type BCacheResource struct {
	BCacheVolumes []BCacheVolume `json:"bcache_volumes,omitempty"`
}

type BCacheVolume struct {
	VolumeNumber int32 `json:"volume_number,omitempty"`
	// block device path
	DevicePath string `json:"device_path,omitempty"`
	// block device path used as cache device
	DevicePathCache  string `json:"device_path_cache,omitempty"`
	AllocatedSizeKib int64  `json:"allocated_size_kib,omitempty"`
	UsableSizeKib    int64  `json:"usable_size_kib,omitempty"`
	// String describing current volume state
	DiskState string `json:"disk_state,omitempty"`
}

// DrbdResource is a struct used to give linstor drbd properties for a resource
type DrbdResource struct {
	DrbdResourceDefinition DrbdResourceDefinitionLayer `json:"drbd_resource_definition,omitempty"`
	NodeId                 int32                       `json:"node_id,omitempty"`
	PeerSlots              int32                       `json:"peer_slots,omitempty"`
	AlStripes              int32                       `json:"al_stripes,omitempty"`
	AlSize                 int64                       `json:"al_size,omitempty"`
	Flags                  []string                    `json:"flags,omitempty"`
	DrbdVolumes            []DrbdVolume                `json:"drbd_volumes,omitempty"`
	Connections            map[string]DrbdConnection   `json:"connections,omitempty"`
	PromotionScore         int32                       `json:"promotion_score,omitempty"`
	MayPromote             bool                        `json:"may_promote,omitempty"`
}

// DrbdConnection is a struct representing the DRBD connection status
type DrbdConnection struct {
	Connected bool `json:"connected,omitempty"`
	// DRBD connection status
	Message string `json:"message,omitempty"`
}

// DrbdVolume is a struct for linstor to get inormation about a drbd-volume
type DrbdVolume struct {
	DrbdVolumeDefinition DrbdVolumeDefinition `json:"drbd_volume_definition,omitempty"`
	// drbd device path e.g. '/dev/drbd1000'
	DevicePath string `json:"device_path,omitempty"`
	// block device used by drbd
	BackingDevice    string `json:"backing_device,omitempty"`
	MetaDisk         string `json:"meta_disk,omitempty"`
	AllocatedSizeKib int64  `json:"allocated_size_kib,omitempty"`
	UsableSizeKib    int64  `json:"usable_size_kib,omitempty"`
	// String describing current volume state
	DiskState string `json:"disk_state,omitempty"`
	// Storage pool name used for external meta data; null for internal
	ExtMetaStorPool string `json:"ext_meta_stor_pool,omitempty"`
}

// LuksResource is a struct to store storage-volumes for a luks-resource
type LuksResource struct {
	StorageVolumes []LuksVolume `json:"storage_volumes,omitempty"`
}

// LuksVolume is a struct used for information about a luks-volume
type LuksVolume struct {
	VolumeNumber int32 `json:"volume_number,omitempty"`
	// block device path
	DevicePath string `json:"device_path,omitempty"`
	// block device used by luks
	BackingDevice    string `json:"backing_device,omitempty"`
	AllocatedSizeKib int64  `json:"allocated_size_kib,omitempty"`
	UsableSizeKib    int64  `json:"usable_size_kib,omitempty"`
	// String describing current volume state
	DiskState string `json:"disk_state,omitempty"`
	Opened    bool   `json:"opened,omitempty"`
}

// StorageResource is a struct which contains the storage-volumes for a storage-resource
type StorageResource struct {
	StorageVolumes []StorageVolume `json:"storage_volumes,omitempty"`
}

// StorageVolume is a struct to store standard poperties of a Volume
type StorageVolume struct {
	VolumeNumber int32 `json:"volume_number,omitempty"`
	// block device path
	DevicePath       string `json:"device_path,omitempty"`
	AllocatedSizeKib int64  `json:"allocated_size_kib,omitempty"`
	UsableSizeKib    int64  `json:"usable_size_kib,omitempty"`
	// String describing current volume state
	DiskState string `json:"disk_state,omitempty"`
}

type NvmeResource struct {
	NvmeVolumes []NvmeVolume `json:"nvme_volumes,omitempty"`
}

type NvmeVolume struct {
	VolumeNumber int32 `json:"volume_number,omitempty"`
	// block device path
	DevicePath string `json:"device_path,omitempty"`
	// block device used by nvme
	BackingDevice    string `json:"backing_device,omitempty"`
	AllocatedSizeKib int64  `json:"allocated_size_kib,omitempty"`
	UsableSizeKib    int64  `json:"usable_size_kib,omitempty"`
	// String describing current volume state
	DiskState string `json:"disk_state,omitempty"`
}

// ResourceState is a struct for getting the status of a resource
type ResourceState struct {
	InUse bool `json:"in_use,omitempty"`
}

// Volume is a struct which holds the information about a linstor-volume
type Volume struct {
	VolumeNumber     int32        `json:"volume_number,omitempty"`
	StoragePoolName  string       `json:"storage_pool_name,omitempty"`
	ProviderKind     ProviderKind `json:"provider_kind,omitempty"`
	DevicePath       string       `json:"device_path,omitempty"`
	AllocatedSizeKib int64        `json:"allocated_size_kib,omitempty"`
	UsableSizeKib    int64        `json:"usable_size_kib,omitempty"`
	// A string to string property map.
	Props         map[string]string `json:"props,omitempty"`
	Flags         []string          `json:"flags,omitempty"`
	State         VolumeState       `json:"state,omitempty"`
	LayerDataList []VolumeLayer     `json:"layer_data_list,omitempty"`
	// unique object id
	Uuid    string      `json:"uuid,omitempty"`
	Reports []ApiCallRc `json:"reports,omitempty"`
}

// VolumeLayer is a struct for storing the layer-properties of a linstor-volume
type VolumeLayer struct {
	Type devicelayerkind.DeviceLayerKind                                                         `json:"type,omitempty"`
	Data OneOfDrbdVolumeLuksVolumeStorageVolumeNvmeVolumeWritecacheVolumeCacheVolumeBCacheVolume `json:"data,omitempty"`
}

// VolumeState is a struct which contains the disk-state for volume
type VolumeState struct {
	DiskState string `json:"disk_state,omitempty"`
}

// AutoPlaceRequest is a struct to store the paramters for the linstor auto-place command
type AutoPlaceRequest struct {
	DisklessOnRemaining bool                              `json:"diskless_on_remaining,omitempty"`
	SelectFilter        AutoSelectFilter                  `json:"select_filter,omitempty"`
	LayerList           []devicelayerkind.DeviceLayerKind `json:"layer_list,omitempty"`
}

// AutoSelectFilter is a struct used to have information about the auto-select function
type AutoSelectFilter struct {
	PlaceCount              int32    `json:"place_count,omitempty"`
	AdditionalPlaceCount    int32    `json:"additional_place_count,omitempty"`
	NodeNameList            []string `json:"node_name_list,omitempty"`
	StoragePool             string   `json:"storage_pool,omitempty"`
	StoragePoolList         []string `json:"storage_pool_list,omitempty"`
	StoragePoolDisklessList []string `json:"storage_pool_diskless_list,omitempty"`
	NotPlaceWithRsc         []string `json:"not_place_with_rsc,omitempty"`
	NotPlaceWithRscRegex    string   `json:"not_place_with_rsc_regex,omitempty"`
	ReplicasOnSame          []string `json:"replicas_on_same,omitempty"`
	ReplicasOnDifferent     []string `json:"replicas_on_different,omitempty"`
	LayerStack              []string `json:"layer_stack,omitempty"`
	ProviderList            []string `json:"provider_list,omitempty"`
	DisklessOnRemaining     bool     `json:"diskless_on_remaining,omitempty"`
	DisklessType            string   `json:"diskless_type,omitempty"`
	Overprovision           *float64 `json:"overprovision,omitempty"`
}

// ResourceConnection is a struct which holds information about a connection between to nodes
type ResourceConnection struct {
	// source node of the connection
	NodeA string `json:"node_a,omitempty"`
	// target node of the connection
	NodeB string `json:"node_b,omitempty"`
	// A string to string property map.
	Props map[string]string `json:"props,omitempty"`
	Flags []string          `json:"flags,omitempty"`
	Port  int32             `json:"port,omitempty"`
}

// Snapshot is a struct for information about a snapshot
type Snapshot struct {
	Name         string   `json:"name,omitempty"`
	ResourceName string   `json:"resource_name,omitempty"`
	Nodes        []string `json:"nodes,omitempty"`
	// A string to string property map.
	Props             map[string]string          `json:"props,omitempty"`
	Flags             []string                   `json:"flags,omitempty"`
	VolumeDefinitions []SnapshotVolumeDefinition `json:"volume_definitions,omitempty"`
	// unique object id
	Uuid      string         `json:"uuid,omitempty"`
	Snapshots []SnapshotNode `json:"snapshots,omitempty"`
}

// SnapshotNode Actual snapshot data from a node
type SnapshotNode struct {
	// Snapshot name this snapshots belongs to
	SnapshotName string `json:"snapshot_name,omitempty"`
	// Node name where this snapshot was taken
	NodeName string `json:"node_name,omitempty"`
	// milliseconds since unix epoch in UTC
	CreateTimestamp *TimeStampMs `json:"create_timestamp,omitempty"`
	Flags           []string     `json:"flags,omitempty"`
	// unique object id
	Uuid string `json:"uuid,omitempty"`
}

// SnapshotShipping struct for SnapshotShipping
type SnapshotShipping struct {
	// Node where to ship the snapshot from
	FromNode string `json:"from_node"`
	// NetInterface of the source node
	FromNic string `json:"from_nic,omitempty"`
	// Node where to ship the snapshot
	ToNode string `json:"to_node"`
	// NetInterface of the destination node
	ToNic string `json:"to_nic,omitempty"`
}

// SnapshotShippingStatus struct for SnapshotShippingStatus
type SnapshotShippingStatus struct {
	Snapshot     Snapshot                              `json:"snapshot,omitempty"`
	FromNodeName string                                `json:"from_node_name,omitempty"`
	ToNodeName   string                                `json:"to_node_name,omitempty"`
	Status       snapshotshipstatus.SnapshotShipStatus `json:"status,omitempty"`
}

// SnapshotVolumeDefinition is a struct to store the properties of a volume from a snapshot
type SnapshotVolumeDefinition struct {
	VolumeNumber int32 `json:"volume_number,omitempty"`
	// Volume size in KiB
	SizeKib uint64 `json:"size_kib,omitempty"`
}

// SnapshotRestore is a struct used to hold the information about where a Snapshot has to be restored
type SnapshotRestore struct {
	// Resource where to restore the snapshot
	ToResource string `json:"to_resource"`
	// List of nodes where to place the restored snapshot
	Nodes []string `json:"nodes,omitempty"`
}

type DrbdProxyModify struct {
	// Compression type used by the proxy.
	CompressionType string `json:"compression_type,omitempty"`
	// A string to string property map.
	CompressionProps map[string]string `json:"compression_props,omitempty"`
	GenericPropsModify
}

// Candidate struct for Candidate
type Candidate struct {
	StoragePool string `json:"storage_pool,omitempty"`
	// maximum size in KiB
	MaxVolumeSizeKib int64    `json:"max_volume_size_kib,omitempty"`
	NodeNames        []string `json:"node_names,omitempty"`
	AllThin          bool     `json:"all_thin,omitempty"`
}

// MaxVolumeSizes struct for MaxVolumeSizes
type MaxVolumeSizes struct {
	Candidates                      []Candidate `json:"candidates,omitempty"`
	DefaultMaxOversubscriptionRatio float64     `json:"default_max_oversubscription_ratio,omitempty"`
}

type ResourceMakeAvailable struct {
	LayerList []devicelayerkind.DeviceLayerKind `json:"layer_list,omitempty"`
	// if true resource will be created as diskful even if diskless would be possible
	Diskful bool `json:"diskful,omitempty"`
}

type ToggleDiskDiskfulProps struct {
	LayerList []devicelayerkind.DeviceLayerKind `json:"layer_list,omitempty"`
}

// custom code

// ResourceProvider acts as an abstraction for an ResourceService. It can be
// swapped out for another ResourceService implementation, for example for
// testing.
type ResourceProvider interface {
	// GetResourceView returns all resources in the cluster. Filters can be set via ListOpts.
	GetResourceView(ctx context.Context, opts ...*ListOpts) ([]ResourceWithVolumes, error)
	// GetAll returns all resources for a resource-definition
	GetAll(ctx context.Context, resName string, opts ...*ListOpts) ([]Resource, error)
	// Get returns information about a resource on a specific node
	Get(ctx context.Context, resName, nodeName string, opts ...*ListOpts) (Resource, error)
	// Create is used to create a resource on a node
	Create(ctx context.Context, res ResourceCreate) error
	// Modify gives the ability to modify a resource on a node
	Modify(ctx context.Context, resName, nodeName string, props GenericPropsModify) error
	// Delete deletes a resource on a specific node
	Delete(ctx context.Context, resName, nodeName string) error
	// GetVolumes lists als volumes of a resource
	GetVolumes(ctx context.Context, resName, nodeName string, opts ...*ListOpts) ([]Volume, error)
	// GetVolume returns information about a specific volume defined by it resource,node and volume-number
	GetVolume(ctx context.Context, resName, nodeName string, volNr int, opts ...*ListOpts) (Volume, error)
	// ModifyVolume modifies an existing volume with the given props
	ModifyVolume(ctx context.Context, resName, nodeName string, volNr int, props GenericPropsModify) error
	// Diskless toggles a resource on a node to diskless - the parameter disklesspool can be set if its needed
	Diskless(ctx context.Context, resName, nodeName, disklessPoolName string) error
	// Diskful toggles a resource to diskful - the parameter storagepool can be set if its needed
	Diskful(ctx context.Context, resName, nodeName, storagePoolName string, props *ToggleDiskDiskfulProps) error
	// Migrate mirgates a resource from one node to another node
	Migrate(ctx context.Context, resName, fromNodeName, toNodeName, storagePoolName string) error
	// Autoplace places a resource on your nodes autmatically
	Autoplace(ctx context.Context, resName string, apr AutoPlaceRequest) error
	// GetConnections lists all resource connections if no node-names are given- if two node-names are given it shows the connection between them
	GetConnections(ctx context.Context, resName, nodeAName, nodeBName string, opts ...*ListOpts) ([]ResourceConnection, error)
	// ModifyConnection allows to modify the connection between two nodes
	ModifyConnection(ctx context.Context, resName, nodeAName, nodeBName string, props GenericPropsModify) error
	// GetSnapshots lists all snapshots of a resource
	GetSnapshots(ctx context.Context, resName string, opts ...*ListOpts) ([]Snapshot, error)
	// GetSnapshotView gets information about all snapshots
	GetSnapshotView(ctx context.Context, opts ...*ListOpts) ([]Snapshot, error)
	// GetSnapshot returns information about a specific Snapshot by its name
	GetSnapshot(ctx context.Context, resName, snapName string, opts ...*ListOpts) (Snapshot, error)
	// CreateSnapshot creates a snapshot of a resource
	CreateSnapshot(ctx context.Context, snapshot Snapshot) error
	// DeleteSnapshot deletes a snapshot by its name. Specify nodes to only delete snapshots on specific nodes.
	DeleteSnapshot(ctx context.Context, resName, snapName string, nodes ...string) error
	// RestoreSnapshot restores a snapshot on a resource
	RestoreSnapshot(ctx context.Context, origResName, snapName string, snapRestoreConf SnapshotRestore) error
	// RestoreVolumeDefinitionSnapshot restores a volume-definition-snapshot on a resource
	RestoreVolumeDefinitionSnapshot(ctx context.Context, origResName, snapName string, snapRestoreConf SnapshotRestore) error
	// RollbackSnapshot rolls back a snapshot from a specific resource
	RollbackSnapshot(ctx context.Context, resName, snapName string) error
	// EnableSnapshotShipping enables snapshot shipping for a resource
	EnableSnapshotShipping(ctx context.Context, resName string, ship SnapshotShipping) error
	// ModifyDRBDProxy is used to modify drbd-proxy properties
	ModifyDRBDProxy(ctx context.Context, resName string, props DrbdProxyModify) error
	// EnableDRBDProxy is used to enable drbd-proxy with the rest-api call from the function enableDisableDRBDProxy
	EnableDRBDProxy(ctx context.Context, resName, nodeAName, nodeBName string) error
	// DisableDRBDProxy is used to disable drbd-proxy with the rest-api call from the function enableDisableDRBDProxy
	DisableDRBDProxy(ctx context.Context, resName, nodeAName, nodeBName string) error
	// QueryMaxVolumeSize finds the maximum size of a volume for a given filter
	QueryMaxVolumeSize(ctx context.Context, filter AutoSelectFilter) (MaxVolumeSizes, error)
	// GetSnapshotShippings gets a view of all snapshot shippings
	GetSnapshotShippings(ctx context.Context, opts ...*ListOpts) ([]SnapshotShippingStatus, error)
	// GetPropsInfos gets meta information about the properties that can be
	// set on a resource.
	GetPropsInfos(ctx context.Context, resName string, opts ...*ListOpts) ([]PropsInfo, error)
	// GetVolumeDefinitionPropsInfos gets meta information about the
	// properties that can be set on a volume definition.
	GetVolumeDefinitionPropsInfos(ctx context.Context, resName string, opts ...*ListOpts) ([]PropsInfo, error)
	// GetVolumePropsInfos gets meta information about the properties that
	// can be set on a volume.
	GetVolumePropsInfos(ctx context.Context, resName, nodeName string, opts ...*ListOpts) ([]PropsInfo, error)
	// GetConnectionPropsInfos gets meta information about the properties
	// that can be set on a connection.
	GetConnectionPropsInfos(ctx context.Context, resName string, opts ...*ListOpts) ([]PropsInfo, error)
	// Activate starts an inactive resource on a given node.
	Activate(ctx context.Context, resName string, nodeName string) error
	// Deactivate stops an active resource on given node.
	Deactivate(ctx context.Context, resName string, nodeName string) error
	// MakeAvailable adds a resource on a node if not already deployed.
	// To use a specific storage pool add the StorPoolName property and use
	// the storage pool name as value. If the StorPoolName property is not
	// set, a storage pool will be chosen automatically using the
	// auto-placer.
	// To create a diskless resource you have to set the "DISKLESS" flag in
	// the flags list.
	MakeAvailable(ctx context.Context, resName, nodeName string, makeAvailable ResourceMakeAvailable) error
}

var _ ResourceProvider = &ResourceService{}

// volumeLayerIn is a struct for volume-layers
type volumeLayerIn struct {
	Type devicelayerkind.DeviceLayerKind `json:"type,omitempty"`
	Data json.RawMessage                 `json:"data,omitempty"`
}

// UnmarshalJSON fulfills the unmarshal interface for the VolumeLayer type
func (v *VolumeLayer) UnmarshalJSON(b []byte) error {
	var vIn volumeLayerIn
	if err := json.Unmarshal(b, &vIn); err != nil {
		return err
	}

	v.Type = vIn.Type
	switch v.Type {
	case devicelayerkind.Drbd:
		dst := new(DrbdVolume)
		if vIn.Data != nil {
			if err := json.Unmarshal(vIn.Data, &dst); err != nil {
				return err
			}
		}
		v.Data = dst
	case devicelayerkind.Luks:
		dst := new(LuksVolume)
		if vIn.Data != nil {
			if err := json.Unmarshal(vIn.Data, &dst); err != nil {
				return err
			}
		}
		v.Data = dst
	case devicelayerkind.Storage:
		dst := new(StorageVolume)
		if vIn.Data != nil {
			if err := json.Unmarshal(vIn.Data, &dst); err != nil {
				return err
			}
		}
		v.Data = dst
	case devicelayerkind.Nvme:
		dst := new(NvmeVolume)
		if vIn.Data != nil {
			if err := json.Unmarshal(vIn.Data, &dst); err != nil {
				return err
			}
		}
		v.Data = dst
	case devicelayerkind.Writecache:
		dst := new(WritecacheVolume)
		if vIn.Data != nil {
			if err := json.Unmarshal(vIn.Data, &dst); err != nil {
				return err
			}
		}
		v.Data = dst
	case devicelayerkind.Cache:
	case devicelayerkind.Openflex:
	case devicelayerkind.Exos:
	default:
		return fmt.Errorf("'%+v' is not a valid type to Unmarshal", v.Type)
	}

	return nil
}

// OneOfDrbdVolumeLuksVolumeStorageVolumeNvmeVolumeWritecacheVolumeCacheVolume is used to prevent that other types than drbd- luks- and storage-volume are used for a VolumeLayer
type OneOfDrbdVolumeLuksVolumeStorageVolumeNvmeVolumeWritecacheVolumeCacheVolumeBCacheVolume interface {
	isOneOfDrbdVolumeLuksVolumeStorageVolumeNvmeVolumeWritecacheVolumeCacheVolumeBCacheVolume()
}

// Functions which are used if type is a correct VolumeLayer
func (d *DrbdVolume) isOneOfDrbdVolumeLuksVolumeStorageVolumeNvmeVolumeWritecacheVolumeCacheVolumeBCacheVolume() {
}
func (d *LuksVolume) isOneOfDrbdVolumeLuksVolumeStorageVolumeNvmeVolumeWritecacheVolumeCacheVolumeBCacheVolume() {
}
func (d *StorageVolume) isOneOfDrbdVolumeLuksVolumeStorageVolumeNvmeVolumeWritecacheVolumeCacheVolumeBCacheVolume() {
}
func (d *NvmeVolume) isOneOfDrbdVolumeLuksVolumeStorageVolumeNvmeVolumeWritecacheVolumeCacheVolumeBCacheVolume() {
}
func (d *WritecacheVolume) isOneOfDrbdVolumeLuksVolumeStorageVolumeNvmeVolumeWritecacheVolumeCacheVolumeBCacheVolume() {
}
func (d *CacheVolume) isOneOfDrbdVolumeLuksVolumeStorageVolumeNvmeVolumeWritecacheVolumeCacheVolumeBCacheVolume() {
}

// GetResourceView returns all resources in the cluster. Filters can be set via ListOpts.
func (n *ResourceService) GetResourceView(ctx context.Context, opts ...*ListOpts) ([]ResourceWithVolumes, error) {
	var reses []ResourceWithVolumes
	_, err := n.client.doGET(ctx, "/v1/view/resources", &reses, opts...)
	return reses, err
}

// GetAll returns all resources for a resource-definition
func (n *ResourceService) GetAll(ctx context.Context, resName string, opts ...*ListOpts) ([]Resource, error) {
	var reses []Resource
	_, err := n.client.doGET(ctx, "/v1/resource-definitions/"+resName+"/resources", &reses, opts...)
	return reses, err
}

// Get returns information about a resource on a specific node
func (n *ResourceService) Get(ctx context.Context, resName, nodeName string, opts ...*ListOpts) (Resource, error) {
	var res Resource
	_, err := n.client.doGET(ctx, "/v1/resource-definitions/"+resName+"/resources/"+nodeName, &res, opts...)
	return res, err
}

// Create is used to create a resource on a node
func (n *ResourceService) Create(ctx context.Context, res ResourceCreate) error {
	_, err := n.client.doPOST(ctx, "/v1/resource-definitions/"+res.Resource.Name+"/resources/"+res.Resource.NodeName, res)
	return err
}

// Modify gives the ability to modify a resource on a node
func (n *ResourceService) Modify(ctx context.Context, resName, nodeName string, props GenericPropsModify) error {
	_, err := n.client.doPUT(ctx, "/v1/resource-definitions/"+resName+"/resources/"+nodeName, props)
	return err
}

// Delete deletes a resource on a specific node
func (n *ResourceService) Delete(ctx context.Context, resName, nodeName string) error {
	_, err := n.client.doDELETE(ctx, "/v1/resource-definitions/"+resName+"/resources/"+nodeName, nil)
	return err
}

func (n *ResourceService) Activate(ctx context.Context, resName, nodeName string) error {
	_, err := n.client.doPOST(ctx, "/v1/resource-definitions/"+resName+"/resources/"+nodeName+"/activate", nil)
	return err
}

func (n *ResourceService) Deactivate(ctx context.Context, resName, nodeName string) error {
	_, err := n.client.doPOST(ctx, "/v1/resource-definitions/"+resName+"/resources/"+nodeName+"/deactivate", nil)
	return err
}

// GetVolumes lists als volumes of a resource
func (n *ResourceService) GetVolumes(ctx context.Context, resName, nodeName string, opts ...*ListOpts) ([]Volume, error) {
	var vols []Volume

	_, err := n.client.doGET(ctx, "/v1/resource-definitions/"+resName+"/resources/"+nodeName+"/volumes", &vols, opts...)
	return vols, err
}

// GetVolume returns information about a specific volume defined by it resource,node and volume-number
func (n *ResourceService) GetVolume(ctx context.Context, resName, nodeName string, volNr int, opts ...*ListOpts) (Volume, error) {
	var vol Volume

	_, err := n.client.doGET(ctx, "/v1/resource-definitions/"+resName+"/resources/"+nodeName+"/volumes/"+strconv.Itoa(volNr), &vol, opts...)
	return vol, err
}

// ModifyVolume modifies an existing volume with the given props
func (n *ResourceService) ModifyVolume(ctx context.Context, resName, nodeName string, volNr int, props GenericPropsModify) error {
	u := fmt.Sprintf("/v1/resource-definitions/%s/resources/%s/volumes/%d", resName, nodeName, volNr)
	_, err := n.client.doPUT(ctx, u, props)
	return err
}

// Diskless toggles a resource on a node to diskless - the parameter disklesspool can be set if its needed
func (n *ResourceService) Diskless(ctx context.Context, resName, nodeName, disklessPoolName string) error {
	u := "/v1/resource-definitions/" + resName + "/resources/" + nodeName + "/toggle-disk/diskless"
	if disklessPoolName != "" {
		u += "/" + disklessPoolName
	}

	_, err := n.client.doPUT(ctx, u, nil)
	return err
}

// Diskful toggles a resource to diskful - the parameter storagepool can be set if its needed
func (n *ResourceService) Diskful(ctx context.Context, resName, nodeName, storagePoolName string, props *ToggleDiskDiskfulProps) error {
	u := "/v1/resource-definitions/" + resName + "/resources/" + nodeName + "/toggle-disk/diskful"
	if storagePoolName != "" {
		u += "/" + storagePoolName
	}
	_, err := n.client.doPUT(ctx, u, props)
	return err
}

// Migrate mirgates a resource from one node to another node
func (n *ResourceService) Migrate(ctx context.Context, resName, fromNodeName, toNodeName, storagePoolName string) error {
	u := "/v1/resource-definitions/" + resName + "/resources/" + toNodeName + "/migrate-disk/" + fromNodeName
	if storagePoolName != "" {
		u += "/" + storagePoolName
	}
	_, err := n.client.doPUT(ctx, u, nil)
	return err
}

// Autoplace places a resource on your nodes autmatically
func (n *ResourceService) Autoplace(ctx context.Context, resName string, apr AutoPlaceRequest) error {
	_, err := n.client.doPOST(ctx, "/v1/resource-definitions/"+resName+"/autoplace", apr)
	return err
}

// GetConnections lists all resource connections if no node-names are given- if two node-names are given it shows the connection between them
func (n *ResourceService) GetConnections(ctx context.Context, resName, nodeAName, nodeBName string, opts ...*ListOpts) ([]ResourceConnection, error) {
	var resConns []ResourceConnection

	u := "/v1/resource-definitions/" + resName + "/resources-connections"
	if nodeAName != "" && nodeBName != "" {
		u += fmt.Sprintf("/%s/%s", nodeAName, nodeBName)
	}

	_, err := n.client.doGET(ctx, u, &resConns, opts...)
	return resConns, err
}

// ModifyConnection allows to modify the connection between two nodes
func (n *ResourceService) ModifyConnection(ctx context.Context, resName, nodeAName, nodeBName string, props GenericPropsModify) error {
	u := fmt.Sprintf("/v1/resource-definitions/%s/resource-connections/%s/%s", resName, nodeAName, nodeBName)
	_, err := n.client.doPUT(ctx, u, props)
	return err
}

// GetSnapshots lists all snapshots of a resource
func (n *ResourceService) GetSnapshots(ctx context.Context, resName string, opts ...*ListOpts) ([]Snapshot, error) {
	var snaps []Snapshot

	_, err := n.client.doGET(ctx, "/v1/resource-definitions/"+resName+"/snapshots", &snaps, opts...)
	return snaps, err
}

// GetSnapshotView gets information about all snapshots
func (r *ResourceService) GetSnapshotView(ctx context.Context, opts ...*ListOpts) ([]Snapshot, error) {
	var snaps []Snapshot
	_, err := r.client.doGET(ctx, "/v1/view/snapshots", &snaps, opts...)
	return snaps, err
}

// GetSnapshot returns information about a specific Snapshot by its name
func (n *ResourceService) GetSnapshot(ctx context.Context, resName, snapName string, opts ...*ListOpts) (Snapshot, error) {
	var snap Snapshot

	_, err := n.client.doGET(ctx, "/v1/resource-definitions/"+resName+"/snapshots/"+snapName, &snap, opts...)
	return snap, err
}

// CreateSnapshot creates a snapshot of a resource
func (n *ResourceService) CreateSnapshot(ctx context.Context, snapshot Snapshot) error {
	_, err := n.client.doPOST(ctx, "/v1/resource-definitions/"+snapshot.ResourceName+"/snapshots", snapshot)
	return err
}

// DeleteSnapshot deletes a snapshot by its name. Specify nodes to only delete snapshots on specific nodes.
func (n *ResourceService) DeleteSnapshot(ctx context.Context, resName, snapName string, nodes ...string) error {
	vals, err := query.Values(struct {
		Nodes []string `url:"nodes"`
	}{Nodes: nodes})
	if err != nil {
		return fmt.Errorf("failed to encode node names: %w", err)
	}

	_, err = n.client.doDELETE(ctx, "/v1/resource-definitions/"+resName+"/snapshots/"+snapName+"?"+vals.Encode(), nil)
	return err
}

// RestoreSnapshot restores a snapshot on a resource
func (n *ResourceService) RestoreSnapshot(ctx context.Context, origResName, snapName string, snapRestoreConf SnapshotRestore) error {
	_, err := n.client.doPOST(ctx, "/v1/resource-definitions/"+origResName+"/snapshot-restore-resource/"+snapName, snapRestoreConf)
	return err
}

// RestoreVolumeDefinitionSnapshot restores a volume-definition-snapshot on a resource
func (n *ResourceService) RestoreVolumeDefinitionSnapshot(ctx context.Context, origResName, snapName string, snapRestoreConf SnapshotRestore) error {
	_, err := n.client.doPOST(ctx, "/v1/resource-definitions/"+origResName+"/snapshot-restore-volume-definition/"+snapName, snapRestoreConf)
	return err
}

// RollbackSnapshot rolls back a snapshot from a specific resource
func (n *ResourceService) RollbackSnapshot(ctx context.Context, resName, snapName string) error {
	_, err := n.client.doPOST(ctx, "/v1/resource-definitions/"+resName+"/snapshot-rollback/"+snapName, nil)
	return err
}

// EnableSnapshotShipping enables snapshot shipping for a resource
func (n *ResourceService) EnableSnapshotShipping(ctx context.Context, resName string, ship SnapshotShipping) error {
	_, err := n.client.doPOST(ctx, "/v1/resource-definitions/"+resName+"/snapshot-shipping", ship)
	return err
}

// ModifyDRBDProxy is used to modify drbd-proxy properties
func (n *ResourceService) ModifyDRBDProxy(ctx context.Context, resName string, props DrbdProxyModify) error {
	_, err := n.client.doPUT(ctx, "/v1/resource-definitions/"+resName+"/drbd-proxy", props)
	return err
}

// enableDisableDRBDProxy enables or disables drbd-proxy between two nodes
func (n *ResourceService) enableDisableDRBDProxy(ctx context.Context, what, resName, nodeAName, nodeBName string) error {
	u := fmt.Sprintf("/v1/resource-definitions/%s/drbd-proxy/%s/%s/%s", resName, what, nodeAName, nodeBName)
	_, err := n.client.doPOST(ctx, u, nil)
	return err
}

// EnableDRBDProxy is used to enable drbd-proxy with the rest-api call from the function enableDisableDRBDProxy
func (n *ResourceService) EnableDRBDProxy(ctx context.Context, resName, nodeAName, nodeBName string) error {
	return n.enableDisableDRBDProxy(ctx, "enable", resName, nodeAName, nodeBName)
}

// DisableDRBDProxy is used to disable drbd-proxy with the rest-api call from the function enableDisableDRBDProxy
func (n *ResourceService) DisableDRBDProxy(ctx context.Context, resName, nodeAName, nodeBName string) error {
	return n.enableDisableDRBDProxy(ctx, "disable", resName, nodeAName, nodeBName)
}

// QueryMaxVolumeSize finds the maximum size of a volume for a given filter
func (n *ResourceService) QueryMaxVolumeSize(ctx context.Context, filter AutoSelectFilter) (MaxVolumeSizes, error) {
	var sizes MaxVolumeSizes
	_, err := n.client.doOPTIONS(ctx, "/v1/query-max-volume-size", &sizes, filter)
	return sizes, err
}

// GetSnapshotShippings gets a view of all snapshot shippings
func (n *ResourceService) GetSnapshotShippings(ctx context.Context, opts ...*ListOpts) ([]SnapshotShippingStatus, error) {
	var shippings []SnapshotShippingStatus
	_, err := n.client.doGET(ctx, "/v1/view/snapshot-shippings", &shippings, opts...)
	return shippings, err
}

// GetPropsInfos gets meta information about the properties that can be set on
// a resource.
func (n *ResourceService) GetPropsInfos(ctx context.Context, resName string, opts ...*ListOpts) ([]PropsInfo, error) {
	var infos []PropsInfo
	_, err := n.client.doGET(ctx, "/v1/resource-definitions/"+resName+"/resources/properties/info", &infos, opts...)
	return infos, err
}

// GetVolumeDefinitionPropsInfos gets meta information about the properties
// that can be set on a volume definition.
func (n *ResourceService) GetVolumeDefinitionPropsInfos(ctx context.Context, resName string, opts ...*ListOpts) ([]PropsInfo, error) {
	var infos []PropsInfo
	_, err := n.client.doGET(ctx, "/v1/resource-definitions/"+resName+"/volume-definitions/properties/info", &infos, opts...)
	return infos, err
}

// GetVolumePropsInfos gets meta information about the properties that can be
// set on a volume.
func (n *ResourceService) GetVolumePropsInfos(ctx context.Context, resName, nodeName string, opts ...*ListOpts) ([]PropsInfo, error) {
	var infos []PropsInfo
	_, err := n.client.doGET(ctx, "/v1/resource-definitions/"+resName+"/resources/"+nodeName+"/volumes/properties/info", &infos, opts...)
	return infos, err
}

// GetConnectionPropsInfos gets meta information about the properties that can
// be set on a connection.
func (n *ResourceService) GetConnectionPropsInfos(ctx context.Context, resName string, opts ...*ListOpts) ([]PropsInfo, error) {
	var infos []PropsInfo
	_, err := n.client.doGET(ctx, "/v1/resource-definitions/"+resName+"/resource-connections/properties/info", &infos, opts...)
	return infos, err
}

// MakeAvailable adds a resource on a node if not already deployed.
// To use a specific storage pool add the StorPoolName property and use the
// storage pool name as value. If the StorPoolName property is not set, a
// storage pool will be chosen automatically using the auto-placer.
// To create a diskless resource you have to set the "DISKLESS" flag in the
// flags list.
func (n *ResourceService) MakeAvailable(ctx context.Context, resName, nodeName string, makeAvailable ResourceMakeAvailable) error {
	u := fmt.Sprintf("/v1/resource-definitions/%s/resources/%s/make-available",
		resName, nodeName)
	_, err := n.client.doPOST(ctx, u, makeAvailable)
	return err
}
