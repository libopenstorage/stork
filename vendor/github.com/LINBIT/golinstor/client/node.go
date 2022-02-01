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

	"github.com/LINBIT/golinstor/devicelayerkind"
)

// copy & paste from generated code

// Node represents a node in LINSTOR
type Node struct {
	Name  string   `json:"name"`
	Type  string   `json:"type"`
	Flags []string `json:"flags,omitempty"`
	// A string to string property map.
	Props         map[string]string `json:"props,omitempty"`
	NetInterfaces []NetInterface    `json:"net_interfaces,omitempty"`
	// Enum describing the current connection status.
	ConnectionStatus string `json:"connection_status,omitempty"`
	// unique object id
	Uuid                 string                                       `json:"uuid,omitempty"`
	StorageProviders     []ProviderKind                               `json:"storage_providers,omitempty"`
	ResourceLayers       []devicelayerkind.DeviceLayerKind            `json:"resource_layers,omitempty"`
	UnsupportedProviders map[ProviderKind][]string                    `json:"unsupported_providers,omitempty"`
	UnsupportedLayers    map[devicelayerkind.DeviceLayerKind][]string `json:"unsupported_layers,omitempty"`
	// milliseconds since unix epoch in UTC
	EvictionTimestamp *TimeStampMs `json:"eviction_timestamp,omitempty"`
}

type NodeModify struct {
	NodeType string `json:"node_type,omitempty"`
	// A string to string property map.
	GenericPropsModify
}

type NodeRestore struct {
	DeleteResources *bool `json:"delete_resources,omitempty"`
	DeleteSnapshots *bool `json:"delete_snapshots,omitempty"`
}

// NetInterface represents a node's network interface.
type NetInterface struct {
	Name                    string `json:"name"`
	Address                 string `json:"address"`
	SatellitePort           int32  `json:"satellite_port,omitempty"`
	SatelliteEncryptionType string `json:"satellite_encryption_type,omitempty"`
	// Defines if this netinterface should be used for the satellite connection
	IsActive bool `json:"is_active,omitempty"`
	// unique object id
	Uuid string `json:"uuid,omitempty"`
}

// StoragePool represents a nodes storage pool as defined in LINSTOR.
type StoragePool struct {
	StoragePoolName string       `json:"storage_pool_name"`
	NodeName        string       `json:"node_name,omitempty"`
	ProviderKind    ProviderKind `json:"provider_kind"`
	// A string to string property map.
	Props map[string]string `json:"props,omitempty"`
	// read only map of static storage pool traits
	StaticTraits map[string]string `json:"static_traits,omitempty"`
	// Kibi - read only
	FreeCapacity int64 `json:"free_capacity,omitempty"`
	// Kibi - read only
	TotalCapacity int64 `json:"total_capacity,omitempty"`
	// read only
	FreeSpaceMgrName string `json:"free_space_mgr_name,omitempty"`
	// unique object id
	Uuid string `json:"uuid,omitempty"`
	// Currently known report messages for this storage pool
	Reports []ApiCallRc `json:"reports,omitempty"`
	// true if the storage pool supports snapshots. false otherwise
	SupportsSnapshots bool `json:"supports_snapshots,omitempty"`
	// name of the shared space or null if none given
	SharedSpace string `json:"shared_space,omitempty"`
	// true if a shared storage pool uses linstor-external locking, like cLVM
	ExternalLocking bool `json:"external_locking,omitempty"`
}

// ProviderKind is a type that represents various types of storage.
type ProviderKind string

// List of ProviderKind
const (
	DISKLESS        ProviderKind = "DISKLESS"
	LVM             ProviderKind = "LVM"
	LVM_THIN        ProviderKind = "LVM_THIN"
	ZFS             ProviderKind = "ZFS"
	ZFS_THIN        ProviderKind = "ZFS_THIN"
	OPENFLEX_TARGET ProviderKind = "OPENFLEX_TARGET"
	FILE            ProviderKind = "FILE"
	FILE_THIN       ProviderKind = "FILE_THIN"
	SPDK            ProviderKind = "SPDK"
)

// custom code

// NodeProvider acts as an abstraction for a NodeService. It can be swapped out
// for another NodeService implementation, for example for testing.
type NodeProvider interface {
	// GetAll gets information for all registered nodes.
	GetAll(ctx context.Context, opts ...*ListOpts) ([]Node, error)
	// Get gets information for a particular node.
	Get(ctx context.Context, nodeName string, opts ...*ListOpts) (Node, error)
	// Create creates a new node object.
	Create(ctx context.Context, node Node) error
	// Modify modifies the given node and sets/deletes the given properties.
	Modify(ctx context.Context, nodeName string, props NodeModify) error
	// Delete deletes the given node.
	Delete(ctx context.Context, nodeName string) error
	// Lost marks the given node as lost to delete an unrecoverable node.
	Lost(ctx context.Context, nodeName string) error
	// Reconnect reconnects a node to the controller.
	Reconnect(ctx context.Context, nodeName string) error
	// GetNetInterfaces gets information about all network interfaces of a given node.
	GetNetInterfaces(ctx context.Context, nodeName string, opts ...*ListOpts) ([]NetInterface, error)
	// GetNetInterface gets information about a particular network interface on a given node.
	GetNetInterface(ctx context.Context, nodeName, nifName string, opts ...*ListOpts) (NetInterface, error)
	// CreateNetInterface creates the given network interface on a given node.
	CreateNetInterface(ctx context.Context, nodeName string, nif NetInterface) error
	// ModifyNetInterface modifies the given network interface on a given node.
	ModifyNetInterface(ctx context.Context, nodeName, nifName string, nif NetInterface) error
	// DeleteNetinterface deletes the given network interface on a given node.
	DeleteNetinterface(ctx context.Context, nodeName, nifName string) error
	// GetStoragePoolView gets information about all storage pools in the cluster.
	GetStoragePoolView(ctx context.Context, opts ...*ListOpts) ([]StoragePool, error)
	// GetStoragePools gets information about all storage pools on a given node.
	GetStoragePools(ctx context.Context, nodeName string, opts ...*ListOpts) ([]StoragePool, error)
	// GetStoragePool gets information about a specific storage pool on a given node.
	GetStoragePool(ctx context.Context, nodeName, spName string, opts ...*ListOpts) (StoragePool, error)
	// CreateStoragePool creates a storage pool on a given node.
	CreateStoragePool(ctx context.Context, nodeName string, sp StoragePool) error
	// ModifyStoragePool modifies a storage pool on a given node.
	ModifyStoragePool(ctx context.Context, nodeName, spName string, sp StoragePool) error
	// DeleteStoragePool deletes a storage pool on a given node.
	DeleteStoragePool(ctx context.Context, nodeName, spName string) error
	// CreateDevicePool creates an LVM, LVM-thin or ZFS pool, optional VDO under it on a given node.
	CreateDevicePool(ctx context.Context, nodeName string, psc PhysicalStorageCreate) error
	// GetPhysicalStorage gets a grouped list of physical storage that can be turned into a LINSTOR storage-pool
	GetPhysicalStorage(ctx context.Context, opts ...*ListOpts) ([]PhysicalStorage, error)
	// GetStoragePoolPropsInfos gets meta information about the properties
	// that can be set on a storage pool on a particular node.
	GetStoragePoolPropsInfos(ctx context.Context, nodeName string, opts ...*ListOpts) ([]PropsInfo, error)
	// GetPropsInfos gets meta information about the properties that can be
	// set on a node.
	GetPropsInfos(ctx context.Context, opts ...*ListOpts) ([]PropsInfo, error)
	// Evict the given node, migrating resources to the remaining nodes, if possible.
	Evict(ctx context.Context, nodeName string) error
	// Restore an evicted node, optionally keeping existing resources.
	Restore(ctx context.Context, nodeName string, restore NodeRestore) error
}

// NodeService is the service that deals with node related tasks.
type NodeService struct {
	client *Client
}

var _ NodeProvider = &NodeService{}

// GetAll gets information for all registered nodes.
func (n *NodeService) GetAll(ctx context.Context, opts ...*ListOpts) ([]Node, error) {
	var nodes []Node
	_, err := n.client.doGET(ctx, "/v1/nodes", &nodes, opts...)
	return nodes, err
}

// Get gets information for a particular node.
func (n *NodeService) Get(ctx context.Context, nodeName string, opts ...*ListOpts) (Node, error) {
	var node Node
	_, err := n.client.doGET(ctx, "/v1/nodes/"+nodeName, &node, opts...)
	return node, err
}

// Create creates a new node object.
func (n *NodeService) Create(ctx context.Context, node Node) error {
	_, err := n.client.doPOST(ctx, "/v1/nodes", node)
	return err
}

// Modify modifies the given node and sets/deletes the given properties.
func (n *NodeService) Modify(ctx context.Context, nodeName string, props NodeModify) error {
	_, err := n.client.doPUT(ctx, "/v1/nodes/"+nodeName, props)
	return err
}

// Delete deletes the given node.
func (n *NodeService) Delete(ctx context.Context, nodeName string) error {
	_, err := n.client.doDELETE(ctx, "/v1/nodes/"+nodeName, nil)
	return err
}

// Lost marks the given node as lost to delete an unrecoverable node.
func (n *NodeService) Lost(ctx context.Context, nodeName string) error {
	_, err := n.client.doDELETE(ctx, "/v1/nodes/"+nodeName+"/lost", nil)
	return err
}

// Reconnect reconnects a node to the controller.
func (n *NodeService) Reconnect(ctx context.Context, nodeName string) error {
	_, err := n.client.doPUT(ctx, "/v1/nodes/"+nodeName+"/reconnect", nil)
	return err
}

// GetNetInterfaces gets information about all network interfaces of a given node.
func (n *NodeService) GetNetInterfaces(ctx context.Context, nodeName string, opts ...*ListOpts) ([]NetInterface, error) {
	var nifs []NetInterface
	_, err := n.client.doGET(ctx, "/v1/nodes/"+nodeName+"/net-interfaces", &nifs, opts...)
	return nifs, err
}

// GetNetInterface gets information about a particular network interface on a given node.
func (n *NodeService) GetNetInterface(ctx context.Context, nodeName, nifName string, opts ...*ListOpts) (NetInterface, error) {
	var nif NetInterface
	_, err := n.client.doGET(ctx, "/v1/nodes/"+nodeName+"/net-interfaces/"+nifName, &nif, opts...)
	return nif, err
}

// CreateNetInterface creates the given network interface on a given node.
func (n *NodeService) CreateNetInterface(ctx context.Context, nodeName string, nif NetInterface) error {
	_, err := n.client.doPOST(ctx, "/v1/nodes/"+nodeName+"/net-interfaces", nif)
	return err
}

// ModifyNetInterface modifies the given network interface on a given node.
func (n *NodeService) ModifyNetInterface(ctx context.Context, nodeName, nifName string, nif NetInterface) error {
	_, err := n.client.doPUT(ctx, "/v1/nodes/"+nodeName+"/net-interfaces/"+nifName, nif)
	return err
}

// DeleteNetinterface deletes the given network interface on a given node.
func (n *NodeService) DeleteNetinterface(ctx context.Context, nodeName, nifName string) error {
	_, err := n.client.doDELETE(ctx, "/v1/nodes/"+nodeName+"/net-interfaces/"+nifName, nil)
	return err
}

// GetStoragePoolView gets information about all storage pools in the cluster.
func (n *NodeService) GetStoragePoolView(ctx context.Context, opts ...*ListOpts) ([]StoragePool, error) {
	var sps []StoragePool
	_, err := n.client.doGET(ctx, "/v1/view/storage-pools", &sps, opts...)
	return sps, err
}

// GetStoragePools gets information about all storage pools on a given node.
func (n *NodeService) GetStoragePools(ctx context.Context, nodeName string, opts ...*ListOpts) ([]StoragePool, error) {
	var sps []StoragePool
	_, err := n.client.doGET(ctx, "/v1/nodes/"+nodeName+"/storage-pools", &sps, opts...)
	return sps, err
}

// GetStoragePool gets information about a specific storage pool on a given node.
func (n *NodeService) GetStoragePool(ctx context.Context, nodeName, spName string, opts ...*ListOpts) (StoragePool, error) {
	var sp StoragePool
	_, err := n.client.doGET(ctx, "/v1/nodes/"+nodeName+"/storage-pools/"+spName, &sp, opts...)
	return sp, err
}

// CreateStoragePool creates a storage pool on a given node.
func (n *NodeService) CreateStoragePool(ctx context.Context, nodeName string, sp StoragePool) error {
	_, err := n.client.doPOST(ctx, "/v1/nodes/"+nodeName+"/storage-pools", sp)
	return err
}

// ModifyStoragePool modifies a storage pool on a given node.
func (n *NodeService) ModifyStoragePool(ctx context.Context, nodeName, spName string, sp StoragePool) error {
	_, err := n.client.doPOST(ctx, "/v1/nodes/"+nodeName+"/storage-pools/"+spName, sp)
	return err
}

// DeleteStoragePool deletes a storage pool on a given node.
func (n *NodeService) DeleteStoragePool(ctx context.Context, nodeName, spName string) error {
	_, err := n.client.doDELETE(ctx, "/v1/nodes/"+nodeName+"/storage-pools/"+spName, nil)
	return err
}

// GetStoragePoolPropsInfos gets meta information about the properties that can
// be set on a storage pool on a particular node.
func (n *NodeService) GetStoragePoolPropsInfos(ctx context.Context, nodeName string, opts ...*ListOpts) ([]PropsInfo, error) {
	var infos []PropsInfo
	_, err := n.client.doGET(ctx, "/v1/nodes/"+nodeName+"/storage-pools/properties/info", &infos, opts...)
	return infos, err
}

// GetPropsInfos gets meta information about the properties that can be set on
// a node.
func (n *NodeService) GetPropsInfos(ctx context.Context, opts ...*ListOpts) ([]PropsInfo, error) {
	var infos []PropsInfo
	_, err := n.client.doGET(ctx, "/v1/nodes/properties/info", &infos, opts...)
	return infos, err
}

// Evict the given node, migrating resources to the remaining nodes, if possible.
func (n NodeService) Evict(ctx context.Context, nodeName string) error {
	_, err := n.client.doPUT(ctx, "/v1/nodes/"+nodeName+"/evict", nil)
	return err
}

// Restore an evicted node, optionally keeping existing resources.
func (n *NodeService) Restore(ctx context.Context, nodeName string, restore NodeRestore) error {
	_, err := n.client.doPUT(ctx, "/v1/nodes/"+nodeName+"/restore", restore)
	return err
}
