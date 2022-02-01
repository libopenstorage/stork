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

import "context"

// copy & paste from generated code

// PhysicalStorageStoragePoolCreate is used for create physical-storage
type PhysicalStorageStoragePoolCreate struct {
	// Name of the linstor storage pool
	Name string `json:"name,omitempty"`
	// A string to string property map.
	Props map[string]string `json:"props,omitempty"`
	// Name of the shared space
	SharedSpace string `json:"shared_space,omitempty"`
	// true if a shared storage pool uses linstor-external locking, like cLVM
	ExternalLocking bool `json:"external_locking,omitempty"`
}

// PhysicalStorageCreate is a configuration struct used to represent pysical storage on a given node.
// If with_storage_pool is set a linstor storage pool will also be created using this device pool
type PhysicalStorageCreate struct {
	ProviderKind ProviderKind `json:"provider_kind"`
	DevicePaths  []string     `json:"device_paths"`
	// RAID level to use for pool.
	RaidLevel         string                           `json:"raid_level,omitempty"`
	PoolName          string                           `json:"pool_name,omitempty"`
	VdoEnable         bool                             `json:"vdo_enable,omitempty"`
	VdoSlabSizeKib    int64                            `json:"vdo_slab_size_kib,omitempty"`
	VdoLogicalSizeKib int64                            `json:"vdo_logical_size_kib,omitempty"`
	WithStoragePool   PhysicalStorageStoragePoolCreate `json:"with_storage_pool,omitempty"`
}

// PhysicalStorageDevice represents a physical storage device on a a node.
type PhysicalStorageDevice struct {
	Device string `json:"device,omitempty"`
	Model  string `json:"model,omitempty"`
	Serial string `json:"serial,omitempty"`
	Wwn    string `json:"wwn,omitempty"`
}

// PhysicalStorage is a view on a physical storage on multiple nodes.
type PhysicalStorage struct {
	Size       int64                              `json:"size,omitempty"`
	Rotational bool                               `json:"rotational,omitempty"`
	Nodes      map[string][]PhysicalStorageDevice `json:"nodes,omitempty"`
}

// GetPhysicalStorage gets a grouped list of physical storage that can be turned into a LINSTOR storage-pool
func (n *NodeService) GetPhysicalStorage(ctx context.Context, opts ...*ListOpts) ([]PhysicalStorage, error) {
	var ps []PhysicalStorage
	_, err := n.client.doGET(ctx, "/v1/physical-storage/", &ps, opts...)
	return ps, err
}

// CreateDevicePool creates an LVM, LVM-thin or ZFS pool, optional VDO under it on a given node.
func (n *NodeService) CreateDevicePool(ctx context.Context, nodeName string, psc PhysicalStorageCreate) error {
	_, err := n.client.doPOST(ctx, "/v1/physical-storage/"+nodeName, psc)
	return err
}
