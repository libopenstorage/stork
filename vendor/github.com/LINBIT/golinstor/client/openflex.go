package client

// OpenflexResourceDefinitionLayer represents the Openflex resource definition layer
type OpenflexResourceDefinitionLayer struct {
	ResourceNameSuffix string `json:"resource_name_suffix,omitempty"`
	Nqn                string `json:"nqn,omitempty"`
}

// OpenflexResource represents an Openflex resource
type OpenflexResource struct {
	OpenflexResourceDefinition OpenflexResourceDefinitionLayer `json:"openflex_resource_definition,omitempty"`
	OpenflexVolumes            []OpenflexVolume                `json:"openflex_volumes,omitempty"`
}

// OpenflexVolume represents an Openflex volume
type OpenflexVolume struct {
	VolumeNumber int32 `json:"volume_number,omitempty"`
	// block device path
	DevicePath       string `json:"device_path,omitempty"`
	AllocatedSizeKib int64  `json:"allocated_size_kib,omitempty"`
	UsableSizeKib    int64  `json:"usable_size_kib,omitempty"`
	// String describing current volume state
	DiskState string `json:"disk_state,omitempty"`
}

// Function used if resource-definition-layertype is correct
func (d *OpenflexResourceDefinitionLayer) isOneOfDrbdResourceDefinitionLayerOpenflexResourceDefinitionLayer() {
}
