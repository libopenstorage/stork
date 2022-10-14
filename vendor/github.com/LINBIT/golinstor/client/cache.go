package client

type CacheResource struct {
	CacheVolumes []CacheVolume `json:"cache_volumes,omitempty"`
}

type CacheVolume struct {
	VolumeNumber int32 `json:"volume_number,omitempty"`
	// block device path
	DevicePath string `json:"device_path,omitempty"`
	// block device path used as cache device
	DevicePathCache string `json:"device_path_cache,omitempty"`
	// block device path used as meta device
	DeviceMetaCache  string `json:"device_meta_cache,omitempty"`
	AllocatedSizeKib int64  `json:"allocated_size_kib,omitempty"`
	UsableSizeKib    int64  `json:"usable_size_kib,omitempty"`
	// String describing current volume state
	DiskState string `json:"disk_state,omitempty"`
}
