package devicelayerkind

type DeviceLayerKind string

const (
	Drbd       DeviceLayerKind = "DRBD"
	Luks       DeviceLayerKind = "LUKS"
	Storage    DeviceLayerKind = "STORAGE"
	Nvme       DeviceLayerKind = "NVME"
	Openflex   DeviceLayerKind = "OPENFLEX"
	Exos       DeviceLayerKind = "EXOS"
	Writecache DeviceLayerKind = "WRITECACHE"
	Cache      DeviceLayerKind = "CACHE"
	BCache     DeviceLayerKind = "BCACHE"
)
