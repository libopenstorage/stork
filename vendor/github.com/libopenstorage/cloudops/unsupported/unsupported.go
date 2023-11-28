package unsupported

import (
	"time"

	"github.com/libopenstorage/cloudops"
)

type unsupportedCompute struct {
}

// NewUnsupportedCompute return wrapper for cloudOps where all methods are not supported
func NewUnsupportedCompute() cloudops.Compute {
	return &unsupportedCompute{}
}

func (u *unsupportedCompute) DeleteInstance(instanceID string, zone string, timeout time.Duration) error {
	return &cloudops.ErrNotSupported{
		Operation: "DeleteInstance",
	}
}
func (u *unsupportedCompute) InstanceID() string {
	return "Unsupported"
}

func (u *unsupportedCompute) InspectInstance(instanceID string) (*cloudops.InstanceInfo, error) {
	return nil, &cloudops.ErrNotSupported{
		Operation: "InspectInstance",
	}
}

func (u *unsupportedCompute) InspectInstanceGroupForInstance(instanceID string) (*cloudops.InstanceGroupInfo, error) {
	return nil, &cloudops.ErrNotSupported{
		Operation: "InspectInstanceGroupForInstance",
	}
}

func (u *unsupportedCompute) GetInstance(displayName string) (interface{}, error) {
	return nil, &cloudops.ErrNotSupported{
		Operation: "GetInstance",
	}
}

func (u *unsupportedCompute) SetInstanceGroupSize(instanceGroupID string,
	count int64,
	timeout time.Duration) error {
	return &cloudops.ErrNotSupported{
		Operation: "SetInstanceGroupSize",
	}
}

func (u *unsupportedCompute) GetInstanceGroupSize(instanceGroupID string) (int64, error) {
	return 0, &cloudops.ErrNotSupported{
		Operation: "GetInstanceGroupSize",
	}
}

func (u *unsupportedCompute) GetClusterSizeForInstance(instanceID string) (int64, error) {
	return int64(0), &cloudops.ErrNotSupported{
		Operation: "GetClusterSizeForInstance",
	}
}

func (u *unsupportedCompute) SetClusterVersion(version string, timeout time.Duration) error {
	return &cloudops.ErrNotSupported{
		Operation: "SetClusterVersion",
	}
}

func (u *unsupportedCompute) SetInstanceGroupVersion(instanceGroupID string,
	version string,
	timeout time.Duration) error {
	return &cloudops.ErrNotSupported{
		Operation: "SetInstanceGroupVersion",
	}
}

type unsupportedStorage struct {
}

// NewUnsupportedStorage return wrapper for cloudOps where all methods are not supported
func NewUnsupportedStorage() cloudops.Storage {
	return &unsupportedStorage{}
}

func (u *unsupportedStorage) Create(template interface{}, labels map[string]string, options map[string]string) (interface{}, error) {
	return nil, &cloudops.ErrNotSupported{
		Operation: "Create",
	}
}

func (u *unsupportedStorage) GetDeviceID(template interface{}) (string, error) {
	return "", &cloudops.ErrNotSupported{
		Operation: "GetDeviceID",
	}
}
func (u *unsupportedStorage) Attach(volumeID string, options map[string]string) (string, error) {
	return "", &cloudops.ErrNotSupported{
		Operation: "Attach",
	}
}

func (u *unsupportedStorage) AreVolumesReadyToExpand(volumeIDs []*string) (bool, error) {
	return true, &cloudops.ErrNotSupported{
		Operation: "unsupportedStorage:IsVolumesReadyToExpand",
	}
}

func (u *unsupportedStorage) Expand(volumeID string, newSizeInGiB uint64, options map[string]string) (uint64, error) {
	return 0, &cloudops.ErrNotSupported{
		Operation: "Expand",
	}
}
func (u *unsupportedStorage) Detach(volumeID string, options map[string]string) error {
	return &cloudops.ErrNotSupported{
		Operation: "Detach",
	}
}
func (u *unsupportedStorage) DetachFrom(volumeID, instanceID string) error {
	return &cloudops.ErrNotSupported{
		Operation: "DetachFrom",
	}
}
func (u *unsupportedStorage) Delete(volumeID string, options map[string]string) error {
	return &cloudops.ErrNotSupported{
		Operation: "Delete",
	}
}
func (u *unsupportedStorage) DeleteFrom(volumeID, instanceID string) error {
	return &cloudops.ErrNotSupported{
		Operation: "DeleteFrom",
	}
}

func (u *unsupportedStorage) Describe() (interface{}, error) {
	return nil, &cloudops.ErrNotSupported{
		Operation: "Describe",
	}
}
func (u *unsupportedStorage) FreeDevices(blockDeviceMappings []interface{}, rootDeviceName string) ([]string, error) {
	return []string{}, &cloudops.ErrNotSupported{
		Operation: "FreeDevices",
	}
}
func (u *unsupportedStorage) Inspect(volumeIds []*string, options map[string]string) ([]interface{}, error) {
	return nil, &cloudops.ErrNotSupported{
		Operation: "Inspect",
	}
}
func (u *unsupportedStorage) DeviceMappings() (map[string]string, error) {
	return map[string]string{}, &cloudops.ErrNotSupported{
		Operation: "DeviceMappings",
	}
}
func (u *unsupportedStorage) Enumerate(volumeIds []*string,
	labels map[string]string,
	setIdentifier string,
) (map[string][]interface{}, error) {
	return map[string][]interface{}{}, &cloudops.ErrNotSupported{
		Operation: "Enumerate",
	}
}
func (u *unsupportedStorage) DevicePath(volumeID string) (string, error) {
	return "", &cloudops.ErrNotSupported{
		Operation: "DevicePath",
	}
}
func (u *unsupportedStorage) Snapshot(volumeID string, readonly bool, options map[string]string) (interface{}, error) {
	return nil, &cloudops.ErrNotSupported{
		Operation: "Snapshot",
	}
}

func (u *unsupportedStorage) SnapshotDelete(snapID string, options map[string]string) error {
	return &cloudops.ErrNotSupported{
		Operation: "SnapshotDelete",
	}
}

func (u *unsupportedStorage) ApplyTags(volumeID string, labels map[string]string, options map[string]string) error {
	return &cloudops.ErrNotSupported{
		Operation: "ApplyTags",
	}
}

func (u *unsupportedStorage) RemoveTags(volumeID string, labels map[string]string, options map[string]string) error {
	return &cloudops.ErrNotSupported{
		Operation: "RemoveTags",
	}
}

func (u *unsupportedStorage) Tags(volumeID string) (map[string]string, error) {
	return map[string]string{}, &cloudops.ErrNotSupported{
		Operation: "Tags",
	}
}

type unsupportedStorageManager struct {
}

// NewUnsupportedStorageManager return wrapper for StorageManager where all methods are not supported
func NewUnsupportedStorageManager() cloudops.StorageManager {
	return &unsupportedStorageManager{}
}

func (u *unsupportedStorageManager) GetStorageDistribution(
	request *cloudops.StorageDistributionRequest,
) (*cloudops.StorageDistributionResponse, error) {
	return nil, &cloudops.ErrNotSupported{
		Operation: "GetStorageDistribution",
	}
}

func (u *unsupportedStorageManager) RecommendStoragePoolUpdate(
	request *cloudops.StoragePoolUpdateRequest) (*cloudops.StoragePoolUpdateResponse, error) {
	return nil, &cloudops.ErrNotSupported{
		Operation: "RecommendStoragePoolUpdate",
	}
}
