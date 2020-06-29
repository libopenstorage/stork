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
