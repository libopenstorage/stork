//go:generate mockgen --package=mock -destination=mock/cloud_storage_management.mock.go github.com/libopenstorage/cloudops StorageManager

package cloudops

import (
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/libopenstorage/openstorage/api"
)

var (
	// ErrNumOfZonesCannotBeZero is returned when the number of zones provided is zero
	ErrNumOfZonesCannotBeZero = errors.New("number of zones cannot be zero or less than zero")
	// ErrCurrentCapacitySameAsDesired is returned when total current capacity
	// of instance is already equal to requested capacity
	ErrCurrentCapacitySameAsDesired = errors.New("current capacity is already equal to new capacity")
)

// ErrStorageDistributionCandidateNotFound is returned when the storage manager fails to
// determine the right storage distribution candidate
type ErrStorageDistributionCandidateNotFound struct {
	Reason string
}

func (e *ErrStorageDistributionCandidateNotFound) Error() string {
	if len(e.Reason) > 0 {
		return fmt.Sprintf("could not find a suitable storage distribution candidate: %s", e.Reason)
	}

	return fmt.Sprintf("could not find a suitable storage distribution candidate")
}

// StorageDecisionMatrixRow defines an entry in the cloud storage decision matrix.
type StorageDecisionMatrixRow struct {
	// MinIOPS is the minimum desired iops from the underlying cloud storage.
	MinIOPS uint64 `json:"min_iops" yaml:"min_iops"`
	// MaxIOPS is the maximum desired iops from the underlying cloud storage.
	MaxIOPS uint64 `json:"max_iops" yaml:"max_iops"`
	// InstanceType is the type of instance on which the cloud storage can
	// be attached.
	InstanceType string `json:"instance_type" yaml:"instance_type"`
	// InstanceMaxDrives is the maximum number of drives that can be attached
	// to an instance without a performance hit.
	InstanceMaxDrives uint64 `json:"instance_max_drives" yaml:"instance_max_drives"`
	// InstanceMinDrives is the minimum number of drives that need to be
	// attached to an instance to achieve maximum performance.
	InstanceMinDrives uint64 `json:"instance_min_drives" yaml:"instance_min_drives"`
	// Region of the instance.
	Region string `json:"region" yaml:"region"`
	// MinSize is the minimum size of the drive that needs to be provisioned
	// to achieve the desired IOPS on the provided instance types.
	MinSize uint64 `json:"min_size" yaml:"min_size"`
	// MaxSize is the maximum size of the drive that can be provisioned
	// without affecting performance on the provided instance type.
	MaxSize uint64 `json:"max_size" yaml:"max_size"`
	// Priority for this entry in the decision matrix.
	Priority int `json:"priority" yaml:"priority"`
	// ThinProvisioning if set will provision the backing device to be thinly provisioned if supported by cloud provider.
	ThinProvisioning bool `json:"thin_provisioning" yaml:"thin_provisioning"`
	// DriveType is the type of drive
	DriveType string `json:"drive_type" yaml:"drive_type"`
}

// StorageDecisionMatrix is used to determine the optimum cloud storage distribution
// for a cluster based on user's requirement specified through StorageSpec
type StorageDecisionMatrix struct {
	// Rows of the decision matrix
	Rows []StorageDecisionMatrixRow `json:"rows" yaml:"rows"`
}

// StorageSpec is the user provided storage requirement for the cluster.
// This specifies desired capacity thresholds and desired IOPS  If there is a requirement
// for two different drive types then multiple StorageSpecs need to be provided to
// the StorageManager
type StorageSpec struct {
	// MinCapacity is the minimum capacity of storage for the cluster.
	MinCapacity uint64 `json:"min_capacity" yaml:"min_capacity"`
	// MaxCapacity is the upper threshold on the total capacity of storage
	// that can be provisioned in this cluster.
	MaxCapacity uint64 `json:"max_capacity" yaml:"max_capacity"`
	// DriveType is the type of drive that's required (optional)
	DriveType string `json:"drive_type" yaml:"drive_type"`
	// IOPS is the desired IOPS from the underlying storage (optional)
	IOPS uint64 `json:"iops" yaml:"iops"`
}

// StorageDistributionRequest is the input the cloud drive decision matrix. It provides
// the user's storage requirement as well as other cloud provider specific details.
type StorageDistributionRequest struct {
	// UserStorageSpec is a list of user's storage requirements.
	UserStorageSpec []*StorageSpec `json:"user_storage_spec" yaml:"user_storage_spec"`
	// InstanceType is the type of instance where user needs to provision storage.
	InstanceType string `json:"instance_type" yaml:"instance_type"`
	// InstancesPerZone is the number of instances in each zone.
	InstancesPerZone uint64 `json:"instances_per_zone" yaml:"instances_per_zone"`
	// ZoneCount is the number of zones across which the instances are
	// distributed in the cluster.
	ZoneCount uint64 `json:"zone_count" yaml:"zone_count"`
}

// StoragePoolSpec defines the type, capacity and number of storage drive that needs
// to be provisioned. These set of drives should be grouped into a single storage pool.
type StoragePoolSpec struct {
	// DriveCapacityGiB is the capacity of the drive in GiB.
	DriveCapacityGiB uint64 `json:"drive_capacity_gb" yaml:"drive_capacity_gb"`
	// DriveType is the type of drive specified in terms of cloud provided names.
	DriveType string `json:"drive_type" yaml:"drive_type"`
	// DriveCount is the number of drives that need to be provisioned on the
	// instance
	DriveCount uint64 `json:"drive_count" yaml:"drive_count"`
	// InstancesPerZone is the number of instances per zone
	InstancesPerZone uint64 `json:"instances_per_zone" yaml:"instances_per_zone"`
	// IOPS is the IOPS of the drive
	IOPS uint64 `json:"iops" yaml:"iops"`
}

// StorageDistributionResponse is the result returned the CloudStorage Decision Matrix
// for the provided request.
type StorageDistributionResponse struct {
	// InstanceStorage defines a list of storage pool specs that need to be
	// provisioned on an instance.
	InstanceStorage []*StoragePoolSpec `json:"instance_storage" yaml:"instance_storage"`
}

// StoragePoolUpdateRequest is the required changes for updating the storage on a given
// cloud instance
type StoragePoolUpdateRequest struct {
	// DesiredCapacity is the new required capacity on the storage pool
	DesiredCapacity uint64 `json:"new_capacity" yaml:"new_capacity"`
	// ResizeOperationType is the operation user wants for the storage resize on the node
	ResizeOperationType api.SdkStoragePool_ResizeOperationType
	// CurrentDriveCount is the current number of drives in the storage pool
	CurrentDriveCount uint64 `json:"current_drive_count" yaml:"current_drive_count"`
	// CurrentIOPS is the current IOPS for drives in the storage pool
	CurrentIOPS uint64 `json:"current_iops" yaml:"current_iops"`
	// CurrentDriveSize is the current size of drives in the storage pool
	CurrentDriveSize uint64 `json:"current_drive_size" yaml:"current_drive_size"`
	// CurrentDriveType is the current type of drives in the storage pool
	CurrentDriveType string `json:"current_drive_type" yaml:"current_drive_type"`
	// TotalDrivesOnNode is the total number of drives attached on the node
	TotalDrivesOnNode uint64 `json:"total_drives_on_node" yaml:"total_drives_on_node"`
}

// StoragePoolUpdateResponse is the result returned by the CloudStorage Decision Matrix
// for the storage update request
type StoragePoolUpdateResponse struct {
	// InstanceStorage defines a list of storage pool specs that need to be
	// provisioned or updated on an instance.
	InstanceStorage []*StoragePoolSpec `json:"instance_storage" yaml:"instance_storage"`
	// ResizeOperationType is the operation caller should perform on the disks in
	// the above InstanceStorage for the storage update on the instance
	ResizeOperationType api.SdkStoragePool_ResizeOperationType
}

// StorageManager interface provides a set of APIs to manage cloud storage drives
// across multiple nodes in the cluster.
type StorageManager interface {
	// GetStorageDistribution returns the storage distribution for the provided request
	GetStorageDistribution(request *StorageDistributionRequest) (*StorageDistributionResponse, error)
	// RecommendStoragePoolUpdate returns the recommended storage configuration on
	// the instance based on the given request
	RecommendStoragePoolUpdate(request *StoragePoolUpdateRequest) (*StoragePoolUpdateResponse, error)
}

var (
	storageManagers    map[ProviderType]InitStorageManagerFn
	storageManagerLock sync.Mutex
)

// InitStorageManagerFn initializes the cloud provider for Storage Management
type InitStorageManagerFn func(StorageDecisionMatrix) (StorageManager, error)

// NewStorageManager returns a cloud provider specific implementation of StorageManager interface.
func NewStorageManager(
	decisionMatrix StorageDecisionMatrix,
	provider ProviderType,
) (StorageManager, error) {
	storageManagerLock.Lock()
	defer storageManagerLock.Unlock()

	storageManagerInitFn, ok := storageManagers[provider]
	if !ok {
		return nil, fmt.Errorf("cloud storage management not available for %v cloud provider", provider)
	}
	return storageManagerInitFn(decisionMatrix)
}

// RegisterStorageManager registers cloud providers who support Storage Management
func RegisterStorageManager(
	provider ProviderType,
	initFn InitStorageManagerFn,
) error {
	storageManagerLock.Lock()
	defer storageManagerLock.Unlock()

	if storageManagers == nil {
		storageManagers = make(map[ProviderType]InitStorageManagerFn)
	}
	if _, ok := storageManagers[provider]; ok {
		return fmt.Errorf("storage manager %v already registered", provider)
	}
	storageManagers[provider] = initFn
	return nil
}

// FilterByDriveType filters out the rows which do not match the requested drive type.
func (dm *StorageDecisionMatrix) FilterByDriveType(requestedDriveType string) *StorageDecisionMatrix {
	var filteredRows []StorageDecisionMatrixRow
	if len(requestedDriveType) > 0 {
		for _, row := range dm.Rows {
			if row.DriveType == requestedDriveType {
				filteredRows = append(filteredRows, row)
			}
		}
		dm.Rows = filteredRows
	}
	return dm
}

// FilterByMinIOPS filters out the rows whose minIOPS are less than the requested IOPS.
func (dm *StorageDecisionMatrix) FilterByMinIOPS(requestedIOPS uint64) *StorageDecisionMatrix {
	var filteredRows []StorageDecisionMatrixRow
	if requestedIOPS > 0 {
		for _, row := range dm.Rows {
			if row.MinIOPS >= requestedIOPS {
				filteredRows = append(filteredRows, row)
			}
		}
		dm.Rows = filteredRows
	}
	return dm
}

// FilterByIOPS filters out the rows for which the requestedIOPS do not lie within the range
// of min and max IOPS or whose minIOPS are less than the requestedIOPS
func (dm *StorageDecisionMatrix) FilterByIOPS(requestedIOPS uint64) *StorageDecisionMatrix {
	var filteredRows []StorageDecisionMatrixRow
	if requestedIOPS > 0 {
		for _, row := range dm.Rows {
			if requestedIOPS <= row.MaxIOPS {
				filteredRows = append(filteredRows, row)
			}
		}
		dm.Rows = filteredRows
	}
	return dm
}

// FilterByDriveSizeRange filters out the rows for which the current drive size does not fit
// within the row's min and max size.
func (dm *StorageDecisionMatrix) FilterByDriveSizeRange(currentDriveSize uint64) *StorageDecisionMatrix {
	var filteredRows []StorageDecisionMatrixRow
	if currentDriveSize > 0 {
		for _, row := range dm.Rows {
			if row.MinSize <= currentDriveSize && currentDriveSize <= row.MaxSize {
				filteredRows = append(filteredRows, row)
			}
		}
		dm.Rows = filteredRows
	}
	return dm
}

// FilterByDriveSize returns a list of rows for which the current drive size fits within the row's min
// and max size or if the row's min size is greater than the current drive size
func (dm *StorageDecisionMatrix) FilterByDriveSize(currentDriveSize uint64) *StorageDecisionMatrix {
	var filteredRows []StorageDecisionMatrixRow
	if currentDriveSize > 0 {
		for _, row := range dm.Rows {
			if row.MinSize >= currentDriveSize {
				filteredRows = append(filteredRows, row)
			} else if currentDriveSize >= row.MinSize && currentDriveSize <= row.MaxSize {
				filteredRows = append(filteredRows, row)
			}
		}
		dm.Rows = filteredRows
	}
	return dm
}

// FilterByDriveCount filters out the rows for which the current drive count does not fit
// within the row's min and max drive count.
func (dm *StorageDecisionMatrix) FilterByDriveCount(currentDriveCount uint64) *StorageDecisionMatrix {
	var filteredRows []StorageDecisionMatrixRow
	if currentDriveCount > 0 {
		for _, row := range dm.Rows {
			if row.InstanceMinDrives <= currentDriveCount && currentDriveCount <= row.InstanceMaxDrives {
				filteredRows = append(filteredRows, row)
			}
		}
		dm.Rows = filteredRows
	}
	return dm
}

// SortByIOPS sorts the rows of the decision matrix in ascending order by MaxIOPS supported by that row.
func (dm *StorageDecisionMatrix) SortByIOPS() *StorageDecisionMatrix {
	sort.Slice(dm.Rows, func(l, r int) bool {
		return dm.Rows[l].MaxIOPS < dm.Rows[r].MaxIOPS
	})
	return dm
}

// SortByPriority sorts the rows of the decision matrix by Priority.
func (dm *StorageDecisionMatrix) SortByPriority() {
	sort.SliceStable(dm.Rows, func(l, r int) bool {
		return dm.Rows[l].Priority < dm.Rows[r].Priority
	})
}
