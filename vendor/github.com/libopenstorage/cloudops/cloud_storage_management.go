package cloudops

// StorageDecisionMatrixRow defines an entry in the cloud storage decision matrix.
type StorageDecisionMatrixRow struct {
	// IOPS is the desired iops from the underlying cloud storage.
	IOPS uint32 `json:"iops" yaml:"iops"`
	// InstanceType is the type of instance on which the cloud storage will
	// be attached.
	InstanceType string `json:"instance_type" yaml:"instance_type"`
	// InstanceMaxDrives is the maximum number of drives that can be attached
	// to an instance without a performance hit.
	InstanceMaxDrives uint32 `json:"instance_max_drives" yaml:"instance_max_drives"`
	// InstanceMinDrives is the minimum number of drives that need to be
	// attached to an instance to achieve maximum performance.
	InstanceMinDrives uint32 `json:"instance_min_drives" yaml:"instance_min_drives"`
	// Region of the instance.
	Region string `json:"region" yaml:"region"`
	// MinSize is the minimum size of the drive that needs to be provisioned
	// to achieve the desired IOPS on the provided instance types.
	MinSize uint64 `json:"min_size" yaml:"min_size"`
	// MaxSize is the maximum size of the drive that can be provisioned
	// without affecting performance on the provided instance type.
	MaxSize uint64 `json:"max_size" yaml:"max_size"`
	// Priority for this entry in the decision matrix.
	Priority string `json:"priority" yaml:"priority"`
	// ThinProvisioning if set will provision the backing device to be thinly provisioned if supported by cloud provider.
	ThinProvisioning bool `json:"thin_provisioning" yaml:"thin_provisioning"`
}

// StorageDecisionMatrix is used to determine the optimum cloud storage distribution
// for a cluster based on user's requirement specified through StorageSpec
type StorageDecisionMatrix struct {
	// Rows of the decision matrix
	Rows []*StorageDecisionMatrixRow `json:"rows" yaml:"rows"`
}

// StorageSpec is the user provided storage requirement for the cluster.
// This specifies desired capacity thresholds and desired IOPS  If there is a requirement
// for two different drive types then multiple StorageSpecs need to be provided to
// the StorageManager
type StorageSpec struct {
	// IOPS is the desired IOPS from the underlying storag.
	IOPS uint32 `json:"iops" yaml:"iops"`
	// MinCapacity is the minimum capacity of storage for the cluster.
	MinCapacity uint64 `json:"min_capacity" yaml:"min_capacity"`
	// MaxCapacity is the upper threshold on the total capacity of storage
	// that can be provisioned in this cluster.
	MaxCapacity uint64 `json:"max_capacity" yaml:"max_capacity"`
}

// StorageDistributionRequest is the input the cloud drive decision matrix. It provides
// the user's storage requirement as well as other cloud provider specific details.
type StorageDistributionRequest struct {
	// UserStorageSpec is a list of user's storage requirements.
	UserStorageSpec []*StorageSpec `json:"user_storage_spec" yaml:"user_storage_spec"`
	// InstanceType is the type of instance where user needs to provision storage.
	InstanceType string `json:"instance_type" yaml:"instance_type"`
	// InstancesPerZone is the number of instances in each zone.
	InstancesPerZone int `json:"instances_per_zone" yaml:"instances_per_zone"`
	// ZoneCount is the number of zones across which the instances are
	// distributed in the cluster.
	ZoneCount int `json:"zone_count" yaml:"zone_count"`
}

// StoragePoolSpec defines the type, capacity and number of storage drive that needs
// to be provisioned. These set of drives should be grouped into a single storage pool.
type StoragePoolSpec struct {
	// DriveCapacityGB is the capacity of the drive in GiB.
	DriveCapacityGB int64 `json:"drive_capacity_gb" yaml:"drive_capacity_gb"`
	// DriveType is the type of drive specified in terms of cloud provided names.
	DriveType string `json:"drive_type" yaml:"drive_type"`
	// DriveCount is the number of drives that need to be provisioned of the
	// specified capacity and type.
	DriveCount int `json:"drive_count" yaml:"drive_count"`
}

// StorageDistributionResponse is the result returned the CloudStorage Decision Matrix
// for the provided request.
type StorageDistributionResponse struct {
	// InstanceStorage defines a list of storage pool specs that need to be
	// provisioned on an instance.
	InstanceStorage []*StoragePoolSpec `json:"instance_storage" yaml:"instance_storage"`
	// InstancesPerZone is the number of instances per zone on which the above
	// defined storage needs to be provisioned.
	InstancesPerZone int `json:"instances_per_zone" yaml:"instances_per_zone"`
}

// StorageManager interface provides a set of APIs to manage cloud storage drives
// across multiple nodes in the cluster.
type StorageManager interface {
	// GetStorageDistribution returns the storage distribution for the provided request
	GetStorageDistribution(request *StorageDistributionRequest) (*StorageDistributionResponse, error)
}
