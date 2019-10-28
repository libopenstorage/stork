package v1alpha1

import (
	v1 "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

const (
	// StorageClusterResourceName is name for "storagecluster" resource
	StorageClusterResourceName = "storagecluster"
	// StorageClusterResourcePlural is plural for "storagecluster" resource
	StorageClusterResourcePlural = "storageclusters"
	// StorageClusterShortName is the shortname for "storagecluster" resource
	StorageClusterShortName = "stc"
)

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// StorageCluster represents a storage cluster
type StorageCluster struct {
	meta.TypeMeta   `json:",inline"`
	meta.ObjectMeta `json:"metadata,omitempty"`
	Spec            StorageClusterSpec   `json:"spec"`
	Status          StorageClusterStatus `json:"status"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// StorageClusterList is a list of StorageCluster
type StorageClusterList struct {
	meta.TypeMeta `json:",inline"`
	meta.ListMeta `json:"metadata,omitempty"`
	Items         []StorageCluster `json:"items"`
}

// StorageClusterSpec is the spec used to define a storage cluster
type StorageClusterSpec struct {
	// An update strategy to replace existing StorageCluster pods with new pods.
	// Default strategy is RollingUpdate
	UpdateStrategy StorageClusterUpdateStrategy `json:"updateStrategy,omitempty"`
	// A delete strategy to uninstall and wipe an existing StorageCluster
	DeleteStrategy *StorageClusterDeleteStrategy `json:"deleteStrategy,omitempty"`
	// RevisionHistoryLimit is the number of old history to retain to allow rollback.
	// This is a pointer to distinguish between explicit zero and not specified.
	// Defaults to 10.
	RevisionHistoryLimit *int32 `json:"revisionHistoryLimit,omitempty"`
	// Placement configuration for the storage cluster nodes
	Placement *PlacementSpec `json:"placement,omitempty"`
	// Image is docker image of the storage driver
	Image string `json:"image,omitempty"`
	// Version is the version of storage driver
	Version string `json:"version,omitempty"`
	// ImagePullPolicy is the image pull policy.
	// One of Always, Never, IfNotPresent. Defaults to Always.
	ImagePullPolicy v1.PullPolicy `json:"imagePullPolicy,omitempty"`
	// ImagePullSecret is a reference to secret in the same namespace as the
	// storage cluster, used for pulling images used by this StorageClusterSpec
	ImagePullSecret *string `json:"imagePullSecret,omitempty"`
	// CustomImageRegistry is a custom container registry server (may include
	// repository) that will be used instead of index.docker.io to download Docker
	// images. (Example: myregistry.net:5443 or myregistry.com/myrepository)
	CustomImageRegistry string `json:"customImageRegistry,omitempty"`
	// Kvdb is the information of kvdb that storage driver uses
	Kvdb *KvdbSpec `json:"kvdb,omitempty"`
	// CloudStorage details of storage in cloud environment.
	// Keep this only at cluster level for now until support to change
	// cloud storage at node level is added.
	CloudStorage *CloudStorageSpec `json:"cloudStorage,omitempty"`
	// SecretsProvider is the name of secret provider that driver will connect to
	SecretsProvider *string `json:"secretsProvider,omitempty"`
	// StartPort is the starting port in the range of ports used by the cluster
	StartPort *uint32 `json:"startPort,omitempty"`
	// CallHome send cluster information for analytics
	CallHome *bool `json:"callHome,omitempty"`
	// FeatureGates are a set of key-value pairs that describe what experimental
	// features need to be enabled
	FeatureGates map[string]string `json:"featureGates,omitempty"`
	// CommonConfig that is present at both cluster and node level
	CommonConfig
	// UserInterface contains details of a user interface for the storage driver
	UserInterface *UserInterfaceSpec `json:"userInterface,omitempty"`
	// Stork contains STORK related parameters. For more information about STORK,
	// check https://github.com/libopenstorage/stork
	Stork *StorkSpec `json:"stork,omitempty"`
	// Nodes node level configurations that will override the ones at cluster
	// level. These configurations can be grouped based on label selectors.
	// Nodes []NodeSpec `json:"nodes"`
}

// NodeSpec is the spec used to define node level configuration. Values
// here will override the ones present at cluster-level for nodes matching
// the selector.
type NodeSpec struct {
	// Selector rest of the attributes are applied to a node that matches
	// the selector
	Selector NodeSelector `json:"selector,omitempty"`
	// Geo is topology information for the node
	Geo *Geography `json:"geography,omitempty"`
	// CommonConfig that is present at both cluster and node level
	CommonConfig
}

// CommonConfig are common configurations that are exposed at both
// cluster and node level
type CommonConfig struct {
	// Network is the network information for storage driver
	Network *NetworkSpec `json:"network,omitempty"`
	// Storage details of storage used by the driver
	Storage *StorageSpec `json:"storage,omitempty"`
	// Env is a list of environment variables used by the driver
	Env []v1.EnvVar `json:"env,omitempty"`
	// RuntimeOpts is a map of options with extra configs for storage driver
	RuntimeOpts map[string]string `json:"runtimeOptions,omitempty"`
}

// NodeSelector let's the user select a node or group of nodes based on either
// the NodeName or the node LabelSelector. If NodeName is specified then,
// LabelSelector is ignored as that is more accurate, even though it does not
// match any node names.
type NodeSelector struct {
	// NodeName is the name of Kubernetes node that it to be selected
	NodeName string `json:"nodeName,omitempty"`
	// LabelSelector is label query over all the nodes in the cluster
	LabelSelector *meta.LabelSelector `json:"labelSelector,omitempty"`
}

// PlacementSpec has placement configuration for the storage cluster nodes
type PlacementSpec struct {
	// NodeAffinity describes node affinity scheduling rules for the pods
	NodeAffinity *v1.NodeAffinity `json:"nodeAffinity,omitempty"`
}

// StorageClusterUpdateStrategy is used to control the update strategy for a StorageCluster
type StorageClusterUpdateStrategy struct {
	// Type of storage cluster update strategy. Default is RollingUpdate.
	Type StorageClusterUpdateStrategyType `json:"type,omitempty"`
	// Rolling update config params. Present only if type = "RollingUpdate".
	RollingUpdate *RollingUpdateStorageCluster `json:"rollingUpdate,omitempty"`
}

// StorageClusterUpdateStrategyType is enum for storage cluster update strategies
type StorageClusterUpdateStrategyType string

const (
	// RollingUpdateStorageClusterStrategyType replace the old pods by new ones
	// using rolling update i.e replace them on each node one after the other.
	RollingUpdateStorageClusterStrategyType StorageClusterUpdateStrategyType = "RollingUpdate"
	// OnDeleteStorageClusterStrategyType replace the old pods only when they are killed
	OnDeleteStorageClusterStrategyType StorageClusterUpdateStrategyType = "OnDelete"
)

// RollingUpdateStorageCluster controls the desired behavior of storage cluster rolling update.
type RollingUpdateStorageCluster struct {
	// The maximum number of StorageCluster pods that can be unavailable during the
	// update. Value can be an absolute number (ex: 5) or a percentage of total
	// number of StorageCluster pods at the start of the update (ex: 10%). Absolute
	// number is calculated from percentage by rounding up.
	// This cannot be 0.
	// Default value is 1.
	// Example: when this is set to 30%, at most 30% of the total number of nodes
	// that should be running the storage pod
	// can have their pods stopped for an update at any given
	// time. The update starts by stopping at most 30% of those StorageCluster pods
	// and then brings up new StorageCluster pods in their place. Once the new pods
	// are available, it then proceeds onto other StorageCluster pods, thus ensuring
	// that at least 70% of original number of StorageCluster pods are available at
	// all times during the update.
	MaxUnavailable *intstr.IntOrString `json:"maxUnavailable,omitempty"`
}

// StorageClusterDeleteStrategyType is enum for storage cluster delete strategies
type StorageClusterDeleteStrategyType string

const (
	// UninstallStorageClusterStrategyType will only uninstall the storage service
	// from all the nodes in the cluster. It will not wipe/format the storage devices
	// being used by the Storage Cluster
	UninstallStorageClusterStrategyType StorageClusterDeleteStrategyType = "Uninstall"
	// UninstallAndWipeStorageClusterStrategyType will uninstall the storage service
	// from all the nodes in the cluster. It also wipe/format the storage devices
	// being used by the Storage Cluster
	UninstallAndWipeStorageClusterStrategyType StorageClusterDeleteStrategyType = "UninstallAndWipe"
)

// StorageClusterDeleteStrategy is used to control the delete strategy for a StorageCluster
type StorageClusterDeleteStrategy struct {
	// Type of storage cluster delete strategy.
	Type StorageClusterDeleteStrategyType `json:"type,omitempty"`
}

// KvdbSpec contains the details to access kvdb
type KvdbSpec struct {
	// Internal flag indicates whether to use internal kvdb or an external one
	Internal bool `json:"internal,omitempty"`
	// Endpoints to access the kvdb
	Endpoints []string `json:"endpoints,omitempty"`
	// AuthSecret is name of the kubernetes secret containing information
	// to authenticate with the kvdb. It could have the username/password
	// for basic auth, certificate information or ACL token.
	AuthSecret string `json:"authSecret,omitempty"`
}

// NetworkSpec contains network information
type NetworkSpec struct {
	// DataInterface is the network interface used by driver for data traffic
	DataInterface *string `json:"dataInterface,omitempty"`
	// MgmtInterface is the network interface used by driver for mgmt traffic
	MgmtInterface *string `json:"mgmtInterface,omitempty"`
}

// StorageSpec details of storage used by the driver
type StorageSpec struct {
	// UseAll use all available, unformatted, unpartioned devices.
	// This will be ignored if Devices is not empty.
	UseAll *bool `json:"useAll,omitempty"`
	// UseAllWithPartitions use all available unformatted devices
	// including partitions. This will be ignored if Devices is not empty.
	UseAllWithPartitions *bool `json:"useAllWithPartitions,omitempty"`
	// ForceUseDisks use the drives even if there is file system present on it.
	// Note that the drives may be wiped before using.
	ForceUseDisks *bool `json:"forceUseDisks,omitempty"`
	// Devices list of devices to be used by storage driver
	Devices *[]string `json:"devices,omitempty"`
	// JournalDevice device for journaling
	JournalDevice *string `json:"journalDevice,omitempty"`
	// SystemMdDevice device that will be used to store system metadata
	SystemMdDevice *string `json:"systemMetadataDevice,omitempty"`
	// DataStorageType backing store type for managing drives and pools
	DataStorageType *string `json:"dataStorageType,omitempty"`
	// RaidLevel raid level for the storage pool
	RaidLevel *string `json:"raidLevel,omitempty"`
}

// CloudStorageCapacitySpec details the minimum and maximum amount of storage
// that will be provisioned in the cluster for a particular set of minimum IOPS.
type CloudStorageCapacitySpec struct {
	// MinIOPS minimum IOPS expected from the cloud drive
	MinIOPS uint32 `json:"minIOPS"`
	// MinCapacityInGiB minimum capacity for this cloud device spec
	MinCapacityInGiB uint64 `json:"minCapacityInGiB"`
	// MaxCapacityInGiB capacity for this cloud device spec should not go above this threshold
	MaxCapacityInGiB uint64 `json:"maxCapacityInGiB,omitempty"`
	// Options additional options required to provision the drive in cloud
	Options map[string]string `json:"options,omitempty"`
}

// CloudStorageSpec details of storage in cloud environment
type CloudStorageSpec struct {
	// DeviceSpecs list of storage device specs. A cloud storage device will
	// be created for every spec in the DeviceSpecs list. Currently,
	// CloudStorageSpec is only at the cluster level, so the below specs
	// be applied to all storage nodes in the cluster.
	// (Deprecated) DeviceSpecs will be removed from StorageCluster in v1alpha2.
	// Use CapacitySpecs instead
	DeviceSpecs *[]string `json:"deviceSpecs,omitempty"`

	// CapacitySpecs list of cluster wide storage types and their capacities.
	// A single capacity spec identifies a storage pool with a set of minimum
	// requested IOPS and size. Based on the cloud provider, the total storage
	// capacity will get divided amongst the nodes. The nodes bearing storage
	// themselves will get uniformly distributed across all the zones.
	// CapacitySpecs is slated to replace DeviceSpecs in v1alpha2 version of StorageCluster.
	CapacitySpecs []CloudStorageCapacitySpec `json:"capacitySpecs,omitempty"`

	// JournalDeviceSpec spec for the journal device
	JournalDeviceSpec *string `json:"journalDeviceSpec,omitempty"`
	// SystemMdDeviceSpec spec for the metadata device
	SystemMdDeviceSpec *string `json:"systemMetadataDeviceSpec,omitempty"`
	// MaxStorageNodes maximum nodes that will have storage in the cluster
	MaxStorageNodes *uint32 `json:"maxStorageNodes,omitempty"`
	// MaxStorageNodesPerZone maximum nodes in every zone that will have
	// storage in the cluster
	MaxStorageNodesPerZone *uint32 `json:"maxStorageNodesPerZone,omitempty"`
}

// Geography is topology information for a node
type Geography struct {
	// Region region in which the node is placed
	Region string `json:"region,omitempty"`
	// Zone zone in which the node is placed
	Zone string `json:"zone,omitempty"`
	// Rack rack on which the node is placed
	Rack string `json:"rack,omitempty"`
}

// UserInterfaceSpec contains details of a user interface for the storage driver
type UserInterfaceSpec struct {
	// Enabled decides whether the user interface component needs to be enabled
	Enabled bool `json:"enabled,omitempty"`
	// Image is the docker image of the user interface container
	Image string `json:"image,omitempty"`
	// LockImage is a boolean indicating if the user interface image needs to be locked
	// to the given image. If the image is not locked, it can be updated by the driver
	// during upgrades.
	LockImage bool `json:"lockImage,omitempty"`
	// Env is a list of environment variables used by UI component
	Env []v1.EnvVar `json:"env,omitempty"`
}

// StorkSpec contains STORK related spec
type StorkSpec struct {
	// Enabled decides whether STORK needs to be enabled
	Enabled bool `json:"enabled,omitempty"`
	// Image is docker image of the STORK container
	Image string `json:"image,omitempty"`
	// LockImage is a boolean indicating if the stork image needs to be locked
	// to the given image. If the image is not locked, it can be updated by the
	// driver during upgrades.
	LockImage bool `json:"lockImage,omitempty"`
	// Args is a map of arguments given to STORK
	Args map[string]string `json:"args,omitempty"`
	// Env is a list of environment variables used by stork
	Env []v1.EnvVar `json:"env,omitempty"`
}

// StorageClusterStatus is the status of a storage cluster
type StorageClusterStatus struct {
	// ClusterName name of the storage cluster
	ClusterName string `json:"clusterName,omitempty"`
	// ClusterUID unique ID for the storage cluster
	ClusterUID string `json:"clusterUid,omitempty"`
	// CreatedAt timestamp at which the storage cluster was created
	CreatedAt *meta.Time `json:"createdAt,omitempty"`
	// Phase is current status of the storage cluster
	Phase string `json:"phase,omitempty"`
	// Count of hash collisions for the StorageCluster. The StorageCluster
	// controller uses this field as a collision avoidance mechanism when it
	// needs to create the name of the newest ControllerRevision.
	CollisionCount *int32 `json:"collisionCount,omitempty"`
	// Conditions describes the current conditions of the cluster
	Conditions []ClusterCondition `json:"conditions,omitempty"`
	// Storage represents cluster storage details
	Storage Storage `json:"storage,omitempty"`
}

// Storage represents cluster storage details
type Storage struct {
	// StorageNodesPerZone describes the amount of instances per zone
	StorageNodesPerZone int32 `json:"storageNodesPerZone,omitempty"`
}

// ClusterCondition contains condition information for the cluster
type ClusterCondition struct {
	// Type is the type of condition
	Type ClusterConditionType `json:"type"`
	// Status of the condition
	Status ClusterConditionStatus `json:"status"`
	// Reason is human readable message indicating details about the current state of the cluster
	Reason string `json:"reason"`
}

// ClusterConditionType is the enum type for different cluster conditions
type ClusterConditionType string

// These are valid cluster condition types
const (
	// ClusterConditionTypeUpgrade indicates the status for an upgrade operation on the cluster
	ClusterConditionTypeUpgrade ClusterConditionType = "Upgrade"
	// ClusterConditionTypeDelete indicates the status for a delete operation on the cluster
	ClusterConditionTypeDelete ClusterConditionType = "Delete"
	// ClusterConditionTypeInstall indicates the status for an install operation on the cluster
	ClusterConditionTypeInstall ClusterConditionType = "Install"
)

// ClusterConditionStatus is the enum type for cluster condition statuses
type ClusterConditionStatus string

// These are valid cluster statuses.
const (
	// ClusterInit means the cluster is initializing
	ClusterInit ClusterConditionStatus = "Initializing"
	// ClusterOnline means the cluster is up and running
	ClusterOnline ClusterConditionStatus = "Online"
	// ClusterOffline means the cluster is offline
	ClusterOffline ClusterConditionStatus = "Offline"
	// ClusterNotInQuorum means the cluster is out of quorum
	ClusterNotInQuorum ClusterConditionStatus = "NotInQuorum"
	// ClusterUnknown means the cluser status is not known
	ClusterUnknown ClusterConditionStatus = "Unknown"
	// ClusterOperationInProgress means the cluster operation is in progress
	ClusterOperationInProgress ClusterConditionStatus = "InProgress"
	// ClusterOperationCompleted means the cluster operation has completed
	ClusterOperationCompleted ClusterConditionStatus = "Completed"
	// ClusterOperationFailed means the cluster operation failed
	ClusterOperationFailed ClusterConditionStatus = "Failed"
	// ClusterOperationTimeout means the cluster operation timedout
	ClusterOperationTimeout ClusterConditionStatus = "Timeout"
)

func init() {
	SchemeBuilder.Register(&StorageCluster{}, &StorageClusterList{})
}
