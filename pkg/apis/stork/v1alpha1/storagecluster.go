package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
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
	// Placement configuration for the storage cluster nodes
	Placement *PlacementSpec `json:"placement"`
	// Image is docker image of the storage driver
	Image string `json:"image"`
	// Kvdb is the information of kvdb that storage driver uses
	Kvdb *KvdbSpec `json:"kvdb"`
	// CloudStorage details of storage in cloud environment.
	// Keep this only at cluster level for now until support to change
	// cloud storage at node level is added.
	CloudStorage *CloudStorageSpec `json:"cloudStorage"`
	// SecretsProvider is the name of secret provider that driver will connect to
	SecretsProvider string `json:"secretsProvider"`
	// CSIEndpoint is the csi endpoint for the driver
	CSIEndpoint string `json:"csiEndpoint"`
	// CallHome send cluster information for analytics
	CallHome bool `json:"callHome"`
	// CommonConfig that is present at both cluster and node level
	CommonConfig
	// Nodes node level configurations that will override the ones at cluster
	// level. These configurations can be grouped based on label selectors.
	Nodes []NodeSpec `json:"nodes"`
}

// NodeSpec is the spec used to define node level configuration. Values
// here will override the ones present at cluster-level for nodes matching
// the selector.
type NodeSpec struct {
	// Selector rest of the attributes are applied to a node that matches
	// the selector
	Selector NodeSelector `json:"selector"`
	// Geo is topology information for the node
	Geo *Geography `json:"geography"`
	// CommonConfig that is present at both cluster and node level
	CommonConfig
}

// CommonConfig are common configurations that are exposed at both
// cluster and node level
type CommonConfig struct {
	// Network is the network information for storage driver
	Network *NetworkSpec `json:"network"`
	// Storage details of storage used by the driver
	Storage *StorageSpec `json:"storage"`
	// Env is a list of environment variables used by the driver
	Env []corev1.EnvVar `json:"env"`
	// RuntimeOpts is a map of options with extra configs for storage driver
	RuntimeOpts map[string]string `json:"runtimeOptions"`
}

// NodeSelector let's the user select a node or group of nodes based on either
// the NodeName or the node LabelSelector. If NodeName is specified then,
// LabelSelector is ignored as that is more accurate, even though it does not
// match any node names.
type NodeSelector struct {
	// NodeName is the name of Kubernetes node that it to be selected
	NodeName string `json:"nodeName"`
	// LabelSelector is label query over all the nodes in the cluster
	LabelSelector *meta.LabelSelector `json:"labelSelector"`
}

// PlacementSpec has placement configuration for the storage cluster nodes
type PlacementSpec struct {
	// NodeAffinity describes node affinity scheduling rules for the pods
	NodeAffinity *corev1.NodeAffinity `json:"nodeAffinity"`
}

// KvdbSpec contains the details to access kvdb
type KvdbSpec struct {
	// Internal flag indicates whether to use internal kvdb or an external one
	Internal bool `json:"internal"`
	// Endpoints to access the kvdb
	Endpoints []string `json:"endpoints"`
	// AuthSecret is name of the kubernetes secret containing information
	// to authenticate with the kvdb. It could have the username/password
	// for basic auth, certificate information or ACL token.
	AuthSecret string `json:"authSecret"`
}

// NetworkSpec contains network information
type NetworkSpec struct {
	// DataInterface is the network interface used by driver for data traffic
	DataInterface *string `json:"dataInterface"`
	// MgmtInterface is the network interface used by driver for mgmt traffic
	MgmtInterface *string `json:"mgmtInterface"`
}

// StorageSpec details of storage used by the driver
type StorageSpec struct {
	// UseAll use all available, unformatted, unpartioned devices.
	// This will be ignored if Devices is not empty.
	UseAll *bool `json:"useAll"`
	// UseAllWithPartitions use all available unformatted devices
	// including partitions. This will be ignored if Devices is not empty.
	UseAllWithPartitions *bool `json:"useAllWithPartitions"`
	// Devices list of devices to be used by storage driver
	Devices *[]string `json:"devices"`
	// JournalDevice device for journaling
	JournalDevice *string `json:"journalDevice"`
	// SystemMdDevice device that will be used to store system metadata
	SystemMdDevice *string `json:"systemMetadataDevice"`
	// DataStorageType backing store type for managing drives and pools
	DataStorageType *string `json:"dataStorageType"`
	// RaidLevel raid level for the storage pool
	RaidLevel *string `json:"raidLevel"`
}

// CloudStorageSpec details of storage in cloud environment
type CloudStorageSpec struct {
	// DeviceSpecs list of storage device specs. A cloud storage device will
	// be created for every spec in the DeviceSpecs list. Currently,
	// CloudStorageSpec is only at the cluster level, so the below specs
	// be applied to all storage nodes in the cluster.
	DeviceSpecs *[]string `json:"deviceSpecs"`
	// JournalDeviceSpec spec for the journal device
	JournalDeviceSpec *string `json:"journalDeviceSpec"`
	// SystemMdDeviceSpec spec for the metadata device
	SystemMdDeviceSpec *string `json:"systemMetadataDeviceSpec"`
	// MaxStorageNodes maximum nodes that will have storage in the cluster
	MaxStorageNodes uint32 `json:"maxStorageNodes"`
	// MaxStorageNodesPerZone maximum nodes in every zone that will have
	// storage in the cluster
	MaxStorageNodesPerZone uint32 `json:"maxStorageNodesPerZone"`
}

// Geography is topology information for a node
type Geography struct {
	// Region region in which the node is placed
	Region string `json:"region"`
	// Zone zone in which the node is placed
	Zone string `json:"zone"`
	// Rack rack on which the node is placed
	Rack string `json:"rack"`
}

// StorageClusterStatus is the status of a storage cluster
type StorageClusterStatus struct {
	// ClusterName name of the storage cluster
	ClusterName string `json:"clusterName"`
	// ClusterUUID uuid for the storage cluster
	ClusterUUID string `json:"clusterUuid"`
	// CreatedAt timestamp at which the storage cluster was created
	CreatedAt *meta.Time `json:"createdAt"`
	// Status status of the storage cluster
	Status ClusterStatus `json:"status"`
	// Reason is human readable message indicating the status of the cluster
	Reason string `json:"reason,omitempty"`
	// NodeStatuses list of statuses for all the nodes in the storage cluster
	NodeStatuses []NodeStatus `json:nodes`
}

// ClusterStatus is the enum type for cluster statuses
type ClusterStatus string

// These are valid cluster statuses.
const (
	// ClusterOK means the cluster is up and healthy
	ClusterOk ClusterStatus = "Ok"
	// ClusterOffline means the cluster is offline
	ClusterOffline ClusterStatus = "Offline"
	// ClusterNotInQuorum means the cluster is out of quorum
	ClusterNotInQuorum ClusterStatus = "NotInQuorum"
	// ClusterUnknown means the cluser status is not known
	ClusterUnknown ClusterStatus = "Unknown"
)

// NodeStatus status of the storage cluster node
type NodeStatus struct {
	// NodeName name of the node
	NodeName string `json:"nodeName"`
	// NodeUUID uuid of the node
	NodeUUID string `json:"nodeUuid"`
	// Network details used by the storage driver
	Network NetworkStatus `json:"network"`
	// Geo topology information for a node
	Geo Geography `json:"geography"`
	// Conditions is an array of current node conditions
	Conditions []NodeCondition `json:"conditions,omitempty"`
}

// NetworkStatus network status of the node
type NetworkStatus struct {
	// DataIP is the IP address used by storage driver for data traffic
	DataIP string `json:"dataIP"`
	// MgmtIP is the IP address used by storage driver for management traffic
	MgmtIP string `json:"mgmtIP"`
}

// NodeCondition contains condition information for a node
type NodeCondition struct {
	// Type of the node condition
	Type NodeConditionType `json:"type"`
	// Status of the condition
	Status ConditionStatus `json:"status"`
	// Reason is human readable message indicating details about the condition status
	Reason string `json:"reason"`
}

// NodeConditionType is the enum type for different node conditions
type NodeConditionType string

// These are valid conditions of the storage node. They correspond to different
// components in the storage cluster node.
const (
	// NodeState is used for overall state of the node
	NodeState NodeConditionType = "NodeState"
	// StorageState is used for the state of storage in the node
	StorageState NodeConditionType = "StorageState"
)

// ConditionStatus is the enum type for node condition statuses
type ConditionStatus string

// These are valid statuses of different node conditions.
const (
	// NodeOnline means the node condition is online and healthy
	NodeOnline ConditionStatus = "Online"
	// NodeInit means the node condition is in intializing state
	NodeInit ConditionStatus = "Intializing"
	// NodeMaintenance means the node condition is in maintenance state
	NodeMaintenance ConditionStatus = "Maintenance"
	// NodeDecommissioned means the node condition is in decommissioned state
	NodeDecommissioned ConditionStatus = "Decommissioned"
	// NodeOffline means the node condition is in offline state
	NodeOffline ConditionStatus = "Offline"
	// NodeUnknown means the node condition is not known
	NodeUnknown ConditionStatus = "Unknown"
)
