package v1

import (
	"k8s.io/apimachinery/pkg/api/resource"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	// StorageNodeResourceName is name for "storagenode" resource
	StorageNodeResourceName = "storagenode"
	// StorageNodeResourcePlural is plural for "storagenode" resource
	StorageNodeResourcePlural = "storagenodes"
	// StorageNodeShortName is the shortname for "storagenode" resource
	StorageNodeShortName = "sn"
)

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// StorageNode represents a storage node
type StorageNode struct {
	meta.TypeMeta   `json:",inline"`
	meta.ObjectMeta `json:"metadata,omitempty"`
	Spec            StorageNodeSpec `json:"spec,omitempty"`
	Status          NodeStatus      `json:"status,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// StorageNodeList is a list of storage nodes
type StorageNodeList struct {
	meta.TypeMeta `json:",inline"`
	meta.ListMeta `json:"metadata,omitempty"`
	Items         []StorageNode `json:"items"`
}

// StorageNodeSpec is the spec used to define a storage node
type StorageNodeSpec struct {
	// Version of the storage driver on the node
	Version string `json:"version,omitempty"`
	// CloudStorage configuration specifying storage for the node in cloud environments
	CloudStorage StorageNodeCloudDriveConfigs `json:"cloudStorage,omitempty"`
}

// StorageNodeCloudDriveConfigs specifies storage for the node in cloud environments
type StorageNodeCloudDriveConfigs struct {
	// DriveConfigs list of cloud drive configs for the storage node
	DriveConfigs []StorageNodeCloudDriveConfig `json:"driveConfigs,omitempty"`
}

// StorageNodeCloudDriveConfig is a structure for storing a configuration for a single drive
type StorageNodeCloudDriveConfig struct {
	// Type of cloud storage
	Type string `json:"type,omitempty"`
	// Size of cloud storage
	SizeInGiB uint64 `json:"sizeInGiB,omitempty"`
	// IOPS provided by cloud storage
	IOPS uint64 `json:"iops,omitempty"`
	// Options are additional options to the storage
	Options map[string]string `json:"options,omitempty"`
}

// NodeStatus contains the status of the storage node
type NodeStatus struct {
	// NodeUID unique identifier for the node
	NodeUID string `json:"nodeUid,omitempty"`
	// Phase is the current status of the storage node
	Phase string `json:"phase,omitempty"`
	// Network details used by the storage driver
	Network NetworkStatus `json:"network,omitempty"`
	// Storage details used by the storage driver
	Storage StorageStatus `json:"storage,omitempty"`
	// Geo topology information for a node
	Geo Geography `json:"geography,omitempty"`
	// Conditions is an array of current node conditions
	Conditions []NodeCondition `json:"conditions,omitempty"`
}

// NetworkStatus network status of the storage node
type NetworkStatus struct {
	// DataIP is the IP address used by storage driver for data traffic
	DataIP string `json:"dataIP,omitempty"`
	// MgmtIP is the IP address used by storage driver for management traffic
	MgmtIP string `json:"mgmtIP,omitempty"`
}

// StorageStatus captures the storage status of the node
type StorageStatus struct {
	// TotalSize is the cumulative total size of all storage pools on the node
	TotalSize resource.Quantity `json:"totalSize,omitempty"`
	// UsedSize is the cumulative used size of all storage pools on the node
	UsedSize resource.Quantity `json:"usedSize,omitempty"`
}

// NodeCondition contains condition information for a storage node
type NodeCondition struct {
	// Type of the node condition
	Type NodeConditionType `json:"type,omitempty"`
	// Status of the condition
	Status NodeConditionStatus `json:"status,omitempty"`
	// LastTransitionTime the condition transitioned from one status to another
	LastTransitionTime meta.Time `json:"lastTransitionTime,omitempty"`
	// Reason is unique one-word, CamelCase reason for the condition's last transition
	Reason string `json:"reason,omitempty"`
	// Message is human readable message indicating details about the last transition
	Message string `json:"message,omitempty"`
}

// NodeConditionType is the enum type for different node conditions
type NodeConditionType string

// These are valid conditions of the storage node. They correspond to different
// components in the storage cluster node.
const (
	// NodeInitCondition is used for initialization state of the node
	NodeInitCondition NodeConditionType = "NodeInit"
	// NodeStateCondition is used for overall state of the node
	NodeStateCondition NodeConditionType = "NodeState"
	// NodeKVDBCondition is used for the KVDB condition on the node if it is part of an internal KVDB cluster
	NodeKVDBCondition NodeConditionType = "NodeKVDB"
)

// NodeConditionStatus is the enum type for node condition statuses
type NodeConditionStatus string

// These are valid statuses of different node conditions.
const (
	// NodeSucceededStatus means the node condition status is succeeded
	NodeSucceededStatus NodeConditionStatus = "Succeeded"
	// NodeFailedStatus means the node condition status is failed
	NodeFailedStatus NodeConditionStatus = "Failed"
	// NodeOnlineStatus means the node condition is online and healthy
	NodeOnlineStatus NodeConditionStatus = "Online"
	// NodeInitStatus means the node condition is in initializing state
	NodeInitStatus NodeConditionStatus = "Initializing"
	// NodeUpdateStatus means the node condition is in updating state
	NodeUpdateStatus NodeConditionStatus = "Updating"
	// NodeNotInQuorumStatus means the node is not in quorum
	NodeNotInQuorumStatus NodeConditionStatus = "NotInQuorum"
	// NodeMaintenanceStatus means the node condition is in maintenance state
	NodeMaintenanceStatus NodeConditionStatus = "Maintenance"
	// NodeDecommissionedStatus means the node condition is in decommissioned state
	NodeDecommissionedStatus NodeConditionStatus = "Decommissioned"
	// NodeDegradedStatus means the node condition is in degraded state
	NodeDegradedStatus NodeConditionStatus = "Degraded"
	// NodeOfflineStatus means the node condition is in offline state
	NodeOfflineStatus NodeConditionStatus = "Offline"
	// NodeUnknownStatus means the node condition is not known
	NodeUnknownStatus NodeConditionStatus = "Unknown"
)

func init() {
	SchemeBuilder.Register(&StorageNode{}, &StorageNodeList{})
}
