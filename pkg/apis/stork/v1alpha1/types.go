package v1alpha1

import (
	crdv1 "github.com/kubernetes-incubator/external-storage/snapshot/pkg/apis/crd/v1"
	corev1 "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/clientcmd/api"
)

// RuleActionType is a type for actions that are supported in a stork rule
type RuleActionType string

const (
	// RuleActionCommand is a command action
	RuleActionCommand RuleActionType = "command"
	// ClusterPairResourceName is name for "clusterpair" resource
	ClusterPairResourceName = "clusterpair"
	// ClusterPairResourcePlural is plural for "clusterpair" resource
	ClusterPairResourcePlural = "clusterpairs"
	// MigrationResourceName is name for "migration" resource
	MigrationResourceName = "migration"
	// MigrationResourcePlural is plural for "migration" resource
	MigrationResourcePlural = "migrations"
	// GroupVolumeSnapshotResourceName is name for "groupvolumesnapshot" resource
	GroupVolumeSnapshotResourceName = "groupvolumesnapshot"
	// GroupVolumeSnapshotResourcePlural is plural for the "groupvolumesnapshot" resource
	GroupVolumeSnapshotResourcePlural = "groupvolumesnapshots"
	// StorageClusterResourceName is name for "storagecluster" resource
	StorageClusterResourceName = "storagecluster"
	// StorageClusterResourcePlural is plural for "storagecluster" resource
	StorageClusterResourcePlural = "storageclusters"
	// StorageClusterShortName is the shortname for "storagecluster" resource
	StorageClusterShortName = "stc"
)

// +genclient
// +genclient:noStatus
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// Rule denotes an object to declare a rule that performs actions on pods
type Rule struct {
	meta.TypeMeta   `json:",inline"`
	meta.ObjectMeta `json:"metadata,omitempty"`
	Rules           []RuleItem `json:"rules"`
}

// RuleItem represents one items in a stork rule spec
type RuleItem struct {
	// PodSelector is a map of key value pairs that are used to select the pods using their labels
	PodSelector map[string]string `json:"podSelector"`
	// Actions are actions to be performed on the pods selected using the selector
	Actions []RuleAction `json:"actions"`
}

// RuleAction represents an action in a stork rule item
type RuleAction struct {
	// Type is a type of the stork rule action
	Type RuleActionType `json:"type"`
	// Background indicates that the action needs to be performed in the background
	// +optional
	Background bool `json:"background,omitempty"`
	// RunInSinglePod indicates that the action needs to be performed in a single pod
	//                from the list of pods that match the selector
	// +optional
	RunInSinglePod bool `json:"runInSinglePod,omitempty"`
	// Value is the actual action value for e.g the command to run
	Value string `json:"value"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// RuleList is a list of stork rules
type RuleList struct {
	meta.TypeMeta `json:",inline"`
	meta.ListMeta `json:"metadata,omitempty"`

	Items []Rule `json:"items"`
}

// ClusterPairSpec is the spec to create the cluster pair
type ClusterPairSpec struct {
	Config  api.Config        `json:"config"`
	Options map[string]string `json:"options"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ClusterPair represents pairing with other clusters
type ClusterPair struct {
	meta.TypeMeta   `json:",inline"`
	meta.ObjectMeta `json:"metadata,omitempty"`
	Spec            ClusterPairSpec   `json:"spec"`
	Status          ClusterPairStatus `json:"status,omitempty"`
}

// ClusterPairStatusType is the status of the pair
type ClusterPairStatusType string

const (
	// ClusterPairStatusInitial is the initial state when pairing is created
	ClusterPairStatusInitial ClusterPairStatusType = ""
	// ClusterPairStatusPending for when pairing is still pending
	ClusterPairStatusPending ClusterPairStatusType = "Pending"
	// ClusterPairStatusReady for when pair is ready
	ClusterPairStatusReady ClusterPairStatusType = "Ready"
	// ClusterPairStatusError for when pairing is in error state
	ClusterPairStatusError ClusterPairStatusType = "Error"
	// ClusterPairStatusDegraded for when pairing is degraded
	ClusterPairStatusDegraded ClusterPairStatusType = "Degraded"
	// ClusterPairStatusDeleting for when pairing is being deleted
	ClusterPairStatusDeleting ClusterPairStatusType = "Deleting"
)

// ClusterPairStatus is the status of the cluster pair
type ClusterPairStatus struct {
	// Status of the pairing with the scheduler
	// +optional
	SchedulerStatus ClusterPairStatusType `json:"schedulerStatus"`
	// Status of pairing with the storage driver
	// +optional
	StorageStatus ClusterPairStatusType `json:"storageStatus"`
	// ID of the remote storage which is paired
	// +optional
	RemoteStorageID string `json:"remoteStorageId"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ClusterPairList is a list of cluster pairs
type ClusterPairList struct {
	meta.TypeMeta `json:",inline"`
	meta.ListMeta `json:"metadata,omitempty"`

	Items []ClusterPair `json:"items"`
}

// MigrationSpec is the spec used to migrate apps between clusterpairs
type MigrationSpec struct {
	ClusterPair       string            `json:"clusterPair"`
	Namespaces        []string          `json:"namespaces"`
	IncludeResources  bool              `json:"includeResources"`
	StartApplications bool              `json:"startApplications"`
	Selectors         map[string]string `json:"selectors"`
	PreExecRule       string            `json:"preExecRule"`
	PostExecRule      string            `json:"postExecRule"`
}

// MigrationStatus is the status of a migration operation
type MigrationStatus struct {
	Stage     MigrationStageType  `json:"stage"`
	Status    MigrationStatusType `json:"status"`
	Resources []*ResourceInfo     `json:"resources"`
	Volumes   []*VolumeInfo       `json:"volumes"`
}

// ResourceInfo is the info for the migration of a resource
type ResourceInfo struct {
	Name                  string `json:"name"`
	Namespace             string `json:"namespace"`
	meta.GroupVersionKind `json:",inline"`
	Status                MigrationStatusType `json:"status"`
	Reason                string              `json:"reason"`
}

// VolumeInfo is the info for the migration of a volume
type VolumeInfo struct {
	PersistentVolumeClaim string              `json:"persistentVolumeClaim"`
	Namespace             string              `json:"namespace"`
	Volume                string              `json:"volume"`
	Status                MigrationStatusType `json:"status"`
	Reason                string              `json:"reason"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// Migration represents migration status
type Migration struct {
	meta.TypeMeta   `json:",inline"`
	meta.ObjectMeta `json:"metadata,omitempty"`
	Spec            MigrationSpec   `json:"spec"`
	Status          MigrationStatus `json:"status"`
}

// MigrationStatusType is the status of the migration
type MigrationStatusType string

const (
	// MigrationStatusInitial is the initial state when migration is created
	MigrationStatusInitial MigrationStatusType = ""
	// MigrationStatusPending for when migration is still pending
	MigrationStatusPending MigrationStatusType = "Pending"
	// MigrationStatusCaptured for when migration specs have been captured
	MigrationStatusCaptured MigrationStatusType = "Captured"
	// MigrationStatusInProgress for when migration is in progress
	MigrationStatusInProgress MigrationStatusType = "InProgress"
	// MigrationStatusFailed for when migration has failed
	MigrationStatusFailed MigrationStatusType = "Failed"
	// MigrationStatusPartialSuccess for when migration was partially successful
	MigrationStatusPartialSuccess MigrationStatusType = "PartialSuccess"
	// MigrationStatusSuccessful for when migration has completed successfully
	MigrationStatusSuccessful MigrationStatusType = "Successful"
)

// MigrationStageType is the stage of the migration
type MigrationStageType string

const (
	// MigrationStageInitial for when migration is created
	MigrationStageInitial MigrationStageType = ""
	// MigrationStagePreExecRule for when the PreExecRule is being executed
	MigrationStagePreExecRule MigrationStageType = "PreExecRule"
	// MigrationStagePostExecRule for when the PostExecRule is being executed
	MigrationStagePostExecRule MigrationStageType = "PostExecRule"
	// MigrationStageVolumes for when volumes are being migrated
	MigrationStageVolumes MigrationStageType = "Volumes"
	// MigrationStageApplications for when applications are being migrated
	MigrationStageApplications MigrationStageType = "Applications"
	// MigrationStageFinal is the final stage for migration
	MigrationStageFinal MigrationStageType = "Final"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// MigrationList is a list of Migrations
type MigrationList struct {
	meta.TypeMeta `json:",inline"`
	meta.ListMeta `json:"metadata,omitempty"`

	Items []Migration `json:"items"`
}

// +genclient
// +genclient:noStatus
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// GroupVolumeSnapshot represents a group snapshot
type GroupVolumeSnapshot struct {
	meta.TypeMeta   `json:",inline"`
	meta.ObjectMeta `json:"metadata,omitempty"`
	Spec            GroupVolumeSnapshotSpec   `json:"spec"`
	Status          GroupVolumeSnapshotStatus `json:"status"`
}

// GroupVolumeSnapshotSpec represents the spec for a group snapshot
type GroupVolumeSnapshotSpec struct {
	// PreExecRule is the name of rule applied before taking the snapshot. The rule needs to be
	// in the same namespace as the group volumesnapshot
	PreExecRule string `json:"preExecRule"`
	// PreExecRule is the name of rule applied after taking the snapshot. The rule needs to be
	// in the same namespace as the group volumesnapshot
	PostExecRule string `json:"postExecRule"`
	// PVCSelector selects the PVCs that are part of the group snapshot
	PVCSelector PVCSelectorSpec `json:"pvcSelector"`
	// Options are pass-through parameters that are passed to the driver handling the group snapshot
	Options map[string]string `json:"options"`
}

// PVCSelectorSpec is the spec to select the PVCs for group snapshot
type PVCSelectorSpec struct {
	meta.LabelSelector
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// GroupVolumeSnapshotList is a list of group volume snapshots
type GroupVolumeSnapshotList struct {
	meta.TypeMeta `json:",inline"`
	meta.ListMeta `json:"metadata,omitempty"`

	Items []GroupVolumeSnapshot `json:"items"`
}

// GroupVolumeSnapshotStatus is status for the group snapshot
type GroupVolumeSnapshotStatus struct {
	Stage           GroupVolumeSnapshotStageType  `json:"stage"`
	Status          GroupVolumeSnapshotStatusType `json:"status"`
	VolumeSnapshots []*VolumeSnapshotStatus       `json:"volumeSnapshots"`
}

// VolumeSnapshotStatus captures the status of a volume snapshot operation
type VolumeSnapshotStatus struct {
	VolumeSnapshotName string
	TaskID             string
	ParentVolumeID     string
	DataSource         *crdv1.VolumeSnapshotDataSource
	Conditions         []crdv1.VolumeSnapshotCondition
}

// GroupVolumeSnapshotStatusType is types of statuses of a group snapshot operation
type GroupVolumeSnapshotStatusType string

const (
	// GroupSnapshotInitial is when the group snapshot is created and no action has yet been performed
	GroupSnapshotInitial GroupVolumeSnapshotStatusType = ""
	// GroupSnapshotPending is when the group snapshot is in pending state waiting for another event
	GroupSnapshotPending GroupVolumeSnapshotStatusType = "Pending"
	// GroupSnapshotInProgress is when the group snapshot is in progress
	GroupSnapshotInProgress GroupVolumeSnapshotStatusType = "InProgress"
	// GroupSnapshotFailed is when the group snapshot has failed
	GroupSnapshotFailed GroupVolumeSnapshotStatusType = "Failed"
	// GroupSnapshotSuccessful is when the group snapshot has succeeded
	GroupSnapshotSuccessful GroupVolumeSnapshotStatusType = "Successful"
)

// GroupVolumeSnapshotStageType is the stage of the group snapshot
type GroupVolumeSnapshotStageType string

const (
	// GroupSnapshotStageInitial is when the group snapshot is just created
	GroupSnapshotStageInitial GroupVolumeSnapshotStageType = ""
	// GroupSnapshotStagePreChecks is when the group snapshot is going through prechecks
	GroupSnapshotStagePreChecks GroupVolumeSnapshotStageType = "PreChecks"
	// GroupSnapshotStagePreSnapshot is when the pre-snapshot rule is executing for the group snapshot
	GroupSnapshotStagePreSnapshot GroupVolumeSnapshotStageType = "PreSnapshot"
	// GroupSnapshotStageSnapshot is when the snapshots are being taken for the group snapshot
	GroupSnapshotStageSnapshot GroupVolumeSnapshotStageType = "Snapshot"
	// GroupSnapshotStagePostSnapshot is when the post-snapshot rule is executing for the group snapshot
	GroupSnapshotStagePostSnapshot GroupVolumeSnapshotStageType = "PostSnapshot"
	// GroupSnapshotStageFinal is when all stages are done for the group snapshot
	GroupSnapshotStageFinal GroupVolumeSnapshotStageType = "Final"
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
