package v1

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

// Metadata contains metadata of StorageCluster components
type Metadata struct {
	// Annotations that will be passed to different StorageCluster components
	Annotations map[string]map[string]string `json:"annotations,omitempty"`
	// Labels that will be passed to different StorageCluster components
	Labels map[string]map[string]string `json:"labels,omitempty"`
}

// StorageClusterSpec is the spec used to define a storage cluster
type StorageClusterSpec struct {
	// Metadata contains metadata of StorageCluster components
	Metadata *Metadata `json:"metadata,omitempty"`
	// An update strategy to replace existing StorageCluster pods with new pods.
	// Default strategy is RollingUpdate
	UpdateStrategy StorageClusterUpdateStrategy `json:"updateStrategy,omitempty"`
	// A delete strategy to uninstall and wipe an existing StorageCluster
	DeleteStrategy *StorageClusterDeleteStrategy `json:"deleteStrategy,omitempty"`
	// AutoUpdateComponents determines how components are to be updated automatically
	AutoUpdateComponents *AutoUpdateComponentStrategyType `json:"autoUpdateComponents,omitempty"`
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
	// This is a hack to stop GetImageURN from swallowing part of the image tag.
	// Without it having a customImageRegistry with a / in it means that an image
	// `portworx/oci-monitor` becomes just `oci-monitor`
	PreserveFullCustomImageRegistry bool `json:"preserveFullCustomImageRegistry,omitempty"`
	// Kvdb is the information of kvdb that storage driver uses
	Kvdb *KvdbSpec `json:"kvdb,omitempty"`
	// CloudStorage details of storage in cloud environment.
	CloudStorage *CloudStorageSpec `json:"cloudStorage,omitempty"`
	// SecretsProvider is the name of secret provider that driver will connect to
	SecretsProvider *string `json:"secretsProvider,omitempty"`
	// StartPort is the starting port in the range of ports used by the cluster
	StartPort *uint32 `json:"startPort,omitempty"`
	// FeatureGates are a set of key-value pairs that describe what experimental
	// features need to be enabled
	FeatureGates map[string]string `json:"featureGates,omitempty"`
	// CommonConfig contains specifications for storage, network, environment
	// variables, etc for all the nodes in the cluster. These config options
	// can be overriden using the CommonConfig in NodeSpec.
	CommonConfig
	// UserInterface contains details of a user interface for the storage driver
	UserInterface *UserInterfaceSpec `json:"userInterface,omitempty"`
	// PxRepo contains configuration for apt repository. Portworx uses it to install dependency modules.
	PxRepo *PxRepoSpec `json:"pxRepo,omitempty"`
	// Stork contains STORK related parameters. For more information about STORK,
	// check https://github.com/libopenstorage/stork
	Stork *StorkSpec `json:"stork,omitempty"`
	// Autopilot contains details for the autopilot component if running external
	// to the storage driver. The autopilot component could augment the storage
	// driver to take intelligent actions based on the current state of the cluster.
	Autopilot *AutopilotSpec `json:"autopilot,omitempty"`
	// Monitoring contains monitoring configuration for the storage cluster.
	Monitoring *MonitoringSpec `json:"monitoring,omitempty"`
	// Security configurations for setting up an auth enabled or disabled cluster
	Security *SecuritySpec `json:"security,omitempty"`
	// Volumes extra list of volumes for the storage driver pod
	Volumes []VolumeSpec `json:"volumes,omitempty"`
	// Nodes node level configurations that will override the ones at cluster
	// level. These configurations can be grouped based on label selectors.
	Nodes []NodeSpec `json:"nodes,omitempty"`
	// Resource requirements for portworx container in a storage cluster pod, e.g. CPU and memory requests or limits
	Resources *v1.ResourceRequirements `json:"resources,omitempty"`
	// CSI configurations for setting up CSI
	CSI *CSISpec `json:"csi,omitempty"`
}

// VolumeSpec describes a volume that needs to be mounted inside a container
type VolumeSpec struct {
	// Volume's name. Must be a DNS_LABEL and unique within the pod.
	// More info: https://kubernetes.io/docs/concepts/overview/working-with-objects/names/#names
	Name string `json:"name,omitempty"`
	// VolumeSource represents the location and type of the mounted volume.
	// If not specified, the Volume is implied to be an EmptyDir.
	// This implied behavior is deprecated and will be removed in a future version.
	v1.VolumeSource `json:",inline"`
	// Mounted read-only if true, read-write otherwise (false or unspecified).
	// Defaults to false.
	ReadOnly bool `json:"readOnly,omitempty"`
	// Path within the container at which the volume should be mounted. Must
	// not contain ':'.
	MountPath string `json:"mountPath,omitempty"`
	// MountPropagation determines how mounts are propagated from the host
	// to container and the other way around.
	// When not set, MountPropagationNone is used.
	MountPropagation *v1.MountPropagationMode `json:"mountPropagation,omitempty"`
}

// TLSSpec is the spec used to define TLS configuration for a cluster
type TLSSpec struct {
	// Defaults to parent (i.e. if missing, takes value from spec.security.enabled )
	Enabled *bool `json:"enabled,omitempty"`
	// RootCA defines the location of the Root CA certificate needed to enable TLS
	RootCA *CertLocation `json:"rootCA,omitempty"`
	// ServerCert defines the location of the Server certificate (public key) certificate needed to enable TLS
	ServerCert *CertLocation `json:"serverCert,omitempty"`
	// ServerKey defines the location of the Server key (private key) needed to enable TLS
	ServerKey *CertLocation `json:"serverKey,omitempty"`
}

// CertLocation specifies where portworx should pick up the certificate.
// Certificate can be in a file on a fixed location or in a secret
type CertLocation struct {
	// file name with path on the node for the cert file. Currently all files must be installed on a subfolder under /etc/pwx
	FileName *string `json:"filename,omitempty"`
	// reference to the k8s secret that holds the cert
	SecretRef *SecretRef `json:"secretRef,omitempty"`
}

// SecretRef specifies which k8s secret portworx should pick up the certificate.
type SecretRef struct {
	// name of the k8s secret. Secret must live in the same namespace as the StorageCluster custom resource
	SecretName string `json:"secretName,omitempty"`
	// the key that contains the cert.
	SecretKey string `json:"secretKey,omitempty"`
}

// NodeSpec is the spec used to define node level configuration. Values
// here will override the ones present at cluster-level for nodes matching
// the selector.
type NodeSpec struct {
	// Selector rest of the attributes are applied to a node that matches
	// the selector
	Selector NodeSelector `json:"selector,omitempty"`
	// CloudStorage details of storage in cloud environment for the nodegroup.
	// This will override the cluster-level cloud storage configuration.
	CloudStorage *CloudStorageNodeSpec `json:"cloudStorage,omitempty"`
	// CommonConfig contains storage, network and other configuration specific
	// to the group of nodes. This will override the cluster-level configuration.
	CommonConfig
}

// CSISpec is used to define the CSI configurations
type CSISpec struct {
	Enabled                   bool             `json:"enabled,omitempty"`
	InstallSnapshotController *bool            `json:"installSnapshotController,omitempty"`
	Topology                  *CSITopologySpec `json:"topology,omitempty"`
}

// CSITopologySpec is used to define the CSI topology configurations
type CSITopologySpec struct {
	Enabled bool `json:"enabled,omitempty"`
}

// SecuritySpec is used to define the security configuration for a cluster.
type SecuritySpec struct {
	Enabled bool      `json:"enabled,omitempty"`
	Auth    *AuthSpec `json:"auth,omitempty"`
	TLS     *TLSSpec  `json:"tls,omitempty"`
}

// AuthSpec lets the user define authorization (RBAC) configurations
// for creating a PX Security enabled cluster
type AuthSpec struct {
	// Defaults to parent (i.e. if missing, takes value from spec.security.enabled )
	Enabled     *bool            `json:"enabled,omitempty"`
	GuestAccess *GuestAccessType `json:"guestAccess,omitempty"`
	SelfSigned  *SelfSignedSpec  `json:"selfSigned,omitempty"`
}

// SelfSignedSpec defines a configuration for self signed authentication
type SelfSignedSpec struct {
	Issuer        *string `json:"issuer,omitempty"`
	TokenLifetime *string `json:"tokenLifetime,omitempty"`
	SharedSecret  *string `json:"sharedSecret,omitempty"`
}

// GuestAccessType lets the user choose the
// level of permissions the system.user has
type GuestAccessType string

const (
	// GuestRoleEnabled sets the guest role to the default volume lifecycle permissions
	GuestRoleEnabled GuestAccessType = "Enabled"
	// GuestRoleManaged lets the admin manage the guest role
	GuestRoleManaged GuestAccessType = "Managed"
	// GuestRoleDisabled disables all access for the guest role
	GuestRoleDisabled GuestAccessType = "Disabled"
)

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
	// Tolerations for the storage pods to tolerate node taints
	Tolerations []v1.Toleration `json:"tolerations,omitempty"`
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

// AutoUpdateComponentStrategyType is enum for auto updating
// storage cluster components
type AutoUpdateComponentStrategyType string

const (
	// AlwaysAutoUpdate always updates components if a new version is available
	AlwaysAutoUpdate AutoUpdateComponentStrategyType = "Always"
	// NeverAutoUpdate does not automatically update components
	NeverAutoUpdate AutoUpdateComponentStrategyType = "Never"
	// OnceAutoUpdate updates the version of a component only once.
	// Useful for manually updating component versions.
	OnceAutoUpdate AutoUpdateComponentStrategyType = "Once"
)

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
	// CacheDevices is list of cache devices
	CacheDevices *[]string `json:"cacheDevices,omitempty"`
	// JournalDevice device for journaling
	JournalDevice *string `json:"journalDevice,omitempty"`
	// SystemMdDevice device that will be used to store system metadata
	SystemMdDevice *string `json:"systemMetadataDevice,omitempty"`
	// KvdbDevice device for internal kvdb
	KvdbDevice *string `json:"kvdbDevice,omitempty"`
}

// CloudStorageCapacitySpec details the minimum and maximum amount of storage
// that will be provisioned in the cluster for a particular set of minimum IOPS.
type CloudStorageCapacitySpec struct {
	// MinIOPS minimum IOPS expected from the cloud drive
	MinIOPS uint64 `json:"minIOPS,omitempty"`
	// MinCapacityInGiB minimum capacity for this cloud device spec
	MinCapacityInGiB uint64 `json:"minCapacityInGiB,omitempty"`
	// MaxCapacityInGiB capacity for this cloud device spec should not go above this threshold
	MaxCapacityInGiB uint64 `json:"maxCapacityInGiB,omitempty"`
	// Options additional options required to provision the drive in cloud
	Options map[string]string `json:"options,omitempty"`
}

// CloudStorageSpec details of storage in cloud environment for entire cluster
type CloudStorageSpec struct {
	// Provider spec for the cloud provider, such as GKE, AWS etc
	Provider *string `json:"provider,omitempty"`
	// CloudStorageCommon common cloud storage configuration
	CloudStorageCommon
	// CapacitySpecs list of cluster wide storage types and their capacities.
	// A single capacity spec identifies a storage pool with a set of minimum
	// requested IOPS and size. Based on the cloud provider, the total storage
	// capacity will get divided amongst the nodes. The nodes bearing storage
	// themselves will get uniformly distributed across all the zones.
	// CapacitySpecs may replace DeviceSpecs in v2 version of StorageCluster.
	CapacitySpecs []CloudStorageCapacitySpec `json:"capacitySpecs,omitempty"`
	// MaxStorageNodes maximum nodes that will have storage in the cluster
	MaxStorageNodes *uint32 `json:"maxStorageNodes,omitempty"`
	// MaxStorageNodesPerZone maximum nodes in every zone that will have
	// storage in the cluster
	MaxStorageNodesPerZone *uint32 `json:"maxStorageNodesPerZone,omitempty"`
	// NodePoolLabel Kubernetes node label key with which nodes are grouped
	// into node pools for cloud storage distribution
	NodePoolLabel string `json:"nodePoolLabel,omitempty"`
}

// CloudStorageNodeSpec details of storage in cloud environment for node groups
type CloudStorageNodeSpec struct {
	// CloudStorageCommon common cloud storage configuration
	CloudStorageCommon
}

// CloudStorageCommon details of storage in cloud environment
type CloudStorageCommon struct {
	// DeviceSpecs list of storage device specs. A cloud storage device will
	// be created for every spec in the DeviceSpecs list.
	// DeviceSpecs may be removed from StorageCluster eventually,
	// in favor of CapacitySpecs at the cluster level.
	DeviceSpecs *[]string `json:"deviceSpecs,omitempty"`
	// JournalDeviceSpec spec for the journal device
	JournalDeviceSpec *string `json:"journalDeviceSpec,omitempty"`
	// SystemMdDeviceSpec spec for the metadata device
	SystemMdDeviceSpec *string `json:"systemMetadataDeviceSpec,omitempty"`
	// KvdbDeviceSpec spec for the internal kvdb device
	KvdbDeviceSpec *string `json:"kvdbDeviceSpec,omitempty"`
	// MaxStorageNodesPerZonePerNodeGroup maximum nodes per zone and per cloud node
	// group that will have storage in the cluster
	MaxStorageNodesPerZonePerNodeGroup *uint32 `json:"maxStorageNodesPerZonePerNodeGroup,omitempty"`
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
	// DEPRECATED: This is no longer needed to lock an image. The new behavior is to
	// use the image as it is, if present, else use a default image.
	LockImage bool `json:"lockImage,omitempty"`
	// Env is a list of environment variables used by UI component
	Env []v1.EnvVar `json:"env,omitempty"`
}

// PxRepoSpec contains apt repository configuration.
type PxRepoSpec struct {
	// Enabled decides whether repository pod needs to be enabled.
	Enabled bool `json:"enabled,omitempty"`
	// Image is docker image of repository container.
	Image string `json:"image,omitempty"`
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
	// DEPRECATED: This is no longer needed to lock an image. The new behavior is to
	// use the image as it is, if present, else use a default image.
	LockImage bool `json:"lockImage,omitempty"`
	// Args is a map of arguments given to STORK
	Args map[string]string `json:"args,omitempty"`
	// Env is a list of environment variables used by stork
	Env []v1.EnvVar `json:"env,omitempty"`
	// Volumes is a list of extra volumes to used by stork
	Volumes []VolumeSpec `json:"volumes,omitempty"`
	// HostNetwork if set, will use host's network for stork pods
	HostNetwork *bool `json:"hostNetwork,omitempty"`
}

// AutopilotSpec contains details of an autopilot component
type AutopilotSpec struct {
	// Enabled decides whether autopilot needs to be enabled
	Enabled bool `json:"enabled,omitempty"`
	// Image is docker image of the autopilot container
	Image string `json:"image,omitempty"`
	// LockImage is a boolean indicating if the autopilot image needs to be locked
	// to the given image. If the image is not locked, it can be updated by the
	// driver during upgrades.
	// DEPRECATED: This is no longer needed to lock an image. The new behavior is to
	// use the image as it is, if present, else use a default image.
	LockImage bool `json:"lockImage,omitempty"`
	// Providers is a list of input data providers for autopilot if it needs any
	Providers []DataProviderSpec `json:"providers,omitempty"`
	// Args is a map of arguments given to autopilot
	Args map[string]string `json:"args,omitempty"`
	// Env is a list of environment variables used by autopilot
	Env []v1.EnvVar `json:"env,omitempty"`
	// Volumes is a list of extra volumes to used by autopilot
	Volumes []VolumeSpec `json:"volumes,omitempty"`
}

// DataProviderSpec contains the details for data providers for components like autopilot
type DataProviderSpec struct {
	// Name is the unique name for the provider
	Name string `json:"name,omitempty"`
	// Type is the type of data provider. For instance, prometheus
	Type string `json:"type,omitempty"`
	// Params is a list of key-value params for the provider
	Params map[string]string `json:"params,omitempty"`
}

// MonitoringSpec contains monitoring configuration for the storage cluster.
type MonitoringSpec struct {
	// DEPRECATED: EnableMetrics this exposes the storage cluster metrics to external
	// monitoring solutions like Prometheus.
	EnableMetrics *bool `json:"enableMetrics,omitempty"`
	// Prometheus contains the details of the Prometheus stack deployed to monitor
	// metrics from the storage cluster.
	Prometheus *PrometheusSpec `json:"prometheus,omitempty"`
	// Telemetry contains custom configuration for storage driver telemetry. This is optional.
	Telemetry *TelemetrySpec `json:"telemetry,omitempty"`
}

// TelemetrySpec contains details of a telemetry component
type TelemetrySpec struct {
	// Enabled decides whether telemetry needs to be enabled
	Enabled bool `json:"enabled,omitempty"`
	// Image is docker image of the telemetry container
	Image string `json:"image,omitempty"`
	// LogUploaderImage is docker image of the log-upload-service container.
	LogUploaderImage string `json:"logUploaderImage,omitempty"`
}

// PrometheusSpec contains configuration of Prometheus stack
type PrometheusSpec struct {
	// ExportMetrics exports the storage cluster metrics to Prometheus
	ExportMetrics bool `json:"exportMetrics,omitempty"`
	// Enabled decides whether prometheus stack needs to be deployed
	Enabled bool `json:"enabled,omitempty"`
	// RemoteWriteEndpoint specifies the remote write endpoint
	RemoteWriteEndpoint string `json:"remoteWriteEndpoint,omitempty"`
	// AlertManager spec for configuring alert manager
	AlertManager *AlertManagerSpec `json:"alertManager,omitempty"`
}

// AlertManagerSpec contains configuration of AlertManager
type AlertManagerSpec struct {
	// Enabled decides whether alert manager needs to be deployed
	Enabled bool `json:"enabled,omitempty"`
}

// StorageClusterStatus is the status of a storage cluster
type StorageClusterStatus struct {
	// ClusterName name of the storage cluster
	ClusterName string `json:"clusterName,omitempty"`
	// ClusterUID unique ID for the storage cluster
	ClusterUID string `json:"clusterUid,omitempty"`
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
	// Version version of the storage driver image
	Version string `json:"version,omitempty"`
	// DesiredImages represents all the desired images of various components
	DesiredImages *ComponentImages `json:"desiredImages,omitempty"`
}

// ComponentImages is a collection of all the images managed by the operator
type ComponentImages struct {
	Stork                      string `json:"stork,omitempty"`
	UserInterface              string `json:"userInterface,omitempty"`
	Autopilot                  string `json:"autopilot,omitempty"`
	CSINodeDriverRegistrar     string `json:"csiNodeDriverRegistrar,omitempty"`
	CSIDriverRegistrar         string `json:"csiDriverRegistrar,omitempty"`
	CSIProvisioner             string `json:"csiProvisioner,omitempty"`
	CSIAttacher                string `json:"csiAttacher,omitempty"`
	CSIResizer                 string `json:"csiResizer,omitempty"`
	CSISnapshotter             string `json:"csiSnapshotter,omitempty"`
	CSISnapshotController      string `json:"csiSnapshotController,omitempty"`
	CSIHealthMonitorController string `json:"csiHealthMonitorController,omitempty"`
	PrometheusOperator         string `json:"prometheusOperator,omitempty"`
	PrometheusConfigMapReload  string `json:"prometheusConfigMapReload,omitempty"`
	PrometheusConfigReloader   string `json:"prometheusConfigReloader,omitempty"`
	Prometheus                 string `json:"prometheus,omitempty"`
	AlertManager               string `json:"alertManager,omitempty"`
	Telemetry                  string `json:"telemetry,omitempty"`
	MetricsCollector           string `json:"metricsCollector,omitempty"`
	MetricsCollectorProxy      string `json:"metricsCollectorProxy,omitempty"` // TODO: use TelemetryProxy only
	LogUploader                string `json:"logUploader,omitempty"`
	TelemetryProxy             string `json:"telemetryProxy,omitempty"`
	PxRepo                     string `json:"pxRepo,omitempty"`
}

// Storage represents cluster storage details
type Storage struct {
	// StorageNodesPerZone describes the amount of instances per zone
	StorageNodesPerZone uint64 `json:"storageNodesPerZone,omitempty"`
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
	// ClusterUnknown means the cluster status is not known
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
