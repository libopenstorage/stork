package volume

import (
	"fmt"
	"time"

	snapv1 "github.com/kubernetes-incubator/external-storage/snapshot/pkg/apis/crd/v1"
	apapi "github.com/libopenstorage/autopilot-api/pkg/apis/autopilot/v1alpha1"
	"github.com/libopenstorage/openstorage/api"
	pxapi "github.com/libopenstorage/operator/api/px"
	v1 "github.com/libopenstorage/operator/pkg/apis/core/v1"
	driver_api "github.com/portworx/torpedo/drivers/api"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/pkg/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Volume is a generic struct encapsulating volumes in the cluster
type Volume struct {
	ID            string
	Name          string
	Namespace     string
	Annotations   map[string]string
	Labels        map[string]string
	Size          uint64
	RequestedSize uint64
	Shared        bool
	Raw           bool
}

// Snapshot is a generic struct encapsulating snapshots in the cluster
type Snapshot struct {
	ID        string
	Name      string
	Namespace string
}

// Image is a generic struct for encapsulating driver images/version
type Image struct {
	Type    string
	Version string
}

type DriveSet struct {
	Configs           map[string]DriveConfig
	NodeID            string
	SchedulerNodeName string
	InstanceID        string
	Zone              string
	State             string
	Labels            map[string]string
}

type DriveConfig struct {
	Type   string
	Size   uint
	ID     string
	Path   string
	IOPS   int
	PXType string
	State  string
	Labels map[string]string
}

type DiskResource struct {
	PoolId    string
	Device    string
	MediaType string
	SizeInGib uint64
}

// Options to pass to APIs
type Options struct {
	ValidateReplicationUpdateTimeout time.Duration
}

// Driver defines an external volume driver interface that must be implemented
// by any external storage provider that wants to qualify their product with
// Torpedo.  The functions defined here are meant to be destructive and illustrative
// of failure scenarios that can happen with an external storage provider.
type Driver interface {
	// Init initializes the volume driver under the given scheduler
	Init(sched string, nodeDriver string, token string, storageProvisioner string, csiGenericConfigMap string) error

	// String returns the string name of this driver.
	String() string

	// GetVolumeDriverNamespace returns the namespace of this driver.
	GetVolumeDriverNamespace() (string, error)

	// ListAllVolumes returns volumeIDs of all volumes present in the cluster
	ListAllVolumes() ([]string, error)

	// CreateVolume creates a volume with the default setting
	// returns volume_id of the new volume
	CreateVolume(volName string, size uint64, haLevel int64) (string, error)

	// CreateVolumeUsingPxctlCmd resizes a pool of a given UUID using CLI command
	CreateVolumeUsingPxctlCmd(n node.Node, volName string, size uint64, haLevel int64) error

	// ResizeVolume resizes Volume to specific size provided
	ResizeVolume(volName string, size uint64) error

	// CreateVolumeUsingRequest creates a volume with the given volume request
	// returns volume_id of the new volume
	CreateVolumeUsingRequest(request *api.SdkVolumeCreateRequest) (string, error)

	// CloneVolume creates a clone of the volume whose volumeName is passed as arg.
	// returns volume_id of the cloned volume and error if there is any
	CloneVolume(volumeID string) (string, error)

	// AttachVolume attaches a volume with the default setting
	// returns the device path
	AttachVolume(volumeID string) (string, error)

	// DetachVolume detaches the volume for given volumeID
	DetachVolume(volumeID string) error

	// DeleteVolume deletes the volume for given volumeID
	DeleteVolume(volumeID string) error

	// InspectVolume inspects the volume with the given name
	InspectVolume(name string) (*api.Volume, error)

	// CleanupVolume forcefully unmounts/detaches and deletes a storage volume.
	// This is only called by Torpedo during cleanup operations, it is not
	// used during orchestration simulations.
	CleanupVolume(name string) error

	// CreateSnapshot creates snapshot for a given volume ID with a given snapshot name
	CreateSnapshot(volumeID string, snapName string) (*api.SdkVolumeSnapshotCreateResponse, error)

	// ValidateCreateVolume validates whether a volume has been created properly.
	// params are the custom volume options passed when creating the volume.
	ValidateCreateVolume(name string, params map[string]string) error

	// ValidateCreateSnapshot validates whether a snapshot has been created properly.
	// params are the custom volume options passed
	ValidateCreateSnapshot(name string, params map[string]string) (string, error)

	// ValidateCreateSnapshotUsingPxctl validates whether a snapshot has been created properly using pxctl.
	ValidateCreateSnapshotUsingPxctl(name string) (string, error)

	// GetCloudsnaps returns cloudsnap backups
	// params are the custom backup options passed
	GetCloudsnaps(name string, params map[string]string) ([]*api.SdkCloudBackupInfo, error)

	// DeleteAllCloudsnaps deletes all  cloudsnap backups
	// params are the custom backup options passed
	DeleteAllCloudsnaps(name, sourceVolumeID string, params map[string]string) error

	// GetCloudsnapsOfGivenVolume returns cloudsnap backups of given volume
	// params are the custom backup options passed
	GetCloudsnapsOfGivenVolume(volumeName string, sourceVolumeID string, params map[string]string) ([]*api.SdkCloudBackupInfo, error)

	// ValidateCreateCloudsnap validates whether a cloudsnap backup can be created properly(or errored expectely)
	// params are the custom backup options passed
	ValidateCreateCloudsnap(name string, params map[string]string) error

	// ValidateCreateCloudsnapUsingPxctl validates whether a cloudsnap backup can be created properly(or errored expectely) using pxctl
	ValidateCreateCloudsnapUsingPxctl(name string) error

	// ValidateCreateGroupSnapshotUsingPxctl validates whether a groupsnap backup can be created properly (or errored expectedly) using pxctl
	ValidateCreateGroupSnapshotUsingPxctl() error

	// ValidateGetByteUsedForVolume validates returning volume statstic succesfully
	ValidateGetByteUsedForVolume(volumeName string, params map[string]string) (uint64, error)

	// ValidatePureVolumesNoReplicaSets validates pure volumes has no replicaset
	ValidatePureVolumesNoReplicaSets(volumeName string, params map[string]string) error

	// InitializePureLocalVolumePaths sets the baseline for how many Pure devices are already attached to the node
	InitializePureLocalVolumePaths() error

	// ValidatePureLocalVolumePaths checks that the given volumes all have the proper local paths present, *and that no other unexpected ones are present*
	ValidatePureLocalVolumePaths() error

	// ValidateVolumeInPxctlList validates that the given volume appears in the output of `pxctl v l`
	ValidateVolumeInPxctlList(name string) error

	// ValidateUpdateVolume validates if volume changes has been applied
	ValidateUpdateVolume(vol *Volume, params map[string]string) error

	// SetIoBandwidth validates if volume changes has been applied
	SetIoBandwidth(vol *Volume, readBandwidthMBps uint32, writeBandwidthMBps uint32) error

	// UpdateVolumeSpec updates given volume with provided spec
	UpdateVolumeSpec(vol *Volume, volumeSpec *api.VolumeSpecUpdate) error

	// ValidateDeleteVolume validates whether a volume is cleanly removed from the volume driver
	ValidateDeleteVolume(vol *Volume) error

	// ValidateVolumeCleanup checks if the necessary cleanup has happened for the volumes by this driver
	ValidateVolumeCleanup() error

	// ValidateVolumeSetup validates if the given volume is setup correctly in the cluster
	ValidateVolumeSetup(vol *Volume) error

	// StopDriver must cause the volume driver to exit on a given node. If force==true, the volume driver should get killed ungracefully
	StopDriver(nodes []node.Node, force bool, triggerOpts *driver_api.TriggerOptions) error

	// KillPXDaemon must kill px -daemon on a given node,the volume driver should get killed ungracefully
	KillPXDaemon(nodes []node.Node, triggerOpts *driver_api.TriggerOptions) error

	// StartDriver must cause the volume driver to start on a given node.
	StartDriver(n node.Node) error

	// RestartDriver must cause the volume driver to get restarted on a given node.
	RestartDriver(n node.Node, triggerOpts *driver_api.TriggerOptions) error

	// WaitDriverUpOnNode must wait till the volume driver becomes usable on a given node
	WaitDriverUpOnNode(n node.Node, timeout time.Duration) error

	// WaitDriverDownOnNode must wait till the volume driver becomes unusable on a given node
	WaitDriverDownOnNode(n node.Node) error

	// GetNodeForVolume returns the node on which the volume is attached
	GetNodeForVolume(vol *Volume, timeout time.Duration, retryInterval time.Duration) (*node.Node, error)

	// GetNodeForBackup returns the node on which cloudsnap backup is started
	GetNodeForBackup(backupID string) (node.Node, error)

	// ExtractVolumeInfo extracts the volume params from the given string
	ExtractVolumeInfo(params string) (string, map[string]string, error)

	// UpgradeDriver upgrades the volume driver using the given endpointVersion
	UpgradeDriver(endpointVersion string) error

	// UpgradeStork upgrades the stork driver using the given endpointVersion
	UpgradeStork(endpointVersion string) error

	// RandomizeVolumeName randomizes the volume name from the given name
	RandomizeVolumeName(name string) string

	// InspectCurrentCluster inspects the current cluster
	InspectCurrentCluster() (*api.SdkClusterInspectCurrentResponse, error)

	// RecoverDriver will recover a volume driver from a failure/storage down state.
	// This could be used by a volume driver to recover itself from any underlying storage
	// failure.
	RecoverDriver(n node.Node) error

	// EnterMaintenance puts the given node in maintenance mode
	EnterMaintenance(n node.Node) error

	// ExitMaintenance exits the given node from maintenance mode
	ExitMaintenance(n node.Node) error

	// UpdatePoolIOPriority updates IO priority of the pool
	UpdatePoolIOPriority(n node.Node, poolUUID string, IOPriority string) error

	// RecoverPool will recover a pool from a failure/storage down state.
	// This could be used by a pool to recover itself from any underlying storage
	// failure.
	RecoverPool(n node.Node) error

	// EnterPoolMaintenance puts pools in the given node in maintenance mode
	EnterPoolMaintenance(n node.Node) error

	// ExitPoolMaintenance exits pools in the given node from maintenance mode
	ExitPoolMaintenance(n node.Node) error

	//GetNodePoolsStatus returns map of pool UUID and status
	GetNodePoolsStatus(n node.Node) (map[string]string, error)

	//GetNodePools returns latest map of pool UUID and id
	GetNodePools(n node.Node) (map[string]string, error)

	//DeletePool deletes the pool with given poolID
	DeletePool(n node.Node, poolID string, retry bool) error

	// GetDriverVersion will return the pxctl version from the node
	GetDriverVersion() (string, error)

	// GetDriverNode returns api.StorageNode
	GetDriverNode(*node.Node, ...api.OpenStorageNodeClient) (*api.StorageNode, error)

	// GetPDriverNodes returns current driver nodes in the cluster
	GetDriverNodes() ([]*api.StorageNode, error)

	//GetDriverVersionOnNode get PXVersion on the given node
	GetDriverVersionOnNode(n node.Node) (string, error)

	// RefreshDriverEndpoints refreshes volume driver endpoint
	RefreshDriverEndpoints() error

	// GetStorageDevices returns the list of storage devices used by the given node.
	GetStorageDevices(n node.Node) ([]string, error)

	GetDriveSet(n *node.Node) (*DriveSet, error)

	//IsDriverInstalled checks for driver to be installed on a node
	IsDriverInstalled(n node.Node) (bool, error)

	// GetReplicationFactor returns the current replication factor of the volume.
	GetReplicationFactor(vol *Volume) (int64, error)

	// SetReplicationFactor sets the volume's replication factor to the passed param rf and nodes.
	SetReplicationFactor(vol *Volume, rf int64, nodesToBeUpdated []string, poolsToBeUpdated []string, waitForUpdateToFinish bool, opts ...Options) error

	//WaitForReplicationToComplete waits for replication factor change to complete
	WaitForReplicationToComplete(vol *Volume, replFactor int64, replicationUpdateTimeout time.Duration) error

	// GetMaxReplicationFactor returns the max supported repl factor of a volume
	GetMaxReplicationFactor() int64

	// GetMinReplicationFactor returns the min supported repl factor of a volume
	GetMinReplicationFactor() int64

	// GetAggregationLevel returns the aggregation level for the given volume
	GetAggregationLevel(vol *Volume) (int64, error)

	// GetClusterPairingInfo returns cluster pairing information from remote cluster
	GetClusterPairingInfo(kubeConfigPath, token string, isPxLBService, reversePair bool) (map[string]string, error)

	// DecommissionNode decommissions the given node from the cluster
	DecommissionNode(n *node.Node) error

	// RecoverNode makes a given node back to normal
	RecoverNode(n *node.Node) error

	// RejoinNode rejoins a given node back to the cluster
	RejoinNode(n *node.Node) error

	// GetNodeStatus returns the status of a given node
	GetNodeStatus(n node.Node) (*api.Status, error)

	// GetReplicaSets returns the replica sets for a given volume
	GetReplicaSets(vol *Volume) ([]*api.ReplicaSet, error)

	// ValidateVolumeSnapshotRestore return nil if snapshot is restored successuflly to
	// given volumes
	ValidateVolumeSnapshotRestore(vol string, snapData *snapv1.VolumeSnapshotData, timeStart time.Time) error

	// CollectDiags collects live diags on a node
	CollectDiags(n node.Node, config *DiagRequestConfig, diagOps DiagOps) error

	// ValidateDiagsOnS3 validates the Diags or diags file collected on S3
	ValidateDiagsOnS3(n node.Node, diagsFile, pxDir string) error

	// ValidateStoragePools validates all the storage pools
	ValidateStoragePools() error

	// ValidateRebalanceJobs validates rebalance jobs
	ValidateRebalanceJobs() error

	// ResizeStoragePoolByPercentage resizes the given stroage pool by percentage
	ResizeStoragePoolByPercentage(string, api.SdkStoragePool_ResizeOperationType, uint64) error

	// IsStorageExpansionEnabled returns true if storage expansion enabled
	IsStorageExpansionEnabled() (bool, error)

	// IsPureVolume returns true if given volume belongs FA/FB DA volumes
	IsPureVolume(volume *Volume) (bool, error)

	// IsPureFileVolume returns true if given volume belongs to FBDA volumes
	IsPureFileVolume(volume *Volume) (bool, error)

	// GetProxySpecForAVolume returns the api.ProxySpec associated with the given volume
	GetProxySpecForAVolume(volume *Volume) (*api.ProxySpec, error)

	// EstimatePoolExpandSize calculates expected pool size based on autopilot rule
	EstimatePoolExpandSize(apRule apapi.AutopilotRule, pool node.StoragePool, node node.Node) (uint64, error)

	// EstimatePoolExpandSize calculates expected volume size based on autopilot rule, initial and workload sizes
	EstimateVolumeExpand(apRule apapi.AutopilotRule, initialSize, workloadSize uint64) (uint64, int, error)

	// GetLicenseSummary returns the activated license SKU and Features
	GetLicenseSummary() (LicenseSummary, error)

	//SetClusterOpts sets cluster options
	SetClusterOpts(n node.Node, clusterOpts map[string]string) error

	//GetClusterOpts gets cluster options
	GetClusterOpts(n node.Node, options []string) (map[string]string, error)

	//SetClusterOptsWithConfirmation sets cluster options and confirm it
	SetClusterOptsWithConfirmation(n node.Node, clusterOpts map[string]string) error

	//SetClusterRunTimeOpts sets cluster run time options
	SetClusterRunTimeOpts(n node.Node, rtOpts map[string]string) error

	//ToggleCallHome toggles Call-home
	ToggleCallHome(n node.Node, enabled bool) error

	//UpdateIOPriority IO priority using pxctl command
	UpdateIOPriority(volumeName string, priorityType string) error

	//UpdateStickyFlag update sticky flag using pxctl command
	UpdateStickyFlag(volumeName, stickyOption string) error

	//ValidatePureFaFbMountOptions validates mount options by executing mount command
	ValidatePureFaFbMountOptions(volumeName string, mountoption []string, volumeNode *node.Node) error

	//ValidatePureFaCreateOptions validates create options using xfs_info and tune2fs commands
	ValidatePureFaCreateOptions(volumeName string, FSType string, volumeNode *node.Node) error

	// UpdateSharedv4FailoverStrategyUsingPxctl updates the sharedv4 failover strategy using pxctl
	UpdateSharedv4FailoverStrategyUsingPxctl(volumeName string, strategy api.Sharedv4FailoverStrategy_Value) error

	// RunSecretsLogin runs secrets login using pxctl
	RunSecretsLogin(n node.Node, secretType string) error

	// GetDriverCluster returns the StorageCluster object
	GetDriver() (*v1.StorageCluster, error)

	//IsOperatorBasedInstall returns if px is operator based
	IsOperatorBasedInstall() (bool, error)

	// ValidateDriver validates all driver components
	ValidateDriver(endpointVersion string, autoUpdateComponents bool) error

	// ExpandPool resizes a pool of a given ID
	ExpandPool(poolUID string, operation api.SdkStoragePool_ResizeOperationType, size uint64, skipWaitForCleanVolumes bool) error

	// ExpandPoolUsingPxctlCmd resizes pool of a given ID using CLI Command
	ExpandPoolUsingPxctlCmd(n node.Node, poolUUID string, operation api.SdkStoragePool_ResizeOperationType, size uint64, skipWaitForCleanVolumes bool) error

	// ListStoragePools lists all existing storage pools
	ListStoragePools(labelSelector metav1.LabelSelector) (map[string]*api.StoragePool, error)

	//GetStorageSpec get the storage spec used to deploy portworx
	GetStorageSpec() (*pxapi.StorageSpec, error)

	// GetStoragelessNodes return list of storageless nodes
	GetStoragelessNodes() ([]*api.StorageNode, error)

	// RecyclePXHost Recycle a node and validate the storageless node picked all it drives
	//RecyclePXHost(*node.Node) error

	// Contains check if StorageNode list conatins a give node or not
	Contains([]*api.StorageNode, *api.StorageNode) bool

	// UpdateNodeWithStorageInfo update a new node object
	UpdateNodeWithStorageInfo(n node.Node, skipNodename string) error

	// WaitForNodeIDToBePickedByAnotherNode wait for another node to pick the down node nodeID
	WaitForNodeIDToBePickedByAnotherNode(n *api.StorageNode) (*api.StorageNode, error)

	// ValidateNodeAfterPickingUpNodeID validates the new node pick the correct drives and pools
	ValidateNodeAfterPickingUpNodeID(n1 *api.StorageNode, n2 *api.StorageNode, snList []*api.StorageNode) error

	// WaitForPxPodsToBeUp waits for px pod to be up in given node
	WaitForPxPodsToBeUp(n node.Node) error

	//GetAutoFsTrimStatus get status of autofstrim
	GetAutoFsTrimStatus(pxEndpoint string) (map[string]api.FilesystemTrim_FilesystemTrimStatus, error)

	//GetAutoFsTrimUsage get usage stats of autofstrim
	GetAutoFsTrimUsage(pxEndpoint string) (map[string]*api.FstrimVolumeUsageInfo, error)

	// GetPxctlCmdOutputConnectionOpts returns the command output run on the given node with ConnectionOpts and any error
	GetPxctlCmdOutputConnectionOpts(n node.Node, command string, opts node.ConnectionOpts, retry bool) (string, error)

	// GetPxctlCmdOutput returns the command output run on the given node and any error
	GetPxctlCmdOutput(n node.Node, command string) (string, error)

	// GetNodeStats returns the node stats of the given node and an error if any
	GetNodeStats(n node.Node) (map[string]map[string]int, error)

	// GetTrashCanVolumeIds returns the node stats of the given node and an error if any
	GetTrashCanVolumeIds(n node.Node) ([]string, error)

	//GetKvdbMembers returns KVDB memebers of the PX cluster
	GetKvdbMembers(n node.Node) (map[string]*MetadataNode, error)

	// GetNodePureVolumeAttachedCountMap returns map of node name and count of pure volume attached on that node
	GetNodePureVolumeAttachedCountMap() (map[string]int, error)

	// AddBlockDrives add drives to the node using PXCTL
	AddBlockDrives(n *node.Node, drivePath []string) error

	// GetPoolDrives returns the map of poolID and drive name
	GetPoolDrives(n *node.Node) (map[string][]DiskResource, error)

	// AddCloudDrive add cloud drives to the node using PXCTL
	AddCloudDrive(n *node.Node, devcieSpec string, poolID int32) error

	// GetPoolsUsedSize returns map of pool id and current used size
	GetPoolsUsedSize(n *node.Node) (map[string]string, error)

	// IsIOsInProgressForTheVolume checks if IOs are happening in the given volume
	IsIOsInProgressForTheVolume(n *node.Node, volumeNameOrID string) (bool, error)

	// GetRebalanceJobs returns the list of rebalance jobs
	GetRebalanceJobs() ([]*api.StorageRebalanceJob, error)

	// GetRebalanceJobStatus returns the rebalance jobs response
	GetRebalanceJobStatus(jobID string) (*api.SdkGetRebalanceJobStatusResponse, error)

	// UpdatePoolLabels updates the label of the desired pool, by appending a custom key-value pair
	UpdatePoolLabels(n node.Node, poolID string, labels map[string]string) error

	// GetPoolLabelValue Gets Property details based on the labels provided
	GetPoolLabelValue(poolUUID string, label string) (string, error)

	// IsNodeInMaintenance returns true if Node in Maintenance
	IsNodeInMaintenance(n node.Node) (bool, error)

	// IsNodeOutOfMaintenance returns true if Node in out of Maintenance
	IsNodeOutOfMaintenance(n node.Node) (bool, error)

	// GetAlertsUsingResourceTypeByTime returns all the alerts by resource type filtered by time
	GetAlertsUsingResourceTypeByTime(resourceType api.ResourceType, startTime time.Time, endTime time.Time) (*api.SdkAlertsEnumerateWithFiltersResponse, error)

	// GetAlertsUsingResourceTypeBySeverity returns all the alerts by resource type filtered by severity
	GetAlertsUsingResourceTypeBySeverity(resourceType api.ResourceType, severity api.SeverityType) (*api.SdkAlertsEnumerateWithFiltersResponse, error)

	// GetJournalDevicePath returns journal device path in the given node
	GetJournalDevicePath(n *node.Node) (string, error)

	IsVolumeAttachedOnNode(volume *api.Volume, node node.Node) (bool, error)

	// IsPxReadyOnNode returns true is Px is ready on the node
	IsPxReadyOnNode(n node.Node) bool

	// GetPxctlStatus returns the PX status using pxctl
	GetPxctlStatus(n node.Node) (string, error)

	DeleteSnapshotsForVolumes(volumeNames []string, globalCredentialConfig string) error

	// EnableSkinnySnap Enables skinnysnap on the cluster
	EnableSkinnySnap() error

	// UpdateSkinnySnapReplNum update skinnysnap Repl factor
	UpdateSkinnySnapReplNum(repl string) error
	// UpdateFBDANFSEndpoint updates the NFS endpoint for a given FBDA volume
	UpdateFBDANFSEndpoint(volumeName string, newEndpoint string) error

	// ValidatePureFBDAMountSource checks that, on all the given nodes, all the provided FBDA volumes are mounted using the expected IP
	ValidatePureFBDAMountSource(nodes []node.Node, vols []*Volume, expectedIP string) error
}

// StorageProvisionerType provisioner to be used for torpedo volumes
type StorageProvisionerType string

const (
	// DefaultStorageProvisioner default storage provisioner name
	DefaultStorageProvisioner StorageProvisionerType = "portworx"
	// CSIStorageProvisioner is the CSI (Container Storage Interface) storage provisioner name
	CSIStorageProvisioner StorageProvisionerType = "csi"
)

var (
	volDrivers   = make(map[string]Driver)
	provisioners = make([]string, 0)
	// StorageDriver to be used to store name of the storage driver
	StorageDriver string
	// StorageProvisioner to be used to store name of the storage provisioner
	StorageProvisioner StorageProvisionerType
)

// Register registers the given volume driver
func Register(name string, driverProvisioners map[StorageProvisionerType]StorageProvisionerType, d Driver) error {
	// Add provisioners supported by driver to slice
	for provisioner := range driverProvisioners {
		provisioners = append(provisioners, string(provisioner))
	}

	if _, ok := volDrivers[name]; !ok {
		volDrivers[name] = d
	} else {
		return fmt.Errorf("volume driver: %s is already registered", name)
	}

	return nil
}

// Get an external storage provider to be used with Torpedo.
func Get(name string) (Driver, error) {
	d, ok := volDrivers[name]
	if ok {
		return d, nil
	}

	return nil, &errors.ErrNotFound{
		ID:   name,
		Type: "VolumeDriver",
	}
}

// GetStorageProvisioner storage provsioner name to be used with Torpedo
func GetStorageProvisioner() string {
	return string(StorageProvisioner)
}

// GetStorageDriver storage driver name to be used with Torpedo
func GetStorageDriver() string {
	return string(StorageDriver)
}

// GetVolumeDrivers returns list of supported volume drivers
func GetVolumeDrivers() []string {
	var voldrivers []string
	for v := range volDrivers {
		voldrivers = append(voldrivers, v)
	}
	return voldrivers
}

// GetVolumeProvisioners returns list of supported volume provisioners
func GetVolumeProvisioners() []string {
	var volumeProvisioners []string
	for _, v := range provisioners {
		volumeProvisioners = append(volumeProvisioners, v)
	}
	return volumeProvisioners
}

func (v *Volume) String() string {
	return v.Name
}
