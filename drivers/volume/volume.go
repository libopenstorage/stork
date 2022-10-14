package volume

import (
	"fmt"
	"github.com/sirupsen/logrus"
	"time"

	snapv1 "github.com/kubernetes-incubator/external-storage/snapshot/pkg/apis/crd/v1"
	apapi "github.com/libopenstorage/autopilot-api/pkg/apis/autopilot/v1alpha1"
	"github.com/libopenstorage/openstorage/api"
	v1 "github.com/libopenstorage/operator/pkg/apis/core/v1"
	driver_api "github.com/portworx/torpedo/drivers/api"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/pkg/errors"
	pxapi "github.com/portworx/torpedo/porx/px/api"
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

// Options to pass to APIs
type Options struct {
	ValidateReplicationUpdateTimeout time.Duration
}

// Driver defines an external volume driver interface that must be implemented
// by any external storage provider that wants to qualify their product with
// Torpedo.  The functions defined here are meant to be destructive and illustrative
// of failure scenarious that can happen with an external storage provider.
type Driver interface {
	// Init initializes the volume driver under the given scheduler
	Init(sched string, nodeDriver string, token string, storageProvisioner string, csiGenericConfigMap string, logger *logrus.Logger) error

	// String returns the string name of this driver.
	String() string

	// CreateVolume creates a volume with the default setting
	// returns volume_id of the new volume
	CreateVolume(volName string, size uint64, haLevel int64) (string, error)

	// CloneVolume creates a clone of the volume whose volumeName is passed as arg.
	// returns volume_id of the cloned volume and error if there is any
	CloneVolume(volumeID string) (string, error)

	// AttachVolume attaches a volume with the default setting
	// returns the device path
	AttachVolume(volumeID string) (string, error)

	// DetachVolume detaches the volume given the volumeID
	DetachVolume(volumeID string) error

	// Delete the volume of the Volume ID provided
	DeleteVolume(volumeID string) error

	// InspectVolume inspects the volume with the given name
	InspectVolume(name string) (*api.Volume, error)

	// CleanupVolume forcefully unmounts/detaches and deletes a storage volume.
	// This is only called by Torpedo during cleanup operations, it is not
	// used during orchestration simulations.
	CleanupVolume(name string) error

	// ValidateCreateVolume validates whether a volume has been created properly.
	// params are the custom volume options passed when creating the volume.
	ValidateCreateVolume(name string, params map[string]string) error

	// ValidateCreateSnapshot validates whether a snapshot has been created properly.
	// params are the custom volume options passed
	ValidateCreateSnapshot(name string, params map[string]string) error

	// ValidateCreateSnapshotUsingPxctl validates whether a snapshot has been created properly using pxctl.
	ValidateCreateSnapshotUsingPxctl(name string) error

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

	// ValidateVolumeInPxctlList validates that the given volume appears in the output of `pxctl v l`
	ValidateVolumeInPxctlList(name string) error

	// ValidateUpdateVolume validates if volume changes has been applied
	ValidateUpdateVolume(vol *Volume, params map[string]string) error

	// SetIoThrottle validates if volume changes has been applied
	SetIoBandwidth(vol *Volume, readBandwidthMBps uint32, writeBandwidthMBps uint32) error

	// ValidateDeleteVolume validates whether a volume is cleanly removed from the volume driver
	ValidateDeleteVolume(vol *Volume) error

	// ValidateVolumeCleanup checks if the necessary cleanup has happened for the volumes by this driver
	ValidateVolumeCleanup() error

	// ValidateVolumeSetup validates if the given volume is setup correctly in the cluster
	ValidateVolumeSetup(vol *Volume) error

	// StopDriver must cause the volume driver to exit on a given node. If force==true, the volume driver should get killed ungracefully
	StopDriver(nodes []node.Node, force bool, triggerOpts *driver_api.TriggerOptions) error

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

	// UpgradeDriver upgrades the volume driver from the given link and checks if it was upgraded to endpointVersion
	UpgradeDriver(endpointURL string, endpointVersion string, enableStork bool) error

	// UpgradeStork upgrades the stork driver from the given link and checks if it was upgraded to endpointVersion
	UpgradeStork(endpointURL string, endpointVersion string) error

	// RandomizeVolumeName randomizes the volume name from the given name
	RandomizeVolumeName(name string) string

	// GetPxNodes returns current PX nodes in the cluster
	GetPxNodes() ([]*api.StorageNode, error)

	// RecoverDriver will recover a volume driver from a failure/storage down state.
	// This could be used by a volume driver to recover itself from any underlying storage
	// failure.
	RecoverDriver(n node.Node) error

	// EnterMaintenance puts the given node in maintenance mode
	EnterMaintenance(n node.Node) error

	// ExitMaintenance exits the given node from maintenance mode
	ExitMaintenance(n node.Node) error

	// GetDriverVersion will return the pxctl version from the node
	GetDriverVersion() (string, error)

	// RefreshDriverEndpoints refreshes volume driver endpoint
	RefreshDriverEndpoints() error

	// GetStorageDevices returns the list of storage devices used by the given node.
	GetStorageDevices(n node.Node) ([]string, error)

	//IsPxInstalled checks for Px to be installed on a node
	IsPxInstalled(n node.Node) (bool, error)

	//GetPxVersionOnNode get PXVersion on the given node
	GetPxVersionOnNode(n node.Node) (string, error)

	// GetReplicationFactor returns the current replication factor of the volume.
	GetReplicationFactor(vol *Volume) (int64, error)

	// SetReplicationFactor sets the volume's replication factor to the passed param rf and nodes.
	SetReplicationFactor(vol *Volume, rf int64, nodesToBeUpdated []string, waitForUpdateToFinish bool, opts ...Options) error

	//WaitForReplicationToComplete waits for replication factor change to complete
	WaitForReplicationToComplete(vol *Volume, replFactor int64, replicationUpdateTimeout time.Duration) error

	// GetMaxReplicationFactor returns the max supported repl factor of a volume
	GetMaxReplicationFactor() int64

	// GetMinReplicationFactor returns the min supported repl factor of a volume
	GetMinReplicationFactor() int64

	// GetAggregationLevel returns the aggregation level for the given volume
	GetAggregationLevel(vol *Volume) (int64, error)

	// GetClusterPairingInfo returns cluster pairing information from remote cluster
	GetClusterPairingInfo(kubeConfigPath, token string) (map[string]string, error)

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
	ValidateDiagsOnS3(n node.Node, diagsFile string) error

	// ValidateStoragePools validates all the storage pools
	ValidateStoragePools() error

	// ValidateRebalanceJobs validates rebalance jobs
	ValidateRebalanceJobs() error

	// ResizeStoragePoolByPercentage resizes the given stroage pool by percentage
	ResizeStoragePoolByPercentage(string, api.SdkStoragePool_ResizeOperationType, uint64) error

	// IsStorageExpansionEnabled returns true if storage expansion enabled
	IsStorageExpansionEnabled() (bool, error)

	// IsPureVolume(volume *torpedovolume.Volume) return true if given volume is FA/FB DA volumes
	IsPureVolume(volume *Volume) (bool, error)

	// IsPureFileVolume(volume *torpedovolume.Volume) return true if given volume is FB volumes
	IsPureFileVolume(volume *Volume) (bool, error)

	// EstimatePoolExpandSize calculates expected pool size based on autopilot rule
	EstimatePoolExpandSize(apRule apapi.AutopilotRule, pool node.StoragePool, node node.Node) (uint64, error)

	// EstimatePoolExpandSize calculates expected volume size based on autopilot rule, initial and workload sizes
	EstimateVolumeExpand(apRule apapi.AutopilotRule, initialSize, workloadSize uint64) (uint64, int, error)

	// GetLicenseSummary returns the activated license SKU and Features
	GetLicenseSummary() (LicenseSummary, error)

	//SetClusterOpts sets cluster options
	SetClusterOpts(n node.Node, clusterOpts map[string]string) error

	//SetClusterOptsWithConfirmation sets cluster options and confirm it
	SetClusterOptsWithConfirmation(n node.Node, clusterOpts map[string]string) error

	//SetClusterRunTimeOpts sets cluster run time options
	SetClusterRunTimeOpts(n node.Node, rtOpts map[string]string) error

	//ToggleCallHome toggles Call-home
	ToggleCallHome(n node.Node, enabled bool) error

	//UpdateIOPriority IO priority using pxctl command
	UpdateIOPriority(volumeName string, priorityType string) error
	// UpdateSharedv4FailoverStrategyUsingPxctl updates the sharedv4 failover strategy using pxctl
	UpdateSharedv4FailoverStrategyUsingPxctl(volumeName string, strategy api.Sharedv4FailoverStrategy_Value) error

	//IsOperatorBasedInstall returns if px is operator based
	IsOperatorBasedInstall() (bool, error)

	// RunSecretsLogin runs secrets login using pxctl
	RunSecretsLogin(n node.Node, secretType string) error

	// GetStorageCluster returns the storageCluster object
	GetStorageCluster() (*v1.StorageCluster, error)

	//UpdateStorageClusterImage update storage cluster image version
	UpdateStorageClusterImage(string) error

	//GetPXStorageCluster returns portworx storage cluster
	GetPXStorageCluster() (*v1.StorageCluster, error)

	// ValidateStorageCluster validates all the storage cluster components
	ValidateStorageCluster(endpointURL, endpointVersion string) error

	// ExpandPool resizes a pool of a given ID
	ExpandPool(poolUID string, operation api.SdkStoragePool_ResizeOperationType, size uint64) error

	// ListStoragePools lists all existing storage pools
	ListStoragePools(labelSelector metav1.LabelSelector) (map[string]*api.StoragePool, error)

	//GetStorageSpec get the storage spec used to deploy portworx
	GetStorageSpec() (*pxapi.StorageSpec, error)

	// GetPxNode return api.StorageNode
	GetPxNode(*node.Node, ...api.OpenStorageNodeClient) (*api.StorageNode, error)

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
}

// StorageProvisionerType provisioner to be used for torpedo volumes
type StorageProvisionerType string

const (
	// DefaultStorageProvisioner default storage provisioner name
	DefaultStorageProvisioner StorageProvisionerType = "portworx"
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
