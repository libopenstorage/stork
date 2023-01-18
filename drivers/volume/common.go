package volume

import (
	"regexp"
	"time"

	pxapi "github.com/portworx/torpedo/porx/px/api"

	snapv1 "github.com/kubernetes-incubator/external-storage/snapshot/pkg/apis/crd/v1"
	apapi "github.com/libopenstorage/autopilot-api/pkg/apis/autopilot/v1alpha1"
	"github.com/libopenstorage/openstorage/api"
	v1 "github.com/libopenstorage/operator/pkg/apis/core/v1"
	"github.com/pborman/uuid"
	driver_api "github.com/portworx/torpedo/drivers/api"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/pkg/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// DiagRequestConfig is a request object which provides all the configuration details
// to PX for running diagnostics on a node. This object can also be passed over
// the wire through an API server for remote diag requests.
type DiagRequestConfig struct {
	// OutputFile for the diags.tgz
	OutputFile string
	// DockerHost config
	DockerHost string
	// ContainerName for PX
	ContainerName string
	// ExecPath of the program making this request (pxctl)
	ExecPath string
	// Profile when set diags command only dumps the go profile
	Profile bool
	// Live gets live diagnostics
	Live bool
	// Upload uploads the diags.tgz to s3
	Upload bool
	// All gets all possible diagnostics from PX
	All bool
	// Force overwrite of existing diags file.
	Force bool
	// OnHost indicates whether diags is being run on the host
	// or inside the container
	OnHost bool
	// Token for security authentication (if enabled)of the program making this request (pxctl)
	Token string
	// Extra indicates whether diags should attempt to collect extra information
	Extra bool
}

// LicenseSummary struct that will hold our SKU and Features
type LicenseSummary struct {
	SKU                 string
	Features            []*pxapi.LicensedFeature
	LicenesConditionMsg string
}

// DiagOps options collection for switching the workflow of the DiagCollection function.
type DiagOps struct {
	// Validate toggle to indicate that we want to test the diags generation (only used in telemetry test currently)
	Validate bool
	// Async toggle to indicate that we want to use async diags
	Async bool
	// PxIsStopped
	PxStopped bool
}

// MetadataNode TODO temporary solution until sdk supports metadataNode response
type MetadataNode struct {
	PeerUrls   []string `json:"PeerUrls"`
	ClientUrls []string `json:"ClientUrls"`
	Leader     bool     `json:"Leader"`
	DbSize     int      `json:"DbSize"`
	IsHealthy  bool     `json:"IsHealthy"`
	ID         string   `json:"ID"`
}

// DefaultDriver implements defaults for Driver interface
type DefaultDriver struct {
}

func (d *DefaultDriver) String() string {
	return ""
}

func (d *DefaultDriver) GetVolumeDriverNamespace() (string, error) {
	return "", &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "GetVolumeDriverNamespace()",
	}

}

// Init initializes the volume driver under the given scheduler
func (d *DefaultDriver) Init(sched, nodeDriver, token, storageProvisioner, csiGenericDriverConfigMap string) error {
	StorageProvisioner = DefaultStorageProvisioner
	return nil
}

// RefreshDriverEndpoints refreshes volume driver endpoint
func (d *DefaultDriver) RefreshDriverEndpoints() error {
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "RefreshDriverEndpoints()",
	}

}

// CreateVolume creates a volume with the given setting
// returns volume_id of the new volume
func (d *DefaultDriver) CreateVolume(volName string, size uint64, haLevel int64) (string, error) {
	return "", &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "CreateVolume()",
	}
}

// CreateVolumeUsingRequest creates a volume with the given create request
// returns volume_id of the new volume
func (d *DefaultDriver) CreateVolumeUsingRequest(request *api.SdkVolumeCreateRequest) (string, error) {
	return "", &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "CreateVolumeUsingRequest()",
	}
}

// CloneVolume clones the volume specified in VolumeId paramerter
// returns the volume_id of the cloned volume
func (d *DefaultDriver) CloneVolume(volumeID string) (string, error) {
	return "", &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "CloneVolume()",
	}
}

// AttachVolume attaches a volume with the default setting
// returns the device path
func (d *DefaultDriver) AttachVolume(volumeID string) (string, error) {
	return "", &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "CreateVolume()",
	}
}

// DetachVolume detaches the volume given the volumeID
func (d *DefaultDriver) DetachVolume(volumeID string) error {
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "DetachVolume()",
	}
}

// DeleteVolume deletes the volume specified by volumeID
func (d *DefaultDriver) DeleteVolume(volumeID string) error {
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "DeleteVolume()",
	}
}

// CleanupVolume forcefully unmounts/detaches and deletes a storage volume.
// This is only called by Torpedo during cleanup operations, it is not
// used during orchestration simulations.
func (d *DefaultDriver) CleanupVolume(name string) error {
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "CleanupVolume()",
	}
}

// InspectVolume inspects the volume with the given name
func (d *DefaultDriver) InspectVolume(name string) (*api.Volume, error) {
	return nil, &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "InspectVolume()",
	}
}

// GetStorageDevices returns the list of storage devices used by the given node.
func (d *DefaultDriver) GetStorageDevices(n node.Node) ([]string, error) {
	// TODO: Implement
	devPaths := []string{}
	return devPaths, &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "GetStorageDevices()",
	}

}

// IsDriverInstalled checks for driver to be installed on a node
func (d *DefaultDriver) IsDriverInstalled(n node.Node) (bool, error) {
	return false, &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "IsDriverInstalled()",
	}
}

// RecoverDriver will recover a volume driver from a failure/storage down state.
// This could be used by a volume driver to recover itself from any underlying storage
// failure.
func (d *DefaultDriver) RecoverDriver(n node.Node) error {
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "RecoverDriver()",
	}
}

// EnterMaintenance puts the given node in maintenance mode
func (d *DefaultDriver) EnterMaintenance(n node.Node) error {
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "EnterMaintenance()",
	}
}

// ExitMaintenance exits the given node from maintenance mode
func (d *DefaultDriver) ExitMaintenance(n node.Node) error {
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "ExitMaintenance()",
	}
}

// RecoverPool will recover a pool from a failure/storage down state.
// This could be used by a pool driver to recover itself from any underlying storage
// failure.
func (d *DefaultDriver) RecoverPool(n node.Node) error {
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "RecoverPool()",
	}
}

// EnterPoolMaintenance puts pools on the given node in maintenance mode
func (d *DefaultDriver) EnterPoolMaintenance(n node.Node) error {
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "EnterPoolMaintenance()",
	}
}

// ExitPoolMaintenance exits pools on the given node from maintenance mode
func (d *DefaultDriver) ExitPoolMaintenance(n node.Node) error {
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "ExitPoolMaintenance()",
	}
}

// GetDriverVersion Returns the pxctl version
func (d *DefaultDriver) GetDriverVersion() (string, error) {
	return "", &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "GetDriverVersion()",
	}
}

// ValidateCreateVolume validates whether a volume has been created properly.
// params are the custom volume options passed when creating the volume.
func (d *DefaultDriver) ValidateCreateVolume(name string, params map[string]string) error {
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "ValidateCreateVolume()",
	}
}

// ValidateCreateSnapshot validates whether a volume has been created properly.
// params are the custom volume options passed when creating the volume.
func (d *DefaultDriver) ValidateCreateSnapshot(name string, params map[string]string) error {
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "ValidateCreateSnapshot()",
	}
}

// ValidateCreateSnapshotUsingPxctl validates whether a volume snapshot has been created properly.
// params are the custom volume options passed when creating the volume.
func (d *DefaultDriver) ValidateCreateSnapshotUsingPxctl(name string) error {
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "ValidateCreateSnapshotUsingPxctl()",
	}
}

// ValidateCreateCloudsnap validates whether a volume has been created properly.
// params are the custom volume options passed when creating the volume.
func (d *DefaultDriver) ValidateCreateCloudsnap(name string, params map[string]string) error {
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "ValidateCreateCloudsnap()",
	}
}

// ValidateCreateCloudsnapUsingPxctl validates whether a cloudsnap has been created properly.
// params are the custom volume options passed when creating the volume.
func (d *DefaultDriver) ValidateCreateCloudsnapUsingPxctl(name string) error {
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "ValidateCreateCloudsnapUsingPxctl()",
	}
}

// ValidateCreateGroupSnapshotUsingPxctl validates whether a group volumesnapshot has been created properly.
// params are the custom volume options passed when creating the volume.
func (d *DefaultDriver) ValidateCreateGroupSnapshotUsingPxctl() error {
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "ValidateCreateGroupSnapshotUsingPxctl()",
	}
}

// ValidateVolumeInPxctlList validates whether the given volume appears in the output of "pxctl v l"
func (d *DefaultDriver) ValidateVolumeInPxctlList(volumeName string) error {
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "ValidateVolumeInPxctlList()",
	}
}

// ValidateGetByteUsedForVolume validates and returns byteUsed for given volume.
func (d *DefaultDriver) ValidateGetByteUsedForVolume(volumeName string, params map[string]string) (uint64, error) {
	return 0, &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "ValidateGetByteUsedForVolume()",
	}
}

// ValidatePureVolumesNoReplicaSets validates Pure volumes has no replicaset.
func (d *DefaultDriver) ValidatePureVolumesNoReplicaSets(volumeName string, params map[string]string) error {
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "ValidatePureVolumesNoReplicaSets()",
	}
}

// ValidateUpdateVolume validates if volume changes has been applied
func (d *DefaultDriver) ValidateUpdateVolume(vol *Volume, params map[string]string) error {
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "ValidateUpdateVolume()",
	}
}

// SetIoBandwidth Sets the max bandwidth for IOPS with given read and write MBps
func (d *DefaultDriver) SetIoBandwidth(vol *Volume, readBandwidthMBps uint32, writeBandwidthMBps uint32) error {
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "SetIoBandwidth()",
	}
}

// ValidateDeleteVolume validates whether a volume is cleanly removed from the volume driver
func (d *DefaultDriver) ValidateDeleteVolume(vol *Volume) error {
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "ValidateDeleteVolume()",
	}
}

// ValidateVolumeCleanup checks if the necessary cleanup has happened for the volumes by this driver
func (d *DefaultDriver) ValidateVolumeCleanup() error {
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "ValidateVolumeCleanup()",
	}
}

// ValidateVolumeSetup validates if the given volume is setup correctly in the cluster
func (d *DefaultDriver) ValidateVolumeSetup(vol *Volume) error {
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "ValidateVolumeSetup()",
	}
}

// StopDriver must cause the volume driver to exit on a given node. If force==true, the volume driver should get killed ungracefully
func (d *DefaultDriver) StopDriver(nodes []node.Node, force bool, triggerOpts *driver_api.TriggerOptions) error {
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "StopDriver()",
	}
}

// GetNodeForVolume returns the node on which the volume is attached
func (d *DefaultDriver) GetNodeForVolume(vol *Volume, timeout time.Duration, retryInterval time.Duration) (*node.Node, error) {
	return nil, &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "GetNodeForVolume()",
	}

}

// GetNodeForBackup returns the node on which the volume is attached
func (d *DefaultDriver) GetNodeForBackup(backupID string) (node.Node, error) {
	return node.Node{}, &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "GetNodeForBackup()",
	}

}

// ExtractVolumeInfo extracts the volume params from the given string
func (d *DefaultDriver) ExtractVolumeInfo(params string) (string, map[string]string, error) {
	var volParams map[string]string
	return "", volParams, &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "ExtractVolumeInfo()",
	}

}

// RandomizeVolumeName randomizes the volume name from the given name
func (d *DefaultDriver) RandomizeVolumeName(params string) string {
	re := regexp.MustCompile("(" + api.Name + "=)([0-9A-Za-z_-]+)(,)?")
	return re.ReplaceAllString(params, "${1}${2}_"+uuid.New()+"${3}")
}

// WaitDriverUpOnNode must wait till the volume driver becomes usable on a given node
func (d *DefaultDriver) WaitDriverUpOnNode(n node.Node, timeout time.Duration) error {
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "WaitDriverUpOnNode()",
	}
}

// GetDriverNodes returns current driver nodes in the cluster
func (d *DefaultDriver) GetDriverNodes() ([]*api.StorageNode, error) {
	return nil, &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "GetDriverNodes()",
	}

}

// GetDriverNode return api.Storage Node
func (d *DefaultDriver) GetDriverNode(n *node.Node, nManagers ...api.OpenStorageNodeClient) (*api.StorageNode, error) {
	return &api.StorageNode{}, &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "GetDriverNode()",
	}
}

// WaitDriverDownOnNode must wait till the volume driver becomes unusable on a given node
func (d *DefaultDriver) WaitDriverDownOnNode(n node.Node) error {
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "WaitDriverDownOnNode()",
	}
}

// GetReplicationFactor returns the current replication factor of the volume.
func (d *DefaultDriver) GetReplicationFactor(vol *Volume) (int64, error) {
	return int64(1), &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "GetReplicationFactor()",
	}

}

// SetReplicationFactor sets the volume's replication factor to the passed param rf.
func (d *DefaultDriver) SetReplicationFactor(vol *Volume, replFactor int64, nodesToBeUpdated []string, poolsToBeUpdated []string, waitForUpdateToFinish bool, opts ...Options) error {
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "SetReplicationFactor()",
	}
}

// WaitForReplicationToComplete waits for replication factor to complete .
func (d *DefaultDriver) WaitForReplicationToComplete(vol *Volume, replFactor int64, replicationUpdateTimeout time.Duration) error {
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "WaitForReplicationToComplete()",
	}
}

// GetMaxReplicationFactor returns the max supported repl factor of a volume
func (d *DefaultDriver) GetMaxReplicationFactor() int64 {
	return 3
}

// GetMinReplicationFactor returns the min supported repl factor of a volume
func (d *DefaultDriver) GetMinReplicationFactor() int64 {
	return 1
}

// GetAggregationLevel returns the aggregation level for the given volume
func (d *DefaultDriver) GetAggregationLevel(vol *Volume) (int64, error) {
	return int64(1), &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "GetAggregationLevel()",
	}
}

// StartDriver must cause the volume driver to start on a given node.
func (d *DefaultDriver) StartDriver(n node.Node) error {
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "StartDriver()",
	}
}

// UpgradeDriver upgrades the volume driver from the given link and checks if it was upgraded to endpointVersion
func (d *DefaultDriver) UpgradeDriver(endpointVersion string) error {
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "UpgradeDriver()",
	}
}

// ValidateDriver validates driver components
func (d *DefaultDriver) ValidateDriver(endpointVersion string, autoUpdateComponents bool) error {
	// TODO: Add implementation
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "ValidateDriver()",
	}
}

// UpgradeStork upgrades the stork driver from the given link and checks if it was upgraded to endpointVersion
func (d *DefaultDriver) UpgradeStork(endpointVersion string) error {
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "UpgradeDriver()",
	}
}

// GetClusterPairingInfo returns cluster pair information
func (d *DefaultDriver) GetClusterPairingInfo(kubeConfigPath, token string) (map[string]string, error) {
	pairInfo := make(map[string]string)
	return pairInfo, &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "GetClusterPairingInfo()",
	}

}

// DecommissionNode decommissions the given node from the cluster
func (d *DefaultDriver) DecommissionNode(n *node.Node) error {
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "DecommissionNode()",
	}
}

// RecoverNode recovers node back to the normal
func (d *DefaultDriver) RecoverNode(n *node.Node) error {
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "RecoverNode()",
	}
}

// GetNodeStatus returns the status of a given node
func (d *DefaultDriver) GetNodeStatus(n node.Node) (*api.Status, error) {
	return nil, &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "GetNodeStatus()",
	}

}

// GetReplicaSets returns the replica sets for a given volume
func (d *DefaultDriver) GetReplicaSets(torpedovol *Volume) ([]*api.ReplicaSet, error) {
	return nil, &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "GetReplicaSets()",
	}

}

// ValidateVolumeSnapshotRestore return nil if snapshot is restored successuflly to
// given volumes
// TODO: additionally check for restore objects in case of cloudsnap
func (d *DefaultDriver) ValidateVolumeSnapshotRestore(vol string, snapshotData *snapv1.VolumeSnapshotData, timeStart time.Time) error {
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "ValidateVolumeSnapshotRestore()",
	}
}

// CollectDiags collects live diags on a node
func (d *DefaultDriver) CollectDiags(n node.Node, config *DiagRequestConfig, diagOps DiagOps) error {
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "CollectDiags()",
	}
}

// ValidateDiagsOnS3 validates the diags or diags file on S3 bucket
func (d *DefaultDriver) ValidateDiagsOnS3(n node.Node, diagsFile string) error {
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "ValidateDiagsOnS3()",
	}
}

// ValidateStoragePools validates all the storage pools
func (d *DefaultDriver) ValidateStoragePools() error {
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "ValidateStoragePools()",
	}
}

// ExpandPool resizes a pool of a given ID
func (d *DefaultDriver) ExpandPool(poolUID string, operation api.SdkStoragePool_ResizeOperationType, size uint64) error {
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "ExpandPool()",
	}
}

// GetAutoFsTrimStatus get auto ftim status of a given volume
func (d *DefaultDriver) GetAutoFsTrimStatus(pxEndpoint string) (map[string]api.FilesystemTrim_FilesystemTrimStatus, error) {
	return nil, &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "GetAutoFsTrimStatus()",
	}
}

// ListStoragePools lists all existing storage pools
func (d *DefaultDriver) ListStoragePools(labelSelector metav1.LabelSelector) (map[string]*api.StoragePool, error) {
	return nil, &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "ListStoragePools()",
	}
}

// GetStorageSpec get the storage spec used to deploy portworx
func (d *DefaultDriver) GetStorageSpec() (*pxapi.StorageSpec, error) {
	return nil, &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "ListStoragePools()",
	}
}

// ValidateRebalanceJobs validates rebalance jobs
func (d *DefaultDriver) ValidateRebalanceJobs() error {
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "ValidateRebalanceJobs()",
	}
}

// ResizeStoragePoolByPercentage validates pool resize
func (d *DefaultDriver) ResizeStoragePoolByPercentage(string, api.SdkStoragePool_ResizeOperationType, uint64) error {
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "ResizeStoragePoolByPercentage()",
	}
}

// CreateAutopilotRules creates autopilot rules
func (d *DefaultDriver) CreateAutopilotRules([]apapi.AutopilotRule) error {
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "CreateAutopilotRules()",
	}
}

// IsStorageExpansionEnabled returns true if storage expansion enabled
func (d *DefaultDriver) IsStorageExpansionEnabled() (bool, error) {
	return true, &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "IsStorageExpansionEnabled()",
	}

}

// CalculateAutopilotObjectSize calculates expected size based on autopilot rule, initial and workload sizes
func (d *DefaultDriver) CalculateAutopilotObjectSize(apRule apapi.AutopilotRule, initSize uint64, workloadSize uint64) uint64 {
	return 0
}

// EstimatePoolExpandSize calculates the expected size based on autopilot rule, initial and workload sizes
func (d *DefaultDriver) EstimatePoolExpandSize(apRule apapi.AutopilotRule, pool node.StoragePool, node node.Node) (uint64, error) {
	return 0, &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "EstimatePoolExpandSize()",
	}
}

// EstimateVolumeExpand calculates the expected size of a volume based on autopilot rule, initial and workload sizes
func (d *DefaultDriver) EstimateVolumeExpand(apRule apapi.AutopilotRule, initialSize, workloadSize uint64) (uint64, int, error) {
	return 0, 0, &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "EstimateVolumeExpand()",
	}
}

// GetLicenseSummary returns the activated License
func (d *DefaultDriver) GetLicenseSummary() (LicenseSummary, error) {
	return LicenseSummary{}, nil
}

// RestartDriver must cause the volume driver to restart on a given node.
func (d *DefaultDriver) RestartDriver(n node.Node, triggerOpts *driver_api.TriggerOptions) error {
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "RestartDriver()",
	}
}

// SetClusterOpts sets cluster options
func (d *DefaultDriver) SetClusterOpts(n node.Node, rtOpts map[string]string) error {
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "SetClusterOpts()",
	}
}

//GetClusterOpts gets cluster options
func (d *DefaultDriver) GetClusterOpts(n node.Node, options []string) (map[string]string, error) {
	return nil, &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "GetClusterOpts()",
	}
}

// SetClusterOptsWithConfirmation sets cluster options and confirm it
func (d *DefaultDriver) SetClusterOptsWithConfirmation(n node.Node, rtOpts map[string]string) error {
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "SetClusterOpts()",
	}
}

// SetClusterRunTimeOpts sets cluster run time options
func (d *DefaultDriver) SetClusterRunTimeOpts(n node.Node, rtOpts map[string]string) error {
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "SetClusterRunTimeOpts()",
	}
}

// ToggleCallHome toggles Call-home
func (d *DefaultDriver) ToggleCallHome(n node.Node, enabled bool) error {
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "ToggleCallHome()",
	}
}

// UpdateSharedv4FailoverStrategyUsingPxctl updates the sharedv4 failover strategy using pxctl
func (d *DefaultDriver) UpdateSharedv4FailoverStrategyUsingPxctl(volumeName string, strategy api.Sharedv4FailoverStrategy_Value) error {
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "UpdateSharedv4FailoverStrategyUsingPxctl",
	}
}

// UpdateIOPriority update IO priority on volume
func (d *DefaultDriver) UpdateIOPriority(volumeName string, priorityType string) error {
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "UpdateIOPriority",
	}
}

// Contains return
func (d *DefaultDriver) Contains(nodes []*api.StorageNode, n *api.StorageNode) bool {
	return false
}

// GetStoragelessNodes return storageless node list
func (d *DefaultDriver) GetStoragelessNodes() ([]*api.StorageNode, error) {
	return []*api.StorageNode{}, &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "GetStoragelessNodes()",
	}
}

// UpdateNodeWithStorageInfo updates storage info in new node object
func (d *DefaultDriver) UpdateNodeWithStorageInfo(n node.Node, skipNode string) error {
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "UpdateNodeWithStorageInfo()",
	}

}

// WaitForNodeIDToBePickedByAnotherNode wait for new node to pick up the drives.
func (d *DefaultDriver) WaitForNodeIDToBePickedByAnotherNode(
	n *api.StorageNode) (*api.StorageNode, error) {
	return &api.StorageNode{}, &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "WaitForNodeIdToBePickedByAnotherNode()",
	}
}

// ValidateNodeAfterPickingUpNodeID validates the node.
func (d *DefaultDriver) ValidateNodeAfterPickingUpNodeID(
	n1 *api.StorageNode, n2 *api.StorageNode, sn []*api.StorageNode) error {
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "ValidateNodeAfterPickingUpNodeId()",
	}
}

// WaitForPxPodsToBeUp waits for px pods to be up
func (d *DefaultDriver) WaitForPxPodsToBeUp(n node.Node) error {
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "WaitForPxPodsToBeUp()",
	}

}

// IsOperatorBasedInstall eturns if px is operator based
func (d *DefaultDriver) IsOperatorBasedInstall() (bool, error) {
	return false, &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "IsOperatorBasedInstall()",
	}
}

// RunSecretsLogin runs secrets login using pxctl
func (d *DefaultDriver) RunSecretsLogin(n node.Node, secretType string) error {
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "RunSecretsLogin()",
	}
}

// GetDriver returns driver object
func (d *DefaultDriver) GetDriver() (*v1.StorageCluster, error) {
	return nil, &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "GetDriver()",
	}
}

// GetDriverVersionOnNode retruns PxVersion on the given node
func (d *DefaultDriver) GetDriverVersionOnNode(n node.Node) (string, error) {
	return "", &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "GetPxVersionOnNode()",
	}
}

// GetPxctlCmdOutputConnectionOpts returns the command output run on the given node with ConnectionOpts and any error
func (d *DefaultDriver) GetPxctlCmdOutputConnectionOpts(n node.Node, command string, opts node.ConnectionOpts, retry bool) (string, error) {
	return "", &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "GetPxctlCmdOutputConnectionOpts()",
	}
}

// GetPxctlCmdOutput returns the command output run on the given node and any error
func (d *DefaultDriver) GetPxctlCmdOutput(n node.Node, command string) (string, error) {
	return "", &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "GetPxctlCmdOutput()",
	}
}

// IsPureVolume returns true if volume is FA/FB DA volumes else false
func (d *DefaultDriver) IsPureVolume(volume *Volume) (bool, error) {
	return false, &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "IsPureVolume()",
	}
}

// GetNodeStats returns the node stats of the given node and an error if any
func (d *DefaultDriver) GetNodeStats(n node.Node) (map[string]map[string]int, error) {
	return nil, &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "GetNodeStats()",
	}
}

// GetTrashCanVolumeIds returns the volume ids in the trashcan and an error if any
func (d *DefaultDriver) GetTrashCanVolumeIds(n node.Node) ([]string, error) {
	return nil, &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "GetTrashCanVolumeIds()",
	}
}

// IsPureFileVolume returns true if volume is FB volumes else false
func (d *DefaultDriver) IsPureFileVolume(volume *Volume) (bool, error) {
	return false, &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "IsPureFileVolume()",
	}
}

// GetKvdbMembers returns the kvdb members of the PX cluster
func (d *DefaultDriver) GetKvdbMembers(n node.Node) (map[string]*MetadataNode, error) {
	return nil, &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "GetKvdbMembers()",
	}
}

// GetNodePureVolumeAttachedCountMap return Map of nodeName and number of pure volume attached on that node
func (d *DefaultDriver) GetNodePureVolumeAttachedCountMap() (map[string]int, error) {
	return nil, &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "GetNodePureVolumeAttachedCountMap()",
	}
}

// RejoinNode rejoins a given node back to the cluster
func (d *DefaultDriver) RejoinNode(n *node.Node) error {
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "RejoinNode()",
	}
}

// AddBlockDrives add drives to the node using PXCTL
func (d *DefaultDriver) AddBlockDrives(n *node.Node, drivePath []string) error {
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "AddBlockDrives()",
	}
}

// AddCloudDrive add drives to the node using PXCTL
func (d *DefaultDriver) AddCloudDrive(n *node.Node, deviceSpec string, poolID int32) error {
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "AddCloudDrive()",
	}
}

// GetPoolsUsedSize returns map of pool id and current used size
func (d *DefaultDriver) GetPoolsUsedSize(n *node.Node) (map[string]string, error) {
	return nil, &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "GetPoolsUsedSize()",
	}
}

// GetRebalanceJobs returns the list of rebalance jobs
func (d *DefaultDriver) GetRebalanceJobs() ([]*api.StorageRebalanceJob, error) {
	return nil, &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "GetRebalanceJobs()",
	}
}

// GetRebalanceJobStatus returns the rebalance jobs response
func (d *DefaultDriver) GetRebalanceJobStatus(jobID string) (*api.SdkGetRebalanceJobStatusResponse, error) {
	return nil, &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "GetRebalanceJobStatus()",
	}
}
