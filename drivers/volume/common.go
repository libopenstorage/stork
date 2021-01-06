package volume

import (
	"regexp"
	"time"

	snapv1 "github.com/kubernetes-incubator/external-storage/snapshot/pkg/apis/crd/v1"
	apapi "github.com/libopenstorage/autopilot-api/pkg/apis/autopilot/v1alpha1"
	"github.com/libopenstorage/openstorage/api"
	"github.com/pborman/uuid"
	driver_api "github.com/portworx/torpedo/drivers/api"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/pkg/errors"
)

// DefaultDriver implements defaults for Driver interface
type DefaultDriver struct {
}

func (d *DefaultDriver) String() string {
	return ""
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

// CleanupVolume forcefully unmounts/detaches and deletes a storage volume.
// This is only called by Torpedo during cleanup operations, it is not
// used during orchestration simulations.
func (d *DefaultDriver) CleanupVolume(name string) error {
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "CleanupVolume()",
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

// RecoverDriver will recover a volume driver from a failure/storage down state.
// This could be used by a volume driver to recover itself from any underlying storage
// failure.
func (d *DefaultDriver) RecoverDriver(n node.Node) error {
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "RecoverDriver()",
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

// ValidateUpdateVolume validates if volume changes has been applied
func (d *DefaultDriver) ValidateUpdateVolume(vol *Volume, params map[string]string) error {
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "ValidateUpdateVolume()",
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
func (d *DefaultDriver) SetReplicationFactor(vol *Volume, replFactor int64, opts ...Options) error {
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "SetReplicationFactor()",
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
func (d *DefaultDriver) UpgradeDriver(endpointURL string, endpointVersion string, enableStork bool) error {
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "UpgradeDriver()",
	}
}

// GetClusterPairingInfo returns cluster pair information
func (d *DefaultDriver) GetClusterPairingInfo() (map[string]string, error) {
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

// RejoinNode rejoins a given node back to the cluster
func (d *DefaultDriver) RejoinNode(n *node.Node) error {
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "RejoinNode()",
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
func (d *DefaultDriver) CollectDiags(n node.Node) error {
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "CollectDiags()",
	}
}

// ValidateStoragePools validates all the storage pools
func (d *DefaultDriver) ValidateStoragePools() error {
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "ValidateStoragePools()",
	}
}

// ValidateRebalanceJobs validates rebalance jobs
func (d *DefaultDriver) ValidateRebalanceJobs() error {
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "ValidateRebalanceJobs()",
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

// RestartDriver must cause the volume driver to restart on a given node.
func (d *DefaultDriver) RestartDriver(n node.Node, triggerOpts *driver_api.TriggerOptions) error {
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "RestartDriver()",
	}
}
