package volume

import (
	"fmt"
	"time"

	snapv1 "github.com/kubernetes-incubator/external-storage/snapshot/pkg/apis/crd/v1"
	apapi "github.com/libopenstorage/autopilot-api/pkg/apis/autopilot/v1alpha1"
	"github.com/libopenstorage/openstorage/api"
	driver_api "github.com/portworx/torpedo/drivers/api"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/pkg/errors"
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
	Init(sched string, nodeDriver string, token string, storageProvisioner string, csiGenericConfigMap string) error

	// String returns the string name of this driver.
	String() string

	// CleanupVolume forcefully unmounts/detaches and deletes a storage volume.
	// This is only called by Torpedo during cleanup operations, it is not
	// used during orchestration simulations.
	CleanupVolume(name string) error

	// ValidateCreateVolume validates whether a volume has been created properly.
	// params are the custom volume options passed when creating the volume.
	ValidateCreateVolume(name string, params map[string]string) error

	// ValidateUpdateVolume validates if volume changes has been applied
	ValidateUpdateVolume(vol *Volume, params map[string]string) error

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

	// RandomizeVolumeName randomizes the volume name from the given name
	RandomizeVolumeName(name string) string

	// RecoverDriver will recover a volume driver from a failure/storage down state.
	// This could be used by a volume driver to recover itself from any underlying storage
	// failure.
	RecoverDriver(n node.Node) error

	// RefreshDriverEndpoints refreshes volume driver endpoint
	RefreshDriverEndpoints() error

	// GetStorageDevices returns the list of storage devices used by the given node.
	GetStorageDevices(n node.Node) ([]string, error)

	// GetReplicationFactor returns the current replication factor of the volume.
	GetReplicationFactor(vol *Volume) (int64, error)

	// SetReplicationFactor sets the volume's replication factor to the passed param rf.
	SetReplicationFactor(vol *Volume, rf int64, opts ...Options) error

	// GetMaxReplicationFactor returns the max supported repl factor of a volume
	GetMaxReplicationFactor() int64

	// GetMinReplicationFactor returns the min supported repl factor of a volume
	GetMinReplicationFactor() int64

	// GetAggregationLevel returns the aggregation level for the given volume
	GetAggregationLevel(vol *Volume) (int64, error)

	// GetClusterPairingInfo returns cluster pairing information from remote cluster
	GetClusterPairingInfo() (map[string]string, error)

	// DecommissionNode decommissions the given node from the cluster
	DecommissionNode(n *node.Node) error

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
	CollectDiags(n node.Node) error

	// ValidateStoragePools validates all the storage pools
	ValidateStoragePools() error

	// IsStorageExpansionEnabled returns true if storage expansion enabled
	IsStorageExpansionEnabled() (bool, error)

	// EstimatePoolExpandSize calculates expected pool size based on autopilot rule
	EstimatePoolExpandSize(apRule apapi.AutopilotRule, pool node.StoragePool, node node.Node) (uint64, error)

	// EstimatePoolExpandSize calculates expected volume size based on autopilot rule, initial and workload sizes
	EstimateVolumeExpand(apRule apapi.AutopilotRule, initialSize, workloadSize uint64) (uint64, int, error)
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
