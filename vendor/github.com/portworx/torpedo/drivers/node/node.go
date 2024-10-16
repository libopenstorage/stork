package node

import (
	"context"
	"fmt"
	"time"

	"github.com/libopenstorage/openstorage/api"
	"github.com/portworx/torpedo/pkg/errors"
	"github.com/vmware/govmomi/object"
)

// Type identifies the type of the cluster node
type Type string

// FindType identifies the type of find command
type FindType string

const (
	// TypeMaster identifies a cluster node that is a master/manager
	TypeMaster Type = "Master"
	// TypeWorker identifies a cluster node that is a worker
	TypeWorker Type = "Worker"
)

const (
	// File identifies a search on find command to look for files only
	File FindType = "f"
	// Directory identifies a search on find command to look for directories only
	Directory FindType = "d"
)

// StoragePool is the storage pool structure on the node
type StoragePool struct {
	*api.StoragePool
	// StoragePoolAtInit in the storage pool that's captured when the test initializes. This is useful for tests that
	// want to track changes in a pool since the test was started. For e.g tracking pool expansion changes
	StoragePoolAtInit *api.StoragePool
	// WorkloadSize is the size in bytes of the workload that will be launched by test on this storage pool
	WorkloadSize uint64
}

// Node encapsulates a node in the cluster
type Node struct {
	*api.StorageNode
	uuid                     string
	VolDriverNodeID          string
	Name                     string
	Addresses                []string
	UsableAddr               string
	Type                     Type
	Zone                     string
	Region                   string
	TopologyZone             string
	TopologyRegion           string
	IsStorageDriverInstalled bool
	IsMetadataNode           bool
	StoragePools             []StoragePool
	PxPodRestartCount        int32
}

// ConnectionOpts provide basic options for all operations and can be embedded by other options
type ConnectionOpts struct {
	Timeout         time.Duration
	TimeBeforeRetry time.Duration
	IgnoreError     bool
	Sudo            bool
}

// RebootNodeOpts provide additional options for reboot operation
type RebootNodeOpts struct {
	Force bool
	ConnectionOpts
}

// CrashNodeOpts provide additional options for crash operation
type CrashNodeOpts struct {
	Force bool
	ConnectionOpts
}

// ShutdownNodeOpts provide additional options for shutdown operation
type ShutdownNodeOpts struct {
	Force bool
	ConnectionOpts
}

// FindOpts provide additional options for find operation
type FindOpts struct {
	Name     string
	MinDepth int
	MaxDepth int
	Type     FindType
	Empty    bool
	ConnectionOpts
}

// SystemctlOpts provide options for systemctl operation
type SystemctlOpts struct {
	Action string
	ConnectionOpts
}

// BlockDrive provide block drive properties
type BlockDrive struct {
	Path       string
	Labels     map[string]string
	MountPoint string
	FSType     string
	Size       string
	Online     bool
	Type       string
}

// TestConnectionOpts provide additional options for test connection operation
type TestConnectionOpts struct {
	ConnectionOpts
}

var (
	nodeDrivers = make(map[string]Driver)
)

// InitOptions initialization options
type InitOptions struct {

	// SpecDir app spec directory
	SpecDir string
}

type DriveSet struct {
	// Configs describes the configuration of the drives present in this set
	// The key is the volumeID
	Configs map[string]DriveConfig
	// NodeID is the id of the node where the drive set is being used/last
	// used
	NodeID string
	// ReservedInstanceID if set is the instance ID of the node that's attempting to transfer the driveset to itself
	ReservedInstanceID string
	// SchedulerNodeName is the name of the node in scheduler context
	SchedulerNodeName string
	// NodeIndex is the index of the node where the drive set is being
	// used/last used
	NodeIndex int
	// CreateTimestamp is the timestamp when the drive set was created
	CreateTimestamp time.Time
	// InstanceID is the cloud provider id of the instance using this drive set
	InstanceID string
	// Zone defines the zone in which the node exists
	Zone string
	// State state of the drive set from the well defined states
	State string
	// Labels associated with this drive set
	Labels *map[string]string `json:"labels"`
}

// DriveConfig defines the configuration for a cloud drive
type DriveConfig struct {
	// Type defines the type of cloud drive
	Type string
	// Size defines the size of the cloud drive in Gi
	Size int64
	// ID is the cloud drive id
	ID string
	// Path is the path where the drive is attached
	Path string
	// Iops is the iops that the drive supports
	Iops int64
	// Vpus provide a measure of disk resources available for
	// performance (IOPS/GBs) of Oracle drives.
	// Oracle uses VPU in lieu of disk types.
	Vpus int64
	// PXType indicates how this drive is being used by PX
	PXType string
	// State state of the drive config from the well defined states
	State string
	// Labels associated with this drive config
	Labels map[string]string `json:"labels"`
	// AttachOptions for cloud drives to be attached
	AttachOptions map[string]string
	// Provisioner is a name of provisioner which was used to create a drive
	Provisioner string
	// Encryption Key string to be passed in device specs
	EncryptionKeyInfo string
	// UUID of VMDK
	DiskUUID string
}

// Driver provides the node driver interface
type Driver interface {
	// Init initializes the node driver under the given scheduler
	Init(nodeOpts InitOptions) error

	// DeleteNode deletes the given node
	//DeleteNode(node Node, timeout time.Duration) error

	// String returns the string name of this driver.
	String() string

	// RebootNode reboots the given node
	RebootNode(node Node, options RebootNodeOpts) error

	// RebootNodeAndWait reboots a given node and waits for the node to be ready.
	RebootNodeAndWait(n Node) error

	// CrashNode Crashes the given node
	CrashNode(node Node, options CrashNodeOpts) error

	// IsUsingSSH returns true if the command will be run using ssh
	IsUsingSSH() bool

	// RunCommand runs the given command on the node and returns the output
	RunCommand(node Node, command string, options ConnectionOpts) (string, error)

	// RunCommandWithNoRetry runs the given command on the node but with no retry
	RunCommandWithNoRetry(node Node, command string, options ConnectionOpts) (string, error)

	// ShutdownNode shuts down the given node
	ShutdownNode(node Node, options ShutdownNodeOpts) error

	// FindFiles finds and returns the files for the given path regex and the node
	FindFiles(path string, node Node, options FindOpts) (string, error)

	// Systemctl runs a systemctl command for the given service on the node
	Systemctl(node Node, service string, options SystemctlOpts) error

	// TestConnection tests connection to given node. returns nil if driver can connect to given node
	TestConnection(node Node, options ConnectionOpts) error

	// YankDrive simulates a failure on the provided drive on the given node.
	// It returns the bus ID of the drive which can be used to recover it back
	YankDrive(node Node, driveNameToFail string, options ConnectionOpts) (string, error)

	// RecoverDrive recovers the given drive from failure on the given node.
	RecoverDrive(node Node, driveNameToRecover string, driveUUID string, options ConnectionOpts) error

	// SystemCheck checks whether core files are present on the given node.
	SystemCheck(node Node, options ConnectionOpts) (string, error)

	// SetASGClusterSize sets node count per zone for an asg cluster
	SetASGClusterSize(perZoneCount int64, timeout time.Duration) error

	// GetASGClusterSize gets node count for an asg cluster
	GetASGClusterSize() (int64, error)

	// SetClusterVersion sets desired version for cluster and its node pools
	SetClusterVersion(version string, timeout time.Duration) error

	// GetClusterVersion returns version of cluster and its node pools
	GetClusterVersion() (clusterVersion string, nodePoolsVersion []string, err error)

	// GetZones returns list of zones in which ASG cluster is running
	GetZones() ([]string, error)

	// PowerOnVM powers VM
	PowerOnVM(node Node) error

	// PowerOffVM powers VM
	PowerOffVM(node Node) error

	// DestroyVM powers VM
	DestroyVM(node Node) error

	// MoveDisks moves disks from one node to another
	MoveDisks(sourceNode Node, targetNode Node) error

	// RemoveNonRootDisks removes non-root disks from the node
	RemoveNonRootDisks(node Node) error

	// SystemctlUnitExist checks if a given service exists in a node
	SystemctlUnitExist(n Node, service string, options SystemctlOpts) (bool, error)

	// AddMachine adds the new machine instance to existing map
	AddMachine(machineName string) error

	// DetachDisk vdisk from node.
	DetachDrivesFromVM(nodeName string, configData map[string]DriveSet) error

	//GetCompatibleDatastores
	GetCompatibleDatastores(portworxNamespace string, datastoreNames []string) ([]*object.Datastore, error)

	//Get All datastores
	GetDatastoresFromDatacenter() ([]*object.Datastore, error)

	// PowerOnVMByName power on the VM using the vm name
	PowerOnVMByName(vmName string) error

	// IsNodeRebootedInGivenTimeRange check if node is rebooted within given time range
	IsNodeRebootedInGivenTimeRange(Node, time.Duration) (bool, error)

	// InjectNetworkError by dropping packets or introdiucing delay in packet tramission
	// nodes=> list of nodes where network injection should be done.
	// errorInjectionType => pass "delay" or "drop"
	// operationType => add/change/delete
	// dropPercentage => intger value from 1 to 100
	// delayInMilliseconds => 1 to 1000
	InjectNetworkError(nodes []Node, errorInjectionType string, operationType string, dropPercentage int, delayInMilliseconds int) error

	// InjectNetworkErrorWithRebootFallback by dropping packets or introdiucing delay in packet tramission and reboot nodes during fallback
	InjectNetworkErrorWithRebootFallback(nodes []Node, errorInjectionType string, operationType string, dropPercentage int, delayInMilliseconds int) error

	// GetDeviceMapperCount return devicemapper count
	GetDeviceMapperCount(Node, time.Duration) (int, error)

	//GetBlockDrives returns the block drives on the node
	GetBlockDrives(n Node, options SystemctlOpts) (map[string]*BlockDrive, error)

	// RebalanceWorkerPool initiates worker pool rebalance for IBM cluster
	RebalanceWorkerPool() error

	// GetNodeState returns current state of the given node
	GetNodeState(n Node) (string, error)
	// GetSupportedDriveTypes returns the types of drives supported by the provider
	GetSupportedDriveTypes() ([]string, error)

	// StorageVmotion selectively relocates specific disks of a virtual machine to a new datastore
	StorageVmotion(ctx context.Context, node Node, portworxNamespace string, moveAllDisks bool) (*object.Datastore, error)

	// GetUUIDFromVMDKPath returns the UUID of the VMDK file
	GetUUIDFromVMDKPath(ctx context.Context, node Node, vmdkPath string) (string, error)

	// findVMByName finds a virtual machine by its name
	FindVMByName(vmName string) (*object.VirtualMachine, error)

	// findDatastoreByName finds a datastore by its name
	FindDatastoreByName(dsName string) (*object.Datastore, error)
}

// Register registers the given node driver
func Register(name string, d Driver) error {
	if _, ok := nodeDrivers[name]; !ok {
		nodeDrivers[name] = d
	} else {
		return fmt.Errorf("node driver: %s is already registered", name)
	}

	return nil
}

// Get returns a registered node driver
func Get(name string) (Driver, error) {
	if d, ok := nodeDrivers[name]; ok {
		return d, nil
	}
	return nil, &errors.ErrNotFound{
		ID:   name,
		Type: "Node Driver",
	}
}

type notSupportedDriver struct{}

// NotSupportedDriver provides the default driver with none of the operations supported
var NotSupportedDriver = &notSupportedDriver{}

func (d *notSupportedDriver) Init(nodeOpts InitOptions) error {
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "Init()",
	}
}

func (d *notSupportedDriver) String() string {
	return fmt.Sprint("Operation String() is not supported")
}

func (d *notSupportedDriver) RebootNode(node Node, options RebootNodeOpts) error {
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "RebootNode()",
	}
}

func (d *notSupportedDriver) DetachDrivesFromVM(nodeName string, configData map[string]DriveSet) error {
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "DetachDrivesFromVM()",
	}
}

func (d *notSupportedDriver) GetDatastoresFromDatacenter() ([]*object.Datastore, error) {
	return nil, &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "GetDatastoresFromDatacenter()",
	}
}

func (d *notSupportedDriver) GetCompatibleDatastores(portworxNamespace string, datastoreNames []string) ([]*object.Datastore, error) {
	return nil, &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "GetDatastoresFromDatacenter()",
	}
}

func (d *notSupportedDriver) RebootNodeAndWait(node Node) error {
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "RebootNodeAndWait()",
	}
}

func (d *notSupportedDriver) CrashNode(node Node, options CrashNodeOpts) error {
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "CrashNode()",
	}
}

func (d *notSupportedDriver) RunCommand(node Node, command string, options ConnectionOpts) (string, error) {
	return "", &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "RunCommand()",
	}
}

func (d *notSupportedDriver) RunCommandWithNoRetry(node Node, command string, options ConnectionOpts) (string, error) {
	return "", &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "RunCommandWithNoRetry()",
	}
}

func (d *notSupportedDriver) ShutdownNode(node Node, options ShutdownNodeOpts) error {
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "ShutdownNode()",
	}
}

func (d *notSupportedDriver) FindFiles(path string, node Node, options FindOpts) (string, error) {
	return "", &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "FindFiles()",
	}
}

func (d *notSupportedDriver) Systemctl(node Node, service string, options SystemctlOpts) error {
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "Systemctl()",
	}
}

func (d *notSupportedDriver) GetBlockDrives(n Node, options SystemctlOpts) (map[string]*BlockDrive, error) {
	return nil, &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "GetBlockDrives()",
	}
}

func (d *notSupportedDriver) YankDrive(node Node, driveToFail string, options ConnectionOpts) (string, error) {
	return "", &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "YankDrive()",
	}
}

func (d *notSupportedDriver) RecoverDrive(node Node, driveToRecover string, driveID string, options ConnectionOpts) error {
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "RecoverDrive()",
	}
}

func (d *notSupportedDriver) TestConnection(node Node, options ConnectionOpts) error {
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "TestConnection()",
	}
}

func (d *notSupportedDriver) SystemCheck(node Node, options ConnectionOpts) (string, error) {
	return "", &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "SystemCheck()",
	}
}

func (d *notSupportedDriver) SetASGClusterSize(count int64, timeout time.Duration) error {
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "SetASGClusterSize()",
	}
}

func (d *notSupportedDriver) GetASGClusterSize() (int64, error) {
	return int64(0), &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "GetASGClusterSize()",
	}
}

func (d *notSupportedDriver) DeleteNode(node Node, timeout time.Duration) error {
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "DeleteNode()",
	}
}

func (d *notSupportedDriver) SetClusterVersion(version string, timeout time.Duration) error {
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "SetClusterVersion()",
	}
}

func (d *notSupportedDriver) GetClusterVersion() (clusterVersion string,
	nodePoolsVersion []string,
	err error) {
	return "", []string{}, &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "GetClusterVersion()",
	}
}

func (d *notSupportedDriver) GetZones() ([]string, error) {
	return []string{}, &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "GetZones()",
	}
}

func (d *notSupportedDriver) PowerOnVM(node Node) error {
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "PowerOnVM()",
	}
}

func (d *notSupportedDriver) PowerOffVM(node Node) error {
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "PowerOffVM()",
	}
}

func (d *notSupportedDriver) DestroyVM(node Node) error {
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "DestroyVM()",
	}
}

func (d *notSupportedDriver) RebootVM(node Node) error {
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "RebootVM()",
	}
}

func (d *notSupportedDriver) SystemctlUnitExist(node Node, service string, options SystemctlOpts) (bool, error) {
	return false, &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "SystemctlUnitExist()",
	}
}

func (d *notSupportedDriver) AddMachine(machineName string) error {
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "AddMachine()",
	}
}

func (d *notSupportedDriver) PowerOnVMByName(vmName string) error {
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "PowerOnVmByName()",
	}
}

// IsUsingSSH returns true if the command will be run using ssh
func (d *notSupportedDriver) IsUsingSSH() bool {
	return false
}

// IsNodeRebootedInGivenTimeRange return true if node rebooted in given time range
func (d *notSupportedDriver) IsNodeRebootedInGivenTimeRange(Node, time.Duration) (bool, error) {
	return false, &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "IsNodeRebootedInGivenTimeRange()",
	}
}

// GetDeviceMapperCount return device mapper count in a node
func (d *notSupportedDriver) GetDeviceMapperCount(Node, time.Duration) (int, error) {
	return -1, &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "GetDeviceMapperCount()",
	}
}

func (d *notSupportedDriver) InjectNetworkError(nodes []Node, errorInjectionType string, operationType string,
	dropPercentage int, delayInMilliseconds int) error {
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "InjectNetworkError()",
	}
}

func (d *notSupportedDriver) InjectNetworkErrorWithRebootFallback(nodes []Node, errorInjectionType string, operationType string,
	dropPercentage int, delayInMilliseconds int) error {
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "InjectNetworkErrorWithRebootFallback()",
	}
}

func (d *notSupportedDriver) RebalanceWorkerPool() error {
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "RebalanceWorkerPool()",
	}
}

func (d *notSupportedDriver) GetNodeState(Node) (string, error) {
	return "", &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "GetNodeState()",
	}
}

func (d *notSupportedDriver) GetSupportedDriveTypes() ([]string, error) {
	return []string{}, &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "GetSupportedDriveTypes()",
	}
}

func (d *notSupportedDriver) MoveDisks(sourceNode Node, targetNode Node) error {
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "MoveDisks()",
	}
}

func (d *notSupportedDriver) RemoveNonRootDisks(node Node) error {
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "RemoveNonRootDisks()",
	}
}

func (d *notSupportedDriver) StorageVmotion(ctx context.Context, node Node, portworxNamespace string, moveAllDisks bool) (*object.Datastore, error) {
	return nil, &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "StorageVmotion()",
	}
}

func (d *notSupportedDriver) GetUUIDFromVMDKPath(ctx context.Context, node Node, vmdkPath string) (string, error) {
	return "", &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "GetUUIDFromVMDKPath()",
	}
}

func (d *notSupportedDriver) FindVMByName(vmName string) (*object.VirtualMachine, error) {
	return nil, &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "FindVMByName()",
	}
}

func (d *notSupportedDriver) FindDatastoreByName(dsName string) (*object.Datastore, error) {
	return nil, &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "FindDatastoreByName()",
	}
}