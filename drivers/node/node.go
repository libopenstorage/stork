package node

import (
	"fmt"
	"time"

	"github.com/libopenstorage/openstorage/api"
	"github.com/portworx/torpedo/pkg/errors"
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
	api.StorageNode
	uuid                     string
	VolDriverNodeID          string
	Name                     string
	Addresses                []string
	UsableAddr               string
	Type                     Type
	Zone                     string
	Region                   string
	IsStorageDriverInstalled bool
	IsMetadataNode           bool
	StoragePools             []StoragePool
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

// Driver provides the node driver interface
type Driver interface {
	// Init initializes the node driver under the given scheduler
	Init(nodeOpts InitOptions) error

	// DeleteNode deletes the given node
	DeleteNode(node Node, timeout time.Duration) error

	// String returns the string name of this driver.
	String() string

	// RebootNode reboots the given node
	RebootNode(node Node, options RebootNodeOpts) error

	// RunCommand runs the given command on the node and returns the output
	RunCommand(node Node, command string, options ConnectionOpts) (string, error)

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

func (d *notSupportedDriver) RunCommand(node Node, command string, options ConnectionOpts) (string, error) {
	return "", &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "RunCommand()",
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

func (d *notSupportedDriver) RebootVM(node Node) error {
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "RebootVM()",
	}
}
