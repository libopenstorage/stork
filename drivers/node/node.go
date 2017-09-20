package node

import (
	"fmt"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/portworx/torpedo/pkg/errors"
)

// Type identifies the type of the cluster node
type Type string

const (
	// TypeMaster identifies a cluster node that is a master/manager
	TypeMaster Type = "Master"
	// TypeWorker identifies a cluster node that is a worker
	TypeWorker Type = "Worker"
)

// Node encapsulates a node in the cluster
type Node struct {
	Name      string
	Addresses []string
	Type      Type
}

// RebootNodeOpts provide additional options for reboot operation
type RebootNodeOpts struct {
	Force bool
}

// ShutdownNodeOpts provide additional options for shutdown operation
type ShutdownNodeOpts struct {
	Force bool
}

// TestConectionOpts provide additional options for test connection operation
type TestConectionOpts struct {
	Timeout         time.Duration
	TimeBeforeRetry time.Duration
}

var (
	nodeDrivers = make(map[string]Driver)
)

// Driver provides the node driver interface
type Driver interface {
	// Init initializes the node driver under the given scheduler
	Init(sched string) error

	// String returns the string name of this driver.
	String() string

	// RebootNode reboots the given node
	RebootNode(node Node, options RebootNodeOpts) error

	// ShutdownNode shuts down the given node
	ShutdownNode(node Node, options ShutdownNodeOpts) error

	// TestConnection tests connection to given node. returns nil if driver can connect to given node
	TestConnection(node Node, options TestConectionOpts) error
}

// Register registers the given node driver
func Register(name string, d Driver) error {
	logrus.Infof("Registering node driver: %v", name)
	nodeDrivers[name] = d
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

func (d *notSupportedDriver) Init(sched string) error {
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

func (d *notSupportedDriver) ShutdownNode(node Node, options ShutdownNodeOpts) error {
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "ShutdownNode()",
	}
}

func (d *notSupportedDriver) TestConnection(node Node, options TestConectionOpts) error {
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "TestConnection()",
	}
}
