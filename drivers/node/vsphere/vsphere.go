package vsphere

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"time"

	"github.com/portworx/sched-ops/task"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/node/ssh"
	"github.com/sirupsen/logrus"
	"github.com/vmware/govmomi"
	"github.com/vmware/govmomi/find"
	"github.com/vmware/govmomi/object"
	"github.com/vmware/govmomi/vim25/types"
)

const (
	// DriverName is the name of the vsphere driver
	DriverName = "vsphere"
	// Protocol is the protocol used
	Protocol = "https://"
)

const (
	vsphereUname = "VSPHERE_USER"
	vspherePwd   = "VSPHERE_PWD"
	vsphereIP    = "VSPHERE_HOST_IP"
)

const (
	// DefaultUsername is the default username used for ssh operations
	DefaultUsername = "root"
	// VMReadyTimeout Timeout for checking VM power state
	VMReadyTimeout = 3 * time.Minute
	// VMReadyRetryInterval interval for retry when checking VM power state
	VMReadyRetryInterval = 5 * time.Second
)

// Vsphere ssh driver
type vsphere struct {
	ssh.SSH
	vsphereUsername string
	vspherePassword string
	vsphereHostIP   string
	ctx             context.Context
	cancel          context.CancelFunc
}

var (
	vmMap = make(map[string]*object.VirtualMachine)
)

func (v *vsphere) String() string {
	return DriverName
}

// InitVsphere initializes the vsphere driver for ssh
func (v *vsphere) Init() error {
	logrus.Infof("Using the vsphere node driver")
	v.SSH.Init()

	v.vsphereUsername = DefaultUsername
	username := os.Getenv(vsphereUname)
	if len(username) != 0 {
		v.vsphereUsername = username
	}

	v.vspherePassword = os.Getenv(vspherePwd)
	if len(v.vspherePassword) == 0 {
		return fmt.Errorf("Vsphere password not provided as env var: %s", vspherePwd)
	}

	v.vsphereHostIP = os.Getenv(vsphereIP)
	if len(v.vsphereHostIP) == 0 {
		return fmt.Errorf("Vsphere host IP not provided as env var: %s", vsphereIP)
	}
	err := v.connect()
	if err != nil {
		return err
	}
	err = v.SSH.Init()
	if err != nil {
		return err
	}
	return nil
}

// TestConnection tests the connection to the given node
func (v *vsphere) TestConnection(n node.Node, options node.ConnectionOpts) error {
	var err error
	logrus.Infof("Testing vsphere driver connection by checking state of the VMs in the vsphere")
	if _, ok := vmMap[n.Name]; !ok {
		return fmt.Errorf("Failed to get VM: %s", n.Name)
	}
	vm := vmMap[n.Name]
	cmd := "hostname"

	t := func() (interface{}, bool, error) {
		powerState, err := vm.PowerState(v.ctx)
		if err != nil || powerState != types.VirtualMachinePowerStatePoweredOn {
			return nil, true, &node.ErrFailedToTestConnection{
				Node:  n,
				Cause: fmt.Sprintf("Failed to test connection to VM: %s Current Status: %v, error: %v", vm.Name(), powerState, err),
			}
		}

		return nil, false, nil
	}
	if _, err := task.DoRetryWithTimeout(t, VMReadyTimeout, VMReadyRetryInterval); err != nil {
		return err
	}
	// Check if VM is not just powered on but also usable
	_, err = v.RunCommand(n, cmd, node.ConnectionOpts{
		Timeout:         VMReadyTimeout,
		TimeBeforeRetry: VMReadyRetryInterval,
	})
	return err
}

func (v *vsphere) connect() error {

	login := fmt.Sprintf("%s%s:%s@%s/sdk", Protocol, v.vsphereUsername, v.vspherePassword, v.vsphereHostIP)
	logrus.Infof("Logging in to ESXi using: %s", login)

	u, err := url.Parse(login)
	if err != nil {
		return fmt.Errorf("error parsing url %s", login)
	}

	v.ctx, v.cancel = context.WithCancel(context.Background())
	//defer cancel()

	c, err := govmomi.NewClient(v.ctx, u, true)
	if err != nil {
		return fmt.Errorf("logging in error: %s", err.Error())
	}
	logrus.Infof("Log in successful to vsphere:  %s:\n", v.vsphereHostIP)

	f := find.NewFinder(c.Client, true)

	// Find one and only datacenter
	dc, err := f.DefaultDatacenter(v.ctx)
	if err != nil {
		return fmt.Errorf("Failed to find data center: %v", err)
	}

	// Make future calls local to this datacenter
	f.SetDatacenter(dc)

	// Find virtual machines in datacenter
	vms, err := f.VirtualMachineList(v.ctx, "*")
	if err != nil {
		return fmt.Errorf("Failed to find any virtual machines on %s: %v", v.vsphereHostIP, err)
	}

	nodes := node.GetNodes()
	for _, vm := range vms {
		for _, n := range nodes {
			if vm.Name() == n.Name {
				if _, ok := vmMap[vm.Name()]; !ok {
					vmMap[vm.Name()] = vm
				}
			}
		}
	}
	return nil
}

// RebootVM reboots vsphere VM
func (v *vsphere) RebootNode(n node.Node, options node.RebootNodeOpts) error {
	if _, ok := vmMap[n.Name]; !ok {
		return fmt.Errorf("Could not fetch VM for node: %s", n.Name)
	}

	vm := vmMap[n.Name]

	logrus.Infof("Rebooting VM: %s  ", vm.Name())
	err := vm.RebootGuest(v.ctx)
	if err != nil {
		return &node.ErrFailedToRebootNode{
			Node:  n,
			Cause: fmt.Sprintf("failed to reboot VM %s. cause %v", vm.Name(), err),
		}
	}
	return nil
}

// PowerOnVM powers on the VM if not already on
func (v *vsphere) PowerOnVM(n node.Node) error {
	var err error
	vm := vmMap[n.Name]

	logrus.Infof("Powering on VM: %s  ", vm.Name())
	tsk, err := vm.PowerOn(v.ctx)
	if err != nil {
		return fmt.Errorf("Failed to power on %s: %v", vm.Name(), err)
	}
	if _, err := tsk.WaitForResult(v.ctx); err != nil {
		return &node.ErrFailedToRebootNode{
			Node:  n,
			Cause: fmt.Sprintf("failed to reboot VM %s. cause %v", vm.Name(), err),
		}
	}

	return nil
}

// PowerOffVM powers off the VM if not already off
func (v *vsphere) PowerOffVM(n node.Node) error {
	var err error
	vm := vmMap[n.Name]

	logrus.Infof("\nPowering off VM: %s  ", vm.Name())
	tsk, err := vm.PowerOff(v.ctx)
	if err != nil {
		return fmt.Errorf("Failed to power off %s: %v", vm.Name(), err)
	}
	if _, err := tsk.WaitForResult(v.ctx); err != nil {
		return &node.ErrFailedToRebootNode{
			Node:  n,
			Cause: fmt.Sprintf("failed to power off  VM %s. cause %v", vm.Name(), err),
		}
	}

	return nil
}

// ShutdownNode shutsdown the vsphere VM
func (v *vsphere) ShutdownNode(n node.Node, options node.ShutdownNodeOpts) error {
	if _, ok := vmMap[n.Name]; !ok {
		return fmt.Errorf("Could not fetch VM for node: %s", n.Name)
	}

	vm := vmMap[n.Name]

	logrus.Infof("Shutting down VM: %s  ", vm.Name())
	err := vm.ShutdownGuest(v.ctx)
	if err != nil {
		return &node.ErrFailedToShutdownNode{
			Node:  n,
			Cause: fmt.Sprintf("failed to shutdown VM %s. cause %v", vm.Name(), err),
		}
	}
	return nil
}

func init() {
	v := &vsphere{
		SSH: ssh.SSH{},
	}

	node.Register(DriverName, v)
}
