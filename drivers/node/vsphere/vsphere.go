package vsphere

import (
	"context"
	"fmt"
	"github.com/vmware/govmomi/vim25/mo"
	"net/url"
	"os"
	"regexp"
	"time"

	"github.com/portworx/sched-ops/task"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/node/ssh"
	"github.com/portworx/torpedo/pkg/log"
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
func (v *vsphere) Init(nodeOpts node.InitOptions) error {
	log.Infof("Using the vsphere node driver")

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
	err = v.SSH.Init(nodeOpts)
	if err != nil {
		return err
	}
	return nil
}

// TestConnection tests the connection to the given node
func (v *vsphere) TestConnection(n node.Node, options node.ConnectionOpts) error {
	var err error
	log.Infof("Testing vsphere driver connection by checking state of the VMs in the vsphere")
	if _, ok := vmMap[n.Name]; !ok {
		return fmt.Errorf("Failed to get VM: %s", n.Name)
	}
	vm := vmMap[n.Name]
	cmd := "hostname"
	t := func() (interface{}, bool, error) {
		powerState, err := vm.PowerState(v.ctx)
		log.Infof("Power state of VM : %s state %v ", vm.Name(), powerState)
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

// getVMFinder return find.Finder instance
func (v *vsphere) getVMFinder() (*find.Finder, error) {
	login := fmt.Sprintf("%s%s:%s@%s/sdk", Protocol, v.vsphereUsername, v.vspherePassword, v.vsphereHostIP)
	log.Infof("Logging in to Virtual Center using: %s", login)
	u, err := url.Parse(login)
	if err != nil {
		return nil, fmt.Errorf("error parsing url %s", login)
	}

	v.ctx, v.cancel = context.WithCancel(context.Background())
	//defer cancel()

	c, err := govmomi.NewClient(v.ctx, u, true)
	if err != nil {
		return nil, fmt.Errorf("logging in error: %s", err.Error())
	}
	log.Infof("Log in successful to vsphere:  %s:\n", v.vsphereHostIP)

	f := find.NewFinder(c.Client, true)

	// Find one and only datacenter
	dc, err := f.DefaultDatacenter(v.ctx)
	if err != nil {
		return nil, fmt.Errorf("Failed to find data center: %v", err)
	}

	// Make future calls local to this datacenter
	f.SetDatacenter(dc)

	return f, nil

}

func (v *vsphere) connect() error {
	var f *find.Finder

	// Getting finder instance
	f, err := v.getVMFinder()
	if err != nil {
		return err
	}
	// vmMap Reset to get the new valid VMs info.
	vmMap = make(map[string]*object.VirtualMachine)
	// Find virtual machines in datacenter
	vms, err := f.VirtualMachineList(v.ctx, "*")

	if err != nil {
		return fmt.Errorf("failed to find any virtual machines on %s: %v", v.vsphereHostIP, err)
	}

	nodes := node.GetNodes()
	if nodes == nil {
		return fmt.Errorf("nodes not found")
	}

	for _, vm := range vms {
		var vmMo mo.VirtualMachine
		err = vm.Properties(v.ctx, vm.Reference(), []string{"guest"}, &vmMo)
		if err != nil {
			re, regErr := regexp.Compile(".*has already been deleted or has not been completely created.*")
			if regErr != nil {
				return regErr
			}
			if re.MatchString(fmt.Sprintf("%v", err)) {
				log.Errorf("%v", err)
				continue
			} else {
				log.Errorf("failed to get properties: %v", err)
				return err
			}
		}

		// Get the hostname
		hostname := vmMo.Guest.HostName
		if hostname == "" {
			continue
		}
		log.Debugf("hostname for vm %v: %v", vm.Name(), hostname)

		for _, n := range nodes {
			if hostname == n.Name {
				if _, ok := vmMap[hostname]; !ok {
					vmMap[hostname] = vm
				}
			}
		}
	}
	return nil
}

// AddVM adds a new VM object to vmMap
func (v *vsphere) AddMachine(vmName string) error {
	var f *find.Finder

	log.Infof("Adding VM: %s into vmMap  ", vmName)

	f, err := v.getVMFinder()
	if err != nil {
		return err
	}

	vm, err := f.VirtualMachine(v.ctx, vmName)
	if err != nil {
		return err
	}

	var vmMo mo.VirtualMachine
	err = vm.Properties(v.ctx, vm.Reference(), []string{"guest.hostName"}, &vmMo)
	if err != nil {
		return err
	}

	// Get the hostname
	hostname := vmMo.Guest.HostName
	log.Debugf("hostname: %v", hostname)
	if hostname == "" {
		return fmt.Errorf("Failed to find hostname for  virtual machine on %s: %v", vm.Name(), err)
	}

	vmMap[hostname] = vm
	return nil
}

// RebootVM reboots vsphere VM
func (v *vsphere) RebootNode(n node.Node, options node.RebootNodeOpts) error {
	if _, ok := vmMap[n.Name]; !ok {
		return fmt.Errorf("could not fetch VM for node: %s", n.Name)
	}
	//Reestblish connection to avoid session timeout.
	v.connect()
	vm := vmMap[n.Name]
	log.Infof("Rebooting VM: %s  ", vm.Name())
	err := vm.RebootGuest(v.ctx)
	if err != nil {
		return &node.ErrFailedToRebootNode{
			Node:  n,
			Cause: fmt.Sprintf("failed to reboot VM %s. cause %v", vm.Name(), err),
		}
	}
	return nil
}

// powerOnVM powers on VM by providing VM object
func (v *vsphere) powerOnVM(vm *object.VirtualMachine) error {
	// Checking the VM state before powering it On
	powerState, err := vm.PowerState(v.ctx)
	if err != nil {
		return err
	}

	if powerState == types.VirtualMachinePowerStatePoweredOn {
		log.Warn("VM is already in powered-on state: ", vm.Name())
		return nil
	}

	tsk, err := vm.PowerOn(v.ctx)
	if err != nil {
		return fmt.Errorf("failed to power on %s: %v", vm.Name(), err)
	}
	if _, err := tsk.WaitForResult(v.ctx); err != nil {
		return fmt.Errorf("failed to reboot VM %s. cause %v", vm.Name(), err)
	}
	return nil
}

// PowerOnVM powers on the VM if not already on
func (v *vsphere) PowerOnVM(n node.Node) error {
	var err error
	vm := vmMap[n.Name]

	log.Infof("Powering on VM: %s  ", vm.Name())
	if err = v.powerOnVM(vm); err != nil {
		return &node.ErrFailedToRebootNode{
			Node:  n,
			Cause: fmt.Sprintf("failed to reboot VM %s. cause %v", vm.Name(), err),
		}
	}

	return nil
}

// PowerOnVMByName powers on VM by using name
func (v *vsphere) PowerOnVMByName(vmName string) error {
	// Make sure vmName is part of vmMap before using this method
	var err error
	vm := vmMap[vmName]

	log.Infof("Powering on VM: %s  ", vm.Name())
	if err = v.powerOnVM(vm); err != nil {
		return err
	}
	return nil
}

// PowerOffVM powers off the VM if not already off
func (v *vsphere) PowerOffVM(n node.Node) error {
	var err error
	vm := vmMap[n.Name]

	log.Infof("\nPowering off VM: %s  ", vm.Name())
	tsk, err := vm.PowerOff(v.ctx)
	if err != nil {
		return fmt.Errorf("Failed to power off %s: %v", vm.Name(), err)
	}
	if _, err := tsk.WaitForResult(v.ctx); err != nil {
		return &node.ErrFailedToShutdownNode{
			Node:  n,
			Cause: fmt.Sprintf("failed to power off  VM %s. cause %v", vm.Name(), err),
		}
	}

	return nil
}

// DestroyVM powers off the VM if not already off
func (v *vsphere) DestroyVM(n node.Node) error {
	var err error
	vm := vmMap[n.Name]

	log.Infof("\nDestroying VM: %s  ", vm.Name())
	tsk, err := vm.Destroy(v.ctx)

	if err != nil {
		return fmt.Errorf("Failed to power off %s: %v", vm.Name(), err)
	}
	if _, err := tsk.WaitForResult(v.ctx); err != nil {

		return &node.ErrFailedToDeleteNode{
			Node:  n,
			Cause: fmt.Sprintf("failed to destroy VM %s. cause %v", vm.Name(), err),
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

	log.Infof("Shutting down VM: %s  ", vm.Name())
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
		SSH: *ssh.New(),
	}

	node.Register(DriverName, v)
}


func(v *vsphere) GetSupportedDriveTypes() ([]string, error) {
	return []string{"thin", "zeroedthick", "eagerzeroedthick", "lazyzeroedthick"}, nil
}
