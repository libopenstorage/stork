package kubevirt

import (
	"fmt"
	"time"

	"github.com/portworx/sched-ops/task"
	k8smetav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubevirtv1 "kubevirt.io/api/core/v1"
)

// VirtualMachineOps is an interface to perform kubevirt virtualMachine operations
type VirtualMachineOps interface {
	// CreateVirtualMachine calls VirtualMachine create client method
	CreateVirtualMachine(*kubevirtv1.VirtualMachine) (*kubevirtv1.VirtualMachine, error)
	// ListVirtualMachines List Kubevirt VirtualMachine in given namespace
	ListVirtualMachines(namespace string) (*kubevirtv1.VirtualMachineList, error)
	// ValidateVirtualMachineRunning check if VirtualMachine is running, if not
	// start VirtualMachine and wait for it get started.
	ValidateVirtualMachineRunning(string, string, time.Duration, time.Duration) error
	// DeleteVirtualMachine Delete VirtualMachine CR
	DeleteVirtualMachine(string, string) error
	// GetVirtualMachine Get updated VirtualMachine from client matching name and namespace
	GetVirtualMachine(string, string) (*kubevirtv1.VirtualMachine, error)
	// StartVirtualMachine Start VirtualMachine
	StartVirtualMachine(*kubevirtv1.VirtualMachine) error
	// StopVirtualMachine Stop VirtualMachine
	StopVirtualMachine(*kubevirtv1.VirtualMachine) error
}

// ListVirtualMachines List Kubevirt VirtualMachine in given namespace
func (c *Client) ListVirtualMachines(namespace string) (*kubevirtv1.VirtualMachineList, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}

	return c.kubevirt.VirtualMachine(namespace).List(&k8smetav1.ListOptions{})
}

// CreateVirtualMachine calls VirtualMachine create client method
func (c *Client) CreateVirtualMachine(vm *kubevirtv1.VirtualMachine) (*kubevirtv1.VirtualMachine, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}

	return c.kubevirt.VirtualMachine(vm.GetNamespace()).Create(vm)
}

// GetVirtualMachine Get updated VirtualMachine from client matching name and namespace
func (c *Client) GetVirtualMachine(name string, namespace string) (*kubevirtv1.VirtualMachine, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}

	return c.kubevirt.VirtualMachine(namespace).Get(name, &k8smetav1.GetOptions{})
}

// DeleteVirtualMachine Delete VirtualMachine CR
func (c *Client) DeleteVirtualMachine(name, namespace string) error {
	if err := c.initClient(); err != nil {
		return err
	}

	return c.kubevirt.VirtualMachine(namespace).Delete(name, &k8smetav1.DeleteOptions{})
}

// ValidateVirtualMachineRunning check if VirtualMachine is running, if not
// start VirtualMachine and wait for it get started.
func (c *Client) ValidateVirtualMachineRunning(name, namespace string, timeout, retryInterval time.Duration) error {
	if err := c.initClient(); err != nil {
		return err
	}
	vm, err := c.GetVirtualMachine(name, namespace)
	if err != nil {
		return fmt.Errorf("failed to get Virtual Machine")
	}

	// Start the VirtualMachine if its not Started yet
	if !*vm.Spec.Running {
		if err = instance.StartVirtualMachine(vm); err != nil {
			return fmt.Errorf("Failed to start VirtualMachine %v", err)
		}
	}

	t := func() (interface{}, bool, error) {

		vm, err = c.GetVirtualMachine(name, namespace)
		if err != nil {
			return "", false, fmt.Errorf("failed to get Virtual Machine")
		}

		if vm.Status.PrintableStatus == kubevirtv1.VirtualMachineStatusRunning {
			return "", false, nil
		}
		return "", true, fmt.Errorf("Virtual Machine not in running state: %v", vm.Status.PrintableStatus)

	}
	if _, err := task.DoRetryWithTimeout(t, timeout, retryInterval); err != nil {
		return err
	}
	return nil

}

// StartVirtualMachine Start VirtualMachine
func (c *Client) StartVirtualMachine(vm *kubevirtv1.VirtualMachine) error {

	if err := c.initClient(); err != nil {
		return err
	}

	return c.kubevirt.VirtualMachine(vm.GetNamespace()).Start(vm.GetName(), &kubevirtv1.StartOptions{})
}

// StopVirtualMachine Stop VirtualMachine
func (c *Client) StopVirtualMachine(vm *kubevirtv1.VirtualMachine) error {
	if err := c.initClient(); err != nil {
		return err
	}

	return c.kubevirt.VirtualMachine(vm.GetNamespace()).Stop(vm.GetName(), &kubevirtv1.StopOptions{})
}
