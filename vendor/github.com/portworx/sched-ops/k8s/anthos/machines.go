package anthos

import (
	"context"
	"encoding/json"

	v1alpha1 "sigs.k8s.io/cluster-api/pkg/apis/deprecated/v1alpha1"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

var (
	machineResource = schema.GroupVersionResource{Group: "cluster.k8s.io", Version: "v1alpha1", Resource: "machines"}
)

const (
	DefaultNamespace = "default"
)

// MachinesOps is an interface to perform k8s machines operations
type MachineOps interface {
	// ListMachines lists all machines in kubernetes cluster
	ListMachines(ctx context.Context) (*v1alpha1.MachineList, error)
	// GetMachine returns a machine for the given name
	GetMachine(ctx context.Context, name string) (*v1alpha1.Machine, error)
	// DeleteMachine delete machine for given name
	DeleteMachine(ctx context.Context, name string) error
}

// ListMachines lists all machines in kubernetes cluster
func (c *Client) ListMachines(ctx context.Context) (*v1alpha1.MachineList, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}

	result, err := c.dynamicClient.Resource(machineResource).Namespace(DefaultNamespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	jsonData, err := result.MarshalJSON()
	if err != nil {
		return nil, err
	}
	machineList := &v1alpha1.MachineList{}
	if err := json.Unmarshal(jsonData, machineList); err != nil {
		return nil, err
	}

	return machineList, err
}

// GetMachine returns a machine for the given name
func (c *Client) GetMachine(ctx context.Context, name string) (*v1alpha1.Machine, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	rawMachine, err := c.dynamicClient.Resource(machineResource).Namespace(DefaultNamespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	jsonData, err := rawMachine.MarshalJSON()
	if err != nil {
		return nil, err
	}
	machine := &v1alpha1.Machine{}
	if err := json.Unmarshal(jsonData, machine); err != nil {
		return nil, err
	}
	return machine, nil
}

// DeleteMachine delete machine for given name
func (c *Client) DeleteMachine(ctx context.Context, name string) error {
	if err := c.initClient(); err != nil {
		return err
	}
	return c.dynamicClient.Resource(machineResource).Namespace(DefaultNamespace).Delete(ctx, name, metav1.DeleteOptions{})

}
