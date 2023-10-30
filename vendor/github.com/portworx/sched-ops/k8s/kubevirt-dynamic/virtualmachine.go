package kubevirtdynamic

import (
	"context"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

var (
	vmResource = schema.GroupVersionResource{Group: "kubevirt.io", Version: "v1", Resource: "virtualmachines"}
)

// VirtualMachine represents an instance of KubeVirt VirtualMachine
type VirtualMachine struct {
	// Name if the VM
	Name string
	// Namespace of the VM
	NameSpace string
	// UID of the VM
	UID string
	// Created indicates if the virtual machine has been created in the cluster
	Created bool
	// Ready indicates if the virtual machine is running and ready
	Ready bool
}

// VirtualMachineOps is an interface to manage VirtualMachineInstance objects
type VirtualMachineOps interface {
	// GetVirtualMachine retrieves some info about the specified VM
	GetVirtualMachine(ctx context.Context, namespace, name string) (*VirtualMachine, error)
	// ListVirtualMachines retrieves VMs in the specified namespace
	ListVirtualMachines(ctx context.Context, namespace string, opts metav1.ListOptions) ([]*VirtualMachine, error)
}

// GetVirtualMachine returns the VirtualMachine
func (c *Client) GetVirtualMachine(ctx context.Context, namespace, name string) (*VirtualMachine, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	vmRaw, err := c.client.Resource(vmResource).Namespace(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	return c.unstructuredGetVM(vmRaw)
}

// ListVirtualMachines returns the VirtualMachines
func (c *Client) ListVirtualMachines(
	ctx context.Context, namespace string, opts metav1.ListOptions,
) ([]*VirtualMachine, error) {
	var ret []*VirtualMachine
	if err := c.initClient(); err != nil {
		return nil, err
	}
	rawVMs, err := c.client.Resource(vmResource).Namespace(namespace).List(ctx, opts)
	if err != nil {
		return nil, err
	}
	for _, rawVM := range rawVMs.Items {
		vm, err := c.unstructuredGetVM(&rawVM)
		if err != nil {
			return nil, fmt.Errorf("failed to parse unstructured VM object: %w", err)
		}
		ret = append(ret, vm)
	}
	return ret, nil
}

// unstructuredGetVM
func (c *Client) unstructuredGetVM(vmRaw *unstructured.Unstructured) (*VirtualMachine, error) {
	// metadata:
	//   name: test-vm-csi
	//   namespace: kubevirt-fedora-vm
	//   uid: ed990548-5f16-4d6e-8b26-6e0acbc1a944
	name, found, err := unstructured.NestedString(vmRaw.Object, "metadata", "name")
	if err != nil || !found {
		return nil, fmt.Errorf("failed to find vm name: %w", err)
	}
	namespace, found, err := unstructured.NestedString(vmRaw.Object, "metadata", "namespace")
	if err != nil || !found {
		return nil, fmt.Errorf("failed to find vm namespace: %w", err)
	}
	uid, found, err := unstructured.NestedString(vmRaw.Object, "metadata", "uid")
	if err != nil || !found {
		return nil, fmt.Errorf("failed to find vm uid: %w", err)
	}

	// created
	created, _, err := unstructured.NestedBool(vmRaw.Object, "status", "created")
	if err != nil {
		return nil, fmt.Errorf("failed to find vm created status: %w", err)
	}

	// ready
	ready, _, err := unstructured.NestedBool(vmRaw.Object, "status", "ready")
	if err != nil {
		return nil, fmt.Errorf("failed to find vm ready status: %w", err)
	}

	return &VirtualMachine{
		Name:      name,
		NameSpace: namespace,
		UID:       uid,
		Created:   created,
		Ready:     ready,
	}, nil
}
