package kubevirt

import (
	"context"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubevirtv1 "kubevirt.io/api/core/v1"
)

// VirtualMachineInstanceOps is an interface to perform kubevirt operations on virtual machine instances
type VirtualMachineInstanceOps interface {
	// GetVirtualMachineInstance gets updated Virtual Machine Instance from client matching name and namespace
	GetVirtualMachineInstance(context.Context, string, string) (*kubevirtv1.VirtualMachineInstance, error)
}

// GetVirtualMachineInstance gets updated Virtual Machine Instance from client matching name and namespace
func (c *Client) GetVirtualMachineInstance(ctx context.Context, name string, namespace string) (*kubevirtv1.VirtualMachineInstance, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}

	return c.kubevirt.VirtualMachineInstance(namespace).Get(ctx, name, &metav1.GetOptions{})
}
