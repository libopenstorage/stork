package anthos

import "fmt"

// OnpremOps is an interface to perform Anthos onprem operations
type OnpremOps interface {
	// GetVMwareVersionInfo returns Vmware Version Info
	GetVMwareVersionInfo(project string, location string) ([]byte, error)
	// GetBareMetalVersionInfo returns Bare-metal Version Info
	GetBareMetalVersionInfo(project string, location string) ([]byte, error)
	// ListVMwareNodePools return VMware pools
	ListVMwareNodePools(project string, location string, clustername string) ([]byte, error)
	// GetVMwareCluster return vmware cluster
	GetVMwareCluster(project string, location string, clustername string) ([]byte, error)
}

// GetVMwareVersionInfo returns Anthos Vmware Version Info
func (c *Client) GetVMwareVersionInfo(project string, location string) ([]byte, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	parent := fmt.Sprintf("projects/%s/locations/%s", project, location)
	queryConfigRespone, err := c.projectLocationVmwareClusterService.QueryVersionConfig(parent).Do()
	if err != nil {
		return nil, fmt.Errorf("unable to retrieve Anthos VMware cluster version. Err: %v", err)
	}
	return queryConfigRespone.MarshalJSON()

}

// GetBareMetalVersionInfo returns Bare-metal Version Info
func (c *Client) GetBareMetalVersionInfo(project string, location string) ([]byte, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	parent := fmt.Sprintf("projects/%s/locations/%s", project, location)
	queryConfigRespone, err := c.projectLocationBareMetalClusterService.QueryVersionConfig(parent).Do()
	if err != nil {
		return nil, fmt.Errorf("unable to retrieve Anthos Bare Metal cluster version. Err: %v", err)
	}
	return queryConfigRespone.MarshalJSON()

}

// ListVMwareNodePools return VMware pools
func (c *Client) ListVMwareNodePools(project string, location string, clustername string) ([]byte, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	parent := fmt.Sprintf("projects/%s/locations/%s/vmwareClusters/%s", project, location, clustername)
	listVmwareNodePoolsResponse, err := c.projectsLocationsVmwareClustersVmwareNodePoolsService.List(parent).Do()
	if err != nil {
		return nil, fmt.Errorf("unable to list VMware node pools. Err: %v", err)
	}
	return listVmwareNodePoolsResponse.MarshalJSON()
}

// GetVMwareCluster return vmware cluster
func (c *Client) GetVMwareCluster(project string, location string, clustername string) ([]byte, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	parent := fmt.Sprintf("projects/%s/locations/%s/vmwareClusters/%s", project, location, clustername)
	vmwareClustersResponse, err := c.projectLocationVmwareClusterService.Get(parent).Do()
	if err != nil {
		return nil, fmt.Errorf("unable to get vmware cluster. Err: %v", err)
	}
	return vmwareClustersResponse.MarshalJSON()
}
