package anthos

import (
	"context"
	"encoding/json"
	"fmt"

	v1alpha1 "sigs.k8s.io/cluster-api/pkg/apis/deprecated/v1alpha1"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

var (
	clusterResource = schema.GroupVersionResource{Group: "cluster.k8s.io", Version: "v1alpha1", Resource: "clusters"}
)

type IpBlock struct {
	// Gateway is vsphere network gateway
	Gateway string
	// netmask is vsphere network netmask
	Netmask int64
	// Hostname is anthos nodename
	Hostname string
	// Ip is anthos IP
	Ip string
}

type NetworkSpec struct {
	// Dns servers list
	Dns []string
	// Ntp server
	Ntp string
	// OtherNtp servers
	OtherNTP []string
	// ReservedAddresses blocks
	ReservedAddresses []IpBlock
}

type ClusterProviderConfig struct {
	// VsphereDatacenter for anthos VMware cluster
	VsphereDatacenter string
	// VsphereDefaultDatastore for anthos VMware cluster nodes
	VsphereDefaultDatastore string
	// VsphereNetwork network used for anthos VMware cluster nodes
	VsphereNetwork string
	// VsphereResourcePool used for anthos VMware cluster nodes
	VsphereResourcePool string
	// VsphereServer server used for anthos vmware cluster
	VsphereServer string
	// VsphereCACertData certificate data
	VsphereCACertData string
	// NetworkSpec used for anthos vmware cluster
	NetworkSpec NetworkSpec
}

// ClusterOps is an interface to perform k8s cluster operations
type ClusterOps interface {
	// ListCluster lists all kubernetes clusters
	ListCluster(ctx context.Context) (*v1alpha1.ClusterList, error)
	// GetCluster returns a cluster for the given name
	GetCluster(ctx context.Context, name, namespace string) (*v1alpha1.Cluster, error)
	// GetClusterStatus return the given cluster status
	GetClusterStatus(ctx context.Context, name, namespace string) (*v1alpha1.ClusterStatus, error)
	// GetClusterProviderSpec return cluster provider spec
	GetClusterProviderSpec(ctx context.Context, name, namespace string) (*ClusterProviderConfig, error)
}

// ListCluster lists all kubernetes clusters
func (c *Client) ListCluster(ctx context.Context) (*v1alpha1.ClusterList, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}

	result, err := c.dynamicClient.Resource(clusterResource).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	jsonData, err := result.MarshalJSON()
	if err != nil {
		return nil, err
	}
	clusterList := &v1alpha1.ClusterList{}
	if err := json.Unmarshal(jsonData, clusterList); err != nil {
		return nil, err
	}

	return clusterList, err
}

// GetCluster returns a cluster for the given name
func (c *Client) GetCluster(ctx context.Context, name, namespace string) (*v1alpha1.Cluster, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	rawCluster, err := c.dynamicClient.Resource(clusterResource).Namespace(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	jsonData, err := rawCluster.MarshalJSON()
	if err != nil {
		return nil, err
	}
	cluster := &v1alpha1.Cluster{}
	if err := json.Unmarshal(jsonData, cluster); err != nil {
		return nil, err
	}
	return cluster, nil
}

// GetClusterStatus return the given cluster status
func (c *Client) GetClusterStatus(ctx context.Context, name, namespace string) (*v1alpha1.ClusterStatus, error) {
	cluster, err := c.GetCluster(ctx, name, namespace)
	if err != nil {
		return nil, err
	}
	return &cluster.Status, err
}

// GetClusterProviderSpec return cluster provider spec
func (c *Client) GetClusterProviderSpec(ctx context.Context, name, namespace string) (*ClusterProviderConfig, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	rawCluster, err := c.dynamicClient.Resource(clusterResource).Namespace(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	return c.unstructuredGetProviderSpec(rawCluster)

}

func (c *Client) unstructuredGetProviderSpec(clRaw *unstructured.Unstructured) (*ClusterProviderConfig, error) {
	clusterProviderConfig := ClusterProviderConfig{}
	networkSpec := NetworkSpec{}
	providerSpec, _, err := unstructured.NestedMap(clRaw.Object, "spec", "providerSpec")
	if err != nil {
		return nil, fmt.Errorf("failed to get 'provider spec' from the cluster spec. Err: %v", err)
	}
	clusterProviderConfig.VsphereDatacenter, _, err = unstructured.NestedString(providerSpec, "value", "vsphereDatacenter")
	if err != nil {
		return nil, fmt.Errorf("failed to get 'vsphereDatacenter' from provider spec. Err: %v", err)
	}
	clusterProviderConfig.VsphereDefaultDatastore, _, err = unstructured.NestedString(providerSpec, "value", "vsphereDefaultDatastore")
	if err != nil {
		return nil, fmt.Errorf("failed to get 'vsphereDefaultDatastore' from provider spec. Err: %v", err)
	}
	clusterProviderConfig.VsphereNetwork, _, err = unstructured.NestedString(providerSpec, "value", "vsphereNetwork")
	if err != nil {
		return nil, fmt.Errorf("failed to get 'vsphereNetwork' from provider spec. Err: %v", err)
	}
	clusterProviderConfig.VsphereResourcePool, _, err = unstructured.NestedString(providerSpec, "value", "vsphereResourcePool")
	if err != nil {
		return nil, fmt.Errorf("failed to get 'vsphereResourcePool' from provider spec. Err: %v", err)
	}
	clusterProviderConfig.VsphereServer, _, err = unstructured.NestedString(providerSpec, "value", "vsphereServer")
	if err != nil {
		return nil, fmt.Errorf("failed to get 'vsphereServer' from provider spec. Err: %v", err)
	}
	clusterProviderConfig.VsphereCACertData, _, err = unstructured.NestedString(providerSpec, "value", "vsphereCACertData")
	if err != nil {
		return nil, fmt.Errorf("failed to get 'vsphereCACertData' from provider spec. Err: %v", err)
	}
	netSpec, _, err := unstructured.NestedMap(providerSpec, "value", "networkSpec")
	if err != nil {
		return nil, fmt.Errorf("failed to get 'netwrok spec' in provider spec. Err: %v", err)
	}
	if netSpec["dns"] != nil {
		networkSpec.Dns, _, err = unstructured.NestedStringSlice(netSpec, "dns")
		if err != nil {
			return nil, fmt.Errorf("failed to get 'DNS server list' in network spec. Err: %v", err)
		}
	}
	if netSpec["ntp"] != nil {
		networkSpec.Ntp, _, err = unstructured.NestedString(netSpec, "ntp")
		if err != nil {
			return nil, fmt.Errorf("failed to get 'ntp' server from network spec. Err: %v", err)
		}
	}
	if netSpec["otherntp"] != nil {
		networkSpec.OtherNTP, _, err = unstructured.NestedStringSlice(netSpec, "otherntp")
		if err != nil {
			return nil, fmt.Errorf("failed to get 'otherntp' server list from network spec. Err: %v", err)
		}
	}
	if netSpec["reservedAddresses"] != nil {
		reservedAddressesSpec, _, err := unstructured.NestedSlice(netSpec, "reservedAddresses")
		if err != nil {
			return nil, fmt.Errorf("failed to get 'reservedAddress' server list from network spec. Err: %v", err)
		}
		for _, ipBlockInt := range reservedAddressesSpec {
			ipBlockMap := ipBlockInt.(map[string]interface{})
			reservedAddresses := IpBlock{}
			reservedAddresses.Gateway, _, err = unstructured.NestedString(ipBlockMap, "gateway")
			if err != nil {
				return nil, fmt.Errorf("failed to get 'gateway' from reserved address. Err: %v", err)
			}
			reservedAddresses.Hostname, _, err = unstructured.NestedString(ipBlockMap, "hostname")
			if err != nil {
				return nil, fmt.Errorf("failed to get 'hostname' from reserved address. Err: %v", err)
			}
			reservedAddresses.Ip, _, err = unstructured.NestedString(ipBlockMap, "ip")
			if err != nil {
				return nil, fmt.Errorf("failed to get 'ip' from reserved address. Err: %v", err)
			}
			reservedAddresses.Netmask, _, err = unstructured.NestedInt64(ipBlockMap, "netmask")
			if err != nil {
				return nil, fmt.Errorf("failed to get 'netmask' from reserved address. Err: %v", err)
			}
			networkSpec.ReservedAddresses = append(networkSpec.ReservedAddresses, reservedAddresses)
		}
	}
	clusterProviderConfig.NetworkSpec = networkSpec
	return &clusterProviderConfig, nil

}
