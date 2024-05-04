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
	//    providerSpec:
	//      value:
	//        apiVersion: vsphereproviderconfig.k8s.io/v1alpha1
	//        kind: VsphereClusterProviderConfig
	//        loadBalancerIP: 10.13.11.216
	//        metadata: {}
	//        networkSpec:
	//          dns:
	//          - 10.7.32.53
	//          - 10.7.32.31
	//          ntp: 216.239.35.4
	//          otherntp:
	//          - 91.189.91.157
	//          reservedAddresses:
	//          - gateway: 10.13.8.1
	//            hostname: tp-anthos-upgrade-0
	//            ip: 10.13.11.221
	//            netmask: 20
	//          searchdomains:
	//          - dev.purestorage.com
	//          - purestorage.com
	//        vsphereCACertData: |
	//          -----BEGIN CERTIFICATE-----
	//          MIIGtDCCBJygAwIBAgITRQAABz+SUKsxEwqPIwAAAAAHPzANBgkqhkiG9w0BAQsF
	//          ADCBjTELMAkGA1UEBhMCVVMxEzARBgNVBAgTCkNhbGlmb3JuaWExFjAUBgNVBAcT
	//          DU1vdW50YWluIFZpZXcxFTATBgNVBAoTDFB1cmUgU3RvcmFnZTEfMB0GA1UECxMW
	//          SW5mb3JtYXRpb24gVGVjaG5vbG9neTEZMBcGA1UEAwwQUFVSRV9TVE9SQUdFLUlD
	//          QTAeFw0yMzA0MjExNjM4MjlaFw0yNTA0MjExNjQ4MjlaMIGHMQswCQYDVQQGEwJV
	//          UzELMAkGA1UECBMCQ0ExFjAUBgNVBAcTDU1vdW50YWluIFZpZXcxFTATBgNVBAoT
	//          DFB1cmUgU3RvcmFnZTEVMBMGA1UECxMMUHVyZSBTdG9yYWdlMSUwIwYDVQQDExx2
	//          Y3NhLnB3eC5kZXYucHVyZXN0b3JhZ2UuY29tMIIBIjANBgkqhkiG9w0BAQEFAAOC
	//          AQ8AMIIBCgKCAQEAsFKwqlUt3X1w6tvE4uSG0V9EV9vvOYiXEZYvnN4YAJ8hQRK/
	//          tOIiMUPrXGJ5A01UpwvUCVuERbtQ40GfWLBazE+dIDsbrft8ttuCdG3A7U1YpWqP
	//          QW9YfPP7+34UJO4MoLHEjEeqz9eiOGoY11//JGdE1qric+9j9Q6Xmspr8S6b5ydn
	//          w8ZGThmgq5w6Pt+Qj7lVPE7uUrB1vNRxou4c6qv/fzKQU+D8lt/34KWoNh+CJLHB
	//          oLMA0LJT9ovxWe0m/MiYkcNdKhdEkLvHlDlX5ur604kTZzvtvjYrtYVmuKHl8GCb
	//          x3gMWGtYBGsPKqxRzA8Ik3Ue1UAo0m3KOqtQlwIDAQABo4ICDzCCAgswDgYDVR0P
	//          AQH/BAQDAgWgMB0GA1UdDgQWBBRvUL6M00dlH8R1Ic3xReux9VjXeTA/BgNVHREE
	//          ODA2gRZsYXZvbmdAcHVyZXN0b3JhZ2UuY29tghx2Y3NhLnB3eC5kZXYucHVyZXN0
	//          b3JhZ2UuY29tMB8GA1UdIwQYMBaAFHHB6UEMZVCDIHrKJq3Rofc8/x4hMHYGA1Ud
	//          HwRvMG0wa6BpoGeGMGh0dHA6Ly9jcmwua2V5ZmFjdG9ycGtpLmNvbS9QVVJFX1NU
	//          T1JBR0UtSUNBLmNybIYzaHR0cDovL2NybC5rZXlmYWN0b3Jwa2lnZW8uY29tL1BV
	//          UkVfU1RPUkFHRS1JQ0EuY3JsMIGOBggrBgEFBQcBAQSBgTB/MDwGCCsGAQUFBzAC
	//          hjBodHRwOi8vY3JsLmtleWZhY3RvcnBraS5jb20vUFVSRV9TVE9SQUdFLUlDQS5j
	//          cnQwPwYIKwYBBQUHMAKGM2h0dHA6Ly9jcmwua2V5ZmFjdG9ycGtpZ2VvLmNvbS9Q
	//          VVJFX1NUT1JBR0UtSUNBLmNydDA9BgkrBgEEAYI3FQcEMDAuBiYrBgEEAYI3FQiE
	//          vIYCg5GbWIeZjzaC46I5g//bUYE5mch/hrDKQAIBZAIBITATBgNVHSUEDDAKBggr
	//          BgEFBQcDATAbBgkrBgEEAYI3FQoEDjAMMAoGCCsGAQUFBwMBMA0GCSqGSIb3DQEB
	//          CwUAA4ICAQAMDwiKSOYm28hldKR+jJumSpfV8JJQvOsuDetnA9DcqTWbFcYavpnW
	//          QmUfdsNifL0iStv6WbkSkMmxT/KEwjVJIF1w+9SOJjtu9F6DQsOs/bIKbLZ5OxPc
	//          n8PMGlTue4yjsTCkkim+nxWzhskaadts9D7U7gDy9NiDhJtjI6cM33E+6mTlr7ld
	//          NiPdtgbhwHTg2DFz0upeC9WlOTtmTBnjx7cbKiim6fCVfswg08mVtzG1Dv2sbRNT
	//          iWINr870MvAQWjWM04jCQMQ4lTEc8LhtBAlHLzGcwna9Gks6S6xSVGvERMqgIhqL
	//          6tE8PEM3AAowkrgaPauV9cisHRIue+UCJNjo8VVHxMfXW8aLIkTyM5f3JkE46r11
	//          yrs8UPRwmL/xr8tZN4JcVfGECDv9rFASd6r4uoizm3Zdj6HH4ZZQo03bGoFQl9xa
	//          lXEBZ7rkDx/RT63RClNsVuHQtIBmp4LJja0ewYAWscHQIKIjaHZfjYp/667kXB2/
	//          zUaBGGd1pKsVqLEEfCf2XmYrmjg7D1jhUirYezjOxKAxIEcu5gK1Qd7zXkjh9xpT
	//          YG9welLe21af8BSA5iwA/YvQfy5KgpYxFVq2nmCuwjLtrkdO3WHFTMn3F7KDgF8G
	//          fl5XeWa5DDXZhXDwuluuZxtlmL4jCrhT1Fd8ExwDk3Ih6JKOAj4+yQ==
	//          -----END CERTIFICATE-----
	//        vsphereDatacenter: CNBU
	//        vsphereDefaultDatastore: ms500-47-FA-FC-39DS3
	//        vsphereNetwork: QA-1
	//        vsphereResourcePool: FA-FC-39DS3/Resources/tp-anthos-upgrade-369-369-resource-pool
	//        vsphereServer: vcsa.pwx.dev.purestorage.com

	//clusterProviderConfig.VsphereDatacenter, _, err = unstructured.NestedString(clRaw.Object, "providerSpec", "name")
	//if err != nil {
	//	return nil, fmt.Errorf("failed to get 'name' from the data volume metadata: %w", err)
	//}

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
