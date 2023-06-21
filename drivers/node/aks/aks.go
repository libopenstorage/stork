package aks

import (
	"encoding/json"
	"fmt"
	"github.com/portworx/sched-ops/task"
	"github.com/portworx/torpedo/pkg/log"
	"github.com/portworx/torpedo/pkg/osutils"
	"os"
	"time"

	"github.com/libopenstorage/cloudops"
	"github.com/libopenstorage/cloudops/azure"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/node/ssh"
)

const (
	// DriverName is the name of the aks driver
	DriverName    = "aks"
	azCli         = "az"
	AKSPXNodepool = "nodepool1"
)

type aks struct {
	ssh.SSH
	ops           cloudops.Ops
	instanceGroup string
}

type AKSCluster struct {
	AadProfile    interface{} `json:"aadProfile"`
	AddonProfiles struct {
		Azurepolicy struct {
			Config   interface{} `json:"config"`
			Enabled  bool        `json:"enabled"`
			Identity interface{} `json:"identity"`
		} `json:"azurepolicy"`
	} `json:"addonProfiles"`
	AgentPoolProfiles []struct {
		AvailabilityZones          []string    `json:"availabilityZones"`
		Count                      int         `json:"count"`
		CreationData               interface{} `json:"creationData"`
		CurrentOrchestratorVersion string      `json:"currentOrchestratorVersion"`
		EnableAutoScaling          bool        `json:"enableAutoScaling"`
		EnableEncryptionAtHost     bool        `json:"enableEncryptionAtHost"`
		EnableFips                 bool        `json:"enableFips"`
		EnableNodePublicIP         bool        `json:"enableNodePublicIp"`
		EnableUltraSsd             bool        `json:"enableUltraSsd"`
		GpuInstanceProfile         interface{} `json:"gpuInstanceProfile"`
		HostGroupID                interface{} `json:"hostGroupId"`
		KubeletConfig              interface{} `json:"kubeletConfig"`
		KubeletDiskType            string      `json:"kubeletDiskType"`
		LinuxOsConfig              interface{} `json:"linuxOsConfig"`
		MaxCount                   interface{} `json:"maxCount"`
		MaxPods                    int         `json:"maxPods"`
		MinCount                   interface{} `json:"minCount"`
		Mode                       string      `json:"mode"`
		Name                       string      `json:"name"`
		NodeImageVersion           string      `json:"nodeImageVersion"`
		NodeLabels                 interface{} `json:"nodeLabels"`
		NodePublicIPPrefixID       interface{} `json:"nodePublicIpPrefixId"`
		NodeTaints                 interface{} `json:"nodeTaints"`
		OrchestratorVersion        string      `json:"orchestratorVersion"`
		OsDiskSizeGb               int         `json:"osDiskSizeGb"`
		OsDiskType                 string      `json:"osDiskType"`
		OsSku                      string      `json:"osSku"`
		OsType                     string      `json:"osType"`
		PodSubnetID                interface{} `json:"podSubnetId"`
		PowerState                 struct {
			Code string `json:"code"`
		} `json:"powerState"`
		ProvisioningState         string      `json:"provisioningState"`
		ProximityPlacementGroupID interface{} `json:"proximityPlacementGroupId"`
		ScaleDownMode             interface{} `json:"scaleDownMode"`
		ScaleSetEvictionPolicy    interface{} `json:"scaleSetEvictionPolicy"`
		ScaleSetPriority          interface{} `json:"scaleSetPriority"`
		SpotMaxPrice              interface{} `json:"spotMaxPrice"`
		Tags                      struct {
			CreatedBy    string `json:"CreatedBy"`
			CreatedOnUTC string `json:"CreatedOnUTC"`
			ExpiresOnUTC string `json:"ExpiresOnUTC"`
			LeaseDays    string `json:"LeaseDays"`
			Owner        string `json:"Owner"`
		} `json:"tags"`
		Type            string `json:"type"`
		UpgradeSettings struct {
			MaxSurge interface{} `json:"maxSurge"`
		} `json:"upgradeSettings"`
		VMSize          string `json:"vmSize"`
		WorkloadRuntime string `json:"workloadRuntime"`
	} `json:"agentPoolProfiles"`
	APIServerAccessProfile   interface{} `json:"apiServerAccessProfile"`
	AutoScalerProfile        interface{} `json:"autoScalerProfile"`
	AutoUpgradeProfile       interface{} `json:"autoUpgradeProfile"`
	AzureMonitorProfile      interface{} `json:"azureMonitorProfile"`
	AzurePortalFqdn          string      `json:"azurePortalFqdn"`
	CurrentKubernetesVersion string      `json:"currentKubernetesVersion"`
	DisableLocalAccounts     bool        `json:"disableLocalAccounts"`
	DiskEncryptionSetID      interface{} `json:"diskEncryptionSetId"`
	DNSPrefix                string      `json:"dnsPrefix"`
	EnablePodSecurityPolicy  bool        `json:"enablePodSecurityPolicy"`
	EnableRbac               bool        `json:"enableRbac"`
	ExtendedLocation         interface{} `json:"extendedLocation"`
	Fqdn                     string      `json:"fqdn"`
	FqdnSubdomain            interface{} `json:"fqdnSubdomain"`
	HTTPProxyConfig          interface{} `json:"httpProxyConfig"`
	ID                       string      `json:"id"`
	Identity                 interface{} `json:"identity"`
	IdentityProfile          interface{} `json:"identityProfile"`
	KubernetesVersion        string      `json:"kubernetesVersion"`
	LinuxProfile             struct {
		AdminUsername string `json:"adminUsername"`
		SSH           struct {
			PublicKeys []struct {
				KeyData string `json:"keyData"`
			} `json:"publicKeys"`
		} `json:"ssh"`
	} `json:"linuxProfile"`
	Location       string `json:"location"`
	MaxAgentPools  int    `json:"maxAgentPools"`
	Name           string `json:"name"`
	NetworkProfile struct {
		DNSServiceIP        string   `json:"dnsServiceIp"`
		DockerBridgeCidr    string   `json:"dockerBridgeCidr"`
		IPFamilies          []string `json:"ipFamilies"`
		LoadBalancerProfile struct {
			AllocatedOutboundPorts interface{} `json:"allocatedOutboundPorts"`
			EffectiveOutboundIPs   []struct {
				ID            string `json:"id"`
				ResourceGroup string `json:"resourceGroup"`
			} `json:"effectiveOutboundIPs"`
			EnableMultipleStandardLoadBalancers interface{} `json:"enableMultipleStandardLoadBalancers"`
			IdleTimeoutInMinutes                interface{} `json:"idleTimeoutInMinutes"`
			ManagedOutboundIPs                  struct {
				Count     int         `json:"count"`
				CountIpv6 interface{} `json:"countIpv6"`
			} `json:"managedOutboundIPs"`
			OutboundIPs        interface{} `json:"outboundIPs"`
			OutboundIPPrefixes interface{} `json:"outboundIpPrefixes"`
		} `json:"loadBalancerProfile"`
		LoadBalancerSku   string      `json:"loadBalancerSku"`
		NatGatewayProfile interface{} `json:"natGatewayProfile"`
		NetworkMode       interface{} `json:"networkMode"`
		NetworkPlugin     string      `json:"networkPlugin"`
		NetworkPolicy     interface{} `json:"networkPolicy"`
		OutboundType      string      `json:"outboundType"`
		PodCidr           string      `json:"podCidr"`
		PodCidrs          []string    `json:"podCidrs"`
		ServiceCidr       string      `json:"serviceCidr"`
		ServiceCidrs      []string    `json:"serviceCidrs"`
	} `json:"networkProfile"`
	NodeResourceGroup string `json:"nodeResourceGroup"`
	OidcIssuerProfile struct {
		Enabled   bool        `json:"enabled"`
		IssuerURL interface{} `json:"issuerUrl"`
	} `json:"oidcIssuerProfile"`
	PodIdentityProfile interface{} `json:"podIdentityProfile"`
	PowerState         struct {
		Code string `json:"code"`
	} `json:"powerState"`
	PrivateFqdn          interface{} `json:"privateFqdn"`
	PrivateLinkResources interface{} `json:"privateLinkResources"`
	ProvisioningState    string      `json:"provisioningState"`
	PublicNetworkAccess  interface{} `json:"publicNetworkAccess"`
	ResourceGroup        string      `json:"resourceGroup"`
	SecurityProfile      struct {
		AzureKeyVaultKms interface{} `json:"azureKeyVaultKms"`
		Defender         struct {
			LogAnalyticsWorkspaceResourceID string `json:"logAnalyticsWorkspaceResourceId"`
			SecurityMonitoring              struct {
				Enabled bool `json:"enabled"`
			} `json:"securityMonitoring"`
		} `json:"defender"`
	} `json:"securityProfile"`
	ServicePrincipalProfile struct {
		ClientID string `json:"clientId"`
	} `json:"servicePrincipalProfile"`
	Sku struct {
		Name string `json:"name"`
		Tier string `json:"tier"`
	} `json:"sku"`
	StorageProfile struct {
		BlobCsiDriver interface{} `json:"blobCsiDriver"`
		DiskCsiDriver struct {
			Enabled bool `json:"enabled"`
		} `json:"diskCsiDriver"`
		FileCsiDriver struct {
			Enabled bool `json:"enabled"`
		} `json:"fileCsiDriver"`
		SnapshotController struct {
			Enabled bool `json:"enabled"`
		} `json:"snapshotController"`
	} `json:"storageProfile"`
	SystemData interface{} `json:"systemData"`
	Tags       struct {
		CreatedBy    string `json:"CreatedBy"`
		CreatedOnUTC string `json:"CreatedOnUTC"`
		ExpiresOnUTC string `json:"ExpiresOnUTC"`
		LeaseDays    string `json:"LeaseDays"`
		Owner        string `json:"Owner"`
	} `json:"tags"`
	Type                      string      `json:"type"`
	WindowsProfile            interface{} `json:"windowsProfile"`
	WorkloadAutoScalerProfile struct {
		Keda interface{} `json:"keda"`
	} `json:"workloadAutoScalerProfile"`
}

func (a *aks) String() string {
	return DriverName
}

func (a *aks) Init(nodeOpts node.InitOptions) error {
	a.SSH.Init(nodeOpts)

	instanceGroup := os.Getenv("INSTANCE_GROUP")
	if len(instanceGroup) != 0 {
		a.instanceGroup = instanceGroup
	} else {
		a.instanceGroup = "nodepool1"
	}

	ops, err := azure.NewClientFromMetadata()
	if err != nil {
		return err
	}
	a.ops = ops

	return nil
}

func (a *aks) SetASGClusterSize(perZoneCount int64, timeout time.Duration) error {
	// Azure SDK requires total cluster size
	zones, err := a.GetZones()
	if err != nil {
		return err
	}
	totalClusterSize := perZoneCount * int64(len(zones))
	err = a.ops.SetInstanceGroupSize(a.instanceGroup, totalClusterSize, timeout)
	if err != nil {
		log.Errorf("failed to set size of node pool %s. Error: %v", a.instanceGroup, err)
		return err
	}

	return nil
}

func (a *aks) GetASGClusterSize() (int64, error) {
	nodeCount, err := a.ops.GetInstanceGroupSize(a.instanceGroup)
	if err != nil {
		log.Errorf("failed to get size of node pool %s. Error: %v", a.instanceGroup, err)
		return 0, err
	}

	return nodeCount, nil
}

func (a *aks) GetZones() ([]string, error) {

	aksCluster, err := GetAKSCluster()

	if err != nil {
		return []string{""}, err
	}

	agentProfiles := aksCluster.AgentPoolProfiles

	for _, profile := range agentProfiles {
		if profile.Name == AKSPXNodepool {
			return profile.AvailabilityZones, nil
		}
	}

	return []string{""}, fmt.Errorf("profile with name %s not found", AKSPXNodepool)
}

func (a *aks) SetClusterVersion(version string, timeout time.Duration) error {

	err := azLogin()
	if err != nil {
		return err
	}

	envAzureClusterName := os.Getenv("AZURE_CLUSTER_NAME")

	cmd := fmt.Sprintf("%s aks upgrade --resource-group %s --name %s  --kubernetes-version %s --control-plane-only --no-wait --yes", azCli, envAzureClusterName, envAzureClusterName, version)
	stdout, stderr, err := osutils.ExecShell(cmd)
	if err != nil {
		return fmt.Errorf("failed to set cluser %s master version to %s. Error: %v %v %v", envAzureClusterName, version, stderr, err, stdout)
	}
	log.Infof("Initiated AKS upgrade successfully.")

	return nil
}

func init() {
	a := &aks{
		SSH: *ssh.New(),
	}

	node.Register(DriverName, a)
}

func UpgradeNodePool(nodePoolName, version string) error {
	err := azLogin()
	if err != nil {
		return err
	}

	envAzureClusterName := os.Getenv("AZURE_CLUSTER_NAME")

	cmd := fmt.Sprintf("%s aks nodepool upgrade --resource-group %s --cluster-name %s --nodepool-name %s  --kubernetes-version %s --no-wait --yes", azCli, envAzureClusterName, envAzureClusterName, nodePoolName, version)
	stdout, stderr, err := osutils.ExecShell(cmd)
	if err != nil {
		return fmt.Errorf("failed to set cluser %s master version to %s. Error: %v %v %v", envAzureClusterName, version, stderr, err, stdout)
	}
	log.Infof("Initiated AKS upgrade successfully.")

	return nil
}

func azLogin() error {
	log.Infof("authenticating with Azure")

	envAzureClusterName := os.Getenv("AZURE_CLUSTER_NAME")
	if envAzureClusterName == "" {
		return fmt.Errorf("environment variable AZURE_CLUSTER_NAME is not defined")
	}
	envAzureClientId := os.Getenv("AZURE_CLIENT_ID")
	if envAzureClientId == "" {
		return fmt.Errorf("environment variable AZURE_CLIENT_ID is not defined")
	}

	envAzureClientSecret := os.Getenv("AZURE_CLIENT_SECRET")
	if envAzureClientSecret == "" {
		return fmt.Errorf("environment variable AZURE_CLIENT_SECRET is not defined")
	}

	envAzureTenantId := os.Getenv("AZURE_TENANT_ID")
	if envAzureTenantId == "" {
		return fmt.Errorf("environment variable AZURE_TENANT_ID is not defined")
	}

	cmd := fmt.Sprintf("%s login --service-principal --username %s --password %s --tenant %s", azCli, envAzureClientId, envAzureClientSecret, envAzureTenantId)
	_, stdErr, err := osutils.ExecShell(cmd)
	if err != nil {
		return fmt.Errorf("error while doing `az login`, Err: %v %v", err, stdErr)
	}
	log.Debug("[%s] successfully authenticated with Azure", envAzureClusterName)
	return nil
}

func GetAKSCluster() (AKSCluster, error) {
	err := azLogin()
	if err != nil {
		return AKSCluster{}, err
	}
	envAzureClusterName := os.Getenv("AZURE_CLUSTER_NAME")

	cmd := fmt.Sprintf("%s aks show -g %s -n %s --output json", azCli, envAzureClusterName, envAzureClusterName)
	stdout, stderr, err := osutils.ExecShell(cmd)
	if err != nil {
		return AKSCluster{}, fmt.Errorf("failed to get workers list info. Error: %v %v %v", stderr, err, stdout)
	}
	var aksCluster AKSCluster
	err = json.Unmarshal([]byte(stdout), &aksCluster)
	if err != nil {
		return AKSCluster{}, err
	}

	return aksCluster, nil

}

func WaitForAKSNodePoolToUpgrade(nodePoolName, version string) error {
	t := func() (interface{}, bool, error) {

		aksCluster, err := GetAKSCluster()

		if err != nil {
			return nil, false, err
		}

		for _, profile := range aksCluster.AgentPoolProfiles {
			if profile.Name == nodePoolName && profile.ProvisioningState == "Upgrading" {
				return nil, true, fmt.Errorf("waiting for node pool %s upgrade complete.Current status : %s", nodePoolName, aksCluster.ProvisioningState)
			}
			if profile.Name == nodePoolName && profile.ProvisioningState == "Failed" {
				return nil, false, fmt.Errorf("node pool %s update to %s failed", nodePoolName, version)
			}
			if profile.Name == nodePoolName && profile.ProvisioningState == "Succeeded" {
				return nil, false, nil
			}
		}

		return nil, false, fmt.Errorf("node pool %s update to %s timed out", nodePoolName, version)
	}
	_, err := task.DoRetryWithTimeout(t, 90*time.Minute, 3*time.Minute)

	return err

}

func WaitForControlPlaneToUpgrade(version string) error {
	t := func() (interface{}, bool, error) {

		aksCluster, err := GetAKSCluster()

		if err != nil {
			return nil, false, err
		}

		if aksCluster.ProvisioningState == "Upgrading" {
			return nil, true, fmt.Errorf("waiting for upgrade complete.Current status : %s", aksCluster.ProvisioningState)
		}

		if aksCluster.ProvisioningState == "Failed" {
			return nil, false, fmt.Errorf("control-plane update to %s failed", version)
		}

		if aksCluster.ProvisioningState == "Succeeded" {
			return nil, false, nil
		}

		return nil, false, fmt.Errorf("control-plane update to %s timed out", version)
	}
	_, err := task.DoRetryWithTimeout(t, 90*time.Minute, 3*time.Minute)

	return err

}
