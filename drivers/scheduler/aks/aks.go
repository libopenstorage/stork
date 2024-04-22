package aks

import (
	"encoding/json"
	"fmt"
	"github.com/portworx/torpedo/drivers/node"
	"os"
	"time"

	"github.com/libopenstorage/cloudops"
	"github.com/libopenstorage/cloudops/azure"
	"github.com/portworx/sched-ops/task"
	"github.com/portworx/torpedo/drivers/scheduler"
	kube "github.com/portworx/torpedo/drivers/scheduler/k8s"
	"github.com/portworx/torpedo/pkg/log"
	"github.com/portworx/torpedo/pkg/osutils"
)

const (
	// SchedName is the name of the kubernetes scheduler driver implementation
	SchedName = "aks"

	// az is the CLI to perform AKS commands
	azCli = "az"

	defaultAksInstanceGroupName = "nodepool1"

	defaultAksUpgradeTimeout  = 90 * time.Minute
	defaultAksUpgradeInterval = 5 * time.Minute

	defaultGetAksClusterTimeout  = 5 * time.Minute
	defaultGetAksClusterInterval = 20 * time.Second

	expectedProvisioningState = "Succeeded"
	updatingProvisioningState = "Updating"
	failedProvisioningState   = "Failed"
)

type aks struct {
	kube.K8s
	ops           cloudops.Ops
	instanceGroup string
	clusterName   string
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

// String returns the string name of this driver.
func (a *aks) String() string {
	return SchedName
}

func init() {
	a := &aks{}
	scheduler.Register(SchedName, a)
}

func (a *aks) Init(schedOpts scheduler.InitOptions) error {
	ops, err := azure.NewClientFromMetadata()
	if err != nil {
		return err
	}
	a.ops = ops

	if err := a.AzureLogin(); err != nil {
		return err
	}

	err = a.K8s.Init(schedOpts)
	if err != nil {
		return err
	}

	return nil
}

func (a *aks) AzureLogin() error {
	log.Info("Authenticating with Azure")

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
		return fmt.Errorf("failed performing `az login`, Err: %v %v", err, stdErr)
	}
	log.Info("Successfully authenticated with Azure")
	return nil
}

func (a *aks) UpgradeScheduler(version string) error {

	aksCluster, err := a.GetAKSCluster()
	if err != nil {
		return fmt.Errorf("failed to get AKS cluster, Err: %v", err)
	}
	log.Infof("Starting AKS cluster upgrade from [%s] to [%s]", aksCluster.CurrentKubernetesVersion, version)

	// Set version to upgrade to
	if err := a.UpgradeControlPlane(version); err != nil {
		return fmt.Errorf("failed to set AKS cluster version, Err: %v", err)
	}

	// Wait for Control Plane to be upgraded
	if err := a.WaitForControlPlaneToUpgrade(version); err != nil {
		return fmt.Errorf("failed to wait for AKS Control Plane to be upgraded to [%s], Err: %v", version, err)
	}

	// Upgrade node pool
	if err := a.UpgradeNodePool(a.instanceGroup, version); err != nil {
		return fmt.Errorf("failed to upgrade AKS Node Pool [%s] to [%s], Err: %v", a.instanceGroup, version, err)
	}

	// Wait for Node Poll to be upgraded
	if err := a.WaitForAKSNodePoolToUpgrade(a.instanceGroup, version); err != nil {
		return fmt.Errorf("failed to wait for AKS Node Pool [%s] to be upgraded to [%s], Err: %v", a.instanceGroup, version, err)
	}

	log.Infof("Successfully finished AKS cluster [%s] upgrade from [%s] to [%s]", aksCluster.Name, aksCluster.CurrentKubernetesVersion, version)
	return nil
}

func (a *aks) SetASGClusterSize(totalClusterSize int64, timeout time.Duration) error {
	// Azure SDK requires total cluster size
	_, err := a.GetAKSCluster()
	if err != nil {
		return err
	}

	log.Infof("Scaling AKS Node Pool [%s] to [%d]", a.instanceGroup, totalClusterSize)

	cmd := fmt.Sprintf("%s aks nodepool update --resource-group %s --cluster-name %s --nodepool-name %s  --update-cluster-autoscaler --min-count %d --max-count %d --no-wait", azCli, a.clusterName, a.clusterName, a.instanceGroup, totalClusterSize, totalClusterSize)
	stdout, stderr, err := osutils.ExecShell(cmd)
	if err != nil {
		return fmt.Errorf("failed to scale cluser [%s]  to [%d], Err: %v %v %v", a.clusterName, totalClusterSize, stderr, err, stdout)
	}
	err = a.waitForNodePoolToScale(totalClusterSize)

	if err != nil {
		return fmt.Errorf("failed to scale AKS Node Pool [%s] to [%d], Err: %v", a.instanceGroup, totalClusterSize, err)
	}

	log.Infof("Scaled AKS Node Pool [%s] upgrade to [%d] successfully", a.instanceGroup, totalClusterSize)
	return nil

}

func (a *aks) waitForNodePoolToScale(nodeCount int64) error {

	t := func() (interface{}, bool, error) {
		aksCluster, err := a.GetAKSCluster()
		if err != nil {
			return nil, false, err
		}

		for _, profile := range aksCluster.AgentPoolProfiles {
			if profile.Name == a.instanceGroup {
				if profile.ProvisioningState == failedProvisioningState {
					return nil, false, fmt.Errorf(" AKS Node Pool [%s] to scale to [%d] failed, expected status [%s], actual status [%s]", a.instanceGroup, nodeCount, expectedProvisioningState, profile.ProvisioningState)
				}

				if profile.ProvisioningState == updatingProvisioningState {
					return nil, true, fmt.Errorf("waiting for AKS Node Pool [%s] to scale to [%d], expected status [%s], actual status [%s]", a.instanceGroup, nodeCount, expectedProvisioningState, profile.ProvisioningState)
				}

				if profile.Count == int(nodeCount) && profile.ProvisioningState == expectedProvisioningState {
					log.Infof("AKS Node Pool [%s] curret ProvisioningState is [%s]", a.instanceGroup, profile.ProvisioningState)
					return nil, false, nil
				}
			}
		}

		return nil, true, fmt.Errorf("AKS Node Pool [%s] count to [%d] not yet updated", a.instanceGroup, nodeCount)
	}
	_, err := task.DoRetryWithTimeout(t, 60*time.Minute, 2*time.Minute)

	return err
}

func (a *aks) GetASGClusterSize() (int64, error) {
	if len(a.instanceGroup) == 0 {
		_, err := a.GetAKSCluster()
		if err != nil {
			return 0, err
		}
	}
	nodeCount, err := a.ops.GetInstanceGroupSize(a.instanceGroup)
	if err != nil {
		return 0, fmt.Errorf("failed to get size of node pool %s. Error: %v", a.instanceGroup, err)
	}

	return nodeCount, nil
}

func (a *aks) GetZones() ([]string, error) {
	aksCluster, err := a.GetAKSCluster()
	if err != nil {
		return []string{""}, err
	}
	agentProfiles := aksCluster.AgentPoolProfiles

	for _, profile := range agentProfiles {
		if profile.Name == a.instanceGroup {
			return profile.AvailabilityZones, nil
		}
	}

	return []string{""}, fmt.Errorf("profile with name %s not found", a.instanceGroup)
}

// UpgradeControlPlane Upgrades Control Plane only to a specific version
func (a *aks) UpgradeControlPlane(version string) error {
	log.Infof("Upgrade AKS Control Plane to [%s]", version)
	cmd := fmt.Sprintf("%s aks upgrade --resource-group %s --name %s  --kubernetes-version %s --control-plane-only --no-wait --yes", azCli, a.clusterName, a.clusterName, version)
	stdout, stderr, err := osutils.ExecShell(cmd)
	if err != nil {
		return fmt.Errorf("failed to set cluser [%s] version to [%s], Err: %v %v %v", a.clusterName, version, stderr, err, stdout)
	}
	log.Infof("Initiated AKS Control Place upgrade to [%s] successfully", version)
	return nil
}

// UpgradeNodePool Upgrades Node Pool to specified version
func (a *aks) UpgradeNodePool(nodePoolName, version string) error {
	log.Infof("Upgrade AKS Node Pool [%s] to [%s]", nodePoolName, version)
	cmd := fmt.Sprintf("%s aks nodepool upgrade --resource-group %s --cluster-name %s --nodepool-name %s  --kubernetes-version %s --no-wait --yes", azCli, a.clusterName, a.clusterName, nodePoolName, version)
	stdout, stderr, err := osutils.ExecShell(cmd)
	if err != nil {
		return fmt.Errorf("failed to set cluser [%s] version to [%s], Err: %v %v %v", a.clusterName, version, stderr, err, stdout)
	}
	log.Infof("Initiated AKS Node Pool [%s] upgrade to [%s] successfully", nodePoolName, version)
	return nil
}

// GetAKSCluster Gets and return AKS cluster object
func (a *aks) GetAKSCluster() (AKSCluster, error) {

	log.Info("Get AKS cluster object")

	instanceGroup := os.Getenv("INSTANCE_GROUP")
	if len(instanceGroup) != 0 {
		a.instanceGroup = instanceGroup
	} else {
		a.instanceGroup = defaultAksInstanceGroupName
	}

	log.Info("Authenticating with Azure")

	envAzureClusterName := os.Getenv("AZURE_CLUSTER_NAME")
	if envAzureClusterName == "" {
		return AKSCluster{}, fmt.Errorf("environment variable AZURE_CLUSTER_NAME is not defined")
	}
	a.clusterName = envAzureClusterName

	var aksCluster AKSCluster

	t := func() (interface{}, bool, error) {
		cmd := fmt.Sprintf("%s aks show -g %s -n %s --output json", azCli, a.clusterName, a.clusterName)
		stdout, stderr, err := osutils.ExecShell(cmd)
		if err != nil {
			return nil, true, fmt.Errorf("failed to get AKS cluster info, Err: %v %v %v", stderr, err, stdout)
		}

		if err := json.Unmarshal([]byte(stdout), &aksCluster); err != nil {
			return nil, true, fmt.Errorf("failed to unmarshal AKS cluster object to json struct, Err: %v", err)
		}
		return nil, false, nil
	}
	_, err := task.DoRetryWithTimeout(t, defaultGetAksClusterTimeout, defaultGetAksClusterInterval)
	if err != nil {
		return aksCluster, fmt.Errorf("failed to get AKS cluster object, Err: %v", err)
	}

	log.Infof("Successfully got AKS cluster [%s] object", aksCluster.Name)
	return aksCluster, nil
}

// WaitForAKSNodePoolToUpgrade Waits for AKS Node Pool to be upgraded to a specific version
func (a *aks) WaitForAKSNodePoolToUpgrade(nodePoolName, version string) error {
	log.Infof("Waiting for AKS Node Pool [%s] to be upgraded to [%s]", nodePoolName, version)

	t := func() (interface{}, bool, error) {
		aksCluster, err := a.GetAKSCluster()
		if err != nil {
			return nil, false, err
		}

		foundNodePool := false
		for _, profile := range aksCluster.AgentPoolProfiles {
			if profile.Name == nodePoolName {
				foundNodePool = true
				if profile.ProvisioningState != expectedProvisioningState {
					return nil, true, fmt.Errorf("waiting for AKS Node Pool [%s] upgrade to [%s] to complete, expected status [%s], actual status [%s]", nodePoolName, version, expectedProvisioningState, profile.ProvisioningState)
				}
			}
		}

		if foundNodePool {
			log.Infof("Upgrade status for AKS Node Pool [%s] to [%s] is [%s]", nodePoolName, version, expectedProvisioningState)
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("failed to find AKS Node Pool [%s]", nodePoolName)
	}
	_, err := task.DoRetryWithTimeout(t, defaultAksUpgradeTimeout, defaultAksUpgradeInterval)
	if err != nil {
		return fmt.Errorf("failed to upgrade AKS Node Pool [%s] version to [%s], Err: %v", nodePoolName, version, err)
	}
	log.Infof("Successfully upgraded AKS Node Pool [%s] to [%s]", nodePoolName, version)
	return nil
}

// WaitForControlPlaneToUpgrade Waits for AKS Control Plane to be upgraded to a specific version
func (a *aks) WaitForControlPlaneToUpgrade(version string) error {
	log.Infof("Waiting for AKS Control Plane to be upgraded to [%s]", version)
	expectedUpgradeStatus := "Succeeded"

	t := func() (interface{}, bool, error) {
		aksCluster, err := a.GetAKSCluster()
		if err != nil {
			return nil, false, err
		}

		if aksCluster.ProvisioningState != expectedUpgradeStatus {
			return nil, true, fmt.Errorf("waiting for AKS Control Plane upgrade to [%s] to complete, expected status [%s], actual status [%s]", version, expectedUpgradeStatus, aksCluster.ProvisioningState)
		}

		log.Infof("Upgrade status for AKS Control Plane to [%s] is [%s]", version, aksCluster.ProvisioningState)
		return nil, false, nil
	}
	_, err := task.DoRetryWithTimeout(t, defaultAksUpgradeTimeout, defaultAksUpgradeInterval)
	if err != nil {
		return fmt.Errorf("failed to upgrade AKS Control Plane version to [%s], Err: %v", version, err)
	}
	log.Infof("Successfully upgraded AKS Control Plane to [%s]", version)
	return nil
}

// DeleteNode deletes the given node
func (a *aks) DeleteNode(node node.Node) error {
	aksCluster, err := a.GetAKSCluster()
	if err != nil {
		return fmt.Errorf("failed to get AKS cluster, Err: %v", err)
	}
	log.Infof("Delete node [%s] from node pool [%s]", node.Hostname, a.instanceGroup)

	cmd := fmt.Sprintf("%s aks nodepool delete-machines --resource-group %s --cluster-name %s --nodepool-name %s  --machine-names %s --no-wait", azCli, aksCluster.Name, aksCluster.Name, a.instanceGroup, node.Hostname)
	stdout, stderr, err := osutils.ExecShell(cmd)
	if err != nil {
		return fmt.Errorf("failed to delete node [%s] , Err: %v %v %v", node.Hostname, stderr, err, stdout)
	}
	log.Infof("Deleted node [%s] from node pool [%s] successfully", node.Hostname, a.instanceGroup)
	return nil
}
