package oke

import (
	"encoding/json"
	"fmt"
	"github.com/portworx/sched-ops/task"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/portworx/torpedo/drivers/volume"
	"github.com/portworx/torpedo/pkg/log"
	"github.com/portworx/torpedo/pkg/osutils"
	"github.com/sirupsen/logrus"
	"os"
	"strings"
	"time"

	"github.com/portworx/torpedo/drivers/node/ssh"

	"github.com/libopenstorage/cloudops"
	oracleOps "github.com/libopenstorage/cloudops/oracle"
	"github.com/oracle/oci-go-sdk/v65/core"
	"github.com/portworx/torpedo/drivers/node"
	kube "github.com/portworx/torpedo/drivers/scheduler/k8s"
)

const (
	// Schedular is the name of the oke driver
	SchedName                = "oke"
	okeConfigFile            = "/root/.oci/config"
	ociCLI                   = "oci"
	inprogressStatus         = "IN_PROGRESS"
	failedStatus             = "FAILED"
	acceptedStatus           = "ACCEPTED"
	workerTimeout            = 60 * time.Minute
	workerRetryTime          = 30 * time.Second
	customImageCompartmentId = "ocid1.compartment.oc1..aaaaaaaab4u67dhgtj5gpdpp3z42xqqsdnufxkatoild46u3hb67vzojfmzq"
)

type oke struct {
	ssh.SSH
	kube.K8s
	ops               cloudops.Ops
	instanceID        string
	instanceGroupName string
	compartmentId     string
	clusterId         string
	isUserConfigured  bool
}

type Cluster struct {
	Data []struct {
		AvailableKubernetesUpgrades []string `json:"available-kubernetes-upgrades"`
		ClusterPodNetworkOptions    []struct {
			CniType string `json:"cni-type"`
		} `json:"cluster-pod-network-options"`
		CompartmentID  string                       `json:"compartment-id"`
		DefinedTags    map[string]map[string]string `json:"defined-tags"`
		EndpointConfig struct {
			IsPublicIPEnabled bool     `json:"is-public-ip-enabled"`
			NsgIDs            []string `json:"nsg-ids"`
			SubnetID          string   `json:"subnet-id"`
		} `json:"endpoint-config"`
		Endpoints struct {
			Kubernetes          interface{} `json:"kubernetes"`
			PrivateEndpoint     string      `json:"private-endpoint"`
			PublicEndpoint      string      `json:"public-endpoint"`
			VcnHostnameEndpoint interface{} `json:"vcn-hostname-endpoint"`
		} `json:"endpoints"`
		FreeformTags      map[string]interface{} `json:"freeform-tags"`
		ID                string                 `json:"id"`
		ImagePolicyConfig struct {
			IsPolicyEnabled bool          `json:"is-policy-enabled"`
			KeyDetails      []interface{} `json:"key-details"`
		} `json:"image-policy-config"`
		KubernetesVersion string `json:"kubernetes-version"`
		LifecycleDetails  string `json:"lifecycle-details"`
		LifecycleState    string `json:"lifecycle-state"`
		Metadata          struct {
			CreatedByUserID          string      `json:"created-by-user-id"`
			CreatedByWorkRequestID   string      `json:"created-by-work-request-id"`
			DeletedByUserID          string      `json:"deleted-by-user-id"`
			DeletedByWorkRequestID   string      `json:"deleted-by-work-request-id"`
			TimeCreated              string      `json:"time-created"`
			TimeCredentialExpiration string      `json:"time-credential-expiration"`
			TimeDeleted              string      `json:"time-deleted"`
			TimeUpdated              interface{} `json:"time-updated"`
			UpdatedByUserID          interface{} `json:"updated-by-user-id"`
			UpdatedByWorkRequestID   interface{} `json:"updated-by-work-request-id"`
		} `json:"metadata"`
		Name    string `json:"name"`
		Options struct {
			AddOns struct {
				IsKubernetesDashboardEnabled bool `json:"is-kubernetes-dashboard-enabled"`
				IsTillerEnabled              bool `json:"is-tiller-enabled"`
			} `json:"add-ons"`
			AdmissionControllerOptions struct {
				IsPodSecurityPolicyEnabled bool `json:"is-pod-security-policy-enabled"`
			} `json:"admission-controller-options"`
			KubernetesNetworkConfig struct {
				PodsCidr     string `json:"pods-cidr"`
				ServicesCidr string `json:"services-cidr"`
			} `json:"kubernetes-network-config"`
			PersistentVolumeConfig struct {
				DefinedTags  interface{} `json:"defined-tags"`
				FreeformTags interface{} `json:"freeform-tags"`
			} `json:"persistent-volume-config"`
			ServiceLbConfig struct {
				DefinedTags  interface{} `json:"defined-tags"`
				FreeformTags interface{} `json:"freeform-tags"`
			} `json:"service-lb-config"`
			ServiceLbSubnetIds []string `json:"service-lb-subnet-ids"`
		} `json:"options"`
		SystemTags interface{} `json:"system-tags"`
		Type       string      `json:"type"`
		VcnID      string      `json:"vcn-id"`
	} `json:"data"`
}

type NodePools struct {
	Data []NodePool `json:"data"`
}

type NodePool struct {
	ClusterID         string                       `json:"cluster-id"`
	CompartmentID     string                       `json:"compartment-id"`
	DefinedTags       map[string]map[string]string `json:"defined-tags"`
	FreeformTags      map[string]interface{}       `json:"freeform-tags"`
	ID                string                       `json:"id"`
	InitialNodeLabels []interface{}                `json:"initial-node-labels"`
	KubernetesVersion string                       `json:"kubernetes-version"`
	LifecycleDetails  interface{}                  `json:"lifecycle-details"`
	LifecycleState    string                       `json:"lifecycle-state"`
	Name              string                       `json:"name"`
	NodeConfigDetails struct {
		DefinedTags                     map[string]interface{} `json:"defined-tags"`
		FreeformTags                    map[string]interface{} `json:"freeform-tags"`
		IsPvEncryptionInTransitEnabled  bool                   `json:"is-pv-encryption-in-transit-enabled"`
		KmsKeyID                        string                 `json:"kms-key-id"`
		NodePoolPodNetworkOptionDetails struct {
			CniType string `json:"cni-type"`
		} `json:"node-pool-pod-network-option-details"`
		NsgIds           []interface{} `json:"nsg-ids"`
		PlacementConfigs []struct {
			AvailabilityDomain    string      `json:"availability-domain"`
			CapacityReservationID interface{} `json:"capacity-reservation-id"`
			FaultDomains          interface{} `json:"fault-domains"`
			PreemptibleNodeConfig interface{} `json:"preemptible-node-config"`
			SubnetID              string      `json:"subnet-id"`
		} `json:"placement-configs"`
		Size int `json:"size"`
	} `json:"node-config-details"`
	NodeEvictionNodePoolSettings *struct {
		EvictionGraceDuration           string `json:"eviction-grace-duration"`
		IsForceDeleteAfterGraceDuration bool   `json:"is-force-delete-after-grace-duration"`
	} `json:"node-eviction-node-pool-settings"`
	NodeImageID            string      `json:"node-image-id"`
	NodeImageName          string      `json:"node-image-name"`
	NodePoolCyclingDetails interface{} `json:"node-pool-cycling-details"`
	NodeShape              string      `json:"node-shape"`
	NodeShapeConfig        *struct {
		MemoryInGBs float64 `json:"memory-in-gbs"`
		Ocpus       float64 `json:"ocpus"`
	} `json:"node-shape-config"`
	NodeSource struct {
		ImageID    string `json:"image-id"`
		SourceName string `json:"source-name"`
		SourceType string `json:"source-type"`
	} `json:"node-source"`
	NodeSourceDetails struct {
		BootVolumeSizeInGBs interface{} `json:"boot-volume-size-in-gbs"`
		ImageID             string      `json:"image-id"`
		SourceType          string      `json:"source-type"`
	} `json:"node-source-details"`
	QuantityPerSubnet int         `json:"quantity-per-subnet"`
	SshPublicKey      string      `json:"ssh-public-key"`
	SubnetIds         []string    `json:"subnet-ids"`
	SystemTags        interface{} `json:"system-tags"`
}

type WorkRequest struct {
	OpcWorkRequestId string `json:"opc-work-request-id"`
}

type WorkRequestGet struct {
	Data struct {
		Resources []struct {
			Identifier string `json:"identifier"`
		} `json:"resources"`
		Status string `json:"status"`
	} `json:"data"`
}

type responseWithListId struct {
	Data []struct {
		DisplayName  string `json:"display-name"`
		Id           string `json:"id"`
		FreeformTags struct {
			K8SVersion string `json:"k8s_version"`
		} `json:"freeform-tags,omitempty"`
	} `json:"data"`
}

func (o *oke) String() string {
	return SchedName
}

// Init initializes the node driver for oke under the given scheduler
func (o *oke) Init(schedOpts scheduler.InitOptions) error {

	ops, err := oracleOps.NewClient()
	if err != nil {
		return err
	}
	o.ops = ops
	err = o.K8s.Init(schedOpts)
	return err
}

func (o *oke) configureUser() error {

	if !o.isUserConfigured {
		instanceGroup := os.Getenv("INSTANCE_GROUP")
		if len(instanceGroup) != 0 {
			o.instanceGroupName = instanceGroup
		} else {
			o.instanceGroupName = "default"
		}
		compartmentId := os.Getenv("PX_ORACLE_compartment_id")
		if len(compartmentId) == 0 {
			return fmt.Errorf("compartment id not provided as env var [PX_ORACLE_compartment_id]")
		}
		o.compartmentId = compartmentId

		userKey := os.Getenv("PX_ORACLE_user_ocid")
		if len(userKey) == 0 {
			return fmt.Errorf("user key not provided as env var [PX_ORACLE_user_oci]")
		}

		fingerprint := os.Getenv("PX_ORACLE_fingerprint")
		if len(fingerprint) == 0 {
			return fmt.Errorf("fingerprint key not provided as env var [PX_ORACLE_fingerprint]")
		}
		privateKey := os.Getenv("PX_ORACLE_private_key_path")
		if len(privateKey) == 0 {
			return fmt.Errorf("private key not provided as env var [PX_ORACLE_private_key_path]")
		}
		region := os.Getenv("PX_ORACLE_cluster_region")
		if len(region) == 0 {
			return fmt.Errorf("region not provided as env var [PX_ORACLE_cluster_region]")
		}
		tenancy := os.Getenv("PX_ORACLE_tenancy")
		if len(tenancy) == 0 {
			return fmt.Errorf("tenancy not provided as env var [PX_ORACLE_tenancy]")
		}

		creatDir := fmt.Sprintf("mkdir %s", "/root/.oci")
		_, stderr, err := osutils.ExecShell(creatDir)
		if err != nil {
			return fmt.Errorf("error in creating .oci directory, error %v %v", err, stderr)
		}

		cmd := fmt.Sprintf("echo '[DEFAULT]\nuser=%s\nfingerprint=%s\ntenancy=%s\nregion=%s\nkey_file=%s' > %s", userKey, fingerprint, tenancy, region, privateKey, okeConfigFile)
		_, stderr, err = osutils.ExecShell(cmd)
		if err != nil {
			return fmt.Errorf("error in configuring User info in OCI CLI, error %v %v", err, stderr)
		}

		cmd = fmt.Sprintf("sudo chmod 400 %s", okeConfigFile)
		_, stderr, err = osutils.ExecShell(cmd)
		if err != nil {
			return fmt.Errorf("error in setting permissions for oci config, error %v %v", err, stderr)
		}

		o.isUserConfigured = true
		err = o.setCluster()
		if err != nil {
			return err
		}
	}

	return nil
}

func (o *oke) setCluster() error {
	clusterName := os.Getenv("PX_ORACLE_cluster_name")
	if len(clusterName) == 0 {
		return fmt.Errorf("cluster name not provided as env var [PX_ORACLE_cluster_name]")
	}

	cmd := fmt.Sprintf("%s ce cluster list --compartment-id %s --output json", ociCLI, o.compartmentId)
	stdout, stderr, err := osutils.ExecShell(cmd)
	if err != nil {
		return fmt.Errorf("failed to get clusters list. stderr: %v, err: %v,stdout: %v", stderr, err, stdout)
	}
	clusters := &Cluster{}
	err = json.Unmarshal([]byte(stdout), clusters)
	if err != nil {
		return err
	}

	for _, cluster := range clusters.Data {
		if cluster.Name == clusterName {
			log.Debugf("Setting cluster id to %s", cluster.ID)
			o.clusterId = cluster.ID
			return nil
		}
	}
	return nil
}

// getNodePool returns Portworx cluster node pool
func (o *oke) getNodePool() (NodePool, error) {
	cmd := fmt.Sprintf("%s ce node-pool list --compartment-id %s --cluster-id %s --output json", ociCLI, o.compartmentId, o.clusterId)
	stdout, stderr, err := osutils.ExecShell(cmd)
	if err != nil {
		return NodePool{}, fmt.Errorf("failed to get node pools list. stderr: %v, err: %v, stdout: %v", stderr, err, stdout)
	}
	nodePools := &NodePools{}
	err = json.Unmarshal([]byte(stdout), nodePools)
	if err != nil {
		return NodePool{}, err
	}

	for _, nodePool := range nodePools.Data {
		if nodePool.Name == o.instanceGroupName {
			return nodePool, nil
		}
	}
	return NodePool{}, fmt.Errorf("node pool %s not found", o.instanceGroupName)
}

// SetASGClusterSize sets node count per zone
func (o *oke) SetASGClusterSize(perZoneCount int64, timeout time.Duration) error {
	err := o.ops.SetInstanceGroupSize(o.instanceGroupName, perZoneCount, timeout)
	if err != nil {
		return fmt.Errorf("failed to set size of node pool [%s] to [%d]. Error: %v", o.instanceGroupName, perZoneCount, err)
	}

	return nil
}

// GetASGClusterSize gets node count for cluster
func (o *oke) GetASGClusterSize() (int64, error) {
	err := o.configureUser()
	if err != nil {
		return 0, err

	}
	err = o.setCluster()
	if err != nil {
		return 0, err

	}
	nodePool, err := o.getNodePool()
	if err != nil {
		return 0, fmt.Errorf("failed to get size of node pool [%s]. Error: %v", o.instanceGroupName, err)
	}
	return int64(nodePool.NodeConfigDetails.Size), nil
}

// GetZones returns list of zones in which cluster is running
func (o *oke) GetZones() ([]string, error) {
	asgInfo, err := o.ops.InspectInstanceGroupForInstance(o.ops.InstanceID())
	if err != nil {
		return []string{}, err
	}

	return asgInfo.Zones, nil
}

// UpgradeScheduler performs OKE cluster upgrade to a specified version
func (o *oke) UpgradeScheduler(version string) error {

	err := o.configureUser()
	if err != nil {
		return err

	}
	err = o.setCluster()
	if err != nil {
		return err

	}

	log.Infof("Starting OKE cluster upgrade to [%s]", version)

	err = o.setControlPlaneVersion(version)
	if err != nil {
		return err
	}
	nodePool, err := o.getNodePool()
	if err != nil {
		return err
	}
	newPoolName := fmt.Sprintf("%s-%s", o.instanceGroupName, strings.Replace(version, ".", "-", -1))
	err = o.addUpgradedNodePool(nodePool, newPoolName, version)
	if err != nil {
		return err
	}
	o.instanceGroupName = newPoolName // Update the instance group name to the new node pool name

	log.Infof("waiting for 5 mins for the new node pool to be ready")
	time.Sleep(5 * time.Minute)

	volDriver, err := volume.Get(o.VolDriverName)
	if err != nil {
		return err
	}

	err = o.RefreshNodeRegistry()
	if err != nil {
		return fmt.Errorf("error updating node registry after creating node pool [%s], err:%v", newPoolName, err)
	}
	stNodes := node.GetStorageDriverNodes()
	for _, stNode := range stNodes {
		err = volDriver.WaitForPxPodsToBeUp(stNode)
		if err != nil {
			return err
		}
		err = volDriver.WaitDriverUpOnNode(stNode, 15*time.Minute)
		if err != nil {
			return err
		}
	}
	err = volDriver.RefreshDriverEndpoints()
	if err != nil {
		return fmt.Errorf("error refreshing driver endpoints after creating node pool [%s], err:%v", newPoolName, err)
	}
	err = o.deleteNodePool(nodePool)
	if err != nil {
		return err
	}

	log.Infof("Successfully upgraded OKE cluster to [%s]", version)
	return nil
}

func (o *oke) setControlPlaneVersion(version string) error {

	log.Infof("Setting control plane version to [%s]", version)
	cmd := fmt.Sprintf("%s ce cluster update --cluster-id %s --kubernetes-version %s", ociCLI, o.clusterId, version)
	stdout, stderr, err := osutils.ExecShell(cmd)
	if err != nil {
		return fmt.Errorf("failed to set controlplane version tio [%s] . stderr: %v, err: %v, stdout: %v", version, stderr, err, stdout)
	}
	var setVersionResp WorkRequest
	err = json.Unmarshal([]byte(stdout), &setVersionResp)
	if err != nil {
		return fmt.Errorf("error while parsing set controlplane version output, error %v %v", err, stderr)
	}

	clusterId, err := o.waitForWorkRequest(setVersionResp.OpcWorkRequestId, fmt.Sprintf("Set controlplane version [%s]", version))
	if err != nil {
		return fmt.Errorf("error in waiting for work request to be over, %v", err)
	}
	logrus.Infof("Controlplane upgrade to version %s successfully for cluster id: %s", version, clusterId)
	return nil
}

// getNodeImageIdToUpgrade returns the image id for the given version
func (o *oke) getNodeImageIdToUpgrade(version string) (string, error) {

	log.Infof("Getting image id for version [%s]", version)

	nodePool, err := o.getNodePool()
	if err != nil {
		return "", err
	}
	nodeImageName := nodePool.NodeImageName
	parts := strings.Split(nodeImageName, "-")
	var nodeImageNameSiffix string
	if len(parts) >= 2 {
		nodeImageNameSiffix = fmt.Sprintf("%s-%s", parts[0], parts[1])

	} else {
		return nodeImageNameSiffix, fmt.Errorf("failed to get node image name suffix from node image [%s]", nodeImageName)
	}

	nodeShape := nodePool.NodeShape

	cmd := fmt.Sprintf("%s compute image list --compartment-id %s --all --shape %s", ociCLI, customImageCompartmentId, nodeShape)
	out, stderr, err := osutils.ExecShell(cmd)
	if err != nil {
		return "", fmt.Errorf("error while getting image list with %s, error %v %v", nodeShape, err, stderr)
	}
	var imageResponse responseWithListId
	if err := json.Unmarshal([]byte(out), &imageResponse); err != nil {
		return "", fmt.Errorf("error while parsing get Node image output, error %v %v", err, stderr)
	}
	for _, image := range imageResponse.Data {
		if strings.Contains(image.DisplayName, nodeImageNameSiffix) && image.FreeformTags.K8SVersion == version {
			return image.Id, nil
		}
	}
	return "", fmt.Errorf("failed to get image id for node image [%s] and version [%s]", nodeImageNameSiffix, version)
}

func (o *oke) addUpgradedNodePool(nodePool NodePool, name, version string) error {
	nodeImageId, err := o.getNodeImageIdToUpgrade(version)
	if err != nil {
		return fmt.Errorf("failed to get node image ID for version [%s], Err: %v", version, err)
	}

	log.Infof("Adding node-pool with %s with node count [%d]", name, nodePool.NodeConfigDetails.Size)
	placementConfig := map[string]string{}
	placementConfig["availability-domain"] = nodePool.NodeConfigDetails.PlacementConfigs[0].AvailabilityDomain
	placementConfig["subnet-id"] = nodePool.NodeConfigDetails.PlacementConfigs[0].SubnetID
	placementJson, err := json.Marshal(placementConfig)

	nodeShapeConfig := map[string]float32{}
	nodeShapeConfig["memory-in-gbs"] = float32(nodePool.NodeShapeConfig.MemoryInGBs)

	nodeShapeConfig["ocpus"] = float32(nodePool.NodeShapeConfig.Ocpus)
	nodeshapeJson, err := json.Marshal(nodeShapeConfig)

	var cmd string
	cmd = fmt.Sprintf("%s ce node-pool create --cluster-id %s --compartment-id %s --kubernetes-version %s --name %s --node-shape %s --placement-configs '[%v]' --node-shape-config '%v' --size %d --node-image-id %s",
		ociCLI, o.clusterId, o.compartmentId, version, name, nodePool.NodeShape, string(placementJson), string(nodeshapeJson), nodePool.NodeConfigDetails.Size, nodeImageId)

	out, stderr, err := osutils.ExecShell(cmd)
	if err != nil {
		return fmt.Errorf("error while create node-pool %s, error %v %v", name, err, stderr)
	}
	var addNodePoolResp WorkRequest
	err = json.Unmarshal([]byte(out), &addNodePoolResp)
	if err != nil {
		return fmt.Errorf("error while parsing nodepool create output, error %v %v", err, stderr)
	}
	nodePoolId, err := o.waitForWorkRequest(addNodePoolResp.OpcWorkRequestId, fmt.Sprintf("Node Pool Add - %s", name))
	if err != nil {
		return fmt.Errorf("error in waiting for work request to be over, %v", err)
	}
	log.Infof("node-pool %s created successfully, id %s", name, nodePoolId)
	return nil
}

func (o *oke) deleteNodePool(nodePool NodePool) error {

	cmd := fmt.Sprintf("%s ce node-pool delete --node-pool-id %s --force", ociCLI, nodePool.ID)
	out, stderr, err := osutils.ExecShell(cmd)
	if err != nil {
		return fmt.Errorf("error while deleting node-pool %s, error %v %v", nodePool.Name, err, stderr)
	}
	var nodePoolDeleteResp WorkRequest
	err = json.Unmarshal([]byte(out), &nodePoolDeleteResp)
	if err != nil {
		return fmt.Errorf("error while parsing nodepool delete output, error %v %v", err, stderr)
	}
	_, err = o.waitForWorkRequest(nodePoolDeleteResp.OpcWorkRequestId, fmt.Sprintf("Node Pool delete - %s", nodePool.Name))
	if err != nil {
		return fmt.Errorf("error in waiting for work request to be over, %v", err)
	}

	return nil

}

// DeleteNode deletes the given node
func (o *oke) DeleteNode(node node.Node, timeout time.Duration) error {
	err := o.configureUser()
	if err != nil {
		return err

	}
	err = o.setCluster()
	if err != nil {
		return err

	}
	log.Infof("Deleting node [%s]", node.Hostname)
	instanceDetails, err := o.ops.GetInstance(node.Hostname)
	if err != nil {
		return err
	}

	oracleInstance, ok := instanceDetails.(core.Instance)
	if !ok {
		return fmt.Errorf("could not retrive oke instance details for %s", node.Hostname)
	}
	err = o.ops.DeleteInstance(*oracleInstance.Id, "", timeout)
	if err != nil {
		return err
	}
	return nil
}

func init() {
	o := &oke{
		SSH: *ssh.New(),
	}
	scheduler.Register(SchedName, o)
}

func (o *oke) waitForWorkRequest(workRequestId string, taskName string) (string, error) {
	logrus.Infof("Query status of %s", workRequestId)
	var workRequestGet WorkRequestGet
	t := func() (interface{}, bool, error) {
		cmd := fmt.Sprintf("%s ce work-request get --work-request-id %s", ociCLI, workRequestId)
		stdout, stderr, err := osutils.ExecShell(cmd)
		err = json.Unmarshal([]byte(stdout), &workRequestGet)
		if err != nil {
			return nil, false, fmt.Errorf("error while parsing work request get output, error %v %v", err, stderr)
		}
		if workRequestGet.Data.Status == inprogressStatus || workRequestGet.Data.Status == acceptedStatus {
			return nil, true, fmt.Errorf("status for: %s is: %s", taskName, workRequestGet.Data.Status)
		}
		logrus.Infof("Work Request status is %s", workRequestGet.Data.Status)
		if workRequestGet.Data.Status == failedStatus {
			return nil, false, fmt.Errorf("work request failed: %s", workRequestId)
		}
		return nil, false, nil
	}
	if _, err := task.DoRetryWithTimeout(t, workerTimeout, workerRetryTime); err != nil {
		return "", err
	}
	return workRequestGet.Data.Resources[0].Identifier, nil
}
