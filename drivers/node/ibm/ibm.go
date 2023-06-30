package ibm

import (
	"encoding/json"
	"fmt"
	"github.com/portworx/sched-ops/k8s/core"
	"github.com/portworx/sched-ops/task"
	"github.com/portworx/torpedo/pkg/osutils"
	"os"
	"strings"
	"time"

	"github.com/portworx/torpedo/pkg/log"

	"github.com/libopenstorage/cloudops"
	iks "github.com/libopenstorage/cloudops/ibm"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/node/ssh"
)

const (
	// DriverName is the name of the ibm driver
	DriverName                  = "ibm"
	ibmAPIKey                   = "IBMCLOUD_API_KEY"
	ibmRegion                   = "us-south"
	ibmResourceGroup            = "Portworx-RG"
	iksClusterInfoConfigMapName = "cluster-info"
	clusterIDconfigMapField     = "cluster-config.json"
	IksPXWorkerpool             = "default"
)

const (
	// DELETED state of the deleted node
	DELETED = "deleted"
	// PROVISIONING state of the node when provisioning
	PROVISIONING = "provisioning"
	// PROVISION_PENDING state of the node when provisioning is pending
	PROVISION_PENDING = "provision_pending"
	// DEPLOYING state of the node when deploying
	DEPLOYING = "deploying"
	// DEPLOYED state of the node when deployed
	DEPLOYED = "deployed"
)

const (
	clusterStateRetryTimeout  = 180 * time.Minute
	clusterStateRetryInterval = 2 * time.Minute
)

type ibm struct {
	ssh.SSH
	ops           cloudops.Ops
	instanceGroup string
	clusterConfig ClusterConfig
}

// Worker stores info about iks worker node as provided by IBM
type Worker struct {
	WorkerID  string `json:"id"`
	Lifecycle struct {
		ActualState string `json:"actualState"`
	} `json:"lifecycle"`
	Health struct {
		State   string `json:"state"`
		Message string `json:"message"`
	} `json:"health"`
	KubeVersion struct {
		Actual string `json:"actual"`
	}
	PoolID            string             `json:"poolID"`
	PoolName          string             `json:"poolName"`
	Location          string             `json:"location"`
	NetworkInterfaces []NetworkInterface `json:"networkInterfaces"`
}

type NetworkInterface struct {
	SubnetID  string `json:"subnetID"`
	IpAddress string `json:"ipAddress"`
	CIDR      string `json:"cidr"`
	Primary   bool   `json:"primary"`
}

type Cluster struct {
	ClusterID         string `json:"id"`
	Name              string `json:"name"`
	MasterKubeVersion string `json:"masterKubeVersion"`
	State             string `json:"state"`
	Status            string `json:"status"`
}

// ClusterConfig stores info about iks cluster as provided by IBM
type ClusterConfig struct {
	ClusterID   string `json:"cluster_id"`
	ClusterName string `json:"name"`
}

func (i *ibm) String() string {
	return DriverName
}

func (i *ibm) Init(nodeOpts node.InitOptions) error {
	i.SSH.Init(nodeOpts)

	instanceGroup := os.Getenv("INSTANCE_GROUP")
	if len(instanceGroup) != 0 {
		i.instanceGroup = instanceGroup
	} else {
		i.instanceGroup = "default"
	}

	ops, err := iks.NewClient()
	if err != nil {
		return err
	}
	i.ops = ops
	cm, err := core.Instance().GetConfigMap(iksClusterInfoConfigMapName, "kube-system")
	if err != nil {
		return err
	}

	clusterInfo := &ClusterConfig{}
	err = json.Unmarshal([]byte(cm.Data[clusterIDconfigMapField]), clusterInfo)
	if err != nil {
		return err
	}
	i.clusterConfig = *clusterInfo

	return nil
}

func (i *ibm) SetASGClusterSize(perZoneCount int64, timeout time.Duration) error {
	// IBM SDK requires per zone cluster size
	err := i.ops.SetInstanceGroupSize(i.instanceGroup, perZoneCount, timeout)
	if err != nil {
		log.Errorf("failed to set size of node pool %s. Error: %v", i.instanceGroup, err)
		return err
	}

	return nil
}

func (i *ibm) GetASGClusterSize() (int64, error) {
	nodeCount, err := i.ops.GetInstanceGroupSize(i.instanceGroup)
	if err != nil {
		log.Errorf("failed to get size of node pool %s. Error: %v", i.instanceGroup, err)
		return 0, err
	}

	return nodeCount, nil
}

func (i *ibm) GetZones() ([]string, error) {
	asgInfo, err := i.ops.InspectInstanceGroupForInstance(i.ops.InstanceID())
	if err != nil {
		return []string{}, err
	}
	return asgInfo.Zones, nil
}

func (i *ibm) DeleteNode(node node.Node, timeout time.Duration) error {

	err := loginToIBMCloud()
	if err != nil {
		return err
	}

	cmd := fmt.Sprintf("ibmcloud ks worker rm -c %s -w %s -f", i.clusterConfig.ClusterName, node.Hostname)
	stdout, stderr, err := osutils.ExecShell(cmd)
	if err != nil {
		return fmt.Errorf("failed to delete node [%s]. Error: %v %v %v", node.Hostname, stderr, err, stdout)
	}

	return nil
}

func (i *ibm) SetClusterVersion(version string, timeout time.Duration) error {

	err := loginToIBMCloud()
	if err != nil {
		return err
	}

	cmd := fmt.Sprintf("ibmcloud ks cluster master update -c %s --version %s -f", i.clusterConfig.ClusterName, version)
	stdout, stderr, err := osutils.ExecShell(cmd)
	if err != nil {
		return fmt.Errorf("failed to set cluser %s master version to %s. Error: %v %v %v", i.clusterConfig.ClusterName, version, stderr, err, stdout)
	}
	log.Infof("Node group version set successfully.")

	return nil
}

func (i *ibm) RebalanceWorkerPool() error {

	err := loginToIBMCloud()
	if err != nil {
		return err
	}

	cmd := fmt.Sprintf("ibmcloud ks worker-pool rebalance -c %s -p %s", i.clusterConfig.ClusterName, IksPXWorkerpool)
	stdout, stderr, err := osutils.ExecShell(cmd)
	if err != nil {
		return fmt.Errorf("failed to rebalance worker pool %s. Error: %v %v %v", IksPXWorkerpool, stderr, err, stdout)
	}

	return nil
}

// GetNodeState returns current state of the given node
func (i *ibm) GetNodeState(node node.Node) (string, error) {
	err := loginToIBMCloud()
	if err != nil {
		return "", err
	}

	cmd := fmt.Sprintf("ibmcloud ks worker get -w %s -c %s --output json", node.Hostname, i.clusterConfig.ClusterName)
	stdout, stderr, err := osutils.ExecShell(cmd)
	if err != nil {
		return "", fmt.Errorf("failed node [%s] info. Error: %v %v %v", node.Hostname, stderr, err, stdout)
	}
	worker := &Worker{}
	err = json.Unmarshal([]byte(stdout), worker)
	if err != nil {
		return "", err
	}
	return worker.Lifecycle.ActualState, nil
}

func GetWorkers() ([]Worker, error) {
	err := loginToIBMCloud()
	if err != nil {
		return nil, err
	}

	cm, err := core.Instance().GetConfigMap(iksClusterInfoConfigMapName, "kube-system")
	if err != nil {
		return nil, err
	}

	clusterInfo := &ClusterConfig{}
	err = json.Unmarshal([]byte(cm.Data[clusterIDconfigMapField]), clusterInfo)
	if err != nil {
		return nil, err
	}
	clusterName := clusterInfo.ClusterName

	cmd := fmt.Sprintf("ibmcloud ks worker ls -c %s --output json", clusterName)
	stdout, stderr, err := osutils.ExecShell(cmd)
	if err != nil {
		return nil, fmt.Errorf("failed to get workers list info. Error: %v %v %v", stderr, err, stdout)
	}
	var workers []Worker
	err = json.Unmarshal([]byte(stdout), &workers)
	if err != nil {
		return nil, err
	}
	return workers, nil
}

func ReplaceWorkerNodeWithUpdate(node node.Node) error {

	err := loginToIBMCloud()
	if err != nil {
		return err
	}
	cm, err := core.Instance().GetConfigMap(iksClusterInfoConfigMapName, "kube-system")
	if err != nil {
		return err
	}

	clusterInfo := &ClusterConfig{}
	err = json.Unmarshal([]byte(cm.Data[clusterIDconfigMapField]), clusterInfo)
	if err != nil {
		return err
	}
	clusterName := clusterInfo.ClusterName

	cmd := fmt.Sprintf("ibmcloud ks worker replace -c %s -w %s --update -f", clusterName, node.Hostname)
	stdout, stderr, err := osutils.ExecShell(cmd)
	if err != nil {
		return fmt.Errorf("failed to replace node [%s]. Error: %v %v %v", node.Hostname, stderr, err, stdout)
	}

	return nil

}

// RemoveWorkerNode delete given node from the cluster
func RemoveWorkerNode(node node.Node) error {

	err := loginToIBMCloud()
	if err != nil {
		return err
	}
	cm, err := core.Instance().GetConfigMap(iksClusterInfoConfigMapName, "kube-system")
	if err != nil {
		return err
	}

	clusterInfo := &ClusterConfig{}
	err = json.Unmarshal([]byte(cm.Data[clusterIDconfigMapField]), clusterInfo)
	if err != nil {
		return err
	}
	clusterName := clusterInfo.ClusterName

	cmd := fmt.Sprintf("ibmcloud ks worker rm -c %s -w %s -f", clusterName, node.Hostname)
	stdout, stderr, err := osutils.ExecShell(cmd)
	if err != nil {
		return fmt.Errorf("failed to remove node [%s]. Error: %v %v %v", node.Hostname, stderr, err, stdout)
	}

	return nil

}

func GetCluster() (Cluster, error) {
	err := loginToIBMCloud()
	if err != nil {
		return Cluster{}, err
	}
	cm, err := core.Instance().GetConfigMap(iksClusterInfoConfigMapName, "kube-system")
	if err != nil {
		return Cluster{}, err
	}

	clusterInfo := &ClusterConfig{}
	err = json.Unmarshal([]byte(cm.Data[clusterIDconfigMapField]), clusterInfo)
	if err != nil {
		return Cluster{}, err
	}
	clusterName := clusterInfo.ClusterName

	cmd := fmt.Sprintf("ibmcloud ks cluster ls  --provider vpc-gen2 --output json")
	stdout, stderr, err := osutils.ExecShell(cmd)
	if err != nil {
		return Cluster{}, fmt.Errorf("failed to get workers list info. Error: %v %v %v", stderr, err, stdout)
	}
	var clusters []Cluster
	err = json.Unmarshal([]byte(stdout), &clusters)
	if err != nil {
		return Cluster{}, err
	}

	for _, cluster := range clusters {
		if cluster.Name == clusterName {
			return cluster, nil
		}
	}
	return Cluster{}, fmt.Errorf("IKS Cluster %s not found", clusterName)
}

func init() {
	i := &ibm{
		SSH: *ssh.New(),
	}

	node.Register(DriverName, i)
}

func AddWorkerPool(poolName string, size int64) error {

	err := loginToIBMCloud()
	if err != nil {
		return err
	}
	cm, err := core.Instance().GetConfigMap(iksClusterInfoConfigMapName, "kube-system")
	if err != nil {
		return err
	}

	clusterInfo := &ClusterConfig{}
	err = json.Unmarshal([]byte(cm.Data[clusterIDconfigMapField]), clusterInfo)
	if err != nil {
		return err
	}
	clusterName := clusterInfo.ClusterName

	cmd := fmt.Sprintf("ibmcloud ks worker-pool create vpc-gen2 --name %s --cluster %s --flavor %s --size-per-zone %d",
		poolName, clusterName, "bx2.4x16", size)
	stdout, stderr, err := osutils.ExecShell(cmd)
	if err != nil {
		return fmt.Errorf("failed to add worker pool [%s]. Error: %v %v %v", poolName, stderr, err, stdout)
	}
	log.InfoD("Added worker pool [%s] successfully.", poolName)
	err = addZones(clusterName, poolName)
	if err != nil {
		return err
	}
	log.InfoD("Waiting for upto %v for cluster to become ready.", clusterStateRetryTimeout)

	err = waitForClusterReady(clusterName)
	if err != nil {
		return err
	}

	log.InfoD("Waiting for upto %v for all worker nodes to become ready.", clusterStateRetryTimeout)
	return waitForWorkersToBeReady(clusterName)

}

func addZones(clusterName, workerPoolName string) error {
	workers, err := GetWorkers()
	if err != nil {
		return err
	}
	zoneSubnetMap := make(map[string]string)
	for _, worker := range workers {
		if worker.PoolName == IksPXWorkerpool {
			if _, ok := zoneSubnetMap[worker.Location]; !ok {
				zoneSubnetMap[worker.Location] = worker.NetworkInterfaces[0].SubnetID
			}
		}
	}

	for k, v := range zoneSubnetMap {
		cmd := fmt.Sprintf("ibmcloud ks zone add vpc-gen2 --zone %s --cluster %s --worker-pool %s --subnet-id %s",
			k, clusterName, workerPoolName, v)
		stdout, stderr, err := osutils.ExecShell(cmd)
		if err != nil {
			return fmt.Errorf("failed to add zone. Error: %v %v %v", stderr, err, stdout)
		}
		log.Infof("Added zone [%s] successfully.", k)
	}
	return nil
}

func waitForClusterReady(clusterName string) error {
	t := func() (interface{}, bool, error) {

		cmd := fmt.Sprintf("ibmcloud ks cluster get -c %s --output json | jq -r .state", clusterName)
		stdout, stderr, err := osutils.ExecShell(cmd)
		if err != nil {
			return nil, false, fmt.Errorf("failed to get cluster state. Error: %v %v %v", stderr, err, stdout)
		}
		clusterState := strings.TrimSpace(stdout)
		log.Infof("Cluster [%s] is in state : %s", clusterName, clusterState)
		if clusterState == "deploying" {
			return nil, true, fmt.Errorf("cluster %s is still [%s]", clusterName, clusterState)
		}

		if clusterState == "normal" || clusterState == "warning" {
			log.Infof("Cluster %s is [%s]", clusterName, clusterState)
			return nil, false, nil
		}

		return nil, true, fmt.Errorf("cluster state does not match [normal] or [deploying]")
	}

	_, err := task.DoRetryWithTimeout(t, clusterStateRetryTimeout, clusterStateRetryInterval)
	if err != nil {
		return err
	}
	return nil
}

func waitForWorkersToBeReady(clusterName string) error {
	workers, err := GetWorkers()
	if err != nil {
		return err
	}

	t := func() (interface{}, bool, error) {
		for _, worker := range workers {

			workerDetails, err := getWorkerDetails(clusterName, worker.WorkerID)
			if err != nil {
				return nil, false, fmt.Errorf("failed to get state of worker [%s]. Error: %v", worker.WorkerID, err)
			}
			log.Infof("Worker [%s] actual state is [%s] and status is [%s]",
				worker.WorkerID, worker.Lifecycle.ActualState, workerDetails.Health.Message)

			if workerDetails.Health.State != "normal" &&
				workerDetails.Health.Message != "Ready" {
				return nil, true, fmt.Errorf("worker %s is still in actual state [%s], health [%s] and status [%s]",
					worker.WorkerID, worker.Lifecycle.ActualState, workerDetails.Health.State, workerDetails.Health.Message)
			}

		}

		return nil, false, nil
	}

	_, err = task.DoRetryWithTimeout(t, clusterStateRetryTimeout, clusterStateRetryInterval)
	if err != nil {
		return err
	}
	return nil
}

func getWorkerDetails(clusterName string, workerName string) (Worker, error) {
	details := Worker{}
	cmd := fmt.Sprintf("ibmcloud ks worker get -c %s -w %s --output json",
		clusterName, workerName)
	stdout, stderr, err := osutils.ExecShell(cmd)
	if err != nil {
		return details,
			fmt.Errorf("failed to get state of worker [%s]. Error: %v %v %v", workerName, stderr, err, stdout)
	}

	if err := json.Unmarshal([]byte(stdout), &details); err != nil {
		return details, fmt.Errorf("failed to unmarshal worker details. Error: [%v]", err)
	}
	return details, nil
}

func loginToIBMCloud() error {

	apiKey := os.Getenv(ibmAPIKey)
	if len(apiKey) == 0 {
		return fmt.Errorf("IKS API key not provided as env var: %s", apiKey)
	}

	cmd := fmt.Sprintf("ibmcloud login --apikey %s -g %s -r %s", apiKey, ibmResourceGroup, ibmRegion)
	stdout, stderr, err := osutils.ExecShell(cmd)
	if err != nil {
		return fmt.Errorf("failed to login to IBM cloud. Error: %v %v %v", stderr, err, stdout)
	}
	log.Info("Logged-in to IBM cloud.")

	cmd = "ibmcloud is target --gen 2"
	stdout, stderr, err = osutils.ExecShell(cmd)
	if err != nil {
		return fmt.Errorf("failed to set gen2 as default generation. Error: %v %v %v", stderr, err, stdout)
	}
	log.Debug("Successfully set Gen 2 as default generation.")
	return nil
}
