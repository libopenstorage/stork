package ibm

import (
	"encoding/json"
	"fmt"
	"github.com/portworx/sched-ops/k8s/core"
	"github.com/portworx/torpedo/pkg/osutils"
	"os"
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
	iksPXWorkerpool             = "default"
)

const (
	// DELETED state of the deleted node
	DELETED = "deleted"
	// PROVISIONING state of the node when provisioning
	PROVISIONING = "provisioning"
	// DEPLOYING state of the node when deploying
	DEPLOYING = "deploying"
	// DEPLOYED state of the node when deployed
	DEPLOYED = "deployed"
)

type ibm struct {
	ssh.SSH
	ops           cloudops.Ops
	instanceGroup string
	clusterConfig ClusterConfig
}

// Worker stores info about iks worker node as proviced by IBM
type Worker struct {
	WorkerID  string `json:"id"`
	Lifecycle struct {
		ActualState string `json:"actualState"`
	} `json:"lifecycle"`
	Health struct {
		State   string `json:"state"`
		Message string `json:"message"`
	} `json:"health"`
	PoolID   string `json:"poolID"`
	PoolName string `json:"poolName"`
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

func (i *ibm) RebalanceWorkerPool() error {

	err := loginToIBMCloud()
	if err != nil {
		return err
	}

	cmd := fmt.Sprintf("ibmcloud ks worker-pool rebalance -c %s -p %s", i.clusterConfig.ClusterName, iksPXWorkerpool)
	stdout, stderr, err := osutils.ExecShell(cmd)
	if err != nil {
		return fmt.Errorf("failed to rebalance worker pool %s. Error: %v %v %v", iksPXWorkerpool, stderr, err, stdout)
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

func init() {
	i := &ibm{
		SSH: *ssh.New(),
	}

	node.Register(DriverName, i)
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
