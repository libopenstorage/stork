package ibm

import (
	"encoding/json"
	"fmt"
	"github.com/sirupsen/logrus"
	"time"

	bluemix "github.com/IBM-Cloud/bluemix-go"
	v2 "github.com/IBM-Cloud/bluemix-go/api/container/containerv2"
	"github.com/IBM-Cloud/bluemix-go/session"
	"github.com/libopenstorage/cloudops"
	"github.com/libopenstorage/cloudops/backoff"
	"github.com/libopenstorage/cloudops/unsupported"
	core "github.com/portworx/sched-ops/k8s/core"
	"github.com/portworx/sched-ops/task"
	"k8s.io/client-go/rest"
)

const (
	labelWorkerPoolName         = "ibm-cloud.kubernetes.io/worker-pool-name"
	labelWorkerPoolID           = "ibm-cloud.kubernetes.io/worker-pool-id"
	labelWorkerID               = "ibm-cloud.kubernetes.io/worker-id"
	labelInstanceID             = "ibm-cloud.kubernetes.io/instance-id"
	vpcProviderName             = "vpc-gen2"
	iksClusterInfoConfigMapName = "cluster-info"
	clusterIDconfigMapField     = "cluster-config.json"
	expectedWorkerHealthState   = "normal"
	expectedWorkerHealthMsg     = "Ready"
)

const retrySeconds = 15

// ClusterConfig stores info about iks cluster as provided by IBM
type ClusterConfig struct {
	ClusterID string `json:"cluster_id"`
}
type ibmOps struct {
	cloudops.Compute
	cloudops.Storage
	ibmClusterClient v2.ContainerServiceAPI
	inst             *instance
}

// instance stores the metadata of the running ibm instance
type instance struct {
	name            string
	hostname        string
	zone            string
	region          string
	clusterName     string
	clusterLocation string
	nodePoolID      string
}

// NewClient creates a new IBM operations client
func NewClient() (cloudops.Ops, error) {

	var i = new(instance)

	c := new(bluemix.Config)

	sess, err := session.New(c)
	if err != nil {
		return nil, fmt.Errorf("failed to get session. error: [%v]", err)
	}

	ibmClusterClient, err := v2.New(sess)
	if err != nil {
		return nil, fmt.Errorf("failed to get ibm cluster client. error: [%v]", err)
	}
	var instanceName, clusterName string
	if IsDevMode() {
		instanceName, clusterName, err = getInfoFromEnv()
		if err != nil {
			return nil, fmt.Errorf("failed to get cluster info from environment variables. error: [%v] ", err)
		}
	} else {
		instanceName, clusterName, err = getIBMInfo()
		if err != nil {
			return nil, fmt.Errorf("failed to get cluster info. error: [%v] ", err)
		}
	}

	i.name = instanceName
	i.clusterName = clusterName

	return backoff.NewExponentialBackoffOps(
		&ibmOps{
			Compute:          unsupported.NewUnsupportedCompute(),
			Storage:          unsupported.NewUnsupportedStorage(),
			ibmClusterClient: ibmClusterClient,
			inst:             i,
		},
		isExponentialError,
		backoff.DefaultExponentialBackoff,
	), nil
}

func (i *ibmOps) Name() string {
	return string(cloudops.IBM)
}

func (i *ibmOps) InstanceID() string {
	return i.inst.name
}

func isExponentialError(err error) bool {
	return true
}

func (i *ibmOps) InspectInstance(instanceID string) (*cloudops.InstanceInfo, error) {
	target := v2.ClusterTargetHeader{
		Provider: vpcProviderName,
	}
	workerDetails, err := i.ibmClusterClient.Workers().Get(i.inst.clusterName, instanceID, target)
	if err != nil {
		return nil, err
	}

	instanceInfo := &cloudops.InstanceInfo{
		CloudResourceInfo: cloudops.CloudResourceInfo{
			Name: workerDetails.ID,
			Labels: map[string]string{
				labelWorkerPoolName: workerDetails.PoolName,
				labelWorkerPoolID:   workerDetails.PoolID,
			},
			Zone:   workerDetails.Location,
			Region: workerDetails.Location,
		},
	}
	return instanceInfo, nil
}

func (i *ibmOps) InspectInstanceGroupForInstance(instanceID string) (*cloudops.InstanceGroupInfo, error) {
	instanceInfo, err := i.InspectInstance(instanceID)
	if err != nil {
		return nil, err
	}
	var instGroupInfo *cloudops.InstanceGroupInfo
	if workerPoolID, ok := instanceInfo.Labels[labelWorkerPoolID]; ok {
		target := v2.ClusterTargetHeader{
			Provider: vpcProviderName,
		}
		workerPoolDetails, err := i.ibmClusterClient.WorkerPools().
			GetWorkerPool(i.inst.clusterName, workerPoolID, target)
		if err != nil {
			return nil, err
		}

		var zones []string
		for _, z := range workerPoolDetails.Zones {
			zones = append(zones, z.ID)
		}

		instGroupInfo = &cloudops.InstanceGroupInfo{
			CloudResourceInfo: cloudops.CloudResourceInfo{
				Name:   workerPoolDetails.PoolName,
				ID:     workerPoolDetails.ID,
				Labels: workerPoolDetails.Labels,
			},
			Zones: zones,
		}

		return instGroupInfo, nil
	}
	return instGroupInfo, fmt.Errorf("no [%s] label found for instance [%s]", labelWorkerPoolID, instanceID)
}

// IsDevMode checks if the pkg is invoked in
// developer mode where IBM credentials are set as env variables
func IsDevMode() bool {
	_, _, err := getInfoFromEnv()
	if err != nil {
		return false
	}
	return true
}

func getInfoFromEnv() (string, string, error) {
	instanceName, err := cloudops.GetEnvValueStrict("IBM_INSTANCE_NAME")
	if err != nil {
		return "", "", err
	}

	clusterName, err := cloudops.GetEnvValueStrict("IBM_CLUSTER_NAME")
	if err != nil {
		return "", "", err
	}
	return instanceName, clusterName, nil
}

func getIBMInfo() (string, string, error) {
	k8sCore := core.Instance()
	kubeconfig, err := rest.InClusterConfig()
	if err != nil {
		return "", "", err
	}

	k8sCore.SetConfig(kubeconfig)
	cm, err := k8sCore.GetConfigMap(iksClusterInfoConfigMapName, "kube-system")
	if err != nil {
		return "", "", err
	}

	clusterInfo := &ClusterConfig{}
	err = json.Unmarshal([]byte(cm.Data[clusterIDconfigMapField]), clusterInfo)
	if err != nil {
		return "", "", err
	}

	nodeName, err := cloudops.GetEnvValueStrict("NODE_NAME")
	if err != nil {
		return "", "", err
	}

	labels, err := k8sCore.GetLabelsOnNode(nodeName)
	if err != nil {
		return "", "", err
	}
	return labels[labelWorkerID], clusterInfo.ClusterID, nil
}

// SetInstanceGroupSize sets node count for a instance group.
// Count here is per availability zone
func (i *ibmOps) SetInstanceGroupSize(instanceGroupID string,
	count int64, timeout time.Duration) error {

	req := v2.ResizeWorkerPoolReq{
		Cluster:    i.inst.clusterName,
		Workerpool: instanceGroupID,
		Size:       count,
	}
	target := v2.ClusterTargetHeader{
		Provider: vpcProviderName,
	}
	err := i.ibmClusterClient.WorkerPools().ResizeWorkerPool(req, target)
	if err != nil {
		return err
	}

	err = i.waitForInstanceGroupResize(instanceGroupID, count, timeout)
	return err
}

// GetInstanceGroupSize returns current node count of given instance group
func (i *ibmOps) GetInstanceGroupSize(instanceGroupID string) (int64, error) {
	target := v2.ClusterTargetHeader{
		Provider: vpcProviderName,
	}
	workerPoolDetails, err := i.ibmClusterClient.WorkerPools().
		GetWorkerPool(i.inst.clusterName, instanceGroupID, target)
	if err != nil {
		return 0, err
	}

	return int64(workerPoolDetails.WorkerCount * len(workerPoolDetails.Zones)), nil
}

func (i *ibmOps) getCurrentWorkers(instanceGroupID string) ([]v2.Worker, error) {
	var currentWorkers []v2.Worker
	target := v2.ClusterTargetHeader{
		Provider: vpcProviderName,
	}
	workerList, err := i.ibmClusterClient.Workers().
		ListByWorkerPool(i.inst.clusterName, instanceGroupID, false, target)
	if err != nil {
		return nil, err
	}

	for _, worker := range workerList {
		workerGet, err := i.ibmClusterClient.Workers().Get(i.inst.clusterName, worker.ID, target)
		if err != nil {
			return nil, err
		}

		if workerGet.LifeCycle.ReasonForDelete != "" {
			// If node is in process of getting deleted, that
			// means node is still present. Include it in the
			// list of current nodes
			currentWorkers = append(currentWorkers, worker)
			continue
		}

		if workerGet.LifeCycle.ActualState == workerGet.LifeCycle.DesiredState &&
			workerGet.Health.State == expectedWorkerHealthState &&
			workerGet.Health.Message == expectedWorkerHealthMsg {
			currentWorkers = append(currentWorkers, worker)
		}
	}
	return currentWorkers, nil
}

func (i *ibmOps) waitForInstanceGroupResize(instanceGroupID string,
	count int64, timeout time.Duration) error {
	target := v2.ClusterTargetHeader{
		Provider: vpcProviderName,
	}

	workerPoolDetails, err := i.ibmClusterClient.WorkerPools().
		GetWorkerPool(i.inst.clusterName, instanceGroupID, target)
	if err != nil {
		return err
	}

	expectedWorkerCount := len(workerPoolDetails.Zones) * int(count)
	if timeout > time.Nanosecond {
		f := func() (interface{}, bool, error) {

			currentWorkers, err := i.getCurrentWorkers(instanceGroupID)
			if err != nil {
				// Error occured, just retry
				return nil, true, err
			}
			// The operation is done
			if len(currentWorkers) == expectedWorkerCount {
				return nil, false, nil
			}
			return nil,
				true,
				fmt.Errorf("number of current worker nodes [%d]. Waiting for [%d] nodes to become HEALTHY",
					len(currentWorkers), expectedWorkerCount)
		}

		_, err = task.DoRetryWithTimeout(f, timeout, retrySeconds*time.Second)
		if err != nil {
			return err
		}
	}
	return nil
}

func (i *ibmOps) DeleteInstance(instanceID string, zone string, timeout time.Duration) error {

	logrus.Infof("Cluster name: %s, Instance ID: %s", i.inst.clusterName, instanceID)
	labels, err := core.Instance().GetLabelsOnNode(instanceID)
	if err != nil {
		return err
	}
	instanceID = labels[labelInstanceID]
	logrus.Infof("Got instance id %s", instanceID)
	req := v2.InstanceDeleteConfig{
		Cluster: i.inst.clusterName,
		Name:    instanceID,
	}
	resp, err := i.ibmClusterClient.Ingresses().GetIngressInstanceList(i.inst.clusterName, false)
	if err != nil {
		logrus.Errorf("got error getting instances, err %v", err)
	}

	logrus.Infof("Instances details : %+v", resp)
	err = i.ibmClusterClient.Ingresses().DeleteIngressInstance(req)
	if err != nil {
		logrus.Errorf("got error while deleting instance, err %v", err)
		return err
	}

	return nil
}
