package iks

import (
	"fmt"
	"github.com/IBM-Cloud/bluemix-go"
	"github.com/IBM-Cloud/bluemix-go/api/container/containerv2"
	"github.com/IBM-Cloud/bluemix-go/rest"
	"github.com/IBM-Cloud/bluemix-go/session"
	"github.com/IBM-Cloud/container-services-go-sdk/kubernetesserviceapiv1"
	"github.com/IBM/go-sdk-core/v5/core"
	"github.com/libopenstorage/cloudops"
	iks "github.com/libopenstorage/cloudops/ibm"
	k8sCore "github.com/portworx/sched-ops/k8s/core"
	"github.com/portworx/sched-ops/task"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/scheduler"
	kube "github.com/portworx/torpedo/drivers/scheduler/k8s"
	"github.com/portworx/torpedo/pkg/log"
	"os"
	"strings"
	"sync"
	"time"
)

const (
	// SchedName is the name of the IKS scheduler driver implementation
	SchedName = "iks"
	// defaultIKSUpgradeTimeout is the default timeout for IKS control plane upgrade
	defaultIKSUpgradeTimeout = 90 * time.Minute
	// defaultIKSUpgradeRetryInterval is the default retry interval for IKS control plane upgrade
	defaultIKSUpgradeRetryInterval = 5 * time.Minute
)

type IKS struct {
	kube.K8s
	ops              cloudops.Ops
	clusterName      string
	region           string
	k8sClient        *kubernetesserviceapiv1.KubernetesServiceApiV1
	containerClient  containerv2.ContainerServiceAPI
	resourceGroupID  string
	pxWorkerPoolName string
}

// String returns the string name of this driver.
func (i *IKS) String() string {
	return SchedName
}

func (i *IKS) Init(schedOpts scheduler.InitOptions) (err error) {
	err = i.K8s.Init(schedOpts)
	if err != nil {
		return err
	}

	ops, err := iks.NewClient()
	if err != nil {
		return err
	}
	i.ops = ops
	return nil
}

// GetCurrentVersion returns the current version of the IKS cluster
func (i *IKS) GetCurrentVersion() (string, error) {
	clusterResponse, detailedResponse, err := i.k8sClient.GetCluster(&kubernetesserviceapiv1.GetClusterOptions{
		Cluster: core.StringPtr(i.clusterName),
	})
	if err != nil {
		return "", fmt.Errorf("failed to get IKS cluster [%s] details. Resp: [%v], Err: [%v]", i.clusterName, detailedResponse, err)
	}
	return core.StringNilMapper(clusterResponse.MasterKubeVersion), nil
}

// GetResourceGroupID returns the resource group ID of the IKS cluster
func (i *IKS) GetResourceGroupID() (string, error) {
	clusterResponse, detailedResponse, err := i.k8sClient.GetCluster(&kubernetesserviceapiv1.GetClusterOptions{
		Cluster: core.StringPtr(i.clusterName),
	})
	if err != nil {
		return "", fmt.Errorf("failed to get IKS cluster [%s] details. Resp: [%v], Err: [%v]", i.clusterName, detailedResponse, err)
	}
	return core.StringNilMapper(clusterResponse.ResourceGroup), nil
}

// UpgradeControlPlane upgrades the IKS control plane to the specified version
func (i *IKS) UpgradeControlPlane(version string) error {
	log.Infof("Upgrading IKS cluster [%s] control plane to version [%s]", i.clusterName, version)
	detailedResponse, err := i.k8sClient.UpdateCluster(
		&kubernetesserviceapiv1.UpdateClusterOptions{
			IdOrName: core.StringPtr(i.clusterName),
			Action:   core.StringPtr("update"),
			Version:  core.StringPtr(version),
		},
	)
	if err != nil {
		return fmt.Errorf("failed to upgrade IKS cluster [%s] control plane version to [%s]. Resp: [%v], Err: [%v]", i.clusterName, version, detailedResponse, err)
	}
	log.Infof("Initiated IKS cluster [%s] control plane upgrade to [%s] successfully", i.clusterName, version)
	return nil
}

// WaitForControlPlaneToUpgrade waits for the IKS control plane to be upgraded to the specified version
func (i *IKS) WaitForControlPlaneToUpgrade(version string) error {
	log.Infof("Waiting for IKS cluster [%s] control plane to be upgraded to [%s]", i.clusterName, version)
	expectedUpgradeStatus := "normal"
	t := func() (interface{}, bool, error) {
		clusterResponse, detailedResponse, err := i.k8sClient.GetCluster(
			&kubernetesserviceapiv1.GetClusterOptions{
				Cluster: core.StringPtr(i.clusterName),
			},
		)
		if err != nil {
			return nil, false, fmt.Errorf("failed to get IKS cluster [%s] details. Resp: [%v], Err: [%v]", i.clusterName, detailedResponse, err)
		}
		status := core.StringNilMapper(clusterResponse.State)
		masterKubeVersion := core.StringNilMapper(clusterResponse.MasterKubeVersion)
		// The master kube-version comparison using strings.HasPrefix is necessary because
		// IKS appends a suffix to the version (e.g., "1.27.11_1566", "4.13.33_1562_openshift").
		if status == expectedUpgradeStatus && strings.HasPrefix(masterKubeVersion, version) {
			log.Infof("IKS cluster [%s] control plane upgrade to [%s] completed successfully. Current status: [%s], version: [%s].", i.clusterName, version, status, masterKubeVersion)
			return nil, false, nil
		} else {
			return nil, true, fmt.Errorf("waiting for IKS cluster [%s] control plane upgrade to [%s] to complete, expected status [%s], actual status [%s], current version [%s]", i.clusterName, version, expectedUpgradeStatus, status, masterKubeVersion)
		}
	}
	_, err := task.DoRetryWithTimeout(t, defaultIKSUpgradeTimeout, defaultIKSUpgradeRetryInterval)
	if err != nil {
		return fmt.Errorf("failed to upgrade IKS cluster [%s] control plane to [%s]. Err: [%v]", i.clusterName, version, err)
	}
	log.Infof("Successfully upgraded IKS cluster [%s] control plane to [%s]", i.clusterName, version)
	return nil
}

// ReplaceWorker replaces the specified worker in the IKS cluster
func (i *IKS) ReplaceWorker(workerID string) error {
	log.Infof("Replacing cluster [%s] worker [%s]", i.clusterName, workerID)
	replaceResponse, err := i.containerClient.Workers().ReplaceWokerNode(
		i.clusterName,
		workerID,
		containerv2.ClusterTargetHeader{
			ResourceGroup: i.resourceGroupID,
		},
	)
	if err != nil && err.Error() != rest.ErrEmptyResponseBody.Error() {
		return fmt.Errorf("failed to replace cluster [%s] worker [%s]. Resp: [%v], Err: [%v]", i.clusterName, workerID, replaceResponse, err)
	}
	waitForWorkerToReplace := func() (interface{}, bool, error) {
		worker, err := i.containerClient.Workers().Get(
			i.clusterName,
			workerID,
			containerv2.ClusterTargetHeader{
				ResourceGroup: i.resourceGroupID,
			},
		)
		if err != nil {
			return nil, false, fmt.Errorf("failed to get worker [%s] while waiting for it to be replaced. Err: [%v]", workerID, err)
		}
		if worker.LifeCycle.ActualState == "deleted" && worker.LifeCycle.ReasonForDelete == "worker_replaced" {
			return nil, false, nil
		}
		return nil, true, fmt.Errorf("waiting for worker [%s] to be replaced, desired state [%s], actual state [%s]", workerID, worker.LifeCycle.DesiredState, worker.LifeCycle.ActualState)
	}
	_, err = task.DoRetryWithTimeout(waitForWorkerToReplace, defaultIKSUpgradeTimeout, defaultIKSUpgradeRetryInterval)
	if err != nil {
		return err
	}
	log.Infof("Successfully replaced cluster [%s] worker [%s]", i.clusterName, workerID)
	return nil
}

// UpgradeWorkerPool upgrades the IKS worker pool to the specified version
func (i *IKS) UpgradeWorkerPool(workerPoolName string, version string) error {
	log.Infof("Starting IKS cluster [%s] worker pool upgrade [%s] to [%s]", i.clusterName, workerPoolName, version)
	workers, err := i.containerClient.Workers().ListByWorkerPool(i.clusterName, workerPoolName, false, containerv2.ClusterTargetHeader{
		ResourceGroup: i.resourceGroupID,
	})
	if err != nil {
		return fmt.Errorf("failed to list workers for pool [%s] in cluster [%s]. Err: %v", workerPoolName, i.clusterName, err)
	}
	locationWorkerMap := make(map[string][]containerv2.Worker)
	for _, worker := range workers {
		locationWorkerMap[worker.Location] = append(locationWorkerMap[worker.Location], worker)
	}
	var wg sync.WaitGroup
	errChan := make(chan error, len(workers))
	for location, workers := range locationWorkerMap {
		wg.Add(1)
		go func(location string, workers []containerv2.Worker) {
			defer wg.Done()
			for _, worker := range workers {
				log.Infof("Replacing worker [%s] in location [%s] to upgrade to version [%s]", worker.ID, location, version)
				err = i.ReplaceWorker(worker.ID)
				if err != nil {
					errChan <- err
				}
			}
		}(location, workers)
	}
	wg.Wait()
	close(errChan)
	var errors []string
	for e := range errChan {
		errors = append(errors, e.Error())
	}
	if len(errors) > 0 {
		return fmt.Errorf("failed to upgrade IKS cluster [%s] worker pool [%s] to version [%s]. Errs: [%v]", i.clusterName, workerPoolName, version, errors)
	}
	log.Infof("Initiated IKS cluster [%s] worker pool [%s] upgrade version to [%s] successfully", i.clusterName, workerPoolName, version)
	return nil
}

func (i *IKS) GetZones() ([]string, error) {
	var zones []string
	err := i.configureIKSClient()
	if err != nil {
		return zones, err
	}
	i.containerClient.WorkerPools()
	target := containerv2.ClusterTargetHeader{
		Provider: "vpc-gen2",
	}
	workerPoolDetails, err := i.containerClient.WorkerPools().
		GetWorkerPool(i.clusterName, i.pxWorkerPoolName, target)

	log.Infof("workerPoolDetails: %v", workerPoolDetails)
	if err != nil {
		return zones, err
	}

	for _, z := range workerPoolDetails.Zones {
		zones = append(zones, z.ID)
	}
	return zones, nil
}

// WaitForWorkerPoolToUpgrade waits for the IKS worker pool to be upgraded to the specified version
func (i *IKS) WaitForWorkerPoolToUpgrade(workerPoolName string, version string) error {
	log.Infof("Waiting for IKS cluster [%s] worker pool [%s] to be upgraded to [%s]", i.clusterName, workerPoolName, version)
	waitForWorkerPoolToUpgrade := func() (interface{}, bool, error) {
		workers, err := i.containerClient.Workers().ListByWorkerPool(i.clusterName, workerPoolName, false, containerv2.ClusterTargetHeader{
			ResourceGroup: i.resourceGroupID,
		})
		if err != nil {
			return nil, false, fmt.Errorf("failed to list workers for pool [%s] in cluster [%s]. Err: %v", workerPoolName, i.clusterName, err)
		}
		for _, worker := range workers {
			if !strings.HasPrefix(worker.KubeVersion.Actual, version) {
				return nil, true, fmt.Errorf("waiting for worker [%s] in pool [%s] to be upgraded to version [%s], current version [%s]", worker.ID, workerPoolName, worker.KubeVersion.Desired, worker.KubeVersion.Actual)
			}
			if worker.Health.State != "normal" {
				return nil, true, fmt.Errorf("waiting for worker [%s] in pool [%s] to be normal, current state [%s]", worker.ID, workerPoolName, worker.Health.State)
			}
		}
		return nil, false, nil
	}
	_, err := task.DoRetryWithTimeout(waitForWorkerPoolToUpgrade, defaultIKSUpgradeTimeout, defaultIKSUpgradeRetryInterval)
	if err != nil {
		return fmt.Errorf("failed to upgrade IKS cluster [%s] worker pool [%s] to [%s]. Err: [%v]", i.clusterName, workerPoolName, version, err)
	}
	log.Infof("Successfully upgraded IKS cluster [%s] worker pool [%s] to [%s]", i.clusterName, workerPoolName, version)
	return nil
}

// UpgradeScheduler upgrades the IKS cluster to the specified version
func (i *IKS) UpgradeScheduler(version string) error {

	err := i.configureIKSClient()
	if err != nil {
		return err
	}

	currentVersion, err := i.GetCurrentVersion()
	if err != nil {
		return fmt.Errorf("failed to get IKS cluster [%s] current version, Err: [%v]", i.clusterName, err)
	}
	log.Infof("Starting IKS cluster [%s] upgrade from [%s] to [%s]", i.clusterName, currentVersion, version)

	// Upgrade Control Plane
	err = i.UpgradeControlPlane(version)
	if err != nil {
		return fmt.Errorf("failed to set IKS cluster [%s] control plane version to [%s], Err: [%v]", i.clusterName, version, err)
	}

	// Wait for control plane to be upgraded
	err = i.WaitForControlPlaneToUpgrade(version)
	if err != nil {
		return fmt.Errorf("failed to upgrade IKS cluster [%s] control plane to [%s], Err: [%v]", i.clusterName, version, err)
	}

	// Upgrade Worker Pool
	err = i.UpgradeWorkerPool(i.pxWorkerPoolName, version)
	if err != nil {
		return fmt.Errorf("failed to upgrade IKS cluster [%s] worker pool to [%s], Err: [%v]", i.clusterName, version, err)
	}

	// Wait for the portworx worker pool to be upgraded
	err = i.WaitForWorkerPoolToUpgrade(i.pxWorkerPoolName, version)
	if err != nil {
		return fmt.Errorf("failed to wait for IKS cluster [%s] worker pool upgrade to [%s], Err: [%v]", i.clusterName, version, err)
	}

	log.Infof("Successfully upgraded IKS cluster [%s] scheduler and worker pool to version [%s]", i.clusterName, version)
	return nil
}

func (i *IKS) configureIKSClient() error {
	// This implementation assumes the IKS cluster has two worker pools: one pool for
	// Torpedo and another pool for Portworx.
	if i.k8sClient == nil || i.containerClient != nil {
		ibmCloudAPIKey := os.Getenv("IBMCLOUD_API_KEY")
		if ibmCloudAPIKey == "" {
			return fmt.Errorf("env IBMCLOUD_API_KEY not set")
		}
		torpedoNodeName := ""
		pods, err := k8sCore.Instance().GetPods("default", nil)
		if err != nil {
			log.Errorf("failed to get pods from default namespace. Err: [%v]", err)
		}
		if pods != nil {
			for _, pod := range pods.Items {
				if pod.Name == "torpedo" {
					torpedoNodeName = pod.Spec.NodeName
				}
			}
		}
		torpedoWorkerPoolName := ""
		workerPoolLabel := "ibm-cloud.kubernetes.io/worker-pool-name"
		nodes, err := k8sCore.Instance().GetNodes()
		if err != nil {
			log.Errorf("failed to get nodes. Err: [%v]", err)
		}
		if nodes != nil {
			for _, n := range nodes.Items {
				if n.Name == torpedoNodeName {
					torpedoWorkerPoolName = n.Labels[workerPoolLabel]
					break
				}
			}
		}
		i.pxWorkerPoolName = os.Getenv("IKS_PX_WORKERPOOL_NAME")
		if i.pxWorkerPoolName == "" {
			log.Warnf("env IKS_PX_WORKERPOOL_NAME not set. Using node label [%s] to determine Portworx worker pool", workerPoolLabel)
			if torpedoWorkerPoolName != "" && nodes != nil {
				for _, n := range nodes.Items {
					if n.Labels[workerPoolLabel] != torpedoWorkerPoolName {
						i.pxWorkerPoolName = n.Labels[workerPoolLabel]
						log.Infof("Used node label [%s] to determine Portworx worker pool [%s]", workerPoolLabel, i.pxWorkerPoolName)
						break
					}
				}
			}
			if i.pxWorkerPoolName == "" {
				return fmt.Errorf("env IKS_PX_WORKERPOOL_NAME or node label [%s] not set", workerPoolLabel)
			}
		}
		i.region = os.Getenv("IKS_CLUSTER_REGION")
		if i.region == "" {
			nodeRegionLabel := "topology.kubernetes.io/region"
			log.Warnf("env IKS_CLUSTER_REGION not set. Using node label [%s] to determine region", nodeRegionLabel)
			if torpedoWorkerPoolName != "" && nodes != nil {
				for _, n := range nodes.Items {
					if n.Labels[workerPoolLabel] != torpedoWorkerPoolName {
						i.region = n.Labels[nodeRegionLabel]
						log.Infof("Used node label [%s] to determine region [%s]", nodeRegionLabel, i.region)
						break
					}
				}
			}
			if i.region == "" {
				return fmt.Errorf("env IKS_CLUSTER_REGION or node label [%s] not set", nodeRegionLabel)
			}
		}
		sess, err := session.New(&bluemix.Config{
			Region:        i.region,
			BluemixAPIKey: ibmCloudAPIKey,
		})
		if err != nil {
			return fmt.Errorf("failed to create IKS session. Err: [%v]", err)
		}
		i.containerClient, err = containerv2.New(sess)
		if err != nil {
			return fmt.Errorf("failed to create IKS container client. Err: [%v]", err)
		}
		i.k8sClient, err = kubernetesserviceapiv1.NewKubernetesServiceApiV1(
			&kubernetesserviceapiv1.KubernetesServiceApiV1Options{
				// Initializes IBM Kubernetes Service client for specified region. More details:
				// https://cloud.ibm.com/docs/containers?topic=containers-regions-and-zones
				URL: fmt.Sprintf("https://%s.containers.cloud.ibm.com", i.region),
				Authenticator: &core.IamAuthenticator{
					ApiKey: ibmCloudAPIKey,
				},
			},
		)
		if err != nil {
			return fmt.Errorf("failed to create IKS Kubernetes Service client. Err: [%v]", err)
		}
		i.clusterName = os.Getenv("IKS_CLUSTER_NAME")
		if nodes != nil && i.clusterName == "" {
		ClusterSearch:
			for _, n := range nodes.Items {
				providerID := n.Spec.ProviderID
				// In IKS, nodes have a ProviderID formatted as ibm://<account-id>///<cluster-id>/<node-id>
				splitID := strings.Split(providerID, "/")
				if len(splitID) < 7 {
					return fmt.Errorf("unexpected format of provider ID [%s]", providerID)
				}
				accountID, clusterID := splitID[2], splitID[5]
				log.Infof("Found account ID [%s] and cluster ID [%s] from node provider ID [%s]", accountID, clusterID, providerID)
				clusters, err := i.containerClient.Clusters().List(
					containerv2.ClusterTargetHeader{
						AccountID: accountID,
					},
				)
				if err != nil {
					return fmt.Errorf("failed to list clusters for account ID [%s], Err: [%v]", accountID, err)
				}
				for _, cluster := range clusters {
					if cluster.ID == clusterID {
						i.clusterName = cluster.Name
						log.Infof("Found cluster name [%s] for cluster ID [%s]", i.clusterName, clusterID)
						break ClusterSearch
					}
				}
			}
		}
		i.resourceGroupID, err = i.GetResourceGroupID()
		if err != nil {
			return fmt.Errorf("failed to get resource group ID for IKS cluster [%s], Err: [%v]", i.clusterName, err)
		}
		log.Infof("Resource group ID for IKS cluster [%s] is [%s]", i.clusterName, i.resourceGroupID)
	}

	return nil
}

func (i *IKS) DeleteNode(node node.Node) error {
	err := i.configureIKSClient()
	if err != nil {
		return err
	}
	err = i.ReplaceWorker(node.Hostname)
	if err != nil {
		return err
	}

	return nil
}

func (i *IKS) GetASGClusterSize() (int64, error) {
	err := i.configureIKSClient()
	if err != nil {
		return 0, err
	}

	target := containerv2.ClusterTargetHeader{
		ResourceGroup: i.resourceGroupID,
		Provider:      "vpc-gen2",
	}

	workerPoolDetails, err := i.containerClient.WorkerPools().
		GetWorkerPool(i.clusterName, i.pxWorkerPoolName, target)
	if err != nil {
		return 0, err
	}
	log.Infof("WorkerCount : %d", workerPoolDetails.WorkerCount)
	log.Infof("Zones : %v", workerPoolDetails.Zones)
	log.Infof("workerPoolDetails: %v", workerPoolDetails)

	return int64(workerPoolDetails.WorkerCount * len(workerPoolDetails.Zones)), nil
}

func (i *IKS) SetASGClusterSize(perZoneCount int64, timeout time.Duration) error {
	err := i.configureIKSClient()
	if err != nil {
		return err
	}

	resizeReg := containerv2.ResizeWorkerPoolReq{Cluster: i.clusterName, Size: perZoneCount, Workerpool: i.pxWorkerPoolName}
	target := containerv2.ClusterTargetHeader{
		ResourceGroup: i.resourceGroupID,
		Provider:      "vpc-gen2",
	}
	err = i.containerClient.WorkerPools().ResizeWorkerPool(resizeReg, target)
	if err != nil {
		return err
	}

	err = i.waitForInstanceGroupResize(i.pxWorkerPoolName, perZoneCount, timeout)
	return err

}

func (i *IKS) waitForInstanceGroupResize(workerPoolName string,
	count int64, timeout time.Duration) error {
	target := containerv2.ClusterTargetHeader{
		Provider: "vpc-gen2",
	}

	workerPoolDetails, err := i.containerClient.WorkerPools().
		GetWorkerPool(i.clusterName, workerPoolName, target)
	if err != nil {
		return err
	}

	expectedWorkerCount := len(workerPoolDetails.Zones) * int(count)
	if timeout > time.Nanosecond {
		f := func() (interface{}, bool, error) {

			currentWorkers, err := i.containerClient.Workers().ListByWorkerPool(i.clusterName, workerPoolName, false, containerv2.ClusterTargetHeader{
				ResourceGroup: i.resourceGroupID,
			})

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

		_, err = task.DoRetryWithTimeout(f, timeout, 15*time.Second)
		if err != nil {
			return err
		}
	}
	return nil
}

func init() {
	i := &IKS{}
	scheduler.Register(SchedName, i)
}
