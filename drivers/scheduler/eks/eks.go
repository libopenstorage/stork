package eks

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/eks"
	"github.com/aws/aws-sdk-go-v2/service/eks/types"
	"github.com/portworx/sched-ops/k8s/core"
	"github.com/portworx/sched-ops/task"
	"github.com/portworx/torpedo/drivers/scheduler"
	kube "github.com/portworx/torpedo/drivers/scheduler/k8s"
	"github.com/portworx/torpedo/pkg/log"
	"os"
	"strings"
	"time"
)

const (
	// SchedName is the name of the EKS scheduler driver implementation
	SchedName = "eks"
	// defaultEKSUpgradeTimeout is the default timeout for EKS control plane upgrade
	defaultEKSUpgradeTimeout = 90 * time.Minute
	// defaultEKSUpgradeRetryInterval is the default retry interval for EKS control plane upgrade
	defaultEKSUpgradeRetryInterval = 5 * time.Minute
)

type EKS struct {
	kube.K8s
	clusterName     string
	region          string
	config          aws.Config
	eksClient       *eks.Client
	pxNodeGroupName string
}

// String returns the string name of this driver.
func (e *EKS) String() string {
	return SchedName
}

func (e *EKS) Init(schedOpts scheduler.InitOptions) (err error) {
	err = e.K8s.Init(schedOpts)
	if err != nil {
		return err
	}
	return nil
}

// GetCurrentVersion returns the current version of the EKS cluster
func (e *EKS) GetCurrentVersion() (string, error) {
	eksDescribeClusterOutput, err := e.eksClient.DescribeCluster(
		context.TODO(),
		&eks.DescribeClusterInput{
			Name: aws.String(e.clusterName),
		},
	)
	if err != nil {
		return "", fmt.Errorf("failed to describe EKS cluster [%s], Err: [%v]", e.clusterName, err)
	}
	if eksDescribeClusterOutput.Cluster == nil {
		return "", fmt.Errorf("failed to describe EKS cluster [%s], cluster not found", e.clusterName)
	}
	return aws.ToString(eksDescribeClusterOutput.Cluster.Version), nil
}

// UpgradeControlPlane upgrades the EKS control plane to the specified version
func (e *EKS) UpgradeControlPlane(version string) error {
	log.Infof("Upgrading EKS cluster [%s] control plane to version [%s]", e.clusterName, version)
	_, err := e.eksClient.UpdateClusterVersion(
		context.TODO(),
		&eks.UpdateClusterVersionInput{
			Name:    aws.String(e.clusterName),
			Version: aws.String(version),
		},
	)
	if err != nil {
		return fmt.Errorf("failed to set EKS cluster [%s] control plane version to [%s], Err: [%v]", e.clusterName, version, err)
	}
	log.Infof("Initiated EKS cluster [%s] control plane upgrade to [%s] successfully", e.clusterName, version)
	return nil
}

// WaitForControlPlaneToUpgrade waits for the EKS control plane to be upgraded to the specified version
func (e *EKS) WaitForControlPlaneToUpgrade(version string) error {
	log.Infof("Waiting for EKS cluster [%s] control plane to be upgraded to [%s]", e.clusterName, version)
	expectedUpgradeStatus := types.ClusterStatusActive
	t := func() (interface{}, bool, error) {
		eksDescribeClusterOutput, err := e.eksClient.DescribeCluster(
			context.TODO(),
			&eks.DescribeClusterInput{
				Name: aws.String(e.clusterName),
			},
		)
		if err != nil {
			return nil, false, err
		}
		if eksDescribeClusterOutput.Cluster == nil {
			return nil, false, fmt.Errorf("failed to describe EKS cluster [%s], cluster not found", e.clusterName)
		}
		status := eksDescribeClusterOutput.Cluster.Status
		currentVersion := aws.ToString(eksDescribeClusterOutput.Cluster.Version)
		if status == expectedUpgradeStatus && currentVersion == version {
			log.Infof("EKS cluster [%s] control plane upgrade to [%s] completed successfully. Current status: [%s], version: [%s].", e.clusterName, version, status, currentVersion)
			return nil, false, nil
		} else {
			return nil, true, fmt.Errorf("waiting for EKS cluster [%s] control plane upgrade to [%s] to complete, expected status [%s], actual status [%s], current version [%s]", e.clusterName, version, expectedUpgradeStatus, status, currentVersion)
		}
	}
	_, err := task.DoRetryWithTimeout(t, defaultEKSUpgradeTimeout, defaultEKSUpgradeRetryInterval)
	if err != nil {
		return fmt.Errorf("failed to upgrade EKS cluster [%s] control plane to [%s], Err: [%v]", e.clusterName, version, err)
	}
	log.Infof("Successfully upgraded EKS cluster [%s] control plane to [%s]", e.clusterName, version)
	return nil
}

// UpgradeNodeGroup upgrades the EKS node group to the specified version
func (e *EKS) UpgradeNodeGroup(nodeGroupName string, version string) error {
	log.Infof("Starting EKS cluster [%s] node group upgrade [%s] to [%s]", e.clusterName, nodeGroupName, version)
	_, err := e.eksClient.UpdateNodegroupVersion(context.TODO(), &eks.UpdateNodegroupVersionInput{
		ClusterName:   aws.String(e.clusterName),
		NodegroupName: aws.String(nodeGroupName),
		Version:       aws.String(version),
	})
	if err != nil {
		return fmt.Errorf("failed to upgrade EKS cluster [%s] node group [%s] version to [%s], Err: [%v]", e.clusterName, nodeGroupName, version, err)
	}
	log.Infof("Initiated EKS cluster [%s] node group [%s] upgrade to version [%s] successfully", e.clusterName, nodeGroupName, version)
	return nil
}

// WaitForNodeGroupToUpgrade waits for the EKS node group to be upgraded to the specified version
func (e *EKS) WaitForNodeGroupToUpgrade(nodeGroupName string, version string) error {
	log.Infof("Waiting for EKS cluster [%s] node group [%s] to be upgraded to [%s]", e.clusterName, nodeGroupName, version)
	expectedUpgradeStatus := types.NodegroupStatusActive
	t := func() (interface{}, bool, error) {
		eksDescribeNodegroupOutput, err := e.eksClient.DescribeNodegroup(
			context.TODO(),
			&eks.DescribeNodegroupInput{
				ClusterName:   aws.String(e.clusterName),
				NodegroupName: aws.String(nodeGroupName),
			},
		)
		if err != nil {
			return nil, false, err
		}
		if eksDescribeNodegroupOutput.Nodegroup == nil {
			return nil, false, fmt.Errorf("failed to describe EKS cluster [%s] node group [%s], node group not found", e.clusterName, nodeGroupName)
		}
		status := eksDescribeNodegroupOutput.Nodegroup.Status
		releaseVersion := aws.ToString(eksDescribeNodegroupOutput.Nodegroup.ReleaseVersion)
		// The release version comparison using strings.HasPrefix is necessary because
		// EKS appends a suffix to the version (e.g., "1.27.9-20240213").
		if status == expectedUpgradeStatus && strings.HasPrefix(releaseVersion, version) {
			log.Infof("EKS cluster [%s] node group [%s] successfully upgraded to version [%s]. Current status: [%s], release version: [%s].", e.clusterName, nodeGroupName, version, status, releaseVersion)
			return nil, false, nil
		} else {
			return nil, true, fmt.Errorf("waiting for EKS cluster [%s] node group [%s] upgrade to [%s] to complete, expected status [%s], actual status [%s], current release version [%s]", e.clusterName, nodeGroupName, version, expectedUpgradeStatus, status, releaseVersion)
		}
	}
	_, err := task.DoRetryWithTimeout(t, defaultEKSUpgradeTimeout, defaultEKSUpgradeRetryInterval)
	if err != nil {
		return fmt.Errorf("failed to upgrade EKS cluster [%s] node group [%s] to version [%s], Err: [%v]", e.clusterName, nodeGroupName, version, err)
	}
	log.Infof("Successfully upgraded EKS cluster [%s] node group [%s] to [%s]", e.clusterName, nodeGroupName, version)
	return nil
}

// UpgradeScheduler upgrades the EKS cluster to the specified version
func (e *EKS) UpgradeScheduler(version string) error {
	// This implementation assumes the EKS cluster has two node groups: one group for
	// Torpedo and another group for Portworx.
	torpedoNodeName := ""
	pods, err := core.Instance().GetPods("default", nil)
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
	torpedoNodeGroupName := ""
	nodeGroupLabel := "eks.amazonaws.com/nodegroup"
	nodes, err := core.Instance().GetNodes()
	if err != nil {
		log.Errorf("failed to get nodes. Err: [%v]", err)
	}
	if nodes != nil {
		for _, node := range nodes.Items {
			if node.Name == torpedoNodeName {
				torpedoNodeGroupName = node.Labels[nodeGroupLabel]
				break
			}
		}
	}
	e.pxNodeGroupName = os.Getenv("EKS_PX_NODEGROUP_NAME")
	if e.pxNodeGroupName == "" {
		log.Warnf("env EKS_PX_NODEGROUP_NAME not set. Using node label [%s] to determine Portworx node group", nodeGroupLabel)
		if torpedoNodeGroupName != "" && nodes != nil {
			for _, node := range nodes.Items {
				if node.Labels[nodeGroupLabel] != torpedoNodeGroupName {
					e.pxNodeGroupName = node.Labels[nodeGroupLabel]
					log.Infof("Used node label [%s] to determine Portworx node group [%s]", nodeGroupLabel, e.pxNodeGroupName)
					break
				}
			}
		}
		if e.pxNodeGroupName == "" {
			return fmt.Errorf("env EKS_PX_NODEGROUP_NAME or node label [%s] not set", nodeGroupLabel)
		}
	}
	e.region = os.Getenv("EKS_CLUSTER_REGION")
	if e.region == "" {
		nodeRegionLabel := "topology.kubernetes.io/region"
		log.Warnf("env EKS_CLUSTER_REGION not set. Using node label [%s] to determine region", nodeRegionLabel)
		if torpedoNodeGroupName != "" && nodes != nil {
			for _, node := range nodes.Items {
				if node.Labels[nodeGroupLabel] != torpedoNodeGroupName {
					e.region = node.Labels[nodeRegionLabel]
					log.Infof("Used node label [%s] to determine region [%s]", nodeRegionLabel, e.region)
					break
				}
			}
		}
		if e.region == "" {
			return fmt.Errorf("env EKS_CLUSTER_REGION or node label [%s] not set", nodeRegionLabel)
		}
	}
	e.config, err = config.LoadDefaultConfig(context.TODO(), config.WithRegion(e.region))
	if err != nil {
		return fmt.Errorf("unable to load config for region [%s]. Err: [%v]", e.region, err)
	}
	e.clusterName = os.Getenv("EKS_CLUSTER_NAME")
	if e.clusterName == "" {
		ec2InstanceLabel := "kubernetes.io/cluster/"
		for _, node := range nodes.Items {
			providerID := node.Spec.ProviderID
			// In EKS, nodes have a ProviderID formatted as aws:///<region>/<instance-id>
			splitID := strings.Split(providerID, "/")
			if len(splitID) < 5 {
				return fmt.Errorf("unexpected format of provider ID [%s]", providerID)
			}
			instanceID := splitID[4]
			ec2Client := ec2.NewFromConfig(e.config)
			result, err := ec2Client.DescribeInstances(
				context.TODO(),
				&ec2.DescribeInstancesInput{
					InstanceIds: []string{instanceID},
				},
			)
			if err != nil {
				return fmt.Errorf("failed to describe EC2 instance [%s]. Err: [%v]", instanceID, err)
			}
			for _, reservation := range result.Reservations {
				for _, instance := range reservation.Instances {
					for _, tag := range instance.Tags {
						if strings.HasPrefix(aws.ToString(tag.Key), ec2InstanceLabel) {
							e.clusterName = strings.TrimPrefix(aws.ToString(tag.Key), ec2InstanceLabel)
							log.Infof("Instance [%s] is part of EKS cluster [%s] in region [%s]", instanceID, e.clusterName, e.region)
							break
						}
					}
				}
			}
		}
		if e.clusterName == "" {
			return fmt.Errorf("env EKS_CLUSTER_NAME or EC2 instance label [%s] not set", ec2InstanceLabel)
		}
	}
	e.eksClient = eks.NewFromConfig(e.config)
	currentVersion, err := e.GetCurrentVersion()
	if err != nil {
		return fmt.Errorf("failed to get EKS cluster [%s] current version, Err: [%v]", e.clusterName, err)
	}
	log.Infof("Starting EKS cluster [%s] upgrade from [%s] to [%s]", e.clusterName, currentVersion, version)

	// Upgrade Control Plane
	err = e.UpgradeControlPlane(version)
	if err != nil {
		return fmt.Errorf("failed to set EKS cluster [%s] control plane version to [%s], Err: [%v]", e.clusterName, version, err)
	}

	// Wait for control plane to be upgraded
	err = e.WaitForControlPlaneToUpgrade(version)
	if err != nil {
		return fmt.Errorf("failed to wait for EKS cluster [%s] control plane to be upgraded to [%s], Err: [%v]", e.clusterName, version, err)
	}

	// Upgrade Node Group
	err = e.UpgradeNodeGroup(e.pxNodeGroupName, version)
	if err != nil {
		return fmt.Errorf("failed to upgrade EKS cluster [%s] node group [%s] to [%s]. Err: [%v]", e.clusterName, e.pxNodeGroupName, version, err)
	}

	// Wait for the portworx node group to be upgraded
	err = e.WaitForNodeGroupToUpgrade(e.pxNodeGroupName, version)
	if err != nil {
		return fmt.Errorf("failed to wait for EKS cluster [%s] node group [%s] to be upgraded to [%s]. Err: [%v]", e.clusterName, e.pxNodeGroupName, version, err)
	}
	log.Infof("Successfully finished EKS cluster [%s] upgrade from [%s] to [%s]", e.clusterName, currentVersion, version)
	return nil
}

func init() {
	e := &EKS{}
	scheduler.Register(SchedName, e)
}
