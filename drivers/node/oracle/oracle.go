package oracle

import (
	"fmt"
	"os"
	"time"

	"github.com/portworx/torpedo/drivers/node/ssh"

	"github.com/libopenstorage/cloudops"
	oracleOps "github.com/libopenstorage/cloudops/oracle"
	"github.com/oracle/oci-go-sdk/v65/core"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/sirupsen/logrus"
)

const (
	// DriverName is the name of the aws driver
	DriverName = "oracle"
)

type oracle struct {
	ssh.SSH
	ops               cloudops.Ops
	instanceID        string
	instanceGroupName string
	log *logrus.Logger
}

func (o *oracle) String() string {
	return DriverName
}

// Init initializes the node driver for oracle under the given scheduler
func (o *oracle) Init(nodeOpts node.InitOptions) error {
	o.SSH.Init(nodeOpts)
	o.log = nodeOpts.Logger
	instanceGroup := os.Getenv("INSTANCE_GROUP")
	if len(instanceGroup) != 0 {
		o.instanceGroupName = instanceGroup
	} else {
		o.instanceGroupName = "default"
	}
	ops, err := oracleOps.NewClient()
	if err != nil {
		return err
	}
	o.ops = ops

	return nil
}

// SetASGClusterSize sets node count per zone
func (o *oracle) SetASGClusterSize(perZoneCount int64, timeout time.Duration) error {
	err := o.ops.SetInstanceGroupSize(o.instanceGroupName, perZoneCount, timeout)
	if err != nil {
		o.log.Errorf("failed to set size of node pool %s. Error: %v", o.instanceGroupName, err)
		return err
	}

	return nil
}

// GetASGClusterSize gets node count for cluster
func (o *oracle) GetASGClusterSize() (int64, error) {
	size, err := o.ops.GetInstanceGroupSize(o.instanceGroupName)
	if err != nil {
		o.log.Errorf("failed to get size of node pool %s. Error: %v", o.instanceGroupName, err)
		return 0, err
	}
	return size, nil
}

// GetZones returns list of zones in which cluster is running
func (o *oracle) GetZones() ([]string, error) {
	asgInfo, err := o.ops.InspectInstanceGroupForInstance(o.ops.InstanceID())
	if err != nil {
		return []string{}, err
	}

	return asgInfo.Zones, nil
}

// SetClusterVersion sets desired version for cluster and its node pools
func (o *oracle) SetClusterVersion(version string, timeout time.Duration) error {
	o.log.Info("[Torpedo] Setting cluster version to :", version)
	err := o.ops.SetClusterVersion(version, timeout)
	if err != nil {
		o.log.Errorf("failed to set version for cluster. Error: %v", err)
		return err
	}
	o.log.Info("[Torpedo] Cluster version set successfully. Setting up node group version now ...")

	err = o.ops.SetInstanceGroupVersion(o.instanceGroupName, version, timeout)
	if err != nil {
		o.log.Errorf("failed to set version for instance group %s. Error: %v", o.instanceGroupName, err)
		return err
	}
	o.log.Info("[Torpedo] Node group version set successfully for group ", o.instanceGroupName)

	return nil
}

// DeleteNode deletes the given node
func (o *oracle) DeleteNode(node node.Node, timeout time.Duration) error {
	o.log.Infof("[Torpedo] Deleting node [%s]", node.Hostname)
	instanceDetails, err := o.ops.GetInstance(node.Hostname)
	if err != nil {
		return err
	}

	oracleInstance, ok := instanceDetails.(core.Instance)
	if !ok {
		return fmt.Errorf("could not retrive oracle instance details for %s", node.Hostname)
	}
	err = o.ops.DeleteInstance(*oracleInstance.Id, "", timeout)
	if err != nil {
		return err
	}
	return nil
}

func init() {
	i := &oracle{
		SSH: *ssh.New(),
	}

	node.Register(DriverName, i)
}
