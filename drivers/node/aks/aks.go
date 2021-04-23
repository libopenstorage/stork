package aks

import (
	"os"
	"time"

	"github.com/libopenstorage/cloudops"
	"github.com/libopenstorage/cloudops/azure"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/node/ssh"
	"github.com/sirupsen/logrus"
)

const (
	// DriverName is the name of the aks driver
	DriverName = "aks"
	// ZoneCount number of zones in autoscaling group
	ZoneCount = 3
)

type aks struct {
	ssh.SSH
	ops           cloudops.Ops
	instanceGroup string
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
	totalClusterSize := perZoneCount * ZoneCount
	err := a.ops.SetInstanceGroupSize(a.instanceGroup, totalClusterSize, timeout)
	if err != nil {
		logrus.Errorf("failed to set size of node pool %s. Error: %v", a.instanceGroup, err)
		return err
	}

	return nil
}

func (a *aks) GetASGClusterSize() (int64, error) {
	nodeCount, err := a.ops.GetInstanceGroupSize(a.instanceGroup)
	if err != nil {
		logrus.Errorf("failed to get size of node pool %s. Error: %v", a.instanceGroup, err)
		return 0, err
	}

	return nodeCount, nil
}

func init() {
	a := &aks{
		SSH: ssh.SSH{},
	}

	node.Register(DriverName, a)
}
