package gke

import (
	"os"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/libopenstorage/cloudops"
	"github.com/libopenstorage/cloudops/gce"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/node/ssh"
)

const (
	// DriverName is the name of the gke driver
	DriverName = "gke"
)

type gke struct {
	ssh.SSH
	ops           cloudops.Ops
	instanceGroup string
}

func (g *gke) String() string {
	return DriverName
}

func (g *gke) Init(installFlushCache bool) error {

	instanceGroup := os.Getenv("INSTANCE_GROUP")
	if len(instanceGroup) != 0 {
		g.instanceGroup = instanceGroup
	} else {
		g.instanceGroup = "default-pool"
	}

	ops, err := gce.NewClient()
	if err != nil {
		return err
	}
	g.ops = ops

	return nil
}

func (g *gke) SetASGClusterSize(count int64, timeout time.Duration) error {
	err := g.ops.SetInstanceGroupSize(g.instanceGroup, count, timeout)
	if err != nil {
		logrus.Errorf("failed to set size of node pool %s. Error: %v", g.instanceGroup, err)
		return err
	}

	return nil
}

func (g *gke) GetASGClusterSize() (int64, error) {
	nodeCount, err := g.ops.GetInstanceGroupSize(g.instanceGroup)
	if err != nil {
		logrus.Errorf("failed to get size of node pool %s. Error: %v", g.instanceGroup, err)
		return 0, err
	}

	return nodeCount, nil
}

func (g *gke) DeleteNode(node node.Node, timeout time.Duration) error {

	err := g.ops.DeleteInstance(node.Name, node.Zone, timeout)
	if err != nil {
		return err
	}
	return nil
}

func init() {

	SSHDriver := ssh.SSH{}
	SSHDriver.Init(false)
	g := &gke{
		SSH: SSHDriver,
	}

	node.Register(DriverName, g)
}
