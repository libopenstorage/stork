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

func (g *gke) Init(nodeOpts node.InitOptions) error {
	g.SSH.Init(nodeOpts)

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

func (g *gke) SetASGClusterSize(perZoneCount int64, timeout time.Duration) error {
	// GCP SDK requires per zone cluster size
	err := g.ops.SetInstanceGroupSize(g.instanceGroup, perZoneCount, timeout)
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

func (g *gke) SetClusterVersion(version string, timeout time.Duration) error {

	err := g.ops.SetClusterVersion(version, timeout)
	if err != nil {
		logrus.Errorf("failed to set version for cluster. Error: %v", err)
		return err
	}
	logrus.Infof("Cluster version set successfully. Setting up node group version now ...")

	err = g.ops.SetInstanceGroupVersion(g.instanceGroup, version, timeout)
	if err != nil {
		logrus.Errorf("failed to set version for instance group %s. Error: %v", g.instanceGroup, err)
		return err
	}
	logrus.Infof("Node group version set successfully.")

	return nil
}

func (g *gke) DeleteNode(node node.Node, timeout time.Duration) error {

	err := g.ops.DeleteInstance(node.Name, node.Zone, timeout)
	if err != nil {
		return err
	}
	return nil
}

func (g *gke) GetZones() ([]string, error) {
	asgInfo, err := g.ops.InspectInstanceGroupForInstance(g.ops.InstanceID())
	if err != nil {
		return []string{}, err
	}
	return asgInfo.Zones, nil
}

func init() {
	g := &gke{
		SSH: ssh.SSH{},
	}

	node.Register(DriverName, g)
}
