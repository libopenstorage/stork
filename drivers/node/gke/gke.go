package gke

import (
	"github.com/libopenstorage/cloudops"
	"github.com/libopenstorage/cloudops/gce"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/node/ssh"
	"github.com/portworx/torpedo/pkg/log"
	"os"
	"time"
)

const (
	// DriverName is the name of the gke driver
	DriverName = "gke"
)

type Gke struct {
	ssh.SSH
	ops           cloudops.Ops
	instanceGroup string
}

func (g *Gke) String() string {
	return DriverName
}

func (g *Gke) Init(nodeOpts node.InitOptions) error {
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

func (g *Gke) SetASGClusterSize(perZoneCount int64, timeout time.Duration) error {
	// GCP SDK requires per zone cluster size
	err := g.ops.SetInstanceGroupSize(g.instanceGroup, perZoneCount, timeout)
	if err != nil {
		log.Errorf("failed to set size of node pool %s. Error: %v", g.instanceGroup, err)
		return err
	}

	return nil
}

func (g *Gke) GetASGClusterSize() (int64, error) {
	nodeCount, err := g.ops.GetInstanceGroupSize(g.instanceGroup)
	if err != nil {
		log.Errorf("failed to get size of node pool %s. Error: %v", g.instanceGroup, err)
		return 0, err
	}

	return nodeCount, nil
}

func (g *Gke) SetClusterVersion(version string, timeout time.Duration) error {

	err := g.ops.SetClusterVersion(version, timeout)
	if err != nil {
		log.Errorf("failed to set version for cluster. Error: %v", err)
		return err
	}
	log.Infof("Cluster version set successfully. Setting up node group version now ...")

	err = g.ops.SetInstanceGroupVersion(g.instanceGroup, version, timeout)
	if err != nil {
		log.Errorf("failed to set version for instance group %s. Error: %v", g.instanceGroup, err)
		return err
	}
	log.Infof("Node group version set successfully.")

	return nil
}

func (g *Gke) DeleteNode(node node.Node, timeout time.Duration) error {

	err := g.ops.DeleteInstance(node.Name, node.Zone, timeout)
	if err != nil {
		return err
	}
	return nil
}

func (g *Gke) GetZones() ([]string, error) {
	storageDriverNodes := node.GetStorageDriverNodes()
	nZones := make(map[string]bool)
	for _, sNode := range storageDriverNodes {
		if _, ok := nZones[sNode.Zone]; !ok {
			nZones[sNode.Zone] = true
		}

	}
	asgZones := make([]string, 0)
	for k := range nZones {
		asgZones = append(asgZones, k)
	}
	return asgZones, nil
}

func init() {
	g := &Gke{
		SSH: *ssh.New(),
	}

	node.Register(DriverName, g)
}
