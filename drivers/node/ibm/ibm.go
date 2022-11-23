package ibm

import (
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
	DriverName = "ibm"
)

type ibm struct {
	ssh.SSH
	ops           cloudops.Ops
	instanceGroup string
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

func init() {
	i := &ibm{
		SSH: *ssh.New(),
	}

	node.Register(DriverName, i)
}
