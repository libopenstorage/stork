package gke

import (
	"fmt"
	"os"
	"time"

	"github.com/libopenstorage/cloudops"
	"github.com/libopenstorage/cloudops/gce"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/node/ssh"
	"github.com/portworx/torpedo/drivers/scheduler"
	kube "github.com/portworx/torpedo/drivers/scheduler/k8s"
	"github.com/portworx/torpedo/pkg/log"
)

const (
	// SchedName is the name of the gke driver
	SchedName = "gke"

	defaultGkeUpgradeTimeout = 90 * time.Minute
)

type Gke struct {
	ssh.SSH
	kube.K8s
	ops           cloudops.Ops
	instanceGroup string
}

func (g *Gke) String() string {
	return SchedName
}

func init() {
	g := &Gke{}
	scheduler.Register(SchedName, g)
}

func (g *Gke) Init(schedOpts scheduler.InitOptions) error {
	ops, err := gce.NewClient()
	if err != nil {
		return err
	}
	g.ops = ops

	err = g.K8s.Init(schedOpts)
	if err != nil {
		return err
	}

	return nil
}

// UpgradeScheduler performs GKE cluster upgrade to a specified version
func (g *Gke) UpgradeScheduler(version string) error {
	log.Infof("Starting GKE cluster upgrade to [%s]", version)

	instanceGroup := os.Getenv("INSTANCE_GROUP")
	if len(instanceGroup) != 0 {
		g.instanceGroup = instanceGroup
	} else {
		g.instanceGroup = "default-pool"
	}

	// Upgrade GKE Control Plane version
	if err := g.upgradeGkeControlPlaneVersion(version, defaultGkeUpgradeTimeout); err != nil {
		return err
	}

	// Update GKE Node Group Upgrade Strategy
	upgradeStrategy := os.Getenv("GKE_UPGRADE_STRATEGY")
	surgeSetting := os.Getenv("GKE_SURGE_VALUE")
	if upgradeStrategy == "" {
		upgradeStrategy = "surge"
	}
	if surgeSetting == "" {
		surgeSetting = "default"
	}

	if err := g.updateGkeNodeGroupUpgradeStrategy(g.instanceGroup,
		upgradeStrategy, defaultGkeUpgradeTimeout, surgeSetting); err != nil {
		return err
	}

	// Upgrade GKE Node Group version
	if err := g.upgradeGkeNodeGroupVersion(g.instanceGroup, version, defaultGkeUpgradeTimeout); err != nil {
		return err
	}

	log.Infof("Successfully upgraded GKE cluster to [%s]", version)
	return nil
}

func (g *Gke) SetASGClusterSize(perZoneCount int64, timeout time.Duration) error {

	instanceGroup := os.Getenv("INSTANCE_GROUP")
	if len(instanceGroup) != 0 {
		g.instanceGroup = instanceGroup
	} else {
		g.instanceGroup = "default-pool"
	}
	// GCP SDK requires per zone cluster size
	if err := g.ops.SetInstanceGroupSize(g.instanceGroup, perZoneCount, timeout); err != nil {
		return fmt.Errorf("failed to set size of node pool %s. Error: %v", g.instanceGroup, err)
	}

	return nil
}

func (g *Gke) GetASGClusterSize() (int64, error) {

	instanceGroup := os.Getenv("INSTANCE_GROUP")
	if len(instanceGroup) != 0 {
		g.instanceGroup = instanceGroup
	} else {
		g.instanceGroup = "default-pool"
	}
	nodeCount, err := g.ops.GetInstanceGroupSize(g.instanceGroup)
	if err != nil {
		return 0, fmt.Errorf("failed to get size of GKE Node Pool [%s] Err: %v", g.instanceGroup, err)
	}

	return nodeCount, nil
}

// upgradeGkeControlPlaneVersion upgrade GKE Control Plane to a specified version
func (g *Gke) upgradeGkeControlPlaneVersion(version string, timeout time.Duration) error {
	log.Infof("Upgrade GKE Control Plane version to [%s]", version)
	if err := g.ops.SetClusterVersion(version, timeout); err != nil {
		return fmt.Errorf("failed to set version for GKE Control Plane to [%s], Err: %v", version, err)
	}
	log.Infof("GKE Control Plane version was successfully set to [%s]", version)
	return nil
}

// upgradeGkeNodeGroupVersion upgrade specified GKE Node Group to a specified version
func (g *Gke) upgradeGkeNodeGroupVersion(nodeGroup, version string, timeout time.Duration) error {
	log.Infof("Upgrade GKE Node Group [%s] version to [%s]", nodeGroup, version)
	if err := g.ops.SetInstanceGroupVersion(nodeGroup, version, timeout); err != nil {
		return fmt.Errorf("failed to set version for GKE Node Group [%s] to [%s], Err: %v", nodeGroup, version, err)
	}
	log.Infof("GKE Node Group [%s] version was successfully set to [%s]", nodeGroup, version)
	return nil
}

// updateGkeNodeGroupUpgradeStrategy updates specified GKE Node Group's upgrade strategy
func (g *Gke) updateGkeNodeGroupUpgradeStrategy(nodeGroup, upgradeStrategy string, timeout time.Duration, surgeSetting string) error {
	log.Infof("Updating GKE Node Group's [%s] Upgrade Strategy to [%s]", nodeGroup, upgradeStrategy)

	if err := g.ops.SetInstanceUpgradeStrategy(nodeGroup, upgradeStrategy, timeout, surgeSetting); err != nil {
		return fmt.Errorf("failed to set upgrade strategy for GKE Node Group [%s] to [%s], Err: %v", nodeGroup, upgradeStrategy, err)
	}
	log.Infof("GKE Node Group [%s] upgrade strategy was successfully set to [%s]", nodeGroup, upgradeStrategy)
	return nil
}

func (g *Gke) DeleteNode(node node.Node) error {
	if err := g.ops.DeleteInstance(node.Name, node.Zone, 10*time.Minute); err != nil {
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
