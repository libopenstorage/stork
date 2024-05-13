package linstor

import (
	"context"
	"fmt"
	"time"

	"github.com/libopenstorage/openstorage/api"

	lclient "github.com/LINBIT/golinstor/client"
	"github.com/portworx/sched-ops/k8s/core"
	"github.com/portworx/sched-ops/task"
	driver_api "github.com/portworx/torpedo/drivers/api"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/volume"
	torpedovolume "github.com/portworx/torpedo/drivers/volume"
	"github.com/portworx/torpedo/drivers/volume/portworx/schedops"
	"github.com/portworx/torpedo/pkg/errors"
	"github.com/portworx/torpedo/pkg/log"
)

const (
	// DriverName is the name of the LINSTOR driver implementation
	DriverName = "linstor"
	// LinstorStorage is LINSTOR's storage driver name
	LinstorStorage       torpedovolume.StorageProvisionerType = "linstor"
	waitVolDriverToCrash                                      = 1 * time.Minute
	defaultRetryInterval                                      = 10 * time.Second
)

// provisioners types of supported provisioners
var provisioners = map[torpedovolume.StorageProvisionerType]torpedovolume.StorageProvisionerType{
	LinstorStorage: "linstor.csi.linbit.com",
}

var k8sCore = core.Instance()

type linstor struct {
	schedOps schedops.Driver
	cli      *lclient.Client
	torpedovolume.DefaultDriver
}

func (d *linstor) String() string {
	return string(LinstorStorage)
}

func (d *linstor) Init(sched, nodeDriver, token, storageProvisioner, csiGenericDriverConfigMap string) error {
	log.Infof("Using the LINSTOR volume driver with provisioner %s under scheduler: %v", storageProvisioner, sched)

	// Configuration of linstor client happens via environment variables:
	// * LS_CONTROLLERS
	// * LS_USERNAME
	// * LS_PASSWORD
	// * LS_USER_CERTIFICATE
	// * LS_USER_KEY
	// * LS_ROOT_CA
	client, err := lclient.NewClient()
	if err != nil {
		return fmt.Errorf("error creating linstor client: %w", err)
	}

	d.cli = client

	// Set provisioner for torpedo
	if storageProvisioner != "" {
		if p, ok := provisioners[torpedovolume.StorageProvisionerType(storageProvisioner)]; ok {
			torpedovolume.StorageProvisioner = p
		} else {
			return fmt.Errorf("driver %s, does not support provisioner %s", DriverName, storageProvisioner)
		}
	} else {
		return fmt.Errorf("Provisioner is empty for volume driver: %s", DriverName)
	}
	return nil
}

func (d *linstor) StopDriver(nodes []node.Node, force bool, triggerOpts *driver_api.TriggerOptions) error {
	stopFn := func() error {
		for _, n := range nodes {
			err := k8sCore.RemoveLabelOnNode(n.Name, "linstor.linbit.com/linstor-node")
			if err != nil {
				return fmt.Errorf("Failed to set label on node %q: %w", n.Name, err)
			}
			log.Infof("Sleeping for %v for volume driver to go down.", waitVolDriverToCrash)
			time.Sleep(waitVolDriverToCrash)
		}
		return nil
	}
	return driver_api.PerformTask(stopFn, triggerOpts)
}

func (d *linstor) StartDriver(n node.Node) error {
	err := k8sCore.AddLabelOnNode(n.Name, "linstor.linbit.com/linstor-node", "true")
	if err != nil {
		return fmt.Errorf("Failed to set label on node %q: %w", n.Name, err)
	}
	return nil
}

func (d *linstor) WaitDriverUpOnNode(n node.Node, timeout time.Duration) error {
	log.Debugf("waiting for LINSTOR node to be up: %s", n.Name)
	t := func() (interface{}, bool, error) {
		log.Debugf("Getting node info: %s", n.Name)

		linstorNode, err := d.cli.Nodes.Get(context.TODO(), n.Name)
		if err != nil {
			return "", true, fmt.Errorf("failed to get info about LINSTOR node '%s': %w", n.Name, err)
		}

		log.Debugf("checking LINSTOR status on node: %s", n.Name)
		switch linstorNode.ConnectionStatus {
		case "ONLINE":
			log.Infof("LINSTOR on node: %s is now up. status: %v", n.Name, linstorNode.ConnectionStatus)
			return "", false, nil
		default:
			return "", true, fmt.Errorf("LINSTOR node '%s' is not online. status: %s", n.Name, linstorNode.ConnectionStatus)
		}
	}

	if _, err := task.DoRetryWithTimeout(t, timeout, defaultRetryInterval); err != nil {
		return err
	}

	log.Debugf("LINSTOR is fully operational on node: %s", n.Name)
	return nil
}

func (d *linstor) GetProxySpecForAVolume(volume *torpedovolume.Volume) (*api.ProxySpec, error) {
	log.Warnf("GetProxySpecForAVolume function has not been implemented for volume driver - %s", d.String())
	return nil, nil
}

func (d *linstor) InspectCurrentCluster() (*api.SdkClusterInspectCurrentResponse, error) {
	log.Warnf("InspectCurrentCluster function has not been implemented for volume driver - %s", d.String())
	return nil, nil
}

func (d *linstor) KillPXDaemon(n []node.Node, triggerOpts *driver_api.TriggerOptions) error {
	log.Warnf("KillPXDaemon function has not been implemented for volume driver - %s", d.String())
	return nil
}

// DeleteSnapshotsForVolumes deletes snapshots for the specified volumes in linstor
func (d *linstor) DeleteSnapshotsForVolumes(volumeNames []string, clusterProviderCredential string) error {
	log.Warnf("DeleteSnapshotsForVolumes function has not been implemented for volume driver - %s", d.String())
	return nil
}

// UpdateFBDANFSEndpoint updates the NFS endpoint for a given FBDA volume
func (d *linstor) UpdateFBDANFSEndpoint(volumeName string, newEndpoint string) error {
	log.Warnf("UpdateFBDANFSEndpoint function has not been implemented for volume driver - %s", d.String())
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "UpdateFBDANFSEndpoint()",
	}
}

// ValidatePureFBDAMountSource checks that, on all the given nodes, all the provided FBDA volumes are mounted using the expected IP
func (d *linstor) ValidatePureFBDAMountSource(nodes []node.Node, vols []*volume.Volume, expectedIP string) error {
	log.Warnf("ValidatePureFBDAMountSource function has not been implemented for volume driver - %s", d.String())
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "ValidatePureFBDAMountSource()",
	}
}

func init() {
	torpedovolume.Register(DriverName, provisioners, &linstor{})
}
