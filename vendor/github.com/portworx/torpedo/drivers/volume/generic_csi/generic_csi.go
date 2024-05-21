package csi

import (
	"fmt"

	"github.com/libopenstorage/openstorage/api"

	"github.com/portworx/sched-ops/k8s/core"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/volume"
	torpedovolume "github.com/portworx/torpedo/drivers/volume"
	"github.com/portworx/torpedo/drivers/volume/portworx/schedops"
	"github.com/portworx/torpedo/pkg/errors"
	"github.com/portworx/torpedo/pkg/log"
)

const (
	// DriverName is the name of the aws driver implementation
	DriverName = "csi"
	// CsiStorage CSI storage driver name
	CsiStorage torpedovolume.StorageProvisionerType = "generic_csi"
	// CsiStorageClassKey CSI Generic driver config map key name
	CsiStorageClassKey = "csi_storageclass_key"
)

// Provisioners types of supported provisioners
var provisioners = map[torpedovolume.StorageProvisionerType]torpedovolume.StorageProvisionerType{
	CsiStorage: "csi",
}

type genericCsi struct {
	schedOps schedops.Driver
	torpedovolume.DefaultDriver
}

func (d *genericCsi) String() string {
	return string(CsiStorage)
}

func (d *genericCsi) ValidateVolumeCleanup() error {
	return nil
}

func (d *genericCsi) RefreshDriverEndpoints() error {
	return nil
}

func (d *genericCsi) GetProxySpecForAVolume(volume *torpedovolume.Volume) (*api.ProxySpec, error) {
	log.Warnf("GetProxySpecForAVolume function has not been implemented for volume driver - %s", d.String())
	return nil, nil
}

func (d *genericCsi) InspectCurrentCluster() (*api.SdkClusterInspectCurrentResponse, error) {
	log.Warnf("InspectCurrentCluster function has not been implemented for volume driver - %s", d.String())
	return nil, nil
}

func (d *genericCsi) Init(sched, nodeDriver, token, storageProvisioner, csiGenericDriverConfigMap string) error {
	log.Infof("Using the generic CSI volume driver with provisioner %s under scheduler: %v", storageProvisioner, sched)
	torpedovolume.StorageDriver = DriverName
	// Set provisioner for torpedo, from
	if storageProvisioner == string(CsiStorage) {
		// Get the provisioner from the config map
		configMap, err := core.Instance().GetConfigMap(csiGenericDriverConfigMap, "default")
		if err != nil {
			return fmt.Errorf("Failed to get config map for volume driver: %s, provisioner: %s", DriverName, storageProvisioner)
		}
		if p, ok := configMap.Data[CsiStorageClassKey]; ok {
			torpedovolume.StorageProvisioner = torpedovolume.StorageProvisionerType(p)
		}
	} else {
		return fmt.Errorf("Invalid provisioner %s for volume driver: %s", storageProvisioner, DriverName)
	}
	return nil
}

// DeleteSnapshotsForVolumes deletes snapshots for the specified volumes in google cloud
func (d *genericCsi) DeleteSnapshotsForVolumes(volumeNames []string, clusterProviderCredential string) error {
	log.Warnf("DeleteSnapshotsForVolumes function has not been implemented for volume driver - %s", d.String())
	return nil
}

// UpdateFBDANFSEndpoint updates the NFS endpoint for a given FBDA volume
func (d *genericCsi) UpdateFBDANFSEndpoint(volumeName string, newEndpoint string) error {
	log.Warnf("UpdateFBDANFSEndpoint function has not been implemented for volume driver - %s", d.String())
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "UpdateFBDANFSEndpoint()",
	}
}

// ValidatePureFBDAMountSource checks that, on all the given nodes, all the provided FBDA volumes are mounted using the expected IP
func (d *genericCsi) ValidatePureFBDAMountSource(nodes []node.Node, vols []*volume.Volume, expectedIP string) error {
	log.Warnf("ValidatePureFBDAMountSource function has not been implemented for volume driver - %s", d.String())
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "ValidatePureFBDAMountSource()",
	}
}

func init() {
	torpedovolume.Register(DriverName, provisioners, &genericCsi{})
}
