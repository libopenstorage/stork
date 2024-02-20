package azure

import (
	"fmt"

	"github.com/libopenstorage/openstorage/api"

	torpedovolume "github.com/portworx/torpedo/drivers/volume"
	"github.com/portworx/torpedo/drivers/volume/portworx/schedops"
	"github.com/portworx/torpedo/pkg/errors"
	"github.com/portworx/torpedo/pkg/log"
)

const (
	// DriverName is the name of the azure driver implementation
	DriverName = "azure"
	// AzureStorage Azure storage driver name
	AzureStorage torpedovolume.StorageProvisionerType = "azure"
)

// Provisioners types of supported provisioners
var provisioners = map[torpedovolume.StorageProvisionerType]torpedovolume.StorageProvisionerType{
	AzureStorage: "disk.csi.azure.com",
}

type azure struct {
	schedOps schedops.Driver
	torpedovolume.DefaultDriver
}

func (d *azure) String() string {
	return string(AzureStorage)
}

func (d *azure) ValidateVolumeCleanup() error {
	return nil
}

func (d *azure) RefreshDriverEndpoints() error {
	log.Warnf("RefreshDriverEndpoints function has not been implemented for volume driver - %s", d.String())
	return nil
}

func (d *azure) GetProxySpecForAVolume(volume *torpedovolume.Volume) (*api.ProxySpec, error) {
	log.Warnf("GetProxySpecForAVolume function has not been implemented for volume driver - %s", d.String())
	return nil, nil
}

func (d *azure) InspectCurrentCluster() (*api.SdkClusterInspectCurrentResponse, error) {
	log.Warnf("InspectCurrentCluster function has not been implemented for volume driver - %s", d.String())
	return nil, nil
}

func (d *azure) GetDriverVersion() (string, error) {
	// TODO: Implementation of GetDriverVersion will be provided in the coming PRs
	log.Warnf("GetDriverVersion function has not been implemented for volume driver - %s", d.String())
	return "", nil
}

func (d *azure) ValidateCreateVolume(name string, params map[string]string) error {
	// TODO: Implementation of ValidateCreateVolume will be provided in the coming PRs
	log.Warnf("ValidateCreateVolume function has not been implemented for volume driver - %s", d.String())
	return nil
}

func (d *azure) ValidateVolumeSetup(vol *torpedovolume.Volume) error {
	// TODO: Implementation of ValidateVolumeSetup will be provided in the coming PRs
	log.Warnf("ValidateVolumeSetup function has not been implemented for volume driver - %s", d.String())
	return nil
}

func (d *azure) ValidateDeleteVolume(vol *torpedovolume.Volume) error {
	// TODO: Implementation of ValidateDeleteVolume will be provided in the coming PRs
	log.Warnf("ValidateDeleteVolume function has not been implemented for volume driver - %s", d.String())
	return nil
}

// InspectVolume inspects the volume with the given name
func (d *azure) InspectVolume(name string) (*api.Volume, error) {
	log.Warnf("InspectVolume function has not been implemented for volume driver - %s", d.String())
	return nil, &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "InspectVolume()",
	}
}

func (d *azure) Init(sched, nodeDriver, token, storageProvisioner, csiGenericDriverConfigMap string) error {
	log.Infof("Using the Azure volume driver with provisioner %s under scheduler: %v", storageProvisioner, sched)
	torpedovolume.StorageDriver = DriverName
	// Set provisioner for torpedo
	if storageProvisioner != "" {
		if p, ok := provisioners[torpedovolume.StorageProvisionerType(storageProvisioner)]; ok {
			torpedovolume.StorageProvisioner = p
		} else {
			return fmt.Errorf("driver %s, does not support provisioner %s", DriverName, storageProvisioner)
		}
	} else {
		torpedovolume.StorageProvisioner = provisioners[torpedovolume.DefaultStorageProvisioner]
	}
	return nil
}

func init() {
	torpedovolume.Register(DriverName, provisioners, &azure{})
}
