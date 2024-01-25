package gce

import (
	"fmt"
	"github.com/libopenstorage/openstorage/api"

	"github.com/portworx/torpedo/pkg/log"

	torpedovolume "github.com/portworx/torpedo/drivers/volume"
	"github.com/portworx/torpedo/drivers/volume/portworx/schedops"
	"github.com/portworx/torpedo/pkg/errors"
)

const (
	// DriverName is the name of the gce driver implementation
	DriverName = "gce"
	// GceStorage GCE storage driver name
	GceStorage         torpedovolume.StorageProvisionerType = "gce-pd"
	GceStandardStorage torpedovolume.StorageProvisionerType = "pd-csi-storage-gke"
)

// Provisioners types of supported provisioners
var provisioners = map[torpedovolume.StorageProvisionerType]torpedovolume.StorageProvisionerType{
	GceStorage:         "kubernetes.io/gce-pd",
	GceStandardStorage: "pd.csi.storage.gke.io",
}

type gce struct {
	schedOps schedops.Driver
	torpedovolume.DefaultDriver
}

func (d *gce) String() string {
	return string(DriverName)
}

func (d *gce) GetProxySpecForAVolume(volume *torpedovolume.Volume) (*api.ProxySpec, error) {
	log.Warnf("GetProxySpecForAVolume function has not been implemented for volume driver - %s", d.String())
	return nil, nil
}

func (d *gce) InspectCurrentCluster() (*api.SdkClusterInspectCurrentResponse, error) {
	log.Warnf("InspectCurrentCluster function has not been implemented for volume driver - %s", d.String())
	return nil, nil
}

func (d *gce) Init(sched, nodeDriver, token, storageProvisioner, csiGenericDriverConfigMap string) error {
	log.Infof("Using the GCE volume driver with provisioner %s under scheduler: %v", storageProvisioner, sched)
	torpedovolume.StorageDriver = DriverName
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

func (d *gce) ValidateCreateVolume(name string, params map[string]string) error {
	// TODO: Implementation of ValidateCreateVolume will be provided in the coming PRs
	log.Warnf("ValidateCreateVolume function has not been implemented for volume driver - %s", d.String())
	return nil
}

func (d *gce) ValidateVolumeSetup(vol *torpedovolume.Volume) error {
	// TODO: Implementation of ValidateVolumeSetup will be provided in the coming PRs
	log.Warnf("ValidateVolumeSetup function has not been implemented for volume driver - %s", d.String())
	return nil
}

func (d *gce) ValidateDeleteVolume(vol *torpedovolume.Volume) error {
	// TODO: Implementation of ValidateDeleteVolume will be provided in the coming PRs
	log.Warnf("ValidateDeleteVolume function has not been implemented for volume driver - %s", d.String())
	return nil
}

func (d *gce) GetDriverVersion() (string, error) {
	// TODO: Implementation of ValidateDeleteVolume will be provided in the coming PRs
	log.Warnf("GetDriverVersion function has not been implemented for volume driver - %s", d.String())
	return "", nil
}

// RefreshDriverEndpoints get the updated driver endpoints for the cluster
func (d *gce) RefreshDriverEndpoints() error {

	log.Warnf("RefreshDriverEndpoints function has not been implemented for volume driver - %s", d.String())
	return nil
}

// InspectVolume inspects the volume with the given name
func (d *gce) InspectVolume(name string) (*api.Volume, error) {
	log.Warnf("InspectVolume function has not been implemented for volume driver - %s", d.String())
	return nil, &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "InspectVolume()",
	}
}
func init() {
	torpedovolume.Register(DriverName, provisioners, &gce{})
}
