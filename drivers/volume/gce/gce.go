package gce

import (
	"fmt"
	"github.com/libopenstorage/openstorage/api"

	"github.com/portworx/torpedo/pkg/log"

	torpedovolume "github.com/portworx/torpedo/drivers/volume"
	"github.com/portworx/torpedo/drivers/volume/portworx/schedops"
)

const (
	// DriverName is the name of the gce driver implementation
	DriverName = "gce"
	// GceStorage GCE storage driver name
	GceStorage torpedovolume.StorageProvisionerType = "gce"
)

// Provisioners types of supported provisioners
var provisioners = map[torpedovolume.StorageProvisionerType]torpedovolume.StorageProvisionerType{
	GceStorage: "kubernetes.io/gce-pd",
}

type gce struct {
	schedOps schedops.Driver
	torpedovolume.DefaultDriver
}

func (d *gce) String() string {
	return string(GceStorage)
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

func init() {
	torpedovolume.Register(DriverName, provisioners, &gce{})
}
