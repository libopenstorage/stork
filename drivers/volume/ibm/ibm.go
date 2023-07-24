package ibm

import (
	"fmt"
	torpedovolume "github.com/portworx/torpedo/drivers/volume"
	"github.com/portworx/torpedo/drivers/volume/portworx"
	"github.com/portworx/torpedo/drivers/volume/portworx/schedops"
	"github.com/portworx/torpedo/pkg/log"
)

const (
	IbmDriverName = "ibm"
	// IbmServiceName is the name of the ibm storage driver implementation
	IbmServiceName = "ibm-csi-controller"
)

// Provisioners types of supported provisioners
var provisionersForIKS = map[torpedovolume.StorageProvisionerType]torpedovolume.StorageProvisionerType{
	IbmDriverName: "vpc.block.csi.ibm.io",
}

type ibm struct {
	schedOps schedops.Driver
	torpedovolume.DefaultDriver
}

func (i *ibm) Init(sched, nodeDriver, token, storageProvisioner, csiGenericDriverConfigMap string) error {
	log.Infof("Using the IBM volume driver with provisioner %s under scheduler: %v", storageProvisioner, sched)
	torpedovolume.StorageDriver = IbmDriverName
	// Set provisioner for torpedo
	if storageProvisioner != "" {
		if p, ok := provisionersForIKS[torpedovolume.StorageProvisionerType(storageProvisioner)]; ok {
			torpedovolume.StorageProvisioner = p
		} else {
			return fmt.Errorf("driver %s, does not support provisioner %s", portworx.DriverName, storageProvisioner)
		}
	} else {
		return fmt.Errorf("Provisioner is empty for volume driver: %s", portworx.DriverName)
	}
	return nil
}

func (i *ibm) String() string {
	return IbmDriverName
}

func (i *ibm) ValidateCreateVolume(name string, params map[string]string) error {
	// TODO: Implementation of ValidateCreateVolume will be provided in the coming PRs
	log.Warnf("ValidateCreateVolume function has not been implemented for volume driver - %s", i.String())
	return nil
}

func (i *ibm) ValidateVolumeSetup(vol *torpedovolume.Volume) error {
	// TODO: Implementation of ValidateVolumeSetup will be provided in the coming PRs
	log.Warnf("ValidateVolumeSetup function has not been implemented for volume driver - %s", i.String())
	return nil
}

func (i *ibm) ValidateDeleteVolume(vol *torpedovolume.Volume) error {
	// TODO: Implementation of ValidateDeleteVolume will be provided in the coming PRs
	log.Warnf("ValidateDeleteVolume function has not been implemented for volume driver - %s", i.String())
	return nil
}

func (i *ibm) GetDriverVersion() (string, error) {
	// TODO: Implementation of ValidateDeleteVolume will be provided in the coming PRs
	log.Warnf("GetDriverVersion function has not been implemented for volume driver - %s", i.String())
	return "", nil
}

// RefreshDriverEndpoints get the updated driver endpoints for the cluster
func (i *ibm) RefreshDriverEndpoints() error {
	log.Warnf("RefreshDriverEndpoints function has not been implemented for volume driver - %s", i.String())
	return nil
}

func init() {
	log.Infof("Registering pso driver")
	torpedovolume.Register(IbmDriverName, provisionersForIKS, &ibm{})
}
