package csi

import (
	"fmt"

	"github.com/portworx/sched-ops/k8s/core"
	torpedovolume "github.com/portworx/torpedo/drivers/volume"
	"github.com/portworx/torpedo/drivers/volume/portworx/schedops"
	"github.com/sirupsen/logrus"
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

func (d *genericCsi) Init(sched, nodeDriver, token, storageProvisioner, csiGenericDriverConfigMap string) error {
	logrus.Infof("Using the generic CSI volume driver with provisioner %s under scheduler: %v", storageProvisioner, sched)
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

func init() {
	torpedovolume.Register(DriverName, provisioners, &genericCsi{})
}
