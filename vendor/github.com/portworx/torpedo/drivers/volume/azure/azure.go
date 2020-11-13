package azure

import (
	"fmt"

	torpedovolume "github.com/portworx/torpedo/drivers/volume"
	"github.com/portworx/torpedo/drivers/volume/portworx/schedops"
	"github.com/sirupsen/logrus"
)

const (
	// DriverName is the name of the azure driver implementation
	DriverName = "azure"
	// AzureStorage Azure storage driver name
	AzureStorage torpedovolume.StorageProvisionerType = "azure"
)

// Provisioners types of supported provisioners
var provisioners = map[torpedovolume.StorageProvisionerType]torpedovolume.StorageProvisionerType{
	AzureStorage: "kubernetes.io/azure-disk",
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
	return nil
}

func (d *azure) Init(sched, nodeDriver, token, storageProvisioner, csiGenericDriverConfigMap string) error {
	logrus.Infof("Using the Azure volume driver with provisioner %s under scheduler: %v", storageProvisioner, sched)
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
