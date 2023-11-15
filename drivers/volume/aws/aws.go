package aws

import (
	"fmt"
	"github.com/libopenstorage/openstorage/api"

	torpedovolume "github.com/portworx/torpedo/drivers/volume"
	"github.com/portworx/torpedo/drivers/volume/portworx/schedops"
	"github.com/portworx/torpedo/pkg/log"
)

const (
	// DriverName is the name of the aws driver implementation
	DriverName = "aws"
	// AwsStorage AWS storage driver name
	AwsStorage torpedovolume.StorageProvisionerType = "aws"
)

// Provisioners types of supported provisioners
var provisioners = map[torpedovolume.StorageProvisionerType]torpedovolume.StorageProvisionerType{
	AwsStorage: "kubernetes.io/aws-ebs",
}

type aws struct {
	schedOps schedops.Driver
	torpedovolume.DefaultDriver
}

func (d *aws) String() string {
	return string(AwsStorage)
}

func (d *aws) ValidateVolumeCleanup() error {
	return nil
}

func (d *aws) RefreshDriverEndpoints() error {
	return nil
}

func (d *aws) GetProxySpecForAVolume(volume *torpedovolume.Volume) (*api.ProxySpec, error) {
	log.Warnf("GetProxySpecForAVolume function has not been implemented for volume driver - %s", d.String())
	return nil, nil
}

func (d *aws) Init(sched, nodeDriver, token, storageProvisioner, csiGenericDriverConfigMap string) error {
	log.Infof("Using the AWS EBS volume driver with provisioner %s under scheduler: %v", storageProvisioner, sched)
	torpedovolume.StorageDriver = DriverName
	// Set provisioner for torpedo
	if storageProvisioner != "" {
		if p, ok := provisioners[torpedovolume.StorageProvisionerType(storageProvisioner)]; ok {
			torpedovolume.StorageProvisioner = p
		} else {
			torpedovolume.StorageProvisioner = provisioners[torpedovolume.DefaultStorageProvisioner]
		}
	} else {
		return fmt.Errorf("Provisioner is empty for volume driver: %s", DriverName)
	}
	return nil
}

func init() {
	torpedovolume.Register(DriverName, provisioners, &aws{})
}
