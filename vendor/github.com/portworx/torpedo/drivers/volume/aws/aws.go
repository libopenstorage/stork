package aws

import (
	"fmt"

	"github.com/libopenstorage/openstorage/api"

	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/volume"
	torpedovolume "github.com/portworx/torpedo/drivers/volume"
	"github.com/portworx/torpedo/drivers/volume/portworx/schedops"
	"github.com/portworx/torpedo/pkg/errors"
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

func (d *aws) InspectCurrentCluster() (*api.SdkClusterInspectCurrentResponse, error) {
	log.Warnf("InspectCurrentCluster function has not been implemented for volume driver - %s", d.String())
	return nil, nil
}

// DeleteSnapshotsForVolumes deletes snapshots for the specified volumes in aws cloud
func (i *aws) DeleteSnapshotsForVolumes(volumeNames []string, clusterProviderCredential string) error {
	log.Warnf("DeleteSnapshotsForVolumes function has not been implemented for volume driver - %s", i.String())
	return nil
}

// UpdateFBDANFSEndpoint updates the NFS endpoint for a given FBDA volume
func (d *aws) UpdateFBDANFSEndpoint(volumeName string, newEndpoint string) error {
	log.Warnf("UpdateFBDANFSEndpoint function has not been implemented for volume driver - %s", d.String())
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "UpdateFBDANFSEndpoint()",
	}
}

// ValidatePureFBDAMountSource checks that, on all the given nodes, all the provided FBDA volumes are mounted using the expected IP
func (d *aws) ValidatePureFBDAMountSource(nodes []node.Node, vols []*volume.Volume, expectedIP string) error {
	log.Warnf("ValidatePureFBDAMountSource function has not been implemented for volume driver - %s", d.String())
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "ValidatePureFBDAMountSource()",
	}
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
