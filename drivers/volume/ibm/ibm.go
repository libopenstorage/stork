package ibm

import (
	"fmt"

	ibmcore "github.com/IBM/go-sdk-core/v5/core"
	"github.com/IBM/vpc-go-sdk/vpcv1"
	"github.com/libopenstorage/openstorage/api"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/volume"
	torpedovolume "github.com/portworx/torpedo/drivers/volume"
	"github.com/portworx/torpedo/drivers/volume/portworx"
	"github.com/portworx/torpedo/drivers/volume/portworx/schedops"
	"github.com/portworx/torpedo/pkg/errors"
	"github.com/portworx/torpedo/pkg/log"
)

const (
	IbmDriverName = "ibm"
	// IbmServiceName is the name of the ibm storage driver implementation
	IbmServiceName = "ibm-csi-controller"
	serviceURL     = "https://us-east.iaas.cloud.ibm.com/v1"
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

func (i *ibm) GetProxySpecForAVolume(volume *torpedovolume.Volume) (*api.ProxySpec, error) {
	log.Warnf("GetProxySpecForAVolume function has not been implemented for volume driver - %s", i.String())
	return nil, nil
}

func (i *ibm) InspectCurrentCluster() (*api.SdkClusterInspectCurrentResponse, error) {
	log.Warnf("InspectCurrentCluster function has not been implemented for volume driver - %s", i.String())
	return nil, nil
}

// InspectVolume inspects the volume with the given name
func (i *ibm) InspectVolume(name string) (*api.Volume, error) {
	log.Warnf("InspectVolume function has not been implemented for volume driver - %s", i.String())
	return nil, &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "InspectVolume()",
	}
}

// UpdateFBDANFSEndpoint updates the NFS endpoint for a given FBDA volume
func (i *ibm) UpdateFBDANFSEndpoint(volumeName string, newEndpoint string) error {
	log.Warnf("UpdateFBDANFSEndpoint function has not been implemented for volume driver - %s", i.String())
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "UpdateFBDANFSEndpoint()",
	}
}

// ValidatePureFBDAMountSource checks that, on all the given nodes, all the provided FBDA volumes are mounted using the expected IP
func (i *ibm) ValidatePureFBDAMountSource(nodes []node.Node, vols []*volume.Volume, expectedIP string) error {
	log.Warnf("ValidatePureFBDAMountSource function has not been implemented for volume driver - %s", i.String())
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "ValidatePureFBDAMountSource()",
	}
}

// DeleteSnapshotsForVolumes deletes snapshots for the specified volumes in ibm cloud
func (i *ibm) DeleteSnapshotsForVolumes(volumeNames []string, apiKey string) error {

	// Initialize the IBM Cloud VPC service client
	authenticator := &ibmcore.IamAuthenticator{
		ApiKey: apiKey,
	}

	options := &vpcv1.VpcV1Options{
		URL:           serviceURL,
		Authenticator: authenticator,
	}
	vpcService, err := vpcv1.NewVpcV1(options)
	if err != nil {
		return fmt.Errorf("error creating VPC service client: %s", err)
	}

	// Iterate over each volume name
	for _, volumeName := range volumeNames {
		// Find the volume by name
		findVolumeOptions := vpcService.NewListVolumesOptions()
		findVolumeOptions.SetName(volumeName)
		volumes, _, err := vpcService.ListVolumes(findVolumeOptions)
		log.Infof("volumes from the vpc service %s", volumes)
		if err != nil {
			return fmt.Errorf("error finding volume '%s': %s", volumeName, err)
		}
		if len(volumes.Volumes) == 0 {
			continue
		}
		volumeID := *volumes.Volumes[0].ID

		// List all snapshots
		snapshots, _, err := vpcService.ListSnapshots(vpcService.NewListSnapshotsOptions())
		if err != nil {
			return fmt.Errorf("error listing snapshots: %s", err)
		}
		log.Infof("volumes from the vpc service %s", snapshots)
		// Delete snapshots associated with the volume
		for _, snapshot := range snapshots.Snapshots {
			if *snapshot.SourceVolume.ID == volumeID {
				// Snapshot belongs to the specified volume, delete it
				snapshotID := *snapshot.ID
				snapshotName := *snapshot.Name
				log.Infof("Deleting snapshot %s associated with volume %s", snapshotName, volumeName)
				_, err = vpcService.DeleteSnapshot(vpcService.NewDeleteSnapshotOptions(snapshotID))
				if err != nil {
					return fmt.Errorf("error deleting snapshot '%s': %s", snapshotName, err)
				}
			}
		}
	}
	return nil
}

func init() {
	log.Infof("Registering IBM volume driver")
	torpedovolume.Register(IbmDriverName, provisionersForIKS, &ibm{})
}
