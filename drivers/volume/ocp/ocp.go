package ocp

import (
	"fmt"

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
	OcpDriverName        = "ocp"
	OcpRbdCephDriverName = "rbd-csi"
	OcpCephfsDriverName  = "cephfs-csi"
	OcpRgwDriverName     = "rgw-csi"
	// OcpServiceName is the name of the ocp storage driver implementation
	OcpServiceName = ""
)

// Provisioners types of supported provisioners
var provisionersForOcp = map[torpedovolume.StorageProvisionerType]torpedovolume.StorageProvisionerType{
	OcpRbdCephDriverName: "openshift-storage.rbd.csi.ceph.com",
	OcpCephfsDriverName:  "openshift-storage.cephfs.csi.ceph.com",
	OcpRgwDriverName:     "openshift-storage.ceph.rook.io/bucket",
}

type ocp struct {
	schedOps schedops.Driver
	torpedovolume.DefaultDriver
}

func (o *ocp) Init(sched, nodeDriver, token, storageProvisioner, csiGenericDriverConfigMap string) error {
	log.Infof("Using the OCP volume driver with provisioner %s under scheduler: %v", storageProvisioner, sched)
	torpedovolume.StorageDriver = OcpDriverName
	// Set provisioner for torpedo
	if storageProvisioner != "" {
		if p, ok := provisionersForOcp[torpedovolume.StorageProvisionerType(storageProvisioner)]; ok {
			torpedovolume.StorageProvisioner = p
		} else {
			return fmt.Errorf("driver %s, does not support provisioner %s", portworx.DriverName, storageProvisioner)
		}
	} else {
		return fmt.Errorf("Provisioner is empty for volume driver: %s", portworx.DriverName)
	}
	return nil
}

func (o *ocp) String() string {
	return OcpDriverName
}

func (o *ocp) ValidateCreateVolume(name string, params map[string]string) error {
	// TODO: Implementation of ValidateCreateVolume will be provided in the coming PRs
	log.Warnf("ValidateCreateVolume function has not been implemented for volume driver - %s", o.String())
	return nil
}

func (o *ocp) ValidateVolumeSetup(vol *torpedovolume.Volume) error {
	// TODO: Implementation of ValidateVolumeSetup will be provided in the coming PRs
	log.Warnf("ValidateVolumeSetup function has not been implemented for volume driver - %s", o.String())
	return nil
}

func (o *ocp) ValidateDeleteVolume(vol *torpedovolume.Volume) error {
	// TODO: Implementation of ValidateDeleteVolume will be provided in the coming PRs
	log.Warnf("ValidateDeleteVolume function has not been implemented for volume driver - %s", o.String())
	return nil
}

func (o *ocp) GetDriverVersion() (string, error) {
	// TODO: Implementation of ValidateDeleteVolume will be provided in the coming PRs
	log.Warnf("GetDriverVersion function has not been implemented for volume driver - %s", o.String())
	return "", nil
}

// RefreshDriverEndpoints get the updated driver endpoints for the cluster
func (o *ocp) RefreshDriverEndpoints() error {

	log.Warnf("RefreshDriverEndpoints function has not been implemented for volume driver - %s", o.String())
	return nil
}

func (o *ocp) GetProxySpecForAVolume(volume *torpedovolume.Volume) (*api.ProxySpec, error) {
	log.Warnf("GetProxySpecForAVolume function has not been implemented for volume driver - %s", o.String())
	return nil, nil
}

func (o *ocp) InspectCurrentCluster() (*api.SdkClusterInspectCurrentResponse, error) {
	log.Warnf("InspectCurrentCluster function has not been implemented for volume driver - %s", o.String())
	return nil, nil
}

// InspectVolume inspects the volume with the given name
func (o *ocp) InspectVolume(name string) (*api.Volume, error) {
	log.Warnf("InspectVolume function has not been implemented for volume driver - %s", o.String())
	return nil, &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "InspectVolume()",
	}
}

// UpdateFBDANFSEndpoint updates the NFS endpoint for a given FBDA volume
func (o *ocp) UpdateFBDANFSEndpoint(volumeName string, newEndpoint string) error {
	log.Warnf("UpdateFBDANFSEndpoint function has not been implemented for volume driver - %s", o.String())
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "UpdateFBDANFSEndpoint()",
	}
}

// ValidatePureFBDAMountSource checks that, on all the given nodes, all the provided FBDA volumes are mounted using the expected IP
func (o *ocp) ValidatePureFBDAMountSource(nodes []node.Node, vols []*volume.Volume, expectedIP string) error {
	log.Warnf("ValidatePureFBDAMountSource function has not been implemented for volume driver - %s", o.String())
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "ValidatePureFBDAMountSource()",
	}
}

// DeleteSnapshotsForVolumes deletes snapshots for the specified volumes in ocp cluster
func (o *ocp) DeleteSnapshotsForVolumes(volumeNames []string, clusterProviderCredential string) error {
	log.Warnf("DeleteSnapshotsForVolumes function has not been implemented for volume driver - %s", o.String())
	return nil
}

func init() {
	log.Infof("Registering ocp driver")
	torpedovolume.Register(OcpDriverName, provisionersForOcp, &ocp{})
}
