package pso

import (
	"fmt"
	"strings"

	"github.com/libopenstorage/openstorage/api"

	"github.com/portworx/sched-ops/k8s/core"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/volume"
	torpedovolume "github.com/portworx/torpedo/drivers/volume"
	"github.com/portworx/torpedo/drivers/volume/portworx"
	"github.com/portworx/torpedo/drivers/volume/portworx/schedops"
	"github.com/portworx/torpedo/pkg/errors"
	"github.com/portworx/torpedo/pkg/log"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	// PureDriverName is the name of the portworx-pure driver implementation
	PureDriverName     = "pso"
	PureFileDriverName = "pso-file"
	PsoServiceName     = "pso-csi-controller"
)

// Provisioners types of supported provisioners
var provisionersForPure = map[torpedovolume.StorageProvisionerType]torpedovolume.StorageProvisionerType{
	PureDriverName:     "pure-csi",
	PureFileDriverName: "pure-csi",
}

// pure is essentially the same as the portworx volume driver, just different in name. This way,
// we can have separate specs for pure volumes vs. normal portworx ones
type pso struct {
	schedOps schedops.Driver
	torpedovolume.DefaultDriver
}

func (d *pso) Init(sched, nodeDriver, token, storageProvisioner, csiGenericDriverConfigMap string) error {
	log.Infof("Using the Pure volume driver with provisioner %s under scheduler: %v", storageProvisioner, sched)
	torpedovolume.StorageDriver = PureDriverName
	// Set provisioner for torpedo
	if storageProvisioner != "" {
		if p, ok := provisionersForPure[torpedovolume.StorageProvisionerType(storageProvisioner)]; ok {
			torpedovolume.StorageProvisioner = p
		} else {
			return fmt.Errorf("driver %s, does not support provisioner %s", portworx.DriverName, storageProvisioner)
		}
	} else {
		return fmt.Errorf("Provisioner is empty for volume driver: %s", portworx.DriverName)
	}
	return nil
}

func (d *pso) String() string {
	return PureDriverName
}

func (d *pso) ValidateCreateVolume(name string, params map[string]string) error {
	// TODO: Implementation of ValidateCreateVolume will be provided in the coming PRs
	log.Warnf("ValidateCreateVolume function has not been implemented for volume driver - %s", d.String())
	return nil
}

func (d *pso) ValidateVolumeSetup(vol *torpedovolume.Volume) error {
	// TODO: Implementation of ValidateVolumeSetup will be provided in the coming PRs
	log.Warnf("ValidateVolumeSetup function has not been implemented for volume driver - %s", d.String())
	return nil
}

func (d *pso) ValidateDeleteVolume(vol *torpedovolume.Volume) error {
	// TODO: Implementation of ValidateDeleteVolume will be provided in the coming PRs
	log.Warnf("ValidateDeleteVolume function has not been implemented for volume driver - %s", d.String())
	return nil
}

func (d *pso) GetDriverVersion() (string, error) {
	labelSelectors := map[string]string{
		"app": "pso-csi-node",
	}
	namespace, err := GetPsoNamespace()
	if err != nil {
		return "", err
	}
	pods, err := core.Instance().GetPods(namespace, labelSelectors)
	if err != nil {
		return "", err
	}
	podImage := pods.Items[0].Spec.Containers[1].Image
	psoVersion := strings.Split(podImage, ":")[1]
	log.Infof("PSO Version - %s", psoVersion)
	return psoVersion, nil
}

// RefreshDriverEndpoints get the updated driver endpoints for the cluster
func (d *pso) RefreshDriverEndpoints() error {
	log.Warnf("RefreshDriverEndpoints function has not been implemented for volume driver - %s", d.String())
	return nil
}

func (d *pso) GetProxySpecForAVolume(volume *torpedovolume.Volume) (*api.ProxySpec, error) {
	log.Warnf("GetProxySpecForAVolume function has not been implemented for volume driver - %s", d.String())
	return nil, nil
}

func (d *pso) InspectCurrentCluster() (*api.SdkClusterInspectCurrentResponse, error) {
	log.Warnf("InspectCurrentCluster function has not been implemented for volume driver - %s", d.String())
	return nil, nil
}

// DeleteSnapshotsForVolumes deletes snapshots for the specified volumes
func (i *pso) DeleteSnapshotsForVolumes(volumeNames []string, clusterProviderCredential string) error {
	log.Warnf("DeleteSnapshotsForVolumes function has not been implemented for volume driver - %s", i.String())
	return nil
}

// UpdateFBDANFSEndpoint updates the NFS endpoint for a given FBDA volume
func (i *pso) UpdateFBDANFSEndpoint(volumeName string, newEndpoint string) error {
	log.Warnf("UpdateFBDANFSEndpoint function has not been implemented for volume driver - %s", i.String())
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "UpdateFBDANFSEndpoint()",
	}
}

// ValidatePureFBDAMountSource checks that, on all the given nodes, all the provided FBDA volumes are mounted using the expected IP
func (i *pso) ValidatePureFBDAMountSource(nodes []node.Node, vols []*volume.Volume, expectedIP string) error {
	log.Warnf("ValidatePureFBDAMountSource function has not been implemented for volume driver - %s", i.String())
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "ValidatePureFBDAMountSource()",
	}
}


// GetPsoNamespace returns namespace where PSO is running
func GetPsoNamespace() (string, error) {
	allServices, err := core.Instance().ListServices("", metav1.ListOptions{})
	if err != nil {
		return "", fmt.Errorf("failed to get list of services. Err: %v", err)
	}
	for _, svc := range allServices.Items {
		if svc.Name == PsoServiceName {
			return svc.Namespace, nil
		}
	}
	return "", fmt.Errorf("can't find PSO service [%s] from list of services", PsoServiceName)
}

func init() {
	log.Infof("Registering pso driver")
	torpedovolume.Register(PureDriverName, provisionersForPure, &pso{})
	torpedovolume.Register(PureFileDriverName, provisionersForPure, &pso{})
}
