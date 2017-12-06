package snapshotcontroller

import (
	"errors"
	"fmt"

	"github.com/kubernetes-incubator/external-storage/lib/controller"
	crdv1 "github.com/kubernetes-incubator/external-storage/snapshot/pkg/apis/crd/v1"
	crdclient "github.com/kubernetes-incubator/external-storage/snapshot/pkg/client"
	"github.com/kubernetes-incubator/external-storage/snapshot/pkg/volume"
	log "github.com/sirupsen/logrus"
	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// Most of this has been taken from the kubernetes-incubator snapshot
// provisioner with some changes. It is a part of the main package there,
// so can't vendor it in here.

type snapshotProvisioner struct {
	// Kubernetes Client.
	client kubernetes.Interface
	// CRD client
	crdclient *rest.RESTClient
	// Identity of this snapshotProvisioner, generated. Used to
	// identify "this"
	// provisioner's PVs.
	identity      string
	volumePlugins map[string]volume.Plugin
}

func newSnapshotProvisioner(
	client kubernetes.Interface,
	crdclient *rest.RESTClient,
	volumePlugins map[string]volume.Plugin,
	id string,
) controller.Provisioner {
	return &snapshotProvisioner{
		client:        client,
		crdclient:     crdclient,
		volumePlugins: volumePlugins,
		identity:      id,
	}
}

var _ controller.Provisioner = &snapshotProvisioner{}

func (p *snapshotProvisioner) getPVFromVolumeSnapshotDataSpec(snapshotDataSpec *crdv1.VolumeSnapshotDataSpec) (*v1.PersistentVolume, error) {
	if snapshotDataSpec.PersistentVolumeRef == nil {
		return nil, fmt.Errorf("VolumeSnapshotDataSpec is not bound to any PV")
	}
	pvName := snapshotDataSpec.PersistentVolumeRef.Name
	if pvName == "" {
		return nil, fmt.Errorf("The PV name is not specified in snapshotdata %#v", *snapshotDataSpec)
	}
	pv, err := p.client.CoreV1().PersistentVolumes().Get(pvName, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("Failed to retrieve PV %s from the API server: %q", pvName, err)
	}
	return pv, nil
}

func (p *snapshotProvisioner) snapshotRestore(
	snapshotName string,
	snapshotData crdv1.VolumeSnapshotData,
	options controller.VolumeOptions,
) (*v1.PersistentVolumeSource, map[string]string, error) {
	// validate the PV supports snapshot and restore
	spec := &snapshotData.Spec
	pv, err := p.getPVFromVolumeSnapshotDataSpec(spec)
	if err != nil {
		return nil, nil, err
	}
	volumeType := crdv1.GetSupportedVolumeFromPVSpec(&pv.Spec)
	if len(volumeType) == 0 {
		return nil, nil, fmt.Errorf("unsupported volume type found in PV %#v", *spec)
	}
	plugin, ok := p.volumePlugins[volumeType]
	if !ok {
		return nil, nil, fmt.Errorf("%s is not supported volume for %#v", volumeType, *spec)
	}

	// restore snapshot
	pvSrc, labels, err := plugin.SnapshotRestore(&snapshotData, options.PVC, options.PVName, options.Parameters)
	if err != nil {
		log.Warnf("failed to snapshot %#v, err: %v", spec, err)
	} else {
		log.Infof("snapshot %#v to snap %#v", spec, pvSrc)
	}

	return pvSrc, labels, err
}

// Provision creates a storage asset and returns a PV object representing it.
func (p *snapshotProvisioner) Provision(options controller.VolumeOptions) (*v1.PersistentVolume, error) {
	if options.PVC.Spec.Selector != nil {
		return nil, fmt.Errorf("claim Selector is not supported")
	}
	snapshotName, ok := options.PVC.Annotations[crdclient.SnapshotPVCAnnotation]
	if !ok {
		return nil, fmt.Errorf("snapshot annotation not found on PV")
	}

	var snapshot crdv1.VolumeSnapshot
	err := p.crdclient.Get().
		Resource(crdv1.VolumeSnapshotResourcePlural).
		Namespace(options.PVC.Namespace).
		Name(snapshotName).
		Do().Into(&snapshot)

	if err != nil {
		return nil, fmt.Errorf("failed to retrieve VolumeSnapshot %s: %v", snapshotName, err)
	}
	// FIXME: should also check if any VolumeSnapshotData points
	// to this VolumeSnapshot
	if len(snapshot.Spec.SnapshotDataName) == 0 {
		return nil, fmt.Errorf("VolumeSnapshot %s is not bound to any VolumeSnapshotData", snapshotName)
	}
	var snapshotData crdv1.VolumeSnapshotData
	err = p.crdclient.Get().
		Resource(crdv1.VolumeSnapshotDataResourcePlural).
		Name(snapshot.Spec.SnapshotDataName).
		Do().Into(&snapshotData)

	if err != nil {
		return nil, fmt.Errorf("failed to retrieve VolumeSnapshotData %s: %v", snapshot.Spec.SnapshotDataName, err)
	}
	log.Infof("restore from VolumeSnapshotData %s", snapshot.Spec.SnapshotDataName)

	pvSrc, labels, err := p.snapshotRestore(snapshot.Spec.SnapshotDataName, snapshotData, options)
	if err != nil || pvSrc == nil {
		return nil, fmt.Errorf("failed to create a PV from snapshot %s: %v", snapshotName, err)
	}
	pv := &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: options.PVName,
			Annotations: map[string]string{
				provisionerIDAnn: p.identity,
			},
		},
		Spec: v1.PersistentVolumeSpec{
			PersistentVolumeReclaimPolicy: options.PersistentVolumeReclaimPolicy,
			AccessModes:                   options.PVC.Spec.AccessModes,
			Capacity: v1.ResourceList{
				v1.ResourceName(v1.ResourceStorage): options.PVC.Spec.Resources.Requests[v1.ResourceName(v1.ResourceStorage)],
			},
			PersistentVolumeSource: *pvSrc,
		},
	}

	if len(labels) != 0 {
		if pv.Labels == nil {
			pv.Labels = make(map[string]string)
		}
		for k, v := range labels {
			pv.Labels[k] = v
		}
	}

	log.Infof("successfully created Snapshot share %#v", pv)

	return pv, nil
}

// Delete removes the storage asset that was created by Provision represented
// by the given PV.
func (p *snapshotProvisioner) Delete(volume *v1.PersistentVolume) error {
	ann, ok := volume.Annotations[provisionerIDAnn]
	if !ok {
		return errors.New("identity annotation not found on PV")
	}
	if ann != p.identity {
		return &controller.IgnoredError{Reason: "identity annotation on PV does not match ours"}
	}

	volumeType := crdv1.GetSupportedVolumeFromPVSpec(&volume.Spec)
	if len(volumeType) == 0 {
		return fmt.Errorf("unsupported volume type found in PV %#v", *volume)
	}
	plugin, ok := p.volumePlugins[volumeType]
	if !ok {
		return fmt.Errorf("%s is not supported volume for %#v", volumeType, *volume)
	}

	// delete PV
	return plugin.VolumeDelete(volume)
}
