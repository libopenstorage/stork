package k8sutils

import (
	"fmt"

	crdv1 "github.com/kubernetes-incubator/external-storage/snapshot/pkg/apis/crd/v1"
	"github.com/portworx/sched-ops/k8s"
	"k8s.io/api/core/v1"
)

// GetPVCsForGroupSnapshot returns all PVCs in given namespace that match the given matchLabels. All PVCs need to be bound.
func GetPVCsForGroupSnapshot(namespace string, matchLabels map[string]string) ([]v1.PersistentVolumeClaim, error) {
	pvcList, err := k8s.Instance().GetPersistentVolumeClaims(namespace, matchLabels)
	if err != nil {
		return nil, err
	}

	if len(pvcList.Items) == 0 {
		return nil, fmt.Errorf("found no PVCs for group snapshot with given label selectors: %v", matchLabels)
	}

	// Check if no PVCs are in pending state
	for _, pvc := range pvcList.Items {
		if pvc.Status.Phase == v1.ClaimPending {
			return nil, fmt.Errorf("PVC: [%s] %s is still in %s phase. Group snapshot will trigger after all PVCs are bound",
				pvc.Namespace, pvc.Name, pvc.Status.Phase)
		}
	}

	return pvcList.Items, nil
}

// GetVolumeNamesFromLabelSelector returns PV names for all PVCs in given namespace that match the given
// labels
func GetVolumeNamesFromLabelSelector(namespace string, labels map[string]string) ([]string, error) {
	pvcs, err := GetPVCsForGroupSnapshot(namespace, labels)
	if err != nil {
		return nil, err
	}

	volNames := make([]string, 0)
	for _, pvc := range pvcs {
		volName, err := k8s.Instance().GetVolumeForPersistentVolumeClaim(&pvc)
		if err != nil {
			return nil, err
		}

		volNames = append(volNames, volName)
	}

	return volNames, nil
}

// GetDriverTypeFromPV returns the name of the provisioner driver managing
// the persistent volume. Supports in-tree and CSI PVs
func GetDriverTypeFromPV(pv *v1.PersistentVolume) (string, error) {
	var volumeType string

	// Check for CSI
	if pv.Spec.CSI != nil {
		volumeType = pv.Spec.CSI.Driver
		if len(volumeType) == 0 {
			return "", fmt.Errorf("CSI Driver not found in PV %#v", *pv)
		}
		return volumeType, nil
	}

	// Fall back to Kubernetes in-tree driver names
	volumeType = crdv1.GetSupportedVolumeFromPVSpec(&pv.Spec)
	if len(volumeType) == 0 {
		return "", fmt.Errorf("unsupported volume type found in PV %#v", *pv)
	}

	return volumeType, nil
}
