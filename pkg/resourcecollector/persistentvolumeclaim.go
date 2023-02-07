package resourcecollector

import (
	"fmt"

	"github.com/libopenstorage/stork/drivers/volume"
	stork_api "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/libopenstorage/stork/pkg/utils"
	"github.com/portworx/sched-ops/k8s/core"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	k8shelper "k8s.io/component-helpers/storage/volume"
)

func (r *ResourceCollector) pvcToBeCollected(
	object runtime.Unstructured,
	namespace string,
	allDrivers bool,
) (bool, error) {
	metadata, err := meta.Accessor(object)
	if err != nil {
		return false, err
	}

	pvcName := metadata.GetName()
	pvc, err := r.coreOps.GetPersistentVolumeClaim(pvcName, namespace)
	if err != nil {
		return false, err
	}

	// Only collect Bound PVCs that aren't being deleted
	if pvc.Status.Phase != v1.ClaimBound || pvc.DeletionTimestamp != nil {
		return false, nil
	}

	// Don't collect PVCs not owned by the driver if collecting for a specific
	// driver
	if !allDrivers && !r.Driver.OwnsPVC(r.coreOps, pvc) {
		return false, nil
	}
	// Else collect PVCs for all supported drivers
	_, err = volume.GetPVCDriver(r.coreOps, pvc)
	if err != nil {
		return false, nil
	}

	return true, nil
}

// Updates the PVC by pointing to the new PV that it should refer to
// pvNameMappings has the map of the original PV name to the new PV name
func (r *ResourceCollector) preparePVCResourceForApply(
	object runtime.Unstructured,
	allObjects []runtime.Unstructured,
	pvNameMappings map[string]string,
	storageClassMappings map[string]string,
	vInfos []*stork_api.ApplicationRestoreVolumeInfo,
) (bool, error) {
	var pvc v1.PersistentVolumeClaim
	var updatedName string
	var present bool

	metadata, err := meta.Accessor(object)
	if err != nil {
		return false, err
	}

	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(object.UnstructuredContent(), &pvc); err != nil {
		return false, fmt.Errorf("error converting PVC object: %v: %v", object, err)
	}

	if len(pvNameMappings) != 0 {
		if updatedName, present = pvNameMappings[pvc.Spec.VolumeName]; !present {
			return false, fmt.Errorf("PV name mapping not found for %v", metadata.GetName())
		}
	}
	pvc.Spec.VolumeName = updatedName
	nodes, err := core.Instance().GetNodes()
	if err != nil {
		return false, fmt.Errorf("failed in getting the nodes: %v", err)
	}
	for _, vol := range vInfos {
		if vol.PersistentVolumeClaim == pvc.Name {
			for _, node := range nodes.Items {
				nodeZone := node.Labels[v1.LabelTopologyZone]
				if len(vol.Zones) != 0 {
					if nodeZone == vol.Zones[0] {
						if pvc.Annotations == nil {
							pvc.Annotations = make(map[string]string)
						}
						pvc.Annotations[k8shelper.AnnSelectedNode] = node.Name
						break
					}
				}
			}
		}

	}
	if len(storageClassMappings) > 0 {
		// In the case of storageClassMappings, we need to reset the
		// storage class annotation and the provisioner annotation
		var newSc string
		var currentSc string
		var exists bool
		var provisioner string
		// Get the existing storage class from the pvc spec
		// It can be in BetaStorageClassAnnotation annotation or in the spec.
		currentSc, err := utils.GetStorageClassNameForPVC(&pvc)
		if err != nil {
			// If the storageclassMapping is present, then we can assume that storage class should be present in the PVC spec.
			// So handling the error and returning it to caller.
			return false, err
		}
		if len(currentSc) != 0 {
			if newSc, exists = storageClassMappings[currentSc]; exists && len(newSc) > 0 {
				if _, ok := pvc.Annotations[v1.BetaStorageClassAnnotation]; ok {
					pvc.Annotations[v1.BetaStorageClassAnnotation] = newSc
				}
				if pvc.Spec.StorageClassName != nil && len(*pvc.Spec.StorageClassName) > 0 {
					*pvc.Spec.StorageClassName = newSc
				}
			}
		}
		if len(newSc) > 0 {
			storageClass, err := r.storageOps.GetStorageClass(newSc)
			if err != nil {
				return false, fmt.Errorf("failed in getting the storage class [%v]: %v", newSc, err)
			}
			provisioner = storageClass.Provisioner
		}
		if _, ok := pvc.Annotations[k8shelper.AnnBetaStorageProvisioner]; ok {
			if len(provisioner) > 0 {
				pvc.Annotations[k8shelper.AnnBetaStorageProvisioner] = provisioner
			}
		}
		if _, ok := pvc.Annotations[k8shelper.AnnStorageProvisioner]; ok {
			if len(provisioner) > 0 {
				pvc.Annotations[k8shelper.AnnStorageProvisioner] = provisioner
			}
		}
	}

	o, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&pvc)
	if err != nil {
		return false, err
	}
	object.SetUnstructuredContent(o)
	return false, nil
}
