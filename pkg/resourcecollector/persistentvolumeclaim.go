package resourcecollector

import (
	"fmt"

	"github.com/libopenstorage/stork/drivers/volume"
	stork_api "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/portworx/sched-ops/k8s/core"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	pvutil "k8s.io/kubernetes/pkg/controller/volume/persistentvolume/util"
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
				if nodeZone == vol.Zones[0] {
					pvc.Annotations[pvutil.AnnSelectedNode] = node.Name
					break
				}
			}
		}

	}

	if len(storageClassMappings) > 0 && pvc.Spec.StorageClassName != nil {
		if newSc, exists := storageClassMappings[*pvc.Spec.StorageClassName]; exists && len(newSc) > 0 {
			pvc.Spec.StorageClassName = &newSc
		}
	}
	o, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&pvc)
	if err != nil {
		return false, err
	}
	object.SetUnstructuredContent(o)
	return false, nil
}
