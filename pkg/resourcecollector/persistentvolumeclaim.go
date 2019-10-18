package resourcecollector

import (
	"fmt"

	"github.com/libopenstorage/stork/drivers/volume"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
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
	pvc, err := r.k8sOps.GetPersistentVolumeClaim(pvcName, namespace)
	if err != nil {
		return false, err
	}
	// Only collect Bound PVCs
	if pvc.Status.Phase != v1.ClaimBound {
		return false, nil
	}

	// Don't collect PVCs not owned by the driver if collecting for a specific
	// driver. Else collect PVCs for all supported drivers
	if !allDrivers && !r.Driver.OwnsPVC(pvc) {
		return false, nil
	} else {
		_, err := volume.GetPVCDriver(pvc)
		if err != nil {
			return false, nil
		}
	}

	return true, nil
}

// Updates the PVC by pointing to the new PV that it should refer to
// pvNameMappings has the map of the original PV name to the new PV name
func (r *ResourceCollector) preparePVCResourceForApply(
	object runtime.Unstructured,
	pvNameMappings map[string]string,
) error {
	var pvc v1.PersistentVolumeClaim
	var updatedName string
	var present bool

	metadata, err := meta.Accessor(object)
	if err != nil {
		return err
	}

	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(object.UnstructuredContent(), &pvc); err != nil {
		return fmt.Errorf("error converting PVC object: %v: %v", object, err)
	}

	if updatedName, present = pvNameMappings[pvc.Spec.VolumeName]; !present {
		return fmt.Errorf("PV name mapping not found for %v", metadata.GetName())
	}
	pvc.Spec.VolumeName = updatedName
	o, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&pvc)
	if err != nil {
		return err
	}
	object.SetUnstructuredContent(o)
	return nil
}
