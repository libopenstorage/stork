package resourcecollector

import (
	"fmt"

	"github.com/libopenstorage/stork/drivers/volume"
	"github.com/sirupsen/logrus"
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

	// Find the matching PV for this PVC.
	pv, err := getPVForPVC(&pvc, allObjects)
	if err != nil {
		// if not a generic CSI PVC, this call may fail. Do not return error to allow for non-CSI PVCs.
		logrus.Debugf("did not find PV for PVC %s in backup resources for generic CSI restore: %v", pvc.Name, err)
	}

	// We have found a PV for this PVC. Check if it is a generic CSI PV
	// that we do not already have native volume driver support for.
	if pv != nil {
		isGenericCSIPVC, err := isGenericCSIPersistentVolume(pv)
		if err != nil {
			logrus.Debugf("failed to check if PVC %s is for a CSI driver: %v", pvc.Name, err)
		}

		// If we have a generic CSI PVC, we should return true to indicate
		// that the resourcecollector should not restore the PVC.
		// Instead, PVC creation is handled by the CSI volume driver.
		if isGenericCSIPVC {
			logrus.Debugf("skipping CSI PVC in pre-restore: %s", metadata.GetName())
			return true, nil
		}
	}

	if updatedName, present = pvNameMappings[pvc.Spec.VolumeName]; !present {
		return false, fmt.Errorf("PV name mapping not found for %v", metadata.GetName())
	}
	pvc.Spec.VolumeName = updatedName
	o, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&pvc)
	if err != nil {
		return false, err
	}
	object.SetUnstructuredContent(o)
	return false, nil
}
