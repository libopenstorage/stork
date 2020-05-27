package resourcecollector

import (
	"fmt"

	"github.com/libopenstorage/stork/drivers/volume"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
)

func (r *ResourceCollector) pvToBeCollected(
	labelSelectors map[string]string,
	object runtime.Unstructured,
	namespace string,
	allDrivers bool,
) (bool, error) {
	var pv v1.PersistentVolume
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(object.UnstructuredContent(), &pv); err != nil {
		return false, fmt.Errorf("error converting to persistent volume: %v", err)
	}

	// Only collect Bound PVs
	if pv.Status.Phase != v1.VolumeBound {
		return false, nil
	}

	if pv.Spec.ClaimRef != nil {
		pvcName := pv.Spec.ClaimRef.Name
		// Collect only PVs which have a reference to a PVC in the namespace requested
		if pvcName == "" {
			return false, nil
		}
		pvcNamespace := pv.Spec.ClaimRef.Namespace
		if pvcNamespace != namespace {
			return false, nil
		}

		pvc, err := r.coreOps.GetPersistentVolumeClaim(pvcName, pvcNamespace)
		if err != nil {
			return false, err
		}

		// Don't collect PVCs not owned by the driver if collecting for a specific
		// driver
		if !allDrivers && !r.Driver.OwnsPVC(pvc) {
			return false, nil
		}
		// Else collect PVCs for all supported drivers
		_, err = volume.GetPVCDriver(pvc)
		if err != nil {
			return false, nil
		}

		// Also check the labels on the PVC since the PV doesn't inherit the labels
		if len(pvc.Labels) == 0 && len(labelSelectors) > 0 {
			return false, nil
		}

		if !labels.AreLabelsInWhiteList(labels.Set(labelSelectors),
			labels.Set(pvc.Labels)) {
			return false, nil
		}
		return true, nil
	}
	return false, nil
}

func (r *ResourceCollector) preparePVResourceForCollection(
	object runtime.Unstructured,
) error {
	err := unstructured.SetNestedField(object.UnstructuredContent(), nil, "spec", "claimRef")
	if err != nil {
		return err
	}
	return unstructured.SetNestedField(object.UnstructuredContent(), "", "spec", "storageClassName")
}

// Updates the PV by pointing to the new volume. Also updated the name of the PV
// itself. The restored PVC will point to this new PV name.
func (r *ResourceCollector) preparePVResourceForApply(
	object runtime.Unstructured,
	pvNameMappings map[string]string,
) (bool, error) {
	var updatedName string
	var present bool

	var pv v1.PersistentVolume
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(object.UnstructuredContent(), &pv); err != nil {
		return false, fmt.Errorf("error converting to persistent volume: %v", err)
	}

	// Skip the PV if it isn't bound to a PVC that needs to be restored
	if updatedName, present = pvNameMappings[pv.Name]; !present {
		return true, nil
	}
	pv.Name = updatedName
	driverName, err := volume.GetPVDriver(&pv)
	if err != nil {
		return false, err
	}
	driver, err := volume.Get(driverName)
	if err != nil {
		return false, err
	}
	_, err = driver.UpdateMigratedPersistentVolumeSpec(&pv)
	if err != nil {
		return false, err
	}
	o, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&pv)
	if err != nil {
		return false, err
	}
	object.SetUnstructuredContent(o)

	return false, err
}
