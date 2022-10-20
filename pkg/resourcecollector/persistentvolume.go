package resourcecollector

import (
	"fmt"
	"github.com/libopenstorage/stork/pkg/utils"
	"github.com/sirupsen/logrus"

	"github.com/libopenstorage/stork/drivers/volume"
	stork_api "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	k8shelper "k8s.io/component-helpers/storage/volume"
)

func (r *ResourceCollector) pvToBeCollected(
	includeObjects map[stork_api.ObjectInfo]bool,
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

		if len(includeObjects) > 0 {
			info := stork_api.ObjectInfo{
				GroupVersionKind: metav1.GroupVersionKind{
					Group:   "core",
					Version: "v1",
					Kind:    "PersistentVolumeClaim",
				},
				Name:      pvc.Name,
				Namespace: pvc.Namespace,
			}
			if val, present := includeObjects[info]; !present || !val {
				return false, nil
			}
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

		// Also check the labels on the PVC since the PV doesn't inherit the labels
		if len(pvc.Labels) == 0 && len(labelSelectors) > 0 {
			return false, nil
		}

		// labels.AreLabelsInWhiteList removed in k8s 1.20. It has been replaced with isSubset:
		// https://github.com/kubernetes/kubernetes/commit/c9051308befb12f66c3e222de6df9d29f3f2f77d
		if !isSubset(labels.Set(labelSelectors),
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
	var pv v1.PersistentVolume
	var currentSc string
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(object.UnstructuredContent(), &pv); err != nil {
		return err
	}
	// Some time pv spec does not contains the storage class.
	// In that case, we will get it from pvc spec.
	if len(pv.Spec.StorageClassName) == 0 {
		pvc, err := r.coreOps.GetPersistentVolumeClaim(pv.Spec.ClaimRef.Name, pv.Spec.ClaimRef.Namespace)
		if err != nil {
			return err
		}
		currentSc, err = utils.GetStorageClassNameForPVC(pvc)
		if err != nil {
			// Not returning error as there might be some cases, where PVC might not have storage class.
			// A case where a PV can be manually bind mounted to a volume ( backend storage volume)
			logrus.Debugf("preparePVResourceForCollection: failed to fetch storage class from PVC %v: %v", pv.Spec.ClaimRef.Name, err)
		}
	} else {
		currentSc = pv.Spec.StorageClassName
	}
	err := unstructured.SetNestedField(object.UnstructuredContent(), nil, "spec", "claimRef")
	if err != nil {
		return err
	}
	if len(currentSc) > 0 {
		annotations, found, err := unstructured.NestedStringMap(object.UnstructuredContent(), "metadata", "annotations")
		if err != nil {
			return err
		}
		if !found {
			annotations = make(map[string]string)
		}
		annotations[CurrentStorageClassName] = currentSc
		if err := unstructured.SetNestedStringMap(object.UnstructuredContent(), annotations, "metadata", "annotations"); err != nil {
			return err
		}
		object.SetUnstructuredContent(object.UnstructuredContent())
	}
	return unstructured.SetNestedField(object.UnstructuredContent(), "", "spec", "storageClassName")
}

// Updates the PV by pointing to the new volume. Also updated the name of the PV
// itself. The restored PVC will point to this new PV name.
func (r *ResourceCollector) preparePVResourceForApply(
	object runtime.Unstructured,
	pvNameMappings map[string]string,
	vInfo []*stork_api.ApplicationRestoreVolumeInfo,
	storageClassMappings map[string]string,
	namespaceMappings map[string]string,
) (bool, error) {
	var updatedName string
	var present bool

	var pv v1.PersistentVolume
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(object.UnstructuredContent(), &pv); err != nil {
		return false, fmt.Errorf("error converting to persistent volume: %v", err)
	}

	// Skip the PV if it isn't bound to a PVC that needs to be restored
	if len(pvNameMappings) == 0 {
		return true, nil
	}
	if updatedName, present = pvNameMappings[pv.Name]; !present {
		return true, nil
	}
	// get the storage class name from the CurrentStorageClassName annotation
	var oldSc string
	var exists bool
	var newSc string
	if pv.Annotations != nil {
		if val, ok := pv.Annotations[CurrentStorageClassName]; ok {
			oldSc = val
			// delete CurrentStorageClassName annotation before applying
			delete(pv.Annotations, CurrentStorageClassName)
			if newSc, exists = storageClassMappings[oldSc]; exists && len(newSc) > 0 {
				// If the oldSc is present the storageclass map, get the new sc and update it in the pv spec
				// Get the provisioner name from the new sc and update it
				storageClass, err := r.storageOps.GetStorageClass(newSc)
				if err != nil {
					return false, fmt.Errorf("failed in getting the storage class [%v]: %v", newSc, err)
				}
				pv.Annotations[k8shelper.AnnDynamicallyProvisioned] = storageClass.Provisioner
				pv.Spec.StorageClassName = newSc
			} else {
				// if the storageclass map does not have the oldSc name, update the PV spec with the oldSC itself before applying.
				pv.Spec.StorageClassName = oldSc
			}
		}
	}

	pv.Name = updatedName
	var driverName string
	var volumeInfo *stork_api.ApplicationRestoreVolumeInfo
	for _, vol := range vInfo {
		if vol.RestoreVolume == pv.Name {
			volumeInfo = vol
			driverName = vol.DriverName
			break
		}
	}

	// in case of non-restore call make sure resourcecollector
	// checks proper driver by looking at pv name
	if driverName == "" {
		var err error
		driverName, err = volume.GetPVDriver(&pv)
		if err != nil {
			return false, err
		}
	}

	driver, err := volume.Get(driverName)
	if err != nil {
		return false, err
	}
	_, err = driver.UpdateMigratedPersistentVolumeSpec(&pv, volumeInfo, namespaceMappings)
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

func isSubset(subSet, superSet labels.Set) bool {
	if len(superSet) == 0 {
		return true
	}

	for k, v := range subSet {
		value, ok := superSet[k]
		if !ok {
			return false
		}
		if value != v {
			return false
		}
	}
	return true
}
