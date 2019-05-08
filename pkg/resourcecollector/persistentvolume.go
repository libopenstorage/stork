package resourcecollector

import (
	"fmt"

	"github.com/heptio/ark/pkg/util/collections"
	"github.com/portworx/sched-ops/k8s"
	"k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
)

func (r *ResourceCollector) pvToBeCollected(
	labelSelectors map[string]string,
	object runtime.Unstructured,
	namespace string,
) (bool, error) {
	phase, err := collections.GetString(object.UnstructuredContent(), "status.phase")
	if err != nil {
		return false, err
	}
	// Only collect Bound PVs
	if phase != string(v1.ClaimBound) {
		return false, nil
	}

	// Collect only PVs which have a reference to a PVC in the namespace requested
	pvcName, err := collections.GetString(object.UnstructuredContent(), "spec.claimRef.name")
	if err != nil {
		return false, err
	}
	if pvcName == "" {
		return false, nil
	}

	pvcNamespace, err := collections.GetString(object.UnstructuredContent(), "spec.claimRef.namespace")
	if err != nil {
		return false, err
	}
	if pvcNamespace != namespace {
		return false, nil
	}

	pvc, err := k8s.Instance().GetPersistentVolumeClaim(pvcName, pvcNamespace)
	if err != nil {
		return false, err
	}
	// Collect only if the PVC bound to the PV is owned by the driver
	if !r.Driver.OwnsPVC(pvc) {
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

func (r *ResourceCollector) preparePVResourceForCollection(
	object runtime.Unstructured,
) error {
	spec, err := collections.GetMap(object.UnstructuredContent(), "spec")
	if err != nil {
		return err
	}

	// Delete the claimRef so that the collected resource can be rebound
	delete(spec, "claimRef")

	// Storage class needs to be removed so that it can rebind to an existing PV
	delete(spec, "storageClassName")

	return nil
}

// Updates the PV by pointing to the new volume. Also updated the name of the PV
// itself. The restored PVC will point to this new PV name.
func (r *ResourceCollector) preparePVResourceForApply(
	object runtime.Unstructured,
	pvNameMappings map[string]string,
) error {
	var updatedName string
	var present bool

	metadata, err := meta.Accessor(object)
	if err != nil {
		return err
	}

	if updatedName, present = pvNameMappings[metadata.GetName()]; !present {
		return fmt.Errorf("PV name mapping not found for %v", metadata.GetName())
	}
	metadata.SetName(updatedName)
	_, err = r.Driver.UpdateMigratedPersistentVolumeSpec(object)
	return err
}
