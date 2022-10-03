package resourcecollector

import "k8s.io/apimachinery/pkg/runtime"

func (r *ResourceCollector) virtualMachineInstanceToBeCollected(
	object runtime.Unstructured,
) (bool, error) {
	return false, nil
}

func (r *ResourceCollector) virtualMachineInstanceMigrationToBeCollected(
	object runtime.Unstructured,
) (bool, error) {
	return false, nil
}
