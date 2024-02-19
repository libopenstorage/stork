package resourcecollector

import (
	"fmt"

	"github.com/portworx/sched-ops/k8s/apps"
	v1 "k8s.io/api/scheduling/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

var (
	ignorePCList = []string{
		"openshift-user-critical",
		"system-cluster-critical",
		"system-node-critical",
		"k8s-cluster-critical",
	}
)

// validatePriorityClassToBeCollected checks if the PC is referred by any
// of the deployments in the namespace that is getting backed. Ignores backup of
// PCs that system defaults which are referred by deployments in the NS.
func (r *ResourceCollector) validatePriorityClassToBeCollected(
	object runtime.Unstructured,
	namespace string,
) (bool, error) {
	var pc v1.PriorityClass
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(object.UnstructuredContent(), &pc); err != nil {
		return false, fmt.Errorf("error converting unstructured to priorityClass Object: %v", err)
	}
	dlist, err := apps.Instance().ListDeployments(namespace, metav1.ListOptions{})
	if err != nil {
		return false, err
	}

	for _, str := range ignorePCList {
		// Ignore system default PC
		if str == pc.Name {
			return false, nil
		}
	}
	for _, deployment := range dlist.Items {

		if deployment.Spec.Template.Spec.PriorityClassName == pc.Name {
			return true, nil
		}
	}
	return false, nil
}
