package resourcecollector

import (
	"context"
	"strings"

	"github.com/projectcalico/libcalico-go/lib/options"
	"github.com/projectcalico/libcalico-go/lib/selector"
	"github.com/sirupsen/logrus"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
)

func (r *ResourceCollector) globalNetworkPolicyToBeCollected(
	object runtime.Unstructured,
	namespace string,
) (bool, error) {
	if r.calicoOps == nil {
		return false, nil
	}

	// Collect policy based on namespace selector
	// TODO: instead of string replace evaluate exprs
	namespaceSelector, found, err := unstructured.NestedString(object.UnstructuredContent(), "spec", "namespaceSelector")
	if err != nil {
		logrus.Warnf("Unable to retrive namespaceSelector from globalpolicy object, %v", err)
	}
	if found {
		// policy is applied to all workerendpoint in given namespace
		if strings.Contains(namespaceSelector, namespace) {
			return true, nil
		}
	}

	// Collect policy based on service account in given namespace
	saSelector, found, err := unstructured.NestedString(object.UnstructuredContent(), "spec", "serviceAccountSelector")
	if err != nil {
		logrus.Warnf("Unable to retrive saSelector from globalpolicy object, %v", err)
	}
	if found {
		saList, err := r.coreOps.ListServiceAccount(namespace, metav1.ListOptions{})
		if err != nil {
			return false, err
		}
		for _, sa := range saList.Items {
			if strings.Contains(saSelector, sa.Name) {
				return true, nil
			}
		}
	}
	policySelector, _, err := unstructured.NestedString(object.UnstructuredContent(), "spec", "selector")
	if err != nil {
		logrus.Warnf("Unable to retrive selector from globalpolicy object, %v", err)
	}
	sel, err := selector.Parse(policySelector)
	if err != nil {
		logrus.Warnf("Unable to parse selector from globalpolicy object, %v", err)
	}
	wkpList, err := r.calicoOps.WorkloadEndpoints().List(context.Background(), options.ListOptions{Namespace: namespace})
	if err != nil {
		logrus.Warnf("Unable to parse selector from globalpolicy object, %v", err)
	}
	for _, worker := range wkpList.Items {
		if sel.Evaluate(worker.Labels) {
			return true, nil
		}
	}
	// If policy applies to all resource in cluster select those as well
	if namespaceSelector == "" && saSelector == "" && policySelector == "" {
		return true, nil
	}
	return false, nil
}
