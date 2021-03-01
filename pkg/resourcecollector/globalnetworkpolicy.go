package resourcecollector

import (
	"strings"

	"github.com/projectcalico/libcalico-go/lib/apis/v3"
	"github.com/sirupsen/logrus"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

func (r *ResourceCollector) globalNetworkPolicyToBeCollected(
	object runtime.Unstructured,
	namespace string,
) (bool, error) {
	globalPolicy := v3.GlobalNetworkPolicy{}
	err := runtime.DefaultUnstructuredConverter.FromUnstructured(object.UnstructuredContent(), &globalPolicy)
	if err != nil {
		logrus.Warnf("Unable to convert from unstruct to globalpolicy object, %v", err)
		return false, err
	}
	// TODO: since globalpolicy.Spec.Selector applies to either hostendpoint/workendpoint which are clusterscope resource
	// we will not be able to collect those poilcies

	// Collect policy based on namespace selector
	// TODO: instead of string replace evaluate exprs
	if strings.Contains(globalPolicy.Spec.NamespaceSelector, namespace) {
		return true, nil
	}

	// Collect policy based on service account in given namespace
	// TODO: instead of string replace evaluate exprs
	// check endpoint api
	saList, err := r.coreOps.ListServiceAccount(namespace, metav1.ListOptions{})
	if err != nil {
		return false, err
	}
	for _, sa := range saList.Items {
		if strings.Contains(globalPolicy.Spec.ServiceAccountSelector, sa.Name) {
			return true, nil
		}
	}
	// If policy applies to all resource in cluster select those as well
	if globalPolicy.Spec.NamespaceSelector == "" && globalPolicy.Spec.ServiceAccountSelector == "" && globalPolicy.Spec.Selector == "" {
		logrus.Infof("Collecting clusterwide global network policy, %v", globalPolicy.Name)
		return true, nil
	}
	return false, nil
}
