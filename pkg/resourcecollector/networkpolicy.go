package resourcecollector

import (
	"fmt"

	v1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

func (r *ResourceCollector) networkPolicyToBeCollected(
	object runtime.Unstructured,
) (bool, error) {
	var networkPolicy v1.NetworkPolicy
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(object.UnstructuredContent(), &networkPolicy); err != nil {
		return false, fmt.Errorf("error converting to networkpolicy: %v", err)
	}
	// Collect NetworkPolicy if ony CIDR is not set.
	// If we backup NetworkPolicy with CIDR present when a user
	// restores this, it's not guaranteed that same network topology is
	// available on restore cluster.
	ingressRule := networkPolicy.Spec.Ingress
	for _, ingress := range ingressRule {
		for _, fromPolicyPeer := range ingress.From {
			ipBlock := fromPolicyPeer.IPBlock
			if len(ipBlock.CIDR) != 0 {
				return false, nil
			}
		}
	}
	egreeRule := networkPolicy.Spec.Egress
	for _, egress := range egreeRule {
		for _, networkPolicyPeer := range egress.To {
			ipBlock := networkPolicyPeer.IPBlock
			if len(ipBlock.CIDR) != 0 {
				return false, nil
			}
		}
	}

	return true, nil
}
