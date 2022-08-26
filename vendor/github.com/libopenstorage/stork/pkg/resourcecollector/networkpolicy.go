package resourcecollector

import (
	"fmt"
	"github.com/libopenstorage/stork/pkg/utils"
	"strings"

	v1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

func (r *ResourceCollector) prepareNetworkPolicyForCollection(
	object runtime.Unstructured,
	opts Options,
) error {
	var (
		networkPolicy v1.NetworkPolicy
		err           error
	)
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(object.UnstructuredContent(), &networkPolicy); err != nil {
		return fmt.Errorf("error converting to networkpolicy: %v", err)
	}

	// Handle namespace selectors for Rancher Network Policies
	if len(opts.RancherProjectMappings) > 0 {

		ingressRule := networkPolicy.Spec.Ingress
		for ingressIndex, ingress := range ingressRule {
			for fromIndex, fromPolicyPeer := range ingress.From {
				if fromPolicyPeer.NamespaceSelector != nil {
					for key, val := range fromPolicyPeer.NamespaceSelector.MatchLabels {
						if strings.Contains(key, utils.CattleProjectPrefix) {
							if newProjectID, ok := opts.RancherProjectMappings[val]; ok {
								networkPolicy.Spec.Ingress[ingressIndex].From[fromIndex].NamespaceSelector.MatchLabels[key] = newProjectID
							}
						}
					}
				}
			}
		}
		egressRule := networkPolicy.Spec.Egress
		for egressIndex, egress := range egressRule {
			for toIndex, toPolicyPeer := range egress.To {
				if toPolicyPeer.NamespaceSelector != nil {
					for key, val := range toPolicyPeer.NamespaceSelector.MatchLabels {
						if strings.Contains(key, utils.CattleProjectPrefix) {
							if newProjectID, ok := opts.RancherProjectMappings[val]; ok {
								networkPolicy.Spec.Egress[egressIndex].To[toIndex].NamespaceSelector.MatchLabels[key] = newProjectID
							}
						}
					}
				}
			}
		}
	}

	o, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&networkPolicy)
	if err != nil {
		return err
	}
	object.SetUnstructuredContent(o)
	return nil
}

func (r *ResourceCollector) networkPolicyToBeCollected(
	object runtime.Unstructured,
	opts Options,
) (bool, error) {
	var networkPolicy v1.NetworkPolicy
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(object.UnstructuredContent(), &networkPolicy); err != nil {
		return false, fmt.Errorf("error converting to networkpolicy: %v", err)
	}

	if opts.IncludeAllNetworkPolicies {
		// collect all network policies
		return true, nil
	}

	// Collect NetworkPolicy if ony CIDR is not set.
	// If we backup NetworkPolicy with CIDR present when a user
	// restores this, it's not guaranteed that same network topology is
	// available on restore cluster.
	ingressRule := networkPolicy.Spec.Ingress
	for _, ingress := range ingressRule {
		for _, fromPolicyPeer := range ingress.From {
			ipBlock := fromPolicyPeer.IPBlock
			if ipBlock != nil && len(ipBlock.CIDR) != 0 {
				return false, nil
			}
		}
	}
	egreeRule := networkPolicy.Spec.Egress
	for _, egress := range egreeRule {
		for _, toPolicyPeer := range egress.To {
			ipBlock := toPolicyPeer.IPBlock
			if ipBlock != nil && len(ipBlock.CIDR) != 0 {
				return false, nil
			}
		}
	}

	return true, nil
}
