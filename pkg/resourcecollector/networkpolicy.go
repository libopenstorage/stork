package resourcecollector

import (
	"fmt"
	"github.com/libopenstorage/stork/pkg/utils"
	"strings"

	v1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
)

func (r *ResourceCollector) prepareRancherNetworkPolicy(
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

	utils.ParseRancherProjectMapping(networkPolicy.Labels, opts.RancherProjectMappings)

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

func (r *ResourceCollector) mergeAndUpdateNetworkPolicy(
	object runtime.Unstructured,
	opts *Options,
) error {

	if len(opts.RancherProjectMappings) == 0 {
		return nil
	}

	var newNetworkPolicy v1.NetworkPolicy
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(object.UnstructuredContent(), &newNetworkPolicy); err != nil {
		return fmt.Errorf("error converting to networkpolicy: %v", err)
	}

	curNetworkPolicy, err := r.coreOps.GetNetworkPolicy(newNetworkPolicy.Name, newNetworkPolicy.Namespace)
	if err != nil {
		if apierrors.IsNotFound(err) {
			_, err = r.coreOps.CreateNetworkPolicy(&newNetworkPolicy)
		}
		return err
	}

	content, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&curNetworkPolicy)
	if err != nil {
		return fmt.Errorf("error converting from networkPolicy to runtime.Unstructured: %v", err)
	}
	obj := &unstructured.Unstructured{}
	obj.SetUnstructuredContent(content)

	if err = r.prepareRancherNetworkPolicy(obj, *opts); err != nil {
		return err
	}

	var parsedNetworkPolicy v1.NetworkPolicy
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(obj.UnstructuredContent(), &parsedNetworkPolicy); err != nil {
		return fmt.Errorf("error converting to networkpolicy: %v", err)
	}

	_, err = r.coreOps.UpdateNetworkPolicy(&parsedNetworkPolicy)
	return err
}
