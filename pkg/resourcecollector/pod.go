package resourcecollector

import (
	"github.com/libopenstorage/stork/pkg/utils"
	v1 "k8s.io/api/core/v1"
	"strings"
)

// PreparePodSpecNamespaceSelector will update the Namespace Selectors
// with the provided project mappings for Rancher
func PreparePodSpecNamespaceSelector(
	podSpec *v1.PodSpec,
	projectMappings map[string]string,
) *v1.PodSpec {
	if podSpec.Affinity == nil || len(projectMappings) == 0 {
		return podSpec
	}

	// Anti Affinity
	// - handle PreferredDuringSchedulingIgnoredDuringExecution
	if podSpec.Affinity.PodAntiAffinity != nil {
		for affinityIndex, affinityTerm := range podSpec.Affinity.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution {
			if affinityTerm.PodAffinityTerm.NamespaceSelector != nil {
				for key, val := range affinityTerm.PodAffinityTerm.NamespaceSelector.MatchLabels {
					if strings.Contains(key, utils.CattleProjectPrefix) {
						if newProjectID, ok := projectMappings[val]; ok {
							affinityTerm.PodAffinityTerm.NamespaceSelector.MatchLabels[key] = newProjectID
							// reset the affinity term at the affinityIndex
							podSpec.Affinity.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution[affinityIndex] = affinityTerm
						}
					}
				}
			}
		}

		// Affinity
		// - handle RequiredDuringSchedulingIgnoredDuringExecution
		for affinityIndex, affinityTerm := range podSpec.Affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution {
			if affinityTerm.NamespaceSelector != nil {
				for key, val := range affinityTerm.NamespaceSelector.MatchLabels {
					if strings.Contains(key, utils.CattleProjectPrefix) {
						if newProjectID, ok := projectMappings[val]; ok {
							affinityTerm.NamespaceSelector.MatchLabels[key] = newProjectID
							// reset the affinity term at the affinityIndex
							podSpec.Affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution[affinityIndex] = affinityTerm
						}
					}
				}
			}
		}
	}

	// Affinity
	// - handle PreferredDuringSchedulingIgnoredDuringExecution
	if podSpec.Affinity.PodAntiAffinity != nil {
		for affinityIndex, affinityTerm := range podSpec.Affinity.PodAffinity.PreferredDuringSchedulingIgnoredDuringExecution {
			if affinityTerm.PodAffinityTerm.NamespaceSelector != nil {
				for key, val := range affinityTerm.PodAffinityTerm.NamespaceSelector.MatchLabels {
					if strings.Contains(key, utils.CattleProjectPrefix) {
						if newProjectID, ok := projectMappings[val]; ok {
							affinityTerm.PodAffinityTerm.NamespaceSelector.MatchLabels[key] = newProjectID
							// reset the affinity term at the affinityIndex
							podSpec.Affinity.PodAffinity.PreferredDuringSchedulingIgnoredDuringExecution[affinityIndex] = affinityTerm
						}
					}
				}
			}
		}

		// Affinity
		// - handle RequiredDuringSchedulingIgnoredDuringExecution
		for affinityIndex, affinityTerm := range podSpec.Affinity.PodAffinity.RequiredDuringSchedulingIgnoredDuringExecution {
			if affinityTerm.NamespaceSelector != nil {
				for key, val := range affinityTerm.NamespaceSelector.MatchLabels {
					if strings.Contains(key, utils.CattleProjectPrefix) {
						if newProjectID, ok := projectMappings[val]; ok {
							affinityTerm.NamespaceSelector.MatchLabels[key] = newProjectID
							// reset the affinity term at the affinityIndex
							podSpec.Affinity.PodAffinity.RequiredDuringSchedulingIgnoredDuringExecution[affinityIndex] = affinityTerm
						}
					}
				}
			}
		}
	}
	return podSpec
}
