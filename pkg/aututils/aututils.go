package aututils

import (
	"fmt"

	apapi "github.com/libopenstorage/autopilot-api/pkg/apis/autopilot/v1alpha1"
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	// RuleScaleTypeAddDisk is name for add disk scale type
	RuleScaleTypeAddDisk = "add-disk"
	// RuleScaleTypeResizeDisk is name for resize disk scale type
	RuleScaleTypeResizeDisk = "resize-disk"
	// RuleMaxSize is name for rule max size
	RuleMaxSize = "maxsize"
	// PxPoolAvailableCapacityMetric is metric for pool available capacity
	PxPoolAvailableCapacityMetric = "100 * ( px_pool_stats_available_bytes/ px_pool_stats_total_bytes)"
	// PxPoolTotalCapacityMetric is metric for pool total capacity
	PxPoolTotalCapacityMetric = "px_pool_stats_total_bytes/(1024*1024*1024)"
	// PxVolumeUsagePercentMetric is metric for volume usage percentage
	PxVolumeUsagePercentMetric = "100 * (px_volume_usage_bytes / px_volume_capacity_bytes)"
	// PxVolumeTotalCapacityMetric is metric for total volume capacity
	PxVolumeTotalCapacityMetric = "px_volume_capacity_bytes / 1000000000"
	// RuleActionsScalePercentage is name for scale percentage rule action
	RuleActionsScalePercentage = "scalepercentage"
	// RuleScaleType is name for scale type
	RuleScaleType = "scaletype"
	// VolumeSpecAction is name for volume spec action
	VolumeSpecAction = "openstorage.io.action.volume/resize"
	// StorageSpecAction is name for storage spec action
	StorageSpecAction = "openstorage.io.action.storagepool/expand"
)

// PoolRuleByTotalSize returns an autopilot pool expand rule that uses total pool size
func PoolRuleByTotalSize(total, scalePercentage int, expandType string, labelSelector map[string]string) apapi.AutopilotRule {
	return apapi.AutopilotRule{
		ObjectMeta: meta_v1.ObjectMeta{
			Name: fmt.Sprintf("pool-%s-total-%d", expandType, total),
		},
		Spec: apapi.AutopilotRuleSpec{
			Selector: apapi.RuleObjectSelector{
				LabelSelector: meta_v1.LabelSelector{
					MatchLabels: labelSelector,
				},
			},
			Conditions: apapi.RuleConditions{
				Expressions: []*apapi.LabelSelectorRequirement{
					{
						Key:      PxPoolTotalCapacityMetric,
						Operator: apapi.LabelSelectorOpLt,
						Values:   []string{fmt.Sprintf("%d", total)},
					},
				},
			},
			Actions: []*apapi.RuleAction{
				{
					Name: StorageSpecAction,
					Params: map[string]string{
						RuleActionsScalePercentage: fmt.Sprintf("%d", scalePercentage),
						RuleScaleType:              expandType,
					},
				},
			},
		},
	}
}

// PoolRuleByAvailableCapacity is a helper method to get the pool expand autopilot spec object using configuration params
func PoolRuleByAvailableCapacity(usage, scalePercentage int, expandType string) apapi.AutopilotRule {
	return apapi.AutopilotRule{
		ObjectMeta: meta_v1.ObjectMeta{
			Name: fmt.Sprintf("pool-%s-available-%d", expandType, usage),
		},
		Spec: apapi.AutopilotRuleSpec{
			Conditions: apapi.RuleConditions{
				Expressions: []*apapi.LabelSelectorRequirement{
					{
						Key:      PxPoolAvailableCapacityMetric,
						Operator: apapi.LabelSelectorOpLt,
						Values:   []string{fmt.Sprintf("%d", usage)},
					},
				},
			},
			Actions: []*apapi.RuleAction{
				{
					Name: StorageSpecAction,
					Params: map[string]string{
						RuleActionsScalePercentage: fmt.Sprintf("%d", scalePercentage),
						RuleScaleType:              expandType,
					},
				},
			},
		},
	}
}
