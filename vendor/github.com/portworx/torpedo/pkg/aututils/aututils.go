package aututils

import (
	"fmt"
	"github.com/portworx/sched-ops/k8s/autopilot"
	"github.com/portworx/sched-ops/k8s/core"
	"github.com/portworx/sched-ops/task"
	"strings"
	"time"

	apapi "github.com/libopenstorage/autopilot-api/pkg/apis/autopilot/v1alpha1"
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	eventCheckInterval                = 2 * time.Second
	eventCheckTimeout                 = 30 * time.Minute
	actionApprovalObjectCheckInterval = 1 * time.Second
	actionApprovalObjectTimeout       = 30 * time.Second

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
	// RuleActionsScaleSize is name for scale size rule action
	RuleActionsScaleSize = "scalesize"
	// RuleScaleType is name for scale type
	RuleScaleType = "scaletype"
	// RulePoolProvDeviationPercKeyAlias is the key alias for pool provision deviation percentage
	RulePoolProvDeviationPercKeyAlias = "PoolProvDeviationPerc"
	// RulePoolUsageDeviationPercKeyAlias  is the key alias for pool usage deviation percentage
	RulePoolUsageDeviationPercKeyAlias = "PoolUsageDeviationPerc"
	// VolumeSpecAction is name for volume spec action
	VolumeSpecAction = "openstorage.io.action.volume/resize"
	// StorageSpecAction is name for storage spec action
	StorageSpecAction = "openstorage.io.action.storagepool/expand"
	// RebalanceSpecAction is name for rebalance spec action
	RebalanceSpecAction = "openstorage.io.action.storagepool/rebalance"
)

var (
	// AnyToTriggeredEvent is an event which contains "=> Triggered" message
	AnyToTriggeredEvent = fmt.Sprintf(" => %s", apapi.RuleStateTriggered)
	// NormalToTriggeredEvent is an event which contains "Normal => Triggered" message
	NormalToTriggeredEvent = fmt.Sprintf("%s => %s", apapi.RuleStateNormal, apapi.RuleStateTriggered)
	// TriggeredToActionAwaitingApprovalEvent is an event which contains "Triggered => ActionAwaitingApproval" message
	TriggeredToActionAwaitingApprovalEvent = fmt.Sprintf("%s => %s", apapi.RuleStateTriggered, apapi.RuleStateActionAwaitingApproval)
	// ActionDeclinedToTriggeredEvent is an event which contains "ActionDeclined => Triggered" message
	ActionDeclinedToTriggeredEvent = fmt.Sprintf("%s => %s", apapi.RuleStateActionsDeclined, apapi.RuleStateTriggered)
	// ActionAwaitingApprovalToActiveActionsPending is an event which contains "ActionAwaitingApproval => ActiveActionsPending" message
	ActionAwaitingApprovalToActiveActionsPending = fmt.Sprintf("%s => %s", apapi.RuleStateActionAwaitingApproval, apapi.RuleStateActiveActionsPending)
	// ActiveActionsPendingToActiveActionsInProgress is an event which contains "ActiveActionsPending => ActiveActionsInProgress" message
	ActiveActionsPendingToActiveActionsInProgress = fmt.Sprintf("%s => %s", apapi.RuleStateActiveActionsPending, apapi.RuleStateActiveActionsInProgress)
	// ActiveActionsInProgressToActiveActionsPending is an event which contains "ActiveActionsInProgress => ActiveActionsPending" message
	ActiveActionsInProgressToActiveActionsPending = fmt.Sprintf("%s => %s", apapi.RuleStateActiveActionsInProgress, apapi.RuleStateActiveActionsPending)
	// ActiveActionsInProgressToActiveActionsTaken is an event which contains "ActiveActionsInProgress => ActiveActionsTaken" message
	ActiveActionsInProgressToActiveActionsTaken = fmt.Sprintf("%s => %s", apapi.RuleStateActiveActionsInProgress, apapi.RuleStateActiveActionsTaken)
	// ActiveActionTakenToNormalEvent is an event which contains "ActiveActionTaken => Normal" message
	ActiveActionTakenToNormalEvent = fmt.Sprintf("%s => %s", apapi.RuleStateActiveActionsTaken, apapi.RuleStateNormal)
	// FailedToExecuteActionEvent is an event for failed action
	FailedToExecuteActionEvent = "failed to execute Action for rule"
)

// PoolRuleByTotalSize returns an autopilot pool expand rule that uses total pool size
func PoolRuleByTotalSize(total, scalePercentage uint64, expandType string, labelSelector map[string]string) apapi.AutopilotRule {
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

// PoolRuleFixedScaleSizeByTotalSize returns an autopilot pool expand rule that
// uses total pool size and fixed scale size action
func PoolRuleFixedScaleSizeByTotalSize(total uint64, scaleSize, expandType string, labelSelector map[string]string) apapi.AutopilotRule {
	return apapi.AutopilotRule{
		ObjectMeta: meta_v1.ObjectMeta{
			Name: fmt.Sprintf("pool-%s-fixedsize-%s-total-%d", expandType, strings.ToLower(scaleSize), total),
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
						RuleActionsScaleSize: scaleSize,
						RuleScaleType:        expandType,
					},
				},
			},
		},
	}
}

// PoolRuleByAvailableCapacity returns an autopilot pool expand rule that uses usage of pool size
func PoolRuleByAvailableCapacity(usage, scalePercentage uint64, expandType string) apapi.AutopilotRule {
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

// PoolRuleFixedScaleSizeByAvailableCapacity returns an autopilot pool expand rule that
// uses usage of pool size and fixed scale size action
func PoolRuleFixedScaleSizeByAvailableCapacity(usage int, scaleSize, expandType string) apapi.AutopilotRule {
	return apapi.AutopilotRule{
		ObjectMeta: meta_v1.ObjectMeta{
			Name: fmt.Sprintf("pool-%s-fixedsize-%s-available-%d", expandType, strings.ToLower(scaleSize), usage),
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
						RuleActionsScaleSize: scaleSize,
						RuleScaleType:        expandType,
					},
				},
			},
		},
	}
}

// PoolRuleRebalanceByProvisionedMean returns an autopilot pool rebalance rule that
// uses provision deviation percentage alias key
func PoolRuleRebalanceByProvisionedMean(values []string, approvalRequired bool) apapi.AutopilotRule {
	apRuleObject := apapi.AutopilotRule{
		ObjectMeta: meta_v1.ObjectMeta{
			Name: fmt.Sprintf("pvc-rebalance-provisioned-mean-%s", strings.Join(values, "-")),
		},
		Spec: apapi.AutopilotRuleSpec{
			Conditions: apapi.RuleConditions{
				Expressions: []*apapi.LabelSelectorRequirement{
					{
						KeyAlias: RulePoolProvDeviationPercKeyAlias,
						Operator: apapi.LabelSelectorOpNotInRange,
						Values:   values,
					},
				},
			},
			Actions: []*apapi.RuleAction{
				{
					Name: RebalanceSpecAction,
				},
			},
		},
	}
	if approvalRequired {
		apRuleObject.Spec.Enforcement = apapi.ApprovalRequired
	}
	return apRuleObject
}

// PoolRuleRebalanceByUsageMean returns an autopilot pool rebalance rule that
// uses usage deviation percentage alias key
func PoolRuleRebalanceByUsageMean(values []string, approvalRequired bool) apapi.AutopilotRule {
	apRuleObject := apapi.AutopilotRule{
		ObjectMeta: meta_v1.ObjectMeta{
			Name: fmt.Sprintf("pvc-rebalance-usage-mean-%s", strings.Join(values, "-")),
		},
		Spec: apapi.AutopilotRuleSpec{
			Conditions: apapi.RuleConditions{
				Expressions: []*apapi.LabelSelectorRequirement{
					{
						KeyAlias: RulePoolUsageDeviationPercKeyAlias,
						Operator: apapi.LabelSelectorOpNotInRange,
						Values:   values,
					},
				},
			},
			Actions: []*apapi.RuleAction{
				{
					Name: RebalanceSpecAction,
				},
			},
		},
	}
	if approvalRequired {
		apRuleObject.Spec.Enforcement = apapi.ApprovalRequired
	}
	return apRuleObject
}

// PVCRuleByTotalSize resizes volume by its total size
func PVCRuleByTotalSize(capacity int, scalePercentage int, maxSize string) apapi.AutopilotRule {
	apRuleObject := apapi.AutopilotRule{
		ObjectMeta: meta_v1.ObjectMeta{
			Name: fmt.Sprintf("pvc-total-%d-scale-%d", capacity, scalePercentage),
		},
		Spec: apapi.AutopilotRuleSpec{
			Conditions: apapi.RuleConditions{
				Expressions: []*apapi.LabelSelectorRequirement{
					{
						Key:      PxVolumeTotalCapacityMetric,
						Operator: apapi.LabelSelectorOpLt,
						Values:   []string{fmt.Sprintf("%d", capacity)},
					},
				},
			},
			Actions: []*apapi.RuleAction{
				{
					Name: VolumeSpecAction,
					Params: map[string]string{
						RuleActionsScalePercentage: fmt.Sprintf("%d", scalePercentage),
					},
				},
			},
		},
	}
	if maxSize != "" {
		apRuleObject.Name = fmt.Sprintf("%s-maxsize-%s", apRuleObject.Name, strings.ToLower(maxSize))
		for _, action := range apRuleObject.Spec.Actions {
			action.Params[RuleMaxSize] = maxSize
		}
	}
	return apRuleObject
}

// PVCRuleByUsageCapacity returns an autopilot pvc expand rule that uses usage of pvc size
func PVCRuleByUsageCapacity(usagePercentage int, scalePercentage int, maxSize string) apapi.AutopilotRule {
	apRuleObject := apapi.AutopilotRule{
		ObjectMeta: meta_v1.ObjectMeta{
			Name: fmt.Sprintf("pvc-usage-%d-scale-%d", usagePercentage, scalePercentage),
		},
		Spec: apapi.AutopilotRuleSpec{
			Conditions: apapi.RuleConditions{
				Expressions: []*apapi.LabelSelectorRequirement{
					{
						Key:      PxVolumeUsagePercentMetric,
						Operator: apapi.LabelSelectorOpGt,
						Values:   []string{fmt.Sprintf("%d", usagePercentage)},
					},
				},
			},
			Actions: []*apapi.RuleAction{
				{
					Name: VolumeSpecAction,
					Params: map[string]string{
						RuleActionsScalePercentage: fmt.Sprintf("%d", scalePercentage),
					},
				},
			},
		},
	}
	if maxSize != "" {
		apRuleObject.Name = fmt.Sprintf("%s-maxsize-%s", apRuleObject.Name, strings.ToLower(maxSize))
		for _, action := range apRuleObject.Spec.Actions {
			action.Params[RuleMaxSize] = maxSize
		}
	}
	return apRuleObject
}

// WaitForAutopilotEvent waits for event which contains a reason and messages for given autopilot rule
func WaitForAutopilotEvent(apRule apapi.AutopilotRule, reason string, messages []string) error {
	t := func() (interface{}, bool, error) {
		ruleEvents, err := core.Instance().ListEvents("", meta_v1.ListOptions{
			FieldSelector: fmt.Sprintf("involvedObject.kind=AutopilotRule,involvedObject.name=%s", apRule.Name),
		})

		if err != nil {
			return nil, false, err
		}

		for _, ruleEvent := range ruleEvents.Items {
			// skip old events and verify only events which were in last 20 seconds
			if ruleEvent.LastTimestamp.Unix() < meta_v1.Now().Unix()-20 {
				continue
			}
			ruleReasonFound := false
			ruleMessageFound := false

			if reason != "" {
				if strings.Contains(ruleEvent.Reason, reason) {
					ruleReasonFound = true
				}
			} else {
				ruleReasonFound = true
			}
			for _, message := range messages {
				if !strings.Contains(ruleEvent.Message, message) {
					ruleMessageFound = false
					break
				} else {
					ruleMessageFound = true
				}
			}
			if ruleReasonFound && ruleMessageFound {
				return nil, false, nil
			}
		}

		return nil, true, fmt.Errorf("autopilot rule has no event with %s reason and %v message", reason, messages)
	}
	if _, err := task.DoRetryWithTimeout(t, eventCheckTimeout, eventCheckInterval); err != nil {
		return err
	}
	return nil
}

// WaitForActionApprovalsObjects waits until action approvals object shows up
// if name is supplied it will wait for action approval object for the given name
func WaitForActionApprovalsObjects(namespace, name string) error {
	t := func() (interface{}, bool, error) {
		actionApprovalsList, err := autopilot.Instance().ListActionApprovals(namespace)

		if err != nil {
			return nil, false, err
		}

		if len(actionApprovalsList.Items) == 0 {
			return nil, true, fmt.Errorf("action approval object is empty")
		}

		if name != "" {
			for _, actionApproval := range actionApprovalsList.Items {
				if strings.Contains(name, actionApproval.Name) {
					return nil, false, nil
				}
			}
			return nil, true, fmt.Errorf("no action approval objects with %s name found", name)
		}
		return nil, false, nil
	}
	if _, err := task.DoRetryWithTimeout(t, actionApprovalObjectTimeout, actionApprovalObjectCheckInterval); err != nil {
		return err
	}
	return nil
}
