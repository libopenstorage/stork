package snapshot

import (
	"math"
	"time"

	crdv1 "github.com/kubernetes-incubator/external-storage/snapshot/pkg/apis/crd/v1"
	"github.com/libopenstorage/stork/pkg/rule"
	"github.com/portworx/sched-ops/k8s"
	"k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/wait"
)

const (
	defaultCmdExecutorImage              = "openstorage/cmdexecutor:0.1"
	cmdExecutorImageOverrideKey          = "stork.libopenstorage.org/cmdexecutor-image"
	storkServiceAccount                  = "stork-account"
	podsWithRunningCommandsKeyDeprecated = "stork/pods-with-running-cmds"
	podsWithRunningCommandsKey           = "stork.libopenstorage.org/pods-with-running-cmds"

	// constants
	perPodCommandExecTimeout = 900 // 15 minutes

	execPodCmdRetryInterval = 5 * time.Second
	execPodCmdRetryFactor   = 1
	execPodStepLow          = 12
	execPodStepMed          = 36
	execPodStepsHigh        = math.MaxInt32

	storkRuleAnnotationPrefixDeprecated = "stork.rule"
	storkRuleAnnotationPrefix           = "stork.libopenstorage.org"
	preSnapRuleAnnotationKey            = storkRuleAnnotationPrefix + "/pre-snapshot-rule"
	postSnapRuleAnnotationKey           = storkRuleAnnotationPrefix + "/post-snapshot-rule"
	preSnapRuleAnnotationKeyDeprecated  = storkRuleAnnotationPrefixDeprecated + "/pre-snapshot"
	postSnapRuleAnnotationKeyDeprecated = storkRuleAnnotationPrefixDeprecated + "/post-snapshot"
)

var ruleAnnotationKeyTypes = map[string]rule.Type{
	preSnapRuleAnnotationKey:            rule.PreExecRule,
	postSnapRuleAnnotationKey:           rule.PostExecRule,
	preSnapRuleAnnotationKeyDeprecated:  rule.PreExecRule,
	postSnapRuleAnnotationKeyDeprecated: rule.PostExecRule,
}

// pod is a simple type to encapsulate a pod's uid and namespace
type pod struct {
	uid       string
	namespace string
}

type podErrorResponse struct {
	pod v1.Pod
	err error
}

// commandTask tracks pods where commands for a taskID might still be running
type commandTask struct {
	taskID string
	pods   []pod
}

var execCmdBackoff = wait.Backoff{
	Duration: execPodCmdRetryInterval,
	Factor:   execPodCmdRetryFactor,
	Steps:    execPodStepsHigh,
}

var snapAPICallBackoff = wait.Backoff{
	Duration: 2 * time.Second,
	Factor:   1.5,
	Steps:    20,
}

// validateSnapRules validates the rules if they are present in the given snapshot's annotations
func validateSnapRules(snap *crdv1.VolumeSnapshot) error {
	if snap.Metadata.Annotations != nil {
		ruleAnnotations := []string{
			preSnapRuleAnnotationKey,
			postSnapRuleAnnotationKey,
			preSnapRuleAnnotationKeyDeprecated,
			preSnapRuleAnnotationKeyDeprecated,
		}
		for _, annotation := range ruleAnnotations {
			ruleName, present := snap.Metadata.Annotations[annotation]
			if present && len(ruleName) > 0 {
				r, err := k8s.Instance().GetRule(ruleName, snap.Metadata.Namespace)
				if err != nil {
					return err
				}
				err = rule.ValidateRule(r, ruleAnnotationKeyTypes[annotation])
				if err != nil {
					return err
				}
			}
		}
	}

	return nil
}

// ExecutePreSnapRule executes the pre snapshot rule. pvcs is a list of PVCs that are associated
// with the snapshot. It returns a channel which the caller can trigger to delete the termination of background commands
func ExecutePreSnapRule(snap *crdv1.VolumeSnapshot, pvcs []v1.PersistentVolumeClaim) (chan bool, error) {
	if snap.Metadata.Annotations != nil {
		ruleName, present := snap.Metadata.Annotations[preSnapRuleAnnotationKey]
		if !present {
			ruleName, present = snap.Metadata.Annotations[preSnapRuleAnnotationKeyDeprecated]
			if !present {
				return nil, nil
			}
		}
		r, err := k8s.Instance().GetRule(ruleName, snap.Metadata.Namespace)
		if err != nil {
			return nil, err
		}
		return rule.ExecuteRule(r, rule.PreExecRule, snap, pvcs)
	}
	return nil, nil
}

// ExecutePostSnapRule executes the post snapshot rule for the given snapshot. pvcs is a list of PVCs
// that are associated with the snapshot
func ExecutePostSnapRule(pvcs []v1.PersistentVolumeClaim, snap *crdv1.VolumeSnapshot) error {
	if snap.Metadata.Annotations != nil {
		ruleName, present := snap.Metadata.Annotations[postSnapRuleAnnotationKey]
		if !present {
			ruleName, present = snap.Metadata.Annotations[postSnapRuleAnnotationKeyDeprecated]
			if !present {
				return nil
			}
		}
		r, err := k8s.Instance().GetRule(ruleName, snap.Metadata.Namespace)
		if err != nil {
			return err
		}
		_, err = rule.ExecuteRule(r, rule.PostExecRule, snap, pvcs)
		return err
	}
	return nil
}
