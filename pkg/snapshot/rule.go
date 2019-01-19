package snapshot

import (
	crdv1 "github.com/kubernetes-incubator/external-storage/snapshot/pkg/apis/crd/v1"
	"github.com/libopenstorage/stork/pkg/rule"
	"github.com/portworx/sched-ops/k8s"
	"github.com/sirupsen/logrus"
	"k8s.io/api/core/v1"
)

const (
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

func setKind(snap *crdv1.VolumeSnapshot) {
	snap.Kind = "VolumeSnapshot"
	snap.APIVersion = crdv1.SchemeGroupVersion.String()
}

// ExecutePreSnapRule executes the pre snapshot rule. pvcs is a list of PVCs that are associated
// with the snapshot. It returns a channel which the caller can trigger to delete the termination of background commands
func ExecutePreSnapRule(snap *crdv1.VolumeSnapshot, pvcs []v1.PersistentVolumeClaim) (chan bool, error) {
	setKind(snap)
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
		return rule.ExecuteRule(r, rule.PreExecRule, snap, snap.Metadata.Namespace)
	}
	return nil, nil
}

// ExecutePostSnapRule executes the post snapshot rule for the given snapshot. pvcs is a list of PVCs
// that are associated with the snapshot
func ExecutePostSnapRule(pvcs []v1.PersistentVolumeClaim, snap *crdv1.VolumeSnapshot) error {
	setKind(snap)
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
		_, err = rule.ExecuteRule(r, rule.PostExecRule, snap, snap.Metadata.Namespace)
		return err
	}
	return nil
}

// performRuleRecovery terminates potential background commands running pods for
// the given snapshot
func performRuleRecovery() error {
	allSnaps, err := k8s.Instance().ListSnapshots(v1.NamespaceAll)
	if err != nil {
		logrus.Errorf("Failed to list all snapshots due to: %v. Will retry.", err)
		return err
	}

	if allSnaps == nil {
		return nil
	}

	var lastError error
	for _, snap := range allSnaps.Items {
		setKind(&snap)
		err := rule.PerformRuleRecovery(&snap)
		if err != nil {
			lastError = err
		}
	}
	return lastError
}
