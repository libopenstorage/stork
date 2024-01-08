package resourceutils

import (
	api "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	kubevirtops "github.com/portworx/sched-ops/k8s/kubevirt"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	//VmFreezePrefix prefix for freeze rule action
	vmFreezePrefix = "vm-freeze-rule"
	//VmUnFreezePrefix prefix for freeze rule action
	vmUnFreezePrefix = "vm-unfreeze-rule"
	// annotationsKeys to identify these are in corrolation with px-backup request.
	annotationKeyPrefix = "portworx.io/"
	backupUIDKey        = annotationKeyPrefix + "backup-uid"
	backupNameKey       = annotationKeyPrefix + "backup-name"
	clusterNameKey      = annotationKeyPrefix + "cluster-name"
	clusterUIDKey       = annotationKeyPrefix + "cluster-uid"
	createdByKey        = annotationKeyPrefix + "created-by"
	createdByValue      = "stork"
	lastUpdateKey       = annotationKeyPrefix + "last-update"
	VmResourcePopulated = annotationKeyPrefix + "vm-resources-populated"
)

func getRuleItemWithAction(podSelector map[string]string, ruleAction string) api.RuleItem {
	var actions []api.RuleAction

	action := api.RuleAction{
		Type:  api.RuleActionCommand,
		Value: ruleAction,
	}
	actions = append(actions, action)
	ruleInfoItem := api.RuleItem{
		PodSelector: podSelector,
		Actions:     actions,
	}
	return ruleInfoItem
}

func resourceToObjectInfo(name, namespace, kind string) api.ObjectInfo {

	return api.ObjectInfo{

		Name:      name,
		Namespace: namespace,
		GroupVersionKind: metav1.GroupVersionKind{
			Kind:    kind,
			Version: "v1",
			Group:   "core",
		},
	}
}

func ProcessVMBackupRequest(backup *api.ApplicationBackup) (*api.Rule, *api.Rule, []api.ObjectInfo, error) {

	freezeRulesItems := make([]api.RuleItem, 0)
	unFreezeRulesItems := make([]api.RuleItem, 0)

	var vmresources []api.ObjectInfo
	updateRuleCr := false
	kv := kubevirtops.Instance()

	if backup.Spec.PreExecRule == "" && backup.Spec.PostExecRule == "" {

		updateRuleCr = true
	}

	for _, resourceInfo := range backup.Spec.IncludeResources {
		switch resourceInfo.Kind {
		case "VirtualMachine":
			vm, err := kv.GetVirtualMachine(resourceInfo.Name, resourceInfo.Namespace)
			if err != nil {
				return nil, nil, nil, err
			}
			volumes := kv.GetVMPersistentVolumeClaims(vm)
			for _, name := range volumes {
				vmresources = append(vmresources,
					resourceToObjectInfo(name, vm.Namespace, "PersistentVolumeClaim"))
			}

			secrets := kv.GetVMSecrets(vm)
			for _, name := range secrets {
				vmresources = append(vmresources,
					resourceToObjectInfo(name, vm.Namespace, "Secret"))
			}
			configMaps := kv.GetVMConfigMaps(vm)
			for _, name := range configMaps {
				vmresources = append(vmresources,
					resourceToObjectInfo(name, vm.Namespace, "ConfigMap"))
			}
			if !updateRuleCr {
				continue
			}
			if !kv.IsVirtualMachineRunning(vm) || !kv.IsVMAgentConnected(vm) {
				continue
			}
			vmPodSelector := kv.GetVMPodLabel(vm)
			vmfreezeAction := kv.GetVMFreezeRule(vm)
			vmUnFreezeAction := kv.GetVMUnFreezeRule(vm)
			preRuleItem := getRuleItemWithAction(vmPodSelector, vmfreezeAction)
			postRuleItem := getRuleItemWithAction(vmPodSelector, vmUnFreezeAction)
			freezeRulesItems = append(freezeRulesItems, preRuleItem)
			unFreezeRulesItems = append(unFreezeRulesItems, postRuleItem)
		}
	}
	if len(freezeRulesItems) == 0 || len(unFreezeRulesItems) == 0 {
		return nil, nil, vmresources, nil
	}

	freezeRulesObject := createRuleCr(freezeRulesItems, backup, vmFreezePrefix)
	unFreezeRulesObject := createRuleCr(unFreezeRulesItems, backup, vmUnFreezePrefix)

	return freezeRulesObject, unFreezeRulesObject, vmresources, nil
}

func createRuleCr(rules []api.RuleItem, backup *api.ApplicationBackup, name string) *api.Rule {

	rulesObject := &api.Rule{
		TypeMeta:   metav1.TypeMeta{},
		ObjectMeta: metav1.ObjectMeta{},
		Rules:      rules,
	}

	rulesObject.Name = name + "-" + backup.Annotations[backupUIDKey]
	rulesObject.Namespace = backup.GetNamespace()

	var annotations = make(map[string]string)
	annotations[createdByKey] = createdByValue
	annotations[lastUpdateKey] = metav1.Now().String()
	annotations[clusterNameKey] = backup.Annotations[clusterNameKey]
	annotations[clusterUIDKey] = backup.Annotations[clusterUIDKey]

	rulesObject.Annotations = annotations

	return rulesObject
}
