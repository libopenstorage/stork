package tests

import (
	"fmt"
	"regexp"
	"time"

	. "github.com/onsi/gomega"
	api "github.com/portworx/px-backup-api/pkg/apis/v1"
	"github.com/portworx/sched-ops/k8s/core"
	"github.com/portworx/torpedo/drivers/backup"
	. "github.com/portworx/torpedo/tests"
	"github.com/sirupsen/logrus"
)

const (
	post_install_hook_pod = "pxcentral-post-install-hook"
	quick_maintenance_pod = "quick-maintenance-repo"
	full_maintenance_pod  = "full-maintenance-repo"
	taskNamePrefix        = "backupcreaterestore"
	defaultTimeout        = 6 * time.Minute
)

func validateBackupCluster() bool {
	flag := false
	labelSelectors := map[string]string{"job-name": post_install_hook_pod}
	ns := backup.GetPxBackupNamespace()
	pods, err := core.Instance().GetPods(ns, labelSelectors)
	if err != nil {
		logrus.Errorf("Unable to fetch pxcentral-post-install-hook pod from backup namespace\n Error : [%v]\n",
			err)
		return false
	}
	for _, pod := range pods.Items {
		logrus.Info("Checking if the pxcentral-post-install-hook pod is in Completed state or not")
		bkp_pod, err := core.Instance().GetPodByName(pod.GetName(), ns)
		if err != nil {
			logrus.Errorf("An Error Occured while getting the pxcentral-post-install-hook pod details")
			return false
		}
		container_list := bkp_pod.Status.ContainerStatuses
		for i := 0; i < len(container_list); i++ {
			status := container_list[i].State.Terminated.Reason
			if status == "Completed" {
				logrus.Info("pxcentral-post-install-hook pod is in completed state")
				flag = true
				break
			}
		}
	}
	if flag == false {
		return false
	}
	bkp_pods, err := core.Instance().GetPods(ns, nil)
	for _, pod := range bkp_pods.Items {
		matched, _ := regexp.MatchString(post_install_hook_pod, pod.GetName())
		if !matched {
			equal, _ := regexp.MatchString(quick_maintenance_pod, pod.GetName())
			equal1, _ := regexp.MatchString(full_maintenance_pod, pod.GetName())
			if !(equal || equal1) {
				logrus.Info("Checking if all the containers are up or not")
				res := core.Instance().IsPodRunning(pod)
				if !res {
					logrus.Errorf("All the containers are not Up")
					return false
				}
				err = core.Instance().ValidatePod(&pod, defaultTimeout, defaultTimeout)
				logrus.Warnf(" ERR is %s", err)
				if err != nil {
					logrus.Errorf("An Error Occured while validating the pod %v", err)
					return false
				}
			}
		}
	}
	return true
}

func createRuleForBackup(rule_name string, orgID string, pod_selector []map[string]string, action_value []string,
	background []bool, run_in_single_pod []bool, container []string) bool {
	var rulesinfo api.RulesInfo
	var uid string
	total_rules := len(action_value)
	rulesinfo_ruleitem := make([]api.RulesInfo_RuleItem, total_rules)
	for i := 0; i < total_rules; i++ {
		rule_action := api.RulesInfo_Action{Background: background[i], RunInSinglePod: run_in_single_pod[i], Value: action_value[i]}
		var actions []*api.RulesInfo_Action = []*api.RulesInfo_Action{&rule_action}
		rulesinfo_ruleitem[i].PodSelector = pod_selector[i]
		rulesinfo_ruleitem[i].Actions = actions
		rulesinfo_ruleitem[i].Container = container[i]
		rulesinfo.Rules = append(rulesinfo.Rules, &rulesinfo_ruleitem[i])
	}
	RuleCreateReq := &api.RuleCreateRequest{
		CreateMetadata: &api.CreateMetadata{
			Name:  rule_name,
			OrgId: orgID,
		},
		RulesInfo: &rulesinfo,
	}
	ctx, err := backup.GetAdminCtxFromSecret()
	Expect(err).NotTo(HaveOccurred(), fmt.Sprintf("Failed to fetch px-central-admin ctx: [%v]", err))
	if err != nil {
		logrus.Errorf("Failed to get context with error:%v", err)
		return false
	}
	_, err = Inst().Backup.CreateRule(ctx, RuleCreateReq)
	Expect(err).NotTo(HaveOccurred(), fmt.Sprintf("Failed to create rules Error: [%v]", err))
	if err != nil {
		logrus.Errorf("Rules failed to get created with error: %v", err)
		return false
	}
	logrus.Info("Validate rules for backup")
	RuleEnumerateReq := &api.RuleEnumerateRequest{
		OrgId: orgID,
	}
	rule_list, err := Inst().Backup.EnumerateRule(ctx, RuleEnumerateReq)
	for i := 0; i < len(rule_list.Rules); i++ {
		if rule_list.Rules[i].Metadata.Name == rule_name {
			uid = rule_list.Rules[i].Metadata.Uid
			break
		}
	}
	RuleInspectReq := &api.RuleInspectRequest{
		OrgId: orgID,
		Name:  rule_name,
		Uid:   uid,
	}
	_, err1 := Inst().Backup.InspectRule(ctx, RuleInspectReq)
	if err1 != nil {
		logrus.Errorf("Failed to validate the created rule with Error: [%v]", err)
		return false
	}
	return true
}
func contains(s []string, str string) bool {
	for _, v := range s {
		if v == str {
			return true
		}
	}
	return false
}
