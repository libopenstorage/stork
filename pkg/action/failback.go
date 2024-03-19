package action

import (
	"fmt"
	"strings"

	storkv1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/libopenstorage/stork/pkg/log"
	"github.com/libopenstorage/stork/pkg/resourceutils"
	"github.com/libopenstorage/stork/pkg/schedule"
	"github.com/libopenstorage/stork/pkg/utils"
	storkops "github.com/portworx/sched-ops/k8s/stork"
	v1 "k8s.io/api/core/v1"
	k8sErrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	restclient "k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

func (ac *ActionController) verifyMigrationScheduleBeforeFailback(action *storkv1.Action) {

	if action.Status.Status == storkv1.ActionStatusSuccessful {
		action.Status.Stage = storkv1.ActionStageScaleDownDestination
		action.Status.Status = storkv1.ActionStatusInitial
		action.Status.FinishTimestamp = metav1.Now()
		ac.updateAction(action)
		return
	} else if action.Status.Status == storkv1.ActionStatusFailed {
		action.Status.Stage = storkv1.ActionStageFinal
		action.Status.FinishTimestamp = metav1.Now()
		ac.updateAction(action)
		return
	}

	migrationSchedule, err := storkops.Instance().GetMigrationSchedule(action.Spec.ActionParameter.FailbackParameter.MigrationScheduleReference, action.Namespace)
	if err != nil {
		msg := fmt.Sprintf("error fetching migrationschedule %s/%s for failback", action.Namespace, action.Spec.ActionParameter.FailbackParameter.MigrationScheduleReference)
		log.ActionLog(action).Errorf(msg)
		action.Status.Status = storkv1.ActionStatusFailed
		action.Status.Reason = msg
		ac.recorder.Event(action,
			v1.EventTypeWarning,
			string(storkv1.ActionStatusFailed),
			msg)
		ac.updateAction(action)
		return
	}

	if len(action.Spec.ActionParameter.FailbackParameter.FailbackNamespaces) > 0 {
		namespaces, err := utils.GetMergedNamespacesWithLabelSelector(migrationSchedule.Spec.Template.Spec.Namespaces, migrationSchedule.Spec.Template.Spec.NamespaceSelectors)
		if err != nil {
			msg := fmt.Sprintf("Failed to get list of namespaces from migrationschedule %s", migrationSchedule.Name)
			log.ActionLog(action).Infof(msg)
			ac.recorder.Event(action,
				v1.EventTypeWarning,
				string(storkv1.ActionStatusScheduled),
				msg)
			ac.updateAction(action)
			return
		}

		if isSubList, _ := utils.IsSubList(namespaces, action.Spec.ActionParameter.FailbackParameter.FailbackNamespaces); !isSubList {
			msg := fmt.Sprintf("Namespaces provided for failback is not a subset of namespaces from migrationschedule %s", migrationSchedule.Name)
			log.ActionLog(action).Infof(msg)
			ac.recorder.Event(action,
				v1.EventTypeWarning,
				string(storkv1.ActionStatusScheduled),
				msg)
			ac.updateAction(action)
			return
		}

	}

	// check if it has any latest successful migration
	schedulePolicyType, latestMigration := getLatestMigrationPolicyAndStatus(*migrationSchedule)
	if latestMigration.Status == storkv1.MigrationStatusFailed {
		msg := fmt.Sprintf("The latest migration %s is in %s state, hence aborting failback", latestMigration.Name, storkv1.MigrationStatusFailed)
		log.ActionLog(action).Errorf(msg)
		action.Status.Status = storkv1.ActionStatusFailed
		action.Status.Reason = msg
		ac.recorder.Event(action,
			v1.EventTypeWarning,
			string(storkv1.ActionStatusFailed),
			msg)
		ac.updateAction(action)
		return
	}

	// Return if any migration is ongoing
	if !isMigrationComplete(latestMigration.Status) {
		msg := fmt.Sprintf("Waiting for the completion of migration %s", latestMigration.Name)
		log.ActionLog(action).Infof(msg)
		ac.recorder.Event(action,
			v1.EventTypeWarning,
			string(storkv1.ActionStatusScheduled),
			msg)
		ac.updateAction(action)
		return
	}

	if latestMigration.Status == storkv1.MigrationStatusSuccessful ||
		latestMigration.Status == storkv1.MigrationStatusPartialSuccess {
		// Check if the latest migration has run with in the policy time
		alreadyTriggered, err := schedule.AlreadyTriggered(
			migrationSchedule.Spec.SchedulePolicyName,
			migrationSchedule.Namespace,
			schedulePolicyType,
			latestMigration.FinishTimestamp,
		)
		if err != nil {
			msg := fmt.Sprintf("Error while checking if the latest migration %s has been successful: %v", latestMigration.Name, err)
			log.ActionLog(action).Errorf(msg)
			action.Status.Status = storkv1.ActionStatusFailed
			action.Status.Reason = msg
			ac.recorder.Event(action,
				v1.EventTypeWarning,
				string(storkv1.ActionStatusFailed),
				msg)
			ac.updateAction(action)
			return
		}
		if alreadyTriggered {
			log.ActionLog(action).Infof("The latest migration %s is in threshold range", latestMigration.Name)
			// Suspend migration schedule
			suspend := true
			if !*migrationSchedule.Spec.Suspend {
				migrationSchedule.Spec.Suspend = &suspend
			}
			msg := fmt.Sprintf("Suspending migration schedule %s before doing failback operation", migrationSchedule.Name)
			log.ActionLog(action).Infof(msg)
			_, err := storkops.Instance().UpdateMigrationSchedule(migrationSchedule)
			if err != nil {
				log.ActionLog(action).Errorf("Error suspending migration schedule %s: %v", migrationSchedule.Name, err)
				return
			}
			action.Status.Status = storkv1.ActionStatusSuccessful
			ac.updateAction(action)
		} else {
			// Fail the action failback if latest successful migration is older than latestMigrationThresholdTime
			// And migrationschedule is in suspended state
			log.ActionLog(action).Infof("The latest migration %s is not in threshold range", latestMigration.Name)
			msg := fmt.Sprintf("Failing failback operation as the latest migration %s was not completed from the scheduled policy duration from now", latestMigration.Name)
			log.ActionLog(action).Errorf(msg)
			action.Status.Status = storkv1.ActionStatusFailed
			action.Status.Reason = msg
			ac.recorder.Event(action,
				v1.EventTypeWarning,
				string(storkv1.ActionStatusFailed),
				msg)
			ac.updateAction(action)
		}
	}
}

func (ac *ActionController) deactivateDestinationDuringFailback(action *storkv1.Action) {
	if action.Status.Status == storkv1.ActionStatusSuccessful {
		action.Status.Stage = storkv1.ActionStageLastMileMigration
		action.Status.Status = storkv1.ActionStatusInitial
		action.Status.FinishTimestamp = metav1.Now()
		ac.updateAction(action)
		return
	} else if action.Status.Status == storkv1.ActionStatusFailed {
		action.Status.Stage = storkv1.ActionStageFinal
		action.Status.FinishTimestamp = metav1.Now()
		ac.updateAction(action)
		return
	}

	migrationSchedule, err := storkops.Instance().GetMigrationSchedule(action.Spec.ActionParameter.FailbackParameter.MigrationScheduleReference, action.Namespace)
	if err != nil {
		msg := fmt.Sprintf("error fetching migrationschedule %s/%s for failback", action.Namespace, action.Spec.ActionParameter.FailbackParameter.MigrationScheduleReference)
		log.ActionLog(action).Errorf(msg)
		action.Status.Status = storkv1.ActionStatusFailed
		action.Status.Reason = msg
		ac.recorder.Event(action,
			v1.EventTypeWarning,
			string(storkv1.ActionStatusFailed),
			msg)
		ac.updateAction(action)
		return
	}

	namespaces, err := utils.GetMergedNamespacesWithLabelSelector(migrationSchedule.Spec.Template.Spec.Namespaces, migrationSchedule.Spec.Template.Spec.NamespaceSelectors)
	if err != nil {
		msg := fmt.Sprintf("Failed to get list of namespaces from migrationschedule %s", migrationSchedule.Name)
		log.ActionLog(action).Infof(msg)
		ac.recorder.Event(action,
			v1.EventTypeWarning,
			string(storkv1.ActionStatusScheduled),
			msg)
		ac.updateAction(action)
		return
	}

	for _, ns := range namespaces {
		err := resourceutils.ScaleReplicasReturningError(ns, false, true, ac.printFunc(action, "ScaleReplicas"), ac.config)
		if err != nil {
			msg := fmt.Sprintf("Failed to scale down replicas: %v", err)
			log.ActionLog(action).Errorf(msg)
			action.Status.Status = storkv1.ActionStatusFailed
			action.Status.Reason = msg
			ac.recorder.Event(action,
				v1.EventTypeWarning,
				string(storkv1.ActionStatusFailed),
				msg)
			ac.updateAction(action)
			return
		}
	}
	log.ActionLog(action).Infof("Going to update the action status successful for stage %s", storkv1.ActionStageScaleDownDestination)
	action.Status.Status = storkv1.ActionStatusSuccessful
	action.Status.Reason = ""
	ac.updateAction(action)
}

func (ac *ActionController) performLastMileMigration(action *storkv1.Action) {
	if action.Status.Status == storkv1.ActionStatusSuccessful {
		action.Status.Stage = storkv1.ActionStageScaleUpSource
		action.Status.Status = storkv1.ActionStatusInitial
		action.Status.FinishTimestamp = metav1.Now()
		ac.updateAction(action)
		return
	} else if action.Status.Status == storkv1.ActionStatusFailed {
		action.Status.Stage = storkv1.ActionStageScaleUpDestination
		action.Status.Status = storkv1.ActionStatusInitial
		action.Status.FinishTimestamp = metav1.Now()
		ac.updateAction(action)
		return
	}

	migrationSchedule, err := storkops.Instance().GetMigrationSchedule(action.Spec.ActionParameter.FailbackParameter.MigrationScheduleReference, action.Namespace)
	if err != nil {
		msg := fmt.Sprintf("error fetching migrationschedule %s/%s for failback", action.Namespace, action.Spec.ActionParameter.FailbackParameter.MigrationScheduleReference)
		log.ActionLog(action).Errorf(msg)
		action.Status.Status = storkv1.ActionStatusFailed
		action.Status.Reason = msg
		ac.recorder.Event(action,
			v1.EventTypeWarning,
			string(storkv1.ActionStatusFailed),
			msg)
		ac.updateAction(action)
		return
	}

	migrationName := getLastMileMigrationName(migrationSchedule.Name, string(storkv1.ActionTypeFailback), utils.GetShortUID(string(action.UID)))
	migrationObject, err := storkops.Instance().GetMigration(migrationName, migrationSchedule.Namespace)
	if err != nil {
		if k8sErrors.IsNotFound(err) {
			// If last mile migration CR does not exist, create one
			migration := getLastMileMigrationSpec(migrationSchedule, string(storkv1.ActionTypeFailback), utils.GetShortUID(string(action.UID)))
			_, err := storkops.Instance().CreateMigration(migration)
			if err != nil {
				msg := fmt.Sprintf("Creating last mile migration from migrationschedule %s failed: %v", migrationSchedule.GetName(), err)
				ac.recorder.Event(action,
					v1.EventTypeWarning,
					string(storkv1.ActionStatusFailed),
					msg)
				ac.updateAction(action)
				return
			}
			action.Status.Status = storkv1.ActionStatusInProgress
			ac.updateAction(action)
			return
		} else {
			msg := fmt.Sprintf("Failed to get last mile migration object %s: %v", migrationName, err)
			log.ActionLog(action).Errorf(msg)
			ac.recorder.Event(action,
				v1.EventTypeWarning,
				string(storkv1.ActionStatusFailed),
				msg)
			ac.updateAction(action)
			return
		}
	}

	if migrationObject.Status.Stage == storkv1.MigrationStageFinal {
		if migrationObject.Status.Status == storkv1.MigrationStatusSuccessful || migrationObject.Status.Status == storkv1.MigrationStatusPartialSuccess {
			action.Status.Status = storkv1.ActionStatusSuccessful
		} else {
			msg := fmt.Sprintf("Failing failback operation as the last mile migration %s failed: status %s", migrationObject.Name, migrationObject.Status.Status)
			log.ActionLog(action).Errorf(msg)
			action.Status.Status = storkv1.ActionStatusFailed
			action.Status.Reason = msg
			ac.recorder.Event(action,
				v1.EventTypeWarning,
				string(storkv1.ActionStatusFailed),
				msg)
		}
		ac.updateAction(action)
		return
	}
	log.ActionLog(action).Infof("Last mile migration %s is still running, currently in stage: %s", migrationObject.Name, migrationObject.Status.Stage)
}

// getLastMileMigrationSpec will get a migration spec from the given migrationschedule spec
func getLastMileMigrationSpec(ms *storkv1.MigrationSchedule, operation string, actionCRUID string) *storkv1.Migration {

	migrationName := getLastMileMigrationName(ms.GetName(), operation, actionCRUID)
	migrationSpec := ms.Spec.Template.Spec
	// Make startApplications always false
	startApplications := false
	migrationSpec.StartApplications = &startApplications

	migration := &storkv1.Migration{
		ObjectMeta: metav1.ObjectMeta{
			Name:      migrationName,
			Namespace: ms.Namespace,
		},
		Spec: migrationSpec,
	}

	if migration.Annotations == nil {
		migration.Annotations = make(map[string]string)
	}
	for k, v := range ms.Annotations {
		migration.Annotations[k] = v
	}
	migration.Annotations[StorkLastMileMigrationScheduleName] = ms.GetName()
	return migration
}

func getLastMileMigrationName(migrationScheduleName string, operation string, inputUID string) string {
	// +3 is for delimiters
	suffixLengths := len(lastmileMigrationPrefix) + len(operation) + len(inputUID) + 3
	// Not adding any datetime as salt to the name as the migration needs to be checked for its status in each reconciler handler cycle.
	if len(migrationScheduleName)+suffixLengths > validation.DNS1123SubdomainMaxLength {
		migrationScheduleName = migrationScheduleName[:validation.DNS1123SubdomainMaxLength-suffixLengths]
	}
	return strings.Join([]string{migrationScheduleName,
		lastmileMigrationPrefix,
		strings.ToLower(operation),
		inputUID}, "-")
}

func (ac *ActionController) activateSourceDuringFailBack(action *storkv1.Action) {
	// Move to Final stage if this stage succeeds or fails
	if action.Status.Status == storkv1.ActionStatusSuccessful || action.Status.Status == storkv1.ActionStatusFailed {
		action.Status.Stage = storkv1.ActionStageFinal
		action.Status.FinishTimestamp = metav1.Now()
		ac.updateAction(action)
		return
	}

	migrationSchedule, err := storkops.Instance().GetMigrationSchedule(action.Spec.ActionParameter.FailbackParameter.MigrationScheduleReference, action.Namespace)
	if err != nil {
		msg := fmt.Sprintf("error fetching migrationschedule %s/%s for failback", action.Namespace, action.Spec.ActionParameter.FailbackParameter.MigrationScheduleReference)
		log.ActionLog(action).Errorf(msg)
		action.Status.Status = storkv1.ActionStatusFailed
		action.Status.Reason = msg
		ac.recorder.Event(action,
			v1.EventTypeWarning,
			string(storkv1.ActionStatusFailed),
			msg)
		ac.updateAction(action)
		return
	}

	namespaces, err := utils.GetMergedNamespacesWithLabelSelector(migrationSchedule.Spec.Template.Spec.Namespaces, migrationSchedule.Spec.Template.Spec.NamespaceSelectors)
	if err != nil {
		msg := fmt.Sprintf("Failed to get list of namespaces from migrationschedule %s", migrationSchedule.Name)
		log.ActionLog(action).Errorf(msg)
		ac.recorder.Event(action,
			v1.EventTypeWarning,
			string(storkv1.ActionStatusScheduled),
			msg)
		ac.updateAction(action)
		return
	}

	remoteConfig, err := getClusterPairSchedulerConfig(migrationSchedule.Spec.Template.Spec.ClusterPair, migrationSchedule.Namespace)
	if err != nil {
		msg := fmt.Sprintf("Failed to get the remote config from the clusterpair %s", migrationSchedule.Spec.Template.Spec.ClusterPair)
		log.ActionLog(action).Errorf(msg)
		action.Status.Status = storkv1.ActionStatusFailed
		action.Status.Reason = msg
		ac.recorder.Event(migrationSchedule,
			v1.EventTypeWarning,
			string(storkv1.ActionStatusFailed),
			err.Error())
		ac.updateAction(action)
		return
	}

	scaleupStatus := true
	failbackSummaryList := make([]*storkv1.FailbackSummary, 0)
	for _, ns := range namespaces {
		err := resourceutils.ScaleReplicasReturningError(ns, true, false, ac.printFunc(action, "ScaleReplicas"), remoteConfig)
		failbackSummary := &storkv1.FailbackSummary{
			Namespace: ns,
			Status:    storkv1.ActionStatusSuccessful,
			Reason:    "",
		}
		if err != nil {
			scaleupStatus = false
			failbackSummary = &storkv1.FailbackSummary{
				Namespace: ns,
				Status:    storkv1.ActionStatusFailed,
				Reason:    fmt.Sprintf("scaling up apps in namespace %s failed: %v", ns, err),
			}
		}
		failbackSummaryList = append(failbackSummaryList, failbackSummary)
	}
	action.Status.Summary = &storkv1.ActionSummary{FailbackSummaryItem: failbackSummaryList}
	if scaleupStatus {
		action.Status.Status = storkv1.ActionStatusSuccessful
	} else {
		action.Status.Status = storkv1.ActionStatusFailed
	}
	ac.updateAction(action)

}

// performActivateFailBackStageDestination will be used for rolling back
// in case failback operation fails to activate apps in destination cluster
func (ac *ActionController) activateDestinationDuringFailBack(action *storkv1.Action) {

	// If the status is Successful or Failed, mark the failback operation failed
	if action.Status.Status == storkv1.ActionStatusSuccessful || action.Status.Status == storkv1.ActionStatusFailed {
		action.Status.Stage = storkv1.ActionStageFinal
		action.Status.Status = storkv1.ActionStatusFailed
		action.Status.FinishTimestamp = metav1.Now()
		ac.updateAction(action)
		return
	}

	migrationSchedule, err := storkops.Instance().GetMigrationSchedule(action.Spec.ActionParameter.FailbackParameter.MigrationScheduleReference, action.Namespace)
	if err != nil {
		msg := fmt.Sprintf("error fetching migrationschedule %s/%s for failback", action.Namespace, action.Spec.ActionParameter.FailbackParameter.MigrationScheduleReference)
		log.ActionLog(action).Errorf(msg)
		action.Status.Status = storkv1.ActionStatusFailed
		action.Status.Reason = msg
		ac.recorder.Event(action,
			v1.EventTypeWarning,
			string(storkv1.ActionStatusFailed),
			msg)
		ac.updateAction(action)
		return
	}

	namespaces, err := utils.GetMergedNamespacesWithLabelSelector(migrationSchedule.Spec.Template.Spec.Namespaces, migrationSchedule.Spec.Template.Spec.NamespaceSelectors)
	if err != nil {
		msg := fmt.Sprintf("Failed to get list of namespaces from migrationschedule %s", migrationSchedule.Name)
		log.ActionLog(action).Errorf(msg)
		ac.recorder.Event(action,
			v1.EventTypeWarning,
			string(storkv1.ActionStatusScheduled),
			msg)
		ac.updateAction(action)
		return
	}

	for _, ns := range namespaces {
		err := resourceutils.ScaleReplicasReturningError(ns, true, false, ac.printFunc(action, "ScaleReplicas"), ac.config)
		if err != nil {
			msg := fmt.Sprintf("scaling up apps in namespace %s failed: %v", ns, err)
			log.ActionLog(action).Errorf(msg)
		}
	}

	// marking the stage's status as successful even if it hit error while activating apps up
	action.Status.Status = storkv1.ActionStatusSuccessful
	ac.updateAction(action)
}

func getClusterPairSchedulerConfig(clusterPairName string, namespace string) (*restclient.Config, error) {
	clusterPair, err := storkops.Instance().GetClusterPair(clusterPairName, namespace)
	if err != nil {
		return nil, fmt.Errorf("error getting clusterpair (%v/%v): %v", namespace, clusterPairName, err)
	}
	remoteClientConfig := clientcmd.NewNonInteractiveClientConfig(
		clusterPair.Spec.Config,
		clusterPair.Spec.Config.CurrentContext,
		&clientcmd.ConfigOverrides{},
		clientcmd.NewDefaultClientConfigLoadingRules())
	return remoteClientConfig.ClientConfig()
}
