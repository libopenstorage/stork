package action

import (
	"fmt"

	storkv1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/libopenstorage/stork/pkg/log"
	"github.com/libopenstorage/stork/pkg/resourceutils"
	"github.com/libopenstorage/stork/pkg/schedule"
	"github.com/libopenstorage/stork/pkg/utils"
	storkops "github.com/portworx/sched-ops/k8s/stork"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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

	if len(action.Spec.ActionParameter.FailbackParameter.Namespaces) > 0 {
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

		if !utils.IsSubList(namespaces, action.Spec.ActionParameter.FailbackParameter.Namespaces) {
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

	// Return if any migration is ongoing
	for _, policyType := range storkv1.GetValidSchedulePolicyTypes() {
		policyMigration, present := migrationSchedule.Status.Items[policyType]
		if present {
			for _, migration := range policyMigration {
				if !isMigrationComplete(migration.Status) {
					msg := fmt.Sprintf("Waiting for the completion of migration %s", migration.Name)
					log.ActionLog(action).Infof(msg)
					ac.recorder.Event(action,
						v1.EventTypeWarning,
						string(storkv1.ActionStatusScheduled),
						msg)
					ac.updateAction(action)
					return
				}
			}
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
			if *migrationSchedule.Spec.Suspend {
				migrationSchedule.Spec.Suspend = &suspend
			}
			msg := fmt.Sprintf("Suspending migration schedule %s before doing failback operation", migrationSchedule.Name)
			log.ActionLog(action).Infof(msg)
			_, err := storkops.Instance().UpdateMigrationSchedule(migrationSchedule)
			if err != nil {
				log.ActionLog(action).Errorf("Error suspending migration schedule %s: %v", migrationSchedule.Name, err)
			}
			action.Status.Status = storkv1.ActionStatusSuccessful
			ac.updateAction(action)
		} else {
			// Fail the action failback if latest successful migration is older than latestMigrationThresholdTime
			// And migrationschedule is in suspended state
			log.ActionLog(action).Infof("The latest migration %s is not in threshold range", latestMigration.Name)
			msg := fmt.Sprintf("Failing failback operation as the latest migration %s is not in threshold range", latestMigration.Name)
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

func (ac *ActionController) performDeactivateFailBackStage(action *storkv1.Action) {
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
		err := resourceutils.ScaleReplicasReturningError(ns, true, true, ac.printFunc(action, "ScaleReplicas"), ac.config)
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
}
