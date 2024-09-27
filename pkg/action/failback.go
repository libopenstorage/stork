package action

import (
	"fmt"
	storkv1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	storkops "github.com/libopenstorage/stork/pkg/crud/stork"
	"github.com/libopenstorage/stork/pkg/log"
	"github.com/libopenstorage/stork/pkg/schedule"
	"github.com/libopenstorage/stork/pkg/utils"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
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

	if action.Status.Status == storkv1.ActionStatusInitial {
		action.Status.Status = storkv1.ActionStatusScheduled
		ac.updateAction(action)
	}

	migrationSchedule, err := storkops.Instance().GetMigrationSchedule(action.Spec.ActionParameter.FailbackParameter.MigrationScheduleReference, action.Namespace)
	if err != nil {
		msg := fmt.Sprintf("Unable to fetch the MigrationSchedule %s/%s needed for failback operation.", action.Namespace, action.Spec.ActionParameter.FailbackParameter.MigrationScheduleReference)
		logEvents := ac.printFunc(action, string(storkv1.ActionStatusFailed))
		logEvents(msg, "err")
		action.Status.Status = storkv1.ActionStatusFailed
		action.Status.Reason = msg
		ac.updateAction(action)
		return
	}

	if len(action.Spec.ActionParameter.FailbackParameter.FailbackNamespaces) > 0 {
		migrationNamespaces, err := utils.GetMergedNamespacesWithLabelSelector(migrationSchedule.Spec.Template.Spec.Namespaces, migrationSchedule.Spec.Template.Spec.NamespaceSelectors)
		if err != nil {
			msg := fmt.Sprintf("Failed to get list of namespaces from MigrationSchedule %s", migrationSchedule.Name)
			logEvents := ac.printFunc(action, string(storkv1.ActionStatusFailed))
			logEvents(msg, "err")
			action.Status.Status = storkv1.ActionStatusFailed
			action.Status.Reason = msg
			ac.updateAction(action)
			return
		}

		if isSubList, _, _ := utils.IsSubList(action.Spec.ActionParameter.FailbackParameter.FailbackNamespaces, migrationNamespaces); !isSubList {
			msg := fmt.Sprintf("The namespaces specified for failback are not contained within the namespaces listed in MigrationSchedule %s/%s", migrationSchedule.Namespace, migrationSchedule.Name)
			logEvents := ac.printFunc(action, string(storkv1.ActionStatusFailed))
			logEvents(msg, "err")
			action.Status.Status = storkv1.ActionStatusFailed
			action.Status.Reason = msg
			ac.updateAction(action)
			return
		}

	}

	// check if it has any latest successful migration
	schedulePolicyType, latestMigration := getLatestMigrationPolicyAndStatus(*migrationSchedule)

	// if no migration found, abort the failback process
	if latestMigration == nil {
		msg := fmt.Sprintf("No migration detected for MigrationSchedule %s/%s, resulting in the abortion of the failback operation.", migrationSchedule.Namespace, migrationSchedule.Name)
		logEvents := ac.printFunc(action, string(storkv1.ActionStatusFailed))
		logEvents(msg, "err")
		action.Status.Status = storkv1.ActionStatusFailed
		action.Status.Reason = msg
		ac.updateAction(action)
		return
	}

	if latestMigration.Status == storkv1.MigrationStatusFailed {
		msg := fmt.Sprintf("The latest migration %s is in %s state, hence aborting failback", latestMigration.Name, storkv1.MigrationStatusFailed)
		logEvents := ac.printFunc(action, string(storkv1.ActionStatusFailed))
		logEvents(msg, "err")
		action.Status.Status = storkv1.ActionStatusFailed
		action.Status.Reason = msg
		ac.updateAction(action)
		return
	}

	// Return if any migration is ongoing
	if !isMigrationComplete(latestMigration.Status) {
		msg := fmt.Sprintf("Pending completion of migration %s", latestMigration.Name)
		logEvents := ac.printFunc(action, string(storkv1.ActionStatusScheduled))
		logEvents(msg, "out")
		action.Status.Status = storkv1.ActionStatusScheduled
		action.Status.Reason = msg
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
			logEvents := ac.printFunc(action, string(storkv1.ActionStatusFailed))
			logEvents(msg, "err")
			action.Status.Status = storkv1.ActionStatusFailed
			action.Status.Reason = msg
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
			msg := fmt.Sprintf("MigrationSchedule %s/%s is being suspended prior to executing the failback operation", migrationSchedule.Namespace, migrationSchedule.Name)
			log.ActionLog(action).Infof(msg)
			_, err := storkops.Instance().UpdateMigrationSchedule(migrationSchedule)
			if err != nil {
				log.ActionLog(action).Errorf("Error suspending MigrationSchedule %s: %v", migrationSchedule.Name, err)
				return
			}

			// before deactivating the apps in destination cluster, activate the source cluster domain (if metro-dr scenario)
			// ideally source cluster domain should already be active since customer has performed a reverse migration themselves before initiating the failover
			drMode, err := ac.getDRMode(migrationSchedule.Spec.Template.Spec.ClusterPair, action.Namespace)
			if err != nil {
				msg := fmt.Sprintf("Failed to determine the mode of the DR plan: %v", err)
				logEvents := ac.printFunc(action, string(storkv1.ActionStatusFailed))
				logEvents(msg, "err")
				action.Status.Status = storkv1.ActionStatusFailed
				action.Status.Reason = msg
				ac.updateAction(action)
				return
			}
			log.ActionLog(action).Infof(fmt.Sprintf("drMode detected is %v", drMode))

			if drMode == syncDR {
				// activate the source cluster domain in case it isn't already active
				err := ac.remoteClusterDomainUpdate(true, action)
				if err != nil {
					msg := fmt.Sprintf("Failed to activate the remote cluster domain: %v", err)
					logEvents := ac.printFunc(action, "ActivateClusterDomain")
					logEvents(msg, "err")
					action.Status.Status = storkv1.ActionStatusFailed
					action.Status.Reason = msg
					ac.updateAction(action)
					return
				}
				msg = "Successfully activated the remote cluster domain"
				logEvents := ac.printFunc(action, "ClusterDomainUpdate")
				logEvents(msg, "out")
				ac.updateAction(action)
			}
			action.Status.Status = storkv1.ActionStatusSuccessful
			ac.updateAction(action)
			return
		} else {
			// Fail the action failback if latest successful migration is older than latestMigrationThresholdTime
			// And migrationschedule is in suspended state
			log.ActionLog(action).Infof("The latest migration %s is not in threshold range", latestMigration.Name)
			msg := fmt.Sprintf("Failback operation failed due to migration %s not completing within the scheduled policy duration", latestMigration.Name)
			logEvents := ac.printFunc(action, string(storkv1.ActionStatusFailed))
			logEvents(msg, "err")
			action.Status.Status = storkv1.ActionStatusFailed
			action.Status.Reason = msg
			ac.updateAction(action)
			return
		}
	}
}

func (ac *ActionController) deactivateDestinationDuringFailback(action *storkv1.Action) {
	if action.Status.Status == storkv1.ActionStatusSuccessful {
		action.Status.Stage = storkv1.ActionStageWaitAfterScaleDown
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

	if action.Status.Status == storkv1.ActionStatusInitial {
		action.Status.Status = storkv1.ActionStatusInProgress
		ac.updateAction(action)
	}

	migrationScheduleName := action.Spec.ActionParameter.FailbackParameter.MigrationScheduleReference
	namespaces := action.Spec.ActionParameter.FailbackParameter.FailbackNamespaces
	migrationSchedule, err := storkops.Instance().GetMigrationSchedule(migrationScheduleName, action.Namespace)
	if err != nil {
		msg := fmt.Sprintf("Unable to fetch the MigrationSchedule %s/%s needed for the %s operation.", action.Namespace, migrationScheduleName, action.Spec.ActionType)
		logEvents := ac.printFunc(action, string(storkv1.ActionStatusFailed))
		logEvents(msg, "err")
		action.Status.Status = storkv1.ActionStatusFailed
		action.Status.Reason = msg
		ac.updateAction(action)
		return
	}

	migrationNamespaces, err := utils.GetMergedNamespacesWithLabelSelector(migrationSchedule.Spec.Template.Spec.Namespaces, migrationSchedule.Spec.Template.Spec.NamespaceSelectors)
	if err != nil {
		msg := fmt.Sprintf("Fetching the list of namespaces from MigrationSchedule %s/%s failed", migrationSchedule.Namespace, migrationSchedule.Name)
		logEvents := ac.printFunc(action, string(storkv1.ActionStatusFailed))
		logEvents(msg, "err")
		action.Status.Status = storkv1.ActionStatusFailed
		action.Status.Reason = msg
		ac.updateAction(action)
		return
	}

	// only consider namespaces which are a part of both namespaces and migrationNamespaces
	// this means if there are some invalid namespaces provided for failover/failback we will ignore them
	_, actualNamespaces, _ := utils.IsSubList(namespaces, migrationNamespaces)

	//get sourceConfig from clusterPair in destination cluster
	sourceConfig, err := getClusterPairSchedulerConfig(migrationSchedule.Spec.Template.Spec.ClusterPair, migrationSchedule.Namespace)
	if err != nil {
		msg := fmt.Sprintf("Unable to fetch the ClusterPair %s/%s needed for %s operation", migrationSchedule.Namespace, migrationSchedule.Spec.Template.Spec.ClusterPair, action.Spec.ActionType)
		logEvents := ac.printFunc(action, string(storkv1.ActionStatusFailed))
		logEvents(msg, "err")
		action.Status.Status = storkv1.ActionStatusFailed
		action.Status.Reason = msg
		ac.updateAction(action)
		return
	}
	// get destination i.e. current cluster's config
	clusterConfig := ac.config
	ac.deactivateClusterDuringDR(action, actualNamespaces, migrationNamespaces, sourceConfig, clusterConfig)
}

func (ac *ActionController) performLastMileMigrationDuringFailback(action *storkv1.Action) {
	if action.Status.Status == storkv1.ActionStatusSuccessful {
		action.Status.Stage = storkv1.ActionStageScaleUpSource
		action.Status.Status = storkv1.ActionStatusInitial
		action.Status.FinishTimestamp = metav1.Now()
		ac.updateAction(action)
		return
	} else if action.Status.Status == storkv1.ActionStatusFailed {
		// move to reactivate/rollback stage
		action.Status.Stage = storkv1.ActionStageScaleUpDestination
		action.Status.Status = storkv1.ActionStatusInitial
		action.Status.FinishTimestamp = metav1.Now()
		ac.updateAction(action)
		return
	}

	if action.Status.Status == storkv1.ActionStatusInitial {
		action.Status.Status = storkv1.ActionStatusInProgress
		ac.updateAction(action)
	}

	migrationScheduleReference := action.Spec.ActionParameter.FailbackParameter.MigrationScheduleReference

	migrationSchedule, err := storkops.Instance().GetMigrationSchedule(migrationScheduleReference, action.Namespace)
	if err != nil {
		msg := fmt.Sprintf("Unable to fetch the MigrationSchedule %s/%s needed for the %s operation.", action.Namespace, migrationScheduleReference, action.Spec.ActionType)
		logEvents := ac.printFunc(action, string(storkv1.ActionStatusFailed))
		logEvents(msg, "err")
		action.Status.Status = storkv1.ActionStatusFailed
		action.Status.Reason = msg
		ac.updateAction(action)
		return
	}
	// In failback last-mile-migration is created in destination->source direction
	config := ac.config
	ac.createLastMileMigration(action, config, migrationSchedule)
}

// activateClusterDuringFailback is used for both activation of source and reactivation/rollback of destination during failback
func (ac *ActionController) activateClusterDuringFailback(action *storkv1.Action, rollback bool) {
	// Always Move to Final stage whether this stage succeeds or fails
	if action.Status.Status == storkv1.ActionStatusSuccessful || action.Status.Status == storkv1.ActionStatusFailed {
		action.Status.Stage = storkv1.ActionStageFinal
		if rollback {
			// Irrespective of stage status, mark the failover operation failed
			action.Status.Status = storkv1.ActionStatusFailed
		}
		action.Status.FinishTimestamp = metav1.Now()
		ac.updateAction(action)
		return
	}

	if action.Status.Status == storkv1.ActionStatusInitial {
		action.Status.Status = storkv1.ActionStatusInProgress
		ac.updateAction(action)
	}

	var config *rest.Config
	namespaces := action.Spec.ActionParameter.FailbackParameter.FailbackNamespaces
	migrationScheduleName := action.Spec.ActionParameter.FailbackParameter.MigrationScheduleReference

	migrationSchedule, err := storkops.Instance().GetMigrationSchedule(migrationScheduleName, action.Namespace)
	if err != nil {
		msg := fmt.Sprintf("Unable to fetch the MigrationSchedule %s/%s needed for the %s operation.", action.Namespace, migrationScheduleName, action.Spec.ActionType)
		logEvents := ac.printFunc(action, string(storkv1.ActionStatusFailed))
		logEvents(msg, "err")
		action.Status.Status = storkv1.ActionStatusFailed
		action.Status.Reason = msg
		ac.updateAction(action)
		return
	}
	if rollback {
		// in case of reactivation/rollback you have to activate the apps which were scaled down in the destination i.e. current cluster
		config = ac.config
	} else {
		config, err = getClusterPairSchedulerConfig(migrationSchedule.Spec.Template.Spec.ClusterPair, migrationSchedule.Namespace)
		if err != nil {
			msg := fmt.Sprintf("Failed to get the remote config from the ClusterPair %s", migrationSchedule.Spec.Template.Spec.ClusterPair)
			logEvents := ac.printFunc(action, string(storkv1.ActionStatusFailed))
			logEvents(msg, "err")
			action.Status.Status = storkv1.ActionStatusFailed
			action.Status.Reason = msg
			ac.updateAction(action)
			return
		}
	}
	ac.activateClusterDuringDR(action, namespaces, migrationSchedule, config, rollback)
}
