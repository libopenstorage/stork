package action

import (
	"fmt"
	"strings"

	storkv1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/libopenstorage/stork/pkg/log"
	"github.com/libopenstorage/stork/pkg/schedule"
	"github.com/libopenstorage/stork/pkg/utils"
	storkops "github.com/portworx/sched-ops/k8s/stork"
	v1 "k8s.io/api/core/v1"
	k8sErrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// TODO: source cluster-domain activation in sync-dr case
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

		if isSubList, _, _ := utils.IsSubList(namespaces, action.Spec.ActionParameter.FailbackParameter.FailbackNamespaces); !isSubList {
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

	// if no migration found, abort the failback process
	if latestMigration == nil {
		msg := fmt.Sprintf("No migration found for migrationschedule %s, hence aborting failback", migrationSchedule.Name)
		log.ActionLog(action).Errorf(msg)
		ac.recorder.Event(action,
			v1.EventTypeWarning,
			string(storkv1.ActionStatusFailed),
			msg)
		ac.updateAction(action)
		return
	}

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

	migrationScheduleName := action.Spec.ActionParameter.FailbackParameter.MigrationScheduleReference
	namespaces := action.Spec.ActionParameter.FailbackParameter.FailbackNamespaces
	migrationSchedule, err := storkops.Instance().GetMigrationSchedule(migrationScheduleName, action.Namespace)
	if err != nil {
		msg := fmt.Sprintf("Error fetching the MigrationSchedule %s/%s", action.Namespace, migrationScheduleName)
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

	migrationNamespaces, err := utils.GetMergedNamespacesWithLabelSelector(migrationSchedule.Spec.Template.Spec.Namespaces, migrationSchedule.Spec.Template.Spec.NamespaceSelectors)
	if err != nil {
		msg := fmt.Sprintf("Failed to fetch list of namespaces from the MigrationSchedule %s/%s", migrationSchedule.Namespace, migrationSchedule.Name)
		log.ActionLog(action).Infof(msg)
		ac.recorder.Event(action,
			v1.EventTypeWarning,
			string(storkv1.ActionStatusScheduled),
			msg)
		ac.updateAction(action)
		return
	}

	// only consider namespaces which are a part of both namespaces and migrationNamespaces
	// this means if there are some invalid namespaces provided for failover/failback we will ignore them
	_, actualNamespaces, _ := utils.IsSubList(namespaces, migrationNamespaces)

	//get sourceConfig from clusterPair in destination cluster
	sourceConfig, err := getClusterPairSchedulerConfig(migrationSchedule.Spec.Template.Spec.ClusterPair, migrationSchedule.Namespace)
	if err != nil {
		msg := fmt.Sprintf("Error fetching the ClusterPair %s/%s", migrationSchedule.Namespace, migrationSchedule.Spec.Template.Spec.ClusterPair)
		logEvents := ac.printFunc(action, string(storkv1.ActionStatusFailed))
		logEvents(msg, "err")
		action.Status.Status = storkv1.ActionStatusFailed
		action.Status.Reason = msg
		ac.updateAction(action)
		return
	}
	// get destination i.e. current cluster's config
	clusterConfig := ac.config
	ac.deactivateClusterDuringDR(action, actualNamespaces, sourceConfig, clusterConfig)
}

func (ac *ActionController) performLastMileMigration(action *storkv1.Action) {
	if action.Status.Status == storkv1.ActionStatusSuccessful {
		if action.Spec.ActionType == storkv1.ActionTypeFailover {
			action.Status.Stage = storkv1.ActionStageScaleUpDestination
		} else if action.Spec.ActionType == storkv1.ActionTypeFailback {
			action.Status.Stage = storkv1.ActionStageScaleUpSource
		}
		action.Status.Status = storkv1.ActionStatusInitial
		action.Status.FinishTimestamp = metav1.Now()
		ac.updateAction(action)
		return
	} else if action.Status.Status == storkv1.ActionStatusFailed {
		// move to reactivate/rollback stage
		if action.Spec.ActionType == storkv1.ActionTypeFailover {
			action.Status.Stage = storkv1.ActionStageScaleUpSource
		} else if action.Spec.ActionType == storkv1.ActionTypeFailback {
			action.Status.Stage = storkv1.ActionStageScaleUpDestination
		}
		action.Status.Status = storkv1.ActionStatusInitial
		action.Status.FinishTimestamp = metav1.Now()
		ac.updateAction(action)
		return
	}

	var migrationScheduleReference string
	var config *rest.Config
	if action.Spec.ActionType == storkv1.ActionTypeFailover {
		migrationScheduleReference = action.Spec.ActionParameter.FailoverParameter.MigrationScheduleReference
	} else if action.Spec.ActionType == storkv1.ActionTypeFailback {
		migrationScheduleReference = action.Spec.ActionParameter.FailbackParameter.MigrationScheduleReference
	}

	migrationSchedule, err := storkops.Instance().GetMigrationSchedule(migrationScheduleReference, action.Namespace)
	if err != nil {
		msg := fmt.Sprintf("Error fetching the MigrationSchedule %s/%s", action.Namespace, migrationScheduleReference)
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

	// In failover last-mile-migration is created in source->destination direction. so config used will change for failover and failback
	// get sourceConfig from clusterPair in destination cluster
	if action.Spec.ActionType == storkv1.ActionTypeFailover {
		config, err = getClusterPairSchedulerConfig(migrationSchedule.Spec.Template.Spec.ClusterPair, migrationSchedule.Namespace)
		if err != nil {
			msg := fmt.Sprintf("Error fetching the ClusterPair %s/%s", migrationSchedule.Namespace, migrationSchedule.Spec.Template.Spec.ClusterPair)
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
	} else if action.Spec.ActionType == storkv1.ActionTypeFailback {
		// get destination i.e. current cluster's config
		config = ac.config
	}

	storkClient, err := storkops.NewForConfig(config)
	if err != nil {
		msg := fmt.Sprintf("Failed to create the stork client to access cluster with the config %v", config.Host)
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

	migrationName := getLastMileMigrationName(migrationSchedule.Name, string(action.Spec.ActionType), utils.GetShortUID(string(action.UID)))
	migrationObject, err := storkClient.GetMigration(migrationName, migrationSchedule.Namespace)
	if err != nil {
		if k8sErrors.IsNotFound(err) {
			// If last mile migration CR does not exist, create one
			migration := getLastMileMigrationSpec(migrationSchedule, string(action.Spec.ActionType), utils.GetShortUID(string(action.UID)))
			_, err := storkClient.CreateMigration(migration)
			if err != nil {
				msg := fmt.Sprintf("Creating last mile migration from the MigrationSchedule %s failed: %v", migrationSchedule.GetName(), err)
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
			msg := fmt.Sprintf("Failed to get the last mile migration object %s: %v", migrationName, err)
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
			msg := fmt.Sprintf("Failing %s operation as the last mile migration %s failed: status %s", action.Spec.ActionType, migrationObject.Name, migrationObject.Status.Status)
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

	var config *rest.Config
	namespaces := action.Spec.ActionParameter.FailbackParameter.FailbackNamespaces
	migrationScheduleName := action.Spec.ActionParameter.FailbackParameter.MigrationScheduleReference

	migrationSchedule, err := storkops.Instance().GetMigrationSchedule(migrationScheduleName, action.Namespace)
	if err != nil {
		msg := fmt.Sprintf("Error fetching the MigrationSchedule %s/%s", action.Namespace, migrationScheduleName)
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
	ac.activateClusterDuringDR(action, namespaces, migrationSchedule, config)
}

func getClusterPairSchedulerConfig(clusterPairName string, namespace string) (*rest.Config, error) {
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