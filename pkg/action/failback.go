package action

import (
	"fmt"
	"slices"
	"strings"
	"time"

	storkv1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/libopenstorage/stork/pkg/log"
	"github.com/libopenstorage/stork/pkg/schedule"
	"github.com/libopenstorage/stork/pkg/utils"
	"github.com/portworx/sched-ops/k8s/core"
	storkops "github.com/portworx/sched-ops/k8s/stork"
	"github.com/sirupsen/logrus"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/rest"
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
		msg := fmt.Sprintf("error fetching Migrationschedule %s/%s for failback", action.Namespace, action.Spec.ActionParameter.FailbackParameter.MigrationScheduleReference)
		logEvents := ac.printFunc(action, string(storkv1.ActionStatusFailed))
		logEvents(msg, "err")
		action.Status.Status = storkv1.ActionStatusFailed
		action.Status.Reason = msg
		ac.updateAction(action)
		return
	}

	if len(action.Spec.ActionParameter.FailbackParameter.FailbackNamespaces) > 0 {
		namespaces, err := utils.GetMergedNamespacesWithLabelSelector(migrationSchedule.Spec.Template.Spec.Namespaces, migrationSchedule.Spec.Template.Spec.NamespaceSelectors)
		if err != nil {
			msg := fmt.Sprintf("Failed to get list of namespaces from Migrationschedule %s", migrationSchedule.Name)
			logEvents := ac.printFunc(action, string(storkv1.ActionStatusFailed))
			logEvents(msg, "err")
			action.Status.Status = storkv1.ActionStatusFailed
			action.Status.Reason = msg
			ac.updateAction(action)
			return
		}

		if isSubList, _, _ := utils.IsSubList(namespaces, action.Spec.ActionParameter.FailbackParameter.FailbackNamespaces); !isSubList {
			msg := fmt.Sprintf("Namespaces provided for failback is not a subset of namespaces from Migrationschedule %s", migrationSchedule.Name)
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
		msg := fmt.Sprintf("No migration found for Migrationschedule %s, hence aborting failback", migrationSchedule.Name)
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
		msg := fmt.Sprintf("Waiting for the completion of migration %s", latestMigration.Name)
		logEvents := ac.printFunc(action, string(storkv1.ActionStatusScheduled))
		logEvents(msg, "out")
		action.Status.Status = storkv1.ActionStatusInProgress
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
			msg := fmt.Sprintf("Suspending migration schedule %s before doing failback operation", migrationSchedule.Name)
			log.ActionLog(action).Infof(msg)
			_, err := storkops.Instance().UpdateMigrationSchedule(migrationSchedule)
			if err != nil {
				log.ActionLog(action).Errorf("Error suspending migration schedule %s: %v", migrationSchedule.Name, err)
				return
			}
			action.Status.Status = storkv1.ActionStatusSuccessful
			ac.updateAction(action)
			return
		} else {
			// Fail the action failback if latest successful migration is older than latestMigrationThresholdTime
			// And migrationschedule is in suspended state
			log.ActionLog(action).Infof("The latest migration %s is not in threshold range", latestMigration.Name)
			msg := fmt.Sprintf("Failing the failback operation because the most recent migration, %s, was not completed within the scheduled policy duration", latestMigration.Name)
			logEvents := ac.printFunc(action, string(storkv1.ActionStatusFailed))
			logEvents(msg, "err")
			action.Status.Status = storkv1.ActionStatusFailed
			action.Status.Reason = msg
			ac.updateAction(action)
			return
		}
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

	if drMode == syncDR {
		// activate the source cluster domain in case it isn't already active
		err := ac.remoteClusterDomainUpdate(true, action)
		if err != nil {
			msg := fmt.Sprintf("Failed to activate the remote cluster domain: %v", err)
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

	migrationScheduleName := action.Spec.ActionParameter.FailbackParameter.MigrationScheduleReference
	namespaces := action.Spec.ActionParameter.FailbackParameter.FailbackNamespaces
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

	migrationNamespaces, err := utils.GetMergedNamespacesWithLabelSelector(migrationSchedule.Spec.Template.Spec.Namespaces, migrationSchedule.Spec.Template.Spec.NamespaceSelectors)
	if err != nil {
		msg := fmt.Sprintf("Failed to fetch list of namespaces from the MigrationSchedule %s/%s", migrationSchedule.Namespace, migrationSchedule.Name)
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

func (ac *ActionController) waitAfterScaleDown(action *storkv1.Action) {
	if action.Status.Status == storkv1.ActionStatusSuccessful {
		if action.Spec.ActionType == storkv1.ActionTypeFailover || action.Spec.ActionType == storkv1.ActionTypeFailback {
			action.Status.Stage = storkv1.ActionStageLastMileMigration
		}
		action.Status.Status = storkv1.ActionStatusInitial
		action.Status.FinishTimestamp = metav1.Now()
		ac.updateAction(action)
		return
	} else if action.Status.Status == storkv1.ActionStatusFailed {
		// moving to reactivate/rollback stage if failed in waiting stage
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
	var namespaces []string
	if action.Spec.ActionType == storkv1.ActionTypeFailover {
		migrationScheduleReference = action.Spec.ActionParameter.FailoverParameter.MigrationScheduleReference
		namespaces = action.Spec.ActionParameter.FailoverParameter.FailoverNamespaces
	} else if action.Spec.ActionType == storkv1.ActionTypeFailback {
		migrationScheduleReference = action.Spec.ActionParameter.FailbackParameter.MigrationScheduleReference
		namespaces = action.Spec.ActionParameter.FailbackParameter.FailbackNamespaces
	}

	migrationSchedule, err := storkops.Instance().GetMigrationSchedule(migrationScheduleReference, action.Namespace)
	if err != nil {
		msg := fmt.Sprintf("Error fetching the MigrationSchedule %s/%s", action.Namespace, migrationScheduleReference)
		logEvents := ac.printFunc(action, string(storkv1.ActionStatusFailed))
		logEvents(msg, "err")
		action.Status.Status = storkv1.ActionStatusFailed
		action.Status.Reason = msg
		ac.updateAction(action)
		return
	}

	// In failover last-mile-migration is created in source->destination direction. so config used will change for failover and failback
	// get sourceConfig from clusterPair in destination cluster
	if action.Spec.ActionType == storkv1.ActionTypeFailover {
		config, err = getClusterPairSchedulerConfig(migrationSchedule.Spec.Template.Spec.ClusterPair, migrationSchedule.Namespace)
		if err != nil {
			msg := fmt.Sprintf("Error fetching the ClusterPair %s/%s", migrationSchedule.Namespace, migrationSchedule.Spec.Template.Spec.ClusterPair)
			logEvents := ac.printFunc(action, string(storkv1.ActionStatusFailed))
			logEvents(msg, "err")
			action.Status.Status = storkv1.ActionStatusFailed
			action.Status.Reason = msg
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
		logEvents := ac.printFunc(action, string(storkv1.ActionStatusFailed))
		logEvents(msg, "err")
		action.Status.Status = storkv1.ActionStatusFailed
		action.Status.Reason = msg
		ac.updateAction(action)
		return
	}

	// check if it has any latest successful migration
	// For failover, get the remote migration schedule and then check for the latest migration
	// For failback, get the current migration schedule and then check for the latest migration
	referencedMigrationScheduleForLatestMigration := migrationSchedule
	if action.Spec.ActionType == storkv1.ActionTypeFailover {
		referencedMigrationScheduleForLatestMigration, err = storkClient.GetMigrationSchedule(migrationSchedule.Name, migrationSchedule.Namespace)
		if err != nil {
			msg := fmt.Sprintf("Error fetching the MigrationSchedule %s/%s", migrationSchedule.Namespace, migrationSchedule.Name)
			logEvents := ac.printFunc(action, string(storkv1.ActionStatusFailed))
			logEvents(msg, "err")
			action.Status.Status = storkv1.ActionStatusFailed
			action.Status.Reason = msg
			ac.updateAction(action)
			return
		}
	}

	_, latestMigration := getLatestMigrationPolicyAndStatus(*referencedMigrationScheduleForLatestMigration)

	// if no migration found, abort the failback process
	if latestMigration == nil {
		msg := fmt.Sprintf("No migration found for Migrationschedule %s, hence aborting %s operation", referencedMigrationScheduleForLatestMigration.Name, action.Spec.ActionType)
		logEvents := ac.printFunc(action, string(storkv1.ActionStatusFailed))
		logEvents(msg, "err")
		action.Status.Status = storkv1.ActionStatusFailed
		action.Status.Reason = msg
		ac.updateAction(action)
		return
	}

	if !isMigrationSuccessful(latestMigration.Status) {
		msg := fmt.Sprintf("Latest migration for Migrationschedule %s/%s has not completed successfully, it's status is %s, hence aborting %s operation", referencedMigrationScheduleForLatestMigration.Namespace, referencedMigrationScheduleForLatestMigration.Name, latestMigration.Status, action.Spec.ActionType)
		logEvents := ac.printFunc(action, string(storkv1.ActionStatusFailed))
		logEvents(msg, "err")
		action.Status.Status = storkv1.ActionStatusFailed
		action.Status.Reason = msg
		ac.updateAction(action)
		return
	}

	migrationObject, err := storkClient.GetMigration(latestMigration.Name, migrationSchedule.Namespace)
	if err != nil {
		msg := fmt.Sprintf("Failed to get the latest migration %s in migration schedule %s/%s: %v", latestMigration.Name, migrationSchedule.Namespace, migrationSchedule.Name, err)
		logEvents := ac.printFunc(action, string(storkv1.ActionStatusFailed))
		logEvents(msg, "err")
		action.Status.Status = storkv1.ActionStatusFailed
		action.Status.Reason = msg
		ac.updateAction(action)
		return
	}

	migrationNamespaces, err := utils.GetMergedNamespacesWithLabelSelector(migrationSchedule.Spec.Template.Spec.Namespaces, migrationSchedule.Spec.Template.Spec.NamespaceSelectors)
	if err != nil {
		msg := fmt.Sprintf("Failed to fetch list of namespaces from the MigrationSchedule %s/%s", migrationSchedule.Namespace, migrationSchedule.Name)
		logEvents := ac.printFunc(action, string(storkv1.ActionStatusFailed))
		logEvents(msg, "err")
		action.Status.Status = storkv1.ActionStatusFailed
		action.Status.Reason = msg
		ac.updateAction(action)
		return
	}
	_, actualNamespaces, _ := utils.IsSubList(namespaces, migrationNamespaces)

	err = ac.waitForVolumesBecomeFree(action, migrationObject, actualNamespaces, config)
	if err != nil {
		msg := fmt.Sprintf("%v", err)
		logEvents := ac.printFunc(action, string(storkv1.ActionStatusFailed))
		logEvents(msg, "err")
		action.Status.Status = storkv1.ActionStatusFailed
		action.Status.Reason = msg
		ac.updateAction(action)
		return
	}

	msg := fmt.Sprintf("Pods using PVCs got scaled down successfully in cluster : %v successful. Moving to the next stage", config.Host)
	logEvents := ac.printFunc(action, string(storkv1.ActionStatusSuccessful))
	logEvents(msg, "out")
	action.Status.Status = storkv1.ActionStatusSuccessful
	action.Status.Reason = ""
	ac.updateAction(action)
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

	migrationScheduleReference := action.Spec.ActionParameter.FailbackParameter.MigrationScheduleReference

	migrationSchedule, err := storkops.Instance().GetMigrationSchedule(migrationScheduleReference, action.Namespace)
	if err != nil {
		msg := fmt.Sprintf("Error fetching the MigrationSchedule %s/%s", action.Namespace, migrationScheduleReference)
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
	ac.activateClusterDuringDR(action, namespaces, migrationSchedule, config, rollback)
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

// checkForPodStatusUsingPVC returns terminated, terminationInProgress, error
// terminated: true if all pods using PVC are terminated
// terminationInProgress: true if any pod using PVC is not terminated
// error: error if any error occurs
func checkForPodStatusUsingPVC(nsToPVCToNumberOfPods map[string]map[string]int, config *rest.Config) (bool, bool, error) {
	coreClient, err := core.NewForConfig(config)
	if err != nil {
		return false, false, err
	}

	terminationInProgress := false
	terminated := true
	for ns, pvcToNumberOfPods := range nsToPVCToNumberOfPods {
		for pvcName, numberOfPods := range pvcToNumberOfPods {
			// If numberOfPods is already 0, then no need to check for pods
			if numberOfPods > 0 {
				pods, err := coreClient.GetPodsUsingPVC(pvcName, ns)
				if err != nil {
					return false, false, err
				}
				if len(pods) == 0 {
					// All pods terminated for atlease one pvc
					terminationInProgress = true
				} else {
					// Atleast one pvc has not been freed up
					terminated = false
				}
				nsToPVCToNumberOfPods[ns][pvcName] = len(pods)
			}
		}
	}

	logrus.Infof("Current status of namespace to pvc to number of pods map: %v, terminated: %v, terminationInProgress: %v", nsToPVCToNumberOfPods, terminated, terminationInProgress)
	return terminated, terminationInProgress, nil
}

// waitForVolumesBecomeFree waits for the volumes from the given namespaces from migration object to become free
func (ac *ActionController) waitForVolumesBecomeFree(action *storkv1.Action, migration *storkv1.Migration, namespaces []string, config *rest.Config) error {
	coreClient, err := core.NewForConfig(config)
	if err != nil {
		msg := fmt.Sprintf("Error creating core client while checking for pods using volumes: %v", err)
		log.ActionLog(action).Errorf(msg)
		return fmt.Errorf(msg)
	}

	nsToPVToPVCMap := make(map[string]map[string]string)
	for _, namespace := range namespaces {
		nsToPVToPVCMap[namespace] = GetPVtoPVCMap(coreClient, namespace)
	}

	// In a loop check if volumes are getting freed up
	// If volumes are not getting freed up for maxIterationWithoutVolumesBecomingFree times, return false
	numIterWithoutTermination := 0
	maxOuterLoopCounter := 10
	maxInnerLoopCounter := 5
	// Waiting for maximum (maxOuterLoopCounter * maxInnerLoopCounter * podTerminationCheckInterval mins)
	// Even then there is an early exit if for maxIterationWithoutVolumesBecomingFree times, volumes are not freed up
	for outerloopCounter := 0; outerloopCounter < maxOuterLoopCounter; outerloopCounter++ {
		// Initialize the map with the number of pods using the volumes
		// Initializing here so that if any intermediate pod termination happens but it again gets created, we can track it
		nsToPVCToNumberOfPods := make(map[string]map[string]int)
		for _, volumeInfo := range migration.Status.Volumes {
			// Check if the volume is from the namespaces provided
			if !slices.Contains(namespaces, volumeInfo.Namespace) {
				continue
			}
			if _, ok := nsToPVToPVCMap[volumeInfo.Namespace][volumeInfo.Volume]; !ok {
				msg := fmt.Sprintf("No PVC was found in the namespace %s for pv %s", volumeInfo.Namespace, volumeInfo.Volume)
				log.ActionLog(action).Errorf(msg)
				continue
			}

			// Get the PVC name from the PV name from the map
			pvcName := nsToPVToPVCMap[volumeInfo.Namespace][volumeInfo.Volume]

			if _, ok := nsToPVCToNumberOfPods[volumeInfo.Namespace]; !ok {
				nsToPVCToNumberOfPods[volumeInfo.Namespace] = make(map[string]int)
			}
			pods, err := coreClient.GetPodsUsingPVC(pvcName, volumeInfo.Namespace)
			if err != nil {
				msg := fmt.Sprintf("Error fetching pods using PVC %s/%s: %v", volumeInfo.Namespace, pvcName, err)
				log.ActionLog(action).Errorf(msg)
				return fmt.Errorf(msg)
			}
			nsToPVCToNumberOfPods[volumeInfo.Namespace][pvcName] = len(pods)
		}
		logrus.Infof("Initializing namespace to PVC to number of pods map: %v", nsToPVCToNumberOfPods)

		// Check if there are any pods using the volumes in a loop
		terminationInprogressIncurrentIteration := false
		for iter := 0; iter < maxInnerLoopCounter; iter++ {
			terminated, terminationInProgress, err := checkForPodStatusUsingPVC(nsToPVCToNumberOfPods, config)
			if err != nil {
				return err
			}
			log.ActionLog(action).Infof("Waiting for pod terminations iter: %d, PVCs to pod numbers map: %v", iter, nsToPVCToNumberOfPods)
			if terminated {
				return nil
			}

			// If terminationInProgress then enable the marker for this iteration
			if terminationInProgress {
				terminationInprogressIncurrentIteration = true
			}
			// If termination is in progress, then wait for podTerminationCheckInterval minutes before checking again
			log.ActionLog(action).Infof("Pods using PVCs from migration are not terminated. Waiting for %v seconds before checking again", podTerminationCheckInterval)
			time.Sleep(podTerminationCheckInterval)
		}

		// If termination is in progress in the current iteration, then reset the counter else increment the counter
		if terminationInprogressIncurrentIteration {
			numIterWithoutTermination = 0
		} else {
			numIterWithoutTermination++
		}

		// if number of iters without termination happening is gte MAX_ITER_WAIT_VOLUMES_FREE, then return error
		if numIterWithoutTermination >= maxIterationWithoutVolumesBecomingFree {
			log.ActionLog(action).Errorf("Volumes are not getting freed up for %d iterations, hence timing out", maxIterationWithoutVolumesBecomingFree)
			return fmt.Errorf("pods using PVCs from migration are not getting terminated for last %v", time.Duration(maxIterationWithoutVolumesBecomingFree*maxInnerLoopCounter)*podTerminationCheckInterval)
		}
	}
	return fmt.Errorf("pods are not getting terminated even after %v", time.Duration(maxOuterLoopCounter)*time.Duration(maxInnerLoopCounter)*podTerminationCheckInterval)
}

// GetPVtoPVCMap returns a map of PV to PVCs in a namespace
func GetPVtoPVCMap(coreClient *core.Client, namespace string) map[string]string {
	pvcList, err := coreClient.GetPersistentVolumeClaims(namespace, nil)
	if err != nil {
		return nil
	}

	pvToPVCMap := make(map[string]string)
	for _, pvc := range pvcList.Items {
		if pvc.Spec.VolumeName != "" {
			pvToPVCMap[pvc.Spec.VolumeName] = pvc.Name
		}
	}
	return pvToPVCMap
}
