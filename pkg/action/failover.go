package action

import (
	"fmt"
	"slices"
	"time"

	storkv1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/libopenstorage/stork/pkg/log"
	"github.com/libopenstorage/stork/pkg/resourceutils"
	"github.com/libopenstorage/stork/pkg/utils"
	coreops "github.com/portworx/sched-ops/k8s/core"
	storkops "github.com/portworx/sched-ops/k8s/stork"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
)

// validationsBeforeFailover This method is called as part of the Initial stage of Failover
func (ac *ActionController) validationsBeforeFailover(action *storkv1.Action) {
	// TODO: source cluster-domain deactivation in sync-dr case
	if action.Status.Status == storkv1.ActionStatusSuccessful {
		// Move to ScaleUpDestination stage directly in case skipDeactivateSource is true
		if *action.Spec.ActionParameter.FailoverParameter.SkipDeactivateSource {
			logEvents := ac.printFunc(action, string(storkv1.ActionStatusSuccessful))
			logEvents("Skipping the deactivation of source cluster and moving to ScaleUpDestination stage since --skip-deactivate-source is provided", "out")
			action.Status.Stage = storkv1.ActionStageScaleUpDestination
		} else {
			action.Status.Stage = storkv1.ActionStageScaleDownSource
		}
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

	// first get the migrationSchedule's static copy in the destination cluster
	migrationSchedule, err := storkops.Instance().GetMigrationSchedule(action.Spec.ActionParameter.FailoverParameter.MigrationScheduleReference, action.Namespace)
	if err != nil {
		msg := fmt.Sprintf("error getting migrationschedule %s/%s for failover", action.Namespace, action.Spec.ActionParameter.FailoverParameter.MigrationScheduleReference)
		logEvents := ac.printFunc(action, string(storkv1.ActionStatusFailed))
		logEvents(msg, "err")
		action.Status.Status = storkv1.ActionStatusFailed
		action.Status.Reason = msg
		ac.updateAction(action)
		return
	}
	// get sourceConfig from clusterPair in the destination cluster
	sourceConfig, err := getClusterPairSchedulerConfig(migrationSchedule.Spec.Template.Spec.ClusterPair, migrationSchedule.Namespace)
	if err != nil {
		msg := fmt.Sprintf("Error while getting the clusterpair %s/%s for failover", migrationSchedule.Namespace, migrationSchedule.Spec.Template.Spec.ClusterPair)
		logEvents := ac.printFunc(action, string(storkv1.ActionStatusFailed))
		logEvents(msg, "err")
		action.Status.Status = storkv1.ActionStatusFailed
		action.Status.Reason = msg
		ac.updateAction(action)
		return
	}
	// check accessibility of the source cluster, get the migrationSchedule in the source cluster to check for any ongoing migrations and to suspend the migrationSchedule
	if !ac.isClusterAccessible(action, sourceConfig) {
		msg := "Unable to access the remote cluster. Directly moving to the Failover ScaleUpDestinationStage"
		log.ActionLog(action).Warnf(msg)
		// logging the Initial stage as successful and directly moving next stage to ScaleUpDestination
		ac.recorder.Event(action,
			v1.EventTypeWarning,
			string(storkv1.ActionStatusSuccessful),
			msg)
		action.Status.Stage = storkv1.ActionStageScaleUpDestination
		action.Status.Status = storkv1.ActionStatusInitial
		action.Status.FinishTimestamp = metav1.Now()
		ac.updateAction(action)
		return
	}

	remoteOps, err := storkops.NewForConfig(sourceConfig)
	if err != nil {
		msg := fmt.Sprintf("Failed to create the stork client to access cluster with the config Host : %v", sourceConfig.Host)
		logEvents := ac.printFunc(action, string(storkv1.ActionStatusFailed))
		logEvents(msg, "err")
		action.Status.Status = storkv1.ActionStatusFailed
		action.Status.Reason = msg
		ac.updateAction(action)
		return
	}
	srcMigrSched, err := remoteOps.GetMigrationSchedule(action.Spec.ActionParameter.FailoverParameter.MigrationScheduleReference, action.Namespace)
	if err != nil {
		msg := fmt.Sprintf("Unable to find MigrationSchedule %s/%s for failover. Skipping MigrationSchedule suspension : %v", migrationSchedule.Namespace, migrationSchedule.Name, err)
		log.ActionLog(action).Warnf(msg)
		ac.recorder.Event(action,
			v1.EventTypeWarning,
			string(storkv1.ActionStatusSuccessful),
			msg)
		action.Status.Status = storkv1.ActionStatusSuccessful
		action.Status.Reason = ""
		ac.updateAction(action)
		return
	}

	_, latestMigration := getLatestMigrationPolicyAndStatus(*srcMigrSched)
	// Suspend the migration schedule in the source cluster
	suspend := true
	if !*srcMigrSched.Spec.Suspend {
		srcMigrSched.Spec.Suspend = &suspend
		msg := fmt.Sprintf("Suspending migration schedule %s/%s before doing the failover operation", srcMigrSched.Namespace, srcMigrSched.Name)
		log.ActionLog(action).Infof(msg)
		_, err := remoteOps.UpdateMigrationSchedule(srcMigrSched)
		if err != nil {
			log.ActionLog(action).Errorf("Error suspending MigrationSchedule %s/%s: %v", srcMigrSched.Namespace, srcMigrSched.Name, err)
			return
		}
	}
	if latestMigration != nil {
		if !isMigrationComplete(latestMigration.Status) {
			// a migration is still in progress. failover needs to wait
			msg := fmt.Sprintf("Waiting for the completion of migration %s/%s", srcMigrSched.Namespace, latestMigration.Name)
			log.ActionLog(action).Infof(msg)
			ac.recorder.Event(action,
				v1.EventTypeWarning,
				string(storkv1.ActionStatusScheduled),
				msg)
			ac.updateAction(action)
			return
		} else if latestMigration.Status == storkv1.MigrationStatusFailed {
			// log a warning event that last migration was unsuccessful. We will still proceed with the failover
			msg := fmt.Sprintf("The latest migration %s/%s status is %s. Proceeding with the failover", srcMigrSched.Namespace, latestMigration.Name, latestMigration.Status)
			log.ActionLog(action).Infof(msg)
			ac.recorder.Event(action,
				v1.EventTypeWarning,
				string(storkv1.ActionStatusScheduled),
				msg)
			ac.updateAction(action)
		}
	}
	msg := fmt.Sprintf("Failover ActionStageInitial status %s", string(storkv1.ActionStatusSuccessful))
	log.ActionLog(action).Infof(msg)
	action.Status.Status = storkv1.ActionStatusSuccessful
	action.Status.Reason = ""
	ac.updateAction(action)
}

// deactivateClusterStage This method will be used in both failover and failback to deactivate apps in source/destination clusters respectively
func (ac *ActionController) deactivateClusterStage(action *storkv1.Action) {
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

	var migrationScheduleReference string
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
		msg := fmt.Sprintf("error getting migrationschedule %s/%s", action.Namespace, migrationScheduleReference)
		logEvents := ac.printFunc(action, string(storkv1.ActionStatusFailed))
		logEvents(msg, "err")
		action.Status.Status = storkv1.ActionStatusFailed
		action.Status.Reason = msg
		ac.updateAction(action)
		return
	}

	migrationNamespaces, err := utils.GetMergedNamespacesWithLabelSelector(migrationSchedule.Spec.Template.Spec.Namespaces, migrationSchedule.Spec.Template.Spec.NamespaceSelectors)
	if err != nil {
		msg := fmt.Sprintf("Failed to get list of namespaces from migrationschedule %s/%s", migrationSchedule.Namespace, migrationSchedule.Name)
		log.ActionLog(action).Errorf(msg)
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

	// get sourceConfig from clusterPair in destination cluster
	remoteConfig, err := getClusterPairSchedulerConfig(migrationSchedule.Spec.Template.Spec.ClusterPair, migrationSchedule.Namespace)
	if err != nil {
		msg := fmt.Sprintf("error getting clusterpair %s/%s", migrationSchedule.Namespace, migrationSchedule.Spec.Template.Spec.ClusterPair)
		logEvents := ac.printFunc(action, string(storkv1.ActionStatusFailed))
		logEvents(msg, "err")
		action.Status.Status = storkv1.ActionStatusFailed
		action.Status.Reason = msg
		ac.updateAction(action)
		return
	}

	// get destination i.e. current cluster's config
	clusterConfig := ac.config

	var activationClusterConfig *rest.Config
	var deactivationClusterConfig *rest.Config
	if action.Spec.ActionType == storkv1.ActionTypeFailover {
		activationClusterConfig = clusterConfig
		deactivationClusterConfig = remoteConfig
	} else if action.Spec.ActionType == storkv1.ActionTypeFailback {
		// in case of FailBack we deactivate apps in the destination/current cluster and activate in the source/remote cluster
		activationClusterConfig = remoteConfig
		deactivationClusterConfig = clusterConfig
	}

	// identify which resources to be scaled down by looking at which resources will be activated in the opposite cluster
	resourcesBeingActivatedMap, err := resourceutils.GetResourcesBeingActivated(actualNamespaces, activationClusterConfig)
	if err != nil {
		msg := fmt.Sprintf("Failed to identify resources to be scaled down: %v", err)
		logEvents := ac.printFunc(action, string(storkv1.ActionStatusFailed))
		logEvents(msg, "err")
		action.Status.Status = storkv1.ActionStatusFailed
		action.Status.Reason = msg
		ac.updateAction(action)
		return
	}
	// this method will scale down the resources which are being activated in the opposite cluster
	err = resourceutils.ScaleDownGivenResources(actualNamespaces, resourcesBeingActivatedMap, deactivationClusterConfig)
	if err != nil {
		msg := fmt.Sprintf("Failed to scale down replicas in cluster %v : %v", deactivationClusterConfig.Host, err)
		logEvents := ac.printFunc(action, string(storkv1.ActionStatusFailed))
		logEvents(msg, "err")
		action.Status.Status = storkv1.ActionStatusFailed
		action.Status.Reason = msg
		ac.updateAction(action)
		return
	}
	msg := fmt.Sprintf("Scaling down of applications in cluster : %s successful. Moving to the next stage", deactivationClusterConfig.Host)
	logEvents := ac.printFunc(action, string(storkv1.ActionStatusSuccessful))
	logEvents(msg, "out")
	action.Status.Status = storkv1.ActionStatusSuccessful
	action.Status.Reason = ""
	ac.updateAction(action)
}

// activateDuringFailover This method is used for both activation of destination and reactivation of source during failover
func (ac *ActionController) activateDuringFailover(action *storkv1.Action, reactivate bool) {
	// Move to Final stage if this stage succeeds or fails
	if action.Status.Status == storkv1.ActionStatusSuccessful || action.Status.Status == storkv1.ActionStatusFailed {
		action.Status.Stage = storkv1.ActionStageFinal
		if reactivate {
			// If the status is Successful or Failed, mark the failover operation failed
			action.Status.Status = storkv1.ActionStatusFailed
		}
		action.Status.FinishTimestamp = metav1.Now()
		ac.updateAction(action)
		return
	}

	var config *rest.Config
	namespaces := action.Spec.ActionParameter.FailoverParameter.FailoverNamespaces

	migrationSchedule, err := storkops.Instance().GetMigrationSchedule(action.Spec.ActionParameter.FailoverParameter.MigrationScheduleReference, action.Namespace)
	if err != nil {
		msg := fmt.Sprintf("error fetching migrationschedule %s/%s", action.Namespace, action.Spec.ActionParameter.FailoverParameter.MigrationScheduleReference)
		logEvents := ac.printFunc(action, string(storkv1.ActionStatusFailed))
		logEvents(msg, "err")
		action.Status.Status = storkv1.ActionStatusFailed
		action.Status.Reason = msg
		ac.updateAction(action)
		return
	}

	migrationNamespaces, err := utils.GetMergedNamespacesWithLabelSelector(migrationSchedule.Spec.Template.Spec.Namespaces, migrationSchedule.Spec.Template.Spec.NamespaceSelectors)
	if err != nil {
		msg := fmt.Sprintf("Failed to get list of namespaces from migrationschedule %s/%s", migrationSchedule.Namespace, migrationSchedule.Name)
		log.ActionLog(action).Errorf(msg)
		ac.recorder.Event(action,
			v1.EventTypeWarning,
			string(storkv1.ActionStatusScheduled),
			msg)
		ac.updateAction(action)
		return
	}

	if reactivate {
		// in case of reactivation you have to activate the apps which were scaled down in the source cluster i.e. remote cluster
		config, err = getClusterPairSchedulerConfig(migrationSchedule.Spec.Template.Spec.ClusterPair, migrationSchedule.Namespace)
		if err != nil {
			msg := fmt.Sprintf("Failed to get the remote config from the clusterpair %s", migrationSchedule.Spec.Template.Spec.ClusterPair)
			logEvents := ac.printFunc(action, string(storkv1.ActionStatusFailed))
			logEvents(msg, "err")
			action.Status.Status = storkv1.ActionStatusFailed
			action.Status.Reason = msg
			ac.updateAction(action)
			return
		}
	} else {
		config = ac.config
	}

	failoverSummaryList := make([]*storkv1.FailoverSummary, 0)
	scaleupStatus := true
	for _, ns := range namespaces {
		// scale replicas only if namespace is a subset of namespaces being migrated
		logEvents := ac.printFunc(action, "ScaleReplicas")
		logEvents(fmt.Sprintf("Scaling up apps in cluster %s", config.Host), "out")
		var failoverSummary *storkv1.FailoverSummary
		if slices.Contains(migrationNamespaces, ns) {
			err := resourceutils.ScaleReplicasReturningError(ns, true, false, ac.printFunc(action, "ScaleReplicas"), config)
			if err != nil {
				scaleupStatus = false
				msg := fmt.Sprintf("scaling up apps in namespace %s failed: %v", ns, err)
				log.ActionLog(action).Errorf(msg)
				failoverSummary = &storkv1.FailoverSummary{Namespace: ns, Status: storkv1.ActionStatusFailed, Reason: msg}
			}
			msg := fmt.Sprintf("scaling up apps in namespace %s successful", ns)
			failoverSummary = &storkv1.FailoverSummary{Namespace: ns, Status: storkv1.ActionStatusSuccessful, Reason: msg}
		} else {
			msg := fmt.Sprintf("skipping scaling up apps in namespace %s since it is not one of the namespaces being migrated by the MigrationSchedule %s/%s", ns, migrationSchedule.Namespace, migrationSchedule.Name)
			failoverSummary = &storkv1.FailoverSummary{Namespace: ns, Status: storkv1.ActionStatusSuccessful, Reason: msg}
		}
		failoverSummaryList = append(failoverSummaryList, failoverSummary)
	}

	if reactivate {
		// marking the stage's status as successful even if it hit error while activating apps up
		action.Status.Status = storkv1.ActionStatusSuccessful
		ac.updateAction(action)
		return
	}
	action.Status.Summary = &storkv1.ActionSummary{FailoverSummaryItem: failoverSummaryList}
	if scaleupStatus {
		msg := fmt.Sprintf("Scaling up of applications in cluster : %s successful. Moving to the next stage", config.Host)
		logEvents := ac.printFunc(action, string(storkv1.ActionStatusSuccessful))
		logEvents(msg, "out")
		action.Status.Status = storkv1.ActionStatusSuccessful
	} else {
		msg := fmt.Sprintf("Scaling up of applications in cluster : %s failed.", config.Host)
		logEvents := ac.printFunc(action, string(storkv1.ActionStatusFailed))
		logEvents(msg, "out")
		action.Status.Status = storkv1.ActionStatusFailed
	}
	ac.updateAction(action)
}

func (ac *ActionController) isClusterAccessible(action *storkv1.Action, config *rest.Config) bool {
	retryCount := 5
	waitInterval := 6 * time.Second
	action.Status.Status = storkv1.ActionStatusScheduled
	ac.updateAction(action)
	for i := retryCount; i > 0; i-- {
		coreClient, err := coreops.NewForConfig(config)
		if err != nil {
			log.ActionLog(action).Warnf("cluster accessibility test failed: %v. Number of retrys left %d ", err, i-1)
			time.Sleep(waitInterval)
			continue
		}
		// If the get k8s version call succeeds then we assume that the cluster is accessible
		k8sVersion, err := coreClient.GetVersion()
		if err != nil {
			log.ActionLog(action).Warnf("cluster accessibility test failed: %v. Number of retrys left %d ", err, i-1)
			time.Sleep(waitInterval)
			continue
		}
		msg := fmt.Sprintf("cluster accessibility test passed. k8s version of the remote cluster is %v", k8sVersion.String())
		logEvents := ac.printFunc(action, "RemoteClusterAccessibility")
		logEvents(msg, "out")
		return true
	}
	msg := fmt.Sprintf("cluster accessibility test failed. Unable to access the remote cluster : %v", config.Host)
	logEvents := ac.printFunc(action, "RemoteClusterAccessibility")
	logEvents(msg, "err")
	return false
}
