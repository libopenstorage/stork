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

// validateBeforeFailover is called as part of the Initial stage of Failover
func (ac *ActionController) validateBeforeFailover(action *storkv1.Action) {
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
		msg := fmt.Sprintf("Error fetching the MigrationSchedule %s/%s for Failover", action.Namespace, action.Spec.ActionParameter.FailoverParameter.MigrationScheduleReference)
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
		msg := fmt.Sprintf("Error fetching the ClusterPair %s/%s for Failover", migrationSchedule.Namespace, migrationSchedule.Spec.Template.Spec.ClusterPair)
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
		msg := fmt.Sprintf("Error fetching the MigrationSchedule %s/%s for Failover in the %s cluster. Skipping MigrationSchedule suspension : %v", sourceConfig.Host, migrationSchedule.Namespace, migrationSchedule.Name, err)
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
		msg := fmt.Sprintf("Suspending the MigrationSchedule %s/%s before proceeding with the Failover operation", srcMigrSched.Namespace, srcMigrSched.Name)
		log.ActionLog(action).Infof(msg)
		_, err := remoteOps.UpdateMigrationSchedule(srcMigrSched)
		if err != nil {
			log.ActionLog(action).Errorf("Error suspending the MigrationSchedule %s/%s: %v", srcMigrSched.Namespace, srcMigrSched.Name, err)
			return
		}
	}
	if latestMigration != nil {
		if !isMigrationComplete(latestMigration.Status) {
			// a migration is still in progress. failover needs to wait
			msg := fmt.Sprintf("Waiting for completion of the migration %s/%s", srcMigrSched.Namespace, latestMigration.Name)
			log.ActionLog(action).Infof(msg)
			ac.recorder.Event(action,
				v1.EventTypeWarning,
				string(storkv1.ActionStatusScheduled),
				msg)
			ac.updateAction(action)
			return
		} else if latestMigration.Status == storkv1.MigrationStatusFailed {
			// log a warning event that last migration was unsuccessful. We will still proceed with the failover
			msg := fmt.Sprintf("The latest migration %s/%s status is %s. Proceeding with the Failover", srcMigrSched.Namespace, latestMigration.Name, latestMigration.Status)
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

func (ac *ActionController) deactivateSourceDuringFailover(action *storkv1.Action) {
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

	migrationScheduleName := action.Spec.ActionParameter.FailoverParameter.MigrationScheduleReference
	namespaces := action.Spec.ActionParameter.FailoverParameter.FailoverNamespaces
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
	ac.deactivateClusterDuringDR(action, actualNamespaces, clusterConfig, remoteConfig)
}

// deactivateClusterStage will be used in both failover and failback to deactivate apps in source/destination clusters respectively
func (ac *ActionController) deactivateClusterDuringDR(action *storkv1.Action, namespaces []string, activationClusterConfig *rest.Config, deactivationClusterConfig *rest.Config) {
	// identify which resources to be scaled down by looking at which resources will be activated in the opposite cluster
	resourcesBeingActivatedMap := make(map[string]map[metav1.GroupVersionKind]map[string]string)
	for _, ns := range namespaces {
		resourcesBeingActivatedInNamespace, err := resourceutils.ScaleUpResourcesInNamespace(ns, true, activationClusterConfig)
		if err != nil {
			msg := fmt.Sprintf("Failed to identify resources to be scaled down: %v", err)
			logEvents := ac.printFunc(action, string(storkv1.ActionStatusFailed))
			logEvents(msg, "err")
			action.Status.Status = storkv1.ActionStatusFailed
			action.Status.Reason = msg
			ac.updateAction(action)
			return
		}
		resourcesBeingActivatedMap[ns] = resourcesBeingActivatedInNamespace
	}

	// this method will scale down the resources which are being activated in the opposite cluster
	err := resourceutils.ScaleDownGivenResources(namespaces, resourcesBeingActivatedMap, deactivationClusterConfig)
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

// activateClusterDuringFailover is used for both activation of destination and reactivation of source during failover
func (ac *ActionController) activateClusterDuringFailover(action *storkv1.Action, rollback bool) {
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
	namespaces := action.Spec.ActionParameter.FailoverParameter.FailoverNamespaces

	migrationSchedule, err := storkops.Instance().GetMigrationSchedule(action.Spec.ActionParameter.FailoverParameter.MigrationScheduleReference, action.Namespace)
	if err != nil {
		msg := fmt.Sprintf("Error fetching the MigrationSchedule %s/%s", action.Namespace, action.Spec.ActionParameter.FailoverParameter.MigrationScheduleReference)
		logEvents := ac.printFunc(action, string(storkv1.ActionStatusFailed))
		logEvents(msg, "err")
		action.Status.Status = storkv1.ActionStatusFailed
		action.Status.Reason = msg
		ac.updateAction(action)
		return
	}
	if rollback {
		// in case of reactivation/rollback you have to activate the apps which were scaled down in the source cluster i.e. remote cluster
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
	} else {
		config = ac.config
	}
	ac.activateClusterDuringDR(action, namespaces, migrationSchedule, config)
}

func (ac *ActionController) activateClusterDuringDR(action *storkv1.Action, namespaces []string, migrationSchedule *storkv1.MigrationSchedule, config *rest.Config) {
	failoverSummaryList := make([]*storkv1.FailoverSummary, 0)
	scaleUpStatus := true

	// we want to scale replicas only if the activation namespace is a subset of namespaces being migrated
	migrationNamespaces, err := utils.GetMergedNamespacesWithLabelSelector(migrationSchedule.Spec.Template.Spec.Namespaces, migrationSchedule.Spec.Template.Spec.NamespaceSelectors)
	if err != nil {
		msg := fmt.Sprintf("Failed to fetch list of namespaces from the MigrationSchedule %s/%s", migrationSchedule.Namespace, migrationSchedule.Name)
		log.ActionLog(action).Errorf(msg)
		ac.recorder.Event(action,
			v1.EventTypeWarning,
			string(storkv1.ActionStatusScheduled),
			msg)
		ac.updateAction(action)
		return
	}

	for _, ns := range namespaces {
		logEvents := ac.printFunc(action, "ScaleReplicas")
		logEvents(fmt.Sprintf("Scaling up apps in cluster %s", config.Host), "out")
		var failoverSummary *storkv1.FailoverSummary
		if slices.Contains(migrationNamespaces, ns) {
			_, err := resourceutils.ScaleUpResourcesInNamespace(ns, false, config)
			if err != nil {
				scaleUpStatus = false
				msg := fmt.Sprintf("scaling up apps in namespace %s failed: %v", ns, err)
				log.ActionLog(action).Errorf(msg)
				failoverSummary = &storkv1.FailoverSummary{Namespace: ns, Status: storkv1.ActionStatusFailed, Reason: msg}
			} else {
				msg := fmt.Sprintf("scaling up apps in namespace %s successful", ns)
				failoverSummary = &storkv1.FailoverSummary{Namespace: ns, Status: storkv1.ActionStatusSuccessful, Reason: msg}
			}
		} else {
			msg := fmt.Sprintf("Skipping scaling up apps in the namespace %s since it is not one of the namespaces being migrated by the MigrationSchedule %s/%s", ns, migrationSchedule.Namespace, migrationSchedule.Name)
			failoverSummary = &storkv1.FailoverSummary{Namespace: ns, Status: storkv1.ActionStatusSuccessful, Reason: msg}
		}
		failoverSummaryList = append(failoverSummaryList, failoverSummary)
	}

	//TODO: Also mark the relevant MigrationSchedule's applicationActivated to true

	action.Status.Summary = &storkv1.ActionSummary{FailoverSummaryItem: failoverSummaryList}
	if scaleUpStatus {
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
			log.ActionLog(action).Warnf("Cluster Accessibility test failed: %v. Number of retrys left %d ", err, i-1)
			time.Sleep(waitInterval)
			continue
		}
		// If the get k8s version call succeeds then we assume that the cluster is accessible
		k8sVersion, err := coreClient.GetVersion()
		if err != nil {
			log.ActionLog(action).Warnf("Cluster Accessibility test failed: %v. Number of retrys left %d ", err, i-1)
			time.Sleep(waitInterval)
			continue
		}
		msg := fmt.Sprintf("Cluster Accessibility test passed. K8s version of the cluster : %s is %v", config.Host, k8sVersion.String())
		logEvents := ac.printFunc(action, "RemoteClusterAccessibility")
		logEvents(msg, "out")
		return true
	}
	msg := fmt.Sprintf("Cluster Accessibility test failed. Unable to access the remote cluster : %v", config.Host)
	logEvents := ac.printFunc(action, "RemoteClusterAccessibility")
	logEvents(msg, "err")
	return false
}
