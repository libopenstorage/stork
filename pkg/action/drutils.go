package action

import (
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/libopenstorage/openstorage/api"
	"github.com/libopenstorage/stork/pkg/resourceutils"

	storkv1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	storkops "github.com/libopenstorage/stork/pkg/crud/stork"
	"github.com/libopenstorage/stork/pkg/log"
	migration "github.com/libopenstorage/stork/pkg/migration/controllers"
	"github.com/libopenstorage/stork/pkg/utils"
	"github.com/pborman/uuid"
	"github.com/portworx/sched-ops/k8s/core"
	"github.com/portworx/sched-ops/task"
	"github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	k8sErrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

type DRKind string

const (
	asyncDR DRKind = "async-dr"
	syncDR  DRKind = "sync-dr"
	invalid DRKind = ""
)

// --- Common methods for Last-Mile Migration stages ---
func (ac *ActionController) createLastMileMigration(action *storkv1.Action, config *rest.Config, migrationSchedule *storkv1.MigrationSchedule) {
	storkClient, err := storkops.NewForConfig(config)
	if err != nil {
		msg := fmt.Sprintf("Creation of the Stork client for cluster access using the Host configuration %v has failed", config.Host)
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

			namespaces := make([]string, 0)
			if action.Spec.ActionType == storkv1.ActionTypeFailover {
				namespaces = action.Spec.ActionParameter.FailoverParameter.FailoverNamespaces
			} else if action.Spec.ActionType == storkv1.ActionTypeFailback {
				namespaces = action.Spec.ActionParameter.FailbackParameter.FailbackNamespaces
			}

			// only consider namespaces which are a part of both namespaces and migrationNamespaces
			// this means if there are some invalid namespaces provided for failover/failback we will ignore them
			_, actualNamespaces, _ := utils.IsSubList(namespaces, migrationNamespaces)

			lastMileMigration := getLastMileMigrationSpec(migrationSchedule, actualNamespaces, string(action.Spec.ActionType), utils.GetShortUID(string(action.UID)))
			_, err = storkClient.CreateMigration(lastMileMigration)
			if err != nil {
				msg := fmt.Sprintf("Creating the last mile migration from MigrationSchedule %s/%s encountered an error: %v", migrationSchedule.GetNamespace(), migrationSchedule.GetName(), err)
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
			msg := fmt.Sprintf("Error encountered while fetching the last mile migration object %s: %v", migrationName, err)
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
			msg := fmt.Sprintf("Last mile migration %s successful", migrationObject.Name)
			logEvents := ac.printFunc(action, "LastMileMigration")
			logEvents(msg, "out")
			action.Status.Status = storkv1.ActionStatusSuccessful
		} else {
			msg := fmt.Sprintf("The %s operation is aborted because the last mile migration %s failed with status %s", action.Spec.ActionType, migrationObject.Name, migrationObject.Status.Status)
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

// getLastMileMigrationSpec will get a migration spec from the given migrationschedule spec
func getLastMileMigrationSpec(ms *storkv1.MigrationSchedule, namespaces []string, operation string, actionCRUID string) *storkv1.Migration {
	migrationName := getLastMileMigrationName(ms.GetName(), operation, actionCRUID)
	migrationSpec := ms.Spec.Template.Spec
	//Overwrite the namespaces being migrated as part of last mile migration to the namespaces being failed over/failed back
	migrationSpec.NamespaceSelectors = nil
	migrationSpec.Namespaces = namespaces
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

// --- Common methods for scaling up and scaling down apps

func (ac *ActionController) activateClusterDuringDR(action *storkv1.Action, namespaces []string, migrationSchedule *storkv1.MigrationSchedule, config *rest.Config, rollback bool) {
	failoverSummaryList := make([]*storkv1.FailoverSummary, 0)
	failbackSummaryList := make([]*storkv1.FailbackSummary, 0)
	scaleUpStatus := true

	// we want to scale replicas only if the activation namespace is a subset of namespaces being migrated
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

	failureStatus := storkv1.ActionStatusFailed
	successStatus := storkv1.ActionStatusSuccessful
	if rollback {
		failureStatus = storkv1.ActionStatusRollbackFailed
		successStatus = storkv1.ActionStatusRollbackSuccessful
	}

	namespacesSuccessfullyActivated := make([]string, 0)

	for _, ns := range namespaces {
		logEvents := ac.printFunc(action, "ScaleReplicas")
		logEvents(fmt.Sprintf("Scaling up apps in cluster %s", config.Host), "out")
		var failoverSummary *storkv1.FailoverSummary
		var failbackSummary *storkv1.FailbackSummary
		if slices.Contains(migrationNamespaces, ns) {
			_, err := resourceutils.ScaleUpResourcesInNamespace(ns, false, config)
			if err != nil {
				scaleUpStatus = false
				msg := fmt.Sprintf("scaling up apps in namespace %s failed: %v", ns, err)
				log.ActionLog(action).Errorf(msg)
				failoverSummary, failbackSummary = ac.createSummary(action, ns, failureStatus, msg)
			} else {
				msg := fmt.Sprintf("scaling up apps in namespace %s successful", ns)
				namespacesSuccessfullyActivated = append(namespacesSuccessfullyActivated, ns)
				failoverSummary, failbackSummary = ac.createSummary(action, ns, successStatus, msg)
			}
		} else {
			msg := fmt.Sprintf("Skipping scaling up apps in the namespace %s since it is not one of the namespaces being migrated by the MigrationSchedule %s/%s", ns, migrationSchedule.Namespace, migrationSchedule.Name)
			failoverSummary, failbackSummary = ac.createSummary(action, ns, successStatus, msg)
		}
		if action.Spec.ActionType == storkv1.ActionTypeFailover {
			failoverSummaryList = append(failoverSummaryList, failoverSummary)
		} else if action.Spec.ActionType == storkv1.ActionTypeFailback {
			failbackSummaryList = append(failbackSummaryList, failbackSummary)
		}
	}

	if action.Spec.ActionType == storkv1.ActionTypeFailover {
		action.Status.Summary = &storkv1.ActionSummary{FailoverSummaryItem: failoverSummaryList}
	} else if action.Spec.ActionType == storkv1.ActionTypeFailback {
		action.Status.Summary = &storkv1.ActionSummary{FailbackSummaryItem: failbackSummaryList}
	}

	if scaleUpStatus {
		// Update ApplicationActivated value in relevant migrationSchedules
		ac.updateApplicationActivatedInRelevantMigrationSchedules(action, config, namespacesSuccessfullyActivated, migrationNamespaces, true)
		msg := fmt.Sprintf("Scaling up of applications in cluster : %s successful. Moving to the next stage", config.Host)
		logEvents := ac.printFunc(action, string(successStatus))
		logEvents(msg, "out")
		action.Status.Status = storkv1.ActionStatusSuccessful
	} else {
		msg := fmt.Sprintf("Scaling up of applications in cluster : %s failed.", config.Host)
		logEvents := ac.printFunc(action, string(failureStatus))
		logEvents(msg, "out")
		action.Status.Status = storkv1.ActionStatusFailed
	}
	ac.updateAction(action)
}

// deactivateClusterDuringDR will be used in both failover and failback to deactivate apps in source/destination clusters respectively
func (ac *ActionController) deactivateClusterDuringDR(action *storkv1.Action, namespaces []string, migrationNamespaces []string, activationClusterConfig *rest.Config, deactivationClusterConfig *rest.Config) {
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
	// Update ApplicationActivated value in relevant migrationSchedules
	ac.updateApplicationActivatedInRelevantMigrationSchedules(action, deactivationClusterConfig, namespaces, migrationNamespaces, false)
	msg := fmt.Sprintf("Scaling down of applications in cluster : %s successful. Moving to the next stage", deactivationClusterConfig.Host)
	logEvents := ac.printFunc(action, string(storkv1.ActionStatusSuccessful))
	logEvents(msg, "out")
	action.Status.Status = storkv1.ActionStatusSuccessful
	action.Status.Reason = ""
	ac.updateAction(action)
}

// --- Common methods for Wait After ScaleDown stages ---
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

	if action.Status.Status == storkv1.ActionStatusInitial {
		action.Status.Status = storkv1.ActionStatusInProgress
		ac.updateAction(action)
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
		msg := fmt.Sprintf("Unable to fetch the MigrationSchedule %s/%s needed for %s operation", action.Namespace, migrationScheduleReference, action.Spec.ActionType)
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
			msg := fmt.Sprintf("Unable to fetch the ClusterPair %s/%s needed for %s operation", migrationSchedule.Namespace, migrationSchedule.Spec.Template.Spec.ClusterPair, action.Spec.ActionType)
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
		msg := fmt.Sprintf("Creation of the Stork client for cluster access using the Host configuration %v has failed", config.Host)
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
			msg := fmt.Sprintf("Unable to fetch the MigrationSchedule %s/%s needed for the %s operation.", migrationSchedule.Namespace, migrationSchedule.Name, action.Spec.ActionType)
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
		msg := fmt.Sprintf("No migration detected for MigrationSchedule %s/%s, resulting in the abortion of the %s operation.", referencedMigrationScheduleForLatestMigration.Namespace, referencedMigrationScheduleForLatestMigration.Name, action.Spec.ActionType)
		logEvents := ac.printFunc(action, string(storkv1.ActionStatusFailed))
		logEvents(msg, "err")
		action.Status.Status = storkv1.ActionStatusFailed
		action.Status.Reason = msg
		ac.updateAction(action)
		return
	}

	if !isMigrationSuccessful(latestMigration.Status) {
		msg := fmt.Sprintf("MigrationSchedule %s/%s's latest migration status is %s. Migration did not complete successfully, so the %s operation is aborted", referencedMigrationScheduleForLatestMigration.Namespace, referencedMigrationScheduleForLatestMigration.Name, latestMigration.Status, action.Spec.ActionType)
		logEvents := ac.printFunc(action, string(storkv1.ActionStatusFailed))
		logEvents(msg, "err")
		action.Status.Status = storkv1.ActionStatusFailed
		action.Status.Reason = msg
		ac.updateAction(action)
		return
	}

	migrationObject, err := storkClient.GetMigration(latestMigration.Name, migrationSchedule.Namespace)
	if err != nil {
		msg := fmt.Sprintf("Failed to get the latest migration %s in MigrationSchedule %s/%s: %v", latestMigration.Name, migrationSchedule.Namespace, migrationSchedule.Name, err)
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

	msg := fmt.Sprintf("Successful scale down of pods using PVCs in cluster %v. Proceeding to the next stage", config.Host)
	logEvents := ac.printFunc(action, string(storkv1.ActionStatusSuccessful))
	logEvents(msg, "out")
	action.Status.Status = storkv1.ActionStatusSuccessful
	action.Status.Reason = ""
	ac.updateAction(action)
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

// checkForPodStatusUsingPVC returns terminated, terminationInProgress, error
// terminated: true if all pods using PVC are terminated
// terminationInProgress: true if any pod using PVC is not terminated
// error: error if any error occurs
//
// It also checks only the pods that are NOT in succeeded state.
// Reason: In some cases, the pod created by the job was in Completed state and was not deleted due
// to which the PVC was still in Bound state with the pod.
// This made stork wait for the pod to get terminated so that the volume can be freed and
// thus, failover can be completed but instead it timed out waiting for this.
// As a fix, we should skip considering the pods in Completed state for waiting for source deactivation in failover.
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
				activePods := []v1.Pod{}
				for _, pod := range pods {
					if pod.Status.Phase != v1.PodSucceeded {
						logrus.Debugf("Skipping checking the PVC status of pod [%s/%s] since it has succeeded", pod.Namespace, pod.Name)
						activePods = append(activePods, pod)
					}
				}
				if len(activePods) == 0 {
					// All pods terminated for atlease one pvc
					terminationInProgress = true
				} else {
					// Atleast one pvc has not been freed up
					terminated = false
				}
				nsToPVCToNumberOfPods[ns][pvcName] = len(activePods)
			}
		}
	}

	logrus.Infof("Current status of namespace to pvc to number of pods map: %v, terminated: %v, terminationInProgress: %v", nsToPVCToNumberOfPods, terminated, terminationInProgress)
	return terminated, terminationInProgress, nil
}

// --- Common methods related to metro-dr use cases ---

// remoteClusterDomainUpdate updates the remote cluster domain.
// If activate is true, it activates the domain. If activate is false, it deactivates the domain.
// The method returns an error if it fails to get cluster domain info or if activate/deactivate call fails.
func (ac *ActionController) remoteClusterDomainUpdate(activate bool, action *storkv1.Action) error {
	currentClusterDomains, err := ac.volDriver.GetClusterDomains()
	if err != nil {
		return fmt.Errorf("failed to get cluster domain info: %v", err)
	}
	domainToNodesMap, err := ac.volDriver.GetClusterDomainNodes()
	if err != nil {
		return fmt.Errorf("failed to get cluster domain nodes info: %v", err)
	}

	log.ActionLog(action).Infof("Count of cluster domains: %v ; Current cluster domains: %v", len(currentClusterDomains.ClusterDomainInfos), currentClusterDomains)

	remoteDomainName := ac.getRemoteNodeName(action, currentClusterDomains, domainToNodesMap)
	if remoteDomainName != "" {
		cduName := uuid.New()
		clusterDomainUpdate := &storkv1.ClusterDomainUpdate{
			ObjectMeta: metav1.ObjectMeta{
				Name: cduName,
			},
			Spec: storkv1.ClusterDomainUpdateSpec{
				ClusterDomain: remoteDomainName,
				Active:        activate,
			},
		}
		if activate {
			err := ac.volDriver.ActivateClusterDomain(clusterDomainUpdate)
			if err != nil {
				return fmt.Errorf("activation of cluster domain: %v failed : %v", remoteDomainName, err)
			} else {
				_, err := ac.waitForRemoteClusterDomainInSync(action, remoteDomainName)
				if err != nil {
					msg := fmt.Sprintf("Cluster domain: %v not in `%v` state.", remoteDomainName, storkv1.ClusterDomainSyncStatusInSync)
					logEvents := ac.printFunc(action, "ActivateClusterDomain")
					logEvents(msg, "err")
					return err
				}
				msg := fmt.Sprintf("Activation of cluster domain: %v successful", remoteDomainName)
				logEvents := ac.printFunc(action, "ActivateClusterDomain")
				logEvents(msg, "out")
			}
		} else {
			err := ac.volDriver.DeactivateClusterDomain(clusterDomainUpdate)
			if err != nil {
				return fmt.Errorf("deactivation of cluster domain: %v failed : %v", remoteDomainName, err)
			} else {
				msg := fmt.Sprintf("Deactivation of cluster domain: %v successful", remoteDomainName)
				logEvents := ac.printFunc(action, "DeactivateClusterDomain")
				logEvents(msg, "out")
			}
		}
	}
	return nil
}

func (ac *ActionController) waitForRemoteClusterDomainInSync(action *storkv1.Action, clusterDomainName string) (bool, error) {
	msg := fmt.Sprintf("Checking if clusterDomain %v is activated and InSync", clusterDomainName)
	logEvents := ac.printFunc(action, "ActivateClusterDomain")
	logEvents(msg, "out")
	f := func() (interface{}, bool, error) {
		currentClusterDomains, err := ac.volDriver.GetClusterDomains()
		if err != nil {
			return "", false, fmt.Errorf("failed to get cluster domain info: %v", err)
		}
		log.ActionLog(action).Infof("Count of cluster domains: %v ; Current cluster domains: %v", len(currentClusterDomains.ClusterDomainInfos), currentClusterDomains)
		for _, apiDomainInfo := range currentClusterDomains.ClusterDomainInfos {
			if apiDomainInfo.Name == clusterDomainName {
				if apiDomainInfo.State == storkv1.ClusterDomainActive && apiDomainInfo.SyncStatus == storkv1.ClusterDomainSyncStatusInSync {
					log.ActionLog(action).Infof("Cluster domain: %v is in %v state and status is %v ", clusterDomainName, apiDomainInfo.State, apiDomainInfo.SyncStatus)
					return "", false, nil
				} else {
					// we should retry
					return "", true, fmt.Errorf("cluster domain: %v is in %v state and status is %v ", clusterDomainName, apiDomainInfo.State, apiDomainInfo.SyncStatus)
				}
			}
		}
		return "", true, fmt.Errorf("failed to get cluster domain info for cluster domain : %v", clusterDomainName)
	}
	_, err := task.DoRetryWithTimeout(f, 10*time.Minute, 10*time.Second)
	if err != nil {
		return false, err
	}
	return true, nil
}

// getDRMode determine the mode of DR either from clusterPair or from the clusterDomains
func (ac *ActionController) getDRMode(clusterPairName string, namespace string) (DRKind, error) {
	clusterPair, cpErr := storkops.Instance().GetClusterPair(clusterPairName, namespace)
	if cpErr == nil {
		if len(clusterPair.Spec.Options) != 0 {
			return asyncDR, nil
		}
		return syncDR, nil
	}
	// since clusterPair get failed, we will fall back to seeing if clusterDomains are present or not
	currentClusterDomains, cdErr := ac.volDriver.GetClusterDomains()
	if cdErr != nil {
		return invalid, fmt.Errorf("unable to determine if the DR plan's mode is async-dr or sync-dr: %v ; %v", cpErr, cdErr)
	}
	if len(currentClusterDomains.ClusterDomainInfos) != 0 {
		return syncDR, nil
	}
	return asyncDR, nil
}

func (ac *ActionController) isClusterAccessible(action *storkv1.Action, config *rest.Config) bool {
	retryCount := 5
	waitInterval := 6 * time.Second
	action.Status.Status = storkv1.ActionStatusScheduled
	ac.updateAction(action)
	for i := retryCount; i > 0; i-- {
		coreClient, err := core.NewForConfig(config)
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
		msg := fmt.Sprintf("Cluster accessibility test succeeded. Kubernetes version of cluster %s is %s", config.Host, k8sVersion.String())
		logEvents := ac.printFunc(action, "RemoteClusterAccessibility")
		logEvents(msg, "out")
		return true
	}
	msg := fmt.Sprintf("Cluster accessibility test unsuccessful. Remote cluster %v is inaccessible.", config.Host)
	logEvents := ac.printFunc(action, "RemoteClusterAccessibility")
	logEvents(msg, "err")
	return false
}

func (ac *ActionController) updateApplicationActivatedInRelevantMigrationSchedules(action *storkv1.Action, config *rest.Config, namespaces []string, migrationNamespaces []string, value bool) {
	// This is best effort only. In case we encounter an error we just log events and logs. Action will not be failed.

	// Only subset of namespaces which are migrated by the reference migrationSchedule are activated / deactivated
	_, activationNamespaces, _ := utils.IsSubList(namespaces, migrationNamespaces)
	// relevant migrationSchedules are ones which are involved in migration of resources from at least one of the namespaces being activated.
	// such migrationSchedules can reside in any of the activationNamespaces or in the adminNamespace
	adminNs := utils.GetAdminNamespace()
	migrationScheduleNamespaces := activationNamespaces
	if !slices.Contains(activationNamespaces, adminNs) {
		migrationScheduleNamespaces = append(migrationScheduleNamespaces, adminNs)
	}

	storkClient, err := storkops.NewForConfig(config)
	if err != nil {
		msg := fmt.Sprintf("Creation of the Stork client for cluster access using the Host configuration %v has failed", config.Host)
		logEvents := ac.printFunc(action, "ApplicationActivatedStatus")
		logEvents(msg, "err")
		action.Status.Status = storkv1.ActionStatusFailed
		ac.updateAction(action)
		return
	}

	migrationSchedules := new(storkv1.MigrationScheduleList)
	for _, ns := range migrationScheduleNamespaces {
		migrSchedules, err := storkClient.ListMigrationSchedules(ns)
		if err != nil {
			msg := fmt.Sprintf("Failed to fetch the list of migrationSchedules in the namespace %v", ns)
			logEvents := ac.printFunc(action, "ApplicationActivatedStatus")
			logEvents(msg, "err")
			ac.updateAction(action)
		}
		migrationSchedules.Items = append(migrationSchedules.Items, migrSchedules.Items...)
	}

	for _, migrSched := range migrationSchedules.Items {
		if migrSched.GetAnnotations() == nil || migrSched.GetAnnotations()[migration.StorkMigrationScheduleCopied] != "true" {
			// no need to update the applicationActivated filed in case the migrationSchedule is not a static copy
			continue
		}
		isMigrSchedRelevant, err := utils.DoesMigrationScheduleMigrateNamespaces(migrSched, activationNamespaces)
		if err != nil {
			msg := fmt.Sprintf("Failed to determine the list of namespaces migrated by the migrationSchedule %v/%v", migrSched.Namespace, migrSched.Name)
			logEvents := ac.printFunc(action, "ApplicationActivatedStatus")
			logEvents(msg, "err")
			ac.updateAction(action)
		}
		if isMigrSchedRelevant {
			migrSched.Status.ApplicationActivated = value
			_, err := storkClient.UpdateMigrationSchedule(&migrSched)
			if err != nil {
				msg := fmt.Sprintf("Failed to update ApplicationActivated field for the migrationSchedule %v/%v", migrSched.Namespace, migrSched.Name)
				logEvents := ac.printFunc(action, "ApplicationActivatedStatus")
				logEvents(msg, "err")
				ac.updateAction(action)
			}
			msg := fmt.Sprintf("ApplicationActivated status in MigrationSchedule %s/%s set to %v", migrSched.Namespace, migrSched.Name, value)
			logEvents := ac.printFunc(action, "ApplicationActivatedStatus")
			logEvents(msg, "out")
		}
	}
}

func (ac *ActionController) createSummary(action *storkv1.Action, ns string, status storkv1.ActionStatusType, msg string) (*storkv1.FailoverSummary, *storkv1.FailbackSummary) {
	var failoverSummary *storkv1.FailoverSummary
	var failbackSummary *storkv1.FailbackSummary
	switch action.Spec.ActionType {
	case storkv1.ActionTypeFailover:
		failoverSummary = &storkv1.FailoverSummary{Namespace: ns, Status: status, Reason: msg}
	case storkv1.ActionTypeFailback:
		failbackSummary = &storkv1.FailbackSummary{Namespace: ns, Status: status, Reason: msg}
	}
	return failoverSummary, failbackSummary
}

func getClusterPairSchedulerConfig(clusterPairName string, namespace string) (*rest.Config, error) {
	clusterPair, err := storkops.Instance().GetClusterPair(clusterPairName, namespace)
	if err != nil {
		return nil, fmt.Errorf("error getting ClusterPair (%v/%v): %v", namespace, clusterPairName, err)
	}
	remoteClientConfig := clientcmd.NewNonInteractiveClientConfig(
		clusterPair.Spec.Config,
		clusterPair.Spec.Config.CurrentContext,
		&clientcmd.ConfigOverrides{},
		clientcmd.NewDefaultClientConfigLoadingRules())
	return remoteClientConfig.ClientConfig()
}

func (ac *ActionController) getRemoteNodeName(action *storkv1.Action, clusterDomains *storkv1.ClusterDomains, domainToNodesMap map[string][]*api.Node) string {
	var remoteDomainName string
	for _, apiDomainInfo := range clusterDomains.ClusterDomainInfos {
		if apiDomainInfo.Name == clusterDomains.LocalDomain {
			continue
		}
		if domainToNodesMap[apiDomainInfo.Name] != nil && len(domainToNodesMap[apiDomainInfo.Name]) == 1 {
			log.ActionLog(action).Debugf("Skipping activation/deactivation of cluster domain: %v as it is the witness node", apiDomainInfo.Name)
			continue
		}
		remoteDomainName = apiDomainInfo.Name
		break
	}
	return remoteDomainName
}
