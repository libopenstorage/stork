package action

import (
	"fmt"
	storkv1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/libopenstorage/stork/pkg/log"
	migration "github.com/libopenstorage/stork/pkg/migration/controllers"
	"github.com/libopenstorage/stork/pkg/utils"
	"github.com/pborman/uuid"
	storkops "github.com/portworx/sched-ops/k8s/stork"
	v1 "k8s.io/api/core/v1"
	k8sErrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
	"slices"
)

type DRKind string

const (
	asyncDR DRKind = "async-dr"
	syncDR  DRKind = "sync-dr"
	invalid DRKind = ""
)

func (ac *ActionController) createLastMileMigration(action *storkv1.Action, config *rest.Config, migrationSchedule *storkv1.MigrationSchedule) {
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
			msg := fmt.Sprintf("Last mile migration %s successful", migrationObject.Name)
			logEvents := ac.printFunc(action, "LastMileMigration")
			logEvents(msg, "out")
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

// remoteClusterDomainUpdate updates the remote cluster domain.
// If activate is true, it activates the domain. If activate is false, it deactivates the domain.
// The method returns an error if it fails to get cluster domain info or if activate/deactivate call fails.
func (ac *ActionController) remoteClusterDomainUpdate(activate bool, action *storkv1.Action) error {
	currentClusterDomains, err := ac.volDriver.GetClusterDomains()
	if err != nil {
		return fmt.Errorf("failed to get cluster domain info: %v", err)
	}
	log.ActionLog(action).Infof("Count of cluster domains: %v ; Current cluster domains: %v", len(currentClusterDomains.ClusterDomainInfos), currentClusterDomains)

	var remoteDomainName string
	for _, apiDomainInfo := range currentClusterDomains.ClusterDomainInfos {
		if apiDomainInfo.Name == currentClusterDomains.LocalDomain {
			continue
		}
		remoteDomainName = apiDomainInfo.Name
		break
	}

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
		msg := fmt.Sprintf("Failed to create the stork client to access cluster with the config %v", config.Host)
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
			msg := fmt.Sprintf("Setting the ApplicationActivated status in the MigrationSchedule %v/%v to %v", migrSched.Namespace, migrSched.Name, value)
			logEvents := ac.printFunc(action, "ApplicationActivatedStatus")
			logEvents(msg, "out")
		}
	}
}
