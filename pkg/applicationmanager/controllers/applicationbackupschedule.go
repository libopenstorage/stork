package controllers

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/libopenstorage/stork/pkg/apis/stork"
	stork_api "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/libopenstorage/stork/pkg/controller"
	"github.com/libopenstorage/stork/pkg/log"
	"github.com/libopenstorage/stork/pkg/schedule"
	"github.com/operator-framework/operator-sdk/pkg/sdk"
	"github.com/portworx/sched-ops/k8s"
	"k8s.io/api/core/v1"
	apiextensionsv1beta1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1beta1"
	"k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/record"
)

const (
	nameTimeSuffixFormat string = "2006-01-02-150405"

	annotationPrefix = "stork.libopenstorage.org/"
	// ApplicationBackupScheduleNameLabel Label used to specify the name of schedule that
	// created the backup
	ApplicationBackupScheduleNameLabel = annotationPrefix + "applicationBackupScheduleName"
	// ApplicationBackupSchedulePolicyTypeLabel Label used to specify the type of the
	// policy that triggered the backup
	ApplicationBackupSchedulePolicyTypeLabel = annotationPrefix + "applicationBackupSchedulePolicyType"
)

// ApplicationBackupScheduleController reconciles ApplicationBackupSchedule objects
type ApplicationBackupScheduleController struct {
	Recorder record.EventRecorder
}

// Init Initialize the backup schedule controller
func (s *ApplicationBackupScheduleController) Init() error {
	err := s.createCRD()
	if err != nil {
		return err
	}
	return controller.Register(
		&schema.GroupVersionKind{
			Group:   stork.GroupName,
			Version: stork_api.SchemeGroupVersion.Version,
			Kind:    reflect.TypeOf(stork_api.ApplicationBackupSchedule{}).Name(),
		},
		"",
		1*time.Minute,
		s)
}

// Handle updates for ApplicationBackupSchedule objects
func (s *ApplicationBackupScheduleController) Handle(ctx context.Context, event sdk.Event) error {
	switch o := event.Object.(type) {
	case *stork_api.ApplicationBackupSchedule:
		backupSchedule := o
		// Nothing to do for delete
		if event.Deleted {
			return nil
		}

		s.setDefaults(backupSchedule)
		// First update the status of any pending backups
		err := s.updateApplicationBackupStatus(backupSchedule)
		if err != nil {
			msg := fmt.Sprintf("Error updating backup status: %v", err)
			s.Recorder.Event(backupSchedule,
				v1.EventTypeWarning,
				string(stork_api.ApplicationBackupStatusFailed),
				msg)
			log.ApplicationBackupScheduleLog(backupSchedule).Error(msg)
			return err
		}

		if backupSchedule.Spec.Suspend == nil || !*backupSchedule.Spec.Suspend {
			// Then check if any of the policies require a trigger
			policyType, start, err := s.shouldStartApplicationBackup(backupSchedule)
			if err != nil {
				msg := fmt.Sprintf("Error checking if backup should be triggered: %v", err)
				s.Recorder.Event(backupSchedule,
					v1.EventTypeWarning,
					string(stork_api.ApplicationBackupStatusFailed),
					msg)
				log.ApplicationBackupScheduleLog(backupSchedule).Error(msg)
				return err
			}

			// Start a backup for a policy if required
			if start {
				err := s.startApplicationBackup(backupSchedule, policyType)
				if err != nil {
					msg := fmt.Sprintf("Error triggering backup for schedule(%v): %v", policyType, err)
					s.Recorder.Event(backupSchedule,
						v1.EventTypeWarning,
						string(stork_api.ApplicationBackupStatusFailed),
						msg)
					log.ApplicationBackupScheduleLog(backupSchedule).Error(msg)
					return err
				}
			}
		}

		// Finally, prune any old backups that were triggered for this
		// schedule
		err = s.pruneApplicationBackups(backupSchedule)
		if err != nil {
			msg := fmt.Sprintf("Error pruning old backups: %v", err)
			s.Recorder.Event(backupSchedule,
				v1.EventTypeWarning,
				string(stork_api.ApplicationBackupStatusFailed),
				msg)
			log.ApplicationBackupScheduleLog(backupSchedule).Error(msg)
			return err
		}
	}
	return nil
}

func (s *ApplicationBackupScheduleController) setDefaults(backupSchedule *stork_api.ApplicationBackupSchedule) {
	if backupSchedule.Spec.ReclaimPolicy == "" {
		backupSchedule.Spec.ReclaimPolicy = stork_api.ReclaimPolicyRetain
	}
}

func getApplicationBackupStatus(name string, namespace string) (stork_api.ApplicationBackupStatusType, error) {
	backup, err := k8s.Instance().GetApplicationBackup(name, namespace)
	if err != nil {
		return stork_api.ApplicationBackupStatusFailed, err
	}

	return backup.Status.Status, nil
}

func (s *ApplicationBackupScheduleController) updateApplicationBackupStatus(backupSchedule *stork_api.ApplicationBackupSchedule) error {
	updated := false
	for _, policyApplicationBackup := range backupSchedule.Status.Items {
		for _, backup := range policyApplicationBackup {
			// Get the updated status if we see it as not completed
			if !s.isApplicationBackupComplete(backup.Status) {
				pendingApplicationBackupStatus, err := getApplicationBackupStatus(backup.Name, backupSchedule.Namespace)
				if err != nil {
					s.Recorder.Event(backupSchedule,
						v1.EventTypeWarning,
						string(stork_api.ApplicationBackupStatusFailed),
						fmt.Sprintf("Error getting status of backup %v: %v", backup.Name, err))
				}

				// Check again and update the status if it is completed
				backup.Status = pendingApplicationBackupStatus
				if s.isApplicationBackupComplete(backup.Status) {
					backup.FinishTimestamp = meta.NewTime(schedule.GetCurrentTime())
					if pendingApplicationBackupStatus == stork_api.ApplicationBackupStatusSuccessful {
						s.Recorder.Event(backupSchedule,
							v1.EventTypeNormal,
							string(stork_api.ApplicationBackupStatusSuccessful),
							fmt.Sprintf("Scheduled backup (%v) completed successfully", backup.Name))
					} else {
						s.Recorder.Event(backupSchedule,
							v1.EventTypeWarning,
							string(stork_api.ApplicationBackupStatusFailed),
							fmt.Sprintf("Scheduled backup (%v) failed", backup.Name))
					}
				}
				updated = true
			}
		}
	}
	if updated {
		err := sdk.Update(backupSchedule)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *ApplicationBackupScheduleController) isApplicationBackupComplete(status stork_api.ApplicationBackupStatusType) bool {
	return status == stork_api.ApplicationBackupStatusFailed ||
		status == stork_api.ApplicationBackupStatusPartialSuccess ||
		status == stork_api.ApplicationBackupStatusSuccessful
}

func (s *ApplicationBackupScheduleController) shouldStartApplicationBackup(backupSchedule *stork_api.ApplicationBackupSchedule) (stork_api.SchedulePolicyType, bool, error) {
	// Don't trigger a new backup if one is already in progress
	for _, policyType := range stork_api.GetValidSchedulePolicyTypes() {
		policyApplicationBackup, present := backupSchedule.Status.Items[policyType]
		if present {
			for _, backup := range policyApplicationBackup {
				if !s.isApplicationBackupComplete(backup.Status) {
					return stork_api.SchedulePolicyTypeInvalid, false, nil
				}
			}
		}
	}

	for _, policyType := range stork_api.GetValidSchedulePolicyTypes() {
		var latestApplicationBackupTimestamp meta.Time
		policyApplicationBackup, present := backupSchedule.Status.Items[policyType]
		if present {
			for _, backup := range policyApplicationBackup {
				if latestApplicationBackupTimestamp.Before(&backup.CreationTimestamp) {
					latestApplicationBackupTimestamp = backup.CreationTimestamp
				}
			}
		}
		trigger, err := schedule.TriggerRequired(
			backupSchedule.Spec.SchedulePolicyName,
			policyType,
			latestApplicationBackupTimestamp,
		)
		if err != nil {
			return stork_api.SchedulePolicyTypeInvalid, false, err
		}
		if trigger {
			return policyType, true, nil
		}
	}
	return stork_api.SchedulePolicyTypeInvalid, false, nil
}

func (s *ApplicationBackupScheduleController) formatApplicationBackupName(backupSchedule *stork_api.ApplicationBackupSchedule, policyType stork_api.SchedulePolicyType) string {
	return strings.Join([]string{backupSchedule.Name, strings.ToLower(string(policyType)), time.Now().Format(nameTimeSuffixFormat)}, "-")
}

func (s *ApplicationBackupScheduleController) startApplicationBackup(backupSchedule *stork_api.ApplicationBackupSchedule, policyType stork_api.SchedulePolicyType) error {
	backupName := s.formatApplicationBackupName(backupSchedule, policyType)
	if backupSchedule.Status.Items == nil {
		backupSchedule.Status.Items = make(map[stork_api.SchedulePolicyType][]*stork_api.ScheduledApplicationBackupStatus)
	}
	if backupSchedule.Status.Items[policyType] == nil {
		backupSchedule.Status.Items[policyType] = make([]*stork_api.ScheduledApplicationBackupStatus, 0)
	}
	backupSchedule.Status.Items[policyType] = append(backupSchedule.Status.Items[policyType],
		&stork_api.ScheduledApplicationBackupStatus{
			Name:              backupName,
			CreationTimestamp: meta.NewTime(schedule.GetCurrentTime()),
			Status:            stork_api.ApplicationBackupStatusPending,
		})
	err := sdk.Update(backupSchedule)
	if err != nil {
		return err
	}

	backup := &stork_api.ApplicationBackup{
		ObjectMeta: meta.ObjectMeta{
			Name:        backupName,
			Namespace:   backupSchedule.Namespace,
			Annotations: backupSchedule.Annotations,
			Labels:      backupSchedule.Labels,
		},
		Spec: backupSchedule.Spec.Template.Spec,
	}
	if backup.Labels == nil {
		backup.Labels = make(map[string]string)
	}
	backup.Labels[ApplicationBackupScheduleNameLabel] = backupSchedule.Name
	backup.Labels[ApplicationBackupSchedulePolicyTypeLabel] = string(policyType)

	log.ApplicationBackupScheduleLog(backupSchedule).Infof("Starting backup %v", backupName)
	// If reclaim policy is set to Delete, this will delete the backups
	// created by this backupschedule when the schedule object is deleted
	if backupSchedule.Spec.ReclaimPolicy == stork_api.ReclaimPolicyDelete {
		backup.OwnerReferences = []meta.OwnerReference{
			{
				Name:       backupSchedule.Name,
				UID:        backupSchedule.UID,
				Kind:       backupSchedule.GetObjectKind().GroupVersionKind().Kind,
				APIVersion: backupSchedule.GetObjectKind().GroupVersionKind().GroupVersion().String(),
			},
		}
	}
	_, err = k8s.Instance().CreateApplicationBackup(backup)
	return err
}

func (s *ApplicationBackupScheduleController) pruneApplicationBackups(backupSchedule *stork_api.ApplicationBackupSchedule) error {
	for policyType, policyApplicationBackup := range backupSchedule.Status.Items {
		numApplicationBackups := len(policyApplicationBackup)
		deleteBefore := 0
		retainNum, err := schedule.GetRetain(backupSchedule.Spec.SchedulePolicyName, policyType)
		if err != nil {
			return err
		}
		numReady := 0

		// Keep up to retainNum successful backup statuses and all failed backups
		// until there is a successful one
		if numApplicationBackups > int(retainNum) {
			// Start from the end and find the retainNum successful backups
			for i := range policyApplicationBackup {
				if policyApplicationBackup[(numApplicationBackups-1-i)].Status == stork_api.ApplicationBackupStatusSuccessful {
					numReady++
					if numReady > int(retainNum) {
						deleteBefore = numApplicationBackups - i
						break
					}
				}
			}
			failedDeletes := make([]*stork_api.ScheduledApplicationBackupStatus, 0)
			if numReady > int(retainNum) {
				for i := 0; i < deleteBefore; i++ {
					err := k8s.Instance().DeleteApplicationBackup(policyApplicationBackup[i].Name, backupSchedule.Namespace)
					if err != nil && !errors.IsNotFound(err) {
						log.ApplicationBackupScheduleLog(backupSchedule).Warnf("Error deleting %v: %v", policyApplicationBackup[i].Name, err)
						// Keep a track of the failed deletes
						failedDeletes = append(failedDeletes, policyApplicationBackup[i])
					}
				}
			}
			// Remove all the ones we tried to delete above
			backupSchedule.Status.Items[policyType] = policyApplicationBackup[deleteBefore:]
			// And re-add the ones that failed so that we don't lose track
			// of them
			backupSchedule.Status.Items[policyType] = append(failedDeletes, backupSchedule.Status.Items[policyType]...)
		}
	}
	return sdk.Update(backupSchedule)
}

func (s *ApplicationBackupScheduleController) createCRD() error {
	resource := k8s.CustomResource{
		Name:    stork_api.ApplicationBackupScheduleResourceName,
		Plural:  stork_api.ApplicationBackupScheduleResourcePlural,
		Group:   stork.GroupName,
		Version: stork_api.SchemeGroupVersion.Version,
		Scope:   apiextensionsv1beta1.NamespaceScoped,
		Kind:    reflect.TypeOf(stork_api.ApplicationBackupSchedule{}).Name(),
	}
	err := k8s.Instance().CreateCRD(resource)
	if err != nil && !errors.IsAlreadyExists(err) {
		return err
	}

	return k8s.Instance().ValidateCRD(resource, validateCRDTimeout, validateCRDInterval)
}
