package controllers

import (
	"context"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	stork_api "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/libopenstorage/stork/pkg/controllers"
	"github.com/libopenstorage/stork/pkg/k8sutils"
	"github.com/libopenstorage/stork/pkg/log"
	"github.com/libopenstorage/stork/pkg/objectstore"
	"github.com/libopenstorage/stork/pkg/schedule"
	"github.com/libopenstorage/stork/pkg/utils"
	"github.com/libopenstorage/stork/pkg/version"
	"github.com/portworx/sched-ops/k8s/apiextensions"
	storkops "github.com/portworx/sched-ops/k8s/stork"
	"github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	apiextensionsv1beta1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1beta1"
	"k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	runtimeclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const (
	nameTimeSuffixFormat string = "2006-01-02-150405"

	annotationPrefix = "stork.libopenstorage.org/"
	// ApplicationBackupScheduleNameAnnotation Annotation used to specify the name of schedule that
	// created the backup
	ApplicationBackupScheduleNameAnnotation = annotationPrefix + "applicationBackupScheduleName"
	// ApplicationBackupSchedulePolicyTypeAnnotation Annotation used to specify the type of the
	// policy that triggered the backup
	ApplicationBackupSchedulePolicyTypeAnnotation = annotationPrefix + "applicationBackupSchedulePolicyType"
	// ApplicationBackupObjectLockRetentionAnnotation - object lock retention period annotation
	// Since this annotation is used in the px-backup, creating with portworx.io annotation prefix.
	ApplicationBackupObjectLockRetentionAnnotation = "portworx.io/" + "object-lock-retention-period"
	dayInSec                                       = 86400
	//ObjectLockDefaultIncrementalCount default incremental backup count
	ObjectLockDefaultIncrementalCount = 5
	backupTypeKey                     = "portworx.io/backup-type"
	genericBackupTypeValue            = "Generic"
)

// NewApplicationBackupSchedule creates a new instance of ApplicationBackupScheduleController.
func NewApplicationBackupSchedule(mgr manager.Manager, r record.EventRecorder) *ApplicationBackupScheduleController {
	return &ApplicationBackupScheduleController{
		client:   mgr.GetClient(),
		recorder: r,
	}
}

// ApplicationBackupScheduleController reconciles ApplicationBackupSchedule objects
type ApplicationBackupScheduleController struct {
	client runtimeclient.Client

	recorder record.EventRecorder
}

// Init Initialize the backup schedule controller
func (s *ApplicationBackupScheduleController) Init(mgr manager.Manager) error {
	err := s.createCRD()
	if err != nil {
		return err
	}

	return controllers.RegisterTo(mgr, "application-backup-schedule-controller", s, &stork_api.ApplicationBackupSchedule{})
}

// Reconcile updates for ApplicationBackupSchedule objects.
func (s *ApplicationBackupScheduleController) Reconcile(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
	logrus.Tracef("Reconciling ApplicationBackupSchedule %s/%s", request.Namespace, request.Name)

	// Fetch the ApplicationBackup instance
	backup := &stork_api.ApplicationBackupSchedule{}
	err := s.client.Get(context.TODO(), request.NamespacedName, backup)
	if err != nil {
		if errors.IsNotFound(err) {
			// Request object not found, could have been deleted after reconcile request.
			// Owned objects are automatically garbage collected. For additional cleanup logic use finalizers.
			// Return and don't requeue
			return reconcile.Result{}, nil
		}
		// Error reading the object - requeue the request.
		return reconcile.Result{RequeueAfter: controllers.DefaultRequeueError}, err
	}

	if err = s.handle(context.TODO(), backup); err != nil {
		logrus.Errorf("%s: %s/%s: %s", reflect.TypeOf(s), backup.Namespace, backup.Name, err)
		return reconcile.Result{RequeueAfter: controllers.DefaultRequeueError}, err
	}

	return reconcile.Result{RequeueAfter: controllers.DefaultRequeue}, nil
}

// Handle updates for ApplicationBackupSchedule objects
func (s *ApplicationBackupScheduleController) handle(_ context.Context, backupSchedule *stork_api.ApplicationBackupSchedule) error {
	if backupSchedule.DeletionTimestamp != nil {
		return nil
	}

	err := handleCSINametoCSIMapMigration(&backupSchedule.Spec.Template.Spec)
	if err != nil {
		log.ApplicationBackupScheduleLog(backupSchedule).Errorf("Error creating CSISnapshotMap: %v", err)
		s.recorder.Event(backupSchedule,
			v1.EventTypeWarning,
			string(stork_api.ApplicationBackupStatusFailed),
			err.Error())
		return nil
	}

	s.setDefaults(backupSchedule)
	// First update the status of any pending backups
	err = s.updateApplicationBackupStatus(backupSchedule)
	if err != nil {
		msg := fmt.Sprintf("Error updating backup status: %v", err)
		s.recorder.Event(backupSchedule,
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
			s.recorder.Event(backupSchedule,
				v1.EventTypeWarning,
				string(stork_api.ApplicationBackupStatusFailed),
				msg)
			log.ApplicationBackupScheduleLog(backupSchedule).Error(msg)
			return nil
		}
		// Start a backup for a policy if required
		if start {
			err := s.startApplicationBackup(backupSchedule, policyType)
			if err != nil {
				msg := fmt.Sprintf("Error triggering backup for schedule(%v): %v", policyType, err)
				s.recorder.Event(backupSchedule,
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
		s.recorder.Event(backupSchedule,
			v1.EventTypeWarning,
			string(stork_api.ApplicationBackupStatusFailed),
			msg)
		log.ApplicationBackupScheduleLog(backupSchedule).Error(msg)
		return err
	}

	return nil
}

func (s *ApplicationBackupScheduleController) setDefaults(backupSchedule *stork_api.ApplicationBackupSchedule) {
	if backupSchedule.Spec.ReclaimPolicy == "" {
		backupSchedule.Spec.ReclaimPolicy = stork_api.ReclaimPolicyRetain
	}
}

func getApplicationBackupStatus(name string, namespace string) (stork_api.ApplicationBackupStatusType, error) {
	backup, err := storkops.Instance().GetApplicationBackup(name, namespace)
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
					s.recorder.Event(backupSchedule,
						v1.EventTypeWarning,
						string(stork_api.ApplicationBackupStatusFailed),
						fmt.Sprintf("Error getting status of backup %v from namespace %s: %v", backup.Name, backupSchedule.Namespace, err))
					// If there was an error other than not found move to
					// the next one. Otherwise we want to mark it as failed
					// since the applicationbackup object is no longer
					// present
					if !errors.IsNotFound(err) {
						continue
					}
				}

				// Check again and update the status if it is completed
				backup.Status = pendingApplicationBackupStatus
				if s.isApplicationBackupComplete(backup.Status) {
					backup.FinishTimestamp = meta.NewTime(schedule.GetCurrentTime())
					if pendingApplicationBackupStatus == stork_api.ApplicationBackupStatusSuccessful {
						s.recorder.Event(backupSchedule,
							v1.EventTypeNormal,
							string(stork_api.ApplicationBackupStatusSuccessful),
							fmt.Sprintf("Scheduled backup (%v) completed successfully for namespace %s", backup.Name, backupSchedule.Namespace))
					} else {
						s.recorder.Event(backupSchedule,
							v1.EventTypeWarning,
							string(stork_api.ApplicationBackupStatusFailed),
							fmt.Sprintf("Scheduled backup (%v) failed for namespace %s", backup.Name, backupSchedule.Namespace))
					}
				}
				updated = true
			}
		}
	}
	if updated {
		err := s.client.Update(context.TODO(), backupSchedule)
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
			backupSchedule.Namespace,
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

func getBackupLocationCR(backupSchedule *stork_api.ApplicationBackupSchedule) (*stork_api.BackupLocation, error) {
	fn := "getBackupLocationCR"
	// Get the backuplocation CR name
	backuplocationCRName := backupSchedule.Spec.Template.Spec.BackupLocation
	// Get the backuplocation CR content
	backuplocationCR, err := storkops.Instance().GetBackupLocation(backuplocationCRName, backupSchedule.Namespace)
	if err != nil {
		logrus.Errorf("%s: failed in getting backuplocation CR %v/%v: %v", fn, backuplocationCRName, backupSchedule.Namespace, err)
		return nil, err
	}
	return backuplocationCR, nil
}

// isPeriodicSchedule - isPeriodicSchedule will check whether the sp has interval type is set.
func isPeriodicSchedule(schedulePolicyCR *stork_api.SchedulePolicy) bool {
	return schedulePolicyCR.Policy.Interval != nil
}

// getLatestSuccessfullbackupTime - will check return the last successful backup for backupschedule.
// It will check the backup list present in the applicationbackupschedule CR.
func getLatestSuccessfullbackupTime(backupSchedule *stork_api.ApplicationBackupSchedule) int64 {
	var lastSuccessfulBackupCreateTime int64
	for _, statusItems := range backupSchedule.Status.Items {
		i := len(statusItems) - 1
		for i >= 0 {
			if statusItems[i].Status == stork_api.ApplicationBackupStatusSuccessful {
				lastSuccessfulBackupCreateTime = statusItems[i].CreationTimestamp.Unix()
				return lastSuccessfulBackupCreateTime
			}
			i = i - 1
		}
	}
	return lastSuccessfulBackupCreateTime
}

func (s *ApplicationBackupScheduleController) startApplicationBackup(backupSchedule *stork_api.ApplicationBackupSchedule, policyType stork_api.SchedulePolicyType) error {
	funct := "startApplicationBackup"
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
	err := s.client.Update(context.TODO(), backupSchedule)
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
	if backup.Annotations == nil {
		backup.Annotations = make(map[string]string)
	}
	backup.Annotations[ApplicationBackupScheduleNameAnnotation] = backupSchedule.Name
	backup.Annotations[ApplicationBackupSchedulePolicyTypeAnnotation] = string(policyType)
	if val, ok := backupSchedule.Annotations[backupTypeKey]; ok {
		if val == genericBackupTypeValue {
			backup.Spec.BackupType = genericBackupTypeValue
		}
	}
	options, err := schedule.GetOptions(backupSchedule.Spec.SchedulePolicyName, backupSchedule.Namespace, policyType)
	if err != nil {
		return err
	}
	if backup.Spec.Options == nil {
		backup.Spec.Options = make(map[string]string)
	}
	for k, v := range options {
		backup.Spec.Options[k] = v
	}

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
	backupLocationCR, err := getBackupLocationCR(backupSchedule)
	if err != nil {
		return err
	}
	// Update the objectLock rentention period annotation
	objectLockInfo, err := objectstore.GetObjLockInfo(backupLocationCR)
	if err != nil {
		return err
	}
	if objectLockInfo.LockEnabled {
		// In case of object locked bucket, verify the minimum retention period is set on bucket or not
		// If user set in years no need of any validity check.
		var minRetentionPeriodReqrd int64
		var valid bool
		var err error
		if objectLockInfo.RetentionPeriodDays != 0 {
			valid, minRetentionPeriodReqrd, err = k8sutils.IsValidBucketRetentionPeriod(objectLockInfo.RetentionPeriodDays)
			if err != nil {
				errMsg := fmt.Sprintf("failed to check validity of bucket retention period %v", err)
				logrus.Errorf("%s: %v", funct, errMsg)
				return fmt.Errorf("%v", errMsg)
			}
			if !valid {
				errMsg := fmt.Sprintf("invalid bucket retention period set for backup location %s, it needs more than minimum number of retention period in days", backupLocationCR.GetName())
				logrus.Errorf("%s: %v", funct, errMsg)
				backup.Status.Status = stork_api.ApplicationBackupStatusFailed
				backup.Status.Reason = fmt.Sprintf("Failed due to insufficient retention period, Please set S3 bucket's minimum retention period to %d days", minRetentionPeriodReqrd)
				backup.Status.Stage = stork_api.ApplicationBackupStageFinal
				backup.Annotations[ApplicationBackupObjectLockRetentionAnnotation] = strconv.FormatInt(objectLockInfo.RetentionPeriodDays, 10)
				// Don't need to process anything further
				_, err = storkops.Instance().CreateApplicationBackup(backup)
				return err
			}
		}
		// Add applicationBackupObjectLockRetentionAnnotation in the applicationbackup CR with configured bucket retention value
		var retentionPeriod int64
		if objectLockInfo.RetentionPeriodYears != 0 {
			currTime := time.Now()
			lastRetentionDate := currTime.AddDate(int(objectLockInfo.RetentionPeriodYears), 0, 0)
			// duration works till 290 years, any duration beyond that will be floored
			duration := lastRetentionDate.Sub(currTime)
			retentionPeriod = int64(duration.Hours() / 24)
		} else {
			retentionPeriod = objectLockInfo.RetentionPeriodDays
		}
		backup.Annotations[ApplicationBackupObjectLockRetentionAnnotation] = strconv.FormatInt(retentionPeriod, 10)
		var lastSuccessfulBackupCreateTime int64
		// If it is a periodic type schedule, we need to force the first backup in a day slot to be full backup.
		// The day slot definition is twenty four hours from the time the backupschedule created.
		// Check whether it is periodic schedule type
		schedulePolicyObj, err := storkops.Instance().GetSchedulePolicy(backupSchedule.Spec.SchedulePolicyName)
		if err != nil {
			return err
		}
		periodic := isPeriodicSchedule(schedulePolicyObj)
		if periodic {
			// Get the last successfull backup create time from backup list of applicationbackupschedule CR.
			lastSuccessfulBackupCreateTime = getLatestSuccessfullbackupTime(backupSchedule)
			// Get the currentTime
			currentTime := time.Now().Unix()
			// Get the applicationbackupschedule CR creation time, to get the 24 hour slot details.
			backupscheduleCreationTime := backupSchedule.CreationTimestamp.Unix()
			diff := currentTime - backupscheduleCreationTime
			// elaspedDays represents the number days elapsed from the starting of applicationbackupschedule CR.
			elaspedDays := diff / dayInSec
			elaspedDaysInSecs := elaspedDays * dayInSec
			// Current day's start time in sec
			currentDayStartTime := backupscheduleCreationTime + elaspedDaysInSecs
			// Check whether lastSuccessfulBackupCreateTime value is less than currentDayStartTime value
			// If yes, that means there is no successful backup in the current day.
			// If there is a successful, then it will be full backup as we are force it below.
			// TODO: Need to remove or move the below debug statement to Tracef level before release.
			// For now adding it as it will be helpful for debugging the issue.
			logrus.Infof("backupschedule name: %v - backup name: %v - lastSuccessfulBackupCreateTime: %v - currentTime %v",
				backupSchedule.Name, backup.Name, lastSuccessfulBackupCreateTime, currentTime)
			logrus.Infof("backupscheduleCreationTime: %v - diff: %v - elaspedDays: %v - elaspedDaysInSecs: %v - currentDayStartTime: %v",
				backupscheduleCreationTime, diff, elaspedDays, elaspedDaysInSecs, currentDayStartTime)

			if lastSuccessfulBackupCreateTime < currentDayStartTime {
				// forcing it to be full backup, by setting the PXIncrementalCountAnnotation to zero
				backup.Spec.Options[utils.PXIncrementalCountAnnotation] = fmt.Sprintf("%v", 0)
			}
		}
	}

	_, err = storkops.Instance().CreateApplicationBackup(backup)
	return err
}

func (s *ApplicationBackupScheduleController) pruneApplicationBackups(backupSchedule *stork_api.ApplicationBackupSchedule) error {
	for policyType, policyApplicationBackup := range backupSchedule.Status.Items {
		numApplicationBackups := len(policyApplicationBackup)
		deleteBefore := 0
		retainNum, err := schedule.GetRetain(backupSchedule.Spec.SchedulePolicyName, backupSchedule.Namespace, policyType)
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
					err := storkops.Instance().DeleteApplicationBackup(policyApplicationBackup[i].Name, backupSchedule.Namespace)
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
	return s.client.Update(context.TODO(), backupSchedule)
}

func (s *ApplicationBackupScheduleController) createCRD() error {
	resource := apiextensions.CustomResource{
		Name:    stork_api.ApplicationBackupScheduleResourceName,
		Plural:  stork_api.ApplicationBackupScheduleResourcePlural,
		Group:   stork_api.SchemeGroupVersion.Group,
		Version: stork_api.SchemeGroupVersion.Version,
		Scope:   apiextensionsv1beta1.NamespaceScoped,
		Kind:    reflect.TypeOf(stork_api.ApplicationBackupSchedule{}).Name(),
	}
	ok, err := version.RequiresV1Registration()
	if err != nil {
		return err
	}
	if ok {
		err := k8sutils.CreateCRDV1(resource)
		if err != nil && !errors.IsAlreadyExists(err) {
			return err
		}
		return apiextensions.Instance().ValidateCRD(resource.Plural+"."+resource.Group, validateCRDTimeout, validateCRDInterval)
	}
	err = apiextensions.Instance().CreateCRDV1beta1(resource)
	if err != nil && !errors.IsAlreadyExists(err) {
		return err
	}
	return apiextensions.Instance().ValidateCRDV1beta1(resource, validateCRDTimeout, validateCRDInterval)
}
