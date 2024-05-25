package controllers

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"time"

	snapv1 "github.com/kubernetes-incubator/external-storage/snapshot/pkg/apis/crd/v1"
	stork_api "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/libopenstorage/stork/pkg/controllers"
	"github.com/libopenstorage/stork/pkg/k8sutils"
	"github.com/libopenstorage/stork/pkg/log"
	"github.com/libopenstorage/stork/pkg/schedule"
	"github.com/libopenstorage/stork/pkg/utils"
	"github.com/libopenstorage/stork/pkg/version"
	"github.com/portworx/sched-ops/k8s/apiextensions"
	k8sextops "github.com/portworx/sched-ops/k8s/externalstorage"
	storkops "github.com/portworx/sched-ops/k8s/stork"
	"github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	apiextensionsv1beta1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1beta1"
	"k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/tools/record"
	runtimeclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const (
	nameTimeSuffixFormat string        = "2006-01-02-150405"
	validateCRDInterval  time.Duration = 5 * time.Second
	validateCRDTimeout   time.Duration = 1 * time.Minute

	// SnapshotScheduleNameAnnotation Annotation used to specify the name of schedule that
	// created the snapshot
	SnapshotScheduleNameAnnotation = "stork.libopenstorage.org/snapshotScheduleName"
	// SnapshotSchedulePolicyTypeAnnotation Annotation used to specify the type of the
	// policy that triggered the snapshot
	SnapshotSchedulePolicyTypeAnnotation = "stork.libopenstorage.org/snapshotSchedulePolicyType"
	storkRuleAnnotationPrefix            = "stork.libopenstorage.org"
	preSnapRuleAnnotationKey             = storkRuleAnnotationPrefix + "/pre-snapshot-rule"
	postSnapRuleAnnotationKey            = storkRuleAnnotationPrefix + "/post-snapshot-rule"
	StorkSnapshotNameLabel               = "stork.libopenstorage.org/snapshotName"
)

// NewSnapshotScheduleController creates a new instance of SnapshotScheduleController.
func NewSnapshotScheduleController(mgr manager.Manager, r record.EventRecorder) *SnapshotScheduleController {
	return &SnapshotScheduleController{
		client:   mgr.GetClient(),
		recorder: r,
	}
}

// SnapshotScheduleController reconciles VolumeSnapshotSchedule objects
type SnapshotScheduleController struct {
	client runtimeclient.Client

	recorder record.EventRecorder
}

// Init Initialize the snapshot schedule controller
func (s *SnapshotScheduleController) Init(mgr manager.Manager) error {
	err := s.createCRD()
	if err != nil {
		return fmt.Errorf("register crd: %s", err)
	}

	return controllers.RegisterTo(mgr, "snapshot-schedule-controller", s, &stork_api.VolumeSnapshotSchedule{})
}

// Reconcile manages SnapshotSchedule resources.
func (s *SnapshotScheduleController) Reconcile(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
	logrus.Tracef("Reconciling VolumeSnapshotSchedule %s/%s", request.Namespace, request.Name)

	// Fetch the ApplicationBackup instance
	snapshotSchedule := &stork_api.VolumeSnapshotSchedule{}
	err := s.client.Get(context.TODO(), request.NamespacedName, snapshotSchedule)
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

	if err = s.handle(context.TODO(), snapshotSchedule); err != nil {
		logrus.Errorf("%s: %s/%s: %s", reflect.TypeOf(s), snapshotSchedule.Namespace, snapshotSchedule.Name, err)
		return reconcile.Result{RequeueAfter: controllers.DefaultRequeueError}, err
	}

	return reconcile.Result{RequeueAfter: controllers.DefaultRequeue}, nil
}

// Handle updates for VolumeSnapshotSchedule objects
func (s *SnapshotScheduleController) handle(ctx context.Context, snapshotSchedule *stork_api.VolumeSnapshotSchedule) error {
	// Nothing to do for delete
	if snapshotSchedule.DeletionTimestamp != nil {
		return nil
	}

	s.setDefaults(snapshotSchedule)
	err := s.client.Update(context.TODO(), snapshotSchedule)
	if err != nil {
		return err
	}
	// First update the status of any pending snapshots
	err = s.updateVolumeSnapshotStatus(snapshotSchedule)
	if err != nil {
		msg := fmt.Sprintf("Error updating snapshot status: %v", err)
		s.recorder.Event(snapshotSchedule,
			v1.EventTypeWarning,
			string(snapv1.VolumeSnapshotConditionError),
			msg)
		log.VolumeSnapshotScheduleLog(snapshotSchedule).Error(msg)
		return err
	}

	if snapshotSchedule.Spec.Suspend == nil || !*snapshotSchedule.Spec.Suspend {
		// Then check if any of the policies require a trigger
		policyType, start, err := s.shouldStartVolumeSnapshot(snapshotSchedule)
		if err != nil {
			msg := fmt.Sprintf("Error checking if snapshot should be triggered: %v", err)
			s.recorder.Event(snapshotSchedule,
				v1.EventTypeWarning,
				string(snapv1.VolumeSnapshotConditionError),
				msg)
			log.VolumeSnapshotScheduleLog(snapshotSchedule).Error(msg)
			return nil
		}

		// Start a snapshot for a policy if required
		if start {
			err := s.startVolumeSnapshot(snapshotSchedule, policyType)
			if err != nil {
				msg := fmt.Sprintf("Error triggering snapshot for schedule(%v): %v", policyType, err)
				s.recorder.Event(snapshotSchedule,
					v1.EventTypeWarning,
					string(snapv1.VolumeSnapshotConditionError),
					msg)
				log.VolumeSnapshotScheduleLog(snapshotSchedule).Error(msg)
				return err
			}
		}
	}

	// Finally, prune any old snapshots that were triggered for this
	// schedule
	err = s.pruneVolumeSnapshots(snapshotSchedule)
	if err != nil {
		msg := fmt.Sprintf("Error pruning old snapshots: %v", err)
		s.recorder.Event(snapshotSchedule,
			v1.EventTypeWarning,
			string(snapv1.VolumeSnapshotConditionError),
			msg)
		log.VolumeSnapshotScheduleLog(snapshotSchedule).Error(msg)
		return err
	}

	return nil
}

func (s *SnapshotScheduleController) setDefaults(snapshotSchedule *stork_api.VolumeSnapshotSchedule) {
	if snapshotSchedule.Spec.ReclaimPolicy == "" {
		snapshotSchedule.Spec.ReclaimPolicy = stork_api.ReclaimPolicyDelete
	}
}

func getVolumeSnapshotStatus(name string, namespace string) (snapv1.VolumeSnapshotConditionType, error) {
	snapshot, err := k8sextops.Instance().GetSnapshot(name, namespace)
	if err != nil {
		return snapv1.VolumeSnapshotConditionError, err
	}
	if snapshot.Status.Conditions == nil || len(snapshot.Status.Conditions) == 0 {
		return snapv1.VolumeSnapshotConditionPending, nil
	}
	lastCondition := snapshot.Status.Conditions[len(snapshot.Status.Conditions)-1]
	if lastCondition.Type == snapv1.VolumeSnapshotConditionReady && lastCondition.Status == v1.ConditionTrue {
		return snapv1.VolumeSnapshotConditionReady, nil
	} else if lastCondition.Type == snapv1.VolumeSnapshotConditionError && lastCondition.Status == v1.ConditionTrue {
		return snapv1.VolumeSnapshotConditionError, nil
	} else if lastCondition.Type == snapv1.VolumeSnapshotConditionPending &&
		(lastCondition.Status == v1.ConditionTrue || lastCondition.Status == v1.ConditionUnknown) {
		return snapv1.VolumeSnapshotConditionPending, nil
	}
	return snapv1.VolumeSnapshotConditionPending, nil

}

func (s *SnapshotScheduleController) updateVolumeSnapshotStatus(snapshotSchedule *stork_api.VolumeSnapshotSchedule) error {
	updated := false
	oldSnapShotSchedule := snapshotSchedule.DeepCopy()
	for _, policyVolumeSnapshot := range snapshotSchedule.Status.Items {
		for _, snapshot := range policyVolumeSnapshot {
			if snapshot.Status != snapv1.VolumeSnapshotConditionReady {
				pendingVolumeSnapshotStatus, err := getVolumeSnapshotStatus(snapshot.Name, snapshotSchedule.Namespace)
				if err != nil {
					s.recorder.Event(snapshotSchedule,
						v1.EventTypeWarning,
						err.Error(),
						fmt.Sprintf("Error updating snapshot (%s) status", snapshot.Name))
					if errors.IsNotFound(err) {
						snapshot.Status = snapv1.VolumeSnapshotConditionError
						updated = true
					}
					continue
				}

				// Check again and update the status if it is completed
				if snapshot.Status == pendingVolumeSnapshotStatus {
					// latest snapshot status is same, lets not update volumesnapshotschedule CR
					continue
				}
				snapshot.Status = pendingVolumeSnapshotStatus
				if s.isVolumeSnapshotComplete(snapshot.Status) {
					snapshot.FinishTimestamp = meta.NewTime(schedule.GetCurrentTime())
					if pendingVolumeSnapshotStatus == snapv1.VolumeSnapshotConditionReady {
						s.recorder.Event(snapshotSchedule,
							v1.EventTypeNormal,
							string(snapv1.VolumeSnapshotConditionReady),
							fmt.Sprintf("Scheduled snapshot (%v) completed successfully", snapshot.Name))
					} else {
						s.recorder.Event(snapshotSchedule,
							v1.EventTypeWarning,
							string(snapv1.VolumeSnapshotConditionError),
							fmt.Sprintf("Scheduled snapshot (%v) failed", snapshot.Name))
					}
				}
				updated = true
			}
		}
	}
	if updated {
		patchBytes, err := utils.CreateVolumeSnapshotSchedulePatch(oldSnapShotSchedule, snapshotSchedule)
		if err != nil {
			return fmt.Errorf("error creating patch for volumesnapshot schedule %s: %v", oldSnapShotSchedule.Name, err)
		}
		err = s.client.Patch(context.TODO(), oldSnapShotSchedule, runtimeclient.RawPatch(types.MergePatchType, patchBytes))
		if err != nil {
			return fmt.Errorf("error applying patch on volumesnapshot schedule %s: %v", oldSnapShotSchedule.Name, err)
		}
	}
	return nil
}

func (s *SnapshotScheduleController) isVolumeSnapshotComplete(status snapv1.VolumeSnapshotConditionType) bool {
	return status != snapv1.VolumeSnapshotConditionPending
}

func (s *SnapshotScheduleController) shouldStartVolumeSnapshot(snapshotSchedule *stork_api.VolumeSnapshotSchedule) (stork_api.SchedulePolicyType, bool, error) {
	// Don't trigger a new snapshot if one is already in progress
	for _, policyType := range stork_api.GetValidSchedulePolicyTypes() {
		policyVolumeSnapshot, present := snapshotSchedule.Status.Items[policyType]
		if present {
			for _, snapshot := range policyVolumeSnapshot {
				if !s.isVolumeSnapshotComplete(snapshot.Status) {
					return stork_api.SchedulePolicyTypeInvalid, false, nil
				}
			}
		}
	}

	for _, policyType := range stork_api.GetValidSchedulePolicyTypes() {
		var latestVolumeSnapshotTimestamp meta.Time
		policyVolumeSnapshot, present := snapshotSchedule.Status.Items[policyType]
		if present {
			for _, snapshot := range policyVolumeSnapshot {
				if latestVolumeSnapshotTimestamp.Before(&snapshot.CreationTimestamp) {
					latestVolumeSnapshotTimestamp = snapshot.CreationTimestamp
				}
			}
		}
		trigger, err := schedule.TriggerRequired(
			snapshotSchedule.Spec.SchedulePolicyName,
			snapshotSchedule.Namespace,
			policyType,
			latestVolumeSnapshotTimestamp,
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

func (s *SnapshotScheduleController) formatVolumeSnapshotName(snapshotSchedule *stork_api.VolumeSnapshotSchedule, policyType stork_api.SchedulePolicyType) string {
	// get a random 4 character suffix from the snapshotschedule's UID
	randSuffix := string(snapshotSchedule.UID)[len(snapshotSchedule.UID)-4:]

	snapSuffix := strings.Join([]string{strings.ToLower(string(policyType)), randSuffix, time.Now().Format(nameTimeSuffixFormat)}, "-")
	scheduleName := snapshotSchedule.Name
	if len(scheduleName) >= validation.LabelValueMaxLength-len(snapSuffix) {
		scheduleName = scheduleName[:validation.LabelValueMaxLength-len(snapSuffix)-1]
	}
	return strings.Join([]string{scheduleName, snapSuffix}, "-")
}

func (s *SnapshotScheduleController) startVolumeSnapshot(inputSnapshotSchedule *stork_api.VolumeSnapshotSchedule, policyType stork_api.SchedulePolicyType) error {
	// Get the latest copy of snapshotschedule for updating
	snapshotSchedule, err := storkops.Instance().GetSnapshotSchedule(inputSnapshotSchedule.Name, inputSnapshotSchedule.Namespace)
	if err != nil {
		return fmt.Errorf("failed to get volumesnapshot schedule %s", inputSnapshotSchedule.Name)
	}
	snapshotName := s.formatVolumeSnapshotName(snapshotSchedule, policyType)
	if snapshotSchedule.Status.Items == nil {
		snapshotSchedule.Status.Items = make(map[stork_api.SchedulePolicyType][]*stork_api.ScheduledVolumeSnapshotStatus)
	}
	if snapshotSchedule.Status.Items[policyType] == nil {
		snapshotSchedule.Status.Items[policyType] = make([]*stork_api.ScheduledVolumeSnapshotStatus, 0)
	}
	snapshotSchedule.Status.Items[policyType] = append(snapshotSchedule.Status.Items[policyType],
		&stork_api.ScheduledVolumeSnapshotStatus{
			Name:              snapshotName,
			CreationTimestamp: meta.NewTime(schedule.GetCurrentTime()),
			Status:            snapv1.VolumeSnapshotConditionPending,
		})
	err = s.client.Update(context.TODO(), snapshotSchedule)
	if err != nil {
		return err
	}

	snapshot := &snapv1.VolumeSnapshot{
		Metadata: meta.ObjectMeta{
			Name:        snapshotName,
			Namespace:   snapshotSchedule.Namespace,
			Annotations: snapshotSchedule.Annotations,
			Labels:      snapshotSchedule.Labels,
		},
		Spec: snapshotSchedule.Spec.Template.Spec,
	}
	if snapshot.Metadata.Annotations == nil {
		snapshot.Metadata.Annotations = make(map[string]string)
	}
	snapshot.Metadata.Annotations[SnapshotScheduleNameAnnotation] = snapshotSchedule.Name
	snapshot.Metadata.Annotations[SnapshotSchedulePolicyTypeAnnotation] = string(policyType)
	if snapshot.Metadata.Labels == nil {
		snapshot.Metadata.Labels = make(map[string]string)
	}
	snapshot.Metadata.Labels[StorkSnapshotNameLabel] = snapshotName
	if snapshotSchedule.Spec.PreExecRule != "" {
		_, err := storkops.Instance().GetRule(snapshotSchedule.Spec.PreExecRule, snapshotSchedule.Namespace)
		if err != nil {
			msg := fmt.Sprintf("error retrieving pre-exec rule %v", err)
			s.recorder.Event(snapshotSchedule,
				v1.EventTypeWarning,
				string(snapv1.VolumeSnapshotConditionError),
				msg)
			log.VolumeSnapshotScheduleLog(snapshotSchedule).Error(msg)
			return err
		}
	}
	snapshot.Metadata.Annotations[preSnapRuleAnnotationKey] = snapshotSchedule.Spec.PreExecRule
	if snapshotSchedule.Spec.PostExecRule != "" {
		_, err := storkops.Instance().GetRule(snapshotSchedule.Spec.PostExecRule, snapshotSchedule.Namespace)
		if err != nil {
			msg := fmt.Sprintf("error retrieving post-exec rule %v", err)
			s.recorder.Event(snapshotSchedule,
				v1.EventTypeWarning,
				string(snapv1.VolumeSnapshotConditionError),
				msg)
			log.VolumeSnapshotScheduleLog(snapshotSchedule).Error(msg)
			return err
		}
	}
	snapshot.Metadata.Annotations[postSnapRuleAnnotationKey] = snapshotSchedule.Spec.PostExecRule

	options, err := schedule.GetOptions(snapshotSchedule.Spec.SchedulePolicyName, snapshotSchedule.Namespace, policyType)
	if err != nil {
		return err
	}
	for k, v := range options {
		snapshot.Metadata.Annotations[k] = v
	}

	log.VolumeSnapshotScheduleLog(snapshotSchedule).Infof("Starting snapshot %v", snapshotName)
	// If reclaim policy is set to Delete, this will delete the snapshots
	// created by this snapshotschedule when the schedule object is deleted
	if snapshotSchedule.Spec.ReclaimPolicy == stork_api.ReclaimPolicyDelete {
		snapshot.Metadata.OwnerReferences = []meta.OwnerReference{
			{
				Name: snapshotSchedule.Name,
				UID:  snapshotSchedule.UID,
				// TODO: Kind of the fetched volumesnapshotschedule is empty, hence using the input one
				Kind:       inputSnapshotSchedule.GetObjectKind().GroupVersionKind().Kind,
				APIVersion: inputSnapshotSchedule.GetObjectKind().GroupVersionKind().GroupVersion().String(),
			},
		}
	}
	_, err = k8sextops.Instance().CreateSnapshot(snapshot)
	return err
}

func (s *SnapshotScheduleController) pruneVolumeSnapshots(snapshotSchedule *stork_api.VolumeSnapshotSchedule) error {
	snapshotScheduleUpdateRequired := false
	for policyType, policyVolumeSnapshot := range snapshotSchedule.Status.Items {
		numVolumeSnapshots := len(policyVolumeSnapshot)
		deleteBefore := 0
		retainNum, err := schedule.GetRetain(snapshotSchedule.Spec.SchedulePolicyName, snapshotSchedule.Namespace, policyType)
		if err != nil {
			return err
		}
		numReady := 0
		numError := 0

		// Keep up to retainNum successful snapshot statuses and all failed snapshots
		// until there is a successful one
		if numVolumeSnapshots > int(retainNum) {
			snapshotScheduleUpdateRequired = true
			// Start from the end and find the retainNum successful snapshots
			for i := range policyVolumeSnapshot {
				if policyVolumeSnapshot[(numVolumeSnapshots-1-i)].Status == snapv1.VolumeSnapshotConditionReady {
					numReady++
					if numReady > int(retainNum) {
						deleteBefore = numVolumeSnapshots - i
						break
					}
				} else if policyVolumeSnapshot[(numVolumeSnapshots-1-i)].Status == snapv1.VolumeSnapshotConditionError {
					numError++
				}
			}
			failedDeletes := make([]*stork_api.ScheduledVolumeSnapshotStatus, 0)
			// If number of ready snapshots are greater than the retain value,
			// delete all the snapshots before the deleteBefore index
			// else as there are already snapshots more than the retain values
			// try to delete the error snapshots more than the retain value
			if numReady > int(retainNum) {
				for i := 0; i < deleteBefore; i++ {
					err := k8sextops.Instance().DeleteSnapshot(policyVolumeSnapshot[i].Name, snapshotSchedule.Namespace)
					if err != nil && !errors.IsNotFound(err) {
						log.VolumeSnapshotScheduleLog(snapshotSchedule).Warnf("Error deleting %v: %v", policyVolumeSnapshot[i].Name, err)
						// Keep a track of the failed deletes
						failedDeletes = append(failedDeletes, policyVolumeSnapshot[i])
					}
				}
				// Remove all the ones we tried to delete above
				snapshotSchedule.Status.Items[policyType] = policyVolumeSnapshot[deleteBefore:]
				// And re-add the ones that failed so that we don't lose track
				// of them
				snapshotSchedule.Status.Items[policyType] = append(failedDeletes, snapshotSchedule.Status.Items[policyType]...)
			} else if numError > 1 {
				modPolicyVolumeSnapshot := make([]*stork_api.ScheduledVolumeSnapshotStatus, 0)
				excessSnapshots := numVolumeSnapshots - int(retainNum)
				includeIndex := 0
				// Only going to loop till the last but one snapshot,
				// we don't need to delete the latest snapshot which has just now been created but in error state
				for i := 0; i < len(policyVolumeSnapshot)-1; i++ {
					inList := true
					if policyVolumeSnapshot[i].Status == snapv1.VolumeSnapshotConditionError {
						log.VolumeSnapshotScheduleLog(snapshotSchedule).Infof("Going to delete snapshot %s in error state as part of pruning", policyVolumeSnapshot[i].Name)
						err := k8sextops.Instance().DeleteSnapshot(policyVolumeSnapshot[i].Name, snapshotSchedule.Namespace)
						if err != nil && !errors.IsNotFound(err) {
							log.VolumeSnapshotScheduleLog(snapshotSchedule).Warnf("Error deleting %v: %v", policyVolumeSnapshot[i].Name, err)
						} else {
							excessSnapshots--
							inList = false
						}
					}
					if inList {
						modPolicyVolumeSnapshot = append(modPolicyVolumeSnapshot, policyVolumeSnapshot[i])
					}
					includeIndex = i + 1
					if excessSnapshots < 1 {
						break
					}
				}
				snapshotSchedule.Status.Items[policyType] = append(modPolicyVolumeSnapshot, policyVolumeSnapshot[includeIndex:]...)
			}
		}
	}
	if snapshotScheduleUpdateRequired {
		// Get the latest copy of snapshotschedule for updating
		currentSnapshotSchedule, err := storkops.Instance().GetSnapshotSchedule(snapshotSchedule.Name, snapshotSchedule.Namespace)
		if err != nil {
			return fmt.Errorf("failed to get volumesnapshot schedule %s", snapshotSchedule.Name)
		}
		currentSnapshotSchedule.Status = snapshotSchedule.Status
		return s.client.Update(context.TODO(), currentSnapshotSchedule)
	}
	return nil
}

func (s *SnapshotScheduleController) createCRD() error {
	resource := apiextensions.CustomResource{
		Name:    stork_api.VolumeSnapshotScheduleResourceName,
		Plural:  stork_api.VolumeSnapshotScheduleResourcePlural,
		Group:   stork_api.SchemeGroupVersion.Group,
		Version: stork_api.SchemeGroupVersion.Version,
		Scope:   apiextensionsv1beta1.NamespaceScoped,
		Kind:    reflect.TypeOf(stork_api.VolumeSnapshotSchedule{}).Name(),
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
