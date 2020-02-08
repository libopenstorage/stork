package controllers

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"time"

	snapv1 "github.com/kubernetes-incubator/external-storage/snapshot/pkg/apis/crd/v1"
	"github.com/libopenstorage/stork/pkg/apis/stork"
	stork_api "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/libopenstorage/stork/pkg/controller"
	"github.com/libopenstorage/stork/pkg/log"
	"github.com/libopenstorage/stork/pkg/schedule"
	"github.com/operator-framework/operator-sdk/pkg/sdk"
	"github.com/portworx/sched-ops/k8s/apiextensions"
	k8sextops "github.com/portworx/sched-ops/k8s/externalstorage"
	v1 "k8s.io/api/core/v1"
	apiextensionsv1beta1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1beta1"
	"k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/record"
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
)

// SnapshotScheduleController reconciles VolumeSnapshotSchedule objects
type SnapshotScheduleController struct {
	Recorder record.EventRecorder
}

// Init Initialize the snapshot schedule controller
func (s *SnapshotScheduleController) Init() error {
	err := s.createCRD()
	if err != nil {
		return err
	}
	return controller.Register(
		&schema.GroupVersionKind{
			Group:   stork.GroupName,
			Version: stork_api.SchemeGroupVersion.Version,
			Kind:    reflect.TypeOf(stork_api.VolumeSnapshotSchedule{}).Name(),
		},
		"",
		1*time.Minute,
		s)
}

// Handle updates for VolumeSnapshotSchedule objects
func (s *SnapshotScheduleController) Handle(ctx context.Context, event sdk.Event) error {
	switch o := event.Object.(type) {
	case *stork_api.VolumeSnapshotSchedule:
		snapshotSchedule := o
		// Nothing to do for delete
		if event.Deleted {
			return nil
		}

		s.setDefaults(snapshotSchedule)
		// First update the status of any pending snapshots
		err := s.updateVolumeSnapshotStatus(snapshotSchedule)
		if err != nil {
			msg := fmt.Sprintf("Error updating snapshot status: %v", err)
			s.Recorder.Event(snapshotSchedule,
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
				s.Recorder.Event(snapshotSchedule,
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
					s.Recorder.Event(snapshotSchedule,
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
			s.Recorder.Event(snapshotSchedule,
				v1.EventTypeWarning,
				string(snapv1.VolumeSnapshotConditionError),
				msg)
			log.VolumeSnapshotScheduleLog(snapshotSchedule).Error(msg)
			return err
		}
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
	for _, policyVolumeSnapshot := range snapshotSchedule.Status.Items {
		for _, snapshot := range policyVolumeSnapshot {
			// Get the updated status if we see it as not completed
			if !s.isVolumeSnapshotComplete(snapshot.Status) {
				pendingVolumeSnapshotStatus, err := getVolumeSnapshotStatus(snapshot.Name, snapshotSchedule.Namespace)
				if err != nil {
					s.Recorder.Event(snapshotSchedule,
						v1.EventTypeWarning,
						string(snapv1.VolumeSnapshotConditionError),
						fmt.Sprintf("Error getting status of snapshot %v: %v", snapshot.Name, err))
				}

				// Check again and update the status if it is completed
				snapshot.Status = pendingVolumeSnapshotStatus
				if s.isVolumeSnapshotComplete(snapshot.Status) {
					snapshot.FinishTimestamp = meta.NewTime(schedule.GetCurrentTime())
					if pendingVolumeSnapshotStatus == snapv1.VolumeSnapshotConditionReady {
						s.Recorder.Event(snapshotSchedule,
							v1.EventTypeNormal,
							string(snapv1.VolumeSnapshotConditionReady),
							fmt.Sprintf("Scheduled snapshot (%v) completed successfully", snapshot.Name))
					} else {
						s.Recorder.Event(snapshotSchedule,
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
		err := sdk.Update(snapshotSchedule)
		if err != nil {
			return err
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
	return strings.Join([]string{snapshotSchedule.Name, strings.ToLower(string(policyType)), time.Now().Format(nameTimeSuffixFormat)}, "-")
}

func (s *SnapshotScheduleController) startVolumeSnapshot(snapshotSchedule *stork_api.VolumeSnapshotSchedule, policyType stork_api.SchedulePolicyType) error {
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
	err := sdk.Update(snapshotSchedule)
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

	log.VolumeSnapshotScheduleLog(snapshotSchedule).Infof("Starting snapshot %v", snapshotName)
	// If reclaim policy is set to Delete, this will delete the snapshots
	// created by this snapshotschedule when the schedule object is deleted
	if snapshotSchedule.Spec.ReclaimPolicy == stork_api.ReclaimPolicyDelete {
		snapshot.Metadata.OwnerReferences = []meta.OwnerReference{
			{
				Name:       snapshotSchedule.Name,
				UID:        snapshotSchedule.UID,
				Kind:       snapshotSchedule.GetObjectKind().GroupVersionKind().Kind,
				APIVersion: snapshotSchedule.GetObjectKind().GroupVersionKind().GroupVersion().String(),
			},
		}
	}
	_, err = k8sextops.Instance().CreateSnapshot(snapshot)
	return err
}

func (s *SnapshotScheduleController) pruneVolumeSnapshots(snapshotSchedule *stork_api.VolumeSnapshotSchedule) error {
	for policyType, policyVolumeSnapshot := range snapshotSchedule.Status.Items {
		numVolumeSnapshots := len(policyVolumeSnapshot)
		deleteBefore := 0
		retainNum, err := schedule.GetRetain(snapshotSchedule.Spec.SchedulePolicyName, policyType)
		if err != nil {
			return err
		}
		numReady := 0

		// Keep up to retainNum successful snapshot statuses and all failed snapshots
		// until there is a successful one
		if numVolumeSnapshots > int(retainNum) {
			// Start from the end and find the retainNum successful snapshots
			for i := range policyVolumeSnapshot {
				if policyVolumeSnapshot[(numVolumeSnapshots-1-i)].Status == snapv1.VolumeSnapshotConditionReady {
					numReady++
					if numReady > int(retainNum) {
						deleteBefore = numVolumeSnapshots - i
						break
					}
				}
			}
			failedDeletes := make([]*stork_api.ScheduledVolumeSnapshotStatus, 0)
			if numReady > int(retainNum) {
				for i := 0; i < deleteBefore; i++ {
					err := k8sextops.Instance().DeleteSnapshot(policyVolumeSnapshot[i].Name, snapshotSchedule.Namespace)
					if err != nil && !errors.IsNotFound(err) {
						log.VolumeSnapshotScheduleLog(snapshotSchedule).Warnf("Error deleting %v: %v", policyVolumeSnapshot[i].Name, err)
						// Keep a track of the failed deletes
						failedDeletes = append(failedDeletes, policyVolumeSnapshot[i])
					}
				}
			}
			// Remove all the ones we tried to delete above
			snapshotSchedule.Status.Items[policyType] = policyVolumeSnapshot[deleteBefore:]
			// And re-add the ones that failed so that we don't lose track
			// of them
			snapshotSchedule.Status.Items[policyType] = append(failedDeletes, snapshotSchedule.Status.Items[policyType]...)
		}
	}
	return sdk.Update(snapshotSchedule)
}

func (s *SnapshotScheduleController) createCRD() error {
	resource := apiextensions.CustomResource{
		Name:    stork_api.VolumeSnapshotScheduleResourceName,
		Plural:  stork_api.VolumeSnapshotScheduleResourcePlural,
		Group:   stork.GroupName,
		Version: stork_api.SchemeGroupVersion.Version,
		Scope:   apiextensionsv1beta1.NamespaceScoped,
		Kind:    reflect.TypeOf(stork_api.VolumeSnapshotSchedule{}).Name(),
	}
	err := apiextensions.Instance().CreateCRD(resource)
	if err != nil && !errors.IsAlreadyExists(err) {
		return err
	}

	return apiextensions.Instance().ValidateCRD(resource, validateCRDTimeout, validateCRDInterval)
}
