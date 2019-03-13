package pvcwatcher

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"time"

	snapv1 "github.com/kubernetes-incubator/external-storage/snapshot/pkg/apis/crd/v1"
	"github.com/libopenstorage/stork/drivers/volume"
	storkv1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/libopenstorage/stork/pkg/controller"
	"github.com/operator-framework/operator-sdk/pkg/sdk"
	"github.com/portworx/sched-ops/k8s"
	"gopkg.in/yaml.v2"
	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/record"
	k8shelper "k8s.io/kubernetes/pkg/apis/core/v1/helper"
)

const (
	annotationPrefix                       = "stork.libopenstorage.org/"
	snapshotSchedulePolicyAnnotationPrefix = "snapshotschedule." + annotationPrefix
)

// PVCWatcher watches for changes in PVCs
type PVCWatcher struct {
	Driver   volume.Driver
	Recorder record.EventRecorder
}

type policyInfo struct {
	SchedulePolicyName string                    `yaml:"schedulePolicyName"`
	ReclaimPolicy      storkv1.ReclaimPolicyType `yaml:"reclaimPolicy"`
	Annotations        map[string]string         `yaml:"annotations"`
}

// Start Starts the controller to watch updates on PVCs
func (p *PVCWatcher) Start() error {
	return controller.Register(
		&schema.GroupVersionKind{
			Group:   v1.GroupName,
			Version: v1.SchemeGroupVersion.Version,
			Kind:    reflect.TypeOf(v1.PersistentVolumeClaim{}).Name(),
		},
		"",
		1*time.Minute,
		p)
}

// Handle updates for PVCs
func (p *PVCWatcher) Handle(ctx context.Context, event sdk.Event) error {
	switch o := event.Object.(type) {
	case *v1.PersistentVolumeClaim:
		err := p.handleSnapshotScheduleUpdates(o, event)
		if err != nil {
			return err
		}
	}
	return nil
}

func getPoliciesFromMap(options map[string]string, scheduleNamePrefix string) (map[string]*policyInfo, error) {
	policyMap := make(map[string]*policyInfo)
	for k, v := range options {
		if strings.HasPrefix(k, snapshotSchedulePolicyAnnotationPrefix) {
			scheduleName := strings.TrimPrefix(k, snapshotSchedulePolicyAnnotationPrefix)
			var policy policyInfo
			err := yaml.Unmarshal([]byte(v), &policy)
			if err != nil {
				return nil, err
			}
			if policy.ReclaimPolicy == "" {
				policy.ReclaimPolicy = storkv1.ReclaimPolicyRetain
			}
			policyMap[scheduleNamePrefix+scheduleName] = &policy
		}
	}

	return policyMap, nil
}

func (p *PVCWatcher) handleSnapshotScheduleUpdates(pvc *v1.PersistentVolumeClaim, event sdk.Event) error {
	// Nothing to do for deletions
	if event.Deleted {
		return nil
	}

	// Do nothing if the driver doesn't own the PVC or if it isn't bound yet
	if !p.Driver.OwnsPVC(pvc) || pvc.Status.Phase != v1.ClaimBound {
		return nil
	}
	storageClassName := k8shelper.GetPersistentVolumeClaimClass(pvc)
	if storageClassName == "" {
		return nil
	}
	storageClass, err := k8s.Instance().GetStorageClass(storageClassName)
	if err != nil {
		return err
	}

	policiesMap, err := getPoliciesFromMap(storageClass.Parameters, pvc.Name+"-")
	if err != nil {
		return err
	}
	for snapshotScheduleName, policy := range policiesMap {
		schedulePolicyName := policy.SchedulePolicyName
		if _, err := k8s.Instance().GetSnapshotSchedule(snapshotScheduleName, pvc.Namespace); err == nil {
			continue
		}

		snapshotSchedule := &storkv1.VolumeSnapshotSchedule{
			ObjectMeta: metav1.ObjectMeta{
				Name:        snapshotScheduleName,
				Namespace:   pvc.Namespace,
				Annotations: policy.Annotations,
				// Set the owner reference so that the schedule gets deleted
				// with the PVC
				OwnerReferences: []metav1.OwnerReference{
					{
						Name:       pvc.Name,
						UID:        pvc.UID,
						Kind:       pvc.GetObjectKind().GroupVersionKind().Kind,
						APIVersion: pvc.GetObjectKind().GroupVersionKind().GroupVersion().String(),
					},
				},
			},
			Spec: storkv1.VolumeSnapshotScheduleSpec{
				Template: storkv1.VolumeSnapshotTemplateSpec{
					Spec: snapv1.VolumeSnapshotSpec{
						PersistentVolumeClaimName: pvc.Name,
					},
				},
				SchedulePolicyName: schedulePolicyName,
				ReclaimPolicy:      policy.ReclaimPolicy,
			},
		}
		_, err = k8s.Instance().CreateSnapshotSchedule(snapshotSchedule)
		if err != nil {
			p.Recorder.Event(pvc,
				v1.EventTypeWarning,
				"Error",
				fmt.Sprintf("Error creating snapshot schedule for PVC: %v", err))
			return err
		}
		p.Recorder.Event(pvc,
			v1.EventTypeNormal,
			"Success",
			fmt.Sprintf("Created volume snapshot schedule (%v) for PVC", snapshotScheduleName))
	}

	return err
}
