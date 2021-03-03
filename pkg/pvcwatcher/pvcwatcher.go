package pvcwatcher

import (
	"context"
	"fmt"
	"strings"
	"time"

	snapv1 "github.com/kubernetes-incubator/external-storage/snapshot/pkg/apis/crd/v1"
	"github.com/libopenstorage/stork/drivers/volume"
	storkv1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/libopenstorage/stork/pkg/controllers"
	"github.com/portworx/sched-ops/k8s/core"
	"github.com/portworx/sched-ops/k8s/storage"
	storkops "github.com/portworx/sched-ops/k8s/stork"
	"github.com/sirupsen/logrus"
	"gopkg.in/yaml.v2"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	k8shelper "k8s.io/kubernetes/pkg/apis/core/v1/helper"
	runtimeclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const (
	annotationPrefix                       = "stork.libopenstorage.org/"
	snapshotSchedulePolicyAnnotationPrefix = "snapshotschedule." + annotationPrefix
	scheduleCreatedAnnotation              = annotationPrefix + "snapshot-schedule-created"
)

// New creates a new instance of PVCWatcher.
func New(mgr manager.Manager, d volume.Driver, r record.EventRecorder) *PVCWatcher {
	return &PVCWatcher{
		client:    mgr.GetClient(),
		volDriver: d,
		recorder:  r,
	}
}

// PVCWatcher watches for changes in PVCs
type PVCWatcher struct {
	client runtimeclient.Client

	volDriver volume.Driver
	recorder  record.EventRecorder
}

type policyInfo struct {
	SchedulePolicyName string                    `yaml:"schedulePolicyName"`
	ReclaimPolicy      storkv1.ReclaimPolicyType `yaml:"reclaimPolicy"`
	Annotations        map[string]string         `yaml:"annotations"`
}

// Start Starts the controller to watch updates on PVCs
func (p *PVCWatcher) Start(mgr manager.Manager) error {
	return controllers.RegisterTo(mgr, "pvc-watcher", p, &corev1.PersistentVolumeClaim{})
}

// Reconcile handles snapshot schedule updates for persistent volume claims.
func (p *PVCWatcher) Reconcile(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
	logrus.Tracef("Reconciling PVC %s/%s", request.Namespace, request.Name)

	// Fetch the ApplicationBackup instance
	pvc := &corev1.PersistentVolumeClaim{}
	err := p.client.Get(context.TODO(), request.NamespacedName, pvc)
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

	if err = p.handleSnapshotScheduleUpdates(pvc); err != nil {
		return reconcile.Result{RequeueAfter: 2 * time.Second}, err
	}

	return reconcile.Result{RequeueAfter: controllers.DefaultRequeue}, nil
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

func (p *PVCWatcher) handleSnapshotScheduleUpdates(pvc *corev1.PersistentVolumeClaim) error {
	// Nothing to do for deletions
	if pvc.DeletionTimestamp != nil {
		return nil
	}

	// Do nothing if the driver doesn't own the PVC or if it isn't bound yet
	if !p.volDriver.OwnsPVC(core.Instance(), pvc) || pvc.Status.Phase != corev1.ClaimBound {
		return nil
	}

	// Also skip if we've already configured the snapshot schedule for this PVC
	if configured, ok := pvc.Annotations[scheduleCreatedAnnotation]; ok && configured == "yes" {
		return nil
	}

	storageClassName := k8shelper.GetPersistentVolumeClaimClass(pvc)
	if storageClassName == "" {
		return nil
	}
	storageClass, err := storage.Instance().GetStorageClass(storageClassName)
	// Ignore if storageclass cannot be found
	if err != nil {
		if errors.IsNotFound(err) {
			return nil
		}
		return err
	}

	policiesMap, err := getPoliciesFromMap(storageClass.Parameters, pvc.Name+"-")
	if err != nil {
		return err
	}
	for snapshotScheduleName, policy := range policiesMap {
		schedulePolicyName := policy.SchedulePolicyName
		if _, err := storkops.Instance().GetSnapshotSchedule(snapshotScheduleName, pvc.Namespace); err == nil {
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
		_, err = storkops.Instance().CreateSnapshotSchedule(snapshotSchedule)
		if err != nil {
			p.recorder.Event(pvc,
				corev1.EventTypeWarning,
				"Error",
				fmt.Sprintf("Error creating snapshot schedule for PVC: %v", err))
			return err
		}
		p.recorder.Event(pvc,
			corev1.EventTypeNormal,
			"Success",
			fmt.Sprintf("Created volume snapshot schedule (%v) for PVC", snapshotScheduleName))
	}
	if len(policiesMap) > 0 {
		if pvc.Annotations == nil {
			pvc.Annotations = make(map[string]string)
		}
		pvc.Annotations[scheduleCreatedAnnotation] = "yes"
		_, err = core.Instance().UpdatePersistentVolumeClaim(pvc)
		if err != nil {
			return err
		}
	}

	return err
}
