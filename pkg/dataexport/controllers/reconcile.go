package controllers

import (
	"context"
	"fmt"
	"reflect"

	storkapi "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/operator-framework/operator-sdk/pkg/sdk"
	"github.com/portworx/sched-ops/k8s"
	"github.com/sirupsen/logrus"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Data export label names/keys.
const (
	LabelController     = "stork.libopenstorage.org/controller"
	LabelControllerName = "controller-name"
	DataExportName      = "data-export"
)

// controllerKind contains the schema.GroupVersionKind for this controller type.
var controllerKind = storkapi.SchemeGroupVersion.WithKind(reflect.TypeOf(storkapi.DataExport{}).Name())

// Sync manages data transfer and syncs DataExport resources.
func Sync(ctx context.Context, event sdk.Event) error {
	var vi *storkapi.DataExport
	var err error

	switch o := event.Object.(type) {
	case *storkapi.DataExport:
		vi = o
	case *batchv1.Job:
		if c := getControllerName(o.Labels); c != DataExportName {
			return nil
		}

		if vi, err = getDataExportFor(o); err != nil {
			if errors.IsNotFound(err) {
				return sdk.Delete(o)
			}
			return err
		}
	}

	return reconcile(vi)
}

func getDataExportFor(job *batchv1.Job) (*storkapi.DataExport, error) {
	controllerRef := metav1.GetControllerOf(job)
	if controllerRef == nil {
		// TODO: should such jobs be handled? (it has proper labels)
		return nil, fmt.Errorf("job %s/%s has no controllerRef", job.Namespace, job.Name)
	}

	return getDataExport(controllerRef.Name, job.Namespace)
}

func reconcile(de *storkapi.DataExport) error {
	if de == nil {
		return nil
	}

	new := &storkapi.DataExport{}
	reflect.Copy(reflect.ValueOf(new), reflect.ValueOf(de))

	logrus.Debugf("handling %s/%s DataExport", new.Namespace, new.Name)

	if new.DeletionTimestamp != nil {
		return deleteJob(toJobName(new.Name), new.Namespace)
	}

	if err := stageInitial(new); err != nil {
		return fmt.Errorf("stage initial: %v", err)
	}

	if err := stageTransfer(new); err != nil {
		return fmt.Errorf("stage transfer: %v", err)
	}

	if err := stageFinal(new); err != nil {
		return fmt.Errorf("stage final: %v", err)
	}

	if !reflect.DeepEqual(new, de) {
		return sdk.Update(new)
	}

	return nil
}

func stageInitial(de *storkapi.DataExport) error {
	if de.Status.Stage != "" {
		return nil
	}

	de.Status.Stage = storkapi.DataExportStageInitial
	de.Status.Status = storkapi.DataExportStatusInitial

	// ensure srd/dst volumes are available
	if err := checkClaims(de); err != nil {
		return updateStatus(de, err.Error())
	}

	return nil
}

func stageTransfer(de *storkapi.DataExport) error {
	// Stage: transfer in progress
	de.Status.Stage = storkapi.DataExportStageTransferScheduled

	// check if a rsync job is created
	viJob, err := getJob(toJobName(de.Name), de.Namespace)
	if err != nil {
		if !errors.IsNotFound(err) {
			return updateStatus(de, err.Error())
		}

		// create a job if it's not exist
		viJob = jobFrom(de)
		if err = sdk.Create(viJob); err != nil {
			return updateStatus(de, err.Error())
		}
	}

	// Stage: transfer in progress
	if !isJobCompleted(viJob) {
		de.Status.Stage = storkapi.DataExportStageTransferInProgress
		de.Status.Status = storkapi.DataExportStatusInProgress
		// TODO: update data transfer progress percentage

		return updateStatus(de, "")
	}

	return nil
}

func stageFinal(de *storkapi.DataExport) error {
	de.Status.Stage = storkapi.DataExportStageFinal
	// TODO: is it required to remove the rsync job? (it contains data transfer logs)

	de.Status.Status = storkapi.DataExportStatusSuccessful
	return nil
}

func updateStatus(de *storkapi.DataExport, errMsg string) error {
	if errMsg != "" {
		de.Status.Status = storkapi.DataExportStatusFailed
		de.Status.Reason = errMsg
	}
	return sdk.Update(de)
}

func checkClaims(de *storkapi.DataExport) error {
	srcPVC := de.Spec.Source.PersistentVolumeClaim
	if err := ensureUnmountedPVC(srcPVC.Name, srcPVC.Namespace, de.Name); err != nil {
		return fmt.Errorf("source pvc: %s/%s: %v", srcPVC.Namespace, srcPVC.Name, err)
	}

	dstPVC := de.Spec.Destination.PersistentVolumeClaim
	if err := ensureUnmountedPVC(dstPVC.Name, dstPVC.Namespace, de.Name); err != nil {
		// create pvc if it's not found
		if errors.IsNotFound(err) {
			_, err = createPVC(dstPVC)
		}

		return fmt.Errorf("destination pvc: %s/%s: %v", dstPVC.Namespace, dstPVC.Name, err)
	}

	return nil
}

func ensureUnmountedPVC(name, namespace, viName string) error {
	pvc, err := getPVC(name, namespace)
	if err != nil {
		return err
	}
	if pvc.Status.Phase != corev1.ClaimBound {
		return fmt.Errorf("status: expected %s, got %s", corev1.ClaimBound, pvc.Status.Phase)
	}

	// check if pvc is mounted
	pods, err := getMountPods(pvc.Name, pvc.Namespace)
	if err != nil {
		return fmt.Errorf("get mounted pods: %v", err)
	}
	mounted := make([]corev1.Pod, 0)
	for _, pod := range pods {
		// pvc is mounted to pod created for this volume
		if pod.Labels[LabelControllerName] == viName {
			continue
		}
		mounted = append(mounted, pod)
	}
	if len(mounted) > 0 {
		return fmt.Errorf("mounted to %v pods", toPodNames(pods))
	}

	return nil
}

func getMountPods(pvcName, namespace string) ([]corev1.Pod, error) {
	return k8s.Instance().GetPodsUsingPVC(pvcName, namespace)
}

func toPodNames(objs []corev1.Pod) []string {
	out := make([]string, 0)
	for _, o := range objs {
		out = append(out, o.Name)
	}
	return out
}

func jobLabels(DataExportName string) map[string]string {
	return map[string]string{
		LabelController:     DataExportName,
		LabelControllerName: DataExportName,
	}
}

func toJobName(DataExportName string) string {
	return fmt.Sprintf("job-%s", DataExportName)
}

func getControllerName(labels map[string]string) string {
	if labels == nil {
		return ""
	}
	return labels[LabelController]
}

func isJobCompleted(j *batchv1.Job) bool {
	for _, c := range j.Status.Conditions {
		if c.Type == batchv1.JobComplete && c.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

func jobFrom(vi *storkapi.DataExport) *batchv1.Job {
	return &batchv1.Job{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Job",
			APIVersion: "batch/v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:            toJobName(vi.Name),
			Namespace:       vi.Namespace,
			OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(vi, controllerKind)},
			Labels:          jobLabels(vi.Name),
		},
		Spec: batchv1.JobSpec{
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels:     jobLabels(vi.Name),
					Finalizers: nil,
				},
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyOnFailure,
					Containers: []corev1.Container{
						{
							Name:    "rsync",
							Image:   "eeacms/rsync",
							Command: []string{"/bin/sh", "-c", "rsync -avz /src/ /dst"},
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      "src-vol",
									MountPath: "/src",
								},
								{
									Name:      "dst-vol",
									MountPath: "/dst",
								},
							},
						},
					},
					Volumes: []corev1.Volume{
						{
							Name: "src-vol",
							VolumeSource: corev1.VolumeSource{
								PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
									ClaimName: vi.Spec.Source.PersistentVolumeClaim.Name,
								},
							},
						},
						{
							Name: "dst-vol",
							VolumeSource: corev1.VolumeSource{
								PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
									ClaimName: vi.Spec.Destination.PersistentVolumeClaim.Name,
								},
							},
						},
					},
				},
			},
		},
	}
}
