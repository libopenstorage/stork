package kopiarestore

import (
	"context"
	"fmt"
	"strings"

	"github.com/portworx/kdmp/pkg/drivers"
	"github.com/portworx/kdmp/pkg/drivers/utils"
	kdmpops "github.com/portworx/kdmp/pkg/util/ops"
	"github.com/portworx/sched-ops/k8s/batch"
	coreops "github.com/portworx/sched-ops/k8s/core"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Driver is a kopiarestore implementation of the data export interface.
type Driver struct{}

// Name returns a name of the driver.
func (d Driver) Name() string {
	return drivers.KopiaRestore
}

// StartJob creates a job for data transfer between volumes.
func (d Driver) StartJob(opts ...drivers.JobOption) (id string, err error) {
	o := drivers.JobOpts{}
	for _, opt := range opts {
		if opt != nil {
			if err := opt(&o); err != nil {
				return "", err
			}
		}
	}

	if err := d.validate(o); err != nil {
		return "", err
	}
	vb, err := kdmpops.Instance().GetVolumeBackup(context.Background(), o.VolumeBackupName, o.VolumeBackupNamespace)
	if err != nil {
		return "", err
	}

	jobName := toJobName(o.DestinationPVCName)
	job, err := jobFor(
		jobName,
		o.Namespace,
		o.DestinationPVCName,
		o.VolumeBackupName,
		utils.FrameCredSecretName(utils.RestoreJobPrefix, o.DataExportName),
		vb.Spec.BackupLocation.Name,
		vb.Spec.BackupLocation.Namespace,
		o.Namespace,
		vb.Status.SnapshotID,
		vb.Spec.Repository,
		o.Labels)
	if err != nil {
		return "", err
	}
	if _, err = batch.Instance().CreateJob(job); err != nil && !apierrors.IsAlreadyExists(err) {
		return "", err
	}

	return utils.NamespacedName(job.Namespace, job.Name), nil
}

// DeleteJob stops data transfer between volumes.
func (d Driver) DeleteJob(id string) error {
	namespace, name, err := utils.ParseJobID(id)
	if err != nil {
		return err
	}

	if err := coreops.Instance().DeleteSecret(name, namespace); err != nil && !apierrors.IsNotFound(err) {
		return err
	}

	if err := utils.CleanServiceAccount(name, namespace); err != nil {
		return err
	}

	if err = batch.Instance().DeleteJob(name, namespace); err != nil && !apierrors.IsNotFound(err) {
		return err
	}

	return nil
}

// JobStatus returns a progress status for a data transfer.
func (d Driver) JobStatus(id string) (*drivers.JobStatus, error) {
	namespace, name, err := utils.ParseJobID(id)
	if err != nil {
		return utils.ToJobStatus(0, err.Error()), nil
	}

	job, err := batch.Instance().GetJob(name, namespace)
	if err != nil {
		return nil, err
	}
	if utils.IsJobFailed(job) {
		errMsg := fmt.Sprintf("check %s/%s job for details: %s", namespace, name, drivers.ErrJobFailed)
		return utils.ToJobStatus(0, errMsg), nil
	}
	if !utils.IsJobCompleted(job) {
		// TODO: update progress
		return utils.ToJobStatus(0, ""), nil
	}

	return utils.ToJobStatus(drivers.TransferProgressCompleted, ""), nil
}

func (d Driver) validate(o drivers.JobOpts) error {
	if o.DestinationPVCName == "" {
		return fmt.Errorf("destination pvc name should be set")
	}
	if o.VolumeBackupName == "" {
		return fmt.Errorf("volumebackup name should be set")
	}
	if o.VolumeBackupNamespace == "" {
		return fmt.Errorf("volumebackup namespace should be set")
	}
	return nil
}

func jobFor(
	jobName,
	namespace,
	pvcName,
	volumeBackupName,
	credSecretName,
	backuplocationName,
	backuplocationNamespace,
	restoreNamespace,
	snapshotID,
	repository string,
	labels map[string]string) (*batchv1.Job, error) {
	labels = addJobLabels(labels)

	resources, err := utils.JobResourceRequirements()
	if err != nil {
		return nil, err
	}

	if err := utils.SetupServiceAccount(jobName, namespace, roleFor()); err != nil {
		return nil, err
	}

	cmd := strings.Join([]string{
		"/kopiaexecutor",
		"restore",
		"--volume-backup-name",
		volumeBackupName,
		"--backup-location",
		backuplocationName,
		"--backup-location-namespace",
		backuplocationNamespace,
		"--repository",
		repository,
		"--restore-namespace",
		restoreNamespace,
		"--credentials",
		credSecretName,
		"--target-path",
		"/data",
		"--snapshot-id",
		snapshotID,
	}, " ")

	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobName,
			Namespace: namespace,
			Labels:    labels,
		},
		Spec: batchv1.JobSpec{
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels,
				},
				Spec: corev1.PodSpec{
					RestartPolicy:      corev1.RestartPolicyOnFailure,
					ImagePullSecrets:   utils.ToImagePullSecret(utils.KopiaExecutorImageSecret()),
					ServiceAccountName: jobName,
					Containers: []corev1.Container{
						{
							Name:            "kopiaexecutor",
							Image:           utils.KopiaExecutorImage(),
							ImagePullPolicy: corev1.PullAlways,
							Command: []string{
								"/bin/sh",
								"-x",
								"-c",
								cmd,
							},
							Resources: resources,
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      "vol",
									MountPath: "/data",
								},
								{
									Name:      "cred-secret",
									MountPath: drivers.KopiaCredSecretMount,
									ReadOnly:  true,
								},
							},
						},
					},
					Volumes: []corev1.Volume{
						{
							Name: "vol",
							VolumeSource: corev1.VolumeSource{
								PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
									ClaimName: pvcName,
								},
							},
						},
						{
							Name: "cred-secret",
							VolumeSource: corev1.VolumeSource{
								Secret: &corev1.SecretVolumeSource{
									SecretName: credSecretName,
								},
							},
						},
					},
				},
			},
		},
	}, nil
}

func toJobName(destinationPVC string) string {
	return fmt.Sprintf("%s-%s", utils.RestoreJobPrefix, destinationPVC)
}

func addJobLabels(labels map[string]string) map[string]string {
	if labels == nil {
		labels = make(map[string]string)
	}

	labels[drivers.DriverNameLabel] = drivers.KopiaRestore
	return labels
}

func roleFor() *rbacv1.Role {
	return &rbacv1.Role{
		Rules: []rbacv1.PolicyRule{
			{
				APIGroups: []string{"stork.libopenstorage.org"},
				Resources: []string{"backuplocations"},
				Verbs:     []string{"get", "list"},
			},
			{
				APIGroups: []string{"kdmp.portworx.com"},
				Resources: []string{"volumebackups"},
				Verbs:     []string{rbacv1.VerbAll},
			},
		},
	}
}
