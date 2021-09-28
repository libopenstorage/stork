package kopiabackup

import (
	"context"
	"fmt"
	"strings"

	"github.com/portworx/kdmp/pkg/drivers"
	"github.com/portworx/kdmp/pkg/drivers/utils"
	kdmpops "github.com/portworx/kdmp/pkg/util/ops"
	"github.com/portworx/sched-ops/k8s/batch"
	coreops "github.com/portworx/sched-ops/k8s/core"
	"github.com/sirupsen/logrus"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	// SkipResourceAnnotation skipping kopia secret to be backed up
	SkipResourceAnnotation = "stork.libopenstorage.org/skip-resource"
)

// Driver is a resticbackup implementation of the data export interface.
type Driver struct{}

// Name returns a name of the driver.
func (d Driver) Name() string {
	return drivers.KopiaBackup
}

// StartJob creates a job for data transfer between volumes.
func (d Driver) StartJob(opts ...drivers.JobOption) (id string, err error) {
	fn := "StartJob"
	o := drivers.JobOpts{}
	for _, opt := range opts {
		if opt != nil {
			if err := opt(&o); err != nil {
				return "", err
			}
		}
	}

	if err := d.validate(o); err != nil {
		logrus.Errorf("%s validate: err: %v", fn, err)
		return "", err
	}
	// DataExportName will be unique name when also generated from stork
	// if there are multiple backups being triggered
	jobName := toJobName(o.DataExportName)
	logrus.Debugf("backup jobname: %s", jobName)
	job, err := buildJob(jobName, o)
	if err != nil {
		errMsg := fmt.Sprintf("building backup job %s failed: %v", jobName, err)
		logrus.Errorf("%s: %v", fn, errMsg)
		return "", fmt.Errorf(errMsg)
	}
	if _, err = batch.Instance().CreateJob(job); err != nil && !apierrors.IsAlreadyExists(err) {
		errMsg := fmt.Sprintf("creation of backup job %s failed: %v", jobName, err)
		logrus.Errorf("%s: %v", fn, errMsg)
		return "", fmt.Errorf(errMsg)
	}

	return utils.NamespacedName(job.Namespace, job.Name), nil
}

// DeleteJob stops data transfer between volumes.
func (d Driver) DeleteJob(id string) error {
	fn := "DeleteJob"
	namespace, name, err := utils.ParseJobID(id)
	if err != nil {
		return err
	}

	logrus.Infof("Delete job: name: %v, namespace: %v", name, namespace)
	err = kdmpops.Instance().DeleteVolumeBackup(context.Background(), name, namespace)
	if err != nil && !apierrors.IsNotFound(err) {
		errMsg := fmt.Sprintf("failed to delete VolumeBackup CR %v: %v", name, err)
		return fmt.Errorf(errMsg)
	}

	if err := coreops.Instance().DeleteSecret(name, namespace); err != nil && !apierrors.IsNotFound(err) {
		errMsg := fmt.Sprintf("deletion of backup credential secret %s failed: %v", name, err)
		logrus.Errorf("%s: %v", fn, errMsg)
		return fmt.Errorf(errMsg)
	}
	if err := utils.CleanServiceAccount(name, namespace); err != nil {
		errMsg := fmt.Sprintf("deletion of service account %s/%s failed: %v", namespace, name, err)
		logrus.Errorf("%s: %v", fn, errMsg)
		return fmt.Errorf(errMsg)
	}

	if err = batch.Instance().DeleteJob(name, namespace); err != nil && !apierrors.IsNotFound(err) {
		errMsg := fmt.Sprintf("deletion of backup job %s/%s failed: %v", namespace, name, err)
		logrus.Errorf("%s: %v", fn, errMsg)
		return fmt.Errorf(errMsg)
	}

	return nil
}

// JobStatus returns a progress status for a data transfer.
func (d Driver) JobStatus(id string) (*drivers.JobStatus, error) {
	fn := "JobStatus"
	namespace, name, err := utils.ParseJobID(id)
	if err != nil {
		return utils.ToJobStatus(0, err.Error()), nil
	}

	job, err := batch.Instance().GetJob(name, namespace)
	if err != nil {
		errMsg := fmt.Sprintf("failed to fetch backup %s/%s job: %v", namespace, name, err)
		logrus.Errorf("%s: %v", fn, errMsg)
		return nil, fmt.Errorf(errMsg)
	}
	if utils.IsJobFailed(job) {
		errMsg := fmt.Sprintf("check %s/%s job for details: %s", namespace, name, drivers.ErrJobFailed)
		return utils.ToJobStatus(0, errMsg), nil
	}

	vb, err := kdmpops.Instance().GetVolumeBackup(context.Background(), name, namespace)
	if err != nil {
		errMsg := fmt.Sprintf("failed to fetch volumebackup %s/%s status: %v", namespace, name, err)
		logrus.Errorf("%s: %v", fn, errMsg)
		return nil, fmt.Errorf(errMsg)
	}

	return utils.ToJobStatus(vb.Status.ProgressPercentage, vb.Status.LastKnownError), nil
}

func (d Driver) validate(o drivers.JobOpts) error {
	if o.BackupLocationName == "" {
		return fmt.Errorf("backuplocation name should be set")
	}
	if o.BackupLocationNamespace == "" {
		return fmt.Errorf("backuplocation namespace should be set")
	}
	return nil
}

func jobFor(
	jobName,
	namespace,
	pvcName,
	credSecretName,
	backuplocationName,
	backuplocationNamespace,
	backupNamespace string,
	resources corev1.ResourceRequirements,
	labels map[string]string) (*batchv1.Job, error) {
	backupName := jobName

	labels = addJobLabels(labels)

	cmd := strings.Join([]string{
		"/kopiaexecutor",
		"backup",
		"--volume-backup-name",
		backupName,
		"--repository",
		toRepoName(pvcName, namespace),
		"--credentials",
		credSecretName,
		"--backup-location",
		backuplocationName,
		"--backup-location-namespace",
		backuplocationNamespace,
		"--backup-namespace",
		backupNamespace,
		"--source-path",
		"/data",
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
							ImagePullPolicy: corev1.PullIfNotPresent,
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

func toJobName(sourcePVCName string) string {
	return fmt.Sprintf("%s-%s", utils.BackupJobPrefix, sourcePVCName)
}

func toRepoName(pvcName, pvcNamespace string) string {
	return fmt.Sprintf("%s-%s", pvcNamespace, pvcName)
}

func addJobLabels(labels map[string]string) map[string]string {
	if labels == nil {
		labels = make(map[string]string)
	}

	labels[drivers.DriverNameLabel] = drivers.KopiaBackup
	return labels
}

func buildJob(jobName string, o drivers.JobOpts) (*batchv1.Job, error) {
	fn := "buildJob"
	resources, err := utils.JobResourceRequirements()
	if err != nil {
		return nil, err
	}
	if err := utils.SetupServiceAccount(jobName, o.Namespace, roleFor()); err != nil {
		errMsg := fmt.Sprintf("error creating service account %s/%s: %v", o.Namespace, jobName, err)
		logrus.Errorf("%s: %v", fn, errMsg)
		return nil, fmt.Errorf(errMsg)
	}

	pods, err := coreops.Instance().GetPodsUsingPVC(o.SourcePVCName, o.Namespace)
	if err != nil {
		errMsg := fmt.Sprintf("error fetching pods using PVC %s/%s: %v", o.Namespace, o.SourcePVCName, err)
		logrus.Errorf("%s: %v", fn, errMsg)
		return nil, fmt.Errorf(errMsg)
	}

	// run a "live" backup if a pvc is mounted (mount a kubelet directory with pod volumes)
	if len(pods) > 0 {
		return jobForLiveBackup(
			jobName,
			o.Namespace,
			o.SourcePVCName,
			utils.FrameCredSecretName(utils.BackupJobPrefix, o.DataExportName),
			o.BackupLocationName,
			o.BackupLocationNamespace,
			o.Namespace,
			pods[0],
			resources,
			o.Labels,
		)
	}

	return jobFor(
		jobName,
		o.Namespace,
		o.SourcePVCName,
		utils.FrameCredSecretName(utils.BackupJobPrefix, o.DataExportName),
		o.BackupLocationName,
		o.BackupLocationNamespace,
		o.Namespace,
		resources,
		o.Labels,
	)
}

func roleFor() *rbacv1.Role {
	return &rbacv1.Role{
		Rules: []rbacv1.PolicyRule{
			{
				APIGroups: []string{"kdmp.portworx.com"},
				Resources: []string{"volumebackups"},
				Verbs:     []string{rbacv1.VerbAll},
			},
		},
	}
}
