package kopiadelete

import (
	"fmt"
	"strings"
	"sync"

	kdmpapi "github.com/portworx/kdmp/pkg/apis/kdmp/v1alpha1"
	"github.com/portworx/kdmp/pkg/drivers"
	"github.com/portworx/kdmp/pkg/drivers/utils"
	"github.com/portworx/kdmp/pkg/jobratelimit"
	"github.com/portworx/sched-ops/k8s/batch"
	kdmpSchedOps "github.com/portworx/sched-ops/k8s/kdmp"
	"github.com/sirupsen/logrus"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	kopiaDeleteJobPrefix = "d"
)

// Driver is a kopia delete snapshot implementation
type Driver struct{}

// Name returns a name of the driver.
func (d Driver) Name() string {
	return drivers.KopiaDelete
}

var deleteJobLock sync.Mutex

// StartJob creates a job for kopia snapshot delete
func (d Driver) StartJob(opts ...drivers.JobOption) (id string, err error) {
	fn := "StartJob:"
	deleteJobLock.Lock()
	defer deleteJobLock.Unlock()
	o := drivers.JobOpts{}
	for _, opt := range opts {
		if opt != nil {
			if err := opt(&o); err != nil {
				return "", err
			}
		}
	}
	// Check whether there is slot to schedule delete job.
	driverType := d.Name()
	available, err := jobratelimit.CanJobBeScheduled(driverType)
	if err != nil {
		logrus.Errorf("%v", err)
		return "", err
	}
	if !available {
		return "", utils.ErrOutOfJobResources
	}
	if err := d.validate(o); err != nil {
		errMsg := fmt.Sprintf("validation failed for snapshot delete job for snapshotID [%v]: %v", o.SnapshotID, err)
		logrus.Infof("%s %v", fn, errMsg)
		return "", fmt.Errorf(errMsg)
	}
	labels := addVolumeBackupDeleteLabels(o)
	// Create the volumeBackupDelete CR to store the delete job status
	vd := &kdmpapi.VolumeBackupDelete{}
	vd.Name = o.VolumeBackupDeleteName
	vd.Annotations = map[string]string{
		utils.SkipResourceAnnotation: "true",
	}
	vd.Labels = labels
	vd.Namespace = o.VolumeBackupDeleteNamespace
	vd.Spec.PvcName = o.SourcePVCName
	vd.Spec.SnapshotID = o.SnapshotID
	_, err = kdmpSchedOps.Instance().CreateVolumeBackupDelete(vd)
	if err != nil && !apierrors.IsAlreadyExists(err) {
		errMsg := fmt.Sprintf("failed in creating volumeBackupDelete  [%s/%s]: %v", o.VolumeBackupDeleteName, o.VolumeBackupDeleteNamespace, err)
		logrus.Errorf("%s %v", fn, errMsg)
		return "", fmt.Errorf("%v", errMsg)
	}
	jobName := toJobName(o.JobName, o.SnapshotID)
	job, err := buildJob(jobName, o)
	if err != nil {
		errMsg := fmt.Sprintf("building backup snapshot delete job [%s] failed: %v", jobName, err)
		logrus.Errorf("%s %v", fn, errMsg)
		return "", fmt.Errorf(errMsg)
	}
	if _, err = batch.Instance().CreateJob(job); err != nil && !apierrors.IsAlreadyExists(err) {
		errMsg := fmt.Sprintf("creation of backup snapshot delete job [%s] failed: %v", jobName, err)
		logrus.Errorf("%s %v", fn, errMsg)
		return "", fmt.Errorf(errMsg)
	}
	logrus.Infof("%s created backup snapshot delete job [%s] successfully", fn, job.Name)
	return utils.NamespacedName(job.Namespace, job.Name), nil
}

// DeleteJob delete the backup snapshot delete job.
func (d Driver) DeleteJob(id string) error {
	fn := "DeleteJob:"
	namespace, name, err := utils.ParseJobID(id)
	if err != nil {
		logrus.Errorf("%s %v", fn, err)
		return err
	}
	err = kdmpSchedOps.Instance().DeleteVolumeBackupDelete(name, namespace)
	if err != nil && !apierrors.IsNotFound(err) {
		errMsg := fmt.Sprintf("failed to delete volumeBackupDelete CR [%v]: %v", id, err)
		logrus.Errorf("%v", errMsg)
		return fmt.Errorf(errMsg)
	}
	if err = batch.Instance().DeleteJob(name, namespace); err != nil && !apierrors.IsNotFound(err) {
		errMsg := fmt.Sprintf("deletion of delete snapshot job [%s/%s] failed: %v", namespace, name, err)
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
		return utils.ToJobStatus(0, err.Error(), batchv1.JobConditionType("")), nil
	}

	job, err := batch.Instance().GetJob(name, namespace)
	if err != nil {
		errMsg := fmt.Sprintf("failed to fetch backup %s/%s job: %v", namespace, name, err)
		logrus.Errorf("%s: %v", fn, errMsg)
		return nil, fmt.Errorf(errMsg)
	}
	err = utils.JobNodeExists(job)
	if err != nil {
		errMsg := fmt.Sprintf("failed to fetch the node info tied to the job %s/%s: %v", namespace, name, err)
		logrus.Errorf("%s: %v", fn, errMsg)
		return nil, fmt.Errorf(errMsg)
	}
	var jobStatus batchv1.JobConditionType
	if len(job.Status.Conditions) != 0 {
		jobStatus = job.Status.Conditions[0].Type

	}
	if err != nil {
		errMsg := fmt.Sprintf("failed to get restart count for job  %s/%s job: %v", namespace, name, err)
		logrus.Errorf("%s: %v", fn, errMsg)
		return nil, fmt.Errorf(errMsg)
	}

	if utils.IsJobFailed(job) {
		errMsg := fmt.Sprintf("check %s/%s job for details: %s", namespace, name, drivers.ErrJobFailed)
		return utils.ToJobStatus(0, errMsg, jobStatus), nil
	}
	if utils.IsJobCompleted(job) {
		return utils.ToJobStatus(drivers.TransferProgressCompleted, "", jobStatus), nil
	}
	return utils.ToJobStatus(0, "", jobStatus), nil
}

func (d Driver) validate(o drivers.JobOpts) error {
	return nil
}

func jobFor(
	jobOption drivers.JobOpts,
	jobName string,
	resources corev1.ResourceRequirements,
	labels map[string]string,
) (*batchv1.Job, error) {
	cmd := strings.Join([]string{
		"/kopiaexecutor",
		"delete",
		"--repository",
		toRepoName(jobOption.SourcePVCName, jobOption.SourcePVCNamespace),
		"--cred-secret-name",
		jobOption.CredSecretName,
		"--cred-secret-namespace",
		jobOption.CredSecretNamespace,
		"--snapshot-id",
		jobOption.SnapshotID,
		"--volume-backup-delete-name",
		jobOption.VolumeBackupDeleteName,
		"--volume-backup-delete-namespace",
		jobOption.VolumeBackupDeleteNamespace,
	}, " ")

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobName,
			Namespace: jobOption.JobNamespace,
			Annotations: map[string]string{
				utils.SkipResourceAnnotation: "true",
			},
			Labels: labels,
		},
		Spec: batchv1.JobSpec{
			BackoffLimit: &utils.JobPodBackOffLimit,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels,
				},
				Spec: corev1.PodSpec{
					RestartPolicy:      corev1.RestartPolicyOnFailure,
					ServiceAccountName: jobOption.ServiceAccountName,
					ImagePullSecrets:   utils.ToImagePullSecret(utils.KopiaExecutorImageSecret()),
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
									Name:      "cred-secret",
									MountPath: drivers.KopiaCredSecretMount,
									ReadOnly:  true,
								},
							},
						},
					},
					Volumes: []corev1.Volume{
						{
							Name: "cred-secret",
							VolumeSource: corev1.VolumeSource{
								Secret: &corev1.SecretVolumeSource{
									SecretName: jobOption.CredSecretName,
								},
							},
						},
					},
				},
			},
		},
	}

	if drivers.CertFilePath != "" {
		volumeMount := corev1.VolumeMount{
			Name:      utils.TLSCertMountVol,
			MountPath: drivers.CertMount,
			ReadOnly:  true,
		}

		job.Spec.Template.Spec.Containers[0].VolumeMounts = append(
			job.Spec.Template.Spec.Containers[0].VolumeMounts,
			volumeMount,
		)

		volume := corev1.Volume{
			Name: utils.TLSCertMountVol,
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName: jobOption.CertSecretName,
				},
			},
		}

		job.Spec.Template.Spec.Volumes = append(job.Spec.Template.Spec.Volumes, volume)

		env := []corev1.EnvVar{
			{
				Name:  drivers.CertDirPath,
				Value: drivers.CertMount,
			},
		}

		job.Spec.Template.Spec.Containers[0].Env = env
	}

	return job, nil
}

func toJobName(jobName, snapshotID string) string {
	if jobName != "" {
		return jobName
	}
	return fmt.Sprintf("%s-%s", kopiaDeleteJobPrefix, snapshotID)
}

func toRepoName(pvcName, pvcNamespace string) string {
	return fmt.Sprintf("%s-%s", pvcNamespace, pvcName)
}

func addVolumeBackupDeleteLabels(jobOpts drivers.JobOpts) map[string]string {
	labels := make(map[string]string)
	labels[utils.BackupObjectNameKey] = jobOpts.BackupObjectName
	labels[utils.BackupObjectUIDKey] = jobOpts.BackupObjectUID
	return labels
}

func addJobLabels(labels map[string]string, jobOpts drivers.JobOpts) map[string]string {
	if labels == nil {
		labels = make(map[string]string)
	}

	labels[drivers.DriverNameLabel] = drivers.KopiaDelete
	labels[utils.BackupObjectNameKey] = jobOpts.BackupObjectName
	labels[utils.BackupObjectUIDKey] = jobOpts.BackupObjectUID
	return labels
}

func buildJob(jobName string, jobOpts drivers.JobOpts) (*batchv1.Job, error) {
	resources, err := utils.KopiaResourceRequirements()
	if err != nil {
		return nil, err
	}

	labels := addJobLabels(jobOpts.Labels, jobOpts)
	return jobFor(
		jobOpts,
		jobName,
		resources,
		labels,
	)
}
