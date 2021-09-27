package kopiadelete

import (
	"fmt"
	"strings"

	"github.com/portworx/kdmp/pkg/drivers"
	"github.com/portworx/kdmp/pkg/drivers/utils"
	"github.com/portworx/sched-ops/k8s/batch"
	"github.com/sirupsen/logrus"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	kopiaDeleteJobPrefix = "snapshot-delete"
)

// Driver is a kopia delete snapshot implementation
type Driver struct{}

// Name returns a name of the driver.
func (d Driver) Name() string {
	return drivers.KopiaDelete
}

// StartJob creates a job for kopia snapshot delete
func (d Driver) StartJob(opts ...drivers.JobOption) (id string, err error) {
	fn := "StartJob:"
	o := drivers.JobOpts{}
	for _, opt := range opts {
		if opt != nil {
			if err := opt(&o); err != nil {
				return "", err
			}
		}
	}

	if err := d.validate(o); err != nil {
		errMsg := fmt.Sprintf("validation failed for snapshot delete job for snapshotID [%v]: %v", o.SnapshotID, err)
		logrus.Infof("%s %v", fn, errMsg)
		return "", fmt.Errorf(errMsg)
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
	if utils.IsJobCompleted(job) {
		return utils.ToJobStatus(drivers.TransferProgressCompleted, ""), nil
	}
	return utils.ToJobStatus(0, ""), nil
}

func (d Driver) validate(o drivers.JobOpts) error {
	if o.SnapshotID == "" {
		return fmt.Errorf("snapshotID should be set")
	}
	if o.CredSecretName == "" {
		return fmt.Errorf("credential secret name should be set")
	}
	if o.CredSecretNamespace == "" {
		return fmt.Errorf("credential secret namespace  should be set")
	}
	if o.Namespace == "" {
		return fmt.Errorf("namespace should be set")
	}
	if o.SourcePVCName == "" {
		return fmt.Errorf("sourcePVC should be set")
	}
	return nil
}

func jobFor(
	jobName,
	jobNamespace,
	pvcName,
	pvcNamespace,
	credSecretName,
	credSecretNamespace,
	snapshotID string,
	resources corev1.ResourceRequirements,
	labels map[string]string) (*batchv1.Job, error) {

	labels = addJobLabels(labels)

	cmd := strings.Join([]string{
		"/kopiaexecutor",
		"delete",
		"--repository",
		toRepoName(pvcName, pvcNamespace),
		"--cred-secret-name",
		credSecretName,
		"--cred-secret-namespace",
		credSecretNamespace,
		"--snapshot-id",
		snapshotID,
	}, " ")

	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobName,
			Namespace: jobNamespace,
			Labels:    labels,
		},
		Spec: batchv1.JobSpec{
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels,
				},
				Spec: corev1.PodSpec{
					RestartPolicy:    corev1.RestartPolicyOnFailure,
					ImagePullSecrets: utils.ToImagePullSecret(utils.KopiaExecutorImageSecret()),
					Containers: []corev1.Container{
						{
							Name:  "kopiaexecutor",
							Image: utils.KopiaExecutorImage(),
							// TODO: Need to revert it to NotPresent. For now keep it as PullAlways.
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

func toJobName(jobName, snapshotID string) string {
	if jobName != "" {
		return jobName
	}
	return fmt.Sprintf("%s-%s", kopiaDeleteJobPrefix, snapshotID)
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
	resources, err := utils.JobResourceRequirements()
	if err != nil {
		return nil, err
	}

	return jobFor(
		jobName,
		o.JobNamespace,
		o.SourcePVCNamespace,
		o.SourcePVCName,
		o.CredSecretName,
		o.CredSecretNamespace,
		o.SnapshotID,
		resources,
		o.Labels,
	)
}
