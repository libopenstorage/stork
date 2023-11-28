package nfsdelete

import (
	"fmt"
	"strings"
	"sync"

	"github.com/portworx/kdmp/pkg/drivers"
	"github.com/portworx/kdmp/pkg/drivers/utils"
	"github.com/portworx/kdmp/pkg/jobratelimit"
	"github.com/portworx/sched-ops/k8s/batch"
	"github.com/sirupsen/logrus"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Driver is an implementation of resource delete in NFS.
type Driver struct{}

// Name returns a name of the driver.
func (d Driver) Name() string {
	return drivers.NFSBackup
}

var deleteJobLock sync.Mutex

// StartJob creates a job for resource delete.
func (d Driver) StartJob(opts ...drivers.JobOption) (id string, err error) {
	fn := "StartJob"
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
	available, err := jobratelimit.CanJobBeScheduled(d.Name())
	if err != nil {
		logrus.Errorf("%v", err)
		return "", err
	}
	if !available {
		return "", utils.ErrOutOfJobResources
	}

	job, err := buildJob(o)
	if err != nil {
		errMsg := fmt.Sprintf("building of resource delete job [%s] failed: %v", job.Name, err)
		logrus.Errorf("%s %v", fn, errMsg)
		return "", fmt.Errorf(errMsg)
	}

	// Create PV & PVC only in case of NFS.
	if o.NfsServer != "" {
		err := utils.CreateNFSPvPvcForJob(o.JobName, job.ObjectMeta.Namespace, o)
		if err != nil {
			return "", err
		}
	}

	if _, err = batch.Instance().CreateJob(job); err != nil && !apierrors.IsAlreadyExists(err) {
		errMsg := fmt.Sprintf("creation of resource delete job %s failed: %v", job.Name, err)
		logrus.Errorf("%s: %v", fn, errMsg)
		return "", fmt.Errorf(errMsg)
	}

	logrus.Infof("%s created resource delete job [%s] successfully", fn, job.Name)
	return utils.NamespacedName(job.Namespace, job.Name), nil

}

// DeleteJob deletes the resource delete job.
func (d Driver) DeleteJob(id string) error {
	fn := "DeleteJob:"
	namespace, name, err := utils.ParseJobID(id)
	if err != nil {
		logrus.Errorf("%s %v", fn, err)
		return err
	}
	if err = batch.Instance().DeleteJob(name, namespace); err != nil && !apierrors.IsNotFound(err) {
		errMsg := fmt.Sprintf("deletion of resource delete job [%s/%s] failed: %v", namespace, name, err)
		logrus.Errorf("%s: %v", fn, errMsg)
		return fmt.Errorf(errMsg)
	}

	return nil
}

// JobStatus fetches job status
func (d Driver) JobStatus(id string) (*drivers.JobStatus, error) {
	fn := "JobStatus"
	namespace, name, err := utils.ParseJobID(id)
	if err != nil {
		return utils.ToJobStatus(0, err.Error(), batchv1.JobConditionType("")), nil
	}

	job, err := batch.Instance().GetJob(name, namespace)
	if err != nil {
		errMsg := fmt.Sprintf("failed to fetch resource delete %s/%s job: %v", namespace, name, err)
		logrus.Errorf("%s: %v", fn, errMsg)
		return nil, fmt.Errorf(errMsg)
	}
	// Check for mount point failure
	mountFailed := utils.IsJobPodMountFailed(job, namespace)
	if mountFailed {
		errMsg := fmt.Sprintf("job [%v/%v] failed while mounting NFS mount endpoint", namespace, name)
		return utils.ToJobStatus(0, errMsg, batchv1.JobFailed), nil
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

	if utils.IsJobFailed(job) {
		errMsg := fmt.Sprintf("check %s/%s job for details: %s", namespace, name, drivers.ErrJobFailed)
		return utils.ToJobStatus(0, errMsg, jobStatus), nil
	}
	if utils.IsJobCompleted(job) {
		return utils.ToJobStatus(drivers.TransferProgressCompleted, "", jobStatus), nil
	}
	return utils.ToJobStatus(0, "", jobStatus), nil
}

func buildJob(
	jobOptions drivers.JobOpts,
) (*batchv1.Job, error) {
	resources, err := utils.NFSResourceRequirements(jobOptions.JobConfigMap, jobOptions.JobConfigMapNs)
	if err != nil {
		return nil, err
	}
	labels := addJobLabels(jobOptions.Labels, jobOptions)
	return jobForDeleteResource(jobOptions, resources, labels)
}

func addJobLabels(labels map[string]string, jobOpts drivers.JobOpts) map[string]string {
	if labels == nil {
		labels = make(map[string]string)
	}

	labels[drivers.DriverNameLabel] = drivers.NFSDelete
	labels[utils.BackupObjectNameKey] = jobOpts.BackupObjectName
	labels[utils.BackupObjectUIDKey] = jobOpts.BackupObjectUID
	return labels
}

func jobForDeleteResource(
	jobOption drivers.JobOpts,
	resources corev1.ResourceRequirements,
	labels map[string]string,
) (*batchv1.Job, error) {
	cmd := strings.Join([]string{
		"/nfsexecutor",
		"delete",
		"--app-cr-name",
		jobOption.AppCRName,
		"--namespace",
		jobOption.AppCRNamespace,
	}, " ")

	nfsExecutorImage, imageRegistrySecret, err := utils.GetExecutorImageAndSecret(drivers.NfsExecutorImage,
		jobOption.NfsImageExecutorSource,
		jobOption.NfsImageExecutorSourceNs,
		jobOption.JobName,
		jobOption)
	if err != nil {
		logrus.Errorf("failed to get the executor image details")
		return nil, fmt.Errorf("failed to get the executor image details for job %s", jobOption.JobName)
	}
	tolerations, err := utils.GetTolerationsFromDeployment(jobOption.NfsImageExecutorSource,
		jobOption.NfsImageExecutorSourceNs)
	if err != nil {
		logrus.Errorf("failed to get the toleration details: %v", err)
		return nil, fmt.Errorf("failed to get the toleration details for job [%s/%s]", jobOption.Namespace, jobOption.JobName)
	}
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobOption.JobName,
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
					Containers: []corev1.Container{
						{
							Name:            "nfsexecutor",
							Image:           nfsExecutorImage,
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
					Tolerations: tolerations,
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
	// Add the image secret in job spec only if it is present in the stork deployment.
	if len(imageRegistrySecret) != 0 {
		job.Spec.Template.Spec.ImagePullSecrets = utils.ToImagePullSecret(utils.GetImageSecretName(jobOption.JobName))
	}
	if len(jobOption.NfsServer) != 0 {
		volumeMount := corev1.VolumeMount{
			Name:      utils.NfsVolumeName,
			MountPath: drivers.NfsMount,
		}
		job.Spec.Template.Spec.Containers[0].VolumeMounts = append(
			job.Spec.Template.Spec.Containers[0].VolumeMounts,
			volumeMount,
		)
		volume := corev1.Volume{
			Name: utils.NfsVolumeName,
			VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
					ClaimName: utils.GetPvcNameForJob(jobOption.JobName),
				},
			},
		}

		job.Spec.Template.Spec.Volumes = append(job.Spec.Template.Spec.Volumes, volume)
	}

	nodeAffinity, err := utils.GetNodeAffinityFromDeployment(jobOption.NfsImageExecutorSource,
		jobOption.NfsImageExecutorSourceNs)
	if err != nil {
		logrus.Errorf("failed to get the node affinity details: %v", err)
		return nil, fmt.Errorf("failed to get the node affinity details for job [%s/%s]", jobOption.Namespace, jobOption.JobName)
	}
	job.Spec.Template.Spec.Affinity = &corev1.Affinity{
		NodeAffinity: nodeAffinity,
	}

	return job, nil
}
