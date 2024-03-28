package kopiarestore

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/portworx/kdmp/pkg/apis/kdmp/v1alpha1"
	"github.com/portworx/kdmp/pkg/drivers"
	"github.com/portworx/kdmp/pkg/drivers/utils"
	"github.com/portworx/kdmp/pkg/jobratelimit"
	kdmpops "github.com/portworx/kdmp/pkg/util/ops"
	"github.com/portworx/sched-ops/k8s/batch"
	"github.com/sirupsen/logrus"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var restoreJobLock sync.Mutex

// Driver is a kopiarestore implementation of the data export interface.
type Driver struct{}

// Name returns a name of the driver.
func (d Driver) Name() string {
	return drivers.KopiaRestore
}

// StartJob creates a job for data transfer between volumes.
func (d Driver) StartJob(opts ...drivers.JobOption) (id string, err error) {
	restoreJobLock.Lock()
	defer restoreJobLock.Unlock()

	o := drivers.JobOpts{}
	for _, opt := range opts {
		if opt != nil {
			if err := opt(&o); err != nil {
				return "", err
			}
		}
	}
	// Check whether there is slot to schedule restore job.
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
		return "", err
	}
	vb, err := kdmpops.Instance().GetVolumeBackup(context.Background(), o.VolumeBackupName, o.VolumeBackupNamespace)
	if err != nil {
		return "", err
	}

	jobName := o.DataExportName
	job, err := jobFor(
		o,
		vb,
		jobName,
	)
	if err != nil {
		return "", err
	}

	// Create PV & PVC only in case of NFS.
	if o.NfsServer != "" {
		err := utils.CreateNFSPvPvcForJob(jobName, job.ObjectMeta.Namespace, o)
		if err != nil {
			return "", err
		}
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
	fn := "JobStatus:"
	namespace, name, err := utils.ParseJobID(id)
	if err != nil {
		return utils.ToJobStatus(0, err.Error(), batchv1.JobConditionType("")), nil
	}

	job, err := batch.Instance().GetJob(name, namespace)
	if err != nil {
		return nil, err
	}
	// Check whether mount point failure
	mountFailed := utils.IsJobPodMountFailed(job, namespace)
	if mountFailed {
		utils.DisplayJobpodLogandEvents(job.Name, job.Namespace)
		errMsg := fmt.Sprintf("job [%v/%v] failed to mount pvc, please check job pod's description for more detail", namespace, name)
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
		utils.DisplayJobpodLogandEvents(job.Name, job.Namespace)
		errMsg := fmt.Sprintf("check %s/%s job for details: %s", namespace, name, drivers.ErrJobFailed)
		return utils.ToJobStatus(0, errMsg, jobStatus), nil
	}
	if utils.IsJobPending(job) {
		logrus.Warnf("restore job %s is in pending state", job.Name)
		return utils.ToJobStatus(0, "", jobStatus), nil
	}

	if !utils.IsJobCompleted(job) {
		// TODO: update progress
		return utils.ToJobStatus(0, "", jobStatus), nil
	}

	return utils.ToJobStatus(drivers.TransferProgressCompleted, "", jobStatus), nil
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
	jobOption drivers.JobOpts,
	vb *v1alpha1.VolumeBackup,
	jobName string,
) (*batchv1.Job, error) {
	podUserId := int64(777)
	notAllowed := false
	allowed := true
	labels := addJobLabels(jobOption)

	resources, err := utils.KopiaResourceRequirements(jobOption.JobConfigMap, jobOption.JobConfigMapNs)
	if err != nil {
		return nil, err
	}

	if err := utils.SetupServiceAccount(jobName, jobOption.Namespace, roleFor()); err != nil {
		return nil, err
	}

	cmd := strings.Join([]string{
		"/kopiaexecutor",
		"restore",
		"--volume-backup-name",
		jobOption.VolumeBackupName,
		"--backup-location",
		vb.Spec.BackupLocation.Name,
		"--backup-location-namespace",
		vb.Spec.BackupLocation.Namespace,
		"--repository",
		vb.Spec.Repository,
		"--restore-namespace",
		jobOption.Namespace,
		"--credentials",
		jobOption.DataExportName,
		"--target-path",
		"/data",
		"--snapshot-id",
		vb.Status.SnapshotID,
	}, " ")

	kopiaExecutorImage, imageRegistrySecret, err := utils.GetExecutorImageAndSecret(drivers.KopiaExecutorImage,
		jobOption.KopiaImageExecutorSource,
		jobOption.KopiaImageExecutorSourceNs,
		jobName,
		jobOption)
	if err != nil {
		errMsg := fmt.Errorf("failed to get the executor image details for job %s", jobName)
		logrus.Errorf("%v", errMsg)
		return nil, errMsg
	}
	tolerations, err := utils.GetTolerationsFromDeployment(jobOption.KopiaImageExecutorSource,
		jobOption.KopiaImageExecutorSourceNs)
	if err != nil {
		logrus.Errorf("failed to get the toleration details: %v", err)
		return nil, fmt.Errorf("failed to get the toleration details for job [%s/%s]", jobOption.Namespace, jobName)
	}
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobName,
			Namespace: jobOption.Namespace,
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
					ServiceAccountName: jobName,
					Containers: []corev1.Container{
						{
							Name:            "kopiaexecutor",
							Image:           kopiaExecutorImage,
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
							SecurityContext: &corev1.SecurityContext{
								RunAsNonRoot:             &(allowed),
								RunAsUser:                &podUserId,
								RunAsGroup:               &podUserId,
								AllowPrivilegeEscalation: &(notAllowed),
								SeccompProfile: &corev1.SeccompProfile{
									Type: "RuntimeDefault",
								},
								Capabilities: &corev1.Capabilities{
									Drop: []corev1.Capability{
										"ALL",
									},
								},
							},
						},
					},
					Tolerations: tolerations,
					Volumes: []corev1.Volume{
						{
							Name: "vol",
							VolumeSource: corev1.VolumeSource{
								PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
									ClaimName: jobOption.DestinationPVCName,
								},
							},
						},
						{
							Name: "cred-secret",
							VolumeSource: corev1.VolumeSource{
								Secret: &corev1.SecretVolumeSource{
									SecretName: utils.GetCredSecretName(jobOption.DataExportName),
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
		job.Spec.Template.Spec.ImagePullSecrets = utils.ToImagePullSecret(utils.GetImageSecretName(jobName))
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
					ClaimName: utils.GetPvcNameForJob(jobName),
				},
			},
		}
		job.Spec.Template.Spec.Volumes = append(job.Spec.Template.Spec.Volumes, volume)
	}

	return job, nil
}

func addJobLabels(jobOpts drivers.JobOpts) map[string]string {
	labels := jobOpts.Labels
	if labels == nil {
		labels = make(map[string]string)
	}

	labels[drivers.DriverNameLabel] = drivers.KopiaRestore
	labels = utils.SetDisableIstioLabel(labels, jobOpts)
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
