package kopiamaintenance

import (
	"fmt"
	"strings"
	"sync"

	"github.com/portworx/kdmp/pkg/drivers"
	"github.com/portworx/kdmp/pkg/drivers/utils"
	"github.com/portworx/kdmp/pkg/version"
	"github.com/portworx/sched-ops/k8s/batch"
	"github.com/sirupsen/logrus"
	batchv1 "k8s.io/api/batch/v1"
	batchv1beta1 "k8s.io/api/batch/v1beta1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	kopiaMaintenanceJobPrefix         = "repo-maintenance"
	defaultFullSchedule               = "1 */23 * * *"
	defaultQuickSchedule              = "1 */3 * * *"
	fullMaintenanceType               = "full"
	quickMaintenaceTye                = "quick"
	defaultFailedJobsHistoryLimit     = 1
	defaultSuccessfulJobsHistoryLimit = 1
)

// Driver is a kopia maintenance snapshot implementation
type Driver struct{}

// Name returns a name of the driver.
func (d Driver) Name() string {
	return drivers.KopiaMaintenance
}

var maintenanceJobLock sync.Mutex

// StartJob creates a cron job for kopia snapshot maintenance
// Note: Not added separate interface apis for cronjob. Reused job apis to start the cron job.
func (d Driver) StartJob(opts ...drivers.JobOption) (id string, err error) {
	fn := "StartJob:"
	maintenanceJobLock.Lock()
	defer maintenanceJobLock.Unlock()
	o := drivers.JobOpts{}
	for _, opt := range opts {
		if opt != nil {
			if err := opt(&o); err != nil {
				return "", err
			}
		}
	}
	if err := d.validate(o); err != nil {
		errMsg := fmt.Sprintf("validation failed for maintenance job for backuplocation [%v]: %v", o.BackupLocationName, err)
		logrus.Infof("%s %v", fn, errMsg)
		return "", fmt.Errorf(errMsg)
	}
	jobName := toJobName(o.JobName, o.BackupLocationName)

	requiresV1, err := version.RequiresV1CronJob()
	if err != nil {
		return "", err
	}

	job, err := buildJob(jobName, o, requiresV1)
	if err != nil {
		errMsg := fmt.Sprintf("building maintenance job [%s] for backuplocation [%v] failed: %v", jobName, o.BackupLocationName, err)
		logrus.Errorf("%s %v", fn, errMsg)
		return "", fmt.Errorf(errMsg)
	}

	// Create PV & PVC only in case of NFS.
	if o.NfsServer != "" {
		err := utils.CreateNFSPvPvcForJob(jobName, o.JobNamespace, o)
		if err != nil {
			return "", err
		}
	}

	if requiresV1 {
		jobV1 := job.(*batchv1.CronJob)
		_, err = batch.Instance().CreateCronJob(jobV1)
	} else {
		jobV1Beta1 := job.(*batchv1beta1.CronJob)
		_, err = batch.Instance().CreateCronJobV1beta1(jobV1Beta1)
	}
	if err != nil && !apierrors.IsAlreadyExists(err) {
		errMsg := fmt.Sprintf("creation of maintenance job [%s] for backuplocation [%v] failed: %v", jobName, o.BackupLocationName, err)
		logrus.Errorf("%s %v", fn, errMsg)
		return "", fmt.Errorf(errMsg)
	}
	logrus.Infof("%s created maintenance job [%s] for backuplocation [%v] successfully", fn, o.BackupLocationName, jobName)
	return utils.NamespacedName(o.JobNamespace, jobName), nil
}

// DeleteJob deletes the maintenance job.
func (d Driver) DeleteJob(id string) error {
	fn := "DeleteJob:"
	namespace, name, err := utils.ParseJobID(id)
	if err != nil {
		logrus.Errorf("%s %v", fn, err)
		return err
	}

	requiresV1, err := version.RequiresV1CronJob()
	if err != nil {
		return err
	}

	if requiresV1 {
		err = batch.Instance().DeleteCronJob(name, namespace)
	} else {
		err = batch.Instance().DeleteCronJobV1beta1(name, namespace)
	}
	if err != nil && !apierrors.IsNotFound(err) {
		errMsg := fmt.Sprintf("deletion of maintenance job [%s/%s] failed: %v", namespace, name, err)
		logrus.Errorf("%s: %v", fn, errMsg)
		return fmt.Errorf(errMsg)
	}
	logrus.Infof("deleted maintenance cron job [%s/%s] successfully", namespace, name)
	return nil
}

// JobStatus returns a progress of the maintenance cron job.
func (d Driver) JobStatus(id string) (*drivers.JobStatus, error) {
	fn := "JobStatus"
	namespace, name, err := utils.ParseJobID(id)
	if err != nil {
		return utils.ToJobStatus(0, err.Error(), batchv1.JobConditionType("")), nil
	}

	job, err := batch.Instance().GetJob(name, namespace)
	if err != nil {
		errMsg := fmt.Sprintf("failed to fetch maintenance [%s/%s] job: %v", namespace, name, err)
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

	if utils.IsJobFailed(job) {
		utils.DisplayJobpodLogandEvents(job.Name, job.Namespace)
		errMsg := fmt.Sprintf("check maintenance [%s/%s] job for details: %s", namespace, name, drivers.ErrJobFailed)
		return utils.ToJobStatus(0, errMsg, jobStatus), nil
	}
	if utils.IsJobCompleted(job) {
		return utils.ToJobStatus(drivers.TransferProgressCompleted, "", jobStatus), nil
	}
	return utils.ToJobStatus(0, "", jobStatus), nil
}

func (d Driver) validate(o drivers.JobOpts) error {
	if o.CredSecretName == "" {
		return fmt.Errorf("credential secret name should be set")
	}
	if o.CredSecretNamespace == "" {
		return fmt.Errorf("credential secret namespace  should be set")
	}
	return nil
}

func jobFor(
	jobOption drivers.JobOpts,
	jobName string,
	resources corev1.ResourceRequirements,
	requiresV1 bool,
) (interface{}, error) {

	labels := addJobLabels(jobOption)
	var successfulJobsHistoryLimit int32 = defaultSuccessfulJobsHistoryLimit
	var failedJobsHistoryLimit int32 = defaultFailedJobsHistoryLimit

	scheduleInterval := defaultQuickSchedule
	if jobOption.MaintenanceType == fullMaintenanceType {
		scheduleInterval = defaultFullSchedule
	}

	cmd := strings.Join([]string{
		"/kopiaexecutor",
		"maintenance",
		"--cred-secret-name",
		jobOption.CredSecretName,
		"--cred-secret-namespace",
		jobOption.CredSecretNamespace,
		"--maintenance-status-name",
		jobOption.MaintenanceStatusName,
		"--maintenance-status-namespace",
		jobOption.MaintenanceStatusNamespace,
		"--maintenance-type",
		jobOption.MaintenanceType,
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

	jobObjectMeta := metav1.ObjectMeta{
		Name:      jobName,
		Namespace: jobOption.JobNamespace,
		Annotations: map[string]string{
			utils.SkipResourceAnnotation: "true",
		},
		Labels: labels,
	}

	jobSpec := corev1.PodSpec{
		RestartPolicy:      corev1.RestartPolicyOnFailure,
		ServiceAccountName: jobOption.ServiceAccountName,
		Containers: []corev1.Container{
			{
				Name:  "kopiaexecutor",
				Image: kopiaExecutorImage,
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
	}
	// Add the image secret in job spec only if it is present in the stork deployment.
	if len(imageRegistrySecret) != 0 {
		jobSpec.ImagePullSecrets = utils.ToImagePullSecret(utils.GetImageSecretName(jobName))
	}
	var volumeMount corev1.VolumeMount
	var volume corev1.Volume
	var env []corev1.EnvVar

	if len(jobOption.NfsServer) != 0 {
		volumeMount = corev1.VolumeMount{
			Name:      utils.NfsVolumeName,
			MountPath: drivers.NfsMount,
		}
		jobSpec.Containers[0].VolumeMounts = append(
			jobSpec.Containers[0].VolumeMounts,
			volumeMount,
		)
		volume = corev1.Volume{
			Name: utils.NfsVolumeName,
			VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
					ClaimName: utils.GetPvcNameForJob(jobName),
				},
			},
		}
		jobSpec.Volumes = append(jobSpec.Volumes, volume)
	}

	if drivers.CertFilePath != "" {
		volumeMount = corev1.VolumeMount{
			Name:      utils.TLSCertMountVol,
			MountPath: drivers.CertMount,
			ReadOnly:  true,
		}

		volume = corev1.Volume{
			Name: utils.TLSCertMountVol,
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName: jobOption.CertSecretName,
				},
			},
		}

		env = []corev1.EnvVar{
			{
				Name:  drivers.CertDirPath,
				Value: drivers.CertMount,
			},
		}
	}
	if len(jobOption.NodeAffinity) > 0 {
		// Maintenance jobs for baas paid customer will run on the dedicated nodes assigned to customer instance
		// Nodes wil have the following label "tenant: <instance name>"
		// Iterate over the map list having affinity rules
		matchExpressions := []corev1.NodeSelectorRequirement{}
		for key, val := range jobOption.NodeAffinity {
			expression := corev1.NodeSelectorRequirement{
				Key:      key,
				Operator: corev1.NodeSelectorOpIn,
				Values:   []string{val},
			}
			matchExpressions = append(matchExpressions, expression)
		}

		jobSpec.Affinity = &corev1.Affinity{
			NodeAffinity: &corev1.NodeAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
					NodeSelectorTerms: []corev1.NodeSelectorTerm{
						{
							MatchExpressions: matchExpressions,
						},
					},
				},
			},
		}
	} else {
		nodeAffinity, err := utils.GetNodeAffinityFromDeployment(jobOption.KopiaImageExecutorSource,
			jobOption.KopiaImageExecutorSourceNs)
		if err != nil {
			logrus.Errorf("failed to get the node affinity details: %v", err)
			return nil, fmt.Errorf("failed to get the node affinity details for job [%s/%s]", jobOption.Namespace, jobName)
		}
		jobSpec.Affinity = &corev1.Affinity{
			NodeAffinity: nodeAffinity,
		}
	}

	if requiresV1 {
		jobV1 := &batchv1.CronJob{
			ObjectMeta: jobObjectMeta,
			Spec: batchv1.CronJobSpec{
				Schedule:                   scheduleInterval,
				SuccessfulJobsHistoryLimit: &successfulJobsHistoryLimit,
				FailedJobsHistoryLimit:     &failedJobsHistoryLimit,
				JobTemplate: batchv1.JobTemplateSpec{
					ObjectMeta: jobObjectMeta,
					Spec: batchv1.JobSpec{
						BackoffLimit: &utils.JobPodBackOffLimit,
						Template: corev1.PodTemplateSpec{
							ObjectMeta: metav1.ObjectMeta{
								Labels: labels,
							},
							Spec: jobSpec,
						},
					},
				},
			},
		}

		if drivers.CertFilePath != "" {
			jobV1.Spec.JobTemplate.Spec.Template.Spec.Containers[0].VolumeMounts = append(
				jobV1.Spec.JobTemplate.Spec.Template.Spec.Containers[0].VolumeMounts,
				volumeMount,
			)
			jobV1.Spec.JobTemplate.Spec.Template.Spec.Volumes = append(jobV1.Spec.JobTemplate.Spec.Template.Spec.Volumes, volume)
			jobV1.Spec.JobTemplate.Spec.Template.Spec.Containers[0].Env = env
		}

		return jobV1, nil
	}
	jobV1Beta1 := &batchv1beta1.CronJob{
		ObjectMeta: jobObjectMeta,
		Spec: batchv1beta1.CronJobSpec{
			Schedule:                   scheduleInterval,
			SuccessfulJobsHistoryLimit: &successfulJobsHistoryLimit,
			FailedJobsHistoryLimit:     &failedJobsHistoryLimit,
			JobTemplate: batchv1beta1.JobTemplateSpec{
				ObjectMeta: jobObjectMeta,
				Spec: batchv1.JobSpec{
					BackoffLimit: &utils.JobPodBackOffLimit,
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: labels,
						},
						Spec: jobSpec,
					},
				},
			},
		},
	}

	if drivers.CertFilePath != "" {
		jobV1Beta1.Spec.JobTemplate.Spec.Template.Spec.Containers[0].VolumeMounts = append(
			jobV1Beta1.Spec.JobTemplate.Spec.Template.Spec.Containers[0].VolumeMounts,
			volumeMount,
		)
		jobV1Beta1.Spec.JobTemplate.Spec.Template.Spec.Volumes = append(jobV1Beta1.Spec.JobTemplate.Spec.Template.Spec.Volumes, volume)
		jobV1Beta1.Spec.JobTemplate.Spec.Template.Spec.Containers[0].Env = env
	}

	return jobV1Beta1, nil
}

func toJobName(jobName, backupLocation string) string {
	if jobName != "" {
		return jobName
	}
	return fmt.Sprintf("%s-%s", kopiaMaintenanceJobPrefix, backupLocation)
}

func addJobLabels(jobOpts drivers.JobOpts) map[string]string {
	labels := jobOpts.Labels
	if labels == nil {
		labels = make(map[string]string)
	}

	labels[drivers.DriverNameLabel] = drivers.KopiaMaintenance
	labels = utils.SetDisableIstioLabel(labels, jobOpts)
	return labels
}

func buildJob(jobName string, jobOpts drivers.JobOpts, requiresV1 bool) (interface{}, error) {
	resources, err := utils.KopiaResourceRequirements(jobOpts.JobConfigMap, jobOpts.JobConfigMapNs)
	if err != nil {
		return nil, err
	}

	return jobFor(
		jobOpts,
		jobName,
		resources,
		requiresV1,
	)
}
