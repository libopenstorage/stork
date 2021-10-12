package kopiamaintenance

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
	batchv1beta1 "k8s.io/api/batch/v1beta1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	kopiaMaintenanceJobPrefix         = "repo-maintenance"
	defaultFullSchedule               = "0 */24 * * *"
	defaultQuickSchedule              = "0 */2 * * *"
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
	// Check whether there is slot to schedule maintenance job.
	driverType := d.Name()
	available, err := jobratelimit.JobCanRun(driverType)
	if err != nil {
		logrus.Errorf("%v", err)
		return "", err
	}
	if !available {
		return "", utils.ErrOutOfJobResources
	}
	if err := d.validate(o); err != nil {
		errMsg := fmt.Sprintf("validation failed for maintenance job for backuplocation [%v]: %v", o.BackupLocationName, err)
		logrus.Infof("%s %v", fn, errMsg)
		return "", fmt.Errorf(errMsg)
	}
	jobName := toJobName(o.JobName, o.BackupLocationName)
	job, err := buildJob(jobName, o)
	if err != nil {
		errMsg := fmt.Sprintf("building maintenance job [%s] for backuplocation [%v] failed: %v", jobName, o.BackupLocationName, err)
		logrus.Errorf("%s %v", fn, errMsg)
		return "", fmt.Errorf(errMsg)
	}
	if _, err = batch.Instance().CreateCronJob(job); err != nil && !apierrors.IsAlreadyExists(err) {
		errMsg := fmt.Sprintf("creation of maintenance job [%s] for backuplocation [%v] failed: %v", jobName, o.BackupLocationName, err)
		logrus.Errorf("%s %v", fn, errMsg)
		return "", fmt.Errorf(errMsg)
	}
	logrus.Infof("%s created maintenance job [%s] for backuplocation [%v] successfully", fn, o.BackupLocationName, job.Name)
	return utils.NamespacedName(job.Namespace, job.Name), nil
}

// DeleteJob deletes the maintenance job.
func (d Driver) DeleteJob(id string) error {
	fn := "DeleteJob:"
	namespace, name, err := utils.ParseJobID(id)
	if err != nil {
		logrus.Errorf("%s %v", fn, err)
		return err
	}

	if err = batch.Instance().DeleteCronJob(name, namespace); err != nil && !apierrors.IsNotFound(err) {
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
		return utils.ToJobStatus(0, err.Error()), nil
	}

	job, err := batch.Instance().GetJob(name, namespace)
	if err != nil {
		errMsg := fmt.Sprintf("failed to fetch maintenance [%s/%s] job: %v", namespace, name, err)
		logrus.Errorf("%s: %v", fn, errMsg)
		return nil, fmt.Errorf(errMsg)
	}
	if utils.IsJobFailed(job) {
		errMsg := fmt.Sprintf("check maintenance [%s/%s] job for details: %s", namespace, name, drivers.ErrJobFailed)
		return utils.ToJobStatus(0, errMsg), nil
	}
	if utils.IsJobCompleted(job) {
		return utils.ToJobStatus(drivers.TransferProgressCompleted, ""), nil
	}
	return utils.ToJobStatus(0, ""), nil
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
	jobName,
	jobNamespace,
	credSecretName,
	credSecretNamespace,
	maintenaceStatusName,
	maintenacneStatusNamespace,
	serviceAccountName string,
	resources corev1.ResourceRequirements,
	labels map[string]string,
	maintenanceType string,
) (*batchv1beta1.CronJob, error) {

	labels = addJobLabels(labels)
	var successfulJobsHistoryLimit int32 = defaultSuccessfulJobsHistoryLimit
	var failedJobsHistoryLimit int32 = defaultFailedJobsHistoryLimit

	scheduleInterval := defaultQuickSchedule
	if maintenanceType == fullMaintenanceType {
		scheduleInterval = defaultFullSchedule
	}

	cmd := strings.Join([]string{
		"/kopiaexecutor",
		"maintenance",
		"--cred-secret-name",
		credSecretName,
		"--cred-secret-namespace",
		credSecretNamespace,
		"--maintenance-status-name",
		maintenaceStatusName,
		"--maintenance-status-namespace",
		maintenacneStatusNamespace,
		"--maintenance-type",
		maintenanceType,
	}, " ")

	return &batchv1beta1.CronJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobName,
			Namespace: jobNamespace,
			Annotations: map[string]string{
				utils.SkipResourceAnnotation: "true",
			},
			Labels: labels,
		},
		Spec: batchv1beta1.CronJobSpec{
			Schedule:                   scheduleInterval,
			SuccessfulJobsHistoryLimit: &successfulJobsHistoryLimit,
			FailedJobsHistoryLimit:     &failedJobsHistoryLimit,
			JobTemplate: batchv1beta1.JobTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Name:      jobName,
					Namespace: jobNamespace,
					Annotations: map[string]string{
						utils.SkipResourceAnnotation: "true",
					},
					Labels: labels,
				},
				Spec: batchv1.JobSpec{
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: labels,
						},
						Spec: corev1.PodSpec{
							RestartPolicy:      corev1.RestartPolicyOnFailure,
							ServiceAccountName: serviceAccountName,
							ImagePullSecrets:   utils.ToImagePullSecret(utils.KopiaExecutorImageSecret()),
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
			},
		},
	}, nil
}

func toJobName(jobName, backupLocation string) string {
	if jobName != "" {
		return jobName
	}
	return fmt.Sprintf("%s-%s", kopiaMaintenanceJobPrefix, backupLocation)
}

func addJobLabels(labels map[string]string) map[string]string {
	if labels == nil {
		labels = make(map[string]string)
	}

	labels[drivers.DriverNameLabel] = drivers.KopiaMaintenance
	return labels
}

func buildJob(jobName string, o drivers.JobOpts) (*batchv1beta1.CronJob, error) {
	resources, err := utils.KopiaResourceRequirements()
	if err != nil {
		return nil, err
	}

	return jobFor(
		jobName,
		o.JobNamespace,
		o.CredSecretName,
		o.CredSecretNamespace,
		o.MaintenanceStatusName,
		o.MaintenanceStatusNamespace,
		o.ServiceAccountName,
		resources,
		o.Labels,
		o.MaintenanceType,
	)
}
