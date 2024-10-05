package nfsrestore

import (
	"fmt"
	"strings"

	storkapi "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/libopenstorage/stork/pkg/k8sutils"
	"github.com/portworx/kdmp/pkg/drivers"
	"github.com/portworx/kdmp/pkg/drivers/utils"
	"github.com/portworx/sched-ops/k8s/batch"
	"github.com/portworx/sched-ops/k8s/kdmp"
	storkops "github.com/portworx/sched-ops/k8s/stork"

	"github.com/sirupsen/logrus"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Driver is a nfsbackup implementation of the data export interface.
type Driver struct{}

// Name returns a name of the driver.
func (d Driver) Name() string {
	return drivers.NFSRestore
}

// StartJob creates a job for data transfer between volumes.
func (d Driver) StartJob(opts ...drivers.JobOption) (id string, err error) {
	// FOr every ns to be backed up a new job should be created
	funct := "NfsStartJob"
	logrus.Infof("Inside function %s", funct)
	o := drivers.JobOpts{}
	for _, opt := range opts {
		if opt != nil {
			if err := opt(&o); err != nil {
				return "", err
			}
		}
	}

	job, err := buildJob(o)
	if err != nil {
		return "", err
	}
	// Create PV & PVC only in case of NFS.
	if o.NfsServer != "" {
		err := utils.CreateNFSPvPvcForJob(o.RestoreExportName, job.ObjectMeta.Namespace, o)
		if err != nil {
			return "", err
		}
	}

	if _, err = batch.Instance().CreateJob(job); err != nil && !apierrors.IsAlreadyExists(err) {
		errMsg := fmt.Sprintf("creation of restore job %s failed: %v", o.RestoreExportName, err)
		logrus.Errorf("%s: %v", funct, errMsg)
		return "", fmt.Errorf(errMsg)
	}

	return utils.NamespacedName(job.Namespace, job.Name), nil
}

// DeleteJob stops data transfer between volumes.
func (d Driver) DeleteJob(id string) error {

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
		errMsg := fmt.Sprintf("failed to fetch restore %s/%s job: %v", namespace, name, err)
		logrus.Errorf("%s: %v", fn, errMsg)
		return nil, fmt.Errorf(errMsg)
	}
	// Check whether job has violated the pod security standard
	psaViolated := utils.IsJobPodSecurityFailed(job, namespace)
	if psaViolated {
		utils.DisplayJobpodLogandEvents(job.Name, job.Namespace)
		errMsg := fmt.Sprintf("job [%v/%v] failed to meet the pod security standard, please check job pod's description for more detail", namespace, name)
		return utils.ToJobStatus(0, errMsg, batchv1.JobFailed), nil
	}

	// Check for mount point failure
	mountFailed := utils.IsJobPodMountFailed(job, namespace)
	if mountFailed {
		utils.DisplayJobpodLogandEvents(job.Name, job.Namespace)
		errMsg := fmt.Sprintf("job [%v/%v] failed while mounting NFS mount endpoint", namespace, name)
		return utils.ToJobStatus(0, errMsg, batchv1.JobFailed), nil
	}
	var jobStatus batchv1.JobConditionType
	if len(job.Status.Conditions) != 0 {
		jobStatus = job.Status.Conditions[0].Type

	}
	err = utils.JobNodeExists(job)
	if err != nil {
		errMsg := fmt.Sprintf("failed to fetch the node info tied to the job %s/%s: %v", namespace, name, err)
		logrus.Errorf("%s: %v", fn, errMsg)
		return nil, fmt.Errorf(errMsg)
	}
	jobErr, nodeErr := utils.IsJobOrNodeFailed(job)

	var errMsg string
	if jobErr {
		utils.DisplayJobpodLogandEvents(job.Name, job.Namespace)
		errMsg = fmt.Sprintf("check %s/%s job for details: %s", namespace, name, drivers.ErrJobFailed)
		return utils.ToJobStatus(0, errMsg, jobStatus), nil
	}
	if nodeErr {
		errMsg = fmt.Sprintf("Node [%v] on which job [%v/%v] schedules is NotReady", job.Spec.Template.Spec.NodeName, namespace, name)
		return utils.ToJobStatus(0, errMsg, jobStatus), nil
	}

	res, err := kdmp.Instance().GetResourceBackup(name, namespace)
	if err != nil {
		if apierrors.IsNotFound(err) {
			if utils.IsJobPending(job) {
				logrus.Warnf("restore job %s is in pending state", job.Name)
				return utils.ToJobStatus(0, err.Error(), jobStatus), nil
			}
		}
	}
	logrus.Tracef("%s jobStatus:%v", fn, jobStatus)
	return utils.ToJobStatus(res.Status.ProgressPercentage, res.Status.Reason, jobStatus), nil
}

func buildJob(
	jobOptions drivers.JobOpts,
) (*batchv1.Job, error) {
	funct := "NfsbuildJob"
	// Setup service account using same role permission as stork role
	logrus.Infof("Inside %s function", funct)
	if err := utils.SetupNFSServiceAccount(jobOptions.RestoreExportName, jobOptions.Namespace, roleFor()); err != nil {
		errMsg := fmt.Sprintf("error creating service account %s/%s: %v", jobOptions.Namespace, jobOptions.RestoreExportName, err)
		logrus.Errorf("%s: %v", funct, errMsg)
		return nil, fmt.Errorf(errMsg)
	}

	resources, err := utils.NFSResourceRequirements(jobOptions.JobConfigMap, jobOptions.JobConfigMapNs)
	if err != nil {
		return nil, err
	}
	job, err := jobForRestoreResource(jobOptions, resources)
	if err != nil {
		errMsg := fmt.Sprintf("building resource restore job %s failed: %v", jobOptions.RestoreExportName, err)
		logrus.Errorf("%s: %v", funct, errMsg)
		return nil, fmt.Errorf(errMsg)
	}

	return job, nil
}

func roleFor() *rbacv1.ClusterRole {
	role := &rbacv1.ClusterRole{
		Rules: []rbacv1.PolicyRule{
			{
				APIGroups: []string{"*"},
				Resources: []string{"*"},
				Verbs:     []string{rbacv1.VerbAll},
			},
		},
	}

	return role
}

func addJobLabels(jobOpts drivers.JobOpts) map[string]string {
	labels := jobOpts.Labels
	if labels == nil {
		labels = make(map[string]string)
	}

	labels[drivers.DriverNameLabel] = drivers.NFSRestore
	labels = utils.SetDisableIstioLabel(labels, jobOpts)
	return labels
}

func jobForRestoreResource(
	jobOption drivers.JobOpts,
	resources corev1.ResourceRequirements,
) (*batchv1.Job, error) {
	funct := "jobForRestoreResource"
	// Read the ApplicationRestore stage and decide which restore operation to perform
	restoreCR, err := storkops.Instance().GetApplicationRestore(jobOption.AppCRName, jobOption.AppCRNamespace)
	if err != nil {
		logrus.Errorf("%s: Error getting restore cr[%v/%v]: %v", funct, jobOption.AppCRNamespace, jobOption.AppCRName, err)
		return nil, err
	}
	var opType string
	switch restoreCR.Status.Stage {
	case storkapi.ApplicationRestoreStageIncludeResources:
		opType = "process-vm-resource"
	case storkapi.ApplicationRestoreStageVolumes:
		opType = "restore-vol"
	case storkapi.ApplicationRestoreStageApplications:
		opType = "restore"
	default:
		errMsg := fmt.Sprintf("invalid stage %v in applicationRestore CR[%v/%v]:",
			restoreCR.Status.Stage, jobOption.AppCRNamespace, jobOption.AppCRName)
		logrus.Errorf("%v", errMsg)
		return nil, fmt.Errorf(errMsg)
	}

	cmd := strings.Join([]string{
		"/nfsexecutor",
		opType,
		"--app-cr-name",
		jobOption.AppCRName,
		"--restore-namespace",
		jobOption.AppCRNamespace,
		// resourcebackup CR name
		"--rb-cr-name",
		jobOption.ResoureBackupName,
		// resourcebackup CR namespace
		"--rb-cr-namespace",
		jobOption.ResoureBackupNamespace,
	}, " ")

	labels := addJobLabels(jobOption)

	nfsExecutorImage, imageRegistrySecret, err := utils.GetExecutorImageAndSecret(drivers.NfsExecutorImage,
		jobOption.NfsImageExecutorSource,
		jobOption.NfsImageExecutorSourceNs,
		jobOption.RestoreExportName,
		jobOption)
	if err != nil {
		logrus.Errorf("failed to get the executor image details")
		return nil, fmt.Errorf("failed to get the executor image details for job %s", jobOption.JobName)
	}
	tolerations, err := utils.GetTolerationsFromDeployment(jobOption.NfsImageExecutorSource,
		jobOption.NfsImageExecutorSourceNs)
	if err != nil {
		logrus.Errorf("failed to get the toleration details: %v", err)
		return nil, fmt.Errorf("failed to get the toleration details for job [%s/%s]", jobOption.Namespace, jobOption.RestoreExportName)
	}
	// Get the stork deployment namespace
	storkPodNamespace, err := k8sutils.GetStorkPodNamespace()
	if err != nil {
		logrus.Errorf("failed to get the stork pod namespace: %v", err)
		return nil, fmt.Errorf("failed to get the stork pod namespace: %v", err)
	}
	// Get the PX service name and namespace
	pxServiceNamespace, pxServiceName, err := k8sutils.GetPxNamespaceFromStorkDeploy(k8sutils.StorkDeploymentName, storkPodNamespace)
	if err != nil {
		logrus.Infof("failed to get the px service name and namespace: %v", err)
		return nil, fmt.Errorf("failed to get the px service name and namespace: %v", err)
	}

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobOption.RestoreExportName,
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
					ServiceAccountName: jobOption.RestoreExportName,
					Containers: []corev1.Container{
						{
							Name: drivers.NfsExecutorImage,
							Env: []corev1.EnvVar{
								{
									Name:  k8sutils.PxServiceEnvName,
									Value: pxServiceName,
								},
								{
									Name:  k8sutils.PxNamespaceEnvName,
									Value: pxServiceNamespace,
								},
							},
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
									SecretName: utils.GetCredSecretName(jobOption.RestoreExportName),
								},
							},
						},
					},
				},
			},
		},
	}
	// Not passing the groupId as we do not want to set the RunAsGroup field in the securityContext
	// This helps us in setting the primaryGroup ID to root for the user ID.
	job, err = utils.AddSecurityContextToJob(job, utils.KdmpJobUid, "")
	if err != nil {
		return nil, err
	}

	// Add node affinity to the job spec
	job, err = utils.AddNodeAffinityToJob(job, jobOption)
	if err != nil {
		return nil, err
	}

	// Add the image secret in job spec only if it is present in the stork deployment.
	if len(imageRegistrySecret) != 0 {
		job.Spec.Template.Spec.ImagePullSecrets = utils.ToImagePullSecret(utils.GetImageSecretName(jobOption.RestoreExportName))
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
					ClaimName: utils.GetPvcNameForJob(jobOption.RestoreExportName),
				},
			},
		}

		job.Spec.Template.Spec.Volumes = append(job.Spec.Template.Spec.Volumes, volume)
	}

	return job, nil
}
