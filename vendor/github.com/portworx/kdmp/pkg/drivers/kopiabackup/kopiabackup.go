package kopiabackup

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/portworx/kdmp/pkg/drivers"
	"github.com/portworx/kdmp/pkg/drivers/utils"
	"github.com/portworx/kdmp/pkg/jobratelimit"
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

var backupJobLock sync.Mutex

// Driver is a kopiabackup implementation of the data export interface.
type Driver struct{}

// Name returns a name of the driver.
func (d Driver) Name() string {
	return drivers.KopiaBackup
}

// StartJob creates a job for data transfer between volumes.
func (d Driver) StartJob(opts ...drivers.JobOption) (id string, err error) {
	fn := "StartJob"
	backupJobLock.Lock()
	defer backupJobLock.Unlock()
	o := drivers.JobOpts{}
	for _, opt := range opts {
		if opt != nil {
			if err := opt(&o); err != nil {
				return "", err
			}
		}
	}
	// Sometimes the StartJob is getting called for the same dataexport CR,
	// If the status update to the CR fails in the reconciler. In that case, if we
	// the find job already created, we will exit from here with out doing anything.
	present := jobratelimit.IsJobAlreadyPresent(o.DataExportName, o.Namespace)
	if present {
		return utils.NamespacedName(o.Namespace, o.DataExportName), nil
	}
	// Check whether already a job running to backup the PVC.
	// If so return already job running on the PVC error and caller will retry.
	running, err := jobratelimit.IsJobForPvcAlreadyRunning(
		o.Labels[jobratelimit.PvcNameKey],
		o.Namespace,
		o.Labels[jobratelimit.PvcUIDKey],
		drivers.KopiaBackup,
	)
	if err != nil {
		logrus.Debugf("error while checking already a backup job is running for PVC [%v/%v]: %v",
			o.Labels[jobratelimit.PvcNameKey], o.Labels[jobratelimit.PvcUIDKey], err)
		return "", err
	}
	if running {
		logrus.Infof("already a backup job is running for PVC [%v/%v]",
			o.Labels[jobratelimit.PvcNameKey], o.Labels[jobratelimit.PvcUIDKey])
		return "", utils.ErrJobAlreadyRunning
	}

	// Check whether there is slot to schedule the job.
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
		logrus.Errorf("%s validate: err: %v", fn, err)
		return "", err
	}
	// DataExportName will be unique name when also generated from stork
	// if there are multiple backups being triggered
	jobName := o.DataExportName
	logrus.Debugf("backup jobname: %s", jobName)
	job, err := buildJob(jobName, o)
	if err != nil {
		errMsg := fmt.Sprintf("building backup job %s failed: %v", jobName, err)
		logrus.Errorf("%s: %v", fn, errMsg)
		return "", fmt.Errorf(errMsg)
	}

	// Create PV & PVC only in case of NFS.
	if o.NfsServer != "" {
		err := utils.CreateNFSPvPvcForJob(jobName, job.ObjectMeta.Namespace, o)
		if err != nil {
			return "", err
		}
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

	if err := utils.CleanServiceAccount(name, namespace); err != nil {
		errMsg := fmt.Sprintf("deletion of service account %s/%s failed: %v", namespace, name, err)
		logrus.Errorf("%s: %v", fn, errMsg)
		return fmt.Errorf(errMsg)
	}
	// Get job and check whether it has nodename set.
	job, err := batch.Instance().GetJob(name, namespace)
	if err != nil && !apierrors.IsNotFound(err) {
		errMsg := fmt.Sprintf("failed in getting job %v/%v with err: %v", namespace, name, err)
		logrus.Errorf("%s: %v", fn, errMsg)
		return fmt.Errorf(errMsg)
	}
	nodeName := job.Spec.Template.Spec.NodeName
	if nodeName != "" {
		err := coreops.Instance().IsNodeReady(nodeName)
		if err != nil {
			// force delete the job
			if err = batch.Instance().DeleteJobWithForce(job.Name, job.Namespace); err != nil && !apierrors.IsNotFound(err) {
				errMsg := fmt.Sprintf("deletion of backup job %s/%s failed: %v", namespace, name, err)
				logrus.Errorf("%s: %v", fn, errMsg)
				return fmt.Errorf(errMsg)
			}
			// force delete the job pod as well
			pods, err := coreops.Instance().GetPods(
				job.Namespace,
				map[string]string{
					"job-name": job.Name,
				},
			)
			if err != nil {
				errMsg := fmt.Sprintf("failed in fetching job pods %s/%s: %v", namespace, name, err)
				logrus.Errorf("%s: %v", fn, errMsg)
				return fmt.Errorf(errMsg)
			}

			for _, pod := range pods.Items {
				err = coreops.Instance().DeletePod(pod.Name, pod.Namespace, true)
				if err != nil && !apierrors.IsNotFound(err) {
					errMsg := fmt.Sprintf("deletion of backup pod %s/%s failed: %v", namespace, pod.Name, err)
					logrus.Errorf("%s: %v", fn, errMsg)
					return fmt.Errorf(errMsg)
				}
			}
		} else {
			if err = batch.Instance().DeleteJob(name, namespace); err != nil && !apierrors.IsNotFound(err) {
				errMsg := fmt.Sprintf("deletion of backup job %s/%s failed: %v", namespace, name, err)
				logrus.Errorf("%s: %v", fn, errMsg)
				return fmt.Errorf(errMsg)
			}
		}

	} else {
		if err = batch.Instance().DeleteJob(name, namespace); err != nil && !apierrors.IsNotFound(err) {
			errMsg := fmt.Sprintf("deletion of backup job %s/%s failed: %v", namespace, name, err)
			logrus.Errorf("%s: %v", fn, errMsg)
			return fmt.Errorf(errMsg)
		}
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
	var jobStatus batchv1.JobConditionType
	if len(job.Status.Conditions) != 0 {
		jobStatus = job.Status.Conditions[0].Type

	}

	// Check whether mount point failure
	mountFailed := utils.IsJobPodMountFailed(job, namespace)
	if mountFailed {
		errMsg := fmt.Sprintf("job [%v/%v] failed to mount pvc, please check job pod's description for more detail", namespace, name)
		return utils.ToJobStatus(0, errMsg, batchv1.JobFailed), nil
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
		errMsg = fmt.Sprintf("check %s/%s job for details: %s", namespace, name, drivers.ErrJobFailed)
		return utils.ToJobStatus(0, errMsg, jobStatus), nil
	}
	if nodeErr {
		errMsg = fmt.Sprintf("Node [%v] on which job [%v/%v] schedules is NotReady", job.Spec.Template.Spec.NodeName, namespace, name)
		return utils.ToJobStatus(0, errMsg, jobStatus), nil
	}

	vb, err := kdmpops.Instance().GetVolumeBackup(context.Background(), name, namespace)
	if err != nil {
		if apierrors.IsNotFound(err) {
			if utils.IsJobPending(job) {
				logrus.Warnf("backup job %s is in pending state", job.Name)
				return utils.ToJobStatus(0, "", jobStatus), nil
			}
		}
		errMsg := fmt.Sprintf("failed to fetch volumebackup %s/%s status: %v", namespace, name, err)
		logrus.Errorf("%s: %v", fn, errMsg)
		return nil, fmt.Errorf(errMsg)
	}

	return utils.ToJobStatus(vb.Status.ProgressPercentage, vb.Status.LastKnownError, jobStatus), nil
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
	jobOption drivers.JobOpts,
	jobName string,
	resources corev1.ResourceRequirements,
	nodeName string,
) (*batchv1.Job, error) {
	backupName := jobName

	labels := addJobLabels(jobOption.Labels)

	cmd := strings.Join([]string{
		"/kopiaexecutor",
		"backup",
		"--volume-backup-name",
		backupName,
		"--repository",
		toRepoName(jobOption.RepoPVCName, jobOption.Namespace),
		"--credentials",
		jobOption.DataExportName,
		"--backup-location",
		jobOption.BackupLocationName,
		"--backup-location-namespace",
		jobOption.BackupLocationNamespace,
		"--backup-namespace",
		jobOption.Namespace,
		"--source-path",
		"/data",
	}, " ")

	// Read the config map from the job option and then add the debug level log if required
	cmd = utils.CheckAndAddKopiaDebugModeEnabled(cmd, jobOption)

	if jobOption.Compression != "" {
		splitCmd := strings.Split(cmd, " ")
		splitCmd = append(splitCmd, "--compression", jobOption.Compression)
		cmd = strings.Join(splitCmd, " ")
	}

	if jobOption.ExcludeFileList != "" {
		splitCmd := strings.Split(cmd, " ")
		splitCmd = append(splitCmd, "--exclude-file-list", jobOption.ExcludeFileList)
		cmd = strings.Join(splitCmd, " ")
	}

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
						},
					},
					Tolerations: tolerations,
					Volumes: []corev1.Volume{
						{
							Name: "vol",
							VolumeSource: corev1.VolumeSource{
								PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
									ClaimName: jobOption.SourcePVCName,
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

	if len(nodeName) != 0 {
		job.Spec.Template.Spec.NodeName = nodeName
	}

	// Add the image secret in job spec only if it is present in the stork deployment.
	if len(imageRegistrySecret) != 0 {
		job.Spec.Template.Spec.ImagePullSecrets = utils.ToImagePullSecret(utils.GetImageSecretName(jobName))
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

func buildJob(jobName string, jobOptions drivers.JobOpts) (*batchv1.Job, error) {
	fn := "buildJob"
	resources, err := utils.KopiaResourceRequirements(jobOptions.JobConfigMap, jobOptions.JobConfigMapNs)
	if err != nil {
		return nil, err
	}
	pods, err := coreops.Instance().GetPodsUsingPVC(jobOptions.SourcePVCName, jobOptions.SourcePVCNamespace)
	if err != nil {
		errMsg := fmt.Sprintf("error fetching pods using PVC %s/%s: %v", jobOptions.Namespace, jobOptions.SourcePVCName, err)
		logrus.Errorf("%s: %v", fn, errMsg)
		return nil, fmt.Errorf(errMsg)
	}
	var resourceNamespace string
	var live bool
	var nodeName string
	// filter out the pods that are create by us
	for _, pod := range pods {
		labels := pod.ObjectMeta.Labels
		if _, ok := labels[drivers.DriverNameLabel]; ok {
			continue
		}
		if pod.Status.Phase == "Running" {
			// get the nodeName, if the pods is in Running state, So that we can schedule
			// kopia job on the same node.
			nodeName = pod.Spec.NodeName
			break
		}
	}
	resourceNamespace = jobOptions.Namespace
	if err := utils.SetupServiceAccount(jobName, resourceNamespace, roleFor(live)); err != nil {
		errMsg := fmt.Sprintf("error creating service account %s/%s: %v", resourceNamespace, jobName, err)
		logrus.Errorf("%s: %v", fn, errMsg)
		return nil, fmt.Errorf(errMsg)
	}
	return jobFor(
		jobOptions,
		jobName,
		resources,
		nodeName,
	)
}

func roleFor(live bool) *rbacv1.Role {
	role := &rbacv1.Role{
		Rules: []rbacv1.PolicyRule{
			{
				APIGroups: []string{"kdmp.portworx.com"},
				Resources: []string{"volumebackups"},
				Verbs:     []string{rbacv1.VerbAll},
			},
		},
	}
	// Only live backup, we will add the hostaccess and privilege option.
	if live {
		hostAccessRule := rbacv1.PolicyRule{
			APIGroups:     []string{"security.openshift.io"},
			Resources:     []string{"securitycontextconstraints"},
			ResourceNames: []string{"hostaccess"},
			Verbs:         []string{"use"},
		}
		role.Rules = append(role.Rules, hostAccessRule)
		PrivilegedRule := rbacv1.PolicyRule{
			APIGroups:     []string{"security.openshift.io"},
			Resources:     []string{"securitycontextconstraints"},
			ResourceNames: []string{"privileged"},
			Verbs:         []string{"use"},
		}
		role.Rules = append(role.Rules, PrivilegedRule)
	}
	return role
}
