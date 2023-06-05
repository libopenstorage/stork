package kopiabackup

import (
	"fmt"
	"strings"

	"github.com/portworx/kdmp/pkg/drivers"
	"github.com/portworx/kdmp/pkg/drivers/utils"
	coreops "github.com/portworx/sched-ops/k8s/core"
	"github.com/sirupsen/logrus"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8shelper "k8s.io/component-helpers/storage/volume"
)

var (
	defaultPodsMountPath = "/var/lib/kubelet/pods"
)

func jobForLiveBackup(
	jobOption drivers.JobOpts,
	jobName string,
	mountPod corev1.Pod,
	resources corev1.ResourceRequirements,
	pvcNamespace string,
) (*batchv1.Job, error) {
	volDir, err := getVolumeDirectory(jobOption.SourcePVCName, jobOption.SourcePVCNamespace)
	if err != nil {
		return nil, err
	}
	// pod volumes reside under /var/lib/kubelet/pods/<podUID>/volumes/<volumePlugin>/<volumeName> directory.
	// mount /var/lib/kubelet/pods/<podUID>/volumes as a /data directory to a resticexecutor job and
	// use /data/*/<volumeName> as a backup directory and determine volume plugin by resticexecutor.
	var podVolumesPath string
	if len(jobOption.PodDataPath) == 0 {
		podVolumesPath = fmt.Sprintf("%s/%s/volumes", defaultPodsMountPath, mountPod.UID)
	} else {
		logrus.Debugf("selecting pod data path %v from config map", jobOption.PodDataPath)
		podVolumesPath = fmt.Sprintf("%s/%s/volumes", jobOption.PodDataPath, mountPod.UID)
	}
	backupPath := fmt.Sprintf("/data/*/%s", volDir)

	backupName := jobName

	labels := addJobLabels(jobOption.Labels)
	cmd := strings.Join([]string{
		"/kopiaexecutor",
		"backup",
		"--volume-backup-name",
		backupName,
		"--credentials",
		jobOption.DataExportName,
		"--backup-location",
		jobOption.BackupLocationName,
		"--backup-location-namespace",
		jobOption.BackupLocationNamespace,
		"--backup-namespace",
		jobOption.Namespace,
		"--repository",
		toRepoName(jobOption.RepoPVCName, pvcNamespace),
		"--source-path-glob",
		backupPath,
	}, " ")

	if jobOption.Compression != "" {
		splitCmd := strings.Split(cmd, " ")
		splitCmd = append(splitCmd, "--compression", jobOption.Compression)
		cmd = strings.Join(splitCmd, " ")
	}

	privileged := true
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
					NodeName:           mountPod.Spec.NodeName,
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
								Privileged: &privileged,
							},
						},
					},
					Tolerations: tolerations,
					Volumes: []corev1.Volume{
						{
							Name: "vol",
							VolumeSource: corev1.VolumeSource{
								HostPath: &corev1.HostPathVolumeSource{
									Path: podVolumesPath,
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

// getVolumeDirectory gets the name of the directory on the host, under /var/lib/kubelet/pods/<podUID>/volumes/,
// where the specified volume lives. For volumes with a CSIVolumeSource, append "/mount" to the directory name.
func getVolumeDirectory(pvcName, pvcNamespace string) (string, error) {
	fn := "getVolumeDirectory"
	pvc, err := coreops.Instance().GetPersistentVolumeClaim(pvcName, pvcNamespace)
	if err != nil {
		errMsg := fmt.Sprintf("error fetching PVC %s/%s: %s", pvcNamespace, pvcName, err)
		logrus.Errorf("%s: %v", fn, errMsg)
		return "", fmt.Errorf(errMsg)
	}

	pv, err := coreops.Instance().GetPersistentVolume(pvc.Spec.VolumeName)
	if err != nil {
		errMsg := fmt.Sprintf("error fetching PV %s/%s: %s", pvcNamespace, pvcName, err)
		logrus.Errorf("%s: %v", fn, errMsg)
		return "", fmt.Errorf(errMsg)
	}

	// PV's been created with a CSI source.
	if pv.Spec.CSI != nil {
		return pvc.Spec.VolumeName + "/mount", nil
	}

	// Check if the pv spec contains pv.kubernetes.io/migrated-to annotation,
	// Then add the mount to the volume dir as it was provisioned by the CSI provisioner.
	// From 1.23 k8s version onwards the in-tree storage provisioner driver are not supported.
	// So all of them will migrated to use CSI provisioner. These driver will have above annotation and
	// will not have the CSI section in the pv spec.
	if pv.Annotations != nil {
		annotations := pv.GetAnnotations()
		if _, ok := annotations[k8shelper.AnnMigratedTo]; ok {
			return pvc.Spec.VolumeName + "/mount", nil
		}
	}

	return pvc.Spec.VolumeName, nil
}
