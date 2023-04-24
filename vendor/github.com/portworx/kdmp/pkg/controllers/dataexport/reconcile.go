package dataexport

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"time"

	kSnapshotv1 "github.com/kubernetes-csi/external-snapshotter/client/v4/apis/volumesnapshot/v1"
	kSnapshotv1beta1 "github.com/kubernetes-csi/external-snapshotter/client/v4/apis/volumesnapshot/v1beta1"
	kSnapshotClient "github.com/kubernetes-csi/external-snapshotter/client/v4/clientset/versioned"
	storkapi "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/libopenstorage/stork/pkg/controllers"
	"github.com/libopenstorage/stork/pkg/snapshotter"
	kdmpapi "github.com/portworx/kdmp/pkg/apis/kdmp/v1alpha1"
	kdmpcontroller "github.com/portworx/kdmp/pkg/controllers"
	"github.com/portworx/kdmp/pkg/drivers"
	"github.com/portworx/kdmp/pkg/drivers/driversinstance"
	"github.com/portworx/kdmp/pkg/drivers/utils"
	kdmpopts "github.com/portworx/kdmp/pkg/util/ops"

	"github.com/portworx/kdmp/pkg/version"
	"github.com/portworx/sched-ops/k8s/batch"
	"github.com/portworx/sched-ops/k8s/core"
	"github.com/portworx/sched-ops/k8s/storage"
	"github.com/portworx/sched-ops/k8s/stork"
	"github.com/portworx/sched-ops/task"
	"github.com/sirupsen/logrus"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	k8sErrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/client-go/rest"
	k8shelper "k8s.io/component-helpers/storage/volume"
)

// Data export label names/keys.
const (
	LabelController          = "kdmp.portworx.com/controller"
	LabelControllerName      = "controller-name"
	KopiaSecretName          = "generic-backup-repo"
	KopiaSecretNamespace     = "kube-system"
	backupCRNameKey          = "kdmp.portworx.com/backup-cr-name"
	pvcNameKey               = "kdmp.portworx.com/pvc-name"
	labelNamelimit           = 63
	csiProvider              = "csi"
	dataExportUIDAnnotation  = "portworx.io/dataexport-uid"
	dataExportNameAnnotation = "portworx.io/dataexport-name"
	skipResourceAnnotation   = "stork.libopenstorage.org/skip-resource"
	baseSCAnnotation         = "volume.beta.kubernetes.io/storage-class"

	pxbackupAnnotationPrefix        = "portworx.io/"
	kdmpAnnotationPrefix            = "kdmp.portworx.com/"
	pxbackupAnnotationCreateByKey   = pxbackupAnnotationPrefix + "created-by"
	pxbackupAnnotationCreateByValue = "px-backup"
	backupObjectUIDKey              = kdmpAnnotationPrefix + "backupobject-uid"
	pvcUIDKey                       = kdmpAnnotationPrefix + "pvc-uid"
	volumeSnapShotCRDirectory       = "csi-generic"
	snapDeleteAnnotation            = "snapshotScheduledForDeletion"
	snapRestoreAnnotation           = "snapshotScheduledForRestore"

	// pvcNameLenLimit is the max length of PVC name that DataExport related CRs
	// will incorporate in their names
	pvcNameLenLimit = 247
	// pvcNameLenLimitForJob is the max length of PVC name that the bound job
	// will incorporate in their names
	pvcNameLenLimitForJob = 48

	defaultTimeout        = 1 * time.Minute
	progressCheckInterval = 5 * time.Second
	compressionKey        = "KDMP_COMPRESSION"
	backupPath            = "KDMP_BACKUP_PATH"
)

type updateDataExportDetail struct {
	stage                kdmpapi.DataExportStage
	status               kdmpapi.DataExportStatus
	transferID           string
	reason               string
	snapshotID           string
	size                 uint64
	progressPercentage   int
	snapshotPVCName      string
	snapshotPVCNamespace string
	snapshotNamespace    string
	removeFinalizer      bool
	volumeSnapshot       string
}

func (c *Controller) sync(ctx context.Context, in *kdmpapi.DataExport) (bool, error) {
	if in == nil {
		return false, nil
	}
	dataExport := in.DeepCopy()
	// set the initial stage
	if dataExport.Status.Stage == "" {
		// TODO: set defaults
		if dataExport.Spec.Type == "" {
			dataExport.Spec.Type = kdmpapi.DataExportRsync
		}

		data := updateDataExportDetail{
			status: kdmpapi.DataExportStatusInitial,
			stage:  kdmpapi.DataExportStageInitial,
		}
		return true, c.updateStatus(dataExport, data)
	}

	// delete an object on the init stage without cleanup
	if dataExport.DeletionTimestamp != nil && dataExport.Status.Stage == kdmpapi.DataExportStageInitial {
		if !controllers.ContainsFinalizer(dataExport, kdmpcontroller.CleanupFinalizer) {
			return false, nil
		}

		if err := c.client.Delete(ctx, dataExport); err != nil {
			return true, fmt.Errorf("failed to delete dataexport object: %s", err)
		}

		data := updateDataExportDetail{
			removeFinalizer: true,
		}
		return true, c.updateStatus(dataExport, data)
	}

	// TODO: validate DataExport resource & update status?
	driverName, err := getDriverType(dataExport)
	if err != nil {
		msg := fmt.Sprintf("failed to get a driver type for %s: %s", dataExport.Spec.Type, err)
		data := updateDataExportDetail{
			status: kdmpapi.DataExportStatusFailed,
			reason: msg,
		}
		return false, c.updateStatus(dataExport, data)
	}
	driver, err := driversinstance.Get(driverName)
	if err != nil {
		msg := fmt.Sprintf("failed to get a driver for a %s type: %s", dataExport.Spec.Type, err)
		data := updateDataExportDetail{
			status: kdmpapi.DataExportStatusFailed,
			reason: msg,
		}
		return false, c.updateStatus(dataExport, data)
	}

	if dataExport.DeletionTimestamp != nil {
		if !controllers.ContainsFinalizer(dataExport, kdmpcontroller.CleanupFinalizer) {
			return false, nil
		}
		if err = c.cleanUp(driver, dataExport); err != nil {
			return true, fmt.Errorf("%s: cleanup: %s", reflect.TypeOf(dataExport), err)
		}

		data := updateDataExportDetail{
			removeFinalizer: true,
		}
		return true, c.updateStatus(dataExport, data)
	}
	switch dataExport.Status.Stage {
	case kdmpapi.DataExportStageInitial:
		return c.stageInitial(ctx, dataExport)
	// TODO: 'merge' scheduled&inProgress&restore stages
	case kdmpapi.DataExportStageSnapshotScheduled:
		return c.stageSnapshotScheduled(ctx, dataExport)
	case kdmpapi.DataExportStageSnapshotInProgress:
		return c.stageSnapshotInProgress(ctx, dataExport)
	case kdmpapi.DataExportStageSnapshotRestore:
		return c.stageSnapshotRestore(ctx, dataExport)
	case kdmpapi.DataExportStageSnapshotRestoreInProgress:
		return c.stageSnapshotRestoreInProgress(ctx, dataExport)
	case kdmpapi.DataExportStageLocalSnapshotRestore:
		return c.stageLocalSnapshotRestore(ctx, dataExport)
	case kdmpapi.DataExportStageLocalSnapshotRestoreInProgress:
		return c.stageLocalSnapshotRestoreInProgress(ctx, dataExport)
	case kdmpapi.DataExportStageTransferScheduled:
		if dataExport.Status.Status == kdmpapi.DataExportStatusSuccessful {
			// set to the next stage
			data := updateDataExportDetail{
				status: kdmpapi.DataExportStatusInitial,
				reason: "",
				stage:  kdmpapi.DataExportStageTransferInProgress,
			}
			return true, c.updateStatus(dataExport, data)
		}

		if dataExport.Status.Status == kdmpapi.DataExportStatusFailed {
			// set to the next stage
			data := updateDataExportDetail{
				stage:  kdmpapi.DataExportStageCleanup,
				status: dataExport.Status.Status,
				reason: "",
			}
			return false, c.updateStatus(dataExport, data)
		}

		// use snapshot pvc in the dst namespace if it's available
		srcPVCName := dataExport.Spec.Source.Name
		if dataExport.Status.SnapshotPVCName != "" {
			srcPVCName = dataExport.Status.SnapshotPVCName
		}

		logrus.Debugf("drivername: %v", driverName)
		var vb *kdmpapi.VolumeBackup
		if driverName == drivers.KopiaRestore {
			// Get the volumebackup
			vb, err = kdmpopts.Instance().GetVolumeBackup(context.Background(),
				dataExport.Spec.Source.Name, dataExport.Spec.Source.Namespace)
			if err != nil {
				msg := fmt.Sprintf("Error accessing volumebackup %s in namespace %s : %v",
					dataExport.Spec.Source.Name, dataExport.Spec.Source.Namespace, err)
				logrus.Errorf(msg)
				data := updateDataExportDetail{
					status: kdmpapi.DataExportStatusFailed,
					reason: msg,
				}
				return false, c.updateStatus(dataExport, data)
			}

			// Create the pvc from the spec provided in the dataexport CR
			pvcSpec := dataExport.Status.RestorePVC
			// For NFS PVC creation happens upfront and createPVC() fails internally during vol restore
			// as in DE CR PVC ref doesn't have all PVC params to create just has pvc name and ns which is
			// expected as PVC is already created so doing additional check.
			_, err = core.Instance().GetPersistentVolumeClaim(pvcSpec.Name, pvcSpec.Namespace)
			if err != nil {
				if k8sErrors.IsNotFound(err) {
					_, err = c.createPVC(dataExport)
					if err != nil {
						msg := fmt.Sprintf("Error creating pvc %s/%s for restore: %v", pvcSpec.Namespace, pvcSpec.Name, err)
						logrus.Errorf(msg)
						data := updateDataExportDetail{
							status: kdmpapi.DataExportStatusFailed,
							reason: msg,
						}
						return false, c.updateStatus(dataExport, data)
					}
				}
			}

			_, err = checkPVCIgnoringJobMounts(dataExport.Spec.Destination, dataExport.Name)
			if err != nil {
				msg := fmt.Sprintf("Destination pvc %s/%s is not bound yet: %v", pvcSpec.Namespace, pvcSpec.Name, err)
				logrus.Errorf(msg)
				data := updateDataExportDetail{
					status: kdmpapi.DataExportStatusFailed,
					reason: msg,
				}
				return false, c.updateStatus(dataExport, data)
			}

			// For restore setting the source PVCName as the destination PVC name for the job
			srcPVCName = dataExport.Spec.Destination.Name
		}
		data, err := c.createJobCredCertSecrets(dataExport, vb, driverName, srcPVCName)
		if err != nil {
			return false, c.updateStatus(dataExport, data)
		}
		// Read the config map to get compression type
		var compressionType string
		var podDataPath string
		kdmpData, err := core.Instance().GetConfigMap(utils.KdmpConfigmapName, utils.KdmpConfigmapNamespace)
		if err != nil {
			logrus.Errorf("failed reading config map %v: %v", utils.KdmpConfigmapName, err)
			logrus.Warnf("default to %s compression", utils.DefaultCompresion)
			compressionType = utils.DefaultCompresion
		} else {
			compressionType = kdmpData.Data[compressionKey]
			podDataPath = kdmpData.Data[backupPath]
		}
		blName := dataExport.Spec.Destination.Name
		blNamespace := dataExport.Spec.Destination.Namespace

		if driverName == drivers.KopiaRestore {
			blName = vb.Spec.BackupLocation.Name
			blNamespace = vb.Spec.BackupLocation.Namespace
		}

		backupLocation, err := readBackupLocation(blName, blNamespace, "")
		if err != nil {
			msg := fmt.Sprintf("reading of backuplocation [%v/%v] failed: %v", blNamespace, blName, err)
			logrus.Errorf(msg)
			data := updateDataExportDetail{
				status: kdmpapi.DataExportStatusFailed,
				reason: msg,
			}
			return false, c.updateStatus(dataExport, data)
		}

		if backupLocation.Location.Type != storkapi.BackupLocationNFS {
			backupLocation.Location.NFSConfig = &storkapi.NFSConfig{}
		}
		// start data transfer
		id, err := startTransferJob(
			driver,
			srcPVCName,
			compressionType,
			dataExport,
			podDataPath,
			utils.KdmpConfigmapName,
			utils.KdmpConfigmapNamespace,
			backupLocation.Location.NFSConfig.ServerAddr,
			backupLocation.Location.NFSConfig.SubPath,
			backupLocation.Location.NFSConfig.MountOptions,
			backupLocation.Location.NFSConfig.SubPath,
		)
		if err != nil && err != utils.ErrJobAlreadyRunning && err != utils.ErrOutOfJobResources {
			msg := fmt.Sprintf("failed to start a data transfer job, dataexport [%v]: %v", dataExport.Name, err)
			logrus.Warnf(msg)
			data := updateDataExportDetail{
				status: kdmpapi.DataExportStatusFailed,
				reason: msg,
			}
			return false, c.updateStatus(dataExport, data)
		} else if err != nil {
			return true, nil
		}

		dataExport.Status.TransferID = id
		data = updateDataExportDetail{
			status:     kdmpapi.DataExportStatusSuccessful,
			transferID: id,
		}
		return true, c.updateStatus(dataExport, data)
	case kdmpapi.DataExportStageTransferInProgress:
		if dataExport.Status.Status == kdmpapi.DataExportStatusSuccessful ||
			dataExport.Status.Status == kdmpapi.DataExportStatusFailed {
			// set to the next stage
			data := updateDataExportDetail{
				stage:  kdmpapi.DataExportStageCleanup,
				status: dataExport.Status.Status,
				reason: "",
			}
			return false, c.updateStatus(dataExport, data)
		}

		// get transfer job status
		progress, err := driver.JobStatus(dataExport.Status.TransferID)
		if err != nil {
			errMsg := fmt.Sprintf("failed to get %s job status: %s", dataExport.Status.TransferID, err)
			data := updateDataExportDetail{
				status: kdmpapi.DataExportStatusFailed,
				reason: errMsg,
			}
			return false, c.updateStatus(dataExport, data)
		}
		// Upon first kopia backup failure, we want job spec to try till the backuOff limit
		// is reached. Once this is reached, k8s marks the job as "Failed" during which we will
		// fail the backup.
		logrus.Infof("DE CR name: %v/%v job status: %v", dataExport.Namespace, dataExport.Name, progress.Status)
		if progress.Status == batchv1.JobFailed {
			data := updateDataExportDetail{
				status: kdmpapi.DataExportStatusFailed,
				reason: progress.Reason,
			}
			if len(progress.Reason) == 0 {
				// As we couldn't get actual reason from kopia executor
				// marking it as internal error
				data.reason = "internal error from executor"
				return true, c.updateStatus(dataExport, data)
			}
			return true, c.updateStatus(dataExport, data)
		} else if progress.Status == batchv1.JobConditionType("") {
			data := updateDataExportDetail{
				status: kdmpapi.DataExportStatusInProgress,
			}
			return true, c.updateStatus(dataExport, data)
		}

		switch progress.State {
		case drivers.JobStateFailed:
			errMsg := fmt.Sprintf("%s transfer job failed: %s", dataExport.Status.TransferID, progress.Reason)
			// If a job has failed it means it has tried all possible retires and given up.
			// In such a scenario we need to fail DE CR and move to clean up stage
			data := updateDataExportDetail{
				status: kdmpapi.DataExportStatusFailed,
				reason: errMsg,
			}
			return true, c.updateStatus(dataExport, data)
		case drivers.JobStateCompleted:
			var vbName string
			var vbNamespace string
			if driverName == drivers.KopiaBackup {
				vbNamespace, vbName, err = utils.ParseJobID(dataExport.Status.TransferID)
				if err != nil {
					errMsg := fmt.Sprintf("failed to parse job ID %v from DataExport CR: %v: %v",
						dataExport.Status.TransferID, dataExport.Name, err)
					data := updateDataExportDetail{
						status: kdmpapi.DataExportStatusFailed,
						reason: errMsg,
					}
					return false, c.updateStatus(dataExport, data)
				}
			}
			if driverName == drivers.KopiaRestore {
				vbName = dataExport.Spec.Source.Name
				vbNamespace = dataExport.Spec.Source.Namespace
			}
			var volumeBackupCR *kdmpapi.VolumeBackup
			var vbErr error
			vbTask := func() (interface{}, bool, error) {
				volumeBackupCR, vbErr = kdmpopts.Instance().GetVolumeBackup(context.Background(),
					vbName, vbNamespace)
				if k8sErrors.IsNotFound(vbErr) {
					errMsg := fmt.Sprintf("volumebackup CR %v/%v not found", vbNamespace, vbName)
					logrus.Errorf("%v", errMsg)
					return "", false, fmt.Errorf(errMsg)
				}
				if vbErr != nil {
					errMsg := fmt.Sprintf("failed to read VolumeBackup CR %v: %v", vbName, err)
					logrus.Errorf("%v", errMsg)
					data := updateDataExportDetail{
						status: kdmpapi.DataExportStatusInProgress,
					}
					err := c.updateStatus(dataExport, data)
					if err != nil {
						return "", false, fmt.Errorf("%v", err)
					}
					return "", true, fmt.Errorf("%v", errMsg)
				}
				return "", false, nil
			}
			if _, err := task.DoRetryWithTimeout(vbTask, defaultTimeout, progressCheckInterval); err != nil {
				errMsg := fmt.Sprintf("max retries done, failed to read VolumeBackup CR %v: %v", vbName, vbErr)
				logrus.Errorf("%v", errMsg)
				// Exhausted all retries, fail the CR
				data := updateDataExportDetail{
					status: kdmpapi.DataExportStatusFailed,
					reason: errMsg,
				}
				return false, c.updateStatus(dataExport, data)
			}

			data := updateDataExportDetail{
				status:             kdmpapi.DataExportStatusSuccessful,
				snapshotID:         volumeBackupCR.Status.SnapshotID,
				size:               volumeBackupCR.Status.TotalBytes,
				progressPercentage: int(progress.ProgressPercents),
			}

			return false, c.updateStatus(dataExport, data)
		}
		data := updateDataExportDetail{
			status: kdmpapi.DataExportStatusInProgress,
		}
		return false, c.updateStatus(dataExport, data)
	case kdmpapi.DataExportStageCleanup:
		var cleanupErr error
		data := updateDataExportDetail{
			stage: kdmpapi.DataExportStageFinal,
		}
		// Append the job-pod log to stork's pod log in case of failure
		// it is best effort approach, hence errors are ignored.
		if dataExport.Status.Status == kdmpapi.DataExportStatusFailed {
			if dataExport.Status.TransferID != "" {
				namespace, name, err := utils.ParseJobID(dataExport.Status.TransferID)
				if err != nil {
					logrus.Infof("job-pod name and namespace extraction failed: %v", err)
				}
				appendPodLogToStork(name, namespace)
			}
		}
		cleanupTask := func() (interface{}, bool, error) {
			cleanupErr := c.cleanUp(driver, dataExport)
			if cleanupErr != nil {
				errMsg := fmt.Sprintf("failed to remove resources: %s", cleanupErr)
				logrus.Errorf("%v", errMsg)
				return "", true, fmt.Errorf("%v", errMsg)
			}

			return "", false, nil
		}
		if _, err := task.DoRetryWithTimeout(cleanupTask, defaultTimeout, progressCheckInterval); err != nil {
			errMsg := fmt.Sprintf("max retries done, dataexport: [%v/%v] cleanup failed with %v", dataExport.Namespace, dataExport.Name, cleanupErr)
			logrus.Errorf("%v", errMsg)
			// Exhausted all retries, fail the CR
			data.status = kdmpapi.DataExportStatusFailed
		}
		return true, c.updateStatus(dataExport, data)
	case kdmpapi.DataExportStageFinal:
		return false, nil
	}
	return false, nil
}

func appendPodLogToStork(jobName string, namespace string) {
	// Get job and check whether it has live pod attaced to it
	job, err := batch.Instance().GetJob(jobName, namespace)
	if err != nil && !k8sErrors.IsNotFound(err) {
		logrus.Infof("failed in getting job %v/%v with err: %v", namespace, jobName, err)
	}
	pods, err := core.Instance().GetPods(
		job.Namespace,
		map[string]string{
			"job-name": job.Name,
		},
	)
	if err != nil {
		logrus.Infof("failed in fetching job pods %s/%s: %v", namespace, jobName, err)
	}
	for _, pod := range pods.Items {
		numLogLines := int64(50)
		podLog, err := core.Instance().GetPodLog(pod.Name, pod.Namespace, &corev1.PodLogOptions{TailLines: &numLogLines})
		if err != nil {
			logrus.Infof("error fetching log of job-pod %s: %v", pod.Name, err)
		} else {
			logrus.Infof("start of job-pod [%s]'s log...", pod.Name)
			logrus.Infof(podLog)
			logrus.Infof("end of job-pod [%s]'s log...", pod.Name)
		}
	}
}

func (c *Controller) createJobCredCertSecrets(
	dataExport *kdmpapi.DataExport,
	vb *kdmpapi.VolumeBackup,
	driverName,
	srcPVCName string,
) (updateDataExportDetail, error) {
	namespace := dataExport.Namespace
	// Below are default values for kopiaRestore. There values will be updated with different, if the driveName is kopiaBackup.
	var blName, blNamespace string
	if driverName == drivers.KopiaRestore {
		blName = vb.Spec.BackupLocation.Name
		blNamespace = vb.Spec.BackupLocation.Namespace
	}
	if driverName == drivers.KopiaBackup {
		pods, err := core.Instance().GetPodsUsingPVC(srcPVCName, dataExport.Spec.Source.Namespace)
		if err != nil {
			msg := fmt.Sprintf("error fetching pods using PVC %s/%s: %v", dataExport.Spec.Source.Namespace, srcPVCName, err)
			logrus.Errorf(msg)
			data := updateDataExportDetail{
				status: kdmpapi.DataExportStatusFailed,
				reason: msg,
			}
			return data, err
		}
		// filter out the pods that are create by us
		count := len(pods)
		for _, pod := range pods {
			labels := pod.ObjectMeta.Labels
			if _, ok := labels[drivers.DriverNameLabel]; ok {
				count--
			}
		}
		if count > 0 {
			namespace = utils.AdminNamespace
		}
		blName = dataExport.Spec.Destination.Name
		blNamespace = dataExport.Spec.Destination.Namespace
	}
	// Creating a secret for ssl enabled objectstore with custom certificates
	// User creates a secret in kube-system ns, mounts it to stork and recommendation is
	// secret data is 'public.crt' always which contains the certificate content
	// This secret path is provided as env section as follows
	// env:
	// - name: SSL_CERT_DIR
	//   value: "/etc/tls-secrets" (can be any path)

	// Check if the above env is present and read the certs file contents and
	// secret for the job pod for kopia to access the same
	err := createCertificateSecret(utils.GetCertSecretName(dataExport.Name), namespace, dataExport.Labels)
	if err != nil {
		msg := fmt.Sprintf("error in creating certificate secret[%v/%v]: %v", namespace, dataExport.Name, err)
		logrus.Errorf(msg)
		data := updateDataExportDetail{
			status: kdmpapi.DataExportStatusFailed,
			reason: msg,
		}
		return data, err
	}
	// This will create a unique secret per PVC being backedup / restore
	// Create secret in source ns because in case of multi ns backup
	// BL CR is created in kube-system ns
	err = CreateCredentialsSecret(
		utils.GetCredSecretName(dataExport.Name),
		blName,
		blNamespace,
		namespace,
		dataExport.Labels,
	)
	if err != nil {
		msg := fmt.Sprintf("failed to create cloud credential secret during %v : %v", driverName, err)
		logrus.Errorf(msg)
		data := updateDataExportDetail{
			status: kdmpapi.DataExportStatusFailed,
			reason: msg,
		}
		return data, err
	}
	return updateDataExportDetail{}, nil
}

func (c *Controller) stageInitial(ctx context.Context, dataExport *kdmpapi.DataExport) (bool, error) {
	if dataExport.Status.Status == kdmpapi.DataExportStatusSuccessful {
		// set to the next stage
		dataExport.Status.Stage = kdmpapi.DataExportStageTransferScheduled
		if hasLocalRestoreStage(dataExport) {
			dataExport.Status.Stage = kdmpapi.DataExportStageLocalSnapshotRestore
		} else if hasSnapshotStage(dataExport) {
			dataExport.Status.Stage = kdmpapi.DataExportStageSnapshotScheduled
		}
		data := updateDataExportDetail{
			stage:  dataExport.Status.Stage,
			status: kdmpapi.DataExportStatusInitial,
			reason: "",
		}
		return true, c.updateStatus(dataExport, data)
	}

	driverName, err := getDriverType(dataExport)
	if err != nil {
		msg := fmt.Sprintf("check failed: %s", err)
		data := updateDataExportDetail{
			status: kdmpapi.DataExportStatusFailed,
			reason: msg,
		}
		return false, c.updateStatus(dataExport, data)
	}
	switch driverName {
	case drivers.Rsync:
		err = c.checkClaims(dataExport)
	case drivers.ResticBackup:
		err = c.checkResticBackup(dataExport)
	case drivers.ResticRestore:
		err = c.checkResticRestore(dataExport)
	case drivers.KopiaBackup:
		err = c.checkKopiaBackup(dataExport)
	case drivers.KopiaRestore:
		err = c.checkKopiaRestore(dataExport)
	}
	if err != nil {
		msg := fmt.Sprintf("check failed: %s", err)
		data := updateDataExportDetail{
			status: kdmpapi.DataExportStatusFailed,
			reason: msg,
		}
		return false, c.updateStatus(dataExport, data)
	}

	data := updateDataExportDetail{
		status: kdmpapi.DataExportStatusSuccessful,
		reason: "",
	}
	return true, c.updateStatus(dataExport, data)
}

func (c *Controller) stageSnapshotScheduled(ctx context.Context, dataExport *kdmpapi.DataExport) (bool, error) {
	if dataExport.Status.Status == kdmpapi.DataExportStatusSuccessful {
		// set to the next stage
		data := updateDataExportDetail{
			stage:  kdmpapi.DataExportStageSnapshotInProgress,
			status: kdmpapi.DataExportStatusInitial,
			reason: "",
		}
		return true, c.updateStatus(dataExport, data)
	}

	snapshotDriverName, err := c.getSnapshotDriverName(dataExport)
	if err != nil {
		return false, fmt.Errorf("failed to get snapshot driver name: %v", err)
	}

	snapshotDriver, err := c.snapshotter.Driver(snapshotDriverName)
	if err != nil {
		return false, fmt.Errorf("failed to get snapshot driver for %v: %v", snapshotDriverName, err)
	}
	// This will create a unique secret per PVC being backed up
	// Create secret in source ns because in case of multi ns backup
	// BL CR is created in kube-system ns
	err = CreateCredentialsSecret(
		dataExport.Name,
		dataExport.Spec.Destination.Name,
		dataExport.Spec.Destination.Namespace,
		dataExport.Spec.Source.Namespace,
		dataExport.Labels,
	)
	if err != nil {
		msg := fmt.Sprintf("failed to create cloud credential secret during kopia backup: %v", err)
		logrus.Errorf(msg)
		data := updateDataExportDetail{
			status: kdmpapi.DataExportStatusFailed,
			reason: msg,
		}
		return false, c.updateStatus(dataExport, data)
	}

	backupUID := getAnnotationValue(dataExport, backupObjectUIDKey)
	pvcUID := getAnnotationValue(dataExport, pvcUIDKey)
	// First check if a snapshot has already been triggered
	snapName := toSnapName(dataExport.Spec.Source.Name, string(dataExport.UID))
	annotations := make(map[string]string)
	annotations[dataExportUIDAnnotation] = string(dataExport.UID)
	annotations[dataExportNameAnnotation] = utils.GetValidLabel(dataExport.Name)
	annotations[backupObjectUIDKey] = backupUID
	annotations[pvcUIDKey] = pvcUID
	labels := make(map[string]string)
	labels[pvcNameKey] = dataExport.Spec.Source.Name
	name, namespace, _, err := snapshotDriver.CreateSnapshot(
		snapshotter.Name(snapName),
		snapshotter.PVCName(dataExport.Spec.Source.Name),
		snapshotter.PVCNamespace(dataExport.Spec.Source.Namespace),
		// TODO: restore namespace is required for external-storage snapshots
		//snapshots.RestoreNamespaces(dataExport.Spec.Destination.Namespace),
		snapshotter.SnapshotClassName(dataExport.Spec.SnapshotStorageClass),
		snapshotter.Annotations(annotations),
		snapshotter.Labels(labels),
	)
	if err != nil {
		msg := fmt.Sprintf("failed to create a snapshot: %s", err)
		data := updateDataExportDetail{
			status: kdmpapi.DataExportStatusFailed,
			reason: msg,
		}
		return false, c.updateStatus(dataExport, data)
	}

	data := updateDataExportDetail{
		snapshotID:        name,
		snapshotNamespace: namespace,
		status:            kdmpapi.DataExportStatusSuccessful,
		volumeSnapshot:    snapName,
		reason:            "",
	}
	return true, c.updateStatus(dataExport, data)
}

func (c *Controller) getSnapshotDriverName(dataExport *kdmpapi.DataExport) (string, error) {
	if len(dataExport.Spec.SnapshotStorageClass) == 0 {
		return "", fmt.Errorf("snapshot storage class not provided")
	}
	if dataExport.Spec.SnapshotStorageClass == "default" ||
		dataExport.Spec.SnapshotStorageClass == "Default" {
		return csiProvider, nil
	}
	// Check if snapshot class is a CSI snapshot class
	config, err := rest.InClusterConfig()
	if err != nil {
		return "", err
	}

	cs, err := kSnapshotClient.NewForConfig(config)
	if err != nil {
		return "", err
	}
	v1SnapshotRequired, err := version.RequiresV1VolumeSnapshot()
	if err != nil {
		return "", err
	}
	if v1SnapshotRequired {
		_, err = cs.SnapshotV1().VolumeSnapshotClasses().Get(context.TODO(), dataExport.Spec.SnapshotStorageClass, metav1.GetOptions{})
	} else {
		_, err = cs.SnapshotV1beta1().VolumeSnapshotClasses().Get(context.TODO(), dataExport.Spec.SnapshotStorageClass, metav1.GetOptions{})
	}
	if err == nil {
		return csiProvider, nil
	}
	if err != nil && !k8sErrors.IsNotFound(err) {
		return "", nil
	}
	return "", fmt.Errorf("did not find any supported snapshot driver for snapshot storage class %s", dataExport.Spec.SnapshotStorageClass)
}

func (c *Controller) stageSnapshotInProgress(ctx context.Context, dataExport *kdmpapi.DataExport) (bool, error) {
	if dataExport.Status.Status == kdmpapi.DataExportStatusSuccessful {
		// set to the next stage
		data := updateDataExportDetail{
			stage:  kdmpapi.DataExportStageSnapshotRestore,
			status: kdmpapi.DataExportStatusInitial,
			reason: "",
		}
		return true, c.updateStatus(dataExport, data)
	}
	if dataExport.Status.Status == kdmpapi.DataExportStatusFailed {
		// set to the next stage
		data := updateDataExportDetail{
			stage:  kdmpapi.DataExportStageCleanup,
			status: dataExport.Status.Status,
		}
		return false, c.updateStatus(dataExport, data)
	}

	snapshotDriverName, err := c.getSnapshotDriverName(dataExport)
	if err != nil {
		return false, fmt.Errorf("failed to get snapshot driver name: %v", err)
	}

	snapshotDriver, err := c.snapshotter.Driver(snapshotDriverName)
	if err != nil {
		return false, fmt.Errorf("failed to get snapshot driver for %v: %v", snapshotDriverName, err)
	}

	snapInfo, err := snapshotDriver.SnapshotStatus(dataExport.Status.SnapshotID, dataExport.Spec.Source.Namespace)
	if err != nil {
		msg := fmt.Sprintf("failed to get a snapshot status: %s", err)
		data := updateDataExportDetail{
			status: kdmpapi.DataExportStatusFailed,
			reason: msg,
		}
		return false, c.updateStatus(dataExport, data)
	}

	if snapInfo.Status == snapshotter.StatusFailed {
		data := updateDataExportDetail{
			status: kdmpapi.DataExportStatusFailed,
			reason: snapInfo.Reason,
		}
		return false, c.updateStatus(dataExport, data)
	}

	if snapInfo.Status != snapshotter.StatusReady {
		data := updateDataExportDetail{
			status: kdmpapi.DataExportStatusInProgress,
			reason: snapInfo.Reason,
		}
		return false, c.updateStatus(dataExport, data)
	}
	// upload the CRs to the objectstore
	var bl *storkapi.BackupLocation
	if bl, err = checkBackupLocation(dataExport.Spec.Destination); err != nil {
		msg := fmt.Sprintf("backuplocation fetch error for %s: %v", dataExport.Spec.Destination.Name, err)
		data := updateDataExportDetail{
			status: kdmpapi.DataExportStatusFailed,
			reason: msg,
		}
		return false, c.updateStatus(dataExport, data)
	}

	backupUID := getAnnotationValue(dataExport, backupObjectUIDKey)
	if err != nil {
		msg := fmt.Sprintf("backup UID annotation is not set in dataexport cr %s/%s: %v", dataExport.Namespace, dataExport.Name, err)
		data := updateDataExportDetail{
			status: kdmpapi.DataExportStatusFailed,
			reason: msg,
		}
		return false, c.updateStatus(dataExport, data)
	}

	pvcUID := getAnnotationValue(dataExport, pvcUIDKey)
	if err != nil {
		msg := fmt.Sprintf("pvc UID annotation is not set in dataexport cr %s/%s: %v", dataExport.Namespace, dataExport.Name, err)
		data := updateDataExportDetail{
			status: kdmpapi.DataExportStatusFailed,
			reason: msg,
		}
		return false, c.updateStatus(dataExport, data)
	}

	if bl.Location.Type != storkapi.BackupLocationNFS {
		v1SnapshotRequired, err := version.RequiresV1VolumeSnapshot()
		if err != nil {
			return false, err
		}

		var vsName, vsNamespace string
		if v1SnapshotRequired {
			vs := snapInfo.SnapshotRequest.(*kSnapshotv1.VolumeSnapshot)
			timestampEpoch := strconv.FormatInt(vs.GetObjectMeta().GetCreationTimestamp().Unix(), 10)
			snapInfoList := []snapshotter.SnapshotInfo{snapInfo}
			err = snapshotDriver.UploadSnapshotObjects(bl, snapInfoList, getCSICRUploadDirectory(pvcUID), getVSFileName(backupUID, timestampEpoch))
			vsName = vs.Name
			vsNamespace = vs.Namespace
		} else {
			vs := snapInfo.SnapshotRequest.(*kSnapshotv1beta1.VolumeSnapshot)
			timestampEpoch := strconv.FormatInt(vs.GetObjectMeta().GetCreationTimestamp().Unix(), 10)
			snapInfoList := []snapshotter.SnapshotInfo{snapInfo}
			err = snapshotDriver.UploadSnapshotObjects(bl, snapInfoList, getCSICRUploadDirectory(pvcUID), getVSFileName(backupUID, timestampEpoch))
			vsName = vs.Name
			vsNamespace = vs.Namespace
		}
		if err != nil {
			msg := fmt.Sprintf("uploading snapshot objects for pvc %s/%s failed with error : %v", vsNamespace, vsName, err)
			logrus.Errorf(msg)
			data := updateDataExportDetail{
				status: kdmpapi.DataExportStatusFailed,
				reason: msg,
			}
			return false, c.updateStatus(dataExport, data)
		}
	}

	data := updateDataExportDetail{
		status: kdmpapi.DataExportStatusSuccessful,
	}
	return true, c.updateStatus(dataExport, data)
}

func (c *Controller) stageSnapshotRestore(ctx context.Context, dataExport *kdmpapi.DataExport) (bool, error) {
	if dataExport.Status.Status == kdmpapi.DataExportStatusSuccessful {
		// set to the next stage
		data := updateDataExportDetail{
			stage:  kdmpapi.DataExportStageSnapshotRestoreInProgress,
			status: kdmpapi.DataExportStatusInitial,
			reason: "",
		}
		return true, c.updateStatus(dataExport, data)
	}

	snapshotDriverName, err := c.getSnapshotDriverName(dataExport)
	if err != nil {
		return false, fmt.Errorf("failed to get snapshot driver name: %v", err)
	}

	snapshotDriver, err := c.snapshotter.Driver(snapshotDriverName)
	if err != nil {
		return false, fmt.Errorf("failed to get snapshot driver for %v: %v", snapshotDriverName, err)
	}

	pvc, err := c.restoreSnapshot(ctx, snapshotDriver, dataExport)
	if err != nil {
		msg := fmt.Sprintf("failed to restore a snapshot: %s", err)
		data := updateDataExportDetail{
			status: kdmpapi.DataExportStatusFailed,
			reason: msg,
		}
		return false, c.updateStatus(dataExport, data)
	}

	dataExport.Status.SnapshotPVCName = pvc.Name
	dataExport.Status.SnapshotPVCNamespace = pvc.Namespace
	data := updateDataExportDetail{
		snapshotPVCName:      pvc.Name,
		snapshotPVCNamespace: pvc.Namespace,
		status:               kdmpapi.DataExportStatusSuccessful,
	}
	return true, c.updateStatus(dataExport, data)
}

func (c *Controller) stageSnapshotRestoreInProgress(ctx context.Context, dataExport *kdmpapi.DataExport) (bool, error) {
	snapshotDriverName, err := c.getSnapshotDriverName(dataExport)
	if err != nil {
		return false, fmt.Errorf("failed to get snapshot driver name: %v", err)
	}

	snapshotDriver, err := c.snapshotter.Driver(snapshotDriverName)
	if err != nil {
		return false, fmt.Errorf("failed to get snapshot driver for %v: %v", snapshotDriverName, err)
	}

	if dataExport.Status.Status == kdmpapi.DataExportStatusSuccessful {
		// set to the next stage
		data := updateDataExportDetail{
			stage:  kdmpapi.DataExportStageTransferScheduled,
			status: kdmpapi.DataExportStatusInitial,
			reason: "",
		}
		return true, c.updateStatus(dataExport, data)
	}
	if dataExport.Status.Status == kdmpapi.DataExportStatusFailed {
		// set to the next stage
		data := updateDataExportDetail{
			stage:  kdmpapi.DataExportStageCleanup,
			status: dataExport.Status.Status,
		}
		return false, c.updateStatus(dataExport, data)
	}

	src := dataExport.Spec.Source
	srcPvc, err := core.Instance().GetPersistentVolumeClaim(src.Name, src.Namespace)
	if err != nil {
		msg := fmt.Sprintf("failed to get restore pvc %v: %v", src.Name, err)
		data := updateDataExportDetail{
			status: kdmpapi.DataExportStatusFailed,
			reason: msg,
		}
		return false, c.updateStatus(dataExport, data)
	}

	restoreInfo, err := snapshotDriver.RestoreStatus(toSnapshotPVCName(srcPvc.Name, string(dataExport.UID)), srcPvc.Namespace)
	if err != nil {
		msg := fmt.Sprintf("failed to get a snapshot restore status: %s", err)
		data := updateDataExportDetail{
			status: kdmpapi.DataExportStatusFailed,
			reason: msg,
		}
		return false, c.updateStatus(dataExport, data)
	}

	if restoreInfo.Status == snapshotter.StatusFailed {
		data := updateDataExportDetail{
			status: kdmpapi.DataExportStatusFailed,
			reason: restoreInfo.Reason,
		}
		return false, c.updateStatus(dataExport, data)
	}

	if restoreInfo.Status != snapshotter.StatusReady {
		data := updateDataExportDetail{
			status: kdmpapi.DataExportStatusInProgress,
			reason: restoreInfo.Reason,
		}
		return false, c.updateStatus(dataExport, data)
	}

	data := updateDataExportDetail{
		status: kdmpapi.DataExportStatusSuccessful,
		reason: restoreInfo.Reason,
	}
	// Add annotation for corresponding PV to be skipped backing
	tempName := toSnapshotPVCName(srcPvc.Name, string(dataExport.UID))
	interPvc, err := core.Instance().GetPersistentVolumeClaim(tempName, srcPvc.Namespace)
	if err != nil {
		msg := fmt.Sprintf("failed to get restore pvc %v: %v", src.Name, err)
		logrus.Warnf("%v", msg)
		data := updateDataExportDetail{
			status: kdmpapi.DataExportStatusFailed,
			reason: msg,
		}
		return false, c.updateStatus(dataExport, data)
	}
	pv, err := core.Instance().GetPersistentVolume(interPvc.Spec.VolumeName)
	if err != nil {
		errMsg := fmt.Sprintf("error fetching PV %s: %s", interPvc.Spec.VolumeName, err)
		logrus.Errorf("%v", errMsg)
	} else {
		pv.Annotations[skipResourceAnnotation] = "true"
		pv, err = core.Instance().UpdatePersistentVolume(pv)
		if err != nil {
			logrus.Warnf("error updating pv %v: %v", pv.Name, err)
		}
	}

	return true, c.updateStatus(dataExport, data)
}

func getBackuplocationFromSecret(secretName, namespace string) (*storkapi.BackupLocation, error) {
	secret, err := core.Instance().GetSecret(secretName, namespace)
	if err != nil {
		logrus.Infof("failed in getting secret [%v]: %v", secretName, err)
		return nil, err
	}
	backupLocation := &storkapi.BackupLocation{
		ObjectMeta: metav1.ObjectMeta{},
		Location:   storkapi.BackupLocationItem{},
	}
	secretData := secret.Data
	var backupType storkapi.BackupLocationType
	switch string(secretData["type"]) {
	case "s3":
		backupType = storkapi.BackupLocationS3
		disableSSL := false
		if string(secretData["disablessl"]) == "true" {
			disableSSL = true
		}
		backupLocation.Location.S3Config = &storkapi.S3Config{
			AccessKeyID:     string(secretData["accessKey"]),
			SecretAccessKey: string(secretData["secretAccessKey"]),
			Endpoint:        string(secretData["endpoint"]),
			DisableSSL:      disableSSL,
			Region:          string(secretData["region"]),
		}
	case "azure":
		backupType = storkapi.BackupLocationAzure
		backupLocation.Location.AzureConfig = &storkapi.AzureConfig{
			StorageAccountName: string(secretData["storageaccountname"]),
			StorageAccountKey:  string(secretData["storageaccountkey"]),
		}
	case "google":
		backupType = storkapi.BackupLocationGoogle
		backupLocation.Location.GoogleConfig = &storkapi.GoogleConfig{
			ProjectID:  string(secretData["projectid"]),
			AccountKey: string(secretData["accountkey"]),
		}
	default:
	}
	backupLocation.Location.Path = string(secretData["path"])
	backupLocation.Name = secretName
	backupLocation.Namespace = namespace
	backupLocation.Location.Type = backupType
	return backupLocation, nil
}

func (c *Controller) stageLocalSnapshotRestore(ctx context.Context, dataExport *kdmpapi.DataExport) (bool, error) {
	if dataExport.Status.Status == kdmpapi.DataExportStatusSuccessful {
		// set to the next stage
		data := updateDataExportDetail{
			stage:  kdmpapi.DataExportStageLocalSnapshotRestoreInProgress,
			status: kdmpapi.DataExportStatusInitial,
			reason: "",
		}
		return true, c.updateStatus(dataExport, data)
	} else if dataExport.Status.Status == kdmpapi.DataExportStatusFailed {
		err := c.cleanupLocalRestoredSnapshotResources(dataExport, false)
		if err != nil {
			logrus.Errorf("cleaning up temporary resources for restoring from snapshot failed for data export %s/%s: %v", dataExport.Namespace, dataExport.Name, err)
		}
		// Already done with max retries, so moving to kdmp restore anyway
		data := updateDataExportDetail{
			stage:  kdmpapi.DataExportStageTransferScheduled,
			status: kdmpapi.DataExportStatusInitial,
			reason: "switching to restore from objectstore bucket as restoring from local snapshot did not happen",
		}
		return false, c.updateStatus(dataExport, data)
	}

	snapshotDriverName, err := c.getSnapshotDriverName(dataExport)
	if err != nil {
		msg := fmt.Sprintf("failed to get snapshot driver name: %v", err)
		data := updateDataExportDetail{
			status: kdmpapi.DataExportStatusFailed,
			reason: msg,
		}
		return false, c.updateStatus(dataExport, data)
	}

	snapshotDriver, err := c.snapshotter.Driver(snapshotDriverName)
	if err != nil {
		msg := fmt.Sprintf("failed to get snapshot driver for %v: %v", snapshotDriverName, err)
		data := updateDataExportDetail{
			status: kdmpapi.DataExportStatusFailed,
			reason: msg,
		}
		return false, c.updateStatus(dataExport, data)
	}

	vb, err := kdmpopts.Instance().GetVolumeBackup(context.Background(),
		dataExport.Spec.Source.Name, dataExport.Spec.Source.Namespace)
	if err != nil {
		msg := fmt.Sprintf("Error accessing volumebackup %s in namespace %s : %v",
			dataExport.Spec.Source.Name, dataExport.Spec.Source.Namespace, err)
		logrus.Errorf(msg)
		data := updateDataExportDetail{
			status: kdmpapi.DataExportStatusFailed,
			reason: msg,
		}
		return false, c.updateStatus(dataExport, data)
	}

	bl, err := stork.Instance().GetBackupLocation(vb.Spec.BackupLocation.Name, vb.Spec.BackupLocation.Namespace)
	if err != nil {
		msg := fmt.Sprintf("Error while getting backuplocation %s/%s : %v",
			dataExport.Spec.Source.Namespace, dataExport.Spec.Source.Name, err)
		data := updateDataExportDetail{
			status: kdmpapi.DataExportStatusFailed,
			reason: msg,
		}
		return false, c.updateStatus(dataExport, data)
	}

	// This will create a unique secret per PVC being restored
	// For restore create the secret in the ns where PVC is referenced
	err = CreateCredentialsSecret(
		dataExport.Name,
		vb.Spec.BackupLocation.Name,
		vb.Spec.BackupLocation.Namespace,
		dataExport.Spec.Destination.Namespace,
		dataExport.Labels,
	)
	if err != nil {
		msg := fmt.Sprintf("failed to create cloud credential secret during local snapshot restore: %v", err)
		logrus.Errorf(msg)
		data := updateDataExportDetail{
			status: kdmpapi.DataExportStatusFailed,
			reason: msg,
		}
		return false, c.updateStatus(dataExport, data)
	}

	backupUID := getAnnotationValue(dataExport, backupObjectUIDKey)
	pvcUID := getAnnotationValue(dataExport, pvcUIDKey)
	status, err := snapshotDriver.RestoreFromLocalSnapshot(bl, dataExport.Status.RestorePVC, snapshotDriverName, pvcUID, backupUID, getCSICRUploadDirectory(pvcUID), dataExport.Namespace)
	if err != nil {
		msg := fmt.Sprintf("Error while restoring from local snapshot with volumebackup %s in namespace %s : %v",
			dataExport.Spec.Source.Name, dataExport.Spec.Source.Namespace, err)
		data := updateDataExportDetail{
			status: kdmpapi.DataExportStatusFailed,
			reason: msg,
		}
		return false, c.updateStatus(dataExport, data)
	}

	if !status {
		msg := fmt.Sprintf("Restoring from local snapshot with volumebackup %s in namespace %s could not be done",
			dataExport.Spec.Source.Name, dataExport.Spec.Source.Namespace)
		data := updateDataExportDetail{
			status: kdmpapi.DataExportStatusFailed,
			reason: msg,
		}
		return false, c.updateStatus(dataExport, data)
	}

	data := updateDataExportDetail{
		status: kdmpapi.DataExportStatusSuccessful,
		reason: fmt.Sprintf("Started restore from local snapshot for volumebackup %s in namespace %s", dataExport.Spec.Source.Name, dataExport.Spec.Source.Namespace),
	}
	return true, c.updateStatus(dataExport, data)
}

func (c *Controller) stageLocalSnapshotRestoreInProgress(ctx context.Context, dataExport *kdmpapi.DataExport) (bool, error) {
	if dataExport.Status.Status == kdmpapi.DataExportStatusSuccessful {
		// set to the next stage
		data := updateDataExportDetail{
			stage:  kdmpapi.DataExportStageCleanup,
			reason: "",
		}
		return true, c.updateStatus(dataExport, data)
	} else if dataExport.Status.Status == kdmpapi.DataExportStatusFailed {
		err := c.cleanupLocalRestoredSnapshotResources(dataExport, false)
		// Already done with max retries, so moving to kdmp restore anyway
		if err != nil {
			logrus.Errorf("cleaning up temporary resources for restoring from snapshot failed for data export %s/%s: %v", dataExport.Namespace, dataExport.Name, err)
		}
		data := updateDataExportDetail{
			stage:  kdmpapi.DataExportStageTransferScheduled,
			status: kdmpapi.DataExportStatusInitial,
			reason: "",
		}
		return false, c.updateStatus(dataExport, data)
	}

	snapshotDriverName, err := c.getSnapshotDriverName(dataExport)
	if err != nil {
		msg := fmt.Sprintf("failed to get snapshot driver name: %v", err)
		data := updateDataExportDetail{
			status: kdmpapi.DataExportStatusFailed,
			reason: msg,
		}
		return false, c.updateStatus(dataExport, data)
	}

	snapshotDriver, err := c.snapshotter.Driver(snapshotDriverName)
	if err != nil {
		msg := fmt.Sprintf("failed to get snapshot driver for %v: %v", snapshotDriverName, err)
		data := updateDataExportDetail{
			status: kdmpapi.DataExportStatusFailed,
			reason: msg,
		}
		return false, c.updateStatus(dataExport, data)
	}

	restoreInfo, err := snapshotDriver.RestoreStatus(dataExport.Status.RestorePVC.Name, dataExport.Namespace)
	if err != nil {
		msg := fmt.Sprintf("failed to get a snapshot restore status: %s", err)
		data := updateDataExportDetail{
			status: kdmpapi.DataExportStatusFailed,
			reason: msg,
		}
		return false, c.updateStatus(dataExport, data)
	}

	if restoreInfo.Status == snapshotter.StatusFailed {
		data := updateDataExportDetail{
			status: kdmpapi.DataExportStatusFailed,
			reason: restoreInfo.Reason,
		}
		return false, c.updateStatus(dataExport, data)
	}

	if restoreInfo.Status != snapshotter.StatusReady {
		data := updateDataExportDetail{
			status: kdmpapi.DataExportStatusInProgress,
			reason: restoreInfo.Reason,
		}
		return false, c.updateStatus(dataExport, data)
	}

	msg := fmt.Sprintf("Volume restore from local snapshot for volumebackup %s in namespace %s is successful", dataExport.Spec.Source.Name, dataExport.Spec.Source.Namespace)
	logrus.Debugf(msg)
	data := updateDataExportDetail{
		status: kdmpapi.DataExportStatusSuccessful,
		reason: msg,
		size:   restoreInfo.Size,
	}
	return true, c.updateStatus(dataExport, data)

}

func (c *Controller) cleanUp(driver drivers.Interface, de *kdmpapi.DataExport) error {
	doCleanup, err := utils.DoCleanupResource()
	if err != nil {
		return err
	}
	if (de.Status.Status == kdmpapi.DataExportStatusFailed) && !doCleanup {
		return nil
	}
	if driver == nil {
		return fmt.Errorf("driver is nil")
	}
	if hasLocalRestoreStage(de) {
		err := c.cleanupLocalRestoredSnapshotResources(de, true)
		if err != nil {
			msg := fmt.Sprintf("failed in cleaning up resources restored for local snapshot restore for dataexport %s/%s: %v", de.Namespace, de.Name, err)
			return fmt.Errorf(msg)
		}
	} else if hasSnapshotStage(de) {
		snapshotDriverName, err := c.getSnapshotDriverName(de)
		if err != nil {
			return fmt.Errorf("failed to get snapshot driver name: %v", err)
		}

		snapshotDriver, err := c.snapshotter.Driver(snapshotDriverName)
		if err != nil {
			return fmt.Errorf("failed to get snapshot driver for %v: %v", snapshotDriverName, err)
		}
		if de.Status.SnapshotPVCName != "" && de.Status.SnapshotPVCNamespace != "" {
			if err := cleanupJobBoundResources(de.Status.SnapshotPVCName, de.Status.SnapshotPVCNamespace); err != nil {
				return fmt.Errorf("cleaning up of bound job resources failed: %v", err)
			}
			if err := core.Instance().DeletePersistentVolumeClaim(de.Status.SnapshotPVCName, de.Status.SnapshotPVCNamespace); err != nil && !k8sErrors.IsNotFound(err) {
				return fmt.Errorf("delete %s/%s pvc: %s", de.Status.SnapshotPVCNamespace, de.Status.SnapshotPVCName, err)
			}
			err = snapshotDriver.DeleteSnapshot(de.Status.VolumeSnapshot, de.Status.SnapshotPVCNamespace, true)
			msg := fmt.Sprintf("failed in removing local volume snapshot CRs for %s/%s: %v", de.Status.VolumeSnapshot, de.Status.SnapshotPVCName, err)
			if err != nil {
				logrus.Errorf(msg)
				return fmt.Errorf(msg)
			}
		}
	}
	// use snapshot pvc in the dst namespace if it's available
	srcPVCName := de.Spec.Source.Name
	if de.Status.SnapshotPVCName != "" {
		srcPVCName = de.Status.SnapshotPVCName
	}
	pods, err := core.Instance().GetPodsUsingPVC(srcPVCName, de.Spec.Source.Namespace)
	if err != nil {
		return fmt.Errorf("error fetching pods using PVC %s/%s: %v", de.Spec.Source.Namespace, srcPVCName, err)
	}
	var namespace string
	if len(pods) > 0 {
		namespace = utils.AdminNamespace
	} else {
		namespace = de.Namespace
	}
	// Delete the tls certificate secret created
	err = core.Instance().DeleteSecret(utils.GetCertSecretName(de.Name), namespace)
	if err != nil && !k8sErrors.IsNotFound(err) {
		errMsg := fmt.Sprintf("failed to delete [%s/%s] secret", namespace, de.Name)
		logrus.Errorf("%v", errMsg)
		return fmt.Errorf("%v", errMsg)
	}
	if de.Status.TransferID != "" {
		err := driver.DeleteJob(de.Status.TransferID)
		if err != nil && !k8sErrors.IsNotFound(err) {
			return fmt.Errorf("delete %s job: %s", de.Status.TransferID, err)
		}
		//TODO : Need better way to find BL type from de CR
		// For now deleting unconditionally for all BL type.
		namespace, jobName, err := utils.ParseJobID(de.Status.TransferID)
		if err != nil {
			return err
		}
		pvcName := utils.GetPvcNameForJob(jobName)
		if err := core.Instance().DeletePersistentVolumeClaim(pvcName, namespace); err != nil && !k8sErrors.IsNotFound(err) {
			return fmt.Errorf("delete %s/%s pvc: %s", namespace, pvcName, err)
		}

		pvName := utils.GetPvNameForJob(jobName)
		if err := core.Instance().DeletePersistentVolume(pvName); err != nil && !k8sErrors.IsNotFound(err) {
			return fmt.Errorf("delete %s pv: %s", pvName, err)
		}
	}

	if err := core.Instance().DeleteSecret(utils.GetCredSecretName(de.Name), namespace); err != nil && !k8sErrors.IsNotFound(err) {
		errMsg := fmt.Sprintf("deletion of backup credential secret %s failed: %v", de.Name, err)
		logrus.Errorf(errMsg)
		return fmt.Errorf(errMsg)
	}
	// Deleting image secret, if present
	// Not checking for presence of the secret, instead try delete and ignore if the error is NotFound
	if err := core.Instance().DeleteSecret(utils.GetImageSecretName(de.Name), namespace); err != nil && !k8sErrors.IsNotFound(err) {
		errMsg := fmt.Sprintf("deletion of image secret %s failed: %v", de.Name, err)
		logrus.Errorf(errMsg)
		return fmt.Errorf(errMsg)
	}

	return nil
}

func (c *Controller) cleanupLocalRestoredSnapshotResources(de *kdmpapi.DataExport, ignorePVC bool) error {
	// ignorepvc should be set where we want to keep the pvc like in a successful restore from local snapshot
	// else this pvc needs to be deleted so that kdmp restore can happen using another pvc without any datasource
	var cleanupErr error
	var snapshotDriverName string
	var snapshotDriver snapshotter.Driver
	var bl *storkapi.BackupLocation
	t := func() (interface{}, bool, error) {

		snapshotDriverName, cleanupErr = c.getSnapshotDriverName(de)
		if snapshotDriverName == "" {
			logrus.Errorf("cleanupLocalRestoredSnapshotResources: snapshotDriverName is empty: %v", cleanupErr)
			return nil, false, nil
		}

		snapshotDriver, cleanupErr = c.snapshotter.Driver(snapshotDriverName)
		if cleanupErr != nil {
			return nil, false, fmt.Errorf("failed to get snapshot driver for %v: %v", snapshotDriverName, cleanupErr)
		}

		// Construct the backuplocation from the secret
		// Since we same name across the resources, using the de name and namespace as secret name and namespace
		bl, cleanupErr = getBackuplocationFromSecret(de.Name, de.Spec.Source.Namespace)
		if cleanupErr != nil {
			// If secret is not found, we assume, it is retry
			if k8sErrors.IsNotFound(cleanupErr) {
				return nil, false, nil
			}
			msg := fmt.Sprintf("error while getting backuplocation from secret %s/%s : %v",
				de.Spec.Source.Namespace, de.Name, cleanupErr)
			return nil, false, fmt.Errorf(msg)
		}

		backupUID := getAnnotationValue(de, backupObjectUIDKey)
		pvcUID := getAnnotationValue(de, pvcUIDKey)
		pvcSpec := &corev1.PersistentVolumeClaim{}

		if !ignorePVC {
			pvcSpec = de.Status.RestorePVC
			if err := cleanupJobBoundResources(pvcSpec.Name, de.Namespace); err != nil {
				return nil, false, fmt.Errorf("cleaning up of bound job resources failed: %v", err)
			}
		}
		cleanupErr = snapshotDriver.CleanUpRestoredResources(bl, pvcSpec, pvcUID, backupUID, getCSICRUploadDirectory(pvcUID), de.Namespace)
		if cleanupErr != nil {
			return nil, false, cleanupErr
		}

		return nil, false, nil
	}

	if _, err := task.DoRetryWithTimeout(t, defaultTimeout, progressCheckInterval); err != nil {
		errMsg := fmt.Sprintf("max retries done, failed updating DE CR %s/%s: %v", de.Namespace, de.Name, cleanupErr)
		logrus.Errorf("%v", errMsg)
		// Exhausted all retries, fail the CR
		return err
	}
	return nil
}

func (c *Controller) updateStatus(de *kdmpapi.DataExport, data updateDataExportDetail) error {
	var actualErr error
	t := func() (interface{}, bool, error) {
		namespacedName := types.NamespacedName{}
		namespacedName.Name = de.Name
		namespacedName.Namespace = de.Namespace
		actualErr := c.client.Get(context.TODO(), namespacedName, de)
		if actualErr != nil && !k8sErrors.IsNotFound(actualErr) {
			errMsg := fmt.Sprintf("failed in getting DE CR %v/%v: %v", de.Namespace, de.Name, actualErr)
			logrus.Infof("%v", errMsg)
			return "", true, fmt.Errorf("%v", errMsg)
		}
		if data.stage != "" {
			de.Status.Stage = data.stage
		}
		if data.status != "" {
			de.Status.Status = data.status
		}
		if data.transferID != "" {
			de.Status.TransferID = data.transferID
		}
		if data.reason != "" {
			de.Status.Reason = data.reason
		}
		if data.snapshotID != "" {
			de.Status.SnapshotID = data.snapshotID
		}
		if data.size != uint64(0) {
			de.Status.Size = data.size
		}
		if data.progressPercentage != 0 {
			de.Status.ProgressPercentage = data.progressPercentage
		}
		if data.snapshotPVCName != "" {
			de.Status.SnapshotPVCName = data.snapshotPVCName
		}
		if data.snapshotPVCNamespace != "" {
			de.Status.SnapshotPVCNamespace = data.snapshotPVCNamespace
		}
		if data.snapshotID != "" {
			de.Status.SnapshotID = data.snapshotID
		}
		if data.snapshotNamespace != "" {
			de.Status.SnapshotNamespace = data.snapshotNamespace
		}
		if data.removeFinalizer {
			controllers.RemoveFinalizer(de, kdmpcontroller.CleanupFinalizer)
		}
		if data.volumeSnapshot != "" {
			de.Status.VolumeSnapshot = data.volumeSnapshot
		}

		actualErr = c.client.Update(context.TODO(), de)
		if actualErr != nil {
			errMsg := fmt.Sprintf("failed updating DE CR %s: %v", de.Name, actualErr)
			logrus.Errorf("%v", errMsg)
			return "", true, fmt.Errorf("%v", errMsg)
		}

		return "", false, nil
	}
	if _, err := task.DoRetryWithTimeout(t, defaultTimeout, progressCheckInterval); err != nil {
		errMsg := fmt.Sprintf("max retries done, failed updating DE CR %s: %v", de.Name, actualErr)
		logrus.Errorf("%v", errMsg)
		// Exhausted all retries, fail the CR
		return fmt.Errorf("%v", errMsg)
	}
	return nil
}

func (c *Controller) restoreSnapshot(ctx context.Context, snapshotDriver snapshotter.Driver, de *kdmpapi.DataExport) (*corev1.PersistentVolumeClaim, error) {
	if snapshotDriver == nil {
		return nil, fmt.Errorf("snapshot driver is nil")
	}

	src := de.Spec.Source
	srcPvc, err := core.Instance().GetPersistentVolumeClaim(src.Name, src.Namespace)
	if err != nil {
		return nil, err
	}

	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      toSnapshotPVCName(srcPvc.Name, string(de.UID)),
			Namespace: srcPvc.Namespace,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      srcPvc.Spec.AccessModes,
			Resources:        srcPvc.Spec.Resources,
			StorageClassName: srcPvc.Spec.StorageClassName, // use the same SC as the source PVC
		},
	}

	// We don't want other reconcilors (if any) to backup this temporary PVC
	pvc.Annotations = make(map[string]string)
	pvc.Annotations[skipResourceAnnotation] = "true"
	pvc.Annotations[dataExportUIDAnnotation] = string(de.UID)
	pvc.Annotations[dataExportNameAnnotation] = utils.GetValidLabel(de.Name)

	// If storage class annotation is set , then put that annotation too in the temp pvc
	// Sometimes the spec.storageclass might be empty, in that case the temp pvc may get the sc as the default sc
	if srcSC, ok := srcPvc.Annotations[baseSCAnnotation]; ok {
		pvc.Annotations[baseSCAnnotation] = srcSC
	}

	pvc, err = snapshotDriver.RestoreVolumeClaim(
		snapshotter.RestoreNamespace(de.Namespace),
		snapshotter.RestoreSnapshotName(de.Status.SnapshotID),
		snapshotter.PVC(*pvc),
	)

	if err != nil {
		return nil, fmt.Errorf("restore pvc from %s snapshot failed: %s", de.Status.SnapshotID, err)
	}

	de.Status.SnapshotPVCName = pvc.Name
	de.Status.SnapshotPVCNamespace = pvc.Namespace
	return pvc, nil
}

func (c *Controller) createPVC(dataExport *kdmpapi.DataExport) (*corev1.PersistentVolumeClaim, error) {
	pvc := dataExport.Status.RestorePVC
	newPVC, err := core.Instance().CreatePersistentVolumeClaim(pvc)
	if err != nil {
		if k8sErrors.IsAlreadyExists(err) {
			return pvc, nil
		}
		return nil, fmt.Errorf("failed to create PVC %s: %s", pvc.Name, err.Error())
	}
	return newPVC, nil
}

func (c *Controller) checkClaims(de *kdmpapi.DataExport) error {
	if !hasSnapshotStage(de) && de.Spec.Source.Namespace != de.Spec.Destination.Namespace {
		return fmt.Errorf("source and destination volume claims should be in the same namespace if no snapshot class is provided")
	}

	// ignore a check for mounted pods if a source pvc has a snapshot (data will be copied from the snapshot)
	srcPVC, err := checkPVC(de.Spec.Source, !hasSnapshotStage(de))
	if err != nil {
		return fmt.Errorf("source pvc: %v", err)
	}

	dstPVC, err := checkPVC(de.Spec.Destination, true)
	if err != nil {
		return fmt.Errorf("destination pvc: %v", err)
	}

	srcReq := srcPVC.Spec.Resources.Requests[corev1.ResourceStorage]
	dstReq := dstPVC.Spec.Resources.Requests[corev1.ResourceStorage]
	// dstReq < srcReq
	if dstReq.Cmp(srcReq) == -1 {
		return fmt.Errorf("size of the destination pvc (%s) is less than of the source one (%s)", dstReq.String(), srcReq.String())
	}

	return nil
}

func (c *Controller) checkResticBackup(de *kdmpapi.DataExport) error {
	return c.checkGenericBackup(de)
}

func (c *Controller) checkResticRestore(de *kdmpapi.DataExport) error {
	return c.checkGenericRestore(de)
}

func (c *Controller) checkKopiaBackup(de *kdmpapi.DataExport) error {
	return c.checkGenericBackup(de)
}

func (c *Controller) checkKopiaRestore(de *kdmpapi.DataExport) error {
	return c.checkGenericRestore(de)
}

func (c *Controller) checkGenericBackup(de *kdmpapi.DataExport) error {
	if !isPVCRef(de.Spec.Source) && !isAPIVersionKindNotSetRef(de.Spec.Source) {
		return fmt.Errorf("source is expected to be PersistentVolumeClaim")
	}
	// restic/kopia supports "live" backups so there is not need to check if it's mounted
	if _, err := checkPVC(de.Spec.Source, false); err != nil {
		return fmt.Errorf("source: %s", err)
	}

	if !isBackupLocationRef(de.Spec.Destination) {
		return fmt.Errorf("source is expected to be Backuplocation")
	}
	if _, err := checkBackupLocation(de.Spec.Destination); err != nil {
		return fmt.Errorf("destination: %s", err)
	}
	return nil
}

func (c *Controller) checkGenericRestore(de *kdmpapi.DataExport) error {
	if !isVolumeBackupRef(de.Spec.Source) {
		return fmt.Errorf("source is expected to be VolumeBackup")
	}
	if _, err := checkVolumeBackup(de.Spec.Source); err != nil {
		return fmt.Errorf("source: %s", err)
	}

	if !isPVCRef(de.Spec.Destination) && !isAPIVersionKindNotSetRef(de.Spec.Destination) {
		return fmt.Errorf("destination is expected to be PersistentVolumeClaim")
	}

	return nil
}

func startTransferJob(
	drv drivers.Interface,
	srcPVCName string,
	compressionType string,
	dataExport *kdmpapi.DataExport,
	podDataPath string,
	jobConfigMap string,
	jobConfigMapNs string,
	nfsServerAddr string,
	nfsExportPath string,
	nfsMountOption string,
	nfsSubPath string,
) (string, error) {
	if drv == nil {
		return "", fmt.Errorf("data transfer driver is not set")
	}

	switch drv.Name() {
	case drivers.Rsync:
		return drv.StartJob(
			drivers.WithSourcePVC(srcPVCName),
			drivers.WithNamespace(dataExport.Spec.Destination.Namespace),
			drivers.WithDestinationPVC(dataExport.Spec.Destination.Name),
			drivers.WithLabels(dataExport.Labels),
		)
	case drivers.ResticBackup:
		return drv.StartJob(
			drivers.WithSourcePVC(srcPVCName),
			drivers.WithNamespace(dataExport.Spec.Destination.Namespace),
			drivers.WithBackupLocationName(dataExport.Spec.Destination.Name),
			drivers.WithBackupLocationNamespace(dataExport.Spec.Destination.Namespace),
			drivers.WithLabels(dataExport.Labels),
		)
	case drivers.ResticRestore:
		return drv.StartJob(
			drivers.WithSourcePVC(srcPVCName),
			drivers.WithDestinationPVC(dataExport.Spec.Destination.Name),
			drivers.WithNamespace(dataExport.Spec.Source.Namespace),
			drivers.WithVolumeBackupName(dataExport.Spec.Source.Name),
			drivers.WithVolumeBackupNamespace(dataExport.Spec.Source.Namespace),
			drivers.WithLabels(dataExport.Labels),
		)
	case drivers.KopiaBackup:
		return drv.StartJob(
			drivers.WithKopiaImageExecutorSource(dataExport.Spec.TriggeredFrom),
			drivers.WithKopiaImageExecutorSourceNs(dataExport.Spec.TriggeredFromNs),
			drivers.WithSourcePVC(srcPVCName),
			drivers.WithSourcePVCNamespace(dataExport.Spec.Source.Namespace),
			drivers.WithRepoPVC(getRepoPVCName(dataExport, srcPVCName)),
			drivers.WithNamespace(dataExport.Spec.Source.Namespace),
			drivers.WithBackupLocationName(dataExport.Spec.Destination.Name),
			drivers.WithBackupLocationNamespace(dataExport.Spec.Destination.Namespace),
			drivers.WithLabels(dataExport.Labels),
			drivers.WithDataExportName(dataExport.GetName()),
			drivers.WithCertSecretName(utils.GetCertSecretName(dataExport.GetName())),
			drivers.WithCertSecretNamespace(dataExport.Spec.Source.Namespace),
			drivers.WithCompressionType(compressionType),
			drivers.WithPodDatapathType(podDataPath),
			drivers.WithJobConfigMap(jobConfigMap),
			drivers.WithJobConfigMapNs(jobConfigMapNs),
			drivers.WithNfsServer(nfsServerAddr),
			drivers.WithNfsExportDir(nfsExportPath),
			drivers.WithNfsMountOption(nfsMountOption),
			drivers.WithNfsSubPath(nfsSubPath),
		)
	case drivers.KopiaRestore:
		return drv.StartJob(
			drivers.WithKopiaImageExecutorSource(dataExport.Spec.TriggeredFrom),
			drivers.WithKopiaImageExecutorSourceNs(dataExport.Spec.TriggeredFromNs),
			drivers.WithDestinationPVC(dataExport.Spec.Destination.Name),
			drivers.WithNamespace(dataExport.Spec.Destination.Namespace),
			drivers.WithVolumeBackupName(dataExport.Spec.Source.Name),
			drivers.WithVolumeBackupNamespace(dataExport.Spec.Source.Namespace),
			drivers.WithBackupLocationNamespace(dataExport.Spec.Source.Namespace),
			drivers.WithLabels(dataExport.Labels),
			drivers.WithDataExportName(dataExport.GetName()),
			drivers.WithCertSecretName(utils.GetCertSecretName(dataExport.GetName())),
			drivers.WithCertSecretNamespace(dataExport.Spec.Destination.Namespace),
			drivers.WithJobConfigMap(jobConfigMap),
			drivers.WithJobConfigMapNs(jobConfigMapNs),
			drivers.WithNfsServer(nfsServerAddr),
			drivers.WithNfsExportDir(nfsExportPath),
			drivers.WithNfsSubPath(nfsSubPath),
		)
	}

	return "", fmt.Errorf("unknown data transfer driver: %s", drv.Name())
}

func checkPVC(in kdmpapi.DataExportObjectReference, checkMounts bool) (*corev1.PersistentVolumeClaim, error) {
	if err := checkNameNamespace(in); err != nil {
		return nil, err
	}
	// wait for pvc to get bound
	pvc, err := utils.WaitForPVCBound(in.Name, in.Namespace)
	if err != nil {
		return nil, err
	}

	if checkMounts {
		pods, err := core.Instance().GetPodsUsingPVC(pvc.Name, pvc.Namespace)
		if err != nil {
			return nil, fmt.Errorf("get mounted pods: %v", err)
		}

		if len(pods) > 0 {
			return nil, fmt.Errorf("mounted to %v pods", toPodNames(pods))
		}
	}
	return pvc, nil
}

func checkPVCIgnoringJobMounts(in kdmpapi.DataExportObjectReference, expectedMountJob string) (*corev1.PersistentVolumeClaim, error) {
	var pvc *corev1.PersistentVolumeClaim
	var checkErr error
	checkTask := func() (interface{}, bool, error) {
		if checkErr := checkNameNamespace(in); checkErr != nil {
			return "", true, checkErr
		}
		pvc, checkErr = core.Instance().GetPersistentVolumeClaim(in.Name, in.Namespace)
		if checkErr != nil {
			return "", true, checkErr
		}
		storageClassName := k8shelper.GetPersistentVolumeClaimClass(pvc)
		if storageClassName != "" {
			var sc *storagev1.StorageClass
			sc, checkErr = storage.Instance().GetStorageClass(storageClassName)
			if checkErr != nil {
				return "", true, checkErr
			}
			logrus.Debugf("checkPVCIgnoringJobMounts: pvc name %v - storage class VolumeBindingMode %v", pvc.Name, *sc.VolumeBindingMode)
			if *sc.VolumeBindingMode != storagev1.VolumeBindingWaitForFirstConsumer {
				// wait for pvc to get bound
				pvc, checkErr = utils.WaitForPVCBound(in.Name, in.Namespace)
				if checkErr != nil {
					return "", false, checkErr
				}
			}
		} else {
			// If sc is not set, we will direct check the pvc status
			// wait for pvc to get bound
			pvc, checkErr = utils.WaitForPVCBound(in.Name, in.Namespace)
			if checkErr != nil {
				return "", false, checkErr
			}
		}
		var pods []corev1.Pod
		pods, checkErr = core.Instance().GetPodsUsingPVC(pvc.Name, pvc.Namespace)
		if checkErr != nil {
			return "", true, fmt.Errorf("get mounted pods: %v", checkErr)
		}

		if len(pods) > 0 {
			for _, pod := range pods {
				if podBelongsToJob(pod, expectedMountJob, pvc.Namespace) {
					return "", false, nil
				}
			}
			checkErr = fmt.Errorf("mounted to %v pods", toPodNames(pods))
			return "", false, checkErr
		}
		return "", false, nil
	}
	if _, err := task.DoRetryWithTimeout(checkTask, defaultTimeout, progressCheckInterval); err != nil {
		errMsg := fmt.Sprintf("max retries done, failed in checking the PVC status of %v/%v: %v", in.Namespace, in.Name, checkErr)
		logrus.Errorf("%v", errMsg)
		// Exhausted all retries, fail the CR
		return nil, fmt.Errorf("%v", errMsg)
	}
	return pvc, nil
}

func podBelongsToJob(pod corev1.Pod, job, namespace string) bool {
	jobPods, err := core.Instance().GetPods(
		namespace,
		map[string]string{
			"job-name": job,
		},
	)
	if err != nil {
		return false
	}
	for _, jobPod := range jobPods.Items {
		if pod.Name == jobPod.Name {
			return true
		}
	}
	return false
}

// try to delete the job used for binding if it is present
// the job name is same as the staging pvc name
func cleanupJobBoundResources(pvcName, namespace string) error {
	pvc, err := core.Instance().GetPersistentVolumeClaim(pvcName, namespace)
	if err != nil {
		if k8sErrors.IsNotFound(err) {
			logrus.Warnf("cleaning up of job for pvc %s/%s failed as pvc does not exist", namespace, pvcName)
			return nil
		}
		return fmt.Errorf("cleaning up of bound job for pvc %s/%s failed: %v", pvcName, namespace, err)
	}

	jobName := toBoundJobPVCName(pvc.Name, string(pvc.GetUID()))

	if err := batch.Instance().DeleteJobWithForce(jobName, namespace); err != nil && !k8sErrors.IsNotFound(err) {
		return fmt.Errorf("deletion of job %s/%s failed: %v", namespace, jobName, err)
	}
	if err := utils.CleanServiceAccount(jobName, namespace); err != nil {
		return fmt.Errorf("deletion of service account %s/%s failed: %v", namespace, jobName, err)
	}
	return nil
}

func checkBackupLocation(ref kdmpapi.DataExportObjectReference) (*storkapi.BackupLocation, error) {
	if err := checkNameNamespace(ref); err != nil {
		return nil, err
	}
	return stork.Instance().GetBackupLocation(ref.Name, ref.Namespace)
}

func checkVolumeBackup(ref kdmpapi.DataExportObjectReference) (*kdmpapi.VolumeBackup, error) {
	if err := checkNameNamespace(ref); err != nil {
		return nil, err
	}
	return kdmpopts.Instance().GetVolumeBackup(context.Background(), ref.Name, ref.Namespace)
}

func toPodNames(objs []corev1.Pod) []string {
	out := make([]string, 0)
	for _, o := range objs {
		out = append(out, o.Name)
	}
	return out
}

func hasSnapshotStage(de *kdmpapi.DataExport) bool {
	return de.Spec.SnapshotStorageClass != ""
}

func hasLocalRestoreStage(de *kdmpapi.DataExport) bool {
	return de.Status.LocalSnapshotRestore
}

func getDriverType(de *kdmpapi.DataExport) (string, error) {
	src := de.Spec.Source
	dst := de.Spec.Destination
	doBackup := false
	doRestore := false

	switch {
	case isPVCRef(src) || isAPIVersionKindNotSetRef(src):
		if isBackupLocationRef(dst) {
			doBackup = true
		} else {
			return "", fmt.Errorf("invalid kind for generic backup destination: expected BackupLocation")
		}
	case isVolumeBackupRef(src):
		if isPVCRef(dst) || (isAPIVersionKindNotSetRef(dst)) {
			doRestore = true
		} else {
			return "", fmt.Errorf("invalid kind for generic restore destination: expected PersistentVolumeClaim")
		}
	}

	switch de.Spec.Type {
	case kdmpapi.DataExportRsync:
	case kdmpapi.DataExportRestic:
		if doBackup {
			return drivers.ResticBackup, nil
		}

		if doRestore {
			return drivers.ResticRestore, nil
		}
		return "", fmt.Errorf("invalid kind for generic source: expected PersistentVolumeClaim or VolumeBackup")
	case kdmpapi.DataExportKopia:
		if doBackup {
			return drivers.KopiaBackup, nil
		}

		if doRestore {
			return drivers.KopiaRestore, nil
		}
		return "", fmt.Errorf("invalid kind for generic source: expected PersistentVolumeClaim or VolumeBackup")
	}

	return string(de.Spec.Type), nil
}

func isPVCRef(ref kdmpapi.DataExportObjectReference) bool {
	return ref.Kind == "PersistentVolumeClaim" && ref.APIVersion == "v1"
}

func isBackupLocationRef(ref kdmpapi.DataExportObjectReference) bool {
	return ref.Kind == "BackupLocation" && ref.APIVersion == "stork.libopenstorage.org/v1alpha1"
}

func isVolumeBackupRef(ref kdmpapi.DataExportObjectReference) bool {
	return ref.Kind == "VolumeBackup" && ref.APIVersion == "kdmp.portworx.com/v1alpha1"
}

func isAPIVersionKindNotSetRef(ref kdmpapi.DataExportObjectReference) bool {
	return ref.Kind == "" && ref.APIVersion == ""
}

func checkNameNamespace(ref kdmpapi.DataExportObjectReference) error {
	if ref.Name == "" {
		return fmt.Errorf("name has to be set")
	}
	if ref.Namespace == "" {
		return fmt.Errorf("namespace has to be set")
	}
	return nil
}

// CreateCredentialsSecret parses the provided backup location and creates secret with cloud credentials
func CreateCredentialsSecret(secretName, blName, blNamespace, namespace string, labels map[string]string) error {
	backupLocation, err := readBackupLocation(blName, blNamespace, "")
	if err != nil {
		return err
	}

	// TODO: Add for other cloud providers
	// Creating cloud cred secret
	switch backupLocation.Location.Type {
	case storkapi.BackupLocationS3:
		return createS3Secret(secretName, backupLocation, namespace, labels)
	case storkapi.BackupLocationGoogle:
		return createGoogleSecret(secretName, backupLocation, namespace, labels)
	case storkapi.BackupLocationAzure:
		return createAzureSecret(secretName, backupLocation, namespace, labels)
	case storkapi.BackupLocationNFS:
		return utils.CreateNfsSecret(secretName, backupLocation, namespace, labels)
	}

	return fmt.Errorf("unsupported backup location: %v", backupLocation.Location.Type)
}

func readBackupLocation(name, namespace, filePath string) (*storkapi.BackupLocation, error) {
	if name != "" {
		if namespace == "" {
			namespace = "default"
		}
		return stork.Instance().GetBackupLocation(name, namespace)
	}

	// TODO: This is needed for restic, we can think of removing it later
	f, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}

	out := &storkapi.BackupLocation{}
	if err = yaml.NewYAMLOrJSONDecoder(f, 1024).Decode(out); err != nil {
		return nil, err
	}

	return out, nil
}

func createS3Secret(secretName string, backupLocation *storkapi.BackupLocation, namespace string, labels map[string]string) error {
	credentialData := make(map[string][]byte)
	credentialData["endpoint"] = []byte(backupLocation.Location.S3Config.Endpoint)
	credentialData["accessKey"] = []byte(backupLocation.Location.S3Config.AccessKeyID)
	credentialData["secretAccessKey"] = []byte(backupLocation.Location.S3Config.SecretAccessKey)
	credentialData["region"] = []byte(backupLocation.Location.S3Config.Region)
	credentialData["path"] = []byte(backupLocation.Location.Path)
	credentialData["type"] = []byte(backupLocation.Location.Type)
	credentialData["password"] = []byte(backupLocation.Location.RepositoryPassword)
	credentialData["disablessl"] = []byte(strconv.FormatBool(backupLocation.Location.S3Config.DisableSSL))
	err := utils.CreateJobSecret(secretName, namespace, credentialData, labels)

	return err
}

func createGoogleSecret(secretName string, backupLocation *storkapi.BackupLocation, namespace string, labels map[string]string) error {
	credentialData := make(map[string][]byte)
	credentialData["type"] = []byte(backupLocation.Location.Type)
	credentialData["password"] = []byte(backupLocation.Location.RepositoryPassword)
	credentialData["accountkey"] = []byte(backupLocation.Location.GoogleConfig.AccountKey)
	credentialData["projectid"] = []byte(backupLocation.Location.GoogleConfig.ProjectID)
	credentialData["path"] = []byte(backupLocation.Location.Path)
	err := utils.CreateJobSecret(secretName, namespace, credentialData, labels)

	return err
}

func createAzureSecret(secretName string, backupLocation *storkapi.BackupLocation, namespace string, labels map[string]string) error {
	credentialData := make(map[string][]byte)
	credentialData["type"] = []byte(backupLocation.Location.Type)
	credentialData["password"] = []byte(backupLocation.Location.RepositoryPassword)
	credentialData["path"] = []byte(backupLocation.Location.Path)
	credentialData["storageaccountname"] = []byte(backupLocation.Location.AzureConfig.StorageAccountName)
	credentialData["storageaccountkey"] = []byte(backupLocation.Location.AzureConfig.StorageAccountKey)
	err := utils.CreateJobSecret(secretName, namespace, credentialData, labels)

	return err
}

func createCertificateSecret(secretName, namespace string, labels map[string]string) error {
	drivers.CertFilePath = os.Getenv(drivers.CertDirPath)
	if drivers.CertFilePath != "" {
		certificateData, err := os.ReadFile(filepath.Join(drivers.CertFilePath, drivers.CertFileName))
		if err != nil {
			errMsg := fmt.Sprintf("failed reading data from file %s : %s", drivers.CertFilePath, err)
			logrus.Errorf("%v", errMsg)
			return fmt.Errorf(errMsg)
		}

		certData := make(map[string][]byte)
		certData[drivers.CertFileName] = certificateData
		err = utils.CreateJobSecret(secretName, namespace, certData, labels)

		return err
	}

	return nil
}

func toSnapName(pvcName, dataExportUID string) string {
	truncatedPVCName := pvcName
	if len(pvcName) > pvcNameLenLimit {
		truncatedPVCName = pvcName[:pvcNameLenLimit]
	}
	uidToken := strings.Split(dataExportUID, "-")
	return fmt.Sprintf("%s-%s", truncatedPVCName, uidToken[0])
}

func toSnapshotPVCName(pvcName string, dataExportUID string) string {
	truncatedPVCName := pvcName
	if len(pvcName) > pvcNameLenLimit {
		truncatedPVCName = pvcName[:pvcNameLenLimit]
	}
	uidToken := strings.Split(dataExportUID, "-")
	return fmt.Sprintf("%s-%s", truncatedPVCName, uidToken[0])
}

func toBoundJobPVCName(pvcName string, pvcUID string) string {
	truncatedPVCName := pvcName
	if len(pvcName) > pvcNameLenLimitForJob {
		truncatedPVCName = pvcName[:pvcNameLenLimitForJob]
	}
	uidToken := strings.Split(pvcUID, "-")
	return fmt.Sprintf("%s-%s-%s", "bound", truncatedPVCName, uidToken[0])
}

func getRepoPVCName(de *kdmpapi.DataExport, pvcName string) string {
	if hasSnapshotStage(de) {
		subStrings := strings.Split(pvcName, "-")
		pvcName = strings.Join(subStrings[:len(subStrings)-1], "-")
	}
	return pvcName
}

func getAnnotationValue(de *kdmpapi.DataExport, key string) string {
	var val string
	if _, ok := de.Annotations[key]; ok {
		val = de.Annotations[key]
	}
	return val
}

func getVSFileName(backupUUID, timestamp string) string {
	return fmt.Sprintf("%s-%s.json", backupUUID, timestamp)
}

func getCSICRUploadDirectory(pvcUID string) string {
	return filepath.Join(volumeSnapShotCRDirectory, pvcUID)
}
