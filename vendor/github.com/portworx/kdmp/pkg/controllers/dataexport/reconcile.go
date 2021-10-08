package dataexport

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	kSnapshotv1beta1 "github.com/kubernetes-csi/external-snapshotter/client/v4/apis/volumesnapshot/v1beta1"
	kSnapshotClient "github.com/kubernetes-csi/external-snapshotter/client/v4/clientset/versioned"
	storkapi "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/libopenstorage/stork/pkg/controllers"
	"github.com/libopenstorage/stork/pkg/objectstore"
	"github.com/libopenstorage/stork/pkg/snapshotter"
	kdmpapi "github.com/portworx/kdmp/pkg/apis/kdmp/v1alpha1"
	"github.com/portworx/kdmp/pkg/drivers"
	"github.com/portworx/kdmp/pkg/drivers/driversinstance"
	"github.com/portworx/kdmp/pkg/drivers/utils"
	"github.com/portworx/kdmp/pkg/snapshots"
	kdmpopts "github.com/portworx/kdmp/pkg/util/ops"
	"github.com/portworx/sched-ops/k8s/core"
	"github.com/portworx/sched-ops/k8s/stork"
	"github.com/sirupsen/logrus"
	"gocloud.dev/blob"
	corev1 "k8s.io/api/core/v1"
	k8sErrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/client-go/rest"
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

	pxbackupAnnotationPrefix        = "portworx.io/"
	kdmpAnnotationPrefix            = "kdmp.portworx.com/"
	pxbackupAnnotationCreateByKey   = pxbackupAnnotationPrefix + "created-by"
	pxbackupAnnotationCreateByValue = "px-backup"
	backupObjectUIDKey              = kdmpAnnotationPrefix + "backupobject-uid"
	pvcUIDKey                       = kdmpAnnotationPrefix + "pvc-uid"
	volumeSnapShotCRDirectory       = "csi-generic"
	localCSIRetention               = 1
	snapDeleteAnnotation            = "snapshotScheduledForDeletion"
	snapRestoreAnnotation           = "snapshotScheduledForRestore"

	// pvcNameLenLimit is the max length of PVC name that DataExport related CRs
	// will incorporate in their names
	pvcNameLenLimit    = 247
	volumeinitialDelay = 2 * time.Second
	volumeFactor       = 1.5
	volumeSteps        = 20
)

var volumeAPICallBackoff = wait.Backoff{
	Duration: volumeinitialDelay,
	Factor:   volumeFactor,
	Steps:    volumeSteps,
}

type csiBackupObject struct {
	VolumeSnapshots        map[string]*kSnapshotv1beta1.VolumeSnapshot        `json:"volumeSnapshots"`
	VolumeSnapshotContents map[string]*kSnapshotv1beta1.VolumeSnapshotContent `json:"volumeSnapshotContents"`
	VolumeSnapshotClasses  map[string]*kSnapshotv1beta1.VolumeSnapshotClass   `json:"volumeSnapshotClasses"`
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

		dataExport.Status.Stage = kdmpapi.DataExportStageInitial
		dataExport.Status.Status = kdmpapi.DataExportStatusInitial
		return true, c.client.Update(ctx, dataExport)
	}

	// delete an object on the init stage without cleanup
	if dataExport.DeletionTimestamp != nil && dataExport.Status.Stage == kdmpapi.DataExportStageInitial {
		if !controllers.ContainsFinalizer(dataExport, cleanupFinalizer) {
			return false, nil
		}

		if err := c.client.Delete(ctx, dataExport); err != nil {
			return true, fmt.Errorf("failed to delete dataexport object: %s", err)
		}

		controllers.RemoveFinalizer(dataExport, cleanupFinalizer)
		return true, c.client.Update(ctx, dataExport)
	}

	// TODO: validate DataExport resource & update status?
	driverName, err := getDriverType(dataExport)
	if err != nil {
		msg := fmt.Sprintf("failed to get a driver type for %s: %s", dataExport.Spec.Type, err)
		return false, c.client.Update(ctx, setStatus(dataExport, kdmpapi.DataExportStatusFailed, msg))
	}
	driver, err := driversinstance.Get(driverName)
	if err != nil {
		msg := fmt.Sprintf("failed to get a driver for a %s type: %s", dataExport.Spec.Type, err)
		return false, c.client.Update(ctx, setStatus(dataExport, kdmpapi.DataExportStatusFailed, msg))
	}

	if dataExport.DeletionTimestamp != nil {
		if !controllers.ContainsFinalizer(dataExport, cleanupFinalizer) {
			return false, nil
		}

		if err = c.cleanUp(driver, dataExport); err != nil {
			return true, fmt.Errorf("%s: cleanup: %s", reflect.TypeOf(dataExport), err)
		}

		controllers.RemoveFinalizer(dataExport, cleanupFinalizer)
		return true, c.client.Update(ctx, dataExport)
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
	case kdmpapi.DataExportStageTransferScheduled:
		if dataExport.Status.Status == kdmpapi.DataExportStatusSuccessful {
			// set to the next stage
			dataExport.Status.Stage = kdmpapi.DataExportStageTransferInProgress
			return true, c.client.Update(ctx, setStatus(dataExport, kdmpapi.DataExportStatusInitial, ""))
		}

		// use snapshot pvc in the dst namespace if it's available
		srcPVCName := dataExport.Spec.Source.Name
		if dataExport.Status.SnapshotPVCName != "" {
			srcPVCName = dataExport.Status.SnapshotPVCName
		}

		logrus.Debugf("drivername: %v", driverName)
		// Creating a secret for ssl enabled objectstore with custom certificates
		// User creates a secret in kube-system ns, mounts it to stork and recommendation is
		// secret data is 'public.crt' always which contains the certificate content
		// This secret path is provided as env section as follows
		// env:
		// - name: SSL_CERT_DIR
		//   value: "/etc/tls-secrets" (can be any path)

		// Check if the above env is present and read the certs file contents and
		// secret for the job pod for kopia to access the same
		err = createCertificateSecret(drivers.CertSecretName, dataExport.Spec.Source.Namespace, dataExport.Labels)
		if err != nil {
			return false, err
		}
		if driverName == drivers.KopiaBackup {
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
				return false, c.updateStatus(dataExport, kdmpapi.DataExportStatusFailed, msg)
			}

		}

		if driverName == drivers.KopiaRestore {
			// Get the volumebackup
			vb, err := kdmpopts.Instance().GetVolumeBackup(context.Background(),
				dataExport.Spec.Source.Name, dataExport.Spec.Source.Namespace)
			if err != nil {
				msg := fmt.Sprintf("Error accessing volumebackup %s in namespace %s : %v",
					dataExport.Spec.Source.Name, dataExport.Spec.Source.Namespace, err)
				logrus.Errorf(msg)
				return false, c.updateStatus(dataExport, kdmpapi.DataExportStatusFailed, msg)
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
				msg := fmt.Sprintf("failed to create cloud credential secret during kopia restore: %v", err)
				logrus.Errorf(msg)
				return false, c.updateStatus(dataExport, kdmpapi.DataExportStatusFailed, msg)
			}
			// For restore setting the source PVCName as the destination PVC name for the job
			srcPVCName = dataExport.Spec.Destination.Name
		}

		// start data transfer
		id, err := startTransferJob(driver, srcPVCName, dataExport)
		if err != nil {
			msg := fmt.Sprintf("failed to start a data transfer job, dataexport [%v]: %v", dataExport.Name, err)
			logrus.Error(msg)
			return false, c.updateStatus(dataExport, kdmpapi.DataExportStatusFailed, msg)
		}

		dataExport.Status.TransferID = id
		return true, c.client.Update(ctx, setStatus(dataExport, kdmpapi.DataExportStatusSuccessful, ""))
	case kdmpapi.DataExportStageTransferInProgress:
		if dataExport.Status.Status == kdmpapi.DataExportStatusSuccessful {
			// set to the next stage
			dataExport.Status.Stage = kdmpapi.DataExportStageFinal
			return true, c.client.Update(ctx, setStatus(dataExport, kdmpapi.DataExportStatusInitial, ""))
		}

		// get transfer job status
		progress, err := driver.JobStatus(dataExport.Status.TransferID)
		if err != nil {
			msg := fmt.Sprintf("failed to get %s job status: %s", dataExport.Status.TransferID, err)
			return false, c.updateStatus(dataExport, kdmpapi.DataExportStatusFailed, msg)
		}

		switch progress.State {
		case drivers.JobStateFailed:
			msg := fmt.Sprintf("%s transfer job failed: %s", dataExport.Status.TransferID, progress.Reason)
			return false, c.updateStatus(dataExport, kdmpapi.DataExportStatusFailed, msg)
		case drivers.JobStateCompleted:
			return false, c.updateStatus(dataExport, kdmpapi.DataExportStatusSuccessful, "")
		}

		dataExport.Status.ProgressPercentage = int(progress.ProgressPercents)
		return false, c.updateStatus(dataExport, kdmpapi.DataExportStatusInProgress, "")
	case kdmpapi.DataExportStageFinal:
		if dataExport.Status.Status == kdmpapi.DataExportStatusSuccessful {
			return false, nil
		}
		var vbName string
		var vbNamespace string
		if driverName == drivers.KopiaBackup {
			vbNamespace, vbName, err = utils.ParseJobID(dataExport.Status.TransferID)
			if err != nil {
				errMsg := fmt.Sprintf("failed to parse job ID %v from DataExport CR: %v: %v",
					dataExport.Status.TransferID, dataExport.Name, err)
				return false, c.updateStatus(dataExport, kdmpapi.DataExportStatusFailed, errMsg)
			}
		}

		if driverName == drivers.KopiaRestore {
			vbName = dataExport.Spec.Source.Name
			vbNamespace = dataExport.Spec.Source.Namespace
		}

		volumeBackupCR, err := kdmpopts.Instance().GetVolumeBackup(context.Background(),
			vbName, vbNamespace)
		if err != nil {
			errMsg := fmt.Sprintf("failed to read VolumeBackup CR %v: %v", vbName, err)
			return false, c.updateStatus(dataExport, kdmpapi.DataExportStatusFailed, errMsg)
		}
		dataExport.Status.SnapshotID = volumeBackupCR.Status.SnapshotID
		dataExport.Status.Size = volumeBackupCR.Status.TotalBytes
		// Delete the tls certificate secret created
		err = core.Instance().DeleteSecret(drivers.CertSecretName, dataExport.Spec.Source.Namespace)
		if err != nil && k8sErrors.IsAlreadyExists(err) {
			errMsg := fmt.Sprintf("failed to delete [%s:%s] secret", dataExport.Spec.Source.Namespace, drivers.CertSecretName)
			logrus.Errorf("%v", errMsg)
			return false, c.updateStatus(dataExport, kdmpapi.DataExportStatusFailed, errMsg)
		}

		if err := c.cleanUp(driver, dataExport); err != nil {
			msg := fmt.Sprintf("failed to remove resources: %s", err)
			return false, c.updateStatus(dataExport, kdmpapi.DataExportStatusFailed, msg)
		}

		return true, c.client.Update(ctx, setStatus(dataExport, kdmpapi.DataExportStatusSuccessful, ""))
	}

	return false, nil
}

func (c *Controller) stageInitial(ctx context.Context, dataExport *kdmpapi.DataExport) (bool, error) {
	if dataExport.Status.Status == kdmpapi.DataExportStatusSuccessful {
		// set to the next stage
		dataExport.Status.Stage = kdmpapi.DataExportStageTransferScheduled
		if hasSnapshotStage(dataExport) {
			dataExport.Status.Stage = kdmpapi.DataExportStageSnapshotScheduled
			csiclient, err := c.getCSIClient()
			if err != nil {
				msg := fmt.Sprintf("could not get csi client: %s", err)
				return false, c.updateStatus(dataExport, kdmpapi.DataExportStatusFailed, msg)
			}
			c.csiclient = csiclient
		}
		return true, c.client.Update(ctx, setStatus(dataExport, kdmpapi.DataExportStatusInitial, ""))
	}

	driverName, err := getDriverType(dataExport)
	if err != nil {
		msg := fmt.Sprintf("check failed: %s", err)
		return false, c.updateStatus(dataExport, kdmpapi.DataExportStatusFailed, msg)
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
		return false, c.updateStatus(dataExport, kdmpapi.DataExportStatusFailed, msg)
	}

	return true, c.client.Update(ctx, setStatus(dataExport, kdmpapi.DataExportStatusSuccessful, ""))
}

func (c *Controller) stageSnapshotScheduled(ctx context.Context, dataExport *kdmpapi.DataExport) (bool, error) {
	if dataExport.Status.Status == kdmpapi.DataExportStatusSuccessful {
		// set to the next stage
		dataExport.Status.Stage = kdmpapi.DataExportStageSnapshotInProgress
		return true, c.client.Update(ctx, setStatus(dataExport, kdmpapi.DataExportStatusInitial, ""))
	}

	snapshotDriverName, err := c.getSnapshotDriverName(dataExport)
	if err != nil {
		return false, fmt.Errorf("failed to get snapshot driver name: %v", err)
	}

	snapshotDriver, err := c.snapshotter.Driver(snapshotDriverName)
	if err != nil {
		return false, fmt.Errorf("failed to get snapshot driver for %v: %v", snapshotDriverName, err)
	}

	backupUID := getAnnotationValue(dataExport, backupObjectUIDKey)
	pvcUID := getAnnotationValue(dataExport, pvcUIDKey)
	// First check if a snapshot has already been triggered
	snapName := toSnapName(dataExport.Spec.Source.Name, string(dataExport.UID))
	annotations := make(map[string]string)
	annotations[dataExportUIDAnnotation] = string(dataExport.UID)
	annotations[dataExportNameAnnotation] = trimLabel(dataExport.Name)
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
		return false, c.updateStatus(dataExport, kdmpapi.DataExportStatusFailed, msg)
	}

	dataExport.Status.SnapshotID = name
	dataExport.Status.SnapshotNamespace = namespace
	return true, c.client.Update(ctx, setStatus(dataExport, kdmpapi.DataExportStatusSuccessful, ""))
}

func (c *Controller) getSnapshotDriverName(dataExport *kdmpapi.DataExport) (string, error) {
	if len(dataExport.Spec.SnapshotStorageClass) == 0 {
		return "", fmt.Errorf("snapshot storage class not provided")
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
	_, err = cs.SnapshotV1beta1().VolumeSnapshotClasses().Get(context.TODO(), dataExport.Spec.SnapshotStorageClass, metav1.GetOptions{})
	if err == nil {
		return csiProvider, nil
	}
	if err != nil && !k8sErrors.IsNotFound(err) {
		return "", nil
	}
	// Default to external-storage snapshot class
	return snapshots.ExternalStorage, nil
}

func (c *Controller) stageSnapshotInProgress(ctx context.Context, dataExport *kdmpapi.DataExport) (bool, error) {
	if dataExport.Status.Status == kdmpapi.DataExportStatusSuccessful {
		// set to the next stage
		dataExport.Status.Stage = kdmpapi.DataExportStageSnapshotRestore
		return true, c.client.Update(ctx, setStatus(dataExport, kdmpapi.DataExportStatusInitial, ""))
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
		return false, c.updateStatus(dataExport, kdmpapi.DataExportStatusFailed, msg)
	}

	if snapInfo.Status == snapshotter.StatusFailed {
		return false, c.updateStatus(dataExport, kdmpapi.DataExportStatusFailed, snapInfo.Reason)
	}

	if snapInfo.Status != snapshotter.StatusReady {
		return false, c.updateStatus(dataExport, kdmpapi.DataExportStatusInProgress, snapInfo.Reason)
	}
	// upload the CRs to the objectstore
	var bl *storkapi.BackupLocation
	if bl, err = checkBackupLocation(dataExport.Spec.Destination); err != nil {
		return false, c.updateStatus(dataExport, kdmpapi.DataExportStatusFailed, fmt.Sprintf("backuplocation fetch error: %v", err))
	}

	backupUID := getAnnotationValue(dataExport, backupObjectUIDKey)
	if err != nil {
		return false, c.updateStatus(dataExport, kdmpapi.DataExportStatusFailed, fmt.Sprintf("backup UID annotation is not set in dataexport cr: %v", err))
	}

	pvcUID := getAnnotationValue(dataExport, pvcUIDKey)
	if err != nil {
		return false, c.updateStatus(dataExport, kdmpapi.DataExportStatusFailed, fmt.Sprintf("pvc UID annotation is not set in dataexport cr: %v", err))
	}

	snapName := toSnapName(dataExport.Spec.Source.Name, string(dataExport.UID))
	vs, err := c.getVolumeSnapshot(snapName, dataExport.Spec.Source.Namespace)
	if err != nil {
		return false, c.updateStatus(dataExport, kdmpapi.DataExportStatusFailed, fmt.Sprintf("failed to get the volumesnapshot for %s: %v", snapName, err))
	}
	timestampEpoch := strconv.FormatInt(vs.GetObjectMeta().GetCreationTimestamp().Unix(), 10)
	err = snapshotDriver.UploadSnapshotData(bl, snapInfo, snapName, getCSICRUploadDirectory(pvcUID), getVSFileName(backupUID, timestampEpoch))
	if err != nil {
		return false, c.updateStatus(dataExport, kdmpapi.DataExportStatusFailed, fmt.Sprintf("uploading snapshot CRs error: %v", err))
	}

	return true, c.client.Update(ctx, setStatus(dataExport, kdmpapi.DataExportStatusSuccessful, snapInfo.Reason))
}

func (c *Controller) stageSnapshotRestore(ctx context.Context, dataExport *kdmpapi.DataExport) (bool, error) {
	if dataExport.Status.Status == kdmpapi.DataExportStatusSuccessful {
		// set to the next stage
		dataExport.Status.Stage = kdmpapi.DataExportStageSnapshotRestoreInProgress
		return true, c.client.Update(ctx, setStatus(dataExport, kdmpapi.DataExportStatusInitial, ""))
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
		return false, c.updateStatus(dataExport, kdmpapi.DataExportStatusFailed, msg)
	}

	dataExport.Status.SnapshotPVCName = pvc.Name
	dataExport.Status.SnapshotPVCNamespace = pvc.Namespace
	return true, c.updateStatus(dataExport, kdmpapi.DataExportStatusSuccessful, "")
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
		dataExport.Status.Stage = kdmpapi.DataExportStageTransferScheduled
		return true, c.client.Update(ctx, setStatus(dataExport, kdmpapi.DataExportStatusInitial, ""))
	}

	src := dataExport.Spec.Source
	srcPvc, err := core.Instance().GetPersistentVolumeClaim(src.Name, src.Namespace)
	if err != nil {
		msg := fmt.Sprintf("failed to get restore pvc %v: %v", src.Name, err)
		return false, c.updateStatus(dataExport, kdmpapi.DataExportStatusFailed, msg)
	}

	restoreInfo, err := snapshotDriver.RestoreStatus(toSnapshotPVCName(srcPvc.Name, string(dataExport.UID)), srcPvc.Namespace)
	if err != nil {
		msg := fmt.Sprintf("failed to get a snapshot restore status: %s", err)
		return false, c.updateStatus(dataExport, kdmpapi.DataExportStatusFailed, msg)
	}

	if restoreInfo.Status == snapshotter.StatusFailed {
		return false, c.updateStatus(dataExport, kdmpapi.DataExportStatusFailed, restoreInfo.Reason)
	}

	if restoreInfo.Status != snapshotter.StatusReady {
		return false, c.updateStatus(dataExport, kdmpapi.DataExportStatusInProgress, restoreInfo.Reason)
	}

	return true, c.client.Update(ctx, setStatus(dataExport, kdmpapi.DataExportStatusSuccessful, restoreInfo.Reason))

}

func (c *Controller) cleanUp(driver drivers.Interface, de *kdmpapi.DataExport) error {
	var bl *storkapi.BackupLocation
	var err error

	if driver == nil {
		return fmt.Errorf("driver is nil")
	}

	if hasSnapshotStage(de) {
		if de.Status.SnapshotPVCName != "" && de.Status.SnapshotPVCNamespace != "" {
			pvcUID := getAnnotationValue(de, pvcUIDKey)
			if bl, err = checkBackupLocation(de.Spec.Destination); err != nil {
				return fmt.Errorf("backuplocation fetch error: %v", err)
			}

			vsCRList, err := c.getCSISnapshotsCRListForDelete(bl, pvcUID)
			if err != nil {
				return fmt.Errorf("failed in getting list of older volumesnapshot CRs from objectstore : %v", err)
			}

			err = c.retainLocalCSISnapshots(driver, de, pvcUID, vsCRList, false)
			if err != nil {
				return fmt.Errorf("failed in removing older local csi snapshots for pvc ID %s: %v", pvcUID, err)
			}
		}

		if de.Status.SnapshotPVCName != "" && de.Status.SnapshotPVCNamespace != "" {
			if err := core.Instance().DeletePersistentVolumeClaim(de.Status.SnapshotPVCName, de.Status.SnapshotPVCNamespace); err != nil && !k8sErrors.IsNotFound(err) {
				return fmt.Errorf("delete %s/%s pvc: %s", de.Status.SnapshotPVCNamespace, de.Status.SnapshotPVCName, err)
			}
		}
	}

	if de.Status.TransferID != "" {
		if err := driver.DeleteJob(de.Status.TransferID); err != nil {
			return fmt.Errorf("delete %s job: %s", de.Status.TransferID, err)
		}
	}

	return nil
}

func (c *Controller) updateStatus(de *kdmpapi.DataExport, status kdmpapi.DataExportStatus, errMsg string) error {
	if isStatusEqual(de, status, errMsg) {
		return nil
	}
	return c.client.Update(context.TODO(), setStatus(de, status, errMsg))
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
	pvc.Annotations[dataExportNameAnnotation] = trimLabel(de.Name)

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
	if _, err := checkPVC(de.Spec.Destination, true); err != nil {
		return fmt.Errorf("destination: %s", err)
	}

	return nil
}

func startTransferJob(drv drivers.Interface, srcPVCName string, dataExport *kdmpapi.DataExport) (string, error) {
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
			drivers.WithSourcePVC(srcPVCName),
			drivers.WithNamespace(dataExport.Spec.Source.Namespace),
			drivers.WithBackupLocationName(dataExport.Spec.Destination.Name),
			drivers.WithBackupLocationNamespace(dataExport.Spec.Destination.Namespace),
			drivers.WithLabels(dataExport.Labels),
			drivers.WithDataExportName(dataExport.GetName()),
			drivers.WithCertSecretName(drivers.CertSecretName),
			drivers.WithCertSecretNamespace(dataExport.Spec.Source.Namespace),
		)
	case drivers.KopiaRestore:
		return drv.StartJob(
			drivers.WithDestinationPVC(dataExport.Spec.Destination.Name),
			drivers.WithNamespace(dataExport.Spec.Destination.Namespace),
			drivers.WithVolumeBackupName(dataExport.Spec.Source.Name),
			drivers.WithVolumeBackupNamespace(dataExport.Spec.Source.Namespace),
			drivers.WithBackupLocationNamespace(dataExport.Spec.Source.Namespace),
			drivers.WithLabels(dataExport.Labels),
			drivers.WithDataExportName(dataExport.GetName()),
			drivers.WithCertSecretName(drivers.CertSecretName),
			drivers.WithCertSecretNamespace(dataExport.Spec.Destination.Namespace),
		)
	}

	return "", fmt.Errorf("unknown data transfer driver: %s", drv.Name())
}

func checkPVC(in kdmpapi.DataExportObjectReference, checkMounts bool) (*corev1.PersistentVolumeClaim, error) {
	if err := checkNameNamespace(in); err != nil {
		return nil, err
	}
	// wait for pvc to get bound
	var pvc *corev1.PersistentVolumeClaim
	var err error
	wErr := wait.ExponentialBackoff(volumeAPICallBackoff, func() (bool, error) {
		pvc, err = core.Instance().GetPersistentVolumeClaim(in.Name, in.Namespace)
		if err != nil {
			return false, err
		}

		if pvc.Status.Phase != corev1.ClaimBound {
			errMsg := fmt.Sprintf("status: expected %s, got %s", corev1.ClaimBound, pvc.Status.Phase)
			logrus.Debugf("%v", errMsg)
			return false, nil
		}

		return true, nil
	})

	if wErr != nil {
		logrus.Errorf("%v", err)
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

func setStatus(de *kdmpapi.DataExport, status kdmpapi.DataExportStatus, reason string) *kdmpapi.DataExport {
	de.Status.Status = status
	de.Status.Reason = reason

	return de
}

func isStatusEqual(de *kdmpapi.DataExport, status kdmpapi.DataExportStatus, reason string) bool {
	return de.Status.Status == status && de.Status.Reason == reason
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
	err := createJobSecret(secretName, namespace, credentialData, labels)

	return err
}

func createGoogleSecret(secretName string, backupLocation *storkapi.BackupLocation, namespace string, labels map[string]string) error {
	credentialData := make(map[string][]byte)
	credentialData["type"] = []byte(backupLocation.Location.Type)
	credentialData["password"] = []byte(backupLocation.Location.RepositoryPassword)
	credentialData["accountkey"] = []byte(backupLocation.Location.GoogleConfig.AccountKey)
	credentialData["projectid"] = []byte(backupLocation.Location.GoogleConfig.ProjectID)
	credentialData["path"] = []byte(backupLocation.Location.Path)
	err := createJobSecret(secretName, namespace, credentialData, labels)

	return err
}

func createAzureSecret(secretName string, backupLocation *storkapi.BackupLocation, namespace string, labels map[string]string) error {
	credentialData := make(map[string][]byte)
	credentialData["type"] = []byte(backupLocation.Location.Type)
	credentialData["password"] = []byte(backupLocation.Location.RepositoryPassword)
	credentialData["path"] = []byte(backupLocation.Location.Path)
	credentialData["storageaccountname"] = []byte(backupLocation.Location.AzureConfig.StorageAccountName)
	credentialData["storageaccountkey"] = []byte(backupLocation.Location.AzureConfig.StorageAccountKey)
	err := createJobSecret(secretName, namespace, credentialData, labels)

	return err
}

func createCertificateSecret(secretName, namespace string, labels map[string]string) error {
	drivers.CertFilePath = os.Getenv(drivers.CertDirPath)
	if drivers.CertFilePath != "" {
		certificateData, err := ioutil.ReadFile(filepath.Join(drivers.CertFilePath, drivers.CertFileName))
		if err != nil {
			errMsg := fmt.Sprintf("failed reading data from file %s : %s", drivers.CertFilePath, err)
			logrus.Errorf("%v", errMsg)
			return fmt.Errorf(errMsg)
		}

		certData := make(map[string][]byte)
		certData[drivers.CertFileName] = certificateData
		err = createJobSecret(secretName, namespace, certData, labels)

		return err
	}

	return nil
}

func createJobSecret(
	secretName string,
	namespace string,
	credentialData map[string][]byte,
	labels map[string]string,
) error {
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      secretName,
			Namespace: namespace,
			Labels:    labels,
			Annotations: map[string]string{
				utils.SkipResourceAnnotation: "true",
			},
		},
		Data: credentialData,
		Type: corev1.SecretTypeOpaque,
	}
	_, err := core.Instance().CreateSecret(secret)
	if err != nil && k8sErrors.IsAlreadyExists(err) {
		return nil
	}

	return err
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

func trimLabel(label string) string {
	if len(label) > 63 {
		return label[:63]
	}
	return label
}

func getAnnotationValue(de *kdmpapi.DataExport, key string) string {
	var val string
	if _, ok := de.Annotations[key]; ok {
		val = de.Annotations[key]
	}
	return val
}

func (c *Controller) getVolumeSnapshot(snapName, namespace string) (*kSnapshotv1beta1.VolumeSnapshot, error) {
	return c.csiclient.SnapshotV1beta1().VolumeSnapshots(namespace).Get(context.TODO(), snapName, metav1.GetOptions{})
}

func (c *Controller) getVolumeSnapshotClass(snapshotClassName string) (*kSnapshotv1beta1.VolumeSnapshotClass, error) {
	return c.csiclient.SnapshotV1beta1().VolumeSnapshotClasses().Get(context.TODO(), snapshotClassName, metav1.GetOptions{})
}

func (c *Controller) createVolumeSnapshotClass(snapshotClassName, driverName string) (*kSnapshotv1beta1.VolumeSnapshotClass, error) {
	return c.csiclient.SnapshotV1beta1().VolumeSnapshotClasses().Create(context.TODO(), &kSnapshotv1beta1.VolumeSnapshotClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: snapshotClassName,
		},
		Driver:         driverName,
		DeletionPolicy: kSnapshotv1beta1.VolumeSnapshotContentRetain,
	}, metav1.CreateOptions{})
}

func getVSFileName(backupUUID, timestamp string) string {
	return fmt.Sprintf("%s-%s.json", backupUUID, timestamp)
}

func getCSICRUploadDirectory(pvcUID string) string {
	return filepath.Join(volumeSnapShotCRDirectory, pvcUID)
}

func getBackupInfoFromObjectKey(objKey string) (string, string) {
	var backupUID, timestamp string

	keySplits := strings.Split(objKey, "/")
	fileName := keySplits[len(keySplits)-1]
	fileSplits := strings.Split(fileName, "-")
	backupUID = strings.Join(fileSplits[0:len(fileSplits)-1], "-")
	timestamp = strings.Split(fileSplits[len(fileSplits)-1], ".")[0]

	return backupUID, timestamp
}

func (cbo *csiBackupObject) GetVolumeSnapshotName() string {
	var snapName string
	// As there will be always one snapshot in this csiBackupObject
	for key := range cbo.VolumeSnapshots {
		snapName = key
		break
	}
	return snapName
}

//  GetVolumeSnapshotContent retrieves a backed up volume snapshot
func (cbo *csiBackupObject) GetVolumeSnapshot(snapshotID string) (*kSnapshotv1beta1.VolumeSnapshot, error) {
	vs, ok := cbo.VolumeSnapshots[snapshotID]
	if !ok {
		return nil, fmt.Errorf("failed to retrieve volume snapshot for snapshotID %s", snapshotID)
	}
	return vs, nil
}

// GetVolumeSnapshotContent retrieves a backed up volume snapshot content
func (cbo *csiBackupObject) GetVolumeSnapshotContent(snapshotID string) (*kSnapshotv1beta1.VolumeSnapshotContent, error) {

	vsc, ok := cbo.VolumeSnapshotContents[snapshotID]
	if !ok {
		return nil, fmt.Errorf("failed to retrieve volume snapshot content for snapshotID %s", snapshotID)
	}
	return vsc, nil
}

// GetVolumeSnapshotClass retrieves a backed up volume snapshot class
func (cbo *csiBackupObject) GetVolumeSnapshotClass(snapshotID string) (*kSnapshotv1beta1.VolumeSnapshotClass, error) {
	vs, ok := cbo.VolumeSnapshots[snapshotID]
	if !ok {
		return nil, fmt.Errorf("failed to retrieve volume snapshot for snapshotID %s", snapshotID)
	}

	if vs.Spec.VolumeSnapshotClassName == nil {
		return nil, fmt.Errorf("failed to retrieve volume snapshot class for snapshot %s. Volume snapshot class is undefined", snapshotID)
	}
	vsClassName := *vs.Spec.VolumeSnapshotClassName

	vsClass, ok := cbo.VolumeSnapshotClasses[vsClassName]
	if !ok {
		return nil, fmt.Errorf("failed to retrieve volume snapshot class for snapshotID %s", snapshotID)
	}

	return vsClass, nil
}

func (c *Controller) restoreVolumeSnapshotClass(vsClass *kSnapshotv1beta1.VolumeSnapshotClass) (*kSnapshotv1beta1.VolumeSnapshotClass, error) {
	vsClass.ResourceVersion = ""
	vsClass.UID = ""
	newVSClass, err := c.csiclient.SnapshotV1beta1().VolumeSnapshotClasses().Create(context.TODO(), vsClass, metav1.CreateOptions{})
	if err != nil {
		if k8sErrors.IsAlreadyExists(err) {
			return vsClass, nil
		}
		return nil, err
	}

	return newVSClass, nil
}

func (c *Controller) restoreVolumeSnapshot(
	namespace string,
	vs *kSnapshotv1beta1.VolumeSnapshot,
	vsc *kSnapshotv1beta1.VolumeSnapshotContent,
) (*kSnapshotv1beta1.VolumeSnapshot, error) {
	vs.ResourceVersion = ""
	vs.Spec.Source.PersistentVolumeClaimName = nil
	vs.Spec.Source.VolumeSnapshotContentName = &vsc.Name
	// The following label is set so that this snapshot does not get picked for restore
	vs.Annotations[snapDeleteAnnotation] = "true"
	vs.Namespace = namespace
	vs, err := c.csiclient.SnapshotV1beta1().VolumeSnapshots(namespace).Create(context.TODO(), vs, metav1.CreateOptions{})
	if err != nil {
		if k8sErrors.IsAlreadyExists(err) {
			// Check if the snapshot is scheduled for restore
			if _, ok := vs.Annotations[snapRestoreAnnotation]; ok {
				if vs.Annotations[snapRestoreAnnotation] == "true" {
					logrus.Infof("volumesnapshot %s is set for restore, hence not deleting it", vs.Name)
					return vs, fmt.Errorf("")
				}
			}
			return vs, nil
		}
		return nil, err
	}

	return vs, nil
}

func (c *Controller) restoreVolumeSnapshotContent(
	namespace string,
	vs *kSnapshotv1beta1.VolumeSnapshot,
	vsc *kSnapshotv1beta1.VolumeSnapshotContent,
) (*kSnapshotv1beta1.VolumeSnapshotContent, error) {
	snapshotHandle := *vsc.Status.SnapshotHandle
	vsc.ResourceVersion = ""
	vsc.Spec.Source.VolumeHandle = nil
	vsc.Spec.Source.SnapshotHandle = &snapshotHandle
	vsc.Spec.VolumeSnapshotRef.Name = vs.Name
	vsc.Spec.VolumeSnapshotRef.Namespace = namespace
	vsc.Spec.VolumeSnapshotRef.UID = vs.UID
	vsc.Spec.DeletionPolicy = kSnapshotv1beta1.VolumeSnapshotContentRetain
	vsc, err := c.csiclient.SnapshotV1beta1().VolumeSnapshotContents().Create(context.TODO(), vsc, metav1.CreateOptions{})
	if err != nil {
		if k8sErrors.IsAlreadyExists(err) {
			return vsc, nil
		}
		return nil, err
	}

	return vsc, nil
}

func (c *Controller) getCSISnapshotsCRListForDelete(backupLocation *storkapi.BackupLocation, pvcUID string) ([]string, error) {
	var vsList []string
	var timestamps []string
	timestampBackupMapping := make(map[string]string)

	bucket, err := objectstore.GetBucket(backupLocation)
	if err != nil {
		return vsList, err
	}
	iterator := bucket.List(&blob.ListOptions{
		Prefix: fmt.Sprintf("%s/", getCSICRUploadDirectory(pvcUID)),
	})

	for {
		object, err := iterator.Next(context.TODO())
		if err == io.EOF {
			break
		}
		if err != nil {
			return vsList, err
		}
		if object.IsDir {
			continue
		}

		backupUID, timestamp := getBackupInfoFromObjectKey(object.Key)
		logrus.Infof("backupUID: %s, timestamp: %s", backupUID, timestamp)
		timestamps = append(timestamps, timestamp)
		timestampBackupMapping[timestamp] = object.Key
	}

	sort.Strings(timestamps)
	if len(timestamps) > localCSIRetention {
		for _, timestamp := range timestamps[:len(timestamps)-localCSIRetention] {
			vsList = append(vsList, timestampBackupMapping[timestamp])
		}
	}
	return vsList, nil
}

func (c *Controller) deleteCSILocalSnapshot(driver drivers.Interface, de *kdmpapi.DataExport, snapName string) error {
	logrus.Infof("in deleteCSILocalSnapshot for snapName %s", snapName)
	snapshotDriverName, err := c.getSnapshotDriverName(de)
	if err != nil {
		return fmt.Errorf("failed to get snapshot driver name: %v", err)
	}

	snapshotDriver, err := c.snapshotter.Driver(snapshotDriverName)
	if err != nil {
		return fmt.Errorf("failed to get snapshot driver for %v: %v", snapshotDriverName, err)
	}

	if err := snapshotDriver.DeleteSnapshot(snapName, de.Status.SnapshotNamespace, false); err != nil && !k8sErrors.IsNotFound(err) {
		return fmt.Errorf("delete %s/%s snapshot: %s", de.Status.SnapshotNamespace, de.Status.SnapshotID, err)
	}
	return nil
}

func (c *Controller) retainLocalCSISnapshots(
	driver drivers.Interface,
	de *kdmpapi.DataExport,
	pvcUID string,
	snapShotCRList []string,
	retainSnapshotContent bool,
) error {
	logrus.Infof("in retainLocalCSISnapshots")
	namespace := de.Spec.Source.Namespace

	snapshotDriverName, err := c.getSnapshotDriverName(de)
	if err != nil {
		return fmt.Errorf("failed to get snapshot driver name: %v", err)
	}

	snapshotDriver, err := c.snapshotter.Driver(snapshotDriverName)
	if err != nil {
		return fmt.Errorf("failed to get snapshot driver for %v: %v", snapshotDriverName, err)
	}

	var bl *storkapi.BackupLocation
	if bl, err = checkBackupLocation(de.Spec.Destination); err != nil {
		return fmt.Errorf("backuplocation fetch error: %v", err)
	}

	for _, snapshotCR := range snapShotCRList {
		logrus.Infof("snapshotCR: %s", snapshotCR)
		// Call the snapshot delete here
		backupObjectBytes, err := snapshotDriver.DownloadObject(bl, snapshotCR)
		if err != nil {
			return err
		}

		cbo := &csiBackupObject{}
		logrus.Infof("backupObjectBytes length: %d", len(backupObjectBytes))
		err = json.Unmarshal(backupObjectBytes, cbo)
		if err != nil {
			return err
		}

		snapName := cbo.GetVolumeSnapshotName()
		if snapName == "" {
			return fmt.Errorf("could not find any snapshot in the downloaded csibackupobject for %s", snapshotCR)
		}

		err = c.recreateSnapshotForDeletion(driver, de, pvcUID, snapName, cbo)
		if err != nil {
			return err
		}
		logrus.Debugf("successfully recreated snapshot resources for snapshot %s", snapName)

		vs, err := c.getVolumeSnapshot(snapName, namespace)
		if err != nil {
			return fmt.Errorf("failed to find Snapshot %s: %s", snapName, err.Error())
		}

		err = snapshotDriver.DeleteSnapshot(
			vs.Name,
			namespace,
			retainSnapshotContent, // retain snapshot content
		)
		if err != nil {
			return err
		}
		logrus.Debugf("successfully deleted snapshot %s", snapName)

		// Delete Local snapshot CRs
		err = c.deleteCSILocalSnapshot(driver, de, snapName)
		if err != nil {
			logrus.Errorf("deleting local snapshots after restore failed for snapshot %s: %v", snapName, err)
		}
		logrus.Debugf("successfully deleted the recreated snapshot resources for snapshot %s", snapName)

		// Delete the CRs from objectstore
		err = snapshotDriver.DeleteCloudObject(bl, snapshotCR)
		if err != nil {
			logrus.Errorf("deletinging the CRs from objectstore failed for snapshot %s: %v", snapName, err)
		}
		logrus.Debugf("successfully deleted snapshot resources in objectstore for snapshot %s", snapName)
	}

	return nil
}

func (c *Controller) recreateSnapshotForDeletion(
	driver drivers.Interface,
	de *kdmpapi.DataExport,
	pvcUID string,
	snapshotName string,
	csiBackupObject *csiBackupObject,
) error {
	var err error
	namespace := de.Spec.Source.Namespace
	snapshotDriverName, err := c.getSnapshotDriverName(de)
	if err != nil {
		return fmt.Errorf("failed to get snapshot driver name: %v", err)
	}

	snapshotClassName := de.Spec.SnapshotStorageClass
	logrus.Debugf("snapshotClassName: %v", snapshotClassName)

	// make sure snapshot class is created for this object.
	// if we have already created it in this batch, do not check if created already.
	err = c.ensureVolumeSnapshotClassCreated(snapshotDriverName, snapshotClassName)
	if err != nil {
		return err
	}

	// Get VSC and VS
	vsc, err := csiBackupObject.GetVolumeSnapshotContent(snapshotName)
	if err != nil {
		return err
	}
	vs, err := csiBackupObject.GetVolumeSnapshot(snapshotName)
	if err != nil {
		return err
	}
	vsClass, err := c.getVolumeSnapshotClass(snapshotClassName)
	if err != nil {
		return err
	}
	logrus.Debugf("vsClass: %v", vsClass)

	// Create vsClass
	_, err = c.restoreVolumeSnapshotClass(vsClass)
	if err != nil {
		return fmt.Errorf("failed to restore VolumeSnapshotClass for deletion: %s", err.Error())
	}
	logrus.Debugf("created volume snapshot class %s for backup %s deletion", vs.Name, snapshotName)

	// Create VS, bound to VSC
	vs, err = c.restoreVolumeSnapshot(namespace, vs, vsc)
	if err != nil {
		return fmt.Errorf("failed to restore VolumeSnapshot for deletion: %s", err.Error())
	}
	logrus.Debugf("created volume snapshot %s for backup %s deletion", vs.Name, snapshotName)

	// Create VSC
	vsc.Spec.DeletionPolicy = kSnapshotv1beta1.VolumeSnapshotContentDelete
	_, err = c.restoreVolumeSnapshotContent(namespace, vs, vsc)
	if err != nil {
		return err
	}
	logrus.Debugf("created volume snapshot content %s for backup %s deletion", vsc.Name, snapshotName)

	return nil
}

func (c *Controller) getCSIClient() (*kSnapshotClient.Clientset, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, err
	}

	snapClient, err := kSnapshotClient.NewForConfig(config)
	if err != nil {
		return nil, err
	}
	return snapClient, nil
}

func (c *Controller) ensureVolumeSnapshotClassCreated(csiDriverName, snapshotClassName string) error {
	vsClass, err := c.getVolumeSnapshotClass(snapshotClassName)
	if k8sErrors.IsNotFound(err) {
		_, err = c.createVolumeSnapshotClass(snapshotClassName, csiDriverName)
		if err != nil {
			return err
		}
		logrus.Debugf("volumesnapshotclass created: %v", snapshotClassName)
	} else if err != nil {
		return err
	}

	// If we've found a vsClass, but it doesn't have a RetainPolicy, update to Retain.
	// This is essential for the storage backend to not delete the snapshot.
	// Some CSI drivers require specific VolumeSnapshotClass parameters, so we will leave those as is.
	if vsClass.DeletionPolicy == kSnapshotv1beta1.VolumeSnapshotContentDelete {
		vsClass.DeletionPolicy = kSnapshotv1beta1.VolumeSnapshotContentRetain
		_, err = c.csiclient.SnapshotV1beta1().VolumeSnapshotClasses().Update(context.TODO(), vsClass, metav1.UpdateOptions{})
		if err != nil {
			return err
		}
	}

	return nil
}
