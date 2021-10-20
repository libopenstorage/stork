package dataexport

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"time"

	kSnapshotClient "github.com/kubernetes-csi/external-snapshotter/client/v4/clientset/versioned"
	storkapi "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/libopenstorage/stork/pkg/controllers"
	"github.com/libopenstorage/stork/pkg/snapshotter"
	kdmpapi "github.com/portworx/kdmp/pkg/apis/kdmp/v1alpha1"
	"github.com/portworx/kdmp/pkg/drivers"
	"github.com/portworx/kdmp/pkg/drivers/driversinstance"
	"github.com/portworx/kdmp/pkg/drivers/utils"
	"github.com/portworx/kdmp/pkg/snapshots"
	kdmpopts "github.com/portworx/kdmp/pkg/util/ops"
	"github.com/portworx/sched-ops/k8s/core"
	"github.com/portworx/sched-ops/k8s/stork"
	"github.com/portworx/sched-ops/task"
	"github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
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
	// pvcNameLenLimit is the max length of PVC name that DataExport related CRs
	// will incorporate in their names
	pvcNameLenLimit       = 247
	volumeinitialDelay    = 2 * time.Second
	volumeFactor          = 1.5
	volumeSteps           = 20
	defaultTimeout        = 1 * time.Minute
	progressCheckInterval = 5 * time.Second
)

var volumeAPICallBackoff = wait.Backoff{
	Duration: volumeinitialDelay,
	Factor:   volumeFactor,
	Steps:    volumeSteps,
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
			logrus.Warnf(msg)
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
		// If we have a failure case in this stage it means in the previous stage
		// all retries were done in cleaning up resources and failed.
		// In such a case we don't want to proceed further as stork will
		// eventually clean up DE CR.
		// Without this, we would again see the resource cleanup error and move
		// the status to DataExportStatusInProgress which is not desireable
		if dataExport.Status.Status == kdmpapi.DataExportStatusSuccessful ||
			dataExport.Status.Status == kdmpapi.DataExportStatusFailed {
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
		var volumeBackupCR *kdmpapi.VolumeBackup
		var vbErr error
		vbTask := func() (interface{}, bool, error) {
			volumeBackupCR, vbErr = kdmpopts.Instance().GetVolumeBackup(context.Background(),
				vbName, vbNamespace)
			if errors.IsNotFound(vbErr) {
				errMsg := fmt.Sprintf("volumebackup CR %v/%v not found", vbNamespace, vbName)
				logrus.Errorf("%v", errMsg)
				return "", false, fmt.Errorf(errMsg)
			}

			if vbErr != nil {
				errMsg := fmt.Sprintf("failed to read VolumeBackup CR %v: %v", vbName, err)
				logrus.Errorf("%v", errMsg)
				err := c.updateStatus(dataExport, kdmpapi.DataExportStatusInProgress, "")
				if err != nil {
					return "", false, fmt.Errorf("%v", err)
				}
				return "", true, fmt.Errorf("%v", errMsg)
			}
			return "", false, nil
		}
		if _, err := task.DoRetryWithTimeout(vbTask, defaultTimeout, progressCheckInterval); err != nil {
			errMsg := fmt.Sprintf("max retries done, failed to read VolumeBackup CR %v: %v", vbName, err)
			logrus.Errorf("%v", errMsg)
			// Exhausted all retries, fail the CR
			return false, c.updateStatus(dataExport, kdmpapi.DataExportStatusFailed, errMsg)
		}

		dataExport.Status.SnapshotID = volumeBackupCR.Status.SnapshotID
		dataExport.Status.Size = volumeBackupCR.Status.TotalBytes
		tlsTask := func() (interface{}, bool, error) {
			// Delete the tls certificate secret created
			err = core.Instance().DeleteSecret(drivers.CertSecretName, dataExport.Spec.Source.Namespace)
			if err != nil && !errors.IsNotFound(err) {
				errMsg := fmt.Sprintf("failed to delete [%s/%s] secret", dataExport.Spec.Source.Namespace, drivers.CertSecretName)
				logrus.Errorf("%v", errMsg)
				err := c.updateStatus(dataExport, kdmpapi.DataExportStatusInProgress, "")
				if err != nil {
					return "", false, fmt.Errorf("%v", err)
				}
				return "", true, fmt.Errorf("%v", errMsg)
			}
			return "", false, nil
		}
		if _, err := task.DoRetryWithTimeout(tlsTask, defaultTimeout, progressCheckInterval); err != nil {
			errMsg := fmt.Sprintf("max retries done, failed to delete [%s/%s] secret", dataExport.Spec.Source.Namespace, drivers.CertSecretName)
			logrus.Errorf("%v", errMsg)
			// Exhausted all retries, fail the CR
			return false, c.updateStatus(dataExport, kdmpapi.DataExportStatusFailed, errMsg)
		}

		cleanupTask := func() (interface{}, bool, error) {
			err := c.cleanUp(driver, dataExport)
			if err != nil {
				errMsg := fmt.Sprintf("failed to remove resources: %s", err)
				logrus.Errorf("%v", errMsg)
				err := c.updateStatus(dataExport, kdmpapi.DataExportStatusInProgress, "")
				if err != nil {
					return "", false, fmt.Errorf("%v", err)
				}
				return "", true, fmt.Errorf("%v", errMsg)
			}

			return "", false, nil
		}
		if _, err := task.DoRetryWithTimeout(cleanupTask, defaultTimeout, progressCheckInterval); err != nil {
			errMsg := fmt.Sprintf("max retries done, failed to delete [%s:%s] secret", dataExport.Spec.Source.Namespace, drivers.CertSecretName)
			logrus.Errorf("%v", errMsg)
			// Exhausted all retries, fail the CR
			return false, c.updateStatus(dataExport, kdmpapi.DataExportStatusFailed, errMsg)
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

	// First check if a snapshot has already been triggered
	snapName := toSnapName(dataExport.Spec.Source.Name, string(dataExport.UID))
	annotations := make(map[string]string)
	annotations[dataExportUIDAnnotation] = string(dataExport.UID)
	annotations[dataExportNameAnnotation] = trimLabel(dataExport.Name)
	name, namespace, _, err := snapshotDriver.CreateSnapshot(
		snapshotter.Name(snapName),
		snapshotter.PVCName(dataExport.Spec.Source.Name),
		snapshotter.PVCNamespace(dataExport.Spec.Source.Namespace),
		// TODO: restore namespace is required for external-storage snapshots
		//snapshots.RestoreNamespaces(dataExport.Spec.Destination.Namespace),
		snapshotter.SnapshotClassName(dataExport.Spec.SnapshotStorageClass),
		snapshotter.Annotations(annotations),
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
	if err != nil && !errors.IsNotFound(err) {
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
	if driver == nil {
		return fmt.Errorf("driver is nil")
	}

	if hasSnapshotStage(de) {
		snapshotDriverName, err := c.getSnapshotDriverName(de)
		if err != nil {
			return fmt.Errorf("failed to get snapshot driver name: %v", err)
		}

		snapshotDriver, err := c.snapshotter.Driver(snapshotDriverName)
		if err != nil {
			return fmt.Errorf("failed to get snapshot driver for %v: %v", snapshotDriverName, err)
		}

		if de.Status.SnapshotID != "" && de.Status.SnapshotNamespace != "" {

			snapName := toSnapName(de.Spec.Source.Name, string(de.UID))
			if err := snapshotDriver.DeleteSnapshot(snapName, de.Status.SnapshotNamespace, false); err != nil && !errors.IsNotFound(err) {
				return fmt.Errorf("delete %s/%s snapshot: %s", de.Status.SnapshotNamespace, de.Status.SnapshotID, err)
			}
		}

		if de.Status.SnapshotPVCName != "" && de.Status.SnapshotPVCNamespace != "" {
			if err := core.Instance().DeletePersistentVolumeClaim(de.Status.SnapshotPVCName, de.Status.SnapshotPVCNamespace); err != nil && !errors.IsNotFound(err) {
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
	t := func() (interface{}, bool, error) {
		err := c.client.Update(context.TODO(), setStatus(de, status, errMsg))
		if err != nil {
			errMsg := fmt.Sprintf("failed updating DE CR %s to status %v: %v", de.Name, status, err)
			logrus.Errorf("%v", errMsg)
			return "", true, fmt.Errorf("%v", errMsg)
		}

		return "", false, nil
	}
	if _, err := task.DoRetryWithTimeout(t, defaultTimeout, progressCheckInterval); err != nil {
		errMsg := fmt.Sprintf("max retries done, failed updating DE CR %s to status %v: %v", de.Name, status, err)
		logrus.Errorf("%v", errMsg)
		// Exhausted all retries, fail the CR
		return err
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
	if err != nil && errors.IsAlreadyExists(err) {
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
