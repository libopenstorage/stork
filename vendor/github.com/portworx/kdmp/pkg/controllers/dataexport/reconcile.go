package dataexport

import (
	"context"
	"fmt"
	"os"
	"reflect"

	storkapi "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/libopenstorage/stork/pkg/controllers"
	kdmpapi "github.com/portworx/kdmp/pkg/apis/kdmp/v1alpha1"
	"github.com/portworx/kdmp/pkg/drivers"
	"github.com/portworx/kdmp/pkg/drivers/driversinstance"
	"github.com/portworx/kdmp/pkg/drivers/kopiabackup"
	"github.com/portworx/kdmp/pkg/drivers/utils"
	"github.com/portworx/kdmp/pkg/snapshots"
	"github.com/portworx/kdmp/pkg/snapshots/snapshotsinstance"
	kdmpopts "github.com/portworx/kdmp/pkg/util/ops"
	"github.com/portworx/sched-ops/k8s/core"
	"github.com/portworx/sched-ops/k8s/stork"
	"github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/utils/pointer"
)

// Data export label names/keys.
const (
	LabelController      = "kdmp.portworx.com/controller"
	LabelControllerName  = "controller-name"
	KopiaSecretName      = "generic-backup-repo"
	KopiaSecretNamespace = "kube-system"
)

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

	snapshotter, err := snapshotsinstance.Get(snapshots.ExternalStorage)
	if err != nil {
		return false, fmt.Errorf("failed to get snapshotter for a storage provider: %v", err)
	}

	if dataExport.DeletionTimestamp != nil {
		if !controllers.ContainsFinalizer(dataExport, cleanupFinalizer) {
			return false, nil
		}

		if err = c.cleanUp(driver, snapshotter, dataExport); err != nil {
			return true, fmt.Errorf("%s: cleanup: %s", reflect.TypeOf(dataExport), err)
		}

		controllers.RemoveFinalizer(dataExport, cleanupFinalizer)
		return true, c.client.Update(ctx, dataExport)
	}

	switch dataExport.Status.Stage {
	case kdmpapi.DataExportStageInitial:
		return c.stageInitial(ctx, snapshotter, dataExport)
	// TODO: 'merge' scheduled&inProgress&restore stages
	case kdmpapi.DataExportStageSnapshotScheduled:
		return c.stageSnapshotScheduled(ctx, snapshotter, dataExport)
	case kdmpapi.DataExportStageSnapshotInProgress:
		return c.stageSnapshotInProgress(ctx, snapshotter, dataExport)
	case kdmpapi.DataExportStageSnapshotRestore:
		return c.stageSnapshotRestore(ctx, snapshotter, dataExport)
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
		// Create the credential secret
		logrus.Debugf("drivername: %v", driverName)
		if driverName == drivers.KopiaBackup {
			// This will create a unique secret per PVC being backed up
			// Create secret in source ns because in case of multi ns backup
			// BL CR is created in kube-system ns
			err = CreateCredentialsSecret(
				utils.FrameCredSecretName(utils.BackupJobPrefix, dataExport.Name),
				dataExport.Spec.Destination.Name,
				dataExport.Spec.Destination.Namespace,
				dataExport.Spec.Source.Namespace,
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
				utils.FrameCredSecretName(utils.RestoreJobPrefix, dataExport.Name),
				vb.Spec.BackupLocation.Name,
				vb.Spec.BackupLocation.Namespace,
				dataExport.Spec.Destination.Namespace,
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
			msg := fmt.Sprintf("failed to start a data transfer job: %v", err)
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

		if err := c.cleanUp(driver, snapshotter, dataExport); err != nil {
			msg := fmt.Sprintf("failed to remove resources: %s", err)
			return false, c.updateStatus(dataExport, kdmpapi.DataExportStatusFailed, msg)
		}

		return true, c.client.Update(ctx, setStatus(dataExport, kdmpapi.DataExportStatusSuccessful, ""))
	}

	return false, nil
}

func (c *Controller) stageInitial(ctx context.Context, snapshotter snapshots.Driver, dataExport *kdmpapi.DataExport) (bool, error) {
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
	}
	if err != nil {
		msg := fmt.Sprintf("check failed: %s", err)
		return false, c.updateStatus(dataExport, kdmpapi.DataExportStatusFailed, msg)
	}

	return true, c.client.Update(ctx, setStatus(dataExport, kdmpapi.DataExportStatusSuccessful, ""))
}

func (c *Controller) stageSnapshotScheduled(ctx context.Context, snapshotter snapshots.Driver, dataExport *kdmpapi.DataExport) (bool, error) {
	if dataExport.Status.Status == kdmpapi.DataExportStatusSuccessful {
		// set to the next stage
		dataExport.Status.Stage = kdmpapi.DataExportStageSnapshotInProgress
		return true, c.client.Update(ctx, setStatus(dataExport, kdmpapi.DataExportStatusInitial, ""))
	}

	if snapshotter == nil {
		return false, fmt.Errorf("snapshot driver is nil")
	}

	name, namespace, err := snapshotter.CreateSnapshot(
		snapshots.PVCName(dataExport.Spec.Source.Name),
		snapshots.PVCNamespace(dataExport.Spec.Source.Namespace),
		snapshots.RestoreNamespaces(dataExport.Spec.Destination.Namespace),
	)
	if err != nil {
		msg := fmt.Sprintf("failed to create a snapshot: %s", err)
		return false, c.updateStatus(dataExport, kdmpapi.DataExportStatusFailed, msg)
	}

	dataExport.Status.SnapshotID = name
	dataExport.Status.SnapshotNamespace = namespace
	return true, c.client.Update(ctx, setStatus(dataExport, kdmpapi.DataExportStatusSuccessful, ""))
}

func (c *Controller) stageSnapshotInProgress(ctx context.Context, snapshotter snapshots.Driver, dataExport *kdmpapi.DataExport) (bool, error) {
	if dataExport.Status.Status == kdmpapi.DataExportStatusSuccessful {
		// set to the next stage
		dataExport.Status.Stage = kdmpapi.DataExportStageSnapshotRestore
		return true, c.client.Update(ctx, setStatus(dataExport, kdmpapi.DataExportStatusInitial, ""))
	}

	if snapshotter == nil {
		return false, fmt.Errorf("snapshot driver is nil")
	}

	status, err := snapshotter.SnapshotStatus(dataExport.Status.SnapshotID, dataExport.Spec.Source.Namespace)
	if err != nil {
		msg := fmt.Sprintf("failed to get a snapshot status: %s", err)
		return false, c.updateStatus(dataExport, kdmpapi.DataExportStatusFailed, msg)
	}

	if status == snapshots.StatusFailed {
		// TODO: pass a reason
		return false, c.updateStatus(dataExport, kdmpapi.DataExportStatusFailed, "")
	}

	if status != snapshots.StatusReady {
		return false, c.updateStatus(dataExport, kdmpapi.DataExportStatusInProgress, "")
	}

	return true, c.client.Update(ctx, setStatus(dataExport, kdmpapi.DataExportStatusSuccessful, ""))
}

func (c *Controller) stageSnapshotRestore(ctx context.Context, snapshotter snapshots.Driver, dataExport *kdmpapi.DataExport) (bool, error) {
	if dataExport.Status.Status == kdmpapi.DataExportStatusSuccessful {
		// set to the next stage
		dataExport.Status.Stage = kdmpapi.DataExportStageTransferScheduled
		return true, c.client.Update(ctx, setStatus(dataExport, kdmpapi.DataExportStatusInitial, ""))
	}

	if snapshotter == nil {
		return false, fmt.Errorf("snapshot driver is nil")
	}

	pvc, err := c.restoreSnapshot(ctx, snapshotter, dataExport)
	if err != nil {
		msg := fmt.Sprintf("failed to restore a snapshot: %s", err)
		return false, c.updateStatus(dataExport, kdmpapi.DataExportStatusFailed, msg)
	}

	if pvc.Status.Phase != corev1.ClaimBound {
		msg := fmt.Sprintf("snapshot pvc phase is %q, expected- %q", pvc.Status.Phase, corev1.ClaimBound)
		return false, c.updateStatus(dataExport, kdmpapi.DataExportStatusInProgress, msg)
	}

	dataExport.Status.SnapshotPVCName = pvc.Name
	dataExport.Status.SnapshotPVCNamespace = pvc.Namespace
	return true, c.updateStatus(dataExport, kdmpapi.DataExportStatusSuccessful, "")
}

func (c *Controller) cleanUp(driver drivers.Interface, snapshotter snapshots.Driver, de *kdmpapi.DataExport) error {
	if driver == nil {
		return fmt.Errorf("driver is nil")
	}
	if snapshotter == nil {
		return fmt.Errorf("snapshot driver is nil")
	}

	if hasSnapshotStage(de) {
		if de.Status.SnapshotID != "" && de.Status.SnapshotNamespace != "" {
			if err := snapshotter.DeleteSnapshot(de.Status.SnapshotID, de.Status.SnapshotNamespace); err != nil && !errors.IsNotFound(err) {
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
	return c.client.Update(context.TODO(), setStatus(de, status, errMsg))
}

func (c *Controller) restoreSnapshot(ctx context.Context, snapshotter snapshots.Driver, de *kdmpapi.DataExport) (*corev1.PersistentVolumeClaim, error) {
	if snapshotter == nil {
		return nil, fmt.Errorf("snapshot driver is nil")
	}

	src := de.Spec.Source
	srcPvc, err := core.Instance().GetPersistentVolumeClaim(src.Name, src.Namespace)
	if err != nil {
		return nil, err
	}

	restoreSpec := corev1.PersistentVolumeClaimSpec{
		StorageClassName: pointer.StringPtr(de.Spec.SnapshotStorageClass),
		AccessModes:      srcPvc.Spec.AccessModes,
		Resources:        srcPvc.Spec.Resources,
	}
	pvc, err := snapshotter.RestoreVolumeClaim(
		snapshots.Name(de.Status.SnapshotID),
		snapshots.Namespace(de.Status.SnapshotNamespace),
		snapshots.PVCName(toSnapshotPVCName(srcPvc.Name)),
		snapshots.PVCNamespace(de.Spec.Destination.Namespace),
		snapshots.PVCSpec(restoreSpec),
	)
	if err != nil {
		return nil, fmt.Errorf("restore pvc from %s snapshot: %s", de.Status.SnapshotID, err)
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
	if !isPVCRef(de.Spec.Source) && !isAPIVersionKindNotSetRef(de.Spec.Source) {
		return fmt.Errorf("source is expected to be PersistentVolumeClaim")
	}
	// restic supports "live" backups so there is not need to check if it's mounted
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

func (c *Controller) checkResticRestore(de *kdmpapi.DataExport) error {
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
			drivers.WithLabels(jobLabels(dataExport.GetName())),
		)
	case drivers.ResticBackup:
		return drv.StartJob(
			drivers.WithSourcePVC(srcPVCName),
			drivers.WithNamespace(dataExport.Spec.Destination.Namespace),
			drivers.WithBackupLocationName(dataExport.Spec.Destination.Name),
			drivers.WithBackupLocationNamespace(dataExport.Spec.Destination.Namespace),
			drivers.WithLabels(jobLabels(dataExport.GetName())),
		)
	case drivers.ResticRestore:
		return drv.StartJob(
			drivers.WithSourcePVC(srcPVCName),
			drivers.WithDestinationPVC(dataExport.Spec.Destination.Name),
			drivers.WithNamespace(dataExport.Spec.Source.Namespace),
			drivers.WithVolumeBackupName(dataExport.Spec.Source.Name),
			drivers.WithVolumeBackupNamespace(dataExport.Spec.Source.Namespace),
			drivers.WithLabels(jobLabels(dataExport.GetName())),
		)
	case drivers.KopiaBackup:
		return drv.StartJob(
			drivers.WithSourcePVC(srcPVCName),
			drivers.WithNamespace(dataExport.Spec.Source.Namespace),
			drivers.WithBackupLocationName(dataExport.Spec.Destination.Name),
			drivers.WithBackupLocationNamespace(dataExport.Spec.Destination.Namespace),
			drivers.WithLabels(jobLabels(dataExport.GetName())),
			drivers.WithDataExportName(dataExport.GetName()),
		)
	case drivers.KopiaRestore:
		return drv.StartJob(
			drivers.WithDestinationPVC(dataExport.Spec.Destination.Name),
			drivers.WithNamespace(dataExport.Spec.Destination.Namespace),
			drivers.WithVolumeBackupName(dataExport.Spec.Source.Name),
			drivers.WithVolumeBackupNamespace(dataExport.Spec.Source.Namespace),
			drivers.WithBackupLocationNamespace(dataExport.Spec.Source.Namespace),
			drivers.WithLabels(jobLabels(dataExport.GetName())),
			drivers.WithDataExportName(dataExport.GetName()),
		)
	}

	return "", fmt.Errorf("unknown data transfer driver: %s", drv.Name())
}

func checkPVC(in kdmpapi.DataExportObjectReference, checkMounts bool) (*corev1.PersistentVolumeClaim, error) {
	if err := checkNameNamespace(in); err != nil {
		return nil, err
	}
	pvc, err := core.Instance().GetPersistentVolumeClaim(in.Name, in.Namespace)
	if err != nil {
		return nil, err
	}
	if pvc.Status.Phase != corev1.ClaimBound {
		return nil, fmt.Errorf("status: expected %s, got %s", corev1.ClaimBound, pvc.Status.Phase)
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

func toSnapshotPVCName(name string) string {
	return fmt.Sprintf("snap-%s", name)
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

func jobLabels(DataExportName string) map[string]string {
	return map[string]string{
		LabelController:     DataExportName,
		LabelControllerName: DataExportName,
	}
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
func CreateCredentialsSecret(secretName, blName, blNamespace, namespace string) error {
	backupLocation, err := readBackupLocation(blName, blNamespace, "")
	if err != nil {
		return err
	}

	// TODO: Add for other cloud providers
	// Creating cloud cred secret
	switch backupLocation.Location.Type {
	case storkapi.BackupLocationS3:
		return createS3Secret(secretName, backupLocation, namespace)
	case storkapi.BackupLocationGoogle:
		return createGoogleSecret(secretName, backupLocation, namespace)
	case storkapi.BackupLocationAzure:
		return createAzureSecret(secretName, backupLocation, namespace)
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

func createS3Secret(secretName string, backupLocation *storkapi.BackupLocation, namespace string) error {
	credentialData := make(map[string][]byte)
	credentialData["endpoint"] = []byte(backupLocation.Location.S3Config.Endpoint)
	credentialData["accessKey"] = []byte(backupLocation.Location.S3Config.AccessKeyID)
	credentialData["secretAccessKey"] = []byte(backupLocation.Location.S3Config.SecretAccessKey)
	credentialData["region"] = []byte(backupLocation.Location.S3Config.Region)
	credentialData["path"] = []byte(backupLocation.Location.Path)
	credentialData["type"] = []byte(backupLocation.Location.Type)
	credentialData["password"] = []byte(backupLocation.Location.RepositoryPassword)
	err := createCredSecret(secretName, namespace, credentialData)

	return err
}

func createGoogleSecret(secretName string, backupLocation *storkapi.BackupLocation, namespace string) error {
	credentialData := make(map[string][]byte)
	credentialData["type"] = []byte(backupLocation.Location.Type)
	credentialData["password"] = []byte(backupLocation.Location.RepositoryPassword)
	credentialData["accountkey"] = []byte(backupLocation.Location.GoogleConfig.AccountKey)
	credentialData["projectid"] = []byte(backupLocation.Location.GoogleConfig.ProjectID)
	credentialData["path"] = []byte(backupLocation.Location.Path)
	err := createCredSecret(secretName, namespace, credentialData)

	return err
}

func createAzureSecret(secretName string, backupLocation *storkapi.BackupLocation, namespace string) error {
	credentialData := make(map[string][]byte)
	credentialData["type"] = []byte(backupLocation.Location.Type)
	credentialData["password"] = []byte(backupLocation.Location.RepositoryPassword)
	credentialData["path"] = []byte(backupLocation.Location.Path)
	credentialData["storageaccountname"] = []byte(backupLocation.Location.AzureConfig.StorageAccountName)
	credentialData["storageaccountkey"] = []byte(backupLocation.Location.AzureConfig.StorageAccountKey)
	err := createCredSecret(secretName, namespace, credentialData)

	return err
}

func createCredSecret(
	secretName string,
	namespace string,
	credentialData map[string][]byte,
) error {
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      secretName,
			Namespace: namespace,
			Annotations: map[string]string{
				kopiabackup.SkipResourceAnnotation: "true",
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
