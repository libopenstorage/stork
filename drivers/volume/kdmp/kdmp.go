package kdmp

import (
	"fmt"
	"os"
	"reflect"
	"strings"
	"time"

	"github.com/aquilax/truncate"
	snapv1 "github.com/kubernetes-incubator/external-storage/snapshot/pkg/apis/crd/v1"
	snapshotVolume "github.com/kubernetes-incubator/external-storage/snapshot/pkg/volume"
	storkvolume "github.com/libopenstorage/stork/drivers/volume"
	storkapi "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/libopenstorage/stork/pkg/errors"
	"github.com/libopenstorage/stork/pkg/log"
	kdmpapi "github.com/portworx/kdmp/pkg/apis/kdmp/v1alpha1"
	"github.com/portworx/kdmp/pkg/controllers/dataexport"
	"github.com/portworx/kdmp/pkg/drivers"
	"github.com/portworx/kdmp/pkg/drivers/driversinstance"
	kdmputils "github.com/portworx/kdmp/pkg/drivers/utils"
	"github.com/portworx/sched-ops/k8s/batch"
	"github.com/portworx/sched-ops/k8s/core"
	kdmpShedOps "github.com/portworx/sched-ops/k8s/kdmp"
	"github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	k8serror "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/wait"
	k8shelper "k8s.io/kubernetes/pkg/apis/core/v1/helper"
)

const (
	prefixRepo             = "generic-backup"
	prefixRestore          = "restore"
	prefixBackup           = "backup"
	prefixDelete           = "delete"
	skipResourceAnnotation = "stork.libopenstorage.org/skip-resource"
	volumeinitialDelay     = 2 * time.Second
	volumeFactor           = 1.5
	volumeSteps            = 20
	// StorkAPIVersion current api version supported by stork
	StorkAPIVersion = "stork.libopenstorage.org/v1alpha1"
	// KdmpAPIVersion current api version supported by KDMP
	KdmpAPIVersion = "kdmp.portworx.com/v1alpha1"
	// PVCKind constant for pvc
	PVCKind           = "PersistentVolumeClaim"
	kopiaDeleteDriver = "kopiadelete"
	secretNamespace   = "kube-system"
	// KdmpAnnotation for pvcs created by kdmp
	KdmpAnnotation = "stork.libopenstorage.org/created-by"
	// optCSISnapshotClassName is an option for providing a snapshot class name
	optCSISnapshotClassName = "stork.libopenstorage.org/csi-snapshot-class-name"
	// StorkAnnotation for pvcs created by stork-kdmp driver
	StorkAnnotation = "stork.libopenstorage.org/kdmp"

	pxbackupAnnotationPrefix        = "portworx.io/"
	pxbackupAnnotationCreateByKey   = pxbackupAnnotationPrefix + "created-by"
	pxbackupAnnotationCreateByValue = "px-backup"
	pxbackupObjectUIDKey            = pxbackupAnnotationPrefix + "backup-uid"
	pxbackupObjectNameKey           = pxbackupAnnotationPrefix + "backup-name"
	pxRestoreObjectUIDKey           = pxbackupAnnotationPrefix + "restore-uid"
	pxRestoreObjectNameKey          = pxbackupAnnotationPrefix + "restore-name"

	//kdmp related labels
	kdmpAnnotationPrefix = "kdmp.portworx.com/"
	// backup related Labels
	applicationBackupCRNameKey = kdmpAnnotationPrefix + "applicationbackup-cr-name"
	applicationBackupCRUIDKey  = kdmpAnnotationPrefix + "applicationbackup-cr-uid"
	backupObjectNameKey        = kdmpAnnotationPrefix + "backupobject-name"
	backupObjectUIDKey         = kdmpAnnotationPrefix + "backupobject-uid"

	// restore related Labels
	applicationRestoreCRNameKey = kdmpAnnotationPrefix + "applicationrestore-cr-name"
	applicationRestoreCRUIDKey  = kdmpAnnotationPrefix + "applicationrestore-cr-uid"
	restoreObjectNameKey        = kdmpAnnotationPrefix + "restoreobject-name"
	restoreObjectUIDKey         = kdmpAnnotationPrefix + "restoreobject-uid"

	pvcNameKey = kdmpAnnotationPrefix + "pvc-name"
	pvcUIDKey  = kdmpAnnotationPrefix + "pvc-uid"
)

var volumeAPICallBackoff = wait.Backoff{
	Duration: volumeinitialDelay,
	Factor:   volumeFactor,
	Steps:    volumeSteps,
}

type kdmp struct {
	storkvolume.ClusterPairNotSupported
	storkvolume.MigrationNotSupported
	storkvolume.GroupSnapshotNotSupported
	storkvolume.ClusterDomainsNotSupported
	storkvolume.CloneNotSupported
	storkvolume.SnapshotRestoreNotSupported
}

func (k *kdmp) Init(_ interface{}) error {
	return nil
}

func (k *kdmp) String() string {
	return storkvolume.KDMPDriverName
}

func (k *kdmp) Stop() error {
	return nil
}

func (k *kdmp) OwnsPVC(coreOps core.Ops, pvc *v1.PersistentVolumeClaim) bool {
	// KDMP can handle any PVC type. KDMP driver will always be a fallback
	// option when none of the other supported drivers by stork own the PVC
	return true
}

func (k *kdmp) OwnsPV(pv *v1.PersistentVolume) bool {
	// KDMP can handle any PVC type. KDMP driver will always be a fallback
	// option when none of the other supported drivers by stork own the PVC
	return true
}

func getGenericCRName(opsPrefix, crUID, pvcUID, ns string) string {
	name := fmt.Sprintf("%s-%s-%s-%s", opsPrefix, getShortUID(crUID), getShortUID(pvcUID), ns)
	name = getValidLabel(name)
	return name
}

func (k *kdmp) StartBackup(backup *storkapi.ApplicationBackup,
	pvcs []v1.PersistentVolumeClaim,
) ([]*storkapi.ApplicationBackupVolumeInfo, error) {
	log.ApplicationBackupLog(backup).Debugf("started generic backup: %v", backup.Name)
	volumeInfos := make([]*storkapi.ApplicationBackupVolumeInfo, 0)
	for _, pvc := range pvcs {
		if pvc.DeletionTimestamp != nil {
			log.ApplicationBackupLog(backup).Warnf("Ignoring PVC %v which is being deleted", pvc.Name)
			continue
		}
		volumeInfo := &storkapi.ApplicationBackupVolumeInfo{}
		volumeInfo.PersistentVolumeClaim = pvc.Name
		volumeInfo.PersistentVolumeClaimUID = string(pvc.UID)
		volumeInfo.StorageClass = k8shelper.GetPersistentVolumeClaimClass(&pvc)
		volumeInfo.Namespace = pvc.Namespace
		volumeInfo.DriverName = storkvolume.KDMPDriverName
		volume, err := core.Instance().GetVolumeForPersistentVolumeClaim(&pvc)
		if err != nil {
			return nil, fmt.Errorf("error getting volume for PVC: %v", err)
		}
		volumeInfo.Volume = volume

		// create kdmp cr
		dataExport := &kdmpapi.DataExport{}
		// Adding required label for debugging
		labels := make(map[string]string)
		labels[applicationBackupCRNameKey] = getValidLabel(backup.Name)
		labels[applicationBackupCRUIDKey] = getValidLabel(getShortUID(string(backup.UID)))
		labels[pvcNameKey] = getValidLabel(pvc.Name)
		labels[pvcUIDKey] = getValidLabel(getShortUID(string(pvc.UID)))
		// If backup from px-backup, update the backup object details in the label
		if val, ok := backup.Annotations[pxbackupAnnotationCreateByKey]; ok {
			if val == pxbackupAnnotationCreateByValue {
				labels[backupObjectNameKey] = getValidLabel(backup.Annotations[pxbackupObjectNameKey])
				labels[backupObjectUIDKey] = getValidLabel(backup.Annotations[pxbackupObjectUIDKey])
			}
		}

		dataExport.Labels = labels
		dataExport.Annotations = make(map[string]string)
		dataExport.Annotations[skipResourceAnnotation] = "true"
		dataExport.Name = getGenericCRName(prefixBackup, string(backup.UID), string(pvc.UID), pvc.Namespace)
		dataExport.Namespace = pvc.Namespace
		dataExport.Spec.Type = kdmpapi.DataExportKopia
		dataExport.Spec.Destination = kdmpapi.DataExportObjectReference{
			Kind:       reflect.TypeOf(storkapi.BackupLocation{}).Name(),
			Name:       backup.Spec.BackupLocation,
			Namespace:  backup.Namespace,
			APIVersion: StorkAPIVersion,
		}
		dataExport.Spec.Source = kdmpapi.DataExportObjectReference{
			Kind:       pvc.Kind,
			Name:       pvc.Name,
			Namespace:  pvc.Namespace,
			APIVersion: pvc.APIVersion,
		}
		dataExport.Spec.SnapshotStorageClass = k.getSnapshotClassName(backup)
		_, err = kdmpShedOps.Instance().CreateDataExport(dataExport)
		if err != nil {
			logrus.Errorf("failed to create DataExport CR: %v", err)
			return volumeInfos, err
		}
		volumeInfos = append(volumeInfos, volumeInfo)
	}

	return volumeInfos, nil
}

func (k *kdmp) GetBackupStatus(backup *storkapi.ApplicationBackup) ([]*storkapi.ApplicationBackupVolumeInfo, error) {
	volumeInfos := make([]*storkapi.ApplicationBackupVolumeInfo, 0)
	for _, vInfo := range backup.Status.Volumes {
		if vInfo.DriverName != storkvolume.KDMPDriverName {
			continue
		}
		crName := getGenericCRName(prefixBackup, string(backup.UID), vInfo.PersistentVolumeClaimUID, vInfo.Namespace)
		dataExport, err := kdmpShedOps.Instance().GetDataExport(crName, vInfo.Namespace)
		if err != nil {
			logrus.Errorf("failed to get backup DataExport CR: %v", err)
			return volumeInfos, err
		}

		if dataExport.Status.Status == kdmpapi.DataExportStatusFailed &&
			dataExport.Status.Stage == kdmpapi.DataExportStageFinal {
			vInfo.Status = storkapi.ApplicationBackupStatusFailed
			vInfo.Reason = fmt.Sprintf("Backup failed at stage %v for volume: %v", dataExport.Status.Stage, dataExport.Status.Reason)
			volumeInfos = append(volumeInfos, vInfo)
			continue
		}

		if dataExport.Status.TransferID == "" {
			vInfo.Status = storkapi.ApplicationBackupStatusInitial
			vInfo.Reason = "Volume backup not started yet"
		} else {
			vInfo.BackupID = dataExport.Status.SnapshotID
			if isDataExportActive(dataExport.Status) {
				vInfo.Status = storkapi.ApplicationBackupStatusInProgress
				vInfo.Reason = "Volume backup in progress"
			} else if isDataExportCompleted(dataExport.Status) {
				vInfo.Status = storkapi.ApplicationBackupStatusSuccessful
				vInfo.Reason = "Backup successful for volume"
				vInfo.TotalSize = dataExport.Status.Size
				vInfo.ActualSize = dataExport.Status.Size
			}
		}
		volumeInfos = append(volumeInfos, vInfo)
	}
	return volumeInfos, nil
}
func isDataExportActive(status kdmpapi.ExportStatus) bool {
	if status.Stage == kdmpapi.DataExportStageTransferInProgress ||
		status.Stage == kdmpapi.DataExportStageSnapshotInProgress ||
		status.Stage == kdmpapi.DataExportStageSnapshotScheduled ||
		status.Status == kdmpapi.DataExportStatusInProgress ||
		status.Status == kdmpapi.DataExportStatusPending ||
		status.Status == kdmpapi.DataExportStatusInitial {
		return true
	}
	return false
}
func isDataExportCompleted(status kdmpapi.ExportStatus) bool {
	if status.Stage == kdmpapi.DataExportStageFinal &&
		status.Status == kdmpapi.DataExportStatusSuccessful {
		return true
	}
	return false
}

func (k *kdmp) CancelBackup(backup *storkapi.ApplicationBackup) error {
	for _, vInfo := range backup.Status.Volumes {
		crName := getGenericCRName(prefixBackup, string(backup.UID), vInfo.PersistentVolumeClaimUID, vInfo.Namespace)
		err := kdmpShedOps.Instance().DeleteDataExport(crName, vInfo.Namespace)
		if err != nil && k8serror.IsNotFound(err) {
			errMsg := fmt.Sprintf("failed to delete data export CR [%v]: %v", crName, err)
			log.ApplicationBackupLog(backup).Errorf("%v", errMsg)
		}
	}

	return nil
}

func (k *kdmp) DeleteBackup(backup *storkapi.ApplicationBackup) (bool, error) {
	// For Applicationbackup CR created by px-backup, we want to handle deleting
	// successful PVC (of in-progress backup) from px-backup deleteworker() to avoid two entities
	// doing the delete of snapshot leading to races.
	if val, ok := backup.Annotations[pxbackupAnnotationCreateByKey]; !ok {
		return deleteKdmpSnapshot(backup)
	} else if val != pxbackupAnnotationCreateByValue {
		return deleteKdmpSnapshot(backup)
	} else {
		logrus.Infof("skipping snapshot deletion as ApplicationBackup CR [%v] is created by px-backup", backup.Name)
	}

	return true, nil
}

func deleteKdmpSnapshot(backup *storkapi.ApplicationBackup) (bool, error) {
	index := -1
	for len(backup.Status.Volumes) >= 1 {
		index++
		if index >= len(backup.Status.Volumes) {
			break
		}
		vInfo := backup.Status.Volumes[index]
		// Delete those successful PVC vols which are completed as part of this backup.
		if vInfo.BackupID != "" {
			secretName := getGenericCRName(prefixDelete, string(backup.UID), vInfo.PersistentVolumeClaimUID, vInfo.Namespace)
			driver, err := driversinstance.Get(kopiaDeleteDriver)
			if err != nil {
				errMsg := fmt.Sprintf("failed in getting driver instance of kdmp delete driver: %v", err)
				logrus.Errorf("%v", errMsg)
				return false, fmt.Errorf(errMsg)
			}
			jobName := getGenericCRName(prefixDelete, string(backup.UID), vInfo.PersistentVolumeClaimUID, vInfo.Namespace)
			_, err = batch.Instance().GetJob(
				jobName,
				vInfo.Namespace,
			)
			if err != nil && k8serror.IsNotFound(err) {
				// Adding required label for debugging
				labels := make(map[string]string)
				labels[applicationBackupCRNameKey] = getValidLabel(backup.Name)
				labels[applicationBackupCRUIDKey] = getValidLabel(string(backup.UID))
				labels[pvcNameKey] = getValidLabel(vInfo.PersistentVolumeClaim)
				labels[pvcUIDKey] = getValidLabel(vInfo.PersistentVolumeClaimUID)
				// If backup from px-backup, update the backup object details in the label
				if val, ok := backup.Annotations[pxbackupAnnotationCreateByKey]; ok {
					if val == pxbackupAnnotationCreateByValue {
						labels[backupObjectNameKey] = getValidLabel(backup.Annotations[pxbackupObjectNameKey])
						labels[backupObjectUIDKey] = getValidLabel(backup.Annotations[pxbackupObjectUIDKey])
					}
				}
				err := dataexport.CreateCredentialsSecret(secretName, backup.Spec.BackupLocation, backup.Namespace, backup.Namespace, labels)
				if err != nil {
					errMsg := fmt.Sprintf("failed to create secret [%v] in namespace [%v]: %v", secretName, backup.Namespace, err)
					log.ApplicationBackupLog(backup).Errorf("%v", errMsg)
					return false, fmt.Errorf(errMsg)
				}
				_, err = driver.StartJob(
					drivers.WithJobName(jobName),
					drivers.WithSnapshotID(vInfo.BackupID),
					drivers.WithNamespace(vInfo.Namespace),
					drivers.WithSourcePVC(vInfo.PersistentVolumeClaim),
					drivers.WithCredSecretName(secretName),
					drivers.WithCredSecretNamespace(secretNamespace),
				)
				if err != nil {
					errMsg := fmt.Sprintf("failed to start kdmp snapshot delete job for snapshot [%v] for backup [%v]: %v",
						vInfo.BackupID, backup.Name, err)
					log.ApplicationBackupLog(backup).Errorf("%v", errMsg)
					return false, fmt.Errorf(errMsg)
				}
				return false, nil
			}
			jobID := kdmputils.NamespacedName(backup.Namespace, jobName)
			canDelete, err := doKdmpDeleteJob(jobID, driver)
			if err != nil {
				return false, err
			}
			if canDelete {
				// Remove the vol from the volInfo list
				copy(backup.Status.Volumes[index:], backup.Status.Volumes[index+1:])
				backup.Status.Volumes = backup.Status.Volumes[:len(backup.Status.Volumes)-1]
				// After each successful deleted iteration, volume list is update.
				// Starting from previous postion as the delted volume slot is empty
				index--
			}
		}
	}

	// When all vols are deleted, go ahead and delete the secret
	if len(backup.Status.Volumes) == 0 {
		err := core.Instance().DeleteSecret(backup.Name, backup.Namespace)
		if err != nil && !k8serror.IsNotFound(err) {
			errMsg := fmt.Sprintf("failed to delete secret [%s] from namespace [%v]:' %v",
				backup.Name, backup.Namespace, err)
			log.ApplicationBackupLog(backup).Errorf("%v", errMsg)
			return false, fmt.Errorf(errMsg)
		}
	} else {
		// Some jobs are still in-progress
		log.ApplicationBackupLog(backup).Debugf("cannot delete ApplicationBackup CR as some vol deletes are in-progress")
		return false, nil
	}

	return true, nil
}

func doKdmpDeleteJob(id string, driver drivers.Interface) (bool, error) {
	fn := "doKdmpDeleteJob:"
	progress, err := driver.JobStatus(id)
	if err != nil {
		errMsg := fmt.Errorf("failed in getting kdmp delete job [%v] status: %v", id, err)
		logrus.Warnf("%s %v", fn, errMsg)
		return false, errMsg
	}
	switch progress.State {
	case drivers.JobStateCompleted:
		if err := driver.DeleteJob(id); err != nil {
			errMsg := fmt.Errorf("deletion of job [%v] failed: %v", id, err)
			logrus.Warnf("%s %v", fn, errMsg)
			return false, errMsg
		}
		return true, nil
	case drivers.JobStateFailed:
		errMsg := fmt.Errorf("kdmp delete job [%v] failed to execute: %v", id, err)
		logrus.Warnf("%s %v", fn, errMsg)
		if err := driver.DeleteJob(id); err != nil {
			errMsg := fmt.Errorf("deletion of job [%v] failed: %v", id, err)
			logrus.Warnf("%s %v", fn, errMsg)
			return false, errMsg
		}
		return true, nil
	case drivers.JobStateInProgress:
		return false, nil
	default:
		errMsg := fmt.Errorf("invalid job [%v] status type: [%v]", id, progress.State)
		logrus.Warnf("%s %v", fn, errMsg)
		return true, nil
	}
}

func (k *kdmp) UpdateMigratedPersistentVolumeSpec(
	pv *v1.PersistentVolume,
) (*v1.PersistentVolume, error) {
	return pv, nil
}

func (k *kdmp) GetPreRestoreResources(
	backup *storkapi.ApplicationBackup,
	restore *storkapi.ApplicationRestore,
	objects []runtime.Unstructured,
) ([]runtime.Unstructured, error) {
	return k.getRestorePVCs(backup, restore, objects)
}

func (k *kdmp) getRestorePVCs(
	backup *storkapi.ApplicationBackup,
	restore *storkapi.ApplicationRestore,
	objects []runtime.Unstructured,
) ([]runtime.Unstructured, error) {
	pvcs := []runtime.Unstructured{}
	// iterate through all of objects and process only pvcs
	// check if source pvc is present in storageclass mapping
	// update pvc storage class if found in storageclass mapping
	// TODO: need to make sure pv name remains updated
	for _, object := range objects {
		objectType, err := meta.TypeAccessor(object)
		if err != nil {
			return nil, err
		}

		switch objectType.GetKind() {
		case "PersistentVolumeClaim":
			var pvc v1.PersistentVolumeClaim
			if err := runtime.DefaultUnstructuredConverter.FromUnstructured(object.UnstructuredContent(), &pvc); err != nil {
				return nil, err
			}
			sc := k8shelper.GetPersistentVolumeClaimClass(&pvc)
			if val, ok := restore.Spec.StorageClassMapping[sc]; ok {
				pvc.Spec.StorageClassName = &val
				pvc.Spec.VolumeName = ""
			}
			if pvc.Annotations != nil {
				delete(pvc.Annotations, "pv.kubernetes.io/bind-completed")
				delete(pvc.Annotations, "pv.kubernetes.io/bound-by-controller")
				pvc.Annotations[KdmpAnnotation] = StorkAnnotation
			}
			o, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&pvc)
			if err != nil {
				logrus.Errorf("unable to convert pvc to unstruct objects, err: %v", err)
				return nil, err
			}
			object.SetUnstructuredContent(o)
			pvcs = append(pvcs, object)
		default:
			continue
		}
	}
	return pvcs, nil
}

func (k *kdmp) StartRestore(
	restore *storkapi.ApplicationRestore,
	volumeBackupInfos []*storkapi.ApplicationBackupVolumeInfo,
) ([]*storkapi.ApplicationRestoreVolumeInfo, error) {
	log.ApplicationRestoreLog(restore).Debugf("started generic restore: %v", restore.Name)
	volumeInfos := make([]*storkapi.ApplicationRestoreVolumeInfo, 0)
	for _, bkpvInfo := range volumeBackupInfos {
		volumeInfo := &storkapi.ApplicationRestoreVolumeInfo{}
		// wait for pvc to get bound
		err := wait.ExponentialBackoff(volumeAPICallBackoff, func() (bool, error) {
			pvc, err := core.Instance().GetPersistentVolumeClaim(bkpvInfo.PersistentVolumeClaim, bkpvInfo.Namespace)
			if err != nil {
				return false, nil
			}
			if pvc.Spec.VolumeName == "" || pvc.Status.Phase != v1.ClaimBound {
				return false, nil
			}
			volumeInfo.RestoreVolume = pvc.Spec.VolumeName
			return true, nil
		})
		if err != nil {
			logrus.Errorf("Failed to get volume restore volume: %s, err: %v", bkpvInfo.PersistentVolumeClaim, err)
			continue
		}
		if err != nil {
			return nil, err
		}
		val, ok := restore.Spec.NamespaceMapping[bkpvInfo.Namespace]
		if !ok {
			return nil, fmt.Errorf("restore namespace mapping not found: %s", bkpvInfo.Namespace)

		}
		restoreNamespace := val
		volumeInfo.PersistentVolumeClaim = bkpvInfo.PersistentVolumeClaim
		volumeInfo.PersistentVolumeClaimUID = bkpvInfo.PersistentVolumeClaimUID
		volumeInfo.SourceNamespace = bkpvInfo.Namespace
		volumeInfo.SourceVolume = bkpvInfo.Volume
		volumeInfo.DriverName = storkvolume.KDMPDriverName

		// create VolumeBackup CR
		// Adding required label for debugging
		labels := make(map[string]string)
		labels[applicationRestoreCRNameKey] = getValidLabel(restore.Name)
		labels[applicationRestoreCRUIDKey] = getValidLabel(string(restore.UID))
		labels[pvcNameKey] = getValidLabel(bkpvInfo.PersistentVolumeClaim)
		labels[pvcUIDKey] = getValidLabel(bkpvInfo.PersistentVolumeClaimUID)
		// If restorefrom px-backup, update the restore object details in the label
		if val, ok := restore.Annotations[pxbackupAnnotationCreateByKey]; ok {
			if val == pxbackupAnnotationCreateByValue {
				labels[restoreObjectNameKey] = getValidLabel(restore.Annotations[pxbackupObjectNameKey])
				labels[restoreObjectUIDKey] = getValidLabel(restore.Annotations[pxbackupObjectUIDKey])
			}
		}

		volBackup := &kdmpapi.VolumeBackup{}
		volBackup.Labels = labels
		volBackup.Annotations = make(map[string]string)
		volBackup.Annotations[skipResourceAnnotation] = "true"
		volBackup.Name = getGenericCRName(prefixRestore, string(restore.UID), bkpvInfo.PersistentVolumeClaimUID, restoreNamespace)
		volBackup.Namespace = restoreNamespace
		volBackup.Spec.BackupLocation = kdmpapi.DataExportObjectReference{
			Kind:       reflect.TypeOf(storkapi.BackupLocation{}).Name(),
			Name:       restore.Spec.BackupLocation,
			Namespace:  restore.Namespace, // since this can be kube-system in case of multple namespace restore
			APIVersion: StorkAPIVersion,
		}
		volBackup.Spec.Repository = fmt.Sprintf("%s/%s-%s/", prefixRepo, volumeInfo.SourceNamespace, bkpvInfo.PersistentVolumeClaim)
		volBackup.Status.SnapshotID = bkpvInfo.BackupID
		if _, err := kdmpShedOps.Instance().CreateVolumeBackup(volBackup); err != nil {
			logrus.Errorf("unable to create volumebackup CR: %v", err)
			return nil, err
		}

		// create kdmp cr
		dataExport := &kdmpapi.DataExport{}
		dataExport.Labels = labels
		dataExport.Annotations = make(map[string]string)
		dataExport.Annotations[skipResourceAnnotation] = "true"
		dataExport.Name = getGenericCRName(prefixRestore, string(restore.UID), bkpvInfo.PersistentVolumeClaimUID, restoreNamespace)
		dataExport.Namespace = restoreNamespace
		dataExport.Status.TransferID = volBackup.Namespace + "/" + volBackup.Name
		dataExport.Spec.Type = kdmpapi.DataExportKopia
		dataExport.Spec.Source = kdmpapi.DataExportObjectReference{
			Kind:       reflect.TypeOf(kdmpapi.VolumeBackup{}).Name(),
			Name:       volBackup.Name,
			Namespace:  volBackup.Namespace,
			APIVersion: KdmpAPIVersion,
		}
		dataExport.Spec.Destination = kdmpapi.DataExportObjectReference{
			Kind:       PVCKind,
			Name:       bkpvInfo.PersistentVolumeClaim,
			Namespace:  restoreNamespace,
			APIVersion: "v1",
		}
		if _, err := kdmpShedOps.Instance().CreateDataExport(dataExport); err != nil {
			logrus.Errorf("failed to create DataExport CR: %v", err)
			return volumeInfos, err
		}
		volumeInfos = append(volumeInfos, volumeInfo)
	}
	return volumeInfos, nil
}

func (k *kdmp) CancelRestore(restore *storkapi.ApplicationRestore) error {
	for _, vInfo := range restore.Status.Volumes {
		val, ok := restore.Spec.NamespaceMapping[vInfo.SourceNamespace]
		if !ok {
			return fmt.Errorf("restore namespace mapping not found: %s", vInfo.SourceNamespace)

		}
		restoreNamespace := val
		crName := getGenericCRName(prefixRestore, string(restore.UID), vInfo.PersistentVolumeClaimUID, restoreNamespace)
		if err := kdmpShedOps.Instance().DeleteDataExport(crName, restoreNamespace); err != nil {
			logrus.Errorf("failed to delete data export CR: %v", err)
			return err
		}
		if err := kdmpShedOps.Instance().DeleteVolumeBackup(crName, restoreNamespace); err != nil {
			logrus.Tracef("failed to delete volume backup CR:%s/%s, err: %v", restoreNamespace, crName, err)
		}
	}
	return nil
}

func (k *kdmp) GetRestoreStatus(restore *storkapi.ApplicationRestore) ([]*storkapi.ApplicationRestoreVolumeInfo, error) {
	volumeInfos := make([]*storkapi.ApplicationRestoreVolumeInfo, 0)
	for _, vInfo := range restore.Status.Volumes {
		if vInfo.DriverName != storkvolume.KDMPDriverName {
			continue
		}
		val, ok := restore.Spec.NamespaceMapping[vInfo.SourceNamespace]
		if !ok {
			return nil, fmt.Errorf("restore namespace mapping not found: %s", vInfo.SourceNamespace)

		}
		restoreNamespace := val
		crName := getGenericCRName(prefixRestore, string(restore.UID), vInfo.PersistentVolumeClaimUID, restoreNamespace)
		dataExport, err := kdmpShedOps.Instance().GetDataExport(crName, restoreNamespace)
		if err != nil {
			logrus.Errorf("failed to get restore DataExport CR: %v", err)
			return volumeInfos, err
		}
		if dataExport.Status.TransferID == "" {
			vInfo.Status = storkapi.ApplicationRestoreStatusInitial
			vInfo.Reason = "Volume restore not started yet"
		} else {
			if isDataExportActive(dataExport.Status) {
				vInfo.Status = storkapi.ApplicationRestoreStatusInProgress
				vInfo.Reason = "Volume restore is in progress. BytesDone"
				// TODO: KDMP does not return size right now, we will need to fill up
				// size for volume once support is available
			} else if dataExport.Status.Status == kdmpapi.DataExportStatusFailed {
				vInfo.Status = storkapi.ApplicationRestoreStatusFailed
				vInfo.Reason = fmt.Sprintf("restore failed for volume:%s, %s", vInfo.SourceVolume, dataExport.Status.Reason)
			} else if isDataExportCompleted(dataExport.Status) {
				vInfo.Status = storkapi.ApplicationRestoreStatusSuccessful
				vInfo.Reason = "restore successful for volume"
				vInfo.TotalSize = dataExport.Status.Size
			}
		}
		volumeInfos = append(volumeInfos, vInfo)
	}
	return volumeInfos, nil
}

func (k *kdmp) InspectVolume(volumeID string) (*storkvolume.Info, error) {
	return nil, &errors.ErrNotSupported{}
}

func (k *kdmp) GetClusterID() (string, error) {
	return "", &errors.ErrNotSupported{}
}

func (k *kdmp) GetNodes() ([]*storkvolume.NodeInfo, error) {
	return nil, &errors.ErrNotSupported{}
}

func (k *kdmp) InspectNode(id string) (*storkvolume.NodeInfo, error) {
	return nil, &errors.ErrNotSupported{}
}

func (k *kdmp) GetPodVolumes(podSpec *v1.PodSpec, namespace string) ([]*storkvolume.Info, error) {
	return nil, &errors.ErrNotSupported{}
}

func (k *kdmp) GetSnapshotPlugin() snapshotVolume.Plugin {
	return nil
}

func (k *kdmp) GetSnapshotType(snap *snapv1.VolumeSnapshot) (string, error) {
	return "", &errors.ErrNotSupported{}
}

func (k *kdmp) GetVolumeClaimTemplates([]v1.PersistentVolumeClaim) (
	[]v1.PersistentVolumeClaim, error) {
	return nil, &errors.ErrNotSupported{}
}

// CleanupBackupResources for specified backup
func (k *kdmp) CleanupBackupResources(backup *storkapi.ApplicationBackup) error {
	// delete data export crs once backup is completed
	for _, vInfo := range backup.Status.Volumes {
		if vInfo.DriverName != storkvolume.KDMPDriverName {
			continue
		}
		crName := getGenericCRName(prefixBackup, string(backup.UID), vInfo.PersistentVolumeClaimUID, vInfo.Namespace)
		logrus.Tracef("deleting data export CR: %s/%s", vInfo.Namespace, crName)
		// delete kdmp crs
		if err := kdmpShedOps.Instance().DeleteDataExport(crName, vInfo.Namespace); err != nil && !k8serror.IsNotFound(err) {
			logrus.Warnf("failed to delete data export CR: %v", err)
		}
	}

	return nil
}

// CleanupRestoreResources for specified restore
func (k *kdmp) CleanupRestoreResources(restore *storkapi.ApplicationRestore) error {
	for _, vInfo := range restore.Status.Volumes {
		if vInfo.DriverName != storkvolume.KDMPDriverName {
			continue
		}
		val, ok := restore.Spec.NamespaceMapping[vInfo.SourceNamespace]
		if !ok {
			return fmt.Errorf("restore namespace mapping not found: %s", vInfo.SourceNamespace)

		}
		restoreNamespace := val
		crName := getGenericCRName(prefixRestore, string(restore.UID), vInfo.PersistentVolumeClaim, restoreNamespace)
		// delete kdmp crs
		logrus.Tracef("deleting data export CR: %s%s", restoreNamespace, crName)
		if err := kdmpShedOps.Instance().DeleteDataExport(crName, restoreNamespace); err != nil && !k8serror.IsNotFound(err) {
			logrus.Warnf("failed to delete data export CR:%s/%s, err: %v", restoreNamespace, crName, err)
		}
		err := kdmpShedOps.Instance().DeleteVolumeBackup(prefixRestore+"-"+crName, restoreNamespace)
		if err != nil && !k8serror.IsNotFound(err) {
			logrus.Warnf("failed to delete volume backup CR:%s/%s, err: %v", restoreNamespace, crName, err)
		}
	}
	return nil
}

// GetGenericDriverName returns current generic backup/restore driver
func GetGenericDriverName() string {
	return storkvolume.KDMPDriverName
}

func (k *kdmp) getSnapshotClassName(backup *storkapi.ApplicationBackup) string {
	if snapshotClassName, ok := backup.Spec.Options[optCSISnapshotClassName]; ok {
		return snapshotClassName
	}
	return ""
}

func init() {
	a := &kdmp{}

	// set kdmp executor name
	if err := os.Setenv(drivers.KopiaExecutorImageKey, drivers.KopiaExecutorImage+":"+"master"); err != nil {
		logrus.Debugf("Unable to set custom kdmp image")
	}
	err := a.Init(nil)
	if err != nil {
		logrus.Debugf("Error init'ing kdmp driver: %v", err)
	}
	if err := storkvolume.Register(storkvolume.KDMPDriverName, a); err != nil {
		logrus.Panicf("Error registering kdmp volume driver: %v", err)
	}
}

// getValidLabel - will validate the label to make sure the length is less 63 and contains valid label format.
// If the length is greater then 63, it will truncate to 63 character.
func getValidLabel(labelVal string) string {
	if len(labelVal) > validation.LabelValueMaxLength {
		labelVal = truncate.Truncate(labelVal, validation.LabelValueMaxLength, "", truncate.PositionEnd)
		// make sure the truncated value does not end with the hyphen.
		labelVal = strings.Trim(labelVal, "-")
		// make sure the truncated value does not end with the dot.
		labelVal = strings.Trim(labelVal, ".")
	}
	return labelVal
}

// getShortUID returns the first part of the UID
func getShortUID(uid string) string {
	return uid[0:7]
}
