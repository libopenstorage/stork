package kdmp

import (
	"fmt"
	"os"
	"reflect"
	"strings"
	"time"

	snapv1 "github.com/kubernetes-incubator/external-storage/snapshot/pkg/apis/crd/v1"
	snapshotVolume "github.com/kubernetes-incubator/external-storage/snapshot/pkg/volume"
	storkvolume "github.com/libopenstorage/stork/drivers/volume"
	storkapi "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/libopenstorage/stork/pkg/errors"
	"github.com/libopenstorage/stork/pkg/log"
	kdmpapi "github.com/portworx/kdmp/pkg/apis/kdmp/v1alpha1"
	"github.com/portworx/kdmp/pkg/drivers"
	"github.com/portworx/sched-ops/k8s/core"
	kdmpShedOps "github.com/portworx/sched-ops/k8s/kdmp"
	"github.com/portworx/sched-ops/k8s/storage"
	"github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	k8shelper "k8s.io/kubernetes/pkg/apis/core/v1/helper"
)

const (
	// driverName is the name of the kdmp driver implementation
	driverName    = "kdmp"
	prefixRepo    = "generic-backup/"
	proxyEndpoint = "proxy_endpoint"
	proxyPath     = "proxy_nfs_exportpath"
	// StorkAPIVersion current api version supported by stork
	StorkAPIVersion        = "stork.libopenstorage.org/v1alpha1"
	skipResourceAnnotation = "stork.libopenstorage.org/skip-resource"
	volumenitialDelay      = 2 * time.Second
	volumeFactor           = 1.5
	volumeSteps            = 20
)

var volumeAPICallBackoff = wait.Backoff{
	Duration: volumenitialDelay,
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
	return driverName
}

func (k *kdmp) Stop() error {
	return nil
}

func (k *kdmp) OwnsPVC(coreOps core.Ops, pvc *v1.PersistentVolumeClaim) bool {
	// Try to get info from the PV since storage class could be deleted
	pv, err := coreOps.GetPersistentVolume(pvc.Spec.VolumeName)
	if err != nil {
		log.PVCLog(pvc).Warnf("error getting pv %v for pvc %v: %v", pvc.Spec.VolumeName, pvc.Name, err)
		return false
	}
	// for px proxy volume use kdmp by default
	storageClassName := k8shelper.GetPersistentVolumeClaimClass(pvc)
	if storageClassName != "" {
		storageClass, err := storage.Instance().GetStorageClass(storageClassName)
		if err != nil {
			logrus.Warnf("Error getting storageclass %v for pvc %v: %v", storageClassName, pvc.Name, err)
		}
		if _, ok := storageClass.Parameters[proxyEndpoint]; ok {
			return true
		}
	}
	return k.OwnsPV(pv)
}

func (k *kdmp) OwnsPV(pv *v1.PersistentVolume) bool {

	// for csi volumes (with or without snapshot) use kdmp driver by default
	// except for px csi volumes
	if pv.Spec.CSI != nil {
		// in case of px csi volume fall back to default csi volume driver
		if pv.Spec.CSI.Driver == snapv1.PortworxCsiProvisionerName ||
			pv.Spec.CSI.Driver == snapv1.PortworxCsiDeprecatedProvisionerName {
			return false
		}
		log.PVLog(pv).Tracef("CSI Owns PV: %s", pv.Name)
		return true
	}

	return true
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
		volumeInfo.StorageClass = k8shelper.GetPersistentVolumeClaimClass(&pvc)
		volumeInfo.Namespace = pvc.Namespace
		volumeInfo.DriverName = driverName
		volume, err := core.Instance().GetVolumeForPersistentVolumeClaim(&pvc)
		if err != nil {
			return nil, fmt.Errorf("error getting volume for PVC: %v", err)
		}
		volumeInfo.Volume = volume

		// create kdmp cr
		dataExport := &kdmpapi.DataExport{}
		dataExport.Annotations = make(map[string]string)
		dataExport.Annotations[skipResourceAnnotation] = "true"
		dataExport.Name = backup.Name + "-" + pvc.Namespace + "-" + pvc.Name
		dataExport.Namespace = pvc.Namespace
		// TODO: this needs to be generic, maybe read it from configmap
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
		_, err = kdmpShedOps.Instance().CreateDataExport(dataExport)
		if err != nil {
			logrus.Errorf("failed to create DataExport CR: %v", err)
			return volumeInfos, err
		}
		logrus.Debugf("adding volume info: %v", volumeInfo)
		volumeInfos = append(volumeInfos, volumeInfo)
	}

	return volumeInfos, nil
}

func (k *kdmp) GetBackupStatus(backup *storkapi.ApplicationBackup) ([]*storkapi.ApplicationBackupVolumeInfo, error) {
	volumeInfos := make([]*storkapi.ApplicationBackupVolumeInfo, 0)
	for _, vInfo := range backup.Status.Volumes {
		if vInfo.DriverName != driverName {
			continue
		}
		crName := backup.Name + "-" + vInfo.Namespace + "-" + vInfo.PersistentVolumeClaim
		dataExport, err := kdmpShedOps.Instance().GetDataExport(crName, backup.Namespace)
		if err != nil {
			logrus.Errorf("failed to get DataExport CR: %v", err)
			return volumeInfos, err
		}
		if dataExport.Status.TransferID == "" {
			vInfo.Status = storkapi.ApplicationBackupStatusInitial
			vInfo.Reason = "Volume backup not started yet"
		} else {
			crName := strings.Split(dataExport.Status.TransferID, "/")[1]
			volumeBackup, err := kdmpShedOps.Instance().GetVolumeBackup(crName, backup.Namespace)
			if err != nil {
				logrus.Errorf("failed to get volumebackup CR: %s, err:%v", crName, err)
				return volumeInfos, err
			}
			vInfo.BackupID = volumeBackup.Status.SnapshotID
			if isBackupActive(dataExport.Status) {
				vInfo.Status = storkapi.ApplicationBackupStatusInProgress
				vInfo.Reason = fmt.Sprintf("Volume backup in progress. BytesDone: %v",
					volumeBackup.Status.ProgressPercentage)
				// TODO: KDMP does not return size right now, we will need to fill up
				// size for volume once support is available
			} else if dataExport.Status.Status == kdmpapi.DataExportStatusFailed {
				vInfo.Status = storkapi.ApplicationBackupStatusFailed
				vInfo.Reason = fmt.Sprintf("Backup failed for volume: %v", dataExport.Status.Reason)
			} else {
				vInfo.Status = storkapi.ApplicationBackupStatusSuccessful
				vInfo.Reason = "Backup successful for volume"
				// delete kdmp crs
				if err := kdmpShedOps.Instance().DeleteDataExport(crName, backup.Namespace); err != nil {
					logrus.Warnf("failed to delete data export CR: %v", err)
				}
				// TODO: KDMP will take care of removing this
				if err := kdmpShedOps.Instance().DeleteVolumeBackup(crName, backup.Namespace); err != nil {
					logrus.Warnf("failed to delete volume backup CR: %v", err)
				}
			}
		}
		volumeInfos = append(volumeInfos, vInfo)
	}
	return volumeInfos, nil
}
func isBackupActive(status kdmpapi.ExportStatus) bool {
	if status.Stage == kdmpapi.DataExportStageTransferInProgress ||
		status.Status == kdmpapi.DataExportStatusInProgress ||
		status.Status == kdmpapi.DataExportStatusPending ||
		status.Status == kdmpapi.DataExportStatusInitial {
		return true
	}
	return false
}

func (k *kdmp) CancelBackup(backup *storkapi.ApplicationBackup) error {
	return fmt.Errorf("not supported")
}

func (k *kdmp) DeleteBackup(backup *storkapi.ApplicationBackup) error {
	for _, vInfo := range backup.Status.Volumes {
		if err := kdmpShedOps.Instance().DeleteDataExport(backup.Name+"-"+vInfo.Namespace+"-"+vInfo.PersistentVolumeClaim, backup.Namespace); err != nil {
			logrus.Errorf("failed to delete data export CR: %v", err)
			return err
		}
		if err := kdmpShedOps.Instance().DeleteVolumeBackup(vInfo.Volume, backup.Namespace); err != nil {
			logrus.Errorf("failed to delete volume backup CR: %v", err)
			return err
		}
	}
	return nil
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
		var pvc v1.PersistentVolumeClaim
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(object.UnstructuredContent(), &pvc); err != nil {
			continue
		}
		sc := k8shelper.GetPersistentVolumeClaimClass(&pvc)
		if val, ok := restore.Spec.StorageClassMapping[sc]; ok {
			pvc.Spec.StorageClassName = &val
			pvc.Spec.VolumeName = ""
		}
		if pvc.Annotations != nil {
			delete(pvc.Annotations, "pv.kubernetes.io/bind-completed")
			delete(pvc.Annotations, "pv.kubernetes.io/bound-by-controller")
		}
		o, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&pvc)
		if err != nil {
			logrus.Errorf("unable to pvc to unstruct objects, err: %v", err)
			return nil, err
		}
		object.SetUnstructuredContent(o)
		pvcs = append(pvcs, object)
	}
	return pvcs, nil
}

func (k *kdmp) StartRestore(
	restore *storkapi.ApplicationRestore,
	volumeBackupInfos []*storkapi.ApplicationBackupVolumeInfo,
) ([]*storkapi.ApplicationRestoreVolumeInfo, error) {
	log.ApplicationRestoreLog(restore).Debugf("started generic restore: %v", restore.Name)
	volumeInfos := make([]*storkapi.ApplicationRestoreVolumeInfo, 0)
	for _, backupVolumeInfo := range volumeBackupInfos {
		volumeInfo := &storkapi.ApplicationRestoreVolumeInfo{}
		volumeInfo.PersistentVolumeClaim = backupVolumeInfo.PersistentVolumeClaim
		// wait for pvc to get bound
		err := wait.ExponentialBackoff(volumeAPICallBackoff, func() (bool, error) {
			pvc, err := core.Instance().GetPersistentVolumeClaim(backupVolumeInfo.PersistentVolumeClaim, backupVolumeInfo.Namespace)
			if err != nil {
				return false, nil
			}
			if pvc.Spec.VolumeName == "" {
				return false, nil
			}
			volumeInfo.RestoreVolume = pvc.Spec.VolumeName
			return true, nil
		})
		if err != nil {
			logrus.Errorf("Failed to get volume restore volume:%s, err: %v", backupVolumeInfo.PersistentVolumeClaim, err)
			continue
		}
		if err != nil {
			return nil, err
		}
		volumeInfo.SourceNamespace = backupVolumeInfo.Namespace
		volumeInfo.SourceVolume = backupVolumeInfo.Volume
		volBackup := &kdmpapi.VolumeBackup{}
		volBackup.Annotations = make(map[string]string)
		volBackup.Annotations[skipResourceAnnotation] = "true"
		volBackup.Name = restore.Name + "-" + backupVolumeInfo.Namespace + "-" + backupVolumeInfo.PersistentVolumeClaim
		volBackup.Namespace = backupVolumeInfo.Namespace
		volBackup.Spec.BackupLocation = kdmpapi.DataExportObjectReference{
			Kind:       reflect.TypeOf(storkapi.BackupLocation{}).Name(),
			Name:       restore.Spec.BackupLocation,
			Namespace:  restore.Namespace,
			APIVersion: StorkAPIVersion,
		}
		volBackup.Spec.Repository = prefixRepo + backupVolumeInfo.Namespace + "-" + backupVolumeInfo.PersistentVolumeClaim + "/"
		volBackup.Status.SnapshotID = backupVolumeInfo.BackupID
		if _, err := kdmpShedOps.Instance().CreateVolumeBackup(volBackup); err != nil {
			logrus.Errorf("unable to create backup cr: %v", err)
			return nil, err
		}
		// create kdmp cr
		dataExport := &kdmpapi.DataExport{}
		dataExport.Annotations = make(map[string]string)
		dataExport.Annotations[skipResourceAnnotation] = "true"
		dataExport.Name = restore.Name + "-" + backupVolumeInfo.Namespace + "-" + backupVolumeInfo.PersistentVolumeClaim
		dataExport.Namespace = backupVolumeInfo.Namespace
		// TODO: this needs to be generic, maybe read it from configmap
		dataExport.Spec.Type = kdmpapi.DataExportKopia
		dataExport.Spec.Source = kdmpapi.DataExportObjectReference{
			Kind:       reflect.TypeOf(kdmpapi.VolumeBackup{}).Name(),
			Name:       volBackup.Name,
			Namespace:  volBackup.Namespace,
			APIVersion: "kdmp.portworx.com/v1alpha1",
		}
		dataExport.Spec.Destination = kdmpapi.DataExportObjectReference{
			Kind:       "PersistentVolumeClaim",
			Name:       backupVolumeInfo.PersistentVolumeClaim,
			Namespace:  backupVolumeInfo.Namespace,
			APIVersion: "v1",
		}
		if _, err := kdmpShedOps.Instance().CreateDataExport(dataExport); err != nil {
			logrus.Errorf("failed to create DataExport CR: %v", err)
			return volumeInfos, err
		}
		volumeInfo.DriverName = driverName
		volumeInfos = append(volumeInfos, volumeInfo)
	}
	return volumeInfos, nil
}

func (k *kdmp) CancelRestore(restore *storkapi.ApplicationRestore) error {
	for _, vInfo := range restore.Status.Volumes {
		crName := restore.Name + "-" + vInfo.SourceNamespace + "-" + vInfo.PersistentVolumeClaim

		if err := kdmpShedOps.Instance().DeleteDataExport(crName, vInfo.SourceNamespace); err != nil {
			logrus.Errorf("failed to delete data export CR: %v", err)
			return err
		}
		if err := kdmpShedOps.Instance().DeleteVolumeBackup(crName, vInfo.SourceNamespace); err != nil {
			logrus.Errorf("failed to delete volume backup CR: %v", err)
			return err
		}
	}
	return nil
}

func (k *kdmp) GetRestoreStatus(restore *storkapi.ApplicationRestore) ([]*storkapi.ApplicationRestoreVolumeInfo, error) {
	volumeInfos := make([]*storkapi.ApplicationRestoreVolumeInfo, 0)
	for _, vInfo := range restore.Status.Volumes {
		if vInfo.DriverName != driverName {
			continue
		}
		crName := restore.Name + "-" + restore.Namespace + "-" + vInfo.PersistentVolumeClaim
		dataExport, err := kdmpShedOps.Instance().GetDataExport(crName, restore.Namespace)
		if err != nil {
			logrus.Errorf("failed to get DataExport CR: %v", err)
			return volumeInfos, err
		}
		if dataExport.Status.TransferID == "" {
			vInfo.Status = storkapi.ApplicationRestoreStatusInitial
			vInfo.Reason = "Volume restore not started yet"
		} else {
			bkpCR := restore.Name + "-" + restore.Namespace + "-" + vInfo.PersistentVolumeClaim
			volumeBackup, err := kdmpShedOps.Instance().GetVolumeBackup(bkpCR, vInfo.SourceNamespace)
			if err != nil {
				logrus.Errorf("failed to get volumebackup CR: %s, err:%v", bkpCR, err)
				return volumeInfos, err
			}
			if isBackupActive(dataExport.Status) {
				vInfo.Status = storkapi.ApplicationRestoreStatusInProgress
				vInfo.Reason = fmt.Sprintf("Volume backup in progress. BytesDone: %v",
					volumeBackup.Status.ProgressPercentage)
				// TODO: KDMP does not return size right now, we will need to fill up
				// size for volume once support is available
			} else if dataExport.Status.Status == kdmpapi.DataExportStatusFailed {
				vInfo.Status = storkapi.ApplicationRestoreStatusFailed
				vInfo.Reason = fmt.Sprintf("Backup failed for volume: %v", dataExport.Status.Reason)
			} else {
				vInfo.Status = storkapi.ApplicationRestoreStatusSuccessful
				vInfo.Reason = "Backup successful for volume"
				// delete kdmp crs
				if err := kdmpShedOps.Instance().DeleteDataExport(crName, restore.Namespace); err != nil {
					logrus.Warnf("failed to delete data export CR: %v", err)
				}
				// TODO: KDMP will take care of removing this
				if err := kdmpShedOps.Instance().DeleteVolumeBackup(bkpCR, vInfo.SourceNamespace); err != nil {
					logrus.Warnf("failed to delete volume backup CR: %v", err)
				}
			}
		}
		volumeInfos = append(volumeInfos, vInfo)
	}
	logrus.Infof("volume infor generic: %v", volumeInfos)
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
	if err := storkvolume.Register(driverName, a); err != nil {
		logrus.Panicf("Error registering kdmp volume driver: %v", err)
	}
}
