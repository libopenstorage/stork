package kdmp

import (
	"fmt"
	"os"
	"reflect"
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
	driverName             = "kdmp"
	prefixRepo             = "generic-backup"
	prefixRestore          = "restore"
	proxyEndpoint          = "proxy_endpoint"
	proxyPath              = "proxy_nfs_exportpath"
	skipResourceAnnotation = "stork.libopenstorage.org/skip-resource"
	volumeinitialDelay     = 2 * time.Second
	volumeFactor           = 1.5
	volumeSteps            = 20
	// StorkAPIVersion current api version supported by stork
	StorkAPIVersion = "stork.libopenstorage.org/v1alpha1"
	// KdmpAPIVersion current api version supported by KDMP
	KdmpAPIVersion = "kdmp.portworx.com/v1alpha1"
	// PVCKind constant for pvc
	PVCKind = "PersistentVolumeClaim"
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
		log.PVLog(pv).Tracef("KDMP (csi) owns PV: %s", pv.Name)
		return true
	}

	return false
}

func getGenericCRName(prefix, ns, name string) string {
	return fmt.Sprintf("%s-%s-%s", prefix, ns, name)
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
		dataExport.Name = getGenericCRName(backup.Name, pvc.Namespace, pvc.Name)
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
		if vInfo.DriverName != driverName {
			continue
		}
		crName := getGenericCRName(backup.Name, vInfo.Namespace, vInfo.PersistentVolumeClaim)
		dataExport, err := kdmpShedOps.Instance().GetDataExport(crName, backup.Namespace)
		if err != nil {
			logrus.Errorf("failed to get DataExport CR: %v", err)
			return volumeInfos, err
		}
		if dataExport.Status.TransferID == "" {
			vInfo.Status = storkapi.ApplicationBackupStatusInitial
			vInfo.Reason = "Volume backup not started yet"
		} else {
			vInfo.BackupID = dataExport.Status.SnapshotID
			if isDataExportActive(dataExport.Status) {
				vInfo.Status = storkapi.ApplicationBackupStatusInProgress
				vInfo.Reason = "Volume backup in progress"
				// TODO: KDMP does not return size right now, we will need to fill up
				// size for volume once support is available
			} else if dataExport.Status.Status == kdmpapi.DataExportStatusFailed {
				vInfo.Status = storkapi.ApplicationBackupStatusFailed
				vInfo.Reason = fmt.Sprintf("Backup failed for volume: %v", dataExport.Status.Reason)
			} else if isDataExportCompleted(dataExport.Status) {
				vInfo.Status = storkapi.ApplicationBackupStatusSuccessful
				vInfo.Reason = "Backup successful for volume"
				// delete kdmp crs
				if err := kdmpShedOps.Instance().DeleteDataExport(crName, backup.Namespace); err != nil {
					logrus.Warnf("failed to delete data export CR: %v", err)
				}
			}
		}
		volumeInfos = append(volumeInfos, vInfo)
	}
	return volumeInfos, nil
}
func isDataExportActive(status kdmpapi.ExportStatus) bool {
	if status.Stage == kdmpapi.DataExportStageTransferInProgress ||
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
	return k.DeleteBackup(backup)
}

func (k *kdmp) DeleteBackup(backup *storkapi.ApplicationBackup) error {
	for _, vInfo := range backup.Status.Volumes {
		crName := getGenericCRName(backup.Name, vInfo.Namespace, vInfo.PersistentVolumeClaim)
		if err := kdmpShedOps.Instance().DeleteDataExport(crName, vInfo.Namespace); err != nil {
			logrus.Warnf("failed to delete data export CR: %v", err)
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
			logrus.Errorf("unable to convert pvc to unstruct objects, err: %v", err)
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
	for _, bkpvInfo := range volumeBackupInfos {
		volumeInfo := &storkapi.ApplicationRestoreVolumeInfo{}
		// wait for pvc to get bound
		err := wait.ExponentialBackoff(volumeAPICallBackoff, func() (bool, error) {
			pvc, err := core.Instance().GetPersistentVolumeClaim(bkpvInfo.PersistentVolumeClaim, bkpvInfo.Namespace)
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
			logrus.Errorf("Failed to get volume restore volume: %s, err: %v", bkpvInfo.PersistentVolumeClaim, err)
			continue
		}
		if err != nil {
			return nil, err
		}
		volumeInfo.PersistentVolumeClaim = bkpvInfo.PersistentVolumeClaim
		volumeInfo.SourceNamespace = bkpvInfo.Namespace
		volumeInfo.SourceVolume = bkpvInfo.Volume
		volumeInfo.DriverName = driverName

		// create VolumeBackup CR
		volBackup := &kdmpapi.VolumeBackup{}
		volBackup.Annotations = make(map[string]string)
		volBackup.Annotations[skipResourceAnnotation] = "true"
		volBackup.Name = getGenericCRName(prefixRestore+"-"+restore.Name, bkpvInfo.Namespace, bkpvInfo.PersistentVolumeClaim)
		volBackup.Namespace = bkpvInfo.Namespace
		volBackup.Spec.BackupLocation = kdmpapi.DataExportObjectReference{
			Kind:       reflect.TypeOf(storkapi.BackupLocation{}).Name(),
			Name:       restore.Spec.BackupLocation,
			Namespace:  restore.Namespace,
			APIVersion: StorkAPIVersion,
		}
		volBackup.Spec.Repository = fmt.Sprintf("%s/%s-%s/", prefixRepo, bkpvInfo.Namespace, bkpvInfo.PersistentVolumeClaim)
		volBackup.Status.SnapshotID = bkpvInfo.BackupID
		if _, err := kdmpShedOps.Instance().CreateVolumeBackup(volBackup); err != nil {
			logrus.Errorf("unable to create backup cr: %v", err)
			return nil, err
		}

		// create kdmp cr
		dataExport := &kdmpapi.DataExport{}
		dataExport.Annotations = make(map[string]string)
		dataExport.Annotations[skipResourceAnnotation] = "true"
		dataExport.Name = getGenericCRName(restore.Name, bkpvInfo.Namespace, bkpvInfo.PersistentVolumeClaim)
		dataExport.Namespace = bkpvInfo.Namespace
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
			Namespace:  bkpvInfo.Namespace,
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
		crName := getGenericCRName(restore.Name, vInfo.SourceNamespace, vInfo.PersistentVolumeClaim)
		if err := kdmpShedOps.Instance().DeleteDataExport(crName, vInfo.SourceNamespace); err != nil {
			logrus.Errorf("failed to delete data export CR: %v", err)
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
		crName := getGenericCRName(restore.Name, vInfo.SourceNamespace, vInfo.PersistentVolumeClaim)
		dataExport, err := kdmpShedOps.Instance().GetDataExport(crName, vInfo.SourceNamespace)
		if err != nil {
			logrus.Errorf("failed to get DataExport CR: %v", err)
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
				// delete kdmp crs
				if err := kdmpShedOps.Instance().DeleteDataExport(crName, vInfo.SourceNamespace); err != nil {
					logrus.Warnf("failed to delete data export CR:%s/%s, err: %v", vInfo.SourceNamespace, crName, err)
				}
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
