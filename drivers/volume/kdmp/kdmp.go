package kdmp

import (
	"fmt"
	"reflect"
	"strings"

	snapv1 "github.com/kubernetes-incubator/external-storage/snapshot/pkg/apis/crd/v1"
	snapshotVolume "github.com/kubernetes-incubator/external-storage/snapshot/pkg/volume"
	storkvolume "github.com/libopenstorage/stork/drivers/volume"
	storkapi "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/libopenstorage/stork/pkg/errors"
	"github.com/libopenstorage/stork/pkg/log"
	kdmpapi "github.com/portworx/kdmp/pkg/apis/kdmp/v1alpha1"
	"github.com/portworx/sched-ops/k8s/core"
	kdmpShedOps "github.com/portworx/sched-ops/k8s/kdmp"
	"github.com/portworx/sched-ops/k8s/storage"
	"github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8shelper "k8s.io/kubernetes/pkg/apis/core/v1/helper"
)

const (
	// driverName is the name of the aws driver implementation
	driverName    = "kdmp"
	proxyEndpoint = "proxy_endpoint"
	proxyPath     = "proxy_nfs_exportpath"
	// StorkAPIVersion current api version supported by stork
	StorkAPIVersion = "stork.libopenstorage.org/v1alpha1"
)

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
		// We px csi volume fall back to default csi volume driver
		if pv.Spec.CSI.Driver == snapv1.PortworxCsiProvisionerName ||
			pv.Spec.CSI.Driver == snapv1.PortworxCsiDeprecatedProvisionerName {
			return false
		}
		log.PVLog(pv).Tracef("CSI Owns PV: %s", pv.Name)
		return true
	}

	return false
}

func (k *kdmp) StartBackup(backup *storkapi.ApplicationBackup,
	pvcs []v1.PersistentVolumeClaim,
) ([]*storkapi.ApplicationBackupVolumeInfo, error) {
	log.ApplicationBackupLog(backup).Debugf("started kdmp backup: %v", backup.Name)
	volumeInfos := make([]*storkapi.ApplicationBackupVolumeInfo, 0)
	for _, pvc := range pvcs {
		if pvc.DeletionTimestamp != nil {
			log.ApplicationBackupLog(backup).Warnf("Ignoring PVC %v which is being deleted", pvc.Name)
			continue
		}
		volumeInfo := &storkapi.ApplicationBackupVolumeInfo{}
		volumeInfo.PersistentVolumeClaim = pvc.Name
		volumeInfo.Namespace = pvc.Namespace
		volumeInfo.DriverName = driverName
		volume, err := core.Instance().GetVolumeForPersistentVolumeClaim(&pvc)
		if err != nil {
			return nil, fmt.Errorf("error getting volume for PVC: %v", err)
		}
		volumeInfo.Volume = volume
		// create kdmp cr

		dataExport := &kdmpapi.DataExport{}
		dataExport.Name = "stork-" + pvc.Name + "-" + pvc.Namespace
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
		logrus.Debugf("Source for KDMP: %v", dataExport.Spec.Source)
		logrus.Debugf("Destination for KDMP: %v", dataExport.Spec.Destination)
		// TODO: do we need retries here
		// in case no of CRs excedded limit handled by kdmp controller
		_, err = kdmpShedOps.Instance().CreateDataExport(dataExport)
		if err != nil {
			logrus.Errorf("failed to create job: %v", err)
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
		dataExport, err := kdmpShedOps.Instance().GetDataExport("stork-"+vInfo.PersistentVolumeClaim+"-"+vInfo.Namespace, backup.Namespace)
		if err != nil {
			logrus.Errorf("failed to get job: %v", err)
			return volumeInfos, err
		}
		if dataExport.Status.TransferID == "" {
			vInfo.Status = storkapi.ApplicationBackupStatusInitial
			vInfo.Reason = "Volume backup not started yet"
		} else {
			vInfo.Volume = strings.Split(dataExport.Status.TransferID, "/")[1]
			volumeBackup, err := kdmpShedOps.Instance().GetVolumeBackup(vInfo.Volume, backup.Namespace)
			if err != nil {
				logrus.Errorf("failed to get volumebackup CR: %s, err:%v", vInfo.Volume, err)
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
		if err := kdmpShedOps.Instance().DeleteDataExport("stork-"+vInfo.PersistentVolumeClaim+"-"+vInfo.Namespace, backup.Namespace); err != nil {
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
	*storkapi.ApplicationBackup,
	[]runtime.Unstructured,
) ([]runtime.Unstructured, error) {
	return nil, nil
}

func (k *kdmp) StartRestore(
	restore *storkapi.ApplicationRestore,
	volumeBackupInfos []*storkapi.ApplicationBackupVolumeInfo,
) ([]*storkapi.ApplicationRestoreVolumeInfo, error) {
	volumeInfos := make([]*storkapi.ApplicationRestoreVolumeInfo, 0)
	return volumeInfos, nil
}

func (k *kdmp) CancelRestore(*storkapi.ApplicationRestore) error {
	return nil
}

func (k *kdmp) GetRestoreStatus(restore *storkapi.ApplicationRestore) ([]*storkapi.ApplicationRestoreVolumeInfo, error) {
	volumeInfos := make([]*storkapi.ApplicationRestoreVolumeInfo, 0)
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
	err := a.Init(nil)
	if err != nil {
		logrus.Debugf("Error init'ing kdmp driver: %v", err)
	}
	if err := storkvolume.Register(driverName, a); err != nil {
		logrus.Panicf("Error registering kdmp volume driver: %v", err)
	}
}
