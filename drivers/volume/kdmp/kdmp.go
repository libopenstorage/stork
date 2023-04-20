package kdmp

import (
	"fmt"
	"os"
	"reflect"
	"strings"
	"time"

	"github.com/libopenstorage/stork/pkg/utils"

	snapv1 "github.com/kubernetes-incubator/external-storage/snapshot/pkg/apis/crd/v1"
	snapshotVolume "github.com/kubernetes-incubator/external-storage/snapshot/pkg/volume"
	stork_driver "github.com/libopenstorage/stork/drivers"
	storkvolume "github.com/libopenstorage/stork/drivers/volume"
	storkapi "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/libopenstorage/stork/pkg/errors"
	"github.com/libopenstorage/stork/pkg/k8sutils"
	"github.com/libopenstorage/stork/pkg/log"
	kdmpapi "github.com/portworx/kdmp/pkg/apis/kdmp/v1alpha1"
	"github.com/portworx/kdmp/pkg/controllers/dataexport"
	"github.com/portworx/kdmp/pkg/drivers"
	"github.com/portworx/kdmp/pkg/drivers/driversinstance"
	kdmputils "github.com/portworx/kdmp/pkg/drivers/utils"
	"github.com/portworx/sched-ops/k8s/batch"
	"github.com/portworx/sched-ops/k8s/core"
	kdmpShedOps "github.com/portworx/sched-ops/k8s/kdmp"
	"github.com/portworx/sched-ops/k8s/storage"
	storkops "github.com/portworx/sched-ops/k8s/stork"
	"github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	k8serror "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	k8shelper "k8s.io/component-helpers/storage/volume"
)

const (
	prefixRepo         = "generic-backup"
	prefixRestore      = "restore"
	prefixDelete       = "delete"
	volumeinitialDelay = 2 * time.Second
	volumeFactor       = 1.5
	volumeSteps        = 20
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
	// backupUID annotation key
	backupUIDKey           = "portworx.io/backup-uid"
	pxRestoreObjectUIDKey  = utils.PxbackupAnnotationPrefix + "restore-uid"
	pxRestoreObjectNameKey = utils.PxbackupAnnotationPrefix + "restore-name"

	//kdmp related labels
	// restore related Labels
	applicationRestoreCRNameKey = utils.KdmpAnnotationPrefix + "applicationrestore-cr-name"
	applicationRestoreCRUIDKey  = utils.KdmpAnnotationPrefix + "applicationrestore-cr-uid"
	restoreObjectNameKey        = utils.KdmpAnnotationPrefix + "restoreobject-name"
	restoreObjectUIDKey         = utils.KdmpAnnotationPrefix + "restoreobject-uid"

	pvcNameKey = utils.KdmpAnnotationPrefix + "pvc-name"
	pvcUIDKey  = utils.KdmpAnnotationPrefix + "pvc-uid"
	// pvcProvisionerAnnotation is the annotation on PVC which has the
	// provisioner name
	pvcProvisionerAnnotation = "volume.beta.kubernetes.io/storage-provisioner"
	// pvProvisionedByAnnotation is the annotation on PV which has the
	// provisioner name
	pvProvisionedByAnnotation = "pv.kubernetes.io/provisioned-by"
	pureCSIProvisioner        = "pure-csi"
	bindCompletedKey          = "pv.kubernetes.io/bind-completed"
	boundByControllerKey      = "pv.kubernetes.io/bound-by-controller"
	storageClassKey           = "volume.beta.kubernetes.io/storage-class"
	storageProvisioner        = "volume.beta.kubernetes.io/storage-provisioner"
	storageNodeAnnotation     = "volume.kubernetes.io/selected-node"
	gkeNodeLabelKey           = "topology.gke.io/zone"
	awsNodeLabelKey           = "alpha.eksctl.io/cluster-name"
	ocpAWSNodeLabelKey        = "topology.ebs.csi.aws.com/zone"
)

var volumeAPICallBackoff = wait.Backoff{
	Duration: volumeinitialDelay,
	Factor:   volumeFactor,
	Steps:    volumeSteps,
}

var (
	nonSupportedProvider = false
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
	return storkvolume.KDMPDriverName
}

func (k *kdmp) Stop() error {
	return nil
}

func (k *kdmp) OwnsPVCForBackup(coreOps core.Ops, pvc *v1.PersistentVolumeClaim, cmBackupType string, crBackupType string, blType storkapi.BackupLocationType) bool {
	// KDMP can handle any PVC type. KDMP driver will always be a fallback
	// option when none of the other supported drivers by stork own the PVC
	return true
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
	name := fmt.Sprintf("%s-%s-%s-%s", opsPrefix, utils.GetShortUID(crUID), utils.GetShortUID(pvcUID), ns)
	name = utils.GetValidLabel(name)
	return name
}

func getProvisionerName(pvc v1.PersistentVolumeClaim) string {
	provisioner := ""
	if val, ok := pvc.Annotations[pvcProvisionerAnnotation]; ok {
		provisioner = val
	}
	if len(provisioner) != 0 {
		return provisioner
	}
	pv, err := core.Instance().GetPersistentVolume(pvc.Spec.VolumeName)
	if err != nil {
		logrus.Warnf("error getting pv for pvc [%v/%v]: %v", pvc.Namespace, pvc.Name, err)
		return provisioner
	}
	if val, ok := pv.Annotations[pvProvisionedByAnnotation]; ok {
		provisioner = val
	}
	return provisioner
}

func isCSISnapshotClassRequired(pvc *v1.PersistentVolumeClaim) bool {
	pv, err := core.Instance().GetPersistentVolume(pvc.Spec.VolumeName)
	if err != nil {
		errMsg := fmt.Sprintf("error getting pv %v for pvc %v: %v", pvc.Spec.VolumeName, pvc.Name, err)
		logrus.Warnf("ifCSISnapshotSupported: %v", errMsg)
		// In the case errors, we will allow the kdmp controller csi steps to decide on snapshot support.
		return true
	}
	return !storkvolume.IsCSIDriverWithoutSnapshotSupport(pv)
}

func getZones(pv *v1.PersistentVolume) ([]string, error) {
	// Check the cloud provider
	gkeCluster, awsCluster, err := getCloudProvider()
	if err != nil {
		return nil, err
	}
	if !gkeCluster && !awsCluster {
		// Not supported cluster
		// For azure we want to skip this where multi zone is not an issue.
		nonSupportedProvider = true
		return nil, nil
	}

	if gkeCluster {
		zones := storkvolume.GetGCPZones(pv)
		return zones, nil
	}

	if awsCluster {
		// Zone are supported only for EBS provisioner
		if pv.Annotations[pvProvisionedByAnnotation] == storkvolume.EbsProvisionerName {
			ebsName := storkvolume.GetEBSVolumeID(pv)
			if ebsName == "" {
				return nil, fmt.Errorf("AWS EBS info not found in PV %v", pv.Name)
			}

			client, err := storkvolume.GetAWSClient()
			if err != nil {
				return nil, err
			}
			ebsVolume, err := storkvolume.GetEBSVolume(ebsName, nil, client)
			if err != nil {
				return nil, err
			}
			logrus.Tracef("getZones: zone: %v", *ebsVolume.AvailabilityZone)

			return []string{*ebsVolume.AvailabilityZone}, nil
		}
	}

	return nil, nil
}

func getCloudProvider() (bool, bool, error) {
	fn := "getCloudProvider"
	gkeCluster := false
	awsCluster := false
	nodes, err := core.Instance().GetNodes()
	if err != nil {
		logrus.Errorf("%s: getting node details failed: %v", fn, err)
		return gkeCluster, awsCluster, err
	}
	for _, node := range nodes.Items {
		if node.Labels[gkeNodeLabelKey] != "" {
			gkeCluster = true
			break
		} else if node.Labels[awsNodeLabelKey] != "" || node.Labels[ocpAWSNodeLabelKey] != "" {
			awsCluster = true
			break
		}
	}

	return gkeCluster, awsCluster, nil
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
		pvName, err := core.Instance().GetVolumeForPersistentVolumeClaim(&pvc)
		if err != nil {
			return nil, fmt.Errorf("error getting PV name for PVC (%v/%v): %v", pvc.Namespace, pvc.Name, err)
		}
		pv, err := core.Instance().GetPersistentVolume(pvName)
		if err != nil {
			return nil, fmt.Errorf("error getting pv %v: %v", pvName, err)
		}
		volumeInfo := &storkapi.ApplicationBackupVolumeInfo{}
		zones, err := getZones(pv)
		if err != nil {
			return nil, fmt.Errorf("error fetching zone information: %v", err)
		}
		if len(zones) != 0 {
			volumeInfo.Zones = zones
		}
		logrus.Infof("volumeInfo zone: %v - pv name %v", volumeInfo.Zones, pv.Name)
		volumeInfo.Provisioner = getProvisionerName(pvc)
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
		labels[utils.ApplicationBackupCRNameKey] = utils.GetValidLabel(backup.Name)
		labels[utils.ApplicationBackupCRUIDKey] = utils.GetValidLabel(utils.GetShortUID(string(backup.UID)))
		labels[pvcNameKey] = utils.GetValidLabel(pvc.Name)
		labels[pvcUIDKey] = utils.GetValidLabel(utils.GetShortUID(string(pvc.UID)))
		// If backup from px-backup, update the backup object details in the label
		if val, ok := backup.Annotations[utils.PxbackupAnnotationCreateByKey]; ok {
			if val == utils.PxbackupAnnotationCreateByValue {
				labels[utils.BackupObjectNameKey] = utils.GetValidLabel(backup.Annotations[utils.PxbackupObjectNameKey])
				labels[utils.BackupObjectUIDKey] = utils.GetValidLabel(backup.Annotations[utils.PxbackupObjectUIDKey])
			}
		}

		dataExport.Labels = labels
		dataExport.Spec.TriggeredFrom = kdmputils.TriggeredFromStork
		storkPodNs, err := k8sutils.GetStorkPodNamespace()
		if err != nil {
			logrus.Errorf("error in getting stork pod namespace: %v", err)
			return nil, err
		}
		dataExport.Spec.TriggeredFromNs = storkPodNs
		dataExport.Annotations = make(map[string]string)
		dataExport.Annotations[utils.SkipResourceAnnotation] = "true"
		dataExport.Annotations[utils.BackupObjectUIDKey] = string(backup.Annotations[utils.PxbackupObjectUIDKey])
		dataExport.Annotations[pvcUIDKey] = string(pvc.UID)
		dataExport.Name = getGenericCRName(utils.PrefixBackup, string(backup.UID), string(pvc.UID), pvc.Namespace)
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
		snapshotClassRequired := isCSISnapshotClassRequired(&pvc)
		if snapshotClassRequired {
			dataExport.Spec.SnapshotStorageClass = k.getSnapshotClassName(backup)
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
		if vInfo.DriverName != storkvolume.KDMPDriverName {
			continue
		}
		crName := getGenericCRName(utils.PrefixBackup, string(backup.UID), vInfo.PersistentVolumeClaimUID, vInfo.Namespace)
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
				if len(dataExport.Status.VolumeSnapshot) == 0 {
					vInfo.VolumeSnapshot = ""
				} else {
					volumeSnapshot := k.getSnapshotClassName(backup)
					if len(volumeSnapshot) > 0 {
						vInfo.VolumeSnapshot = fmt.Sprintf("%s,%s", volumeSnapshot, dataExport.Status.VolumeSnapshot)
					}
				}
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
		crName := getGenericCRName(utils.PrefixBackup, string(backup.UID), vInfo.PersistentVolumeClaimUID, vInfo.Namespace)
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
	if val, ok := backup.Annotations[utils.PxbackupAnnotationCreateByKey]; !ok {
		return deleteKdmpSnapshot(backup)
	} else if val != utils.PxbackupAnnotationCreateByValue {
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
				labels[utils.ApplicationBackupCRNameKey] = utils.GetValidLabel(backup.Name)
				labels[utils.ApplicationBackupCRUIDKey] = utils.GetValidLabel(string(backup.UID))
				labels[pvcNameKey] = utils.GetValidLabel(vInfo.PersistentVolumeClaim)
				labels[pvcUIDKey] = utils.GetValidLabel(vInfo.PersistentVolumeClaimUID)
				// If backup from px-backup, update the backup object details in the label
				if val, ok := backup.Annotations[utils.PxbackupAnnotationCreateByKey]; ok {
					if val == utils.PxbackupAnnotationCreateByValue {
						labels[utils.BackupObjectNameKey] = utils.GetValidLabel(backup.Annotations[utils.PxbackupObjectNameKey])
						labels[utils.BackupObjectUIDKey] = utils.GetValidLabel(backup.Annotations[utils.PxbackupObjectUIDKey])
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
					drivers.WithJobConfigMap(stork_driver.KdmpConfigmapName),
					drivers.WithJobConfigMapNs(stork_driver.KdmpConfigmapNamespace),
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
	vInfo *storkapi.ApplicationRestoreVolumeInfo,
	namespaceMapping map[string]string,
) (*v1.PersistentVolume, error) {
	return pv, nil
}

func (k *kdmp) GetPreRestoreResources(
	backup *storkapi.ApplicationBackup,
	restore *storkapi.ApplicationRestore,
	objects []runtime.Unstructured,
	storageClassBytes []byte,
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
			}
			// If pvc storageClassName is empty, we want to pick up the
			// default storageclass configured on the cluster.
			// Default storage class will not selected, if the storageclass
			// is empty. So setting it to nil.
			if pvc.Spec.StorageClassName != nil {
				if len(*pvc.Spec.StorageClassName) == 0 {
					pvc.Spec.StorageClassName = nil
				}
			}
			pvc.Spec.VolumeName = ""
			pvc.Spec.DataSource = nil
			if pvc.Annotations != nil {
				delete(pvc.Annotations, bindCompletedKey)
				delete(pvc.Annotations, boundByControllerKey)
				delete(pvc.Annotations, storageClassKey)
				delete(pvc.Annotations, k8shelper.AnnBetaStorageProvisioner)
				delete(pvc.Annotations, k8shelper.AnnStorageProvisioner)
				delete(pvc.Annotations, storageNodeAnnotation)
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
	objects []runtime.Unstructured,
) ([]*storkapi.ApplicationRestoreVolumeInfo, error) {
	funct := "kdmp.StartRestore"
	log.ApplicationRestoreLog(restore).Debugf("started generic restore: %v", restore.Name)
	volumeInfos := make([]*storkapi.ApplicationRestoreVolumeInfo, 0)
	nodes, err := core.Instance().GetNodes()
	if err != nil {
		return nil, fmt.Errorf("failed in getting the nodes: %v", err)
	}
	// Support only for gke and aws
	zoneMap := map[string]string{}
	splitDestRegion := []string{}
	gkeCluster, awsCluster, err := getCloudProvider()
	if err != nil {
		return nil, err
	}
	if !gkeCluster && !awsCluster {
		// Not supported cluster
		nonSupportedProvider = true
	}
	if !nonSupportedProvider {
		nodeZoneList, err := storkvolume.GetNodeZones()
		if err != nil {
			return nil, err
		}
		backupZoneList := storkvolume.GetVolumeBackupZones(volumeBackupInfos)
		zoneMap = storkvolume.MapZones(backupZoneList, nodeZoneList)
		nodeZone, err := storkvolume.GetNodeZone()
		if err != nil {
			return nil, err
		}
		splitDestRegion = strings.Split(nodeZone, "-")
	}

	for _, bkpvInfo := range volumeBackupInfos {
		var destFullZoneName string
		volumeInfo := &storkapi.ApplicationRestoreVolumeInfo{}
		// zone is only applicable for EBS, skip for EFS
		if len(bkpvInfo.Zones) == 0 {
			nonSupportedProvider = true
		}
		if !nonSupportedProvider {
			destZoneName := strings.Split(bkpvInfo.Zones[0], "-")
			splitDestRegion[2] = zoneMap[destZoneName[2]]
			destFullZoneName = strings.Join(splitDestRegion, "-")
			volumeInfo.Zones = append(volumeInfo.Zones, destFullZoneName)
		}
		val, ok := restore.Spec.NamespaceMapping[bkpvInfo.Namespace]
		if !ok {
			return nil, fmt.Errorf("restore namespace mapping not found: %s", bkpvInfo.Namespace)

		}
		restoreNamespace := val
		pvc := &v1.PersistentVolumeClaim{}

		if objects != nil {
			// get corresponding pvc object from objects list
			pvc, err = storkvolume.GetPVCFromObjects(objects, bkpvInfo)
			if err != nil {
				return nil, err
			}
			if !nonSupportedProvider {
				for _, node := range nodes.Items {
					zone := node.Labels[v1.LabelTopologyZone]
					if zone == destFullZoneName {
						pvc.Annotations[storageNodeAnnotation] = node.Name
					}
				}
			}
			pvc.Namespace = restoreNamespace
		} else {
			pvc.Name = bkpvInfo.PersistentVolumeClaim
			pvc.Namespace = restoreNamespace
		}
		volumeInfo.PersistentVolumeClaim = bkpvInfo.PersistentVolumeClaim
		volumeInfo.PersistentVolumeClaimUID = bkpvInfo.PersistentVolumeClaimUID
		volumeInfo.SourceNamespace = bkpvInfo.Namespace
		volumeInfo.SourceVolume = bkpvInfo.Volume
		volumeInfo.DriverName = storkvolume.KDMPDriverName

		// create VolumeBackup CR
		// Adding required label for debugging
		labels := make(map[string]string)
		labels[applicationRestoreCRNameKey] = utils.GetValidLabel(restore.Name)
		labels[applicationRestoreCRUIDKey] = utils.GetValidLabel(string(restore.UID))
		labels[pvcNameKey] = utils.GetValidLabel(bkpvInfo.PersistentVolumeClaim)
		labels[pvcUIDKey] = utils.GetValidLabel(bkpvInfo.PersistentVolumeClaimUID)
		// If restorefrom px-backup, update the restore object details in the label
		if val, ok := restore.Annotations[utils.PxbackupAnnotationCreateByKey]; ok {
			if val == utils.PxbackupAnnotationCreateByValue {
				labels[restoreObjectNameKey] = utils.GetValidLabel(restore.Annotations[utils.PxbackupObjectNameKey])
				labels[restoreObjectUIDKey] = utils.GetValidLabel(restore.Annotations[utils.PxbackupObjectUIDKey])
			}
		}
		volBackup := &kdmpapi.VolumeBackup{}
		volBackup.Labels = labels
		volBackup.Annotations = make(map[string]string)
		volBackup.Annotations[utils.SkipResourceAnnotation] = "true"
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

		backup, err := storkops.Instance().GetApplicationBackup(restore.Spec.BackupName, restore.Namespace)
		if err != nil {
			return nil, fmt.Errorf("unable to get applicationbackup cr %s/%s: %v", restore.Namespace, restore.Spec.BackupName, err)
		}
		var backupUID string
		if _, ok := backup.Annotations[backupUIDKey]; !ok {
			msg := fmt.Sprintf("unable to find backup uid from applicationbackup %s/%s", restore.Namespace, restore.Spec.BackupName)
			return nil, fmt.Errorf(msg)
		}
		backupUID = backup.Annotations[backupUIDKey]
		// create kdmp cr
		dataExport := &kdmpapi.DataExport{}
		dataExport.Labels = labels
		dataExport.Spec.TriggeredFrom = kdmputils.TriggeredFromStork
		storkPodNs, err := k8sutils.GetStorkPodNamespace()
		if err != nil {
			logrus.Errorf("error in getting stork pod namespace: %v", err)
			return nil, err
		}
		dataExport.Spec.TriggeredFromNs = storkPodNs
		dataExport.Annotations = make(map[string]string)
		dataExport.Annotations[utils.SkipResourceAnnotation] = "true"
		dataExport.Annotations[utils.BackupObjectUIDKey] = backupUID
		dataExport.Annotations[pvcUIDKey] = bkpvInfo.PersistentVolumeClaimUID
		dataExport.Name = getGenericCRName(prefixRestore, string(restore.UID), bkpvInfo.PersistentVolumeClaimUID, restoreNamespace)
		dataExport.Namespace = restoreNamespace
		dataExport.Status.TransferID = volBackup.Namespace + "/" + volBackup.Name
		dataExport.Status.RestorePVC = pvc
		dataExport.Status.LocalSnapshotRestore = k.doLocalRestore(restore, bkpvInfo)
		dataExport.Spec.Type = kdmpapi.DataExportKopia
		if dataExport.Status.LocalSnapshotRestore {
			dataExport.Spec.SnapshotStorageClass = getVolumeSnapshotClassFromBackupVolumeInfo(bkpvInfo)
		}
		dataExport.Spec.Source = kdmpapi.DataExportObjectReference{
			Kind:       reflect.TypeOf(kdmpapi.VolumeBackup{}).Name(),
			Name:       volBackup.Name,
			Namespace:  volBackup.Namespace,
			APIVersion: KdmpAPIVersion,
		}
		dataExport.Spec.Destination = kdmpapi.DataExportObjectReference{
			Kind:       PVCKind,
			Name:       pvc.Name,
			Namespace:  restoreNamespace,
			APIVersion: "v1",
		}
		logrus.Tracef("%s de cr name [%v/%v]", funct, dataExport.Namespace, dataExport.Name)
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
		if vInfo.Status == storkapi.ApplicationRestoreStatusSuccessful || vInfo.Status == storkapi.ApplicationRestoreStatusFailed || vInfo.Status == storkapi.ApplicationRestoreStatusRetained {
			volumeInfos = append(volumeInfos, vInfo)
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

		if dataExport.Status.Status == kdmpapi.DataExportStatusFailed &&
			dataExport.Status.Stage == kdmpapi.DataExportStageFinal {
			vInfo.Status = storkapi.ApplicationRestoreStatusFailed
			vInfo.Reason = fmt.Sprintf("restore failed for volume:%s reason: %s", vInfo.SourceVolume, dataExport.Status.Reason)
			volumeInfos = append(volumeInfos, vInfo)
			continue
		}
		if dataExport.Status.TransferID == "" {
			vInfo.Status = storkapi.ApplicationRestoreStatusInitial
			vInfo.Reason = "Volume restore not started yet"
		} else {
			if isDataExportActive(dataExport.Status) {
				vInfo.Status = storkapi.ApplicationRestoreStatusInProgress
				vInfo.Reason = "Volume restore is in progress. BytesDone"
			} else if isDataExportCompleted(dataExport.Status) {
				restoredPVC, err := core.Instance().GetPersistentVolumeClaim(dataExport.Status.RestorePVC.Name, dataExport.Status.RestorePVC.Namespace)
				if err != nil {
					logrus.Errorf("failed to get pvc object for %s/%s: %v", dataExport.Status.RestorePVC.Namespace, dataExport.Status.RestorePVC.Name, err)
					vInfo.Status = storkapi.ApplicationRestoreStatusFailed
					vInfo.Reason = fmt.Sprintf("failed to get pvc object for %s/%s", dataExport.Status.RestorePVC.Namespace, dataExport.Status.RestorePVC.Name)
					volumeInfos = append(volumeInfos, vInfo)
					continue
				}
				vInfo.Status = storkapi.ApplicationRestoreStatusSuccessful
				vInfo.Reason = "restore successful for volume"
				vInfo.TotalSize = dataExport.Status.Size
				vInfo.RestoreVolume = restoredPVC.Spec.VolumeName
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

func (k *kdmp) GetPodVolumes(podSpec *v1.PodSpec, namespace string, includePendingWFFC bool) ([]*storkvolume.Info, []*storkvolume.Info, error) {
	return nil, nil, &errors.ErrNotSupported{}
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

func (k *kdmp) doLocalRestore(restore *storkapi.ApplicationRestore, bkpvInfo *storkapi.ApplicationBackupVolumeInfo) bool {
	// without snapshotting case
	if bkpvInfo.VolumeSnapshot == "" {
		return false
	}

	if scName, ok := restore.Spec.StorageClassMapping[bkpvInfo.StorageClass]; ok {
		sc, err := storage.Instance().GetStorageClass(scName)
		if err != nil {
			return false
		}
		// If provisioners are not same , do not try for restore from localsnapshot
		if sc.Provisioner != bkpvInfo.Provisioner {
			return false
		}
	}
	return true
}

// CleanupBackupResources for specified backup
func (k *kdmp) CleanupBackupResources(backup *storkapi.ApplicationBackup) error {
	// delete data export crs once backup is completed
	for _, vInfo := range backup.Status.Volumes {
		if vInfo.DriverName != storkvolume.KDMPDriverName {
			continue
		}
		crName := getGenericCRName(utils.PrefixBackup, string(backup.UID), vInfo.PersistentVolumeClaimUID, vInfo.Namespace)
		logrus.Infof("deleting data export CR: %s%s", vInfo.Namespace, crName)
		de, err := kdmpShedOps.Instance().GetDataExport(crName, vInfo.Namespace)
		if err != nil && !k8serror.IsNotFound(err) {
			logrus.Tracef("failed in getting data export CR: %v", err)
			return err
		}
		if de.DeletionTimestamp == nil {
			// delete kdmp crs
			if err := kdmpShedOps.Instance().DeleteDataExport(crName, vInfo.Namespace); err != nil && !k8serror.IsNotFound(err) {
				logrus.Tracef("failed to delete data export CR: %v", err)
				return err
			}
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
		crName := getGenericCRName(prefixRestore, string(restore.UID), vInfo.PersistentVolumeClaimUID, restoreNamespace)
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

// GetPodPatches returns driver-specific json patches to mutate the pod in a webhook
func (k *kdmp) GetPodPatches(podNamespace string, pod *v1.Pod) ([]k8sutils.JSONPatchOp, error) {
	return nil, nil
}

// GetCSIPodPrefix returns prefix for the csi pod names in the deployment
func (a *kdmp) GetCSIPodPrefix() (string, error) {
	return "", &errors.ErrNotSupported{}
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

// getVolumeSnapshotClassFromBackupVolumeInfo returns the volumesnapshotclass if it is present
func getVolumeSnapshotClassFromBackupVolumeInfo(bkvpInfo *storkapi.ApplicationBackupVolumeInfo) string {
	var vsClass string
	if bkvpInfo.VolumeSnapshot == "" {
		return vsClass
	}
	subStrings := strings.Split(bkvpInfo.VolumeSnapshot, ",")
	vsClass = strings.Join(subStrings[0:len(subStrings)-1], ",")

	return vsClass
}
