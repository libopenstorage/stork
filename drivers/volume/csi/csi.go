package csi

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"time"

	kSnapshotv1beta1 "github.com/kubernetes-csi/external-snapshotter/v2/pkg/apis/volumesnapshot/v1beta1"
	kSnapshotClient "github.com/kubernetes-csi/external-snapshotter/v2/pkg/client/clientset/versioned"
	snapv1 "github.com/kubernetes-incubator/external-storage/snapshot/pkg/apis/crd/v1"
	snapshotVolume "github.com/kubernetes-incubator/external-storage/snapshot/pkg/volume"
	storkvolume "github.com/libopenstorage/stork/drivers/volume"
	storkapi "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/libopenstorage/stork/pkg/applicationmanager/controllers"
	"github.com/libopenstorage/stork/pkg/crypto"
	"github.com/libopenstorage/stork/pkg/errors"
	"github.com/libopenstorage/stork/pkg/log"
	"github.com/libopenstorage/stork/pkg/objectstore"
	"github.com/portworx/sched-ops/k8s/core"
	storkops "github.com/portworx/sched-ops/k8s/stork"
	"github.com/sirupsen/logrus"
	"gocloud.dev/gcerrors"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	k8s_errors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
)

const (
	// storkDriverName is the name of the k8s driver implementation.
	// not to be confused with a CSI Driver Name
	storkCSIDriverName = "csi"
	// snapshotPrefix is appended to CSI backup snapshot
	snapshotPrefix = "snapshot"
	// snapshotClassNamePrefix is the prefix for snapshot classes per CSI driver
	snapshotClassNamePrefix = "stork-csi-snapshot-class-"

	// snapshotObjectName is the object stored for the volumesnapshot
	snapshotObjectName = "snapshots.json"
	// storageClassesObjectName is the object stored for storageclasses
	storageClassesObjectName = "storageclasses.json"
	// resourcesObjectName is the object stored for the all backup resources
	resourcesObjectName = "resources.json"

	// optCSIDriverName is an option for storing which CSI Driver a volumesnapshot was created with
	optCSIDriverName = "csi-driver-name"
	// optCSISnapshotClassName is an option for providing a snapshot class name
	optCSISnapshotClassName = "stork.libopenstorage.org/csi-snapshot-class-name"
	// optVolumeSnapshotContentName is used for recording which vsc to check has been deleted
	optVolumeSnapshotContentName = "volumesnapshotcontent-name"

	// annSnapshotClassStorkOwned is an option for providing a snapshot class name
	annPVBindCompleted     = "pv.kubernetes.io/bind-completed"
	annPVBoundByController = "pv.kubernetes.io/bound-by-controller"

	// snapshotTimeout represents the duration to wait before timing out on snapshot completion
	snapshotTimeout = time.Minute * 5
	// restoreTimeout is the duration to wait before timing out the restore
	restoreTimeout = time.Minute * 5
)

// csiBackupObject represents a backup of a series of CSI objects
type csiBackupObject struct {
	VolumeSnapshots        map[string]*kSnapshotv1beta1.VolumeSnapshot        `json:"volumeSnapshots"`
	VolumeSnapshotContents map[string]*kSnapshotv1beta1.VolumeSnapshotContent `json:"volumeSnapshotContents"`
	VolumeSnapshotClasses  map[string]*kSnapshotv1beta1.VolumeSnapshotClass   `json:"volumeSnapshotClasses"`
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

type csi struct {
	snapshotClient *kSnapshotClient.Clientset

	storkvolume.ClusterPairNotSupported
	storkvolume.MigrationNotSupported
	storkvolume.GroupSnapshotNotSupported
	storkvolume.ClusterDomainsNotSupported
	storkvolume.CloneNotSupported
	storkvolume.SnapshotRestoreNotSupported
}

func (c *csi) Init(_ interface{}) error {
	config, err := rest.InClusterConfig()
	if err != nil {
		return err
	}

	cs, err := kSnapshotClient.NewForConfig(config)
	if err != nil {
		return err
	}
	c.snapshotClient = cs

	return nil
}

func (c *csi) String() string {
	return storkCSIDriverName
}

func (c *csi) Stop() error {
	return nil
}

func (c *csi) OwnsPVC(coreOps core.Ops, pvc *v1.PersistentVolumeClaim) bool {
	// Try to get info from the PV since storage class could be deleted
	pv, err := coreOps.GetPersistentVolume(pvc.Spec.VolumeName)
	if err != nil {
		log.PVCLog(pvc).Warnf("error getting pv %v for pvc %v: %v", pvc.Spec.VolumeName, pvc.Name, err)
		return false
	}
	return c.OwnsPV(pv)
}

func (c *csi) HasNativeVolumeDriverSupport(pv *v1.PersistentVolume) bool {
	return pv.Spec.CSI.Driver == snapv1.PortworxCsiProvisionerName ||
		pv.Spec.CSI.Driver == snapv1.PortworxCsiDeprecatedProvisionerName ||
		pv.Spec.CSI.Driver == "pd.csi.storage.gke.io" ||
		pv.Spec.CSI.Driver == "ebs.csi.aws.com" ||
		pv.Spec.CSI.Driver == "disk.csi.azure.com"
}

func (c *csi) OwnsPV(pv *v1.PersistentVolume) bool {
	// check if CSI volume
	if pv.Spec.CSI != nil {
		// We support certain CSI drivers natively
		if c.HasNativeVolumeDriverSupport(pv) {
			return false
		}

		log.PVLog(pv).Debugf("CSI Owns PV: %s", pv.Name)
		return true
	}

	log.PVLog(pv).Debugf("CSI does not own PV: %s", pv.Name)
	return false
}

func (c *csi) getSnapshotClassName(
	backup *storkapi.ApplicationBackup,
	driverName string,
) string {
	if snapshotClassName, ok := backup.Spec.Options[optCSISnapshotClassName]; ok {
		return snapshotClassName
	}
	return snapshotClassNamePrefix + driverName
}

func (c *csi) getVolumeSnapshotClass(snapshotClassName string) (*kSnapshotv1beta1.VolumeSnapshotClass, error) {
	return c.snapshotClient.SnapshotV1beta1().VolumeSnapshotClasses().Get(snapshotClassName, metav1.GetOptions{})
}

func (c *csi) createVolumeSnapshotClass(snapshotClassName, driverName string) (*kSnapshotv1beta1.VolumeSnapshotClass, error) {
	return c.snapshotClient.SnapshotV1beta1().VolumeSnapshotClasses().Create(&kSnapshotv1beta1.VolumeSnapshotClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: snapshotClassName,
		},
		Driver:         driverName,
		DeletionPolicy: kSnapshotv1beta1.VolumeSnapshotContentRetain,
	})
}

func (c *csi) getVolumeCSIDriver(info *storkapi.ApplicationBackupVolumeInfo) string {
	return info.Options[optCSIDriverName]
}

func (c *csi) ensureVolumeSnapshotClassCreated(snapshotClassCreatedForDriver map[string]bool, csiDriverName, snapshotClassName string) (map[string]bool, error) {
	if !snapshotClassCreatedForDriver[csiDriverName] {
		vsClass, err := c.getVolumeSnapshotClass(snapshotClassName)
		if k8s_errors.IsNotFound(err) {
			_, err = c.createVolumeSnapshotClass(snapshotClassName, csiDriverName)
			if err != nil {
				return nil, err
			}
			logrus.Debugf("volumesnapshotclass created: %v", snapshotClassName)
		} else if err != nil {
			return nil, err
		}

		// If we've found a vsClass, but it doesn't have a RetainPolicy, update to Retain.
		// This is essential for the storage backend to not delete the snapshot.
		// Some CSI drivers require specific VolumeSnapshotClass parameters, so we will leave those as is.
		if vsClass.DeletionPolicy == kSnapshotv1beta1.VolumeSnapshotContentDelete {
			vsClass.DeletionPolicy = kSnapshotv1beta1.VolumeSnapshotContentRetain
			_, err = c.snapshotClient.SnapshotV1beta1().VolumeSnapshotClasses().Update(vsClass)
			if err != nil {
				return nil, err
			}
		}

		snapshotClassCreatedForDriver[snapshotClassName] = true
	}

	return snapshotClassCreatedForDriver, nil
}

// backupStorageClasses backs up all storage classes needed to restore the backup PVCs
func (c *csi) backupStorageClasses(storageClasses []*storagev1.StorageClass, backup *storkapi.ApplicationBackup) error {
	scBytes, err := json.Marshal(storageClasses)
	if err != nil {
		return err
	}

	err = c.uploadObject(backup, storageClassesObjectName, scBytes)
	if err != nil {
		return err
	}

	return nil
}

func (c *csi) cancelBackupDuringStartFailure(backup *storkapi.ApplicationBackup, volumeBackupInfos []*storkapi.ApplicationBackupVolumeInfo) {
	backup.Status.Volumes = volumeBackupInfos
	err := c.CancelBackup(backup)
	if err != nil {
		log.ApplicationBackupLog(backup).Warnf("failed to cleanup backup %s after StartBackup failed: %v", backup.Name, err)
	}
	log.ApplicationBackupLog(backup).Warnf("successfully cancelled backup %s after StartBackup failed", backup.Name)
}

func (c *csi) StartBackup(
	backup *storkapi.ApplicationBackup,
	pvcs []v1.PersistentVolumeClaim,
) ([]*storkapi.ApplicationBackupVolumeInfo, error) {
	volumeInfos := make([]*storkapi.ApplicationBackupVolumeInfo, 0)
	snapshotClassCreatedForDriver := make(map[string]bool)
	var storageClasses []*storagev1.StorageClass
	storageClassAdded := make(map[string]bool)
	log.ApplicationBackupLog(backup).Debugf("started CSI backup: %v", backup.Name)
	for _, pvc := range pvcs {
		if pvc.DeletionTimestamp != nil {
			log.ApplicationBackupLog(backup).Warnf("Ignoring PVC %v which is being deleted", pvc.Name)
			continue
		}
		volumeInfo := &storkapi.ApplicationBackupVolumeInfo{}
		volumeInfo.Options = make(map[string]string)
		volumeInfo.PersistentVolumeClaim = pvc.Name
		volumeInfo.Namespace = pvc.Namespace
		volumeInfo.DriverName = storkCSIDriverName
		volumeInfo.Volume = pvc.Spec.VolumeName
		volumeInfos = append(volumeInfos, volumeInfo)

		// get snapshotclass name based on pv provisioner
		pvName, err := core.Instance().GetVolumeForPersistentVolumeClaim(&pvc)
		if err != nil {
			c.cancelBackupDuringStartFailure(backup, volumeInfos)
			return nil, fmt.Errorf("error getting PV name for PVC (%v/%v): %v", pvc.Namespace, pvc.Name, err)
		}
		volumeInfo.Volume = pvName
		pv, err := core.Instance().GetPersistentVolume(pvName)
		if err != nil {
			c.cancelBackupDuringStartFailure(backup, volumeInfos)
			return nil, fmt.Errorf("error getting pv %v: %v", pvName, err)
		}
		csiDriverName := pv.Spec.CSI.Driver
		volumeInfo.Options[optCSIDriverName] = csiDriverName
		snapshotClassName := c.getSnapshotClassName(backup, csiDriverName)

		// ensure volumesnapshotclass is created for this driver
		snapshotClassCreatedForDriver, err = c.ensureVolumeSnapshotClassCreated(snapshotClassCreatedForDriver, csiDriverName, snapshotClassName)
		if err != nil {
			c.cancelBackupDuringStartFailure(backup, volumeInfos)
			return nil, fmt.Errorf("failed to ensure volumesnapshotclass was created: %v", err)
		}

		// Create CSI volume snapshot
		vsName := c.getSnapshotName(&pvc, backup)
		vs := &kSnapshotv1beta1.VolumeSnapshot{
			ObjectMeta: metav1.ObjectMeta{
				Name:      vsName,
				Namespace: pvc.Namespace,
			},
			Spec: kSnapshotv1beta1.VolumeSnapshotSpec{
				VolumeSnapshotClassName: stringPtr(snapshotClassName),
				Source: kSnapshotv1beta1.VolumeSnapshotSource{
					PersistentVolumeClaimName: stringPtr(pvc.Name),
				},
			},
		}
		log.ApplicationBackupLog(backup).Debugf("creating volumesnapshot: %v", vsName)
		_, err = c.snapshotClient.SnapshotV1beta1().VolumeSnapshots(pvc.Namespace).Create(vs)
		if err != nil {
			c.cancelBackupDuringStartFailure(backup, volumeInfos)
			return nil, fmt.Errorf("failed to create volumesnapshot %s: %v", vsName, err)
		}
		volumeInfo.BackupID = string(vsName)

		sc, err := core.Instance().GetStorageClassForPVC(&pvc)
		if err != nil {
			c.cancelBackupDuringStartFailure(backup, volumeInfos)
			return nil, fmt.Errorf("failed to get storage class for PVC %s: %v", pvc.Name, err)
		}

		// only add one instance of a storageclass
		if !storageClassAdded[sc.Name] {
			sc.Kind = "StorageClass"
			sc.APIVersion = "storage.k8s.io/v1"
			sc.ResourceVersion = ""
			storageClasses = append(storageClasses, sc)
			storageClassAdded[sc.Name] = true
		}
	}

	// Backup the storage class
	err := c.backupStorageClasses(storageClasses, backup)
	if err != nil {
		c.cancelBackupDuringStartFailure(backup, volumeInfos)
		return nil, fmt.Errorf("failed to backup storage classes: %v", err)
	}

	return volumeInfos, nil
}

func (c *csi) getSnapshotName(pvc *v1.PersistentVolumeClaim, backup *storkapi.ApplicationBackup) string {
	return fmt.Sprintf("%s-%s-%s", snapshotPrefix, string(pvc.UID), string(backup.UID))
}

func (c *csi) snapshotReady(vs *kSnapshotv1beta1.VolumeSnapshot) bool {
	return vs.Status != nil && vs.Status.ReadyToUse != nil && *vs.Status.ReadyToUse
}

func (c *csi) snapshotContentReady(vscontent *kSnapshotv1beta1.VolumeSnapshotContent) bool {
	return vscontent.Status != nil && vscontent.Status.ReadyToUse != nil && *vscontent.Status.ReadyToUse
}

// uploadObject uploads the given data to the backup location specified in the backup object
func (c *csi) uploadObject(
	backup *storkapi.ApplicationBackup,
	objectName string,
	data []byte,
) error {
	backupLocation, err := storkops.Instance().GetBackupLocation(backup.Spec.BackupLocation, backup.Namespace)
	if err != nil {
		return err
	}
	bucket, err := objectstore.GetBucket(backupLocation)
	if err != nil {
		return err
	}

	if backupLocation.Location.EncryptionKey != "" {
		if data, err = crypto.Encrypt(data, backupLocation.Location.EncryptionKey); err != nil {
			return err
		}
	}

	objectPath := controllers.GetObjectPath(backup)
	writer, err := bucket.NewWriter(context.TODO(), filepath.Join(objectPath, objectName), nil)
	if err != nil {
		return err
	}

	_, err = writer.Write(data)
	if err != nil {
		closeErr := writer.Close()
		if closeErr != nil {
			log.ApplicationBackupLog(backup).Errorf("error closing writer for objectstore: %v", closeErr)
		}
		return err
	}
	err = writer.Close()
	if err != nil {
		log.ApplicationBackupLog(backup).Errorf("error closing writer for objectstore: %v", err)
		return err
	}
	return nil
}

// uploadSnapshotsAndContents issues an object upload for all VolumeSnapshots and VolumeSnapshotContents provided
func (c *csi) uploadCSIBackupObject(
	backup *storkapi.ApplicationBackup,
	vsMap map[string]*kSnapshotv1beta1.VolumeSnapshot,
	vsContentMap map[string]*kSnapshotv1beta1.VolumeSnapshotContent,
	vsClassMap map[string]*kSnapshotv1beta1.VolumeSnapshotClass,
) error {
	csiBackup := csiBackupObject{
		VolumeSnapshots:        vsMap,
		VolumeSnapshotContents: vsContentMap,
		VolumeSnapshotClasses:  vsClassMap,
	}

	var csiBackupBytes []byte

	csiBackupBytes, err := json.Marshal(csiBackup)
	if err != nil {
		return err
	}

	err = c.uploadObject(backup, snapshotObjectName, csiBackupBytes)
	if err != nil {
		return err
	}

	return nil
}

func (c *csi) cleanupSnapshots(
	vsMap map[string]*kSnapshotv1beta1.VolumeSnapshot,
	vsContentMap map[string]*kSnapshotv1beta1.VolumeSnapshotContent,
	vsClassMap map[string]*kSnapshotv1beta1.VolumeSnapshotClass,
	retainContent bool,
) error {
	desiredRetainPolicy := kSnapshotv1beta1.VolumeSnapshotContentRetain
	if !retainContent {
		desiredRetainPolicy = kSnapshotv1beta1.VolumeSnapshotContentDelete
	}

	// ensure all vscontent have the desired delete policy
	for _, vsc := range vsContentMap {
		if vsc.Spec.DeletionPolicy != desiredRetainPolicy {
			vsc.UID = ""
			vsc.Spec.DeletionPolicy = desiredRetainPolicy
			_, err := c.snapshotClient.SnapshotV1beta1().VolumeSnapshotContents().Update(vsc)
			if err != nil {
				return err
			}
		}
	}

	// delete all vs & vscontent
	for _, vs := range vsMap {
		err := c.snapshotClient.SnapshotV1beta1().VolumeSnapshots(vs.Namespace).Delete(vs.Name, &metav1.DeleteOptions{})
		if k8s_errors.IsNotFound(err) {
			continue
		} else if err != nil {
			return err
		}
	}
	for _, vsc := range vsContentMap {
		err := c.snapshotClient.SnapshotV1beta1().VolumeSnapshotContents().Delete(vsc.Name, &metav1.DeleteOptions{})
		if k8s_errors.IsNotFound(err) {
			continue
		} else if err != nil {
			return err
		}
	}

	return nil
}

func (c *csi) GetBackupStatus(backup *storkapi.ApplicationBackup) ([]*storkapi.ApplicationBackupVolumeInfo, error) {
	if c.snapshotClient == nil {
		if err := c.Init(nil); err != nil {
			return nil, err
		}
	}

	volumeInfos := make([]*storkapi.ApplicationBackupVolumeInfo, 0)
	var anyInProgress bool
	var anyFailed bool
	vsMap := make(map[string]*kSnapshotv1beta1.VolumeSnapshot)
	vsContentMap := make(map[string]*kSnapshotv1beta1.VolumeSnapshotContent)
	vsClassMap := make(map[string]*kSnapshotv1beta1.VolumeSnapshotClass)
	for _, vInfo := range backup.Status.Volumes {
		if vInfo.DriverName != storkCSIDriverName {
			continue
		}

		pvc, err := core.Instance().GetPersistentVolumeClaim(vInfo.PersistentVolumeClaim, vInfo.Namespace)
		if err != nil {
			return nil, err
		}

		// Once we're in ApplicationBackupStatusInCleanup, we only check if cleanup has finished.
		if backup.Status.Status == storkapi.ApplicationBackupStatusInCleanup {
			var snapshotDeleted, snapshotContentDeleted bool
			_, err := c.snapshotClient.SnapshotV1beta1().VolumeSnapshots(vInfo.Namespace).Get(c.getSnapshotName(pvc, backup), metav1.GetOptions{})
			if err != nil {
				if k8s_errors.IsNotFound(err) {
					snapshotDeleted = true
				} else {
					return nil, err
				}
			}

			vscName, ok := vInfo.Options[optVolumeSnapshotContentName]
			if !ok {
				log.ApplicationBackupLog(backup).Debugf("failed to find volume snapshot content name for cleanup check: %v", vInfo.Options)
			}
			_, err = c.snapshotClient.SnapshotV1beta1().VolumeSnapshotContents().Get(vscName, metav1.GetOptions{})
			if err != nil {
				if k8s_errors.IsNotFound(err) {
					snapshotContentDeleted = true
				} else {
					return nil, err
				}
			}

			if snapshotDeleted && snapshotContentDeleted {
				vInfo.Status = storkapi.ApplicationBackupStatusSuccessful
				vInfo.Reason = "Backup successful for volume"
			}

			volumeInfos = append(volumeInfos, vInfo)
			continue
		}

		// Not in cleanup, continue to wait for Backup finish
		snapshot, err := c.snapshotClient.SnapshotV1beta1().VolumeSnapshots(vInfo.Namespace).Get(c.getSnapshotName(pvc, backup), metav1.GetOptions{})
		if err != nil {
			return nil, err
		}
		vsMap[vInfo.BackupID] = snapshot

		var snapshotClassName string
		if snapshot.Spec.VolumeSnapshotClassName != nil {
			snapshotClassName = *snapshot.Spec.VolumeSnapshotClassName
		}
		snapshotClass, err := c.snapshotClient.SnapshotV1beta1().VolumeSnapshotClasses().Get(snapshotClassName, metav1.GetOptions{})
		if err != nil {
			return nil, err
		}
		volumeSnapshotReady := c.snapshotReady(snapshot)
		var volumeSnapshotContentReady bool
		var snapshotContent *kSnapshotv1beta1.VolumeSnapshotContent
		if volumeSnapshotReady && snapshot.Status.BoundVolumeSnapshotContentName != nil {
			snapshotContent, err = c.snapshotClient.SnapshotV1beta1().VolumeSnapshotContents().Get(*snapshot.Status.BoundVolumeSnapshotContentName, metav1.GetOptions{})
			if err != nil {
				return nil, err
			}
			vsContentMap[vInfo.BackupID] = snapshotContent
			// Only backup one instance of VSClass
			vsClassMap[snapshotClass.Name] = snapshotClass
			volumeSnapshotContentReady = c.snapshotContentReady(snapshotContent)
		}

		switch {
		case volumeSnapshotReady && volumeSnapshotContentReady:
			vInfo.Status = storkapi.ApplicationBackupStatusInCleanup
			vInfo.Reason = "Backup uploaded, cleaning up snapshot objects"
			size, _ := snapshot.Status.RestoreSize.AsInt64()
			vInfo.ActualSize = uint64(size)
			vInfo.TotalSize = uint64(size)
			vInfo.Options[optVolumeSnapshotContentName] = snapshotContent.Name

		case time.Now().After(snapshot.CreationTimestamp.Add(snapshotTimeout)):
			vInfo.Status = storkapi.ApplicationBackupStatusFailed
			vInfo.Reason = fmt.Sprintf("Snapshot timeout out after %s", snapshotTimeout.String())
			anyFailed = true

		default:
			vInfo.Status = storkapi.ApplicationBackupStatusInProgress
			vInfo.Reason = "Volume backup in progress"
			anyInProgress = true
		}

		volumeInfos = append(volumeInfos, vInfo)
	}

	// if a failure occurred with any snapshot, make sure to clean up all snapshots
	if anyFailed {
		// Delete all snapshots after a failure
		err := c.CancelBackup(backup)
		if err != nil {
			return nil, err
		}
		log.ApplicationBackupLog(backup).Debugf("cleaned up all snapshots after a backup failure")

		return volumeInfos, nil
	}

	// if all have finished, add all VolumeSnapshot and VolumeSnapshotContent to objectstore
	// in the case where no volumes are being backed up, skip uploading empty lists
	if !anyInProgress && len(vsContentMap) > 0 && len(vsMap) > 0 && backup.Status.Status != storkapi.ApplicationBackupStatusInCleanup {
		err := c.uploadCSIBackupObject(backup, vsMap, vsContentMap, vsClassMap)
		if err != nil {
			return nil, err
		}
		log.ApplicationBackupLog(backup).Debugf("finished and uploaded %v snapshots and %v snapshotcontents", len(vsMap), len(vsContentMap))

		// enter cleanup phase after a successful object upload
		backup.Status.Status = storkapi.ApplicationBackupStatusInCleanup
		err = c.cleanupSnapshots(vsMap, vsContentMap, vsClassMap, true)
		if err != nil {
			return nil, err
		}
		log.ApplicationBackupLog(backup).Debugf("started clean up of %v snapshots and %v snapshotcontents", len(vsMap), len(vsContentMap))
	}

	return volumeInfos, nil
}

func (c *csi) recreateSnapshotForDeletion(
	backup *storkapi.ApplicationBackup,
	vbInfo *storkapi.ApplicationBackupVolumeInfo,
	csiBackupObject *csiBackupObject,
	snapshotClassCreatedForDriver map[string]bool,
) error {
	var err error
	driverName := c.getVolumeCSIDriver(vbInfo)
	snapshotClassName := c.getSnapshotClassName(backup, driverName)

	// make sure snapshot class is created for this object.
	// if we have already created it in this batch, do not check if created already.
	_, err = c.ensureVolumeSnapshotClassCreated(snapshotClassCreatedForDriver, driverName, snapshotClassName)
	if err != nil {
		return err
	}

	// Get VSC and VS
	snapshotID := vbInfo.BackupID
	vsc, err := csiBackupObject.GetVolumeSnapshotContent(snapshotID)
	if err != nil {
		return err
	}
	vs, err := csiBackupObject.GetVolumeSnapshot(snapshotID)
	if err != nil {
		return err
	}
	vsClass, err := csiBackupObject.GetVolumeSnapshotClass(snapshotID)
	if err != nil {
		return err
	}

	// Create vsClass
	_, err = c.restoreVolumeSnapshotClass(vsClass)
	if err != nil {
		return fmt.Errorf("failed to restore VolumeSnapshotClass for deletion: %s", err.Error())
	}
	log.ApplicationBackupLog(backup).Debugf("created volume snapshot class %s for backup %s deletion", vs.Name, snapshotID)

	// Create VS, bound to VSC
	vs, err = c.restoreVolumeSnapshot(backup.Namespace, vs, vsc)
	if err != nil {
		return fmt.Errorf("failed to restore VolumeSnapshot for deletion: %s", err.Error())
	}
	log.ApplicationBackupLog(backup).Debugf("created volume snapshot %s for backup %s deletion", vs.Name, snapshotID)

	// Create VSC
	vsc.Spec.DeletionPolicy = kSnapshotv1beta1.VolumeSnapshotContentDelete
	_, err = c.restoreVolumeSnapshotContent(backup.Namespace, vs, vsc)
	if err != nil {
		return err
	}
	log.ApplicationBackupLog(backup).Debugf("created volume snapshot content %s for backup %s deletion", vsc.Name, snapshotID)

	return nil
}

func (c *csi) CancelBackup(backup *storkapi.ApplicationBackup) error {
	if backup.Status.Status == storkapi.ApplicationBackupStatusInProgress {
		// set of all snapshot classes deleted
		for _, vInfo := range backup.Status.Volumes {
			snapshotName := vInfo.BackupID
			snapshot, err := c.snapshotClient.SnapshotV1beta1().VolumeSnapshots(backup.Namespace).Get(snapshotName, metav1.GetOptions{})
			if err != nil {
				if k8s_errors.IsNotFound(err) {
					// already deleted or failed to create
					log.ApplicationBackupLog(backup).Debugf("snapshot already deleted or does not exist during backup cancel: %s", snapshotName)
					continue
				}
				return err
			}

			// If snapshot content has been created and bound, mark for deletion with delete policy
			if snapshot.Status != nil && snapshot.Status.BoundVolumeSnapshotContentName != nil {
				snapshotContent, err := c.snapshotClient.SnapshotV1beta1().VolumeSnapshotContents().Get(*snapshot.Status.BoundVolumeSnapshotContentName, metav1.GetOptions{})
				if err != nil {
					// continue deleting snapshot, as the content may have been manually deleted
					log.ApplicationBackupLog(backup).Debugf("Cancel Backup failed to find snapshotcontent: %s", *snapshot.Status.BoundVolumeSnapshotContentName)
				}

				// update snapshot content if we found one
				if snapshotContent != nil {
					snapshotContent.UID = ""
					snapshotContent.Spec.DeletionPolicy = kSnapshotv1beta1.VolumeSnapshotContentDelete
					_, err = c.snapshotClient.SnapshotV1beta1().VolumeSnapshotContents().Update(snapshotContent)
					if err != nil {
						return fmt.Errorf("failed to update VolumeSnapshotContent %v with deletion policy", snapshotContent.Name)
					}
				}
			}

			err = c.snapshotClient.SnapshotV1beta1().VolumeSnapshots(backup.Namespace).Delete(snapshotName, &metav1.DeleteOptions{})
			if err != nil {
				log.ApplicationBackupLog(backup).Warnf("Cancel backup failed to delete volumesnapshot %s: %v", snapshotName, err)
			}
		}
	}

	return nil

}

func (c *csi) cleanupBackupLocation(backup *storkapi.ApplicationBackup) error {
	backupLocation, err := storkops.Instance().GetBackupLocation(backup.Spec.BackupLocation, backup.Namespace)
	if err != nil {
		// Can't do anything if the backup location is deleted
		if k8s_errors.IsNotFound(err) {
			return nil
		}
		return err
	}
	bucket, err := objectstore.GetBucket(backupLocation)
	if err != nil {
		return err
	}

	objectPath := backup.Status.BackupPath
	if objectPath != "" {
		if err = bucket.Delete(context.TODO(), filepath.Join(objectPath, snapshotObjectName)); err != nil && gcerrors.Code(err) != gcerrors.NotFound {
			return fmt.Errorf("error deleting resources for backup %v/%v: %v", backup.Namespace, backup.Name, err)
		}
		if err = bucket.Delete(context.TODO(), filepath.Join(objectPath, storageClassesObjectName)); err != nil && gcerrors.Code(err) != gcerrors.NotFound {
			return fmt.Errorf("error deleting resources for backup %v/%v: %v", backup.Namespace, backup.Name, err)
		}
	}

	return nil
}

func (c *csi) DeleteBackup(backup *storkapi.ApplicationBackup) error {
	// if successful, re-create VS and VSC
	backupSuccessful := backup.Status.Status == storkapi.ApplicationBackupStatusSuccessful

	// collect all volumesnapshots and volumesnapshotcontents
	vsMap := make(map[string]*kSnapshotv1beta1.VolumeSnapshot)
	vsContentMap := make(map[string]*kSnapshotv1beta1.VolumeSnapshotContent)
	vsClassMap := make(map[string]*kSnapshotv1beta1.VolumeSnapshotClass)
	snapshotClassCreatedForDriver := make(map[string]bool)
	csiBackupObject, err := c.getCSIBackupObject(backup.Name, backup.Namespace)
	if err != nil {
		return err
	}

	for _, vInfo := range backup.Status.Volumes {
		if backupSuccessful {
			err = c.recreateSnapshotForDeletion(backup, vInfo, csiBackupObject, snapshotClassCreatedForDriver)
			if err != nil {
				return err
			}
		}
		vs, err := csiBackupObject.GetVolumeSnapshot(vInfo.BackupID)
		if err != nil {
			return fmt.Errorf("failed to find Snapshot for backup %s: %s", vInfo.BackupID, err.Error())
		}
		snapshot, err := c.snapshotClient.SnapshotV1beta1().VolumeSnapshots(vInfo.Namespace).Get(vs.Name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		if snapshot.Status == nil || snapshot.Status.BoundVolumeSnapshotContentName == nil {
			return fmt.Errorf("failed to find get status for snapshot: %s/%s", snapshot.Namespace, snapshot.Name)
		}
		snapshotContent, err := c.snapshotClient.SnapshotV1beta1().VolumeSnapshotContents().Get(*snapshot.Status.BoundVolumeSnapshotContentName, metav1.GetOptions{})
		if err != nil {
			return err
		}
		snapshotClass, err := c.snapshotClient.SnapshotV1beta1().VolumeSnapshotClasses().Get(*vs.Spec.VolumeSnapshotClassName, metav1.GetOptions{})
		if err != nil {
			return err
		}
		vsMap[vInfo.BackupID] = snapshot
		vsContentMap[vInfo.BackupID] = snapshotContent
		vsClassMap[vInfo.BackupID] = snapshotClass
	}
	err = c.cleanupSnapshots(vsMap, vsContentMap, vsClassMap, false)
	if err != nil {
		return err
	}
	log.ApplicationBackupLog(backup).Debugf("deleted %v snapshots for backup %s", len(vsMap), string(backup.UID))

	err = c.cleanupBackupLocation(backup)
	if err != nil {
		return err
	}
	log.ApplicationBackupLog(backup).Debugf("cleaned up objects for backup %s", string(backup.UID))

	return nil
}

func (c *csi) UpdateMigratedPersistentVolumeSpec(
	pv *v1.PersistentVolume,
) (*v1.PersistentVolume, error) {
	return nil, &errors.ErrNotSupported{}
}

func (c *csi) getRestoreStorageClasses(backup *storkapi.ApplicationBackup, resources []runtime.Unstructured) ([]runtime.Unstructured, error) {
	storageClassesBytes, err := c.downloadObject(backup, backup.Spec.BackupLocation, backup.Namespace, storageClassesObjectName)
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(storageClassesBytes, &resources)
	if err != nil {
		return nil, err
	}

	return resources, nil
}

// GetPreRestoreResources gets all storage classes needed
// in order to restore the backed up PVCs
func (c *csi) GetPreRestoreResources(
	backup *storkapi.ApplicationBackup,
	resources []runtime.Unstructured,
) ([]runtime.Unstructured, error) {
	return c.getRestoreStorageClasses(backup, resources)
}

func (c *csi) downloadObject(
	backup *storkapi.ApplicationBackup,
	backupLocation string,
	namespace string,
	objectName string,
) ([]byte, error) {
	restoreLocation, err := storkops.Instance().GetBackupLocation(backupLocation, namespace)
	if err != nil {
		return nil, err
	}
	bucket, err := objectstore.GetBucket(restoreLocation)
	if err != nil {
		return nil, err
	}

	objectPath := backup.Status.BackupPath
	exists, err := bucket.Exists(context.TODO(), filepath.Join(objectPath, objectName))
	if err != nil || !exists {
		return nil, nil
	}

	data, err := bucket.ReadAll(context.TODO(), filepath.Join(objectPath, objectName))
	if err != nil {
		return nil, err
	}
	if restoreLocation.Location.EncryptionKey != "" {
		if data, err = crypto.Decrypt(data, restoreLocation.Location.EncryptionKey); err != nil {
			return nil, err
		}
	}

	return data, nil
}

// getRestoreSnapshotsAndContent retrieves the volumeSnapshots and
// volumeSnapshotContents associated with a backupID
func (c *csi) getCSIBackupObject(backupName, backupNamespace string) (*csiBackupObject, error) {
	backup, err := storkops.Instance().GetApplicationBackup(backupName, backupNamespace)
	if err != nil {
		return nil, fmt.Errorf("error getting backup spec for CSI restore: %v", err)
	}

	backupObjectBytes, err := c.downloadObject(backup, backup.Spec.BackupLocation, backup.Namespace, snapshotObjectName)
	if err != nil {
		return nil, err
	}

	cbo := &csiBackupObject{}
	err = json.Unmarshal(backupObjectBytes, cbo)
	if err != nil {
		return nil, err
	}

	return cbo, nil
}

// getBackupResources gets all objects in resource.json
func (c *csi) getBackupResources(restore *storkapi.ApplicationRestore) ([]runtime.Unstructured, error) {
	backup, err := storkops.Instance().GetApplicationBackup(restore.Spec.BackupName, restore.Namespace)
	if err != nil {
		return nil, fmt.Errorf("error getting backup resources for CSI restore: %v", err)
	}

	backupObjectBytes, err := c.downloadObject(backup, backup.Spec.BackupLocation, backup.Namespace, resourcesObjectName)
	if err != nil {
		return nil, err
	}

	objects := make([]*unstructured.Unstructured, 0)
	if err = json.Unmarshal(backupObjectBytes, &objects); err != nil {
		return nil, err
	}
	runtimeObjects := make([]runtime.Unstructured, 0)
	for _, o := range objects {
		runtimeObjects = append(runtimeObjects, o)
	}
	return runtimeObjects, nil
}

func (c *csi) findPVCInResources(resources []runtime.Unstructured, pvcName, pvcNamespace string) (*v1.PersistentVolumeClaim, error) {
	for _, object := range resources {
		objectType, err := meta.TypeAccessor(object)
		if err != nil {
			return nil, fmt.Errorf("error getting objectType from object: %v", err)
		}
		metadata, err := meta.Accessor(object)
		if err != nil {
			return nil, fmt.Errorf("error getting metadata from object: %v", err)
		}

		if objectType.GetKind() == "PersistentVolumeClaim" &&
			metadata.GetName() == pvcName &&
			metadata.GetNamespace() == pvcNamespace {
			pvc := v1.PersistentVolumeClaim{}
			if err := runtime.DefaultUnstructuredConverter.FromUnstructured(object.UnstructuredContent(), &pvc); err != nil {
				return nil, fmt.Errorf("error converting to persistent volume claim: %v", err)
			}

			return &pvc, nil
		}
	}

	return nil, fmt.Errorf("PVC %s not backed up in resources.json", pvcName)
}

func (c *csi) cleanK8sPVCAnnotations(pvc *v1.PersistentVolumeClaim) *v1.PersistentVolumeClaim {
	if pvc.Annotations != nil {
		newAnnotations := make(map[string]string)

		// we will remove the following annotations to prevent controller confusion:
		// - pv.kubernetes.io/bind-completed
		// - pv.kubernetes.io/bound-by-controller
		for key, val := range pvc.Annotations {
			if key != annPVBindCompleted && key != annPVBoundByController {
				newAnnotations[key] = val
			}
		}
		pvc.Annotations = newAnnotations
	}

	return pvc
}

func (c *csi) restoreVolumeSnapshotClass(vsClass *kSnapshotv1beta1.VolumeSnapshotClass) (*kSnapshotv1beta1.VolumeSnapshotClass, error) {
	vsClass.ResourceVersion = ""
	vsClass.UID = ""
	newVSClass, err := c.snapshotClient.SnapshotV1beta1().VolumeSnapshotClasses().Create(vsClass)
	if err != nil {
		if k8s_errors.IsAlreadyExists(err) {
			return vsClass, nil
		}
		return nil, err
	}

	return newVSClass, nil
}

func (c *csi) restoreVolumeSnapshot(namespace string, vs *kSnapshotv1beta1.VolumeSnapshot, vsc *kSnapshotv1beta1.VolumeSnapshotContent) (*kSnapshotv1beta1.VolumeSnapshot, error) {
	vs.ResourceVersion = ""
	vs.Spec.Source.PersistentVolumeClaimName = nil
	vs.Spec.Source.VolumeSnapshotContentName = &vsc.Name
	vs.Namespace = namespace
	vs, err := c.snapshotClient.SnapshotV1beta1().VolumeSnapshots(namespace).Create(vs)
	if err != nil {
		if k8s_errors.IsAlreadyExists(err) {
			return vs, nil
		}
		return nil, err
	}

	return vs, nil
}

func (c *csi) restoreVolumeSnapshotContent(namespace string, vs *kSnapshotv1beta1.VolumeSnapshot, vsc *kSnapshotv1beta1.VolumeSnapshotContent) (*kSnapshotv1beta1.VolumeSnapshotContent, error) {
	snapshotHandle := *vsc.Status.SnapshotHandle
	vsc.ResourceVersion = ""
	vsc.Spec.Source.VolumeHandle = nil
	vsc.Spec.Source.SnapshotHandle = &snapshotHandle
	vsc.Spec.VolumeSnapshotRef.Name = vs.Name
	vsc.Spec.VolumeSnapshotRef.Namespace = namespace
	vsc.Spec.VolumeSnapshotRef.UID = vs.UID
	vsc, err := c.snapshotClient.SnapshotV1beta1().VolumeSnapshotContents().Create(vsc)
	if err != nil {
		if k8s_errors.IsAlreadyExists(err) {
			return vsc, nil
		}
		return nil, err
	}

	return vsc, nil
}

func (c *csi) restorePVC(
	restore *storkapi.ApplicationRestore,
	pvc *v1.PersistentVolumeClaim,
	snapshotID string,
) (*v1.PersistentVolumeClaim, error) {
	var err error
	pvc = c.cleanK8sPVCAnnotations(pvc)

	// handle namespace mapping
	destNamespace := c.getDestinationNamespace(restore, pvc.Namespace)
	pvc.Namespace = destNamespace

	// Create new PVC
	pvc.ResourceVersion = ""
	pvc.Namespace = destNamespace
	pvc.Spec.VolumeName = ""
	pvc.Spec.DataSource = &v1.TypedLocalObjectReference{
		APIGroup: stringPtr("snapshot.storage.k8s.io"),
		Kind:     "VolumeSnapshot",
		Name:     snapshotID,
	}
	pvc.Status = v1.PersistentVolumeClaimStatus{
		Phase: v1.ClaimPending,
	}
	pvc, err = core.Instance().CreatePersistentVolumeClaim(pvc)
	if err != nil {
		return nil, fmt.Errorf("failed to create PVC %s: %s", pvc.Name, err.Error())
	}
	log.ApplicationRestoreLog(restore).Debugf("created pvc: %s", pvc.Name)

	return pvc, nil
}

func (c *csi) createRestoreSnapshotsAndPVCs(
	restore *storkapi.ApplicationRestore,
	volumeBackupInfos []*storkapi.ApplicationBackupVolumeInfo,
	csiBackupObject *csiBackupObject,
) ([]*storkapi.ApplicationRestoreVolumeInfo, error) {
	var err error
	volumeRestoreInfos := []*storkapi.ApplicationRestoreVolumeInfo{}

	// Get all backed up resources to find PVC spec
	resources, err := c.getBackupResources(restore)
	if err != nil {
		return nil, fmt.Errorf("failed to get backup resources: %s", err.Error())
	}

	// ensure volumesnapshotclass is created for this driver
	log.ApplicationRestoreLog(restore).Debugf("restoring %v volumes", len(volumeBackupInfos))
	for _, vbInfo := range volumeBackupInfos {
		vrInfo := &storkapi.ApplicationRestoreVolumeInfo{}
		log.ApplicationRestoreLog(restore).Debugf("restoring CSI volume %s", vbInfo.BackupID)

		// Get VSC and VS
		snapshotID := vbInfo.BackupID
		vsc, err := csiBackupObject.GetVolumeSnapshotContent(snapshotID)
		if err != nil {
			return nil, err
		}

		vs, err := csiBackupObject.GetVolumeSnapshot(snapshotID)
		if err != nil {
			return nil, fmt.Errorf("failed to retrieve volume snapshot for snapshotID %s", snapshotID)
		}

		vsClass, err := csiBackupObject.GetVolumeSnapshotClass(snapshotID)
		if err != nil {
			return nil, err
		}

		// Create VSClass
		vsClass, err = c.restoreVolumeSnapshotClass(vsClass)
		if err != nil {
			return nil, err
		}
		log.ApplicationRestoreLog(restore).Debugf("created vsClass: %s", vsClass.Name)

		// Create VS, bound to VSC
		destNamespace := c.getDestinationNamespace(restore, vs.Namespace)
		vs, err = c.restoreVolumeSnapshot(destNamespace, vs, vsc)
		if err != nil {
			return nil, err
		}
		log.ApplicationRestoreLog(restore).Debugf("created vs: %s", vs.Name)

		// Create VSC
		vsc, err = c.restoreVolumeSnapshotContent(destNamespace, vs, vsc)
		if err != nil {
			return nil, err
		}
		log.ApplicationRestoreLog(restore).Debugf("created vsc: %s", vsc.Name)

		// Find PVC from resources.json
		pvc, err := c.findPVCInResources(resources, vbInfo.PersistentVolumeClaim, vbInfo.Namespace)
		if err != nil {
			return nil, fmt.Errorf("failed to find pvc %s in resources: %v", vbInfo.PersistentVolumeClaim, err.Error())
		}

		// Update PVC to restore from snapshot
		pvc, err = c.restorePVC(restore, pvc, vbInfo.BackupID)
		if err != nil {
			return nil, fmt.Errorf("failed to restore pvc %s: %v", vbInfo.PersistentVolumeClaim, err.Error())
		}
		log.ApplicationRestoreLog(restore).Debugf("created pvc: %s", pvc.Name)

		// Populate volumeRestoreInfo
		vrInfo.DriverName = storkCSIDriverName
		vrInfo.PersistentVolumeClaim = pvc.Name
		vrInfo.SourceNamespace = vbInfo.Namespace
		vrInfo.SourceVolume = vbInfo.Volume
		vrInfo.Status = storkapi.ApplicationRestoreStatusInitial
		vrInfo.TotalSize = vbInfo.TotalSize
		volumeRestoreInfos = append(volumeRestoreInfos, vrInfo)
	}

	return volumeRestoreInfos, nil
}

func (c *csi) StartRestore(
	restore *storkapi.ApplicationRestore,
	volumeBackupInfos []*storkapi.ApplicationBackupVolumeInfo,
) ([]*storkapi.ApplicationRestoreVolumeInfo, error) {
	if c.snapshotClient == nil {
		if err := c.Init(nil); err != nil {
			return nil, err
		}
	}
	log.ApplicationRestoreLog(restore).Debugf("started CSI restore %s", restore.UID)

	// Get volumesnapshots.json and volumesnapshotcontents.json
	csiBackupObject, err := c.getCSIBackupObject(restore.Spec.BackupName, restore.Namespace)
	if err != nil {
		return nil, err
	}

	// Create Restore Snapshots and PVCs
	volumeRestoreInfos, err := c.createRestoreSnapshotsAndPVCs(restore, volumeBackupInfos, csiBackupObject)
	if err != nil {
		return nil, err
	}

	return volumeRestoreInfos, nil
}

func (c *csi) CancelRestore(restore *storkapi.ApplicationRestore) error {
	for _, vrInfo := range restore.Status.Volumes {
		pvcRestoreSucceeded := (vrInfo.Status == storkapi.ApplicationRestoreStatusPartialSuccess || vrInfo.Status == storkapi.ApplicationRestoreStatusSuccessful)

		// Only clean up dangling PVC if it's restore did not succeed
		if !pvcRestoreSucceeded {
			destNamespace := c.getDestinationNamespace(restore, vrInfo.SourceNamespace)
			err := core.Instance().DeletePersistentVolumeClaim(vrInfo.PersistentVolumeClaim, destNamespace)
			if err != nil {
				return err
			}
		}
	}

	csiBackupObject, err := c.getCSIBackupObject(restore.Spec.BackupName, restore.Namespace)
	if err != nil {
		return err
	}

	err = c.cleanupSnapshots(csiBackupObject.VolumeSnapshots, csiBackupObject.VolumeSnapshotContents, csiBackupObject.VolumeSnapshotClasses, true)
	if err != nil {
		return err
	}

	return nil
}

func (c *csi) GetRestoreStatus(restore *storkapi.ApplicationRestore) ([]*storkapi.ApplicationRestoreVolumeInfo, error) {
	volumeInfos := make([]*storkapi.ApplicationRestoreVolumeInfo, 0)
	var anyInProgress bool

	for _, vrInfo := range restore.Status.Volumes {
		// Handle namespace mapping
		destNamespace := c.getDestinationNamespace(restore, vrInfo.SourceNamespace)

		// Check on PVC status
		pvc, err := core.Instance().GetPersistentVolumeClaim(vrInfo.PersistentVolumeClaim, destNamespace)
		if err != nil {
			return nil, err
		}
		switch pvc.Status.Phase {
		case v1.ClaimBound:
			vrInfo.RestoreVolume = pvc.Spec.VolumeName
			vrInfo.Status = storkapi.ApplicationRestoreStatusSuccessful
			vrInfo.Reason = fmt.Sprintf("Volume restore successful: PVC %s is bound", pvc.Name)
		case v1.ClaimLost:
			vrInfo.Status = storkapi.ApplicationRestoreStatusFailed
			vrInfo.Reason = fmt.Sprintf("Volume restore failed: PVC %s is lost", pvc.Name)
		case v1.ClaimPending:
			vrInfo.Status = storkapi.ApplicationRestoreStatusInProgress
			vrInfo.Reason = fmt.Sprintf("Volume restore in progress: PVC %s is pending", pvc.Name)
			anyInProgress = true
		}

		if time.Now().After(pvc.CreationTimestamp.Add(restoreTimeout)) {
			vrInfo.Status = storkapi.ApplicationRestoreStatusFailed
			vrInfo.Reason = fmt.Sprintf("PVC restore timeout out after %s", restoreTimeout.String())
		}

		volumeInfos = append(volumeInfos, vrInfo)
	}

	// If none are in progress, we can safely cleanup our volumesnapshot objects
	if !anyInProgress {
		csiBackupObject, err := c.getCSIBackupObject(restore.Spec.BackupName, restore.Namespace)
		if err != nil {
			return nil, fmt.Errorf("failed to get CSI backup object: %v", err)
		}

		// cleanupSnapshots with DeletionPolicy as retain
		err = c.cleanupSnapshots(csiBackupObject.VolumeSnapshots, csiBackupObject.VolumeSnapshotContents, csiBackupObject.VolumeSnapshotClasses, true)
		if err != nil {
			return nil, fmt.Errorf("failed to clean CSI snapshots: %v", err)
		}

		return volumeInfos, nil
	}

	return volumeInfos, nil
}

func (c *csi) getDestinationNamespace(restore *storkapi.ApplicationRestore, ns string) string {
	destNamespace, ok := restore.Spec.NamespaceMapping[ns]
	if !ok {
		destNamespace = restore.Namespace
	}

	return destNamespace
}

func (c *csi) InspectVolume(volumeID string) (*storkvolume.Info, error) {
	return nil, &errors.ErrNotSupported{}
}

func (c *csi) GetClusterID() (string, error) {
	return "", &errors.ErrNotSupported{}
}

func (c *csi) GetNodes() ([]*storkvolume.NodeInfo, error) {
	return nil, &errors.ErrNotSupported{}
}

func (c *csi) InspectNode(id string) (*storkvolume.NodeInfo, error) {
	return nil, &errors.ErrNotSupported{}
}

func (c *csi) GetPodVolumes(podSpec *v1.PodSpec, namespace string) ([]*storkvolume.Info, error) {
	return nil, &errors.ErrNotSupported{}
}

func (c *csi) GetSnapshotPlugin() snapshotVolume.Plugin {
	return nil
}

func (c *csi) GetSnapshotType(snap *snapv1.VolumeSnapshot) (string, error) {
	return "", &errors.ErrNotSupported{}
}

func (c *csi) GetVolumeClaimTemplates([]v1.PersistentVolumeClaim) (
	[]v1.PersistentVolumeClaim, error) {
	return nil, &errors.ErrNotSupported{}
}

func stringPtr(s string) *string {
	return &s
}

func init() {
	c := &csi{}
	err := c.Init(nil)
	if err != nil {
		logrus.Debugf("Error init'ing csi driver: %v", err)
	}
	if err := storkvolume.Register(storkCSIDriverName, c); err != nil {
		logrus.Panicf("Error registering csi volume driver: %v", err)
	}
}
