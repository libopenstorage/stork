package csi

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"

	kSnapshotv1beta1 "github.com/kubernetes-csi/external-snapshotter/client/v4/apis/volumesnapshot/v1beta1"
	kSnapshotClient "github.com/kubernetes-csi/external-snapshotter/client/v4/clientset/versioned"
	snapv1 "github.com/kubernetes-incubator/external-storage/snapshot/pkg/apis/crd/v1"
	snapshotVolume "github.com/kubernetes-incubator/external-storage/snapshot/pkg/volume"
	storkvolume "github.com/libopenstorage/stork/drivers/volume"
	storkapi "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/libopenstorage/stork/pkg/applicationmanager/controllers"
	"github.com/libopenstorage/stork/pkg/crypto"
	"github.com/libopenstorage/stork/pkg/errors"
	"github.com/libopenstorage/stork/pkg/log"
	"github.com/libopenstorage/stork/pkg/objectstore"
	"github.com/libopenstorage/stork/pkg/snapshotter"
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
	"k8s.io/apimachinery/pkg/types"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

const (
	// snapshotPrefix is appended to CSI backup snapshot
	snapshotBackupPrefix = "backup"
	// snapshotPrefix is appended to CSI restore snapshot
	snapshotRestorePrefix = "restore"
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

	annPVBindCompleted     = "pv.kubernetes.io/bind-completed"
	annPVBoundByController = "pv.kubernetes.io/bound-by-controller"
	restoreUIDLabel        = "restoreUID"
	pureCSIProvisioner     = "pure-csi"
	vSphereCSIProvisioner  = "csi.vsphere.vmware.com"
	efsCSIProvisioner      = "efs.csi.aws.com"
	pureBackendParam       = "backend"
	pureFileParam          = "file"
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
	snapshotter    snapshotter.Driver

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

	if c.isCSIInstalled() {
		logrus.Infof("Creating default CSI SnapshotClasses")
		err = c.createDefaultSnapshotClasses()
		if err != nil {
			return err
		}
	} else {
		logrus.Infof("CSI VolumeSnapshotClass CRD does not exist, skipping default SnapshotClass creation")
	}

	c.snapshotter, err = snapshotter.NewCSIDriver()
	if err != nil {
		return err
	}

	return nil
}

func (c *csi) isCSIInstalled() bool {
	_, err := c.snapshotClient.SnapshotV1beta1().VolumeSnapshotClasses().List(context.TODO(), metav1.ListOptions{})
	return err == nil
}

func (c *csi) createDefaultSnapshotClasses() error {
	config, err := rest.InClusterConfig()
	if err != nil {
		return fmt.Errorf("failed to get config for creating default CSI snapshot classes: %v", err)
	}

	k8sClient, err := clientset.NewForConfig(config)
	if err != nil {
		return fmt.Errorf("failed to get client for creating default CSI snapshot classes: %v", err)
	}

	// Get all drivers
	driverList, err := k8sClient.StorageV1beta1().CSIDrivers().List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("failed to list all CSI drivers: %v", err)

	}

	// Create VolumeSnapshotClass for each driver
	for _, driver := range driverList.Items {
		// skip drivers with native supports
		if c.HasNativeVolumeDriverSupport(driver.Name) {
			logrus.Infof("CSI driver %s has native support, skipping default snapshotclass creation", driver.Name)
			continue
		}

		snapshotClassName := c.getDefaultSnapshotClassName(driver.Name)
		_, err := c.createVolumeSnapshotClass(snapshotClassName, driver.Name)
		if err != nil && !k8s_errors.IsAlreadyExists(err) {
			return fmt.Errorf("failed to create default snapshotclass %s: %v", snapshotClassName, err)
		} else if k8s_errors.IsAlreadyExists(err) {
			logrus.Infof("VolumeSnapshotClass %s already exists, skipping default snapshotclass creation", snapshotClassName)
		}
	}

	return nil
}

func (c *csi) String() string {
	return storkvolume.CSIDriverName
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

func (c *csi) HasNativeVolumeDriverSupport(driverName string) bool {
	return driverName == snapv1.PortworxCsiProvisionerName ||
		driverName == snapv1.PortworxCsiDeprecatedProvisionerName ||
		driverName == "pd.csi.storage.gke.io" ||
		driverName == "ebs.csi.aws.com" ||
		driverName == "disk.csi.azure.com"
}

func (c *csi) IsDriverWithoutSnapshotSupport(pv *v1.PersistentVolume) bool {
	driverName := pv.Spec.CSI.Driver
	// pure FB csi driver does not support snapshot
	if driverName == pureCSIProvisioner {
		if pv.Spec.CSI.VolumeAttributes[pureBackendParam] == pureFileParam {
			return true
		}
	}
	// vSphere and efs does not support snapshot
	if driverName == vSphereCSIProvisioner || driverName == efsCSIProvisioner {
		return true
	}
	return false
}

func (c *csi) OwnsPV(pv *v1.PersistentVolume) bool {
	// check if CSI volume
	if pv.Spec.CSI != nil {
		// We support certain CSI drivers natively
		if c.HasNativeVolumeDriverSupport(pv.Spec.CSI.Driver) {
			return false
		}
		// If the CSI driver does not support snapshot feature, we will return false,
		// It will default to kdmp generic backup.
		if c.IsDriverWithoutSnapshotSupport(pv) {
			return false
		}

		log.PVLog(pv).Tracef("CSI Owns PV: %s", pv.Name)
		return true
	}

	log.PVLog(pv).Tracef("CSI does not own PV: %s", pv.Name)
	return false
}

func (c *csi) getDefaultSnapshotClassName(driverName string) string {
	return snapshotClassNamePrefix + driverName
}

func (c *csi) getSnapshotClassName(
	backup *storkapi.ApplicationBackup,
	driverName string,
) string {
	if snapshotClassName, ok := backup.Spec.Options[optCSISnapshotClassName]; ok {
		return snapshotClassName
	}
	if driverName != "" {
		return c.getDefaultSnapshotClassName(driverName)
	}
	return ""
}

func (c *csi) getVolumeSnapshotClass(snapshotClassName string) (*kSnapshotv1beta1.VolumeSnapshotClass, error) {
	return c.snapshotClient.SnapshotV1beta1().VolumeSnapshotClasses().Get(context.TODO(), snapshotClassName, metav1.GetOptions{})
}

func (c *csi) createVolumeSnapshotClass(snapshotClassName, driverName string) (*kSnapshotv1beta1.VolumeSnapshotClass, error) {
	return c.snapshotClient.SnapshotV1beta1().VolumeSnapshotClasses().Create(context.TODO(), &kSnapshotv1beta1.VolumeSnapshotClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: snapshotClassName,
		},
		Driver:         driverName,
		DeletionPolicy: kSnapshotv1beta1.VolumeSnapshotContentRetain,
	}, metav1.CreateOptions{})
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
			_, err = c.snapshotClient.SnapshotV1beta1().VolumeSnapshotClasses().Update(context.TODO(), vsClass, metav1.UpdateOptions{})
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
		volumeInfo.PersistentVolumeClaimUID = string(pvc.UID)
		volumeInfo.Namespace = pvc.Namespace
		volumeInfo.DriverName = storkvolume.CSIDriverName
		volumeInfo.Volume = pvc.Spec.VolumeName
		volumeInfos = append(volumeInfos, volumeInfo)

		vsName := c.getBackupSnapshotName(&pvc, backup)
		_, _, csiDriverName, err := c.snapshotter.CreateSnapshot(
			snapshotter.Name(vsName),
			snapshotter.PVCName(pvc.Name),
			snapshotter.PVCNamespace(pvc.Namespace),
			snapshotter.SnapshotClassName(c.getSnapshotClassName(backup, "")),
		)
		if err != nil {
			c.cancelBackupDuringStartFailure(backup, volumeInfos)
			return nil, fmt.Errorf("failed to ensure volumesnapshotclass was created: %v", err)
		}

		volumeInfo.Options[optCSIDriverName] = csiDriverName
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

func (c *csi) getBackupSnapshotName(pvc *v1.PersistentVolumeClaim, backup *storkapi.ApplicationBackup) string {
	return fmt.Sprintf("%s-%s-%s", snapshotBackupPrefix, getUIDLastSection(backup.UID), getUIDLastSection(pvc.UID))
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

func (c *csi) getRestoreUIDLabelSelector(restore *storkapi.ApplicationRestore) string {
	return fmt.Sprintf("%s=%s", restoreUIDLabel, string(restore.GetUID()))
}

func (c *csi) getRestoreUIDLabels(restore *storkapi.ApplicationRestore, labels map[string]string) map[string]string {
	if labels == nil {
		labels = make(map[string]string)
	}
	labels[restoreUIDLabel] = string(restore.GetUID())
	return labels
}

func (c *csi) cleanupSnapshotsForRestore(
	restore *storkapi.ApplicationRestore,
	retainContent bool,
) error {
	vsMap := make(map[string]*kSnapshotv1beta1.VolumeSnapshot)
	vsContentMap := make(map[string]*kSnapshotv1beta1.VolumeSnapshotContent)

	// Check all destination namespaces for VolumeSnapshots with restoreUID
	for _, ns := range restore.Spec.NamespaceMapping {
		vsList, err := c.snapshotClient.SnapshotV1beta1().VolumeSnapshots(ns).List(context.TODO(), metav1.ListOptions{
			LabelSelector: c.getRestoreUIDLabelSelector(restore),
		})
		if err != nil {
			return err
		}
		for _, vs := range vsList.Items {
			vsMap[vs.Name] = &vs
		}

	}

	// Check for VolumeSnapshotContents with restoreUID
	vsContentList, err := c.snapshotClient.SnapshotV1beta1().VolumeSnapshotContents().List(context.TODO(), metav1.ListOptions{
		LabelSelector: c.getRestoreUIDLabelSelector(restore),
	})
	if err != nil {
		return err
	}
	for _, vsContent := range vsContentList.Items {
		vsContentMap[vsContent.Name] = &vsContent
	}
	log.ApplicationRestoreLog(restore).Debugf("collected %v snapshots and %v snapshotcontents to clean", len(vsMap), len(vsContentMap))

	return c.cleanupSnapshots(vsMap, vsContentMap, retainContent)
}

func (c *csi) cleanupSnapshots(
	vsMap map[string]*kSnapshotv1beta1.VolumeSnapshot,
	vsContentMap map[string]*kSnapshotv1beta1.VolumeSnapshotContent,
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
			_, err := c.snapshotClient.SnapshotV1beta1().VolumeSnapshotContents().Update(context.TODO(), vsc, metav1.UpdateOptions{})
			if err != nil {
				return err
			}
		}
	}

	// delete all vs & vscontent
	for _, vs := range vsMap {
		err := c.snapshotClient.SnapshotV1beta1().VolumeSnapshots(vs.Namespace).Delete(context.TODO(), vs.Name, metav1.DeleteOptions{})
		if k8s_errors.IsNotFound(err) {
			continue
		} else if err != nil {
			return err
		}
	}
	for _, vsc := range vsContentMap {
		err := c.snapshotClient.SnapshotV1beta1().VolumeSnapshotContents().Delete(context.TODO(), vsc.Name, metav1.DeleteOptions{})
		if k8s_errors.IsNotFound(err) {
			continue
		} else if err != nil {
			return err
		}
	}

	logrus.Debugf("started clean up of %v snapshots and %v snapshotcontents", len(vsMap), len(vsContentMap))
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
		if vInfo.DriverName != storkvolume.CSIDriverName {
			continue
		}

		// Get PVC we're checking the backup for
		pvc, err := core.Instance().GetPersistentVolumeClaim(vInfo.PersistentVolumeClaim, vInfo.Namespace)
		if err != nil {
			return nil, err
		}

		// Not in cleanup state. From here on, we're checking if the PVC snapshot has finished.
		snapshotName := c.getBackupSnapshotName(pvc, backup)

		snapshotInfo, err := c.snapshotter.SnapshotStatus(
			snapshotName,
			vInfo.Namespace,
		)
		if err != nil {
			vInfo.Reason = snapshotInfo.Reason
			vInfo.Status = mapSnapshotInfoStatus(snapshotInfo.Status)
			anyFailed = true
			volumeInfos = append(volumeInfos, vInfo)
			continue
		}

		snapshot, ok := snapshotInfo.SnapshotRequest.(*kSnapshotv1beta1.VolumeSnapshot)
		if !ok {
			vInfo.Reason = "failed to map volumesnapshot object"
			vInfo.Status = storkapi.ApplicationBackupStatusFailed
			anyFailed = true
			volumeInfos = append(volumeInfos, vInfo)
			continue
		}
		vsMap[vInfo.BackupID] = snapshot

		if snapshotInfo.Status == snapshotter.StatusReady {
			snapshotContent, ok := snapshotInfo.Content.(*kSnapshotv1beta1.VolumeSnapshotContent)
			if !ok {
				vInfo.Reason = "failed to map volumesnapshotcontent object"
				vInfo.Status = storkapi.ApplicationBackupStatusFailed
				anyFailed = true
				volumeInfos = append(volumeInfos, vInfo)
				continue
			}
			vsContentMap[vInfo.BackupID] = snapshotContent
			vInfo.Options[optVolumeSnapshotContentName] = snapshotContent.Name
			snapshotClass, ok := snapshotInfo.Class.(*kSnapshotv1beta1.VolumeSnapshotClass)
			if !ok {
				vInfo.Reason = "failed to map volumesnapshotcontent object"
				vInfo.Status = storkapi.ApplicationBackupStatusFailed
				anyFailed = true
				volumeInfos = append(volumeInfos, vInfo)
				continue
			}
			vsClassMap[snapshotClass.Name] = snapshotClass
			vInfo.ActualSize = uint64(snapshotInfo.Size)
			vInfo.TotalSize = uint64(snapshotInfo.Size)
		} else if snapshotInfo.Status == snapshotter.StatusInProgress {
			anyInProgress = true
			vInfo.ActualSize = uint64(snapshotInfo.Size)
			vInfo.TotalSize = uint64(snapshotInfo.Size)
		}
		vInfo.Reason = snapshotInfo.Reason
		vInfo.Status = mapSnapshotInfoStatus(snapshotInfo.Status)
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
	if !anyInProgress && len(vsContentMap) > 0 && len(vsMap) > 0 {
		err := c.uploadCSIBackupObject(backup, vsMap, vsContentMap, vsClassMap)
		if err != nil {
			return nil, err
		}
		log.ApplicationBackupLog(backup).Debugf("finished and uploaded %v snapshots and %v snapshotcontents", len(vsMap), len(vsContentMap))
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
			if vInfo.DriverName != storkvolume.CSIDriverName {
				continue
			}
			snapshotName := vInfo.BackupID

			// Delete VS
			err := c.snapshotClient.SnapshotV1beta1().VolumeSnapshots(backup.Namespace).Delete(context.TODO(), snapshotName, metav1.DeleteOptions{})
			if !k8s_errors.IsNotFound(err) {
				log.ApplicationBackupLog(backup).Warnf("Cancel backup failed to delete volumesnapshot %s: %v", snapshotName, err)
			}

			// Get VSC, update to delete policy, and delete it
			vscName := vInfo.Options[optVolumeSnapshotContentName]
			if vscName != "" {
				snapshotContent, err := c.snapshotClient.SnapshotV1beta1().VolumeSnapshotContents().Get(context.TODO(), vscName, metav1.GetOptions{})
				if !k8s_errors.IsNotFound(err) {
					log.ApplicationBackupLog(backup).Debugf("Cancel Backup failed to find snapshotcontent: %s", vscName)
				} else if err == nil {
					// update snapshot content if we found one
					snapshotContent.UID = ""
					snapshotContent.Spec.DeletionPolicy = kSnapshotv1beta1.VolumeSnapshotContentDelete
					_, err = c.snapshotClient.SnapshotV1beta1().VolumeSnapshotContents().Update(context.TODO(), snapshotContent, metav1.UpdateOptions{})
					if err != nil {
						return fmt.Errorf("failed to update VolumeSnapshotContent %v with deletion policy", snapshotContent.Name)
					}

					err = c.snapshotClient.SnapshotV1beta1().VolumeSnapshotContents().Delete(context.TODO(), vscName, metav1.DeleteOptions{})
					if err != nil {
						if !k8s_errors.IsNotFound(err) {
							log.ApplicationBackupLog(backup).Warnf("Cancel backup failed to delete volumesnapshotcontent %s: %v", vscName, err)
						}
					}
				}
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

func (c *csi) DeleteBackup(backup *storkapi.ApplicationBackup) (bool, error) {
	// if successful, re-create VS and VSC
	backupSuccessful := backup.Status.Status == storkapi.ApplicationBackupStatusSuccessful

	// collect all volumesnapshots and volumesnapshotcontents
	snapshotClassCreatedForDriver := make(map[string]bool)
	csiBackupObject, err := c.getCSIBackupObject(backup.Name, backup.Namespace)
	if err != nil {
		return true, err
	}

	for _, vInfo := range backup.Status.Volumes {
		if vInfo.DriverName != storkvolume.CSIDriverName {
			continue
		}
		if backupSuccessful {
			err = c.recreateSnapshotForDeletion(backup, vInfo, csiBackupObject, snapshotClassCreatedForDriver)
			if err != nil {
				return true, err
			}
		}
		vs, err := csiBackupObject.GetVolumeSnapshot(vInfo.BackupID)
		if err != nil {
			return true, fmt.Errorf("failed to find Snapshot for backup %s: %s", vInfo.BackupID, err.Error())
		}

		err = c.snapshotter.DeleteSnapshot(
			vs.Name,
			vInfo.Namespace,
			false, // retain snapshot content
		)
		if err != nil {
			return true, err
		}
		log.ApplicationBackupLog(backup).Debugf("deleted %v snapshot for backup %s", vs.Name, string(backup.UID))
	}

	err = c.cleanupBackupLocation(backup)
	if err != nil {
		return true, err
	}
	log.ApplicationBackupLog(backup).Debugf("cleaned up objects for backup %s", string(backup.UID))

	return true, nil
}

func (c *csi) UpdateMigratedPersistentVolumeSpec(
	pv *v1.PersistentVolume,
) (*v1.PersistentVolume, error) {
	return pv, nil
}

func (c *csi) getRestoreStorageClasses(backup *storkapi.ApplicationBackup, resources []runtime.Unstructured) ([]runtime.Unstructured, error) {
	storageClasses := make([]storagev1.StorageClass, 0)
	storageClassesBytes, err := c.downloadObject(backup, backup.Spec.BackupLocation, backup.Namespace, storageClassesObjectName)
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(storageClassesBytes, &storageClasses)
	if err != nil {
		return nil, err
	}

	restoreObjects := make([]runtime.Unstructured, 0)
	for _, sc := range storageClasses {
		content, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&sc)
		if err != nil {
			return nil, fmt.Errorf("error converting from StorageClass to runtime.Unstructured: %v", err)
		}
		obj := &unstructured.Unstructured{}
		obj.SetUnstructuredContent(content)

		restoreObjects = append(restoreObjects, runtime.Unstructured(obj))
	}

	return restoreObjects, nil
}

// GetPreRestoreResources gets all storage classes needed
// in order to restore the backed up PVCs
func (c *csi) GetPreRestoreResources(
	backup *storkapi.ApplicationBackup,
	restore *storkapi.ApplicationRestore,
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

func (c *csi) restoreVolumeSnapshotClass(vsClass *kSnapshotv1beta1.VolumeSnapshotClass) (*kSnapshotv1beta1.VolumeSnapshotClass, error) {
	vsClass.ResourceVersion = ""
	vsClass.UID = ""
	newVSClass, err := c.snapshotClient.SnapshotV1beta1().VolumeSnapshotClasses().Create(context.TODO(), vsClass, metav1.CreateOptions{})
	if err != nil {
		if k8s_errors.IsAlreadyExists(err) {
			return vsClass, nil
		}
		return nil, err
	}

	return newVSClass, nil
}

func (c *csi) restoreVolumeSnapshot(
	namespace string,
	vs *kSnapshotv1beta1.VolumeSnapshot,
	vsc *kSnapshotv1beta1.VolumeSnapshotContent,
) (*kSnapshotv1beta1.VolumeSnapshot, error) {
	vs.ResourceVersion = ""
	vs.Spec.Source.PersistentVolumeClaimName = nil
	vs.Spec.Source.VolumeSnapshotContentName = &vsc.Name
	vs.Namespace = namespace
	vs, err := c.snapshotClient.SnapshotV1beta1().VolumeSnapshots(namespace).Create(context.TODO(), vs, metav1.CreateOptions{})
	if err != nil {
		if k8s_errors.IsAlreadyExists(err) {
			return vs, nil
		}
		return nil, err
	}

	return vs, nil
}

func (c *csi) restoreVolumeSnapshotContent(
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
	vsc, err := c.snapshotClient.SnapshotV1beta1().VolumeSnapshotContents().Create(context.TODO(), vsc, metav1.CreateOptions{})
	if err != nil {
		if k8s_errors.IsAlreadyExists(err) {
			return vsc, nil
		}
		return nil, err
	}

	return vsc, nil
}

func getUIDLastSection(uid types.UID) string {
	parts := strings.Split(string(uid), "-")
	uidLastSection := parts[len(parts)-1]

	if uidLastSection == "" {
		uidLastSection = string(uid)
	}
	return uidLastSection
}

func (c *csi) getRestoreSnapshotName(existingSnapshotUID types.UID, restoreUID types.UID) string {
	return fmt.Sprintf("%s-vs-%s-%s", snapshotRestorePrefix, getUIDLastSection(restoreUID), getUIDLastSection(existingSnapshotUID))
}

func (c *csi) getRestoreSnapshotContentName(existingSnapshotUID types.UID, restoreUID types.UID) string {
	return fmt.Sprintf("%s-vsc-%s-%s", snapshotRestorePrefix, getUIDLastSection(restoreUID), getUIDLastSection(existingSnapshotUID))
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
		if vbInfo.DriverName != storkvolume.CSIDriverName {
			continue
		}

		vrInfo := &storkapi.ApplicationRestoreVolumeInfo{}
		log.ApplicationRestoreLog(restore).Debugf("restoring CSI volume %s", vbInfo.BackupID)

		// Find PVC from resources.json
		pvc, err := c.findPVCInResources(resources, vbInfo.PersistentVolumeClaim, vbInfo.Namespace)
		if err != nil {
			return nil, fmt.Errorf("failed to find pvc %s in resources: %v", vbInfo.PersistentVolumeClaim, err.Error())
		}

		// Get VSC and VS
		snapshotID := vbInfo.BackupID
		vsc, err := csiBackupObject.GetVolumeSnapshotContent(snapshotID)
		if err != nil {
			return nil, err
		}
		vsc.Name = c.getRestoreSnapshotContentName(vsc.UID, restore.UID)

		vs, err := csiBackupObject.GetVolumeSnapshot(snapshotID)
		if err != nil {
			return nil, fmt.Errorf("failed to retrieve volume snapshot for snapshotID %s", snapshotID)
		}
		vs.Name = c.getRestoreSnapshotName(vs.UID, restore.UID)

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
		vs.ObjectMeta.Labels = c.getRestoreUIDLabels(restore, vs.ObjectMeta.Labels)
		vs, err = c.restoreVolumeSnapshot(destNamespace, vs, vsc)
		if err != nil {
			return nil, err
		}
		log.ApplicationRestoreLog(restore).Debugf("created vs: %s", vs.Name)

		// Create VSC, bound to VS
		vsc.ObjectMeta.Labels = c.getRestoreUIDLabels(restore, vsc.ObjectMeta.Labels)
		vsc, err = c.restoreVolumeSnapshotContent(destNamespace, vs, vsc)
		if err != nil {
			return nil, err
		}
		log.ApplicationRestoreLog(restore).Debugf("created vsc: %s", vsc.Name)

		pvc, err = c.snapshotter.RestoreVolumeClaim(
			snapshotter.RestoreSnapshotName(vs.Name),
			snapshotter.RestoreNamespace(c.getDestinationNamespace(restore, pvc.Namespace)),
			snapshotter.PVC(*pvc),
		)
		if err != nil {
			return nil, fmt.Errorf("failed to restore pvc %s: %v", vbInfo.PersistentVolumeClaim, err.Error())
		}
		log.ApplicationRestoreLog(restore).Debugf("created pvc: %s", pvc.Name)

		// Populate volumeRestoreInfo
		vrInfo.DriverName = storkvolume.CSIDriverName
		vrInfo.PersistentVolumeClaim = pvc.Name
		vrInfo.PersistentVolumeClaimUID = string(pvc.UID)
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
	preRestoreObjects []runtime.Unstructured,
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
		// if any failed during
		cleanupErr := c.cleanupSnapshotsForRestore(restore, true)
		if cleanupErr != nil {
			log.ApplicationRestoreLog(restore).Errorf("cleaing up failed restore failed: %v", cleanupErr)
		}
		return nil, err
	}

	return volumeRestoreInfos, nil
}

func (c *csi) CancelRestore(restore *storkapi.ApplicationRestore) error {
	for _, vrInfo := range restore.Status.Volumes {
		if vrInfo.DriverName != storkvolume.CSIDriverName {
			continue
		}
		pvcRestoreSucceeded := (vrInfo.Status == storkapi.ApplicationRestoreStatusPartialSuccess || vrInfo.Status == storkapi.ApplicationRestoreStatusSuccessful)

		// Only clean up dangling PVC if it's restore did not succeed
		if !pvcRestoreSucceeded {
			destNamespace := c.getDestinationNamespace(restore, vrInfo.SourceNamespace)
			err := c.snapshotter.CancelRestore(vrInfo.PersistentVolumeClaim, destNamespace)
			if err != nil {
				return err
			}
		}
	}

	err := c.cleanupSnapshotsForRestore(restore, true)
	if err != nil {
		return fmt.Errorf("failed to clean CSI snapshots: %v", err)
	}

	return nil
}

func (c *csi) GetRestoreStatus(restore *storkapi.ApplicationRestore) ([]*storkapi.ApplicationRestoreVolumeInfo, error) {
	volumeInfos := make([]*storkapi.ApplicationRestoreVolumeInfo, 0)
	var anyInProgress bool
	var anyFailed bool

	for _, vrInfo := range restore.Status.Volumes {
		if vrInfo.DriverName != storkvolume.CSIDriverName {
			continue
		}
		// Handle namespace mapping
		destNamespace := c.getDestinationNamespace(restore, vrInfo.SourceNamespace)

		restoreInfo, err := c.snapshotter.RestoreStatus(vrInfo.PersistentVolumeClaim, destNamespace)
		if err != nil {
			return nil, err
		}

		switch restoreInfo.Status {
		case snapshotter.StatusInProgress:
			anyInProgress = true
			vrInfo.Status = storkapi.ApplicationRestoreStatusInProgress
			vrInfo.Reason = restoreInfo.Reason
		case snapshotter.StatusFailed:
			anyFailed = true
			vrInfo.Status = storkapi.ApplicationRestoreStatusFailed
			vrInfo.Reason = restoreInfo.Reason
		case snapshotter.StatusReady:
			vrInfo.TotalSize = restoreInfo.Size
			vrInfo.RestoreVolume = restoreInfo.VolumeName
		}
		volumeInfos = append(volumeInfos, vrInfo)
	}

	// If none are in progress, we can safely cleanup our volumesnapshot objects
	if !anyInProgress {
		// Mark complete once cleanup has started
		for _, vrInfo := range restore.Status.Volumes {
			if vrInfo.DriverName != storkvolume.CSIDriverName {
				continue
			}
			vrInfo.Reason = fmt.Sprintf("Volume restore successful: PVC %s is bound", vrInfo.PersistentVolumeClaim)
			vrInfo.Status = storkapi.ApplicationRestoreStatusSuccessful
		}

		return volumeInfos, nil
	}

	if anyFailed {
		err := c.CancelRestore(restore)
		if err != nil {
			return nil, fmt.Errorf("failed to clean cancel restore: %v", err)
		}
		log.ApplicationRestoreLog(restore).Infof("cancel restore started for failed restore")
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

func mapSnapshotInfoStatus(status snapshotter.Status) storkapi.ApplicationBackupStatusType {
	switch status {
	case snapshotter.StatusInProgress:
		return storkapi.ApplicationBackupStatusInProgress
	case snapshotter.StatusReady:
		return storkapi.ApplicationBackupStatusSuccessful
	}
	return storkapi.ApplicationBackupStatusFailed
}

// CleanupBackupResources for specified backup
func (c *csi) CleanupBackupResources(backup *storkapi.ApplicationBackup) error {
	vsMap := make(map[string]*kSnapshotv1beta1.VolumeSnapshot)
	vsContentMap := make(map[string]*kSnapshotv1beta1.VolumeSnapshotContent)
	for _, vInfo := range backup.Status.Volumes {
		if vInfo.DriverName != storkvolume.CSIDriverName {
			continue
		}
		// Get PVC we're checking the backup for
		pvc, err := core.Instance().GetPersistentVolumeClaim(vInfo.PersistentVolumeClaim, vInfo.Namespace)
		if err != nil {
			return err
		}
		snapshotName := c.getBackupSnapshotName(pvc, backup)
		snapshotInfo, err := c.snapshotter.SnapshotStatus(
			snapshotName,
			vInfo.Namespace,
		)
		if err != nil {
			logrus.Warnf("unable to cleanup snapshot :%s, err:%v", snapshotName, err)
			continue
		}
		snapshot, ok := snapshotInfo.SnapshotRequest.(*kSnapshotv1beta1.VolumeSnapshot)
		if !ok {
			logrus.Warnf("failed to map volumesnapshot object")
			continue
		}
		vsMap[vInfo.BackupID] = snapshot
		if snapshotInfo.Status == snapshotter.StatusReady {
			snapshotContent, ok := snapshotInfo.Content.(*kSnapshotv1beta1.VolumeSnapshotContent)
			if !ok {
				logrus.Warnf("failed to map volumesnapshotcontent object")
				continue
			}
			vsContentMap[vInfo.BackupID] = snapshotContent
		}
	}
	// cleanup after a successful object upload
	err := c.cleanupSnapshots(vsMap, vsContentMap, true)
	if err != nil {
		logrus.Tracef("failed to cleanup snapshots: %v", err)
	}
	log.ApplicationBackupLog(backup).Tracef("started clean up of %v snapshots and %v snapshotcontents", len(vsMap), len(vsContentMap))
	return nil
}

// CleanupBackupResources for specified restore
func (c *csi) CleanupRestoreResources(restore *storkapi.ApplicationRestore) error {
	err := c.cleanupSnapshotsForRestore(restore, true)
	if err != nil {
		logrus.Tracef("failed to clean CSI snapshots: %v", err)
	}
	return nil
}

func init() {
	c := &csi{}
	err := c.Init(nil)
	if err != nil {
		logrus.Debugf("Error init'ing csi driver: %v", err)
	}
	if err := storkvolume.Register(storkvolume.CSIDriverName, c); err != nil {
		logrus.Panicf("Error registering csi volume driver: %v", err)
	}
}
