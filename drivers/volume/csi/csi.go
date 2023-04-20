package csi

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"

	kSnapshotv1 "github.com/kubernetes-csi/external-snapshotter/client/v4/apis/volumesnapshot/v1"
	kSnapshotv1beta1 "github.com/kubernetes-csi/external-snapshotter/client/v4/apis/volumesnapshot/v1beta1"
	kSnapshotClient "github.com/kubernetes-csi/external-snapshotter/client/v4/clientset/versioned"
	snapv1 "github.com/kubernetes-incubator/external-storage/snapshot/pkg/apis/crd/v1"
	snapshotVolume "github.com/kubernetes-incubator/external-storage/snapshot/pkg/volume"
	storkvolume "github.com/libopenstorage/stork/drivers/volume"
	storkapi "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/libopenstorage/stork/pkg/applicationmanager/controllers"
	"github.com/libopenstorage/stork/pkg/crypto"
	"github.com/libopenstorage/stork/pkg/errors"
	"github.com/libopenstorage/stork/pkg/k8sutils"
	"github.com/libopenstorage/stork/pkg/log"
	"github.com/libopenstorage/stork/pkg/objectstore"
	"github.com/libopenstorage/stork/pkg/snapshotter"
	"github.com/libopenstorage/stork/pkg/utils"
	"github.com/libopenstorage/stork/pkg/version"
	"github.com/portworx/kdmp/pkg/executor"
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
	// SnapshotBackupPrefix is appended to CSI backup snapshot
	SnapshotBackupPrefix = "backup"
	// snapshotRestorePrefix is appended to CSI restore snapshot
	snapshotRestorePrefix = "restore"
	// snapshotClassNamePrefix is the prefix for snapshot classes per CSI driver
	snapshotClassNamePrefix = "stork-csi-snapshot-class-"

	// SnapshotObjectName is the object stored for the volumesnapshot
	SnapshotObjectName = "snapshots.json"
	// StorageClassesObjectName is the object stored for storageclasses
	StorageClassesObjectName = "storageclasses.json"
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
)

// CsiBackupObject represents a backup of a series of CSI objects
type CsiBackupObject struct {
	VolumeSnapshots          interface{} `json:"volumeSnapshots"`
	VolumeSnapshotContents   interface{} `json:"volumeSnapshotContents"`
	VolumeSnapshotClasses    interface{} `json:"volumeSnapshotClasses"`
	V1VolumeSnapshotRequired bool
}

// BackupObjectv1Csi represents a backup of a series of v1 VolumeSnapshot CSI objects
type BackupObjectv1Csi struct {
	VolumeSnapshots        map[string]*kSnapshotv1.VolumeSnapshot        `json:"volumeSnapshots"`
	VolumeSnapshotContents map[string]*kSnapshotv1.VolumeSnapshotContent `json:"volumeSnapshotContents"`
	VolumeSnapshotClasses  map[string]*kSnapshotv1.VolumeSnapshotClass   `json:"volumeSnapshotClasses"`
	V1SnapshotRequired     bool
}

// BackupObjectv1beta1Csi represents a backup of a series of v1beta1 VolumeSnapshot CSI objects
type BackupObjectv1beta1Csi struct {
	VolumeSnapshots        map[string]*kSnapshotv1beta1.VolumeSnapshot        `json:"volumeSnapshots"`
	VolumeSnapshotContents map[string]*kSnapshotv1beta1.VolumeSnapshotContent `json:"volumeSnapshotContents"`
	VolumeSnapshotClasses  map[string]*kSnapshotv1beta1.VolumeSnapshotClass   `json:"volumeSnapshotClasses"`
	V1SnapshotRequired     bool
}

// GetVolumeSnapshotContent retrieves a backed up volume snapshot
func (cbo *CsiBackupObject) GetVolumeSnapshot(snapshotID string) (interface{}, error) {
	var vs interface{}
	var ok bool

	if cbo.V1VolumeSnapshotRequired {
		vs, ok = cbo.VolumeSnapshots.(map[string]*kSnapshotv1.VolumeSnapshot)[snapshotID]
	} else {
		vs, ok = cbo.VolumeSnapshots.(map[string]*kSnapshotv1beta1.VolumeSnapshot)[snapshotID]
	}
	if !ok {
		return nil, fmt.Errorf("failed to retrieve volume snapshot for snapshotID %s", snapshotID)
	}
	return vs, nil
}

// GetVolumeSnapshotContent retrieves a backed up volume snapshot content
func (cbo *CsiBackupObject) GetVolumeSnapshotContent(snapshotID string) (interface{}, error) {
	var vsc interface{}
	var ok bool

	if cbo.V1VolumeSnapshotRequired {
		vsc, ok = cbo.VolumeSnapshotContents.(map[string]*kSnapshotv1.VolumeSnapshotContent)[snapshotID]
	} else {
		vsc, ok = cbo.VolumeSnapshotContents.(map[string]*kSnapshotv1beta1.VolumeSnapshotContent)[snapshotID]
	}
	if !ok {
		return nil, fmt.Errorf("failed to retrieve volume snapshot content for snapshotID %s", snapshotID)
	}
	return vsc, nil
}

// GetVolumeSnapshotClass retrieves a backed up volume snapshot class
func (cbo *CsiBackupObject) GetVolumeSnapshotClass(snapshotID string) (interface{}, error) {
	var vsClass, vs interface{}
	var vsClassName string
	var ok bool

	if cbo.V1VolumeSnapshotRequired {
		vs, ok = cbo.VolumeSnapshots.(map[string]*kSnapshotv1.VolumeSnapshot)[snapshotID]
		if !ok {
			logrus.Errorf("failed to retrieve volume snapshot for snapshot ID %s", snapshotID)
			return nil, fmt.Errorf("failed to retrieve volume snapshot for snapshot ID %s", snapshotID)
		}
		if vs.(*kSnapshotv1.VolumeSnapshot).Spec.VolumeSnapshotClassName == nil {
			return nil, fmt.Errorf("failed to retrieve volume snapshot class for snapshot %s. Volume snapshot class is undefined", snapshotID)
		}
		vsClassName = *vs.(*kSnapshotv1.VolumeSnapshot).Spec.VolumeSnapshotClassName

		vsClass, ok = cbo.VolumeSnapshotClasses.(map[string]*kSnapshotv1.VolumeSnapshotClass)[vsClassName]
	} else {
		vs, ok = cbo.VolumeSnapshots.(map[string]*kSnapshotv1beta1.VolumeSnapshot)[snapshotID]
		if !ok {
			logrus.Errorf("failed to retrieve volume snapshot for snapshot ID %s", snapshotID)
			return nil, fmt.Errorf("failed to retrieve volume snapshot for snapshot ID %s", snapshotID)
		}
		if vs.(*kSnapshotv1beta1.VolumeSnapshot).Spec.VolumeSnapshotClassName == nil {
			return nil, fmt.Errorf("failed to retrieve volume snapshot class for snapshot %s. Volume snapshot class is undefined", snapshotID)
		}
		vsClassName = *vs.(*kSnapshotv1beta1.VolumeSnapshot).Spec.VolumeSnapshotClassName

		vsClass, ok = cbo.VolumeSnapshotClasses.(map[string]*kSnapshotv1beta1.VolumeSnapshotClass)[vsClassName]
	}
	if !ok {
		return nil, fmt.Errorf("failed to retrieve volume snapshot class for snapshotID %s", snapshotID)
	}

	return vsClass, nil
}

type csi struct {
	snapshotClient     *kSnapshotClient.Clientset
	snapshotter        snapshotter.Driver
	v1SnapshotRequired bool

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

	c.v1SnapshotRequired, err = version.RequiresV1VolumeSnapshot()
	if err != nil {
		return err
	}

	err = c.createDefaultSnapshotClasses()
	if err != nil {
		return err
	}

	c.snapshotter, err = snapshotter.NewCSIDriver()
	if err != nil {
		return err
	}

	return nil
}

func (c *csi) createDefaultSnapshotClasses() error {
	var existingSnapshotClassesDriverList []string
	if c.v1SnapshotRequired {
		existingSnapshotClasses, err := c.snapshotClient.SnapshotV1().VolumeSnapshotClasses().List(context.TODO(), metav1.ListOptions{})
		if err != nil {
			logrus.Infof("CSI v1 VolumeSnapshotClass CRD does not exist, skipping default SnapshotClass creation - error:  %v", err)
			return nil
		}
		for _, existingSnapClass := range existingSnapshotClasses.Items {
			logrus.Infof("Driver name %s", existingSnapClass.Driver)
			existingSnapshotClassesDriverList = append(existingSnapshotClassesDriverList, existingSnapClass.Driver)
		}
	} else {
		existingSnapshotClasses, err := c.snapshotClient.SnapshotV1beta1().VolumeSnapshotClasses().List(context.TODO(), metav1.ListOptions{})
		if err != nil {
			logrus.Infof("CSI v1beta1 VolumeSnapshotClass CRD does not exist, skipping default SnapshotClass creation - error: %v", err)
			return nil
		}
		for _, existingSnapClass := range existingSnapshotClasses.Items {
			logrus.Infof("Driver name %s", existingSnapClass.Driver)
			existingSnapshotClassesDriverList = append(existingSnapshotClassesDriverList, existingSnapClass.Driver)
		}
	}

	logrus.Infof("Creating default CSI SnapshotClasses")
	config, err := rest.InClusterConfig()
	if err != nil {
		return fmt.Errorf("failed to get config for creating default CSI snapshot classes: %v", err)
	}

	k8sClient, err := clientset.NewForConfig(config)
	if err != nil {
		return fmt.Errorf("failed to get client for creating default CSI snapshot classes: %v", err)
	}

	ok, err := version.RequiresV1CSIdriver()
	if err != nil {
		return err
	}
	var csiDriverNameList []string
	if ok {
		// Get all drivers
		driverList, err := k8sClient.StorageV1().CSIDrivers().List(context.TODO(), metav1.ListOptions{})
		if err != nil {
			return fmt.Errorf("failed to list all CSI drivers(V1): %v", err)
		}
		// Fill the driver names from CSIDrivers to list
		for _, driver := range driverList.Items {
			csiDriverNameList = append(csiDriverNameList, driver.Name)
		}
	} else {

		// Get all drivers
		driverList, err := k8sClient.StorageV1beta1().CSIDrivers().List(context.TODO(), metav1.ListOptions{})
		if err != nil {
			return fmt.Errorf("failed to list all CSI drivers(V1beta1): %v", err)
		}
		// Fill the driver names from CSIDrivers to list
		for _, driver := range driverList.Items {
			csiDriverNameList = append(csiDriverNameList, driver.Name)
		}
	}

	// Create VolumeSnapshotClass for each driver
	for _, driverName := range csiDriverNameList {
		// skip drivers with native supports
		if c.HasNativeVolumeDriverSupport(driverName) {
			logrus.Infof("CSI driver %s has native support, skipping default snapshotclass creation", driverName)
			continue
		}

		foundSnapClass := false
		for _, existingSnapClassDriverName := range existingSnapshotClassesDriverList {
			if driverName == existingSnapClassDriverName {
				logrus.Infof("CSI VolumeSnapshotClass exists for driver %v. Skipping creation of snapshotclass", driverName)
				foundSnapClass = true
				break
			}
		}
		if foundSnapClass {
			continue
		}
		snapshotClassName := c.getDefaultSnapshotClassName(driverName)
		_, err := c.createVolumeSnapshotClass(snapshotClassName, driverName)
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

func (c *csi) OwnsPVCForBackup(
	coreOps core.Ops,
	pvc *v1.PersistentVolumeClaim,
	cmBackupType string,
	crBackupType string,
	blType storkapi.BackupLocationType,
) bool {
	if cmBackupType == storkapi.ApplicationBackupGeneric || crBackupType == storkapi.ApplicationBackupGeneric {
		// If user has forced the backupType in config map or applicationbackup CR, default to generic always
		return false
	}
	return c.OwnsPVC(coreOps, pvc)
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

func (c *csi) OwnsPV(pv *v1.PersistentVolume) bool {
	// check if CSI volume
	if pv.Spec.CSI != nil {
		// We support certain CSI drivers natively
		if c.HasNativeVolumeDriverSupport(pv.Spec.CSI.Driver) {
			return false
		}
		// If the CSI driver does not support snapshot feature, we will return false,
		// It will default to kdmp generic backup.
		if storkvolume.IsCSIDriverWithoutSnapshotSupport(pv) {
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

func (c *csi) getVolumeSnapshotClass(snapshotClassName string) (interface{}, error) {
	if c.v1SnapshotRequired {
		return c.snapshotClient.SnapshotV1().VolumeSnapshotClasses().Get(context.TODO(), snapshotClassName, metav1.GetOptions{})
	}
	return c.snapshotClient.SnapshotV1beta1().VolumeSnapshotClasses().Get(context.TODO(), snapshotClassName, metav1.GetOptions{})
}

func (c *csi) createVolumeSnapshotClass(snapshotClassName, driverName string) (interface{}, error) {
	if c.v1SnapshotRequired {
		return c.snapshotClient.SnapshotV1().VolumeSnapshotClasses().Create(context.TODO(), &kSnapshotv1.VolumeSnapshotClass{
			ObjectMeta: metav1.ObjectMeta{
				Name: snapshotClassName,
			},
			Driver:         driverName,
			DeletionPolicy: kSnapshotv1.VolumeSnapshotContentRetain,
		}, metav1.CreateOptions{})
	}
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
		if c.v1SnapshotRequired {
			if vsClass.(*kSnapshotv1.VolumeSnapshotClass).DeletionPolicy == kSnapshotv1.VolumeSnapshotContentDelete {
				vsClass.(*kSnapshotv1.VolumeSnapshotClass).DeletionPolicy = kSnapshotv1.VolumeSnapshotContentRetain
				_, err = c.snapshotClient.SnapshotV1().VolumeSnapshotClasses().Update(context.TODO(), vsClass.(*kSnapshotv1.VolumeSnapshotClass), metav1.UpdateOptions{})
				if err != nil {
					return nil, err
				}
			}
		} else {
			if vsClass.(*kSnapshotv1beta1.VolumeSnapshotClass).DeletionPolicy == kSnapshotv1beta1.VolumeSnapshotContentDelete {
				vsClass.(*kSnapshotv1beta1.VolumeSnapshotClass).DeletionPolicy = kSnapshotv1beta1.VolumeSnapshotContentRetain
				_, err = c.snapshotClient.SnapshotV1beta1().VolumeSnapshotClasses().Update(context.TODO(), vsClass.(*kSnapshotv1beta1.VolumeSnapshotClass), metav1.UpdateOptions{})
				if err != nil {
					return nil, err
				}
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

	err = c.uploadObject(backup, StorageClassesObjectName, scBytes)
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
	funct := "StartBackup:"
	volumeInfos := make([]*storkapi.ApplicationBackupVolumeInfo, 0)
	var storageClasses []*storagev1.StorageClass
	storageClassAdded := make(map[string]bool)
	log.ApplicationBackupLog(backup).Debugf("started CSI backup: %v", backup.Name)

	nfs, err := utils.IsNFSBackuplocationType(backup.Namespace, backup.Spec.BackupLocation)
	if err != nil {
		logrus.Errorf("%v error in checking backuplocation type: %v", funct, err)
		return nil, err
	}

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
		// We should bail-out if snapshotter is not initialized right
		if c.snapshotter == nil {
			return nil, fmt.Errorf("found uninitialized snapshotter object")
		}
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
		if !nfs {
			// In the case of nfs backuplocation type, uploading of storageclass.json will
			// happen as part of resource exexutor job as part of resource stage.
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
	}
	if !nfs {
		// In the case of nfs backuplocation type, uploading of storageclass.json will
		// happen as part of resource exexutor job as part of resource stage.
		err := c.backupStorageClasses(storageClasses, backup)
		if err != nil {
			c.cancelBackupDuringStartFailure(backup, volumeInfos)
			return nil, fmt.Errorf("failed to backup storage classes: %v", err)
		}
	}

	return volumeInfos, nil
}

func (c *csi) getBackupSnapshotName(pvc *v1.PersistentVolumeClaim, backup *storkapi.ApplicationBackup) string {
	return fmt.Sprintf("%s-%s-%s", SnapshotBackupPrefix, utils.GetUIDLastSection(backup.UID), utils.GetUIDLastSection(pvc.UID))
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
		return fmt.Errorf("EncryptionKey is deprecated, use EncryptionKeyV2 instead")
	}
	if backupLocation.Location.EncryptionV2Key != "" {
		if data, err = crypto.Encrypt(data, backupLocation.Location.EncryptionV2Key); err != nil {
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
	vsMap interface{},
	vsContentMap interface{},
	vsClassMap interface{},
) error {
	var csiBackup interface{}
	v1VolumeSnapshotRequired, err := version.RequiresV1VolumeSnapshot()
	if err != nil {
		return fmt.Errorf("failed to get volumesnapshot version supported by cluster")
	}
	if v1VolumeSnapshotRequired {
		csiBackup = CsiBackupObject{
			VolumeSnapshots:          vsMap.(map[string]*kSnapshotv1.VolumeSnapshot),
			VolumeSnapshotContents:   vsContentMap.(map[string]*kSnapshotv1.VolumeSnapshotContent),
			VolumeSnapshotClasses:    vsClassMap.(map[string]*kSnapshotv1.VolumeSnapshotClass),
			V1VolumeSnapshotRequired: true,
		}
	} else {
		csiBackup = CsiBackupObject{
			VolumeSnapshots:          vsMap.(map[string]*kSnapshotv1beta1.VolumeSnapshot),
			VolumeSnapshotContents:   vsContentMap.(map[string]*kSnapshotv1beta1.VolumeSnapshotContent),
			VolumeSnapshotClasses:    vsClassMap.(map[string]*kSnapshotv1beta1.VolumeSnapshotClass),
			V1VolumeSnapshotRequired: false,
		}
	}

	var csiBackupBytes []byte

	csiBackupBytes, err = json.Marshal(csiBackup)
	if err != nil {
		return err
	}

	err = c.uploadObject(backup, SnapshotObjectName, csiBackupBytes)
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
	if c.v1SnapshotRequired {
		vsMap := make(map[string]*kSnapshotv1.VolumeSnapshot)
		vsContentMap := make(map[string]*kSnapshotv1.VolumeSnapshotContent)

		// Check all destination namespaces for VolumeSnapshots with restoreUID
		for _, ns := range restore.Spec.NamespaceMapping {
			vsList, err := c.snapshotClient.SnapshotV1().VolumeSnapshots(ns).List(context.TODO(), metav1.ListOptions{
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
		vsContentList, err := c.snapshotClient.SnapshotV1().VolumeSnapshotContents().List(context.TODO(), metav1.ListOptions{
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
	vsMap interface{},
	vsContentMap interface{},
	retainContent bool,
) error {
	if c.v1SnapshotRequired {
		desiredRetainPolicy := kSnapshotv1.VolumeSnapshotContentRetain
		if !retainContent {
			desiredRetainPolicy = kSnapshotv1.VolumeSnapshotContentDelete
		}

		// ensure all vscontent have the desired delete policy
		for _, vsc := range vsContentMap.(map[string]*kSnapshotv1.VolumeSnapshotContent) {
			if vsc.Spec.DeletionPolicy != desiredRetainPolicy {
				vsc.UID = ""
				vsc.Spec.DeletionPolicy = desiredRetainPolicy
				_, err := c.snapshotClient.SnapshotV1().VolumeSnapshotContents().Update(context.TODO(), vsc, metav1.UpdateOptions{})
				if err != nil {
					return err
				}
			}
		}

		// delete all vs & vscontent
		for _, vs := range vsMap.(map[string]*kSnapshotv1.VolumeSnapshot) {
			err := c.snapshotClient.SnapshotV1().VolumeSnapshots(vs.Namespace).Delete(context.TODO(), vs.Name, metav1.DeleteOptions{})
			if k8s_errors.IsNotFound(err) {
				continue
			} else if err != nil {
				return err
			}
		}
		for _, vsc := range vsContentMap.(map[string]*kSnapshotv1.VolumeSnapshotContent) {
			err := c.snapshotClient.SnapshotV1beta1().VolumeSnapshotContents().Delete(context.TODO(), vsc.Name, metav1.DeleteOptions{})
			if k8s_errors.IsNotFound(err) {
				continue
			} else if err != nil {
				return err
			}
		}

		logrus.Debugf("started clean up of %v snapshots and %v snapshotcontents", len(vsMap.(map[string]*kSnapshotv1.VolumeSnapshot)), len(vsContentMap.(map[string]*kSnapshotv1.VolumeSnapshotContent)))
		return nil
	}

	desiredRetainPolicy := kSnapshotv1beta1.VolumeSnapshotContentRetain
	if !retainContent {
		desiredRetainPolicy = kSnapshotv1beta1.VolumeSnapshotContentDelete
	}

	// ensure all vscontent have the desired delete policy
	for _, vsc := range vsContentMap.(map[string]*kSnapshotv1beta1.VolumeSnapshotContent) {
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
	for _, vs := range vsMap.(map[string]*kSnapshotv1beta1.VolumeSnapshot) {
		err := c.snapshotClient.SnapshotV1beta1().VolumeSnapshots(vs.Namespace).Delete(context.TODO(), vs.Name, metav1.DeleteOptions{})
		if k8s_errors.IsNotFound(err) {
			continue
		} else if err != nil {
			return err
		}
	}
	for _, vsc := range vsContentMap.(map[string]*kSnapshotv1beta1.VolumeSnapshotContent) {
		err := c.snapshotClient.SnapshotV1beta1().VolumeSnapshotContents().Delete(context.TODO(), vsc.Name, metav1.DeleteOptions{})
		if k8s_errors.IsNotFound(err) {
			continue
		} else if err != nil {
			return err
		}
	}

	logrus.Debugf("started clean up of %v snapshots and %v snapshotcontents", len(vsMap.(map[string]*kSnapshotv1beta1.VolumeSnapshot)), len(vsContentMap.(map[string]*kSnapshotv1beta1.VolumeSnapshotContent)))
	return nil
}

func (c *csi) GetBackupStatus(backup *storkapi.ApplicationBackup) ([]*storkapi.ApplicationBackupVolumeInfo, error) {
	funct := "csi GetBackupStatus"
	if c.snapshotClient == nil {
		if err := c.Init(nil); err != nil {
			return nil, err
		}
	}

	volumeInfos := make([]*storkapi.ApplicationBackupVolumeInfo, 0)
	var anyInProgress bool
	var anyFailed bool

	var vsMap, vsContentMap, vsClassMap interface{}
	var vsMapLen, vsContentMapLen int

	if c.v1SnapshotRequired {
		vsMap = make(map[string]*kSnapshotv1.VolumeSnapshot)
		vsContentMap = make(map[string]*kSnapshotv1.VolumeSnapshotContent)
		vsClassMap = make(map[string]*kSnapshotv1.VolumeSnapshotClass)
	} else {
		vsMap = make(map[string]*kSnapshotv1beta1.VolumeSnapshot)
		vsContentMap = make(map[string]*kSnapshotv1beta1.VolumeSnapshotContent)
		vsClassMap = make(map[string]*kSnapshotv1beta1.VolumeSnapshotClass)
	}
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
		if c.v1SnapshotRequired {
			snapshot, ok := snapshotInfo.SnapshotRequest.(*kSnapshotv1.VolumeSnapshot)
			if !ok {
				vInfo.Reason = "failed to map volumesnapshot object"
				vInfo.Status = storkapi.ApplicationBackupStatusFailed
				anyFailed = true
				volumeInfos = append(volumeInfos, vInfo)
				continue
			}
			vsMap.(map[string]*kSnapshotv1.VolumeSnapshot)[vInfo.BackupID] = snapshot
		} else {
			snapshot, ok := snapshotInfo.SnapshotRequest.(*kSnapshotv1beta1.VolumeSnapshot)
			if !ok {
				vInfo.Reason = "failed to map volumesnapshot object"
				vInfo.Status = storkapi.ApplicationBackupStatusFailed
				anyFailed = true
				volumeInfos = append(volumeInfos, vInfo)
				continue
			}
			vsMap.(map[string]*kSnapshotv1beta1.VolumeSnapshot)[vInfo.BackupID] = snapshot
		}

		if snapshotInfo.Status == snapshotter.StatusReady {
			if c.v1SnapshotRequired {
				snapshotContent, ok := snapshotInfo.Content.(*kSnapshotv1.VolumeSnapshotContent)
				if !ok {
					vInfo.Reason = "failed to map volumesnapshotcontent object"
					vInfo.Status = storkapi.ApplicationBackupStatusFailed
					anyFailed = true
					volumeInfos = append(volumeInfos, vInfo)
					continue
				}
				vsContentMap.(map[string]*kSnapshotv1.VolumeSnapshotContent)[vInfo.BackupID] = snapshotContent
				vInfo.Options[optVolumeSnapshotContentName] = snapshotContent.Name
			} else {
				snapshotContent, ok := snapshotInfo.Content.(*kSnapshotv1beta1.VolumeSnapshotContent)
				if !ok {
					vInfo.Reason = "failed to map volumesnapshotcontent object"
					vInfo.Status = storkapi.ApplicationBackupStatusFailed
					anyFailed = true
					volumeInfos = append(volumeInfos, vInfo)
					continue
				}
				vsContentMap.(map[string]*kSnapshotv1beta1.VolumeSnapshotContent)[vInfo.BackupID] = snapshotContent
				vInfo.Options[optVolumeSnapshotContentName] = snapshotContent.Name
			}
			if c.v1SnapshotRequired {
				snapshotClass, ok := snapshotInfo.Class.(*kSnapshotv1.VolumeSnapshotClass)
				if !ok {
					vInfo.Reason = "failed to map volumesnapshotcontent object"
					vInfo.Status = storkapi.ApplicationBackupStatusFailed
					anyFailed = true
					volumeInfos = append(volumeInfos, vInfo)
					continue
				}
				vsClassMap.(map[string]*kSnapshotv1.VolumeSnapshotClass)[snapshotClass.Name] = snapshotClass
			} else {
				snapshotClass, ok := snapshotInfo.Class.(*kSnapshotv1beta1.VolumeSnapshotClass)
				if !ok {
					vInfo.Reason = "failed to map volumesnapshotcontent object"
					vInfo.Status = storkapi.ApplicationBackupStatusFailed
					anyFailed = true
					volumeInfos = append(volumeInfos, vInfo)
					continue
				}
				vsClassMap.(map[string]*kSnapshotv1beta1.VolumeSnapshotClass)[snapshotClass.Name] = snapshotClass
			}
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

	if c.v1SnapshotRequired {
		vsMapLen = len(vsMap.(map[string]*kSnapshotv1.VolumeSnapshot))
		vsContentMapLen = len(vsContentMap.(map[string]*kSnapshotv1.VolumeSnapshotContent))
	} else {
		vsMapLen = len(vsMap.(map[string]*kSnapshotv1beta1.VolumeSnapshot))
		vsContentMapLen = len(vsContentMap.(map[string]*kSnapshotv1beta1.VolumeSnapshotContent))
	}

	nfs, err := utils.IsNFSBackuplocationType(backup.Namespace, backup.Spec.BackupLocation)
	if err != nil {
		logrus.Errorf("%v error in checking backuplocation type: %v", funct, err)
		return nil, err
	}
	// In the case of nfs backuplocation type, uploading of snapshot.json will
	// happen as part of resource exexutor job as part of resource stage.
	if !nfs {
		// if all have finished, add all VolumeSnapshot and VolumeSnapshotContent to objectstore
		if !anyInProgress && vsContentMapLen > 0 && vsMapLen > 0 {
			err := c.uploadCSIBackupObject(backup, vsMap, vsContentMap, vsClassMap)
			if err != nil {
				return nil, err
			}
			log.ApplicationBackupLog(backup).Debugf("finished and uploaded %v snapshots and %v snapshotcontents", vsMapLen, vsContentMapLen)
		}
	}
	return volumeInfos, nil
}

func (c *csi) recreateSnapshotForDeletion(
	backup *storkapi.ApplicationBackup,
	vbInfo *storkapi.ApplicationBackupVolumeInfo,
	csiBackupObject *CsiBackupObject,
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
	if c.v1SnapshotRequired {
		log.ApplicationBackupLog(backup).Debugf("created volume snapshot class %s for backup %s deletion", vs.(*kSnapshotv1.VolumeSnapshot).Name, snapshotID)
	} else {
		log.ApplicationBackupLog(backup).Debugf("created volume snapshot class %s for backup %s deletion", vs.(*kSnapshotv1beta1.VolumeSnapshot).Name, snapshotID)
	}

	// Create VS, bound to VSC
	vs, err = c.restoreVolumeSnapshot(backup.Namespace, vs, vsc)
	if err != nil {
		return fmt.Errorf("failed to restore VolumeSnapshot for deletion: %s", err.Error())
	}
	//log.ApplicationBackupLog(backup).Debugf("created volume snapshot %s for backup %s deletion", vs.Name, snapshotID)

	// Create VSC
	_, err = c.restoreVolumeSnapshotContent(backup.Namespace, vs, vsc)
	if err != nil {
		return err
	}
	//log.ApplicationBackupLog(backup).Debugf("created volume snapshot content %s for backup %s deletion", vsc.Name, snapshotID)

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
			if c.v1SnapshotRequired {
				err := c.snapshotClient.SnapshotV1().VolumeSnapshots(backup.Namespace).Delete(context.TODO(), snapshotName, metav1.DeleteOptions{})
				if !k8s_errors.IsNotFound(err) {
					log.ApplicationBackupLog(backup).Warnf("Cancel backup failed to delete volumesnapshot %s: %v", snapshotName, err)
				}
			} else {
				err := c.snapshotClient.SnapshotV1beta1().VolumeSnapshots(backup.Namespace).Delete(context.TODO(), snapshotName, metav1.DeleteOptions{})
				if !k8s_errors.IsNotFound(err) {
					log.ApplicationBackupLog(backup).Warnf("Cancel backup failed to delete volumesnapshot %s: %v", snapshotName, err)
				}
			}

			// Get VSC, update to delete policy, and delete it
			vscName := vInfo.Options[optVolumeSnapshotContentName]
			if vscName != "" {
				if c.v1SnapshotRequired {
					snapshotContent, err := c.snapshotClient.SnapshotV1().VolumeSnapshotContents().Get(context.TODO(), vscName, metav1.GetOptions{})
					if !k8s_errors.IsNotFound(err) {
						log.ApplicationBackupLog(backup).Debugf("Cancel Backup failed to find snapshotcontent: %s", vscName)
					} else if err == nil {
						// update snapshot content if we found one
						snapshotContent.UID = ""
						snapshotContent.Spec.DeletionPolicy = kSnapshotv1.VolumeSnapshotContentDelete
						_, err = c.snapshotClient.SnapshotV1().VolumeSnapshotContents().Update(context.TODO(), snapshotContent, metav1.UpdateOptions{})
						if err != nil {
							return fmt.Errorf("failed to update VolumeSnapshotContent %v with deletion policy", snapshotContent.Name)
						}

						err = c.snapshotClient.SnapshotV1().VolumeSnapshotContents().Delete(context.TODO(), vscName, metav1.DeleteOptions{})
						if err != nil {
							if !k8s_errors.IsNotFound(err) {
								log.ApplicationBackupLog(backup).Warnf("Cancel backup failed to delete volumesnapshotcontent %s: %v", vscName, err)
							}
						}
					}
				} else {
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
		if err = bucket.Delete(context.TODO(), filepath.Join(objectPath, SnapshotObjectName)); err != nil && gcerrors.Code(err) != gcerrors.NotFound {
			return fmt.Errorf("error deleting resources for backup %v/%v: %v", backup.Namespace, backup.Name, err)
		}
		if err = bucket.Delete(context.TODO(), filepath.Join(objectPath, StorageClassesObjectName)); err != nil && gcerrors.Code(err) != gcerrors.NotFound {
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
		switch v := vs.(type) {
		case *kSnapshotv1beta1.VolumeSnapshot:
			err = c.snapshotter.DeleteSnapshot(
				vs.(*kSnapshotv1beta1.VolumeSnapshot).Name,
				vInfo.Namespace,
				false, // retain snapshot content
			)
			if err != nil {
				return true, err
			}
			log.ApplicationBackupLog(backup).Debugf("deleted %v snapshot for backup %s", vs.(*kSnapshotv1beta1.VolumeSnapshot).Name, string(backup.UID))
		case *kSnapshotv1.VolumeSnapshot:
			err = c.snapshotter.DeleteSnapshot(
				vs.(*kSnapshotv1.VolumeSnapshot).Name,
				vInfo.Namespace,
				false, // retain snapshot content
			)
			if err != nil {
				return true, err
			}
			log.ApplicationBackupLog(backup).Debugf("deleted %v snapshot for backup %s", vs.(*kSnapshotv1.VolumeSnapshot).Name, string(backup.UID))
		default:
			log.ApplicationBackupLog(backup).Errorf("unknown volume snapshot API version %s detected for snapshot %v and backup id %s", v, vs.(*kSnapshotv1.VolumeSnapshot).Name, string(backup.UID))
		}
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
	vInfo *storkapi.ApplicationRestoreVolumeInfo,
	namespaceMapping map[string]string,
) (*v1.PersistentVolume, error) {
	return pv, nil
}

func (c *csi) getRestoreStorageClasses(backup *storkapi.ApplicationBackup, resources []runtime.Unstructured, storageClassesBytes []byte) ([]runtime.Unstructured, error) {
	storageClasses := make([]storagev1.StorageClass, 0)
	err := json.Unmarshal(storageClassesBytes, &storageClasses)
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
	storageClassBytes []byte,
) ([]runtime.Unstructured, error) {
	return c.getRestoreStorageClasses(backup, resources, storageClassBytes)
}

func (c *csi) downloadObject(
	backup *storkapi.ApplicationBackup,
	backupLocation string,
	namespace string,
	objectName string,
) ([]byte, error) {

	funct := "downloadObject"
	var data []byte
	restoreLocation, err := storkops.Instance().GetBackupLocation(backupLocation, namespace)
	if err != nil {
		return nil, err
	}
	if restoreLocation.Location.Type == storkapi.BackupLocationNFS {
		// NFS backuplocation type.
		repo, err := executor.ParseCloudCred()
		if err != nil {
			logrus.Errorf("%s: error parsing cloud cred: %v", funct, err)
			return nil, err
		}
		bkpDir := filepath.Join(repo.Path, backup.Status.BackupPath)
		data, err = executor.DownloadObject(bkpDir, objectName, restoreLocation.Location.EncryptionV2Key)
		if err != nil {
			return nil, fmt.Errorf("error downloading resources: %v", err)
		}
		return data, nil
	} else {
		// Non NFS backuplocation type
		bucket, err := objectstore.GetBucket(restoreLocation)
		if err != nil {
			return nil, err
		}

		objectPath := backup.Status.BackupPath
		exists, err := bucket.Exists(context.TODO(), filepath.Join(objectPath, objectName))
		if err != nil || !exists {
			return nil, nil
		}

		data, err = bucket.ReadAll(context.TODO(), filepath.Join(objectPath, objectName))
		if err != nil {
			return nil, err
		}
		if restoreLocation.Location.EncryptionKey != "" {
			return nil, fmt.Errorf("EncryptionKey is deprecated, use EncryptionKeyV2 instead")
		}
		if restoreLocation.Location.EncryptionV2Key != "" {
			var decryptData []byte
			if decryptData, err = crypto.Decrypt(data, restoreLocation.Location.EncryptionV2Key); err != nil {
				logrus.Debugf("decrypt failed with: %v and returning the data as it is", err)
				return data, nil
			}
			return decryptData, nil
		}
	}

	return data, nil
}

// getRestoreSnapshotsAndContent retrieves the volumeSnapshots and
// volumeSnapshotContents associated with a backupID
func (c *csi) getCSIBackupObject(backupName, backupNamespace string) (*CsiBackupObject, error) {
	backup, err := storkops.Instance().GetApplicationBackup(backupName, backupNamespace)
	if err != nil {
		return nil, fmt.Errorf("error getting backup spec for CSI restore: %v", err)
	}

	backupObjectBytes, err := c.downloadObject(backup, backup.Spec.BackupLocation, backup.Namespace, SnapshotObjectName)
	if err != nil {
		return nil, err
	}
	cboCommon := &CsiBackupObject{}
	if c.v1SnapshotRequired {
		cbov1 := &BackupObjectv1Csi{}
		err = json.Unmarshal(backupObjectBytes, cbov1)
		if err != nil {
			return nil, err
		}
		cboCommon.VolumeSnapshots = cbov1.VolumeSnapshots
		cboCommon.VolumeSnapshotContents = cbov1.VolumeSnapshotContents
		cboCommon.VolumeSnapshotClasses = cbov1.VolumeSnapshotClasses
		cboCommon.V1VolumeSnapshotRequired = c.v1SnapshotRequired
		return cboCommon, nil
	}
	cbov1beta1 := &BackupObjectv1beta1Csi{}
	err = json.Unmarshal(backupObjectBytes, cbov1beta1)
	if err != nil {
		return nil, err
	}
	cboCommon.VolumeSnapshots = cbov1beta1.VolumeSnapshots
	cboCommon.VolumeSnapshotContents = cbov1beta1.VolumeSnapshotContents
	cboCommon.VolumeSnapshotClasses = cbov1beta1.VolumeSnapshotClasses
	cboCommon.V1VolumeSnapshotRequired = c.v1SnapshotRequired

	return cboCommon, nil
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

func (c *csi) restoreVolumeSnapshotClass(vsClass interface{}) (interface{}, error) {
	var newVSClass interface{}
	var err error
	if c.v1SnapshotRequired {
		vsClass.(*kSnapshotv1.VolumeSnapshotClass).ResourceVersion = ""
		vsClass.(*kSnapshotv1.VolumeSnapshotClass).UID = ""
		newVSClass, err = c.snapshotClient.SnapshotV1().VolumeSnapshotClasses().Create(context.TODO(), vsClass.(*kSnapshotv1.VolumeSnapshotClass), metav1.CreateOptions{})
		if err != nil {
			if k8s_errors.IsAlreadyExists(err) {
				return vsClass, nil
			}
			return nil, err
		}
	} else {
		vsClass.(*kSnapshotv1beta1.VolumeSnapshotClass).ResourceVersion = ""
		vsClass.(*kSnapshotv1beta1.VolumeSnapshotClass).UID = ""
		newVSClass, err = c.snapshotClient.SnapshotV1beta1().VolumeSnapshotClasses().Create(context.TODO(), vsClass.(*kSnapshotv1beta1.VolumeSnapshotClass), metav1.CreateOptions{})
		if err != nil {
			if k8s_errors.IsAlreadyExists(err) {
				return vsClass, nil
			}
			return nil, err
		}
	}
	return newVSClass, nil
}

func (c *csi) restoreVolumeSnapshot(
	namespace string,
	vs interface{},
	vsc interface{},
) (interface{}, error) {
	var err error
	if c.v1SnapshotRequired {
		vsObj, ok := vs.(*kSnapshotv1.VolumeSnapshot)
		if !ok {
			return nil, fmt.Errorf("failed to typecast volumesnapshot interface to v1 version structure")
		}
		vsObj.ResourceVersion = ""
		vsObj.Spec.Source.PersistentVolumeClaimName = nil
		vsObj.Spec.Source.VolumeSnapshotContentName = &vsc.(*kSnapshotv1.VolumeSnapshotContent).Name
		vsObj.Namespace = namespace
		vs, err = c.snapshotClient.SnapshotV1().VolumeSnapshots(namespace).Create(context.TODO(), vsObj, metav1.CreateOptions{})
		if err != nil {
			if k8s_errors.IsAlreadyExists(err) {
				vs, err = c.snapshotClient.SnapshotV1().VolumeSnapshots(namespace).Get(context.TODO(), vsObj.Name, metav1.GetOptions{})
				if err != nil {
					return nil, fmt.Errorf("failed to get v1 volumesnapshot %s", vsObj.Name)
				}
				return vs, nil
			}
			return nil, err
		}
	} else {
		vsObj, ok := vs.(*kSnapshotv1beta1.VolumeSnapshot)
		if !ok {
			return nil, fmt.Errorf("failed to typecast volumesnapshot interface to v1beta1 version structure")
		}
		vsObj.ResourceVersion = ""
		vsObj.Spec.Source.PersistentVolumeClaimName = nil
		vsObj.Spec.Source.VolumeSnapshotContentName = &vsc.(*kSnapshotv1beta1.VolumeSnapshotContent).Name
		vsObj.Namespace = namespace
		vs, err = c.snapshotClient.SnapshotV1beta1().VolumeSnapshots(namespace).Create(context.TODO(), vsObj, metav1.CreateOptions{})
		if err != nil {
			if k8s_errors.IsAlreadyExists(err) {
				vs, err = c.snapshotClient.SnapshotV1beta1().VolumeSnapshots(namespace).Get(context.TODO(), vsObj.Name, metav1.GetOptions{})
				if err != nil {
					return nil, fmt.Errorf("failed to get v1beta1 volumesnapshot %s", vsObj.Name)
				}
				return vs, nil
			}
			return nil, err
		}
	}
	return vs, nil
}

func (c *csi) restoreVolumeSnapshotContent(
	namespace string,
	vs interface{},
	vsc interface{},
) (interface{}, error) {
	var err error
	if c.v1SnapshotRequired {
		vscObj, ok := vsc.(*kSnapshotv1.VolumeSnapshotContent)
		if !ok {
			return nil, fmt.Errorf("failed to typecast volumesnapshotContent interface to v1 version structure")
		}
		snapshotHandle := *vscObj.Status.SnapshotHandle

		vscObj.ResourceVersion = ""
		vscObj.Spec.DeletionPolicy = kSnapshotv1.VolumeSnapshotContentDelete
		vscObj.Spec.Source.VolumeHandle = nil
		vscObj.Spec.Source.SnapshotHandle = &snapshotHandle
		vscObj.Spec.VolumeSnapshotRef.Name = vs.(*kSnapshotv1.VolumeSnapshot).Name
		vscObj.Spec.VolumeSnapshotRef.Namespace = namespace
		vscObj.Spec.VolumeSnapshotRef.UID = vs.(*kSnapshotv1.VolumeSnapshot).UID
		vscObj.Spec.DeletionPolicy = kSnapshotv1.VolumeSnapshotContentRetain
		vsc, err = c.snapshotClient.SnapshotV1().VolumeSnapshotContents().Create(context.TODO(), vscObj, metav1.CreateOptions{})
		if err != nil {
			if k8s_errors.IsAlreadyExists(err) {
				vsc, err = c.snapshotClient.SnapshotV1().VolumeSnapshotContents().Get(context.TODO(), vscObj.Name, metav1.GetOptions{})
				if err != nil {
					return nil, fmt.Errorf("failed to get v1 volumesnapshot content %s", vscObj.Name)
				}
				return vsc, nil
			}
			return nil, err
		}
	} else {
		vscObj, ok := vsc.(*kSnapshotv1beta1.VolumeSnapshotContent)
		if !ok {
			return nil, fmt.Errorf("failed to typecast volumesnapshotContent interface to v1beta1 version structure")
		}
		snapshotHandle := *vscObj.Status.SnapshotHandle
		vscObj.ResourceVersion = ""
		vscObj.Spec.DeletionPolicy = kSnapshotv1beta1.VolumeSnapshotContentDelete
		vscObj.Spec.Source.VolumeHandle = nil
		vscObj.Spec.Source.SnapshotHandle = &snapshotHandle
		vscObj.Spec.VolumeSnapshotRef.Name = vs.(*kSnapshotv1beta1.VolumeSnapshot).Name
		vscObj.Spec.VolumeSnapshotRef.Namespace = namespace
		vscObj.Spec.VolumeSnapshotRef.UID = vs.(*kSnapshotv1beta1.VolumeSnapshot).UID
		vscObj.Spec.DeletionPolicy = kSnapshotv1beta1.VolumeSnapshotContentRetain
		vsc, err = c.snapshotClient.SnapshotV1beta1().VolumeSnapshotContents().Create(context.TODO(), vscObj, metav1.CreateOptions{})
		if err != nil {
			if k8s_errors.IsAlreadyExists(err) {
				vsc, err = c.snapshotClient.SnapshotV1beta1().VolumeSnapshotContents().Get(context.TODO(), vscObj.Name, metav1.GetOptions{})
				if err != nil {
					return nil, fmt.Errorf("failed to get v1beta1 volumesnapshot content %s", vscObj.Name)
				}
				return vsc, nil
			}
			return nil, err
		}
	}
	return vsc, nil
}

func GetUIDLastSection(uid types.UID) string {
	parts := strings.Split(string(uid), "-")
	uidLastSection := parts[len(parts)-1]

	if uidLastSection == "" {
		uidLastSection = string(uid)
	}
	return uidLastSection
}

func (c *csi) getRestoreSnapshotName(existingSnapshotUID types.UID, restoreUID types.UID) string {
	return fmt.Sprintf("%s-vs-%s-%s", snapshotRestorePrefix, utils.GetUIDLastSection(restoreUID), utils.GetUIDLastSection(existingSnapshotUID))
}

func (c *csi) getRestoreSnapshotContentName(existingSnapshotUID types.UID, restoreUID types.UID) string {
	return fmt.Sprintf("%s-vsc-%s-%s", snapshotRestorePrefix, utils.GetUIDLastSection(restoreUID), utils.GetUIDLastSection(existingSnapshotUID))
}

func (c *csi) createRestoreSnapshotsAndPVCs(
	restore *storkapi.ApplicationRestore,
	volumeBackupInfos []*storkapi.ApplicationBackupVolumeInfo,
	csiBackupObject *CsiBackupObject,
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
		switch v := vsc.(type) {
		case *kSnapshotv1beta1.VolumeSnapshotContent:
			vsc.(*kSnapshotv1beta1.VolumeSnapshotContent).Name = c.getRestoreSnapshotContentName(vsc.(*kSnapshotv1beta1.VolumeSnapshotContent).UID, restore.UID)
			vsc.(*kSnapshotv1beta1.VolumeSnapshotContent).ObjectMeta.Labels = c.getRestoreUIDLabels(restore, vsc.(*kSnapshotv1beta1.VolumeSnapshotContent).ObjectMeta.Labels)
		case *kSnapshotv1.VolumeSnapshotContent:
			vsc.(*kSnapshotv1.VolumeSnapshotContent).Name = c.getRestoreSnapshotContentName(vsc.(*kSnapshotv1.VolumeSnapshotContent).UID, restore.UID)
			vsc.(*kSnapshotv1.VolumeSnapshotContent).ObjectMeta.Labels = c.getRestoreUIDLabels(restore, vsc.(*kSnapshotv1.VolumeSnapshotContent).ObjectMeta.Labels)
		default:
			log.ApplicationRestoreLog(restore).Errorf("Unknown Type %T recieved for volumeSnapshotContent %v", v, v)
		}

		vs, err := csiBackupObject.GetVolumeSnapshot(snapshotID)
		if err != nil {
			return nil, fmt.Errorf("failed to retrieve volume snapshot for snapshotID %s", snapshotID)
		}
		var destNamespace string
		switch v := vs.(type) {
		case *kSnapshotv1beta1.VolumeSnapshot:
			vs.(*kSnapshotv1beta1.VolumeSnapshot).Name = c.getRestoreSnapshotName(vs.(*kSnapshotv1beta1.VolumeSnapshot).UID, restore.UID)
			destNamespace = c.getDestinationNamespace(restore, vs.(*kSnapshotv1beta1.VolumeSnapshot).Namespace)
			vs.(*kSnapshotv1beta1.VolumeSnapshot).ObjectMeta.Labels = c.getRestoreUIDLabels(restore, vs.(*kSnapshotv1beta1.VolumeSnapshot).ObjectMeta.Labels)
		case *kSnapshotv1.VolumeSnapshot:
			vs.(*kSnapshotv1.VolumeSnapshot).Name = c.getRestoreSnapshotName(vs.(*kSnapshotv1.VolumeSnapshot).UID, restore.UID)
			destNamespace = c.getDestinationNamespace(restore, vs.(*kSnapshotv1.VolumeSnapshot).Namespace)
			vs.(*kSnapshotv1.VolumeSnapshot).ObjectMeta.Labels = c.getRestoreUIDLabels(restore, vs.(*kSnapshotv1.VolumeSnapshot).ObjectMeta.Labels)
		default:
			log.ApplicationRestoreLog(restore).Errorf("Unknown Type %T recieved for volumeSnapshot %s", v, v)
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
		// Just for a log we are doing below type checking of interface
		// Can we remove it or print it some other way
		switch v := vsClass.(type) {
		case *kSnapshotv1beta1.VolumeSnapshotClass:
			log.ApplicationRestoreLog(restore).Debugf("created vsClass: %s", vsClass.(*kSnapshotv1beta1.VolumeSnapshotClass).Name)
		case *kSnapshotv1.VolumeSnapshotClass:
			log.ApplicationRestoreLog(restore).Debugf("created vsClass: %s", vsClass.(*kSnapshotv1.VolumeSnapshotClass).Name)
		default:
			log.ApplicationRestoreLog(restore).Errorf("Unknown Type %T recieved for volumeSnapshotClass %s", v, v)
		}

		// Create VS, bound to VSC
		// TODO: Check restore happen between two different K8s version(<1.19 & >1.20) based cluster
		vs, err = c.restoreVolumeSnapshot(destNamespace, vs, vsc)
		if err != nil {
			return nil, err
		}
		switch v := vs.(type) {
		case *kSnapshotv1beta1.VolumeSnapshot:
			log.ApplicationRestoreLog(restore).Debugf("created vs: %s", vs.(*kSnapshotv1beta1.VolumeSnapshot).Name)

			// Create VSC, bound to VS
			vsc, err = c.restoreVolumeSnapshotContent(destNamespace, vs, vsc)
			if err != nil {
				return nil, err
			}
			log.ApplicationRestoreLog(restore).Debugf("created vsc: %s", vsc.(*kSnapshotv1beta1.VolumeSnapshotContent).Name)

			pvc, err = c.snapshotter.RestoreVolumeClaim(
				snapshotter.RestoreSnapshotName(vs.(*kSnapshotv1beta1.VolumeSnapshot).Name),
				snapshotter.RestoreNamespace(c.getDestinationNamespace(restore, pvc.Namespace)),
				snapshotter.PVC(*pvc),
			)
		case *kSnapshotv1.VolumeSnapshot:
			log.ApplicationRestoreLog(restore).Debugf("created vs: %s", vs.(*kSnapshotv1.VolumeSnapshot).Name)

			// Create VSC, bound to VS
			vsc, err = c.restoreVolumeSnapshotContent(destNamespace, vs, vsc)
			if err != nil {
				return nil, err
			}
			log.ApplicationRestoreLog(restore).Debugf("created vsc: %s", vsc.(*kSnapshotv1.VolumeSnapshotContent).Name)

			pvc, err = c.snapshotter.RestoreVolumeClaim(
				snapshotter.RestoreSnapshotName(vs.(*kSnapshotv1.VolumeSnapshot).Name),
				snapshotter.RestoreNamespace(c.getDestinationNamespace(restore, pvc.Namespace)),
				snapshotter.PVC(*pvc),
			)
		default:
			log.ApplicationRestoreLog(restore).Errorf("Unknown Type %T recieved for volumeSnapshot %+v", v, v)
		}

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
		if vrInfo.Status == storkapi.ApplicationRestoreStatusSuccessful || vrInfo.Status == storkapi.ApplicationRestoreStatusFailed || vrInfo.Status == storkapi.ApplicationRestoreStatusRetained {
			volumeInfos = append(volumeInfos, vrInfo)
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
			reason := fmt.Sprintf("Volume restore successful: PVC %s is bound", vrInfo.PersistentVolumeClaim)
			vrStatus := storkapi.ApplicationRestoreStatusSuccessful
			if vrInfo.Status == storkapi.ApplicationRestoreStatusRetained {
				vrStatus = storkapi.ApplicationRestoreStatusRetained
				reason = fmt.Sprintf("Skipped from volume restore as policy is set to %s and pvc already exists", storkapi.ApplicationRestoreReplacePolicyRetain)
			}
			vrInfo.Status = vrStatus
			vrInfo.Reason = reason
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

func (c *csi) GetPodVolumes(podSpec *v1.PodSpec, namespace string, includePendingWFFC bool) ([]*storkvolume.Info, []*storkvolume.Info, error) {
	return nil, nil, &errors.ErrNotSupported{}
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
	var vsMap interface{}
	var vsContentMap interface{}

	if c.v1SnapshotRequired {
		vsMap = make(map[string]*kSnapshotv1.VolumeSnapshot)
		vsContentMap = make(map[string]*kSnapshotv1.VolumeSnapshotContent)
	} else {
		vsMap = make(map[string]*kSnapshotv1beta1.VolumeSnapshot)
		vsContentMap = make(map[string]*kSnapshotv1beta1.VolumeSnapshotContent)
	}
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
		if c.v1SnapshotRequired {
			snapshot, ok := snapshotInfo.SnapshotRequest.(*kSnapshotv1.VolumeSnapshot)
			if !ok {
				logrus.Warnf("failed to map volumesnapshot object")
				continue
			}
			vsMap.(map[string]*kSnapshotv1.VolumeSnapshot)[vInfo.BackupID] = snapshot
			if snapshotInfo.Status == snapshotter.StatusReady {
				snapshotContent, ok := snapshotInfo.Content.(*kSnapshotv1.VolumeSnapshotContent)
				if !ok {
					logrus.Warnf("failed to map volumesnapshotcontent object")
					continue
				}
				vsContentMap.(map[string]*kSnapshotv1.VolumeSnapshotContent)[vInfo.BackupID] = snapshotContent
			}
		} else {
			snapshot, ok := snapshotInfo.SnapshotRequest.(*kSnapshotv1beta1.VolumeSnapshot)
			if !ok {
				logrus.Warnf("failed to map volumesnapshot object")
				continue
			}
			vsMap.(map[string]*kSnapshotv1beta1.VolumeSnapshot)[vInfo.BackupID] = snapshot
			if snapshotInfo.Status == snapshotter.StatusReady {
				snapshotContent, ok := snapshotInfo.Content.(*kSnapshotv1beta1.VolumeSnapshotContent)
				if !ok {
					logrus.Warnf("failed to map volumesnapshotcontent object")
					continue
				}
				vsContentMap.(map[string]*kSnapshotv1beta1.VolumeSnapshotContent)[vInfo.BackupID] = snapshotContent
			}
		}
	}
	// cleanup after a successful object upload
	err := c.cleanupSnapshots(vsMap, vsContentMap, true)
	if err != nil {
		logrus.Tracef("failed to cleanup snapshots: %v", err)
	}
	if c.v1SnapshotRequired {
		log.ApplicationBackupLog(backup).Tracef("started clean up of %v snapshots and %v snapshotcontents", len(vsMap.(map[string]*kSnapshotv1.VolumeSnapshot)), len(vsContentMap.(map[string]*kSnapshotv1.VolumeSnapshotContent)))
	} else {
		log.ApplicationBackupLog(backup).Tracef("started clean up of %v snapshots and %v snapshotcontents", len(vsMap.(map[string]*kSnapshotv1beta1.VolumeSnapshot)), len(vsContentMap.(map[string]*kSnapshotv1beta1.VolumeSnapshotContent)))
	}
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

// GetPodPatches returns driver-specific json patches to mutate the pod in a webhook
func (c *csi) GetPodPatches(podNamespace string, pod *v1.Pod) ([]k8sutils.JSONPatchOp, error) {
	return nil, nil
}

// GetCSIPodPrefix returns prefix for the csi pod names in the deployment
func (a *csi) GetCSIPodPrefix() (string, error) {
	return "", &errors.ErrNotSupported{}
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
