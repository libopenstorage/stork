package csi

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"

	kSnapshotv1beta1 "github.com/kubernetes-csi/external-snapshotter/v2/pkg/apis/volumesnapshot/v1beta1"
	kSnapshotClient "github.com/kubernetes-csi/external-snapshotter/v2/pkg/client/clientset/versioned"

	snapv1 "github.com/kubernetes-incubator/external-storage/snapshot/pkg/apis/crd/v1"
	snapshotVolume "github.com/kubernetes-incubator/external-storage/snapshot/pkg/volume"
	storkvolume "github.com/libopenstorage/stork/drivers/volume"
	storkapi "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/libopenstorage/stork/pkg/crypto"
	"github.com/libopenstorage/stork/pkg/errors"
	"github.com/libopenstorage/stork/pkg/log"
	"github.com/libopenstorage/stork/pkg/objectstore"
	"github.com/portworx/sched-ops/k8s/core"
	"github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	k8s_errors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
)

const (
	// driverName is the name of the k8s driver implementation
	driverName = "csi"
	// snapshotPrefix is appended to CSI backup snapshot
	snapshotPrefix = "snapshot-"
	// snapshotClassNamePrefix is the prefix for snapshot classes per CSI driver
	snapshotClassNamePrefix = "stork-csi-snapshot-class-"

	// snapshotObjectName is the object stored for the volumesnapshot
	snapshotObjectName = "volumesnapshot.json"
	// snapshotContentObjectName is the object stored for the volumesnapshotcontent
	snapshotContentObjectName = "volumesnapshotcontent.json"
)

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
	return driverName
}

func (c *csi) Stop() error {
	return nil
}

func (c *csi) OwnsPVC(pvc *v1.PersistentVolumeClaim) bool {
	// Try to get info from the PV since storage class could be deleted
	pv, err := core.Instance().GetPersistentVolume(pvc.Spec.VolumeName)
	if err != nil {
		logrus.Warnf("error getting pv %v for pvc %v: %v", pvc.Spec.VolumeName, pvc.Name, err)
		return false
	}
	return c.OwnsPV(pv)
}

func (c *csi) OwnsPV(pv *v1.PersistentVolume) bool {
	// check if CSI volume
	if pv.Spec.CSI != nil {
		// Portworx driver handles this case
		if pv.Spec.CSI.Driver == snapv1.PortworxCsiProvisionerName || pv.Spec.CSI.Driver == snapv1.PortworxCsiDeprecatedProvisionerName {
			return false
		}

		return true
	}

	return true
}

func (c *csi) getSnapshotClassName(driverName string) string {
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

// Construct the full base path for a given backup
// The format is "namespace/backupName/backupUID" which will be unique for each backup
func (c *csi) getObjectPath(
	backup *storkapi.ApplicationBackup,
) string {
	return filepath.Join(backup.Namespace, backup.Name, string(backup.UID))
}

func (c *csi) StartBackup(backup *storkapi.ApplicationBackup,
	pvcs []v1.PersistentVolumeClaim,
) ([]*storkapi.ApplicationBackupVolumeInfo, error) {
	volumeInfos := make([]*storkapi.ApplicationBackupVolumeInfo, 0)
	snapshotClassCreatedForDriver := make(map[string]bool)
	for _, pvc := range pvcs {
		if pvc.DeletionTimestamp != nil {
			log.ApplicationBackupLog(backup).Warnf("Ignoring PVC %v which is being deleted", pvc.Name)
			continue
		}
		volumeInfo := &storkapi.ApplicationBackupVolumeInfo{}
		volumeInfo.PersistentVolumeClaim = pvc.Name
		volumeInfo.Namespace = pvc.Namespace
		volumeInfo.DriverName = driverName
		volumeInfo.Size = uint64(pvc.Size())
		volumeInfos = append(volumeInfos, volumeInfo)

		// get snapshotclass name based on pv provisioner
		pvName, err := core.Instance().GetVolumeForPersistentVolumeClaim(&pvc)
		if err != nil {
			return nil, fmt.Errorf("error getting PV name for PVC (%v/%v): %v", pvc.Namespace, pvc.Name, err)
		}
		pv, err := core.Instance().GetPersistentVolume(pvName)
		if err != nil {
			return nil, fmt.Errorf("error getting pv %v: %v", pvName, err)
		}
		driverName := pv.Spec.CSI.Driver
		snapshotClassName := c.getSnapshotClassName(driverName)

		// ensure volumesnapshotclass is created
		if !snapshotClassCreatedForDriver[driverName] {
			_, err = c.getVolumeSnapshotClass(snapshotClassName)
			if k8s_errors.IsNotFound(err) {
				_, err = c.createVolumeSnapshotClass(snapshotClassName, pv.Spec.CSI.Driver)
				if err != nil {
					return nil, err
				}
			} else {
				return nil, err
			}
			snapshotClassCreatedForDriver[snapshotClassName] = true
		}

		vs := &kSnapshotv1beta1.VolumeSnapshot{
			ObjectMeta: metav1.ObjectMeta{
				Name:      c.getSnapshotName(string(pvc.GetUID())),
				Namespace: pvc.Namespace,
			},
			Spec: kSnapshotv1beta1.VolumeSnapshotSpec{
				VolumeSnapshotClassName: stringPtr(snapshotClassName),
				Source: kSnapshotv1beta1.VolumeSnapshotSource{
					PersistentVolumeClaimName: stringPtr(pvc.Name),
				},
			},
		}
		_, err = c.snapshotClient.SnapshotV1beta1().VolumeSnapshots(pvc.Namespace).Create(vs)
		if err != nil {
			return nil, err
		}

	}
	return volumeInfos, nil
}

func (c *csi) getSnapshotName(pvcUUID string) string {
	return fmt.Sprintf(snapshotPrefix + pvcUUID)
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

	objectPath := c.getObjectPath(backup)
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

// uploadSnapshotAndContent issues an object upload for all VolumeSnapshots and VolumeSnapshotContents provided
func (c *csi) uploadSnapshotAndContent(backup *storkapi.ApplicationBackup, vsList []*kSnapshotv1beta1.VolumeSnapshot, vscontentList []*kSnapshotv1beta1.VolumeSnapshotContent) error {
	var vsBytes []byte
	var vscBytes []byte

	vsBytes, err := json.Marshal(vsList)
	if err != nil {
		return err
	}
	vscBytes, err = json.Marshal(vscontentList)
	if err != nil {
		return err
	}

	err = c.uploadObject(backup, snapshotObjectName, vsBytes)
	if err != nil {
		return err
	}

	err = c.uploadObject(backup, snapshotContentObjectName, vscBytes)
	if err != nil {
		return err
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
	var inProgress bool
	var vsList []*kSnapshotv1beta1.VolumeSnapshot
	var vsContentList []*kSnapshotv1beta1.VolumeSnapshotContent
	for _, vInfo := range backup.Status.Volumes {
		if vInfo.DriverName != driverName {
			continue
		}
		pvc, err := core.Instance().GetPersistentVolumeClaim(vInfo.PersistentVolumeClaim, vInfo.Namespace)
		if err != nil {
			return nil, err
		}
		snapshot, err := c.snapshotClient.SnapshotV1beta1().VolumeSnapshots(vInfo.Namespace).Get(c.getSnapshotName(string(pvc.GetUID())), metav1.GetOptions{})
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

			volumeSnapshotContentReady = c.snapshotContentReady(snapshotContent)
		}

		switch {
		case volumeSnapshotReady && volumeSnapshotContentReady:
			vInfo.Status = storkapi.ApplicationBackupStatusSuccessful
			vInfo.Reason = "Backup successful for volume"
			size, _ := snapshot.Status.RestoreSize.AsInt64()
			vInfo.Size = uint64(size)

		case snapshot.DeletionTimestamp != nil:
			vInfo.Status = storkapi.ApplicationBackupStatusFailed
			vInfo.Reason = "Backup failed for volume"
			inProgress = true
		default:
			vInfo.Status = storkapi.ApplicationBackupStatusInProgress
			vInfo.Reason = "Volume backup in progress"
			inProgress = true
		}

		volumeInfos = append(volumeInfos, vInfo)
	}

	// if all have finished, add all VolumeSnapshot and VolumeSnapshotContent to objectstore
	// in the case where no volumes are being backed up, skip uploading empty lists
	if !inProgress && len(vsContentList) > 0 && len(vsList) > 0 {
		err := c.uploadSnapshotAndContent(backup, vsList, vsContentList)
		if err != nil {
			return nil, err
		}
	}

	return volumeInfos, nil
}

func (c *csi) CancelBackup(backup *storkapi.ApplicationBackup) error {
	return &errors.ErrNotSupported{}

}

func (c *csi) DeleteBackup(backup *storkapi.ApplicationBackup) error {
	return &errors.ErrNotSupported{}

}

func (c *csi) UpdateMigratedPersistentVolumeSpec(
	pv *v1.PersistentVolume,
) (*v1.PersistentVolume, error) {
	return nil, &errors.ErrNotSupported{}
}

func (c *csi) GetPreRestoreResources(
	*storkapi.ApplicationBackup,
	[]runtime.Unstructured,
) ([]runtime.Unstructured, error) {
	return nil, nil
}

func (c *csi) StartRestore(
	restore *storkapi.ApplicationRestore,
	volumeBackupInfos []*storkapi.ApplicationBackupVolumeInfo,
) ([]*storkapi.ApplicationRestoreVolumeInfo, error) {
	return nil, &errors.ErrNotSupported{}
}

func (c *csi) CancelRestore(restore *storkapi.ApplicationRestore) error {
	return &errors.ErrNotSupported{}
}

func (c *csi) GetRestoreStatus(restore *storkapi.ApplicationRestore) ([]*storkapi.ApplicationRestoreVolumeInfo, error) {
	return nil, &errors.ErrNotSupported{}
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
	if err := storkvolume.Register(driverName, c); err != nil {
		logrus.Panicf("Error registering csi volume driver: %v", err)
	}
}
