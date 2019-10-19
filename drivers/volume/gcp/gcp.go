package gcp

import (
	"context"
	"fmt"

	"cloud.google.com/go/compute/metadata"
	snapv1 "github.com/kubernetes-incubator/external-storage/snapshot/pkg/apis/crd/v1"
	snapshotVolume "github.com/kubernetes-incubator/external-storage/snapshot/pkg/volume"
	storkvolume "github.com/libopenstorage/stork/drivers/volume"
	storkapi "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/libopenstorage/stork/pkg/errors"
	"github.com/libopenstorage/stork/pkg/log"
	"github.com/portworx/sched-ops/k8s"
	"github.com/sirupsen/logrus"
	"golang.org/x/oauth2/google"
	compute "google.golang.org/api/compute/v1"
	"google.golang.org/api/option"
	"k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/uuid"
	k8shelper "k8s.io/kubernetes/pkg/apis/core/v1/helper"
)

const (
	// driverName is the name of the gcp driver implementation
	driverName = "gce"
	// provisioner names for gce volumes
	provisionerName = "kubernetes.io/gce-pd"
	// pvcProvisionerAnnotation is the annotation on PVC which has the
	// provisioner name
	pvcProvisionerAnnotation = "volume.beta.kubernetes.io/storage-provisioner"
	// pvProvisionedByAnnotation is the annotation on PV which has the
	// provisioner name
	pvProvisionedByAnnotation = "pv.kubernetes.io/provisioned-by"
	pvNamePrefix              = "pvc-"
)

type gcp struct {
	projectID string
	zone      string
	service   *compute.Service
	storkvolume.ClusterPairNotSupported
	storkvolume.MigrationNotSupported
	storkvolume.GroupSnapshotNotSupported
	storkvolume.ClusterDomainsNotSupported
	storkvolume.CloneNotSupported
	storkvolume.SnapshotRestoreNotSupported
}

func (g *gcp) Init(_ interface{}) error {
	var err error
	g.zone, err = metadata.Zone()
	if err != nil {
		return fmt.Errorf("error getting zone for gce: %v", err)
	}

	g.projectID, err = metadata.ProjectID()
	if err != nil {
		return fmt.Errorf("error getting projectID for gce: %v", err)
	}

	creds, err := google.FindDefaultCredentials(context.Background(), compute.ComputeScope)
	if err != nil {
		return err
	}

	g.service, err = compute.NewService(context.TODO(), option.WithTokenSource(creds.TokenSource))
	if err != nil {
		return err
	}

	return nil
}

func (g *gcp) String() string {
	return driverName
}

func (g *gcp) Stop() error {
	return nil
}

func (g *gcp) OwnsPVC(pvc *v1.PersistentVolumeClaim) bool {

	provisioner := ""
	// Check for the provisioner in the PVC annotation. If not populated
	// try getting the provisioner from the Storage class.
	if val, ok := pvc.Annotations[pvcProvisionerAnnotation]; ok {
		provisioner = val
	} else {
		storageClassName := k8shelper.GetPersistentVolumeClaimClass(pvc)
		if storageClassName != "" {
			storageClass, err := k8s.Instance().GetStorageClass(storageClassName)
			if err == nil {
				provisioner = storageClass.Provisioner
			} else {
				logrus.Warnf("Error getting storageclass %v for pvc %v: %v", storageClassName, pvc.Name, err)
			}
		}
	}

	if provisioner == "" {
		// Try to get info from the PV since storage class could be deleted
		pv, err := k8s.Instance().GetPersistentVolume(pvc.Spec.VolumeName)
		if err != nil {
			logrus.Warnf("Error getting pv %v for pvc %v: %v", pvc.Spec.VolumeName, pvc.Name, err)
			return false
		}
		return g.OwnsPV(pv)
	}

	if provisioner != provisionerName &&
		!isCsiProvisioner(provisioner) {
		logrus.Debugf("Provisioner in Storageclass not GCE: %v", provisioner)
		return false
	}
	return true
}

func (g *gcp) OwnsPV(pv *v1.PersistentVolume) bool {
	var provisioner string
	// Check the annotation in the PV for the provisioner
	if val, ok := pv.Annotations[pvProvisionedByAnnotation]; ok {
		provisioner = val
	} else {
		// Finally check the volume reference in the spec
		if pv.Spec.GCEPersistentDisk != nil {
			return true
		}
	}
	if provisioner != provisionerName &&
		!isCsiProvisioner(provisioner) {
		logrus.Debugf("Provisioner in Storageclass not GCE: %v", provisioner)
		return false
	}
	return true
}

func isCsiProvisioner(provisioner string) bool {
	return false
}

func (g *gcp) StartBackup(backup *storkapi.ApplicationBackup,
	pvcs []v1.PersistentVolumeClaim,
) ([]*storkapi.ApplicationBackupVolumeInfo, error) {
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
		volumeInfos = append(volumeInfos, volumeInfo)

		pvName, err := k8s.Instance().GetVolumeForPersistentVolumeClaim(&pvc)
		if err != nil {
			return nil, fmt.Errorf("error getting PV name for PVC (%v/%v): %v", pvc.Namespace, pvc.Name, err)
		}
		pv, err := k8s.Instance().GetPersistentVolume(pvName)
		if err != nil {
			return nil, fmt.Errorf("error getting pv %v: %v", pvName, err)
		}
		volume := pvc.Spec.VolumeName
		pdName := pv.Spec.GCEPersistentDisk.PDName
		volumeInfo.Volume = volume
		snapshot := &compute.Snapshot{
			Name:   "stork-snapshot-" + string(uuid.NewUUID()),
			Labels: storkvolume.GetApplicationBackupLabels(backup, &pvc),
		}
		snapshotCall := g.service.Disks.CreateSnapshot(g.projectID, g.zone, pdName, snapshot)

		_, err = snapshotCall.Do()
		if err != nil {
			return nil, fmt.Errorf("error triggering backup for volume: %v (PVC: %v, Namespace: %v): %v", volume, pvc.Name, pvc.Namespace, err)
		}
		volumeInfo.BackupID = snapshot.Name
		volumeInfo.Options = map[string]string{
			"projectID": g.projectID,
		}
	}
	return volumeInfos, nil
}

func (g *gcp) GetBackupStatus(backup *storkapi.ApplicationBackup) ([]*storkapi.ApplicationBackupVolumeInfo, error) {
	volumeInfos := make([]*storkapi.ApplicationBackupVolumeInfo, 0)

	for _, vInfo := range backup.Status.Volumes {
		if vInfo.DriverName != driverName {
			continue
		}
		snapshot, err := g.service.Snapshots.Get(g.projectID, vInfo.BackupID).Do()
		if err != nil {
			return nil, err
		}
		switch snapshot.Status {
		case "CREATING", "UPLOADING":
			vInfo.Status = storkapi.ApplicationBackupStatusInProgress
			vInfo.Reason = fmt.Sprintf("Volume backup in progress: %v", snapshot.Status)
		case "DELETING", "FAILED":
			vInfo.Status = storkapi.ApplicationBackupStatusFailed
			vInfo.Reason = fmt.Sprintf("Backup failed for volume: %v", snapshot.Status)
		case "READY":
			vInfo.Status = storkapi.ApplicationBackupStatusSuccessful
			vInfo.Reason = "Backup successful for volume"
		}
		volumeInfos = append(volumeInfos, vInfo)
	}

	return volumeInfos, nil

}

func (g *gcp) CancelBackup(backup *storkapi.ApplicationBackup) error {
	return g.DeleteBackup(backup)
}

func (g *gcp) DeleteBackup(backup *storkapi.ApplicationBackup) error {
	for _, vInfo := range backup.Status.Volumes {
		if vInfo.DriverName != driverName {
			continue
		}
		_, err := g.service.Snapshots.Delete(vInfo.Options["projectID"], vInfo.BackupID).Do()
		if err != nil {
			return err
		}
	}
	return nil
}

func (g *gcp) UpdateMigratedPersistentVolumeSpec(
	pv *v1.PersistentVolume,
) (*v1.PersistentVolume, error) {
	if pv.Spec.CSI != nil {
		pv.Spec.CSI.VolumeHandle = pv.Name
		return pv, nil
	}

	pv.Spec.GCEPersistentDisk.PDName = pv.Name
	return pv, nil
}

func (g *gcp) generatePVName() string {
	return pvNamePrefix + string(uuid.NewUUID())
}

func (g *gcp) getSnapshotResourceName(
	backupVolumeInfo *storkapi.ApplicationBackupVolumeInfo,
) string {
	return fmt.Sprintf("https://www.googleapis.com/compute/v1/projects/%v/global/snapshots/%v",
		backupVolumeInfo.Options["projectID"], backupVolumeInfo.BackupID)
}

func (g *gcp) StartRestore(
	restore *storkapi.ApplicationRestore,
	volumeBackupInfos []*storkapi.ApplicationBackupVolumeInfo,
) ([]*storkapi.ApplicationRestoreVolumeInfo, error) {

	volumeInfos := make([]*storkapi.ApplicationRestoreVolumeInfo, 0)
	for _, backupVolumeInfo := range volumeBackupInfos {
		volumeInfo := &storkapi.ApplicationRestoreVolumeInfo{
			PersistentVolumeClaim: backupVolumeInfo.PersistentVolumeClaim,
			SourceNamespace:       backupVolumeInfo.Namespace,
			SourceVolume:          backupVolumeInfo.Volume,
			RestoreVolume:         g.generatePVName(),
			DriverName:            driverName,
		}
		volumeInfos = append(volumeInfos, volumeInfo)
		disk := &compute.Disk{
			Name:           volumeInfo.RestoreVolume,
			SourceSnapshot: g.getSnapshotResourceName(backupVolumeInfo),
			Labels:         storkvolume.GetApplicationRestoreLabels(restore, volumeInfo),
		}
		_, err := g.service.Disks.Insert(g.projectID, g.zone, disk).Do()
		if err != nil {
			return nil, err
		}
	}
	return volumeInfos, nil
}

func (g *gcp) CancelRestore(restore *storkapi.ApplicationRestore) error {
	// Do nothing to cancel restores for now
	return nil
}

func (g *gcp) GetRestoreStatus(restore *storkapi.ApplicationRestore) ([]*storkapi.ApplicationRestoreVolumeInfo, error) {
	volumeInfos := make([]*storkapi.ApplicationRestoreVolumeInfo, 0)
	for _, vInfo := range restore.Status.Volumes {
		if vInfo.DriverName != driverName {
			continue
		}
		disk, err := g.service.Disks.Get(g.projectID, g.zone, vInfo.RestoreVolume).Do()
		if err != nil {
			return nil, err
		}
		switch disk.Status {
		case "CREATING", "RESTORING":
			vInfo.Status = storkapi.ApplicationRestoreStatusInProgress
			vInfo.Reason = fmt.Sprintf("Volume restore in progress: %v", disk.Status)
		case "DELETING", "FAILED":
			vInfo.Status = storkapi.ApplicationRestoreStatusFailed
			vInfo.Reason = fmt.Sprintf("Restore failed for volume: %v", disk.Status)
		case "READY":
			vInfo.Status = storkapi.ApplicationRestoreStatusSuccessful
			vInfo.Reason = "Restore successful for volume"
		}
		volumeInfos = append(volumeInfos, vInfo)
	}

	return volumeInfos, nil
}

func (g *gcp) InspectVolume(volumeID string) (*storkvolume.Info, error) {
	return nil, &errors.ErrNotSupported{}
}

func (g *gcp) GetClusterID() (string, error) {
	return "", &errors.ErrNotSupported{}
}

func (g *gcp) GetNodes() ([]*storkvolume.NodeInfo, error) {
	return nil, &errors.ErrNotSupported{}
}

func (g *gcp) GetPodVolumes(podSpec *v1.PodSpec, namespace string) ([]*storkvolume.Info, error) {
	return nil, &errors.ErrNotSupported{}
}

func (g *gcp) GetSnapshotPlugin() snapshotVolume.Plugin {
	return nil
}

func (g *gcp) GetSnapshotType(snap *snapv1.VolumeSnapshot) (string, error) {
	return "", &errors.ErrNotSupported{}
}

func (g *gcp) GetVolumeClaimTemplates([]v1.PersistentVolumeClaim) (
	[]v1.PersistentVolumeClaim, error) {
	return nil, &errors.ErrNotSupported{}
}

func init() {
	g := &gcp{}
	err := g.Init(nil)
	if err != nil {
		logrus.Errorf("Error init'ing gcp driver: %v", err)
		return
	}
	if err := storkvolume.Register(driverName, g); err != nil {
		logrus.Panicf("Error registering gcp volume driver: %v", err)
	}
}
