package gcp

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"cloud.google.com/go/compute/metadata"
	snapv1 "github.com/kubernetes-incubator/external-storage/snapshot/pkg/apis/crd/v1"
	snapshotVolume "github.com/kubernetes-incubator/external-storage/snapshot/pkg/volume"
	storkvolume "github.com/libopenstorage/stork/drivers/volume"
	storkapi "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/libopenstorage/stork/pkg/errors"
	"github.com/libopenstorage/stork/pkg/log"
	"github.com/portworx/sched-ops/k8s/core"
	"github.com/portworx/sched-ops/k8s/storage"
	"github.com/sirupsen/logrus"
	"golang.org/x/oauth2/google"
	compute "google.golang.org/api/compute/v1"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/uuid"
	k8shelper "k8s.io/kubernetes/pkg/apis/core/v1/helper"
	"sigs.k8s.io/gcp-compute-persistent-disk-csi-driver/pkg/common"
)

const (
	// driverName is the name of the gcp driver implementation
	driverName = "gce"
	// provisioner names for gce volumes
	provisionerName = "kubernetes.io/gce-pd"
	// CSI provisioner name for for gce volumes
	csiProvisionerName = "pd.csi.storage.gke.io"
	// pvcProvisionerAnnotation is the annotation on PVC which has the
	// provisioner name
	pvcProvisionerAnnotation = "volume.beta.kubernetes.io/storage-provisioner"
	// pvProvisionedByAnnotation is the annotation on PV which has the
	// provisioner name
	pvProvisionedByAnnotation = "pv.kubernetes.io/provisioned-by"
	pvNamePrefix              = "pvc-"
	zoneSeperator             = "__"
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

func (g *gcp) OwnsPVC(coreOps core.Ops, pvc *v1.PersistentVolumeClaim) bool {

	provisioner := ""
	// Check for the provisioner in the PVC annotation. If not populated
	// try getting the provisioner from the Storage class.
	if val, ok := pvc.Annotations[pvcProvisionerAnnotation]; ok {
		provisioner = val
	} else {
		storageClassName := k8shelper.GetPersistentVolumeClaimClass(pvc)
		if storageClassName != "" {
			storageClass, err := storage.Instance().GetStorageClass(storageClassName)
			if err == nil {
				provisioner = storageClass.Provisioner
			} else {
				logrus.Warnf("Error getting storageclass %v for pvc %v: %v", storageClassName, pvc.Name, err)
			}
		}
	}

	if provisioner == "" {
		// Try to get info from the PV since storage class could be deleted
		pv, err := coreOps.GetPersistentVolume(pvc.Spec.VolumeName)
		if err != nil {
			logrus.Warnf("Error getting pv %v for pvc %v: %v", pvc.Spec.VolumeName, pvc.Name, err)
			return false
		}
		return g.OwnsPV(pv)
	}

	if provisioner != provisionerName &&
		!isCsiProvisioner(provisioner) {
		logrus.Tracef("Provisioner in Storageclass not GCE: %v", provisioner)
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
		logrus.Tracef("Provisioner in Storageclass not GCE: %v", provisioner)
		return false
	}
	return true
}

func isCsiProvisioner(provisioner string) bool {
	return csiProvisionerName == provisioner
}

func (g *gcp) StartBackup(backup *storkapi.ApplicationBackup,
	pvcs []v1.PersistentVolumeClaim,
) ([]*storkapi.ApplicationBackupVolumeInfo, error) {
	if g.service == nil {
		if err := g.Init(nil); err != nil {
			return nil, err
		}
	}

	volumeInfos := make([]*storkapi.ApplicationBackupVolumeInfo, 0)

	for _, pvc := range pvcs {
		if pvc.DeletionTimestamp != nil {
			log.ApplicationBackupLog(backup).Warnf("Ignoring PVC %v which is being deleted", pvc.Name)
			continue
		}
		volumeInfo := &storkapi.ApplicationBackupVolumeInfo{}
		volumeInfo.PersistentVolumeClaim = pvc.Name
		volumeInfo.PersistentVolumeClaimUID = string(pvc.UID)
		volumeInfo.Namespace = pvc.Namespace
		volumeInfo.DriverName = driverName
		volumeInfo.Options = map[string]string{
			"projectID": g.projectID,
		}
		volumeInfos = append(volumeInfos, volumeInfo)

		pvName, err := core.Instance().GetVolumeForPersistentVolumeClaim(&pvc)
		if err != nil {
			return nil, fmt.Errorf("error getting PV name for PVC (%v/%v): %v", pvc.Namespace, pvc.Name, err)
		}
		pv, err := core.Instance().GetPersistentVolume(pvName)
		if err != nil {
			return nil, fmt.Errorf("error getting pv %v: %v", pvName, err)
		}
		volume := pvc.Spec.VolumeName
		var pdName string
		if pv.Spec.GCEPersistentDisk != nil {
			pdName = pv.Spec.GCEPersistentDisk.PDName
		} else if pv.Spec.CSI != nil {
			key, err := common.VolumeIDToKey(pv.Spec.CSI.VolumeHandle)
			if err != nil {
				return nil, err
			}
			pdName = key.Name
		} else {
			return nil, fmt.Errorf("GCE PD info not found in PV %v", pvName)
		}
		// Get the zone from the PV, fallback to the zone where stork is running
		// if the label is empty
		volumeInfo.Zones = g.getZones(pv)
		if len(volumeInfo.Zones) == 0 {
			volumeInfo.Zones = []string{g.zone}
		}
		volumeInfo.Volume = volume
		labels := storkvolume.GetApplicationBackupLabels(backup, &pvc)
		filter := g.getFilterFromMap(labels)
		// First check if the snapshot has already been created with the same labels
		if snapshots, err := g.service.Snapshots.List(g.projectID).Filter(filter).Do(); err == nil && len(snapshots.Items) == 1 {
			volumeInfo.BackupID = snapshots.Items[0].Name
		} else {
			if len(volumeInfo.Zones) > 1 {
				snapshot := &compute.Snapshot{
					Name:   "stork-snapshot-" + string(uuid.NewUUID()),
					Labels: labels,
				}
				region, err := g.getRegion(volumeInfo.Zones[0])
				if err != nil {
					return nil, err
				}
				snapshotCall := g.service.RegionDisks.CreateSnapshot(g.projectID, region, pdName, snapshot)

				_, err = snapshotCall.Do()
				if err != nil {
					return nil, fmt.Errorf("error triggering backup for volume: %v (PVC: %v, Namespace: %v): %v", volume, pvc.Name, pvc.Namespace, err)
				}
				volumeInfo.BackupID = snapshot.Name
			} else {
				snapshot := &compute.Snapshot{
					Name:   "stork-snapshot-" + string(uuid.NewUUID()),
					Labels: labels,
				}
				snapshotCall := g.service.Disks.CreateSnapshot(g.projectID, volumeInfo.Zones[0], pdName, snapshot)

				_, err = snapshotCall.Do()
				if err != nil {
					return nil, fmt.Errorf("error triggering backup for volume: %v (PVC: %v, Namespace: %v): %v", volume, pvc.Name, pvc.Namespace, err)
				}
				volumeInfo.BackupID = snapshot.Name
			}
		}
	}
	return volumeInfos, nil
}

func (g *gcp) getFilterFromMap(labels map[string]string) string {
	filters := make([]string, 0)
	// Construct an array of filters in the format "labels.key=value"
	for k, v := range labels {

		filters = append(filters, fmt.Sprintf("labels.%v=%v", k, v))
	}
	// Add all the filters to one string seperated by AND
	return strings.Join(filters, " AND ")
}

func (g *gcp) getZones(pv *v1.PersistentVolume) []string {
	if pv.Spec.GCEPersistentDisk != nil {
		zone := pv.Labels[v1.LabelZoneFailureDomain]
		if g.isRegional(zone) {
			return g.getRegionalZones(zone)
		}
		return []string{zone}
	}
	if pv.Spec.NodeAffinity != nil &&
		pv.Spec.NodeAffinity.Required != nil &&
		len(pv.Spec.NodeAffinity.Required.NodeSelectorTerms) != 0 {
		for _, selector := range pv.Spec.NodeAffinity.Required.NodeSelectorTerms[0].MatchExpressions {
			if selector.Key == common.TopologyKeyZone {
				return selector.Values
			}
		}
	}
	return nil

}

func (g *gcp) getRegionalZones(zones string) []string {
	return strings.Split(zones, zoneSeperator)
}

func (g *gcp) isRegional(zone string) bool {
	return strings.Contains(zone, zoneSeperator)
}

func (g *gcp) getRegion(zone string) (string, error) {
	s := strings.Split(zone, "-")
	if len(s) < 3 {
		return "", fmt.Errorf("invalid zone: %v", zone)
	}
	return strings.Join(s[0:2], "-"), nil
}

func (g *gcp) getZoneURLs(zones []string) ([]string, error) {
	zoneURLs := make([]string, 0)
	for _, zone := range zones {
		zoneInfo, err := g.service.Zones.Get(g.projectID, zone).Do()
		if err != nil {
			return nil, err
		}
		zoneURLs = append(zoneURLs, zoneInfo.SelfLink)
	}
	return zoneURLs, nil
}

func (g *gcp) GetBackupStatus(backup *storkapi.ApplicationBackup) ([]*storkapi.ApplicationBackupVolumeInfo, error) {
	if g.service == nil {
		if err := g.Init(nil); err != nil {
			return nil, err
		}
	}

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
			vInfo.TotalSize = uint64(snapshot.StorageBytes)
			vInfo.ActualSize = uint64(snapshot.StorageBytes)
		}
		volumeInfos = append(volumeInfos, vInfo)
	}

	return volumeInfos, nil

}

func (g *gcp) CancelBackup(backup *storkapi.ApplicationBackup) error {
	_, err := g.DeleteBackup(backup)
	return err
}

func (g *gcp) DeleteBackup(backup *storkapi.ApplicationBackup) (bool, error) {
	if g.service == nil {
		if err := g.Init(nil); err != nil {
			return true, err
		}
	}

	for _, vInfo := range backup.Status.Volumes {
		if vInfo.DriverName != driverName {
			continue
		}
		_, err := g.service.Snapshots.Delete(vInfo.Options["projectID"], vInfo.BackupID).Do()
		if err != nil {
			return true, err
		}
	}
	return true, nil
}

func (g *gcp) UpdateMigratedPersistentVolumeSpec(
	pv *v1.PersistentVolume,
) (*v1.PersistentVolume, error) {
	if pv.Spec.CSI != nil {
		key, err := common.VolumeIDToKey(pv.Spec.CSI.VolumeHandle)
		if err != nil {
			return nil, err
		}
		key.Name = pv.Name
		pv.Spec.CSI.VolumeHandle, err = common.KeyToVolumeID(key, g.projectID)
		if err != nil {
			return nil, err
		}
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

func (g *gcp) GetPreRestoreResources(
	*storkapi.ApplicationBackup,
	*storkapi.ApplicationRestore,
	[]runtime.Unstructured,
) ([]runtime.Unstructured, error) {
	return nil, nil
}

func (g *gcp) StartRestore(
	restore *storkapi.ApplicationRestore,
	volumeBackupInfos []*storkapi.ApplicationBackupVolumeInfo,
) ([]*storkapi.ApplicationRestoreVolumeInfo, error) {
	if g.service == nil {
		if err := g.Init(nil); err != nil {
			return nil, err
		}
	}

	var err error

	volumeInfos := make([]*storkapi.ApplicationRestoreVolumeInfo, 0)
	for _, backupVolumeInfo := range volumeBackupInfos {
		volumeInfo := &storkapi.ApplicationRestoreVolumeInfo{
			PersistentVolumeClaim:    backupVolumeInfo.PersistentVolumeClaim,
			PersistentVolumeClaimUID: backupVolumeInfo.PersistentVolumeClaimUID,
			SourceNamespace:          backupVolumeInfo.Namespace,
			SourceVolume:             backupVolumeInfo.Volume,
			DriverName:               driverName,
			Zones:                    backupVolumeInfo.Zones,
		}
		volumeInfos = append(volumeInfos, volumeInfo)
		labels := storkvolume.GetApplicationRestoreLabels(restore, volumeInfo)
		filter := g.getFilterFromMap(labels)
		disk := &compute.Disk{
			Name:           g.generatePVName(),
			SourceSnapshot: g.getSnapshotResourceName(backupVolumeInfo),
			Labels:         labels,
		}
		if len(backupVolumeInfo.Zones) == 0 {
			return nil, fmt.Errorf("zones missing for backup volume %v/%v",
				backupVolumeInfo.Namespace,
				backupVolumeInfo.PersistentVolumeClaim,
			)
		} else if len(backupVolumeInfo.Zones) > 1 {
			disk.ReplicaZones, err = g.getZoneURLs(backupVolumeInfo.Zones)
			if err != nil {
				return nil, err
			}
			region, err := g.getRegion(backupVolumeInfo.Zones[0])
			if err != nil {
				return nil, err
			}

			// First check if the disk has already been created with the same labels
			if disks, err := g.service.RegionDisks.List(g.projectID, region).Filter(filter).Do(); err == nil && len(disks.Items) == 1 {
				volumeInfo.RestoreVolume = disks.Items[0].Name
			} else {
				_, err = g.service.RegionDisks.Insert(g.projectID, region, disk).Do()
				if err != nil {
					return nil, err
				}
				volumeInfo.RestoreVolume = disk.Name
			}
		} else {
			// First check if the disk has already been created with the same labels
			if disks, err := g.service.Disks.List(g.projectID, backupVolumeInfo.Zones[0]).Filter(filter).Do(); err == nil && len(disks.Items) == 1 {
				volumeInfo.RestoreVolume = disks.Items[0].Name
			} else {
				_, err := g.service.Disks.Insert(g.projectID, backupVolumeInfo.Zones[0], disk).Do()
				if err != nil {
					return nil, err
				}
				volumeInfo.RestoreVolume = disk.Name
			}
		}
	}
	return volumeInfos, nil
}

func (g *gcp) CancelRestore(restore *storkapi.ApplicationRestore) error {
	// Do nothing to cancel restores for now
	return nil
}

func (g *gcp) GetRestoreStatus(restore *storkapi.ApplicationRestore) ([]*storkapi.ApplicationRestoreVolumeInfo, error) {
	if g.service == nil {
		if err := g.Init(nil); err != nil {
			return nil, err
		}
	}

	volumeInfos := make([]*storkapi.ApplicationRestoreVolumeInfo, 0)
	for _, vInfo := range restore.Status.Volumes {
		if vInfo.DriverName != driverName {
			continue
		}
		var status string
		if len(vInfo.Zones) == 0 {
			return nil, fmt.Errorf("zones missing for restore volume %v",
				vInfo.PersistentVolumeClaim,
			)
		} else if len(vInfo.Zones) > 1 {
			region, err := g.getRegion(vInfo.Zones[0])
			if err != nil {
				return nil, err
			}
			disk, err := g.service.RegionDisks.Get(g.projectID, region, vInfo.RestoreVolume).Do()
			if err != nil {
				if googleErr, ok := err.(*googleapi.Error); ok {
					if googleErr.Code == http.StatusNotFound {
						vInfo.Status = storkapi.ApplicationRestoreStatusFailed
						vInfo.Reason = "Restore failed for volume: NotFound"
						volumeInfos = append(volumeInfos, vInfo)
						continue
					}
				}
				return nil, err
			}
			status = disk.Status
			// Returns size in GB to the nearest decimal, converting it into bytes
			// to be consistent with other cloud providers
			size := disk.SizeGb * 1024 * 1024
			vInfo.TotalSize = uint64(size)
		} else {
			disk, err := g.service.Disks.Get(g.projectID, vInfo.Zones[0], vInfo.RestoreVolume).Do()
			if err != nil {
				if googleErr, ok := err.(*googleapi.Error); ok {
					if googleErr.Code == http.StatusNotFound {
						vInfo.Status = storkapi.ApplicationRestoreStatusFailed
						vInfo.Reason = "Restore failed for volume: NotFound"
						volumeInfos = append(volumeInfos, vInfo)
						continue
					}
				}
				return nil, err
			}
			status = disk.Status
			size := disk.SizeGb * 1024 * 1024
			vInfo.TotalSize = uint64(size)
		}
		switch status {
		case "CREATING", "RESTORING":
			vInfo.Status = storkapi.ApplicationRestoreStatusInProgress
			vInfo.Reason = fmt.Sprintf("Volume restore in progress: %v", status)
		case "DELETING", "FAILED":
			vInfo.Status = storkapi.ApplicationRestoreStatusFailed
			vInfo.Reason = fmt.Sprintf("Restore failed for volume: %v", status)
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

func (g *gcp) InspectNode(id string) (*storkvolume.NodeInfo, error) {
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

// CleanupBackupResources for specified backup
func (g *gcp) CleanupBackupResources(*storkapi.ApplicationBackup) error {
	return nil
}

// CleanupBackupResources for specified restore
func (g *gcp) CleanupRestoreResources(*storkapi.ApplicationRestore) error {
	return nil
}
func init() {
	g := &gcp{}
	err := g.Init(nil)
	if err != nil {
		logrus.Debugf("Error init'ing gcp driver: %v", err)
	}
	if err := storkvolume.Register(driverName, g); err != nil {
		logrus.Panicf("Error registering gcp volume driver: %v", err)
	}
}
