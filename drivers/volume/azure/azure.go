package azure

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/Azure/azure-sdk-for-go/services/compute/mgmt/2019-03-01/compute"
	"github.com/Azure/go-autorest/autorest"
	azure_rest "github.com/Azure/go-autorest/autorest/azure"
	"github.com/Azure/go-autorest/autorest/azure/auth"
	"github.com/Azure/go-autorest/autorest/to"
	snapv1 "github.com/kubernetes-incubator/external-storage/snapshot/pkg/apis/crd/v1"
	snapshotVolume "github.com/kubernetes-incubator/external-storage/snapshot/pkg/volume"
	storkvolume "github.com/libopenstorage/stork/drivers/volume"
	storkapi "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/libopenstorage/stork/pkg/errors"
	"github.com/libopenstorage/stork/pkg/log"
	"github.com/portworx/sched-ops/k8s/core"
	"github.com/portworx/sched-ops/k8s/storage"
	"github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/uuid"
	k8shelper "k8s.io/kubernetes/pkg/apis/core/v1/helper"
)

const (
	// driverName is the name of the azure driver implementation
	driverName = "azure"
	// provisioner names for azure disks
	provisionerName = "kubernetes.io/azure-disk"
	// CSI provisioner names for azure disks
	csiProvisionerName = "disk.csi.azure.com"
	// pvcProvisionerAnnotation is the annotation on PVC which has the
	// provisioner name
	pvcProvisionerAnnotation = "volume.beta.kubernetes.io/storage-provisioner"
	// pvProvisionedByAnnotation is the annotation on PV which has the
	// provisioner name
	pvProvisionedByAnnotation = "pv.kubernetes.io/provisioned-by"
	pvNamePrefix              = "pvc-"
	subscriptionIDKey         = "subscriptionId"
	resourceGroupKey          = "resourceGroupName"
	metadataURL               = "http://169.254.169.254/metadata/instance/compute"
	apiVersion                = "2018-02-01"
)

type azure struct {
	initDone       bool
	resourceGroup  string
	diskClient     compute.DisksClient
	snapshotClient compute.SnapshotsClient
	storkvolume.ClusterPairNotSupported
	storkvolume.MigrationNotSupported
	storkvolume.GroupSnapshotNotSupported
	storkvolume.ClusterDomainsNotSupported
	storkvolume.CloneNotSupported
	storkvolume.SnapshotRestoreNotSupported
}

func (a *azure) Init(_ interface{}) error {

	authorizer, err := auth.NewAuthorizerFromEnvironment()
	if err != nil {
		return err
	}
	metadata, err := a.getMetadata()
	if err != nil {
		return err
	}
	var ok bool
	var subscriptionID string
	if subscriptionID, ok = metadata[subscriptionIDKey]; !ok {
		return fmt.Errorf("error detecting subscription ID from cluster context")
	}

	a.diskClient = compute.NewDisksClient(subscriptionID)
	a.snapshotClient = compute.NewSnapshotsClient(subscriptionID)
	a.diskClient.Authorizer = authorizer
	a.snapshotClient.Authorizer = authorizer

	if a.resourceGroup, ok = metadata[resourceGroupKey]; !ok {
		return fmt.Errorf("error detecting subscription ID from cluster context")
	}

	a.initDone = true
	return nil
}

func (a *azure) getMetadata() (map[string]string, error) {
	client := http.Client{Timeout: time.Second * 3}

	req, err := http.NewRequest("GET", metadataURL, nil)
	if err != nil {
		return nil, fmt.Errorf("error querying Azure metadata: %v", err)
	}

	req.Header.Add("Metadata", "True")

	q := req.URL.Query()
	q.Add("format", "json")
	q.Add("api-version", apiVersion)
	req.URL.RawQuery = q.Encode()
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("error querying Azure metadata: %v", err)
	}

	defer func() {
		err := resp.Body.Close()
		if err != nil {
			logrus.Errorf("Error closing body when getching azure metadate: %v", err)
		}
	}()
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("error querying Azure metadata: Code %d returned for url %s", resp.StatusCode, req.URL)
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("error querying Azure metadata: %v", err)
	}
	if len(body) == 0 {
		return nil, fmt.Errorf("error querying Azure metadata: Empty response")
	}

	metadata := make(map[string]string)
	err = json.Unmarshal(body, &metadata)
	if err != nil {
		return nil, fmt.Errorf("error parsing Azure metadata: %v", err)
	}

	return metadata, nil
}

func (a *azure) String() string {
	return driverName
}

func (a *azure) Stop() error {
	return nil
}

func (a *azure) OwnsPVC(coreOps core.Ops, pvc *v1.PersistentVolumeClaim) bool {

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
		return a.OwnsPV(pv)
	}

	if provisioner != provisionerName &&
		!isCsiProvisioner(provisioner) {
		logrus.Debugf("Provisioner in Storageclass not Azure: %v", provisioner)
		return false
	}
	return true
}

func (a *azure) OwnsPV(pv *v1.PersistentVolume) bool {
	var provisioner string
	// Check the annotation in the PV for the provisioner
	if val, ok := pv.Annotations[pvProvisionedByAnnotation]; ok {
		provisioner = val
	} else {
		// Finally check the volume reference in the spec
		if pv.Spec.AzureDisk != nil {
			return true
		}
	}
	if provisioner != provisionerName &&
		!isCsiProvisioner(provisioner) {
		logrus.Debugf("Provisioner in Storageclass not AzureDisk: %v", provisioner)
		return false
	}
	return true
}

func isCsiProvisioner(provisioner string) bool {
	return csiProvisionerName == provisioner
}

func (a *azure) findExistingSnapshot(tags map[string]string) (*compute.Snapshot, error) {
	snapshotList, err := a.snapshotClient.List(context.TODO())
	if err != nil {
		return nil, err
	}
	for {
		for _, snap := range snapshotList.Values() {
			match := true
			// If any of the tags aren't found or match, skip the snapshot
			for k, v := range tags {
				if value, present := snap.Tags[k]; !present || *value != v {
					match = false
					break
				}
			}
			if match {
				return &snap, nil
			}
		}
		// Move to the next page if there are more snapshots
		if snapshotList.NotDone() {
			if err := snapshotList.Next(); err != nil {
				return nil, err
			}
		} else {
			break
		}
	}
	return nil, nil
}

func (a *azure) StartBackup(
	backup *storkapi.ApplicationBackup,
	pvcs []v1.PersistentVolumeClaim,
) ([]*storkapi.ApplicationBackupVolumeInfo, error) {
	if !a.initDone {
		if err := a.Init(nil); err != nil {
			return nil, err
		}
	}

	volumeInfos := make([]*storkapi.ApplicationBackupVolumeInfo, 0)

	for _, pvc := range pvcs {
		if pvc.DeletionTimestamp != nil {
			log.ApplicationBackupLog(backup).Warnf("Ignoring PVC %v which is being deleted", pvc.Name)
			continue
		}
		volumeInfo := &storkapi.ApplicationBackupVolumeInfo{
			PersistentVolumeClaim: pvc.Name,
			Namespace:             pvc.Namespace,
			DriverName:            driverName,
			Volume:                pvc.Spec.VolumeName,
			Options: map[string]string{
				resourceGroupKey: a.resourceGroup,
			},
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
		tags := storkvolume.GetApplicationBackupLabels(backup, &pvc)

		if snapshot, err := a.findExistingSnapshot(tags); err == nil && snapshot != nil {
			volumeInfo.BackupID = *snapshot.Name
		} else {
			var volume string
			if pv.Spec.AzureDisk != nil {
				volume = pv.Spec.AzureDisk.DiskName
			} else if pv.Spec.CSI != nil {
				resource, err := azure_rest.ParseResourceID(pv.Spec.CSI.VolumeHandle)
				if err != nil {
					return nil, err
				}
				volume = resource.ResourceName
			} else {
				return nil, fmt.Errorf("azure disk info not found in PV %v", pvName)
			}
			disk, err := a.diskClient.Get(context.TODO(), a.resourceGroup, volume)
			if err != nil {
				return nil, err
			}

			snapshot := compute.Snapshot{
				Name: to.StringPtr("stork-snapshot-" + string(uuid.NewUUID())),
				SnapshotProperties: &compute.SnapshotProperties{
					CreationData: &compute.CreationData{
						CreateOption:     compute.Copy,
						SourceResourceID: disk.ID,
					},
				},
				Tags:     make(map[string]*string),
				Location: disk.Location,
			}
			for k, v := range tags {
				snapshot.Tags[k] = to.StringPtr(v)
			}
			_, err = a.snapshotClient.CreateOrUpdate(context.TODO(), a.resourceGroup, *snapshot.Name, snapshot)
			if err != nil {
				return nil, fmt.Errorf("error triggering backup for volume: %v (PVC: %v, Namespace: %v): %v", volume, pvc.Name, pvc.Namespace, err)
			}
			volumeInfo.BackupID = *snapshot.Name
		}
	}
	return volumeInfos, nil
}

func (a *azure) GetBackupStatus(backup *storkapi.ApplicationBackup) ([]*storkapi.ApplicationBackupVolumeInfo, error) {
	if !a.initDone {
		if err := a.Init(nil); err != nil {
			return nil, err
		}
	}

	volumeInfos := make([]*storkapi.ApplicationBackupVolumeInfo, 0)

	for _, vInfo := range backup.Status.Volumes {
		if vInfo.DriverName != driverName {
			continue
		}
		snapshot, err := a.snapshotClient.Get(context.TODO(), a.resourceGroup, vInfo.BackupID)
		if err != nil {
			return nil, err
		}
		switch *snapshot.ProvisioningState {
		case "Failed":
			vInfo.Status = storkapi.ApplicationBackupStatusFailed
			vInfo.Reason = fmt.Sprintf("Backup failed for volume: %v", snapshot.ProvisioningState)
		case "Succeeded":
			vInfo.Status = storkapi.ApplicationBackupStatusSuccessful
			vInfo.Reason = "Backup successful for volume"
			vInfo.TotalSize = uint64(*snapshot.DiskSizeBytes)
			vInfo.ActualSize = uint64(*snapshot.DiskSizeBytes)
		default:
			vInfo.Status = storkapi.ApplicationBackupStatusInProgress
			vInfo.Reason = fmt.Sprintf("Volume backup in progress: %v", snapshot.ProvisioningState)
		}
		volumeInfos = append(volumeInfos, vInfo)
	}

	return volumeInfos, nil

}

func (a *azure) CancelBackup(backup *storkapi.ApplicationBackup) error {
	return a.DeleteBackup(backup)
}

func (a *azure) DeleteBackup(backup *storkapi.ApplicationBackup) error {
	if !a.initDone {
		if err := a.Init(nil); err != nil {
			return err
		}
	}

	for _, vInfo := range backup.Status.Volumes {
		if vInfo.DriverName != driverName {
			continue
		}
		_, err := a.snapshotClient.Delete(context.TODO(), a.resourceGroup, vInfo.BackupID)
		if err != nil {
			// Ignore if the snaphot has already been deleted
			if azureErr, ok := err.(autorest.DetailedError); ok {
				if azureErr.StatusCode == http.StatusNotFound {
					continue
				}
			}
		}
	}
	return nil
}

func (a *azure) UpdateMigratedPersistentVolumeSpec(
	pv *v1.PersistentVolume,
) (*v1.PersistentVolume, error) {
	disk, err := a.diskClient.Get(context.TODO(), a.resourceGroup, pv.Name)
	if err != nil {
		return nil, err
	}

	if pv.Spec.CSI != nil {
		pv.Spec.CSI.VolumeHandle = *disk.ID
		return pv, nil
	}

	pv.Spec.AzureDisk.DiskName = pv.Name
	pv.Spec.AzureDisk.DataDiskURI = *disk.ID

	return pv, nil
}

func (a *azure) generatePVName() string {
	return pvNamePrefix + string(uuid.NewUUID())
}

func (a *azure) findExistingDisk(tags map[string]string) (*compute.Disk, error) {
	diskList, err := a.diskClient.List(context.TODO())
	if err != nil {
		return nil, err
	}
	for {
		for _, disk := range diskList.Values() {
			match := true
			// If any of the tags aren't found or match, skip the disk
			for k, v := range tags {
				if value, present := disk.Tags[k]; !present || *value != v {
					match = false
					break
				}
			}
			if match {
				return &disk, nil
			}
		}
		// Move to the next page if there are more disks
		if diskList.NotDone() {
			if err := diskList.Next(); err != nil {
				return nil, err
			}
		} else {
			break
		}
	}
	return nil, nil
}

func (a *azure) GetPreRestoreResources(
	*storkapi.ApplicationBackup,
	[]runtime.Unstructured,
) ([]runtime.Unstructured, error) {
	return nil, nil
}

func (a *azure) StartRestore(
	restore *storkapi.ApplicationRestore,
	volumeBackupInfos []*storkapi.ApplicationBackupVolumeInfo,
) ([]*storkapi.ApplicationRestoreVolumeInfo, error) {
	if !a.initDone {
		if err := a.Init(nil); err != nil {
			return nil, err
		}
	}

	volumeInfos := make([]*storkapi.ApplicationRestoreVolumeInfo, 0)
	for _, backupVolumeInfo := range volumeBackupInfos {
		var resourceGroup string
		if val, present := backupVolumeInfo.Options[resourceGroupKey]; present {
			resourceGroup = val
		} else {
			resourceGroup = a.resourceGroup
			logrus.Warnf("missing resource group in snapshot %v, will use current resource group", backupVolumeInfo.BackupID)
		}

		snapshot, err := a.snapshotClient.Get(context.TODO(), resourceGroup, backupVolumeInfo.BackupID)
		if err != nil {
			return nil, err
		}
		volumeInfo := &storkapi.ApplicationRestoreVolumeInfo{
			PersistentVolumeClaim: backupVolumeInfo.PersistentVolumeClaim,
			SourceNamespace:       backupVolumeInfo.Namespace,
			SourceVolume:          backupVolumeInfo.Volume,
			DriverName:            driverName,
		}
		volumeInfos = append(volumeInfos, volumeInfo)

		tags := storkvolume.GetApplicationRestoreLabels(restore, volumeInfo)

		if disk, err := a.findExistingDisk(tags); err == nil && disk != nil {
			volumeInfo.RestoreVolume = *disk.Name
		} else {
			disk := compute.Disk{

				Name: to.StringPtr(a.generatePVName()),
				DiskProperties: &compute.DiskProperties{
					CreationData: &compute.CreationData{
						CreateOption:     compute.Copy,
						SourceResourceID: snapshot.ID,
					},
				},
				Tags:     make(map[string]*string),
				Location: snapshot.Location,
			}

			for k, v := range tags {
				disk.Tags[k] = to.StringPtr(v)
			}
			_, err = a.diskClient.CreateOrUpdate(context.TODO(), a.resourceGroup, *disk.Name, disk)
			if err != nil {
				return nil, fmt.Errorf("error triggering restore for volume: %v: %v",
					backupVolumeInfo.Volume, err)
			}
			volumeInfo.RestoreVolume = *disk.Name
		}
	}
	return volumeInfos, nil
}

func (a *azure) CancelRestore(*storkapi.ApplicationRestore) error {
	// Do nothing to cancel restores for now
	return nil
}

func (a *azure) GetRestoreStatus(restore *storkapi.ApplicationRestore) ([]*storkapi.ApplicationRestoreVolumeInfo, error) {
	if !a.initDone {
		if err := a.Init(nil); err != nil {
			return nil, err
		}
	}

	volumeInfos := make([]*storkapi.ApplicationRestoreVolumeInfo, 0)
	for _, vInfo := range restore.Status.Volumes {
		disk, err := a.diskClient.Get(context.TODO(), a.resourceGroup, vInfo.RestoreVolume)
		if err != nil {
			if azureErr, ok := err.(autorest.DetailedError); ok {
				if azureErr.StatusCode == http.StatusNotFound {
					vInfo.Status = storkapi.ApplicationRestoreStatusFailed
					vInfo.Reason = "Restore failed for volume: NotFound"
					volumeInfos = append(volumeInfos, vInfo)
					continue
				}
			}

			return nil, err
		}
		switch *disk.ProvisioningState {
		case "Failed":
			vInfo.Status = storkapi.ApplicationRestoreStatusFailed
			vInfo.Reason = fmt.Sprintf("Restore failed for volume: %v", disk.ProvisioningState)
		case "Succeeded":
			vInfo.Status = storkapi.ApplicationRestoreStatusSuccessful
			vInfo.Reason = "Restore successful for volume"
			vInfo.TotalSize = uint64(*disk.DiskSizeBytes)
		default:
			vInfo.Status = storkapi.ApplicationRestoreStatusInProgress
			vInfo.Reason = fmt.Sprintf("Volume restore in progress: %v", disk.ProvisioningState)
		}
		volumeInfos = append(volumeInfos, vInfo)
	}

	return volumeInfos, nil
}

func (a *azure) InspectVolume(volumeID string) (*storkvolume.Info, error) {
	return nil, &errors.ErrNotSupported{}
}

func (a *azure) GetClusterID() (string, error) {
	return "", &errors.ErrNotSupported{}
}

func (a *azure) GetNodes() ([]*storkvolume.NodeInfo, error) {
	return nil, &errors.ErrNotSupported{}
}

func (a *azure) InspectNode(id string) (*storkvolume.NodeInfo, error) {
	return nil, &errors.ErrNotSupported{}
}

func (a *azure) GetPodVolumes(podSpec *v1.PodSpec, namespace string) ([]*storkvolume.Info, error) {
	return nil, &errors.ErrNotSupported{}
}

func (a *azure) GetSnapshotPlugin() snapshotVolume.Plugin {
	return nil
}

func (a *azure) GetSnapshotType(snap *snapv1.VolumeSnapshot) (string, error) {
	return "", &errors.ErrNotSupported{}
}

func (a *azure) GetVolumeClaimTemplates([]v1.PersistentVolumeClaim) (
	[]v1.PersistentVolumeClaim, error) {
	return nil, &errors.ErrNotSupported{}
}

func init() {
	a := &azure{}
	err := a.Init(nil)
	if err != nil {
		logrus.Debugf("Error init'ing azure driver: %v", err)
	}
	if err := storkvolume.Register(driverName, a); err != nil {
		logrus.Panicf("Error registering azure volume driver: %v", err)
	}
}
