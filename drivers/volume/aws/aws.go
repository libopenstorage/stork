package aws

import (
	"fmt"
	"regexp"
	"time"

	aws_sdk "github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/credentials/ec2rolecreds"
	"github.com/aws/aws-sdk-go/aws/ec2metadata"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/ec2"
	snapv1 "github.com/kubernetes-incubator/external-storage/snapshot/pkg/apis/crd/v1"
	snapshotVolume "github.com/kubernetes-incubator/external-storage/snapshot/pkg/volume"
	"github.com/kubernetes-sigs/aws-ebs-csi-driver/pkg/cloud"
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
	"k8s.io/apimachinery/pkg/util/wait"
	k8shelper "k8s.io/kubernetes/pkg/apis/core/v1/helper"
)

const (
	// driverName is the name of the aws driver implementation
	driverName = "aws"
	// provisioner names for ebs volumes
	provisionerName = "kubernetes.io/aws-ebs"
	// CSI provisioner name for ebs volumes
	csiProvisionerName = "ebs.csi.aws.com"
	// pvcProvisionerAnnotation is the annotation on PVC which has the
	// provisioner name
	pvcProvisionerAnnotation = "volume.beta.kubernetes.io/storage-provisioner"
	// pvProvisionedByAnnotation is the annotation on PV which has the
	// provisioner name
	pvProvisionedByAnnotation = "pv.kubernetes.io/provisioned-by"
	pvNamePrefix              = "pvc-"

	// Tags used for snapshots and disks
	restoreUIDTag         = "restore-uid"
	nameTag               = "Name"
	createdByTag          = "created-by"
	backupUIDTag          = "backup-uid"
	sourcePVCNameTag      = "source-pvc-name"
	sourcePVCNamespaceTag = "source-pvc-namespace"
)

var (
	apiBackoff = wait.Backoff{
		Duration: 5 * time.Second,
		Factor:   2,
		Jitter:   1,
		Steps:    10,
	}
)

type aws struct {
	client *ec2.EC2
	storkvolume.ClusterPairNotSupported
	storkvolume.MigrationNotSupported
	storkvolume.GroupSnapshotNotSupported
	storkvolume.ClusterDomainsNotSupported
	storkvolume.CloneNotSupported
	storkvolume.SnapshotRestoreNotSupported
}

func (a *aws) Init(_ interface{}) error {

	s, err := session.NewSession(&aws_sdk.Config{})
	if err != nil {
		return err
	}
	creds := credentials.NewChainCredentials(
		[]credentials.Provider{
			&credentials.EnvProvider{},
			&ec2rolecreds.EC2RoleProvider{
				Client: ec2metadata.New(s),
			},
			&credentials.SharedCredentialsProvider{},
		})
	metadata, err := cloud.NewMetadata()
	if err != nil {
		return err
	}

	s, err = session.NewSession(&aws_sdk.Config{
		Region:      aws_sdk.String(metadata.GetRegion()),
		Credentials: creds,
	})
	if err != nil {
		return err
	}
	a.client = ec2.New(s)

	return nil
}

func (a *aws) String() string {
	return driverName
}

func (a *aws) Stop() error {
	return nil
}

func (a *aws) OwnsPVC(coreOps core.Ops, pvc *v1.PersistentVolumeClaim) bool {

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
		logrus.Debugf("Provisioner in Storageclass not AWS EBS: %v", provisioner)
		return false
	}
	return true
}

func (a *aws) OwnsPV(pv *v1.PersistentVolume) bool {
	var provisioner string
	// Check the annotation in the PV for the provisioner
	if val, ok := pv.Annotations[pvProvisionedByAnnotation]; ok {
		provisioner = val
	} else {
		// Finally check the volume reference in the spec
		if pv.Spec.AWSElasticBlockStore != nil {
			return true
		}
	}
	if provisioner != provisionerName &&
		!isCsiProvisioner(provisioner) {
		logrus.Debugf("Provisioner in Storageclass not AWS EBS: %v", provisioner)
		return false
	}
	return true
}

func isCsiProvisioner(provisioner string) bool {
	return csiProvisionerName == provisioner
}

func (a *aws) StartBackup(backup *storkapi.ApplicationBackup,
	pvcs []v1.PersistentVolumeClaim,
) ([]*storkapi.ApplicationBackupVolumeInfo, error) {
	if a.client == nil {
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
		volumeInfo := &storkapi.ApplicationBackupVolumeInfo{}
		volumeInfo.PersistentVolumeClaim = pvc.Name
		volumeInfo.Namespace = pvc.Namespace
		volumeInfo.DriverName = driverName
		volumeInfos = append(volumeInfos, volumeInfo)

		pvName, err := core.Instance().GetVolumeForPersistentVolumeClaim(&pvc)
		if err != nil {
			return nil, fmt.Errorf("error getting PV name for PVC (%v/%v): %v", pvc.Namespace, pvc.Name, err)
		}
		pv, err := core.Instance().GetPersistentVolume(pvName)
		if err != nil {
			return nil, fmt.Errorf("error getting pv %v: %v", pvName, err)
		}
		ebsName := a.getEBSVolumeID(pv)
		if ebsName == "" {
			return nil, fmt.Errorf("AWS EBS info not found in PV %v", pvName)
		}

		ebsVolume, err := a.getEBSVolume(ebsName, nil)
		if err != nil {
			return nil, err
		}

		volume := pvc.Spec.VolumeName
		volumeInfo.Volume = volume
		volumeInfo.Zones = []string{*ebsVolume.AvailabilityZone}

		tags := storkvolume.GetApplicationBackupLabels(backup, &pvc)
		tags[nameTag] = "stork-snapshot-" + volume

		// First check if we've already created a snapshot for this volume
		if snapshot, err := a.getEBSSnapshot("", tags); err == nil {
			volumeInfo.BackupID = *snapshot.SnapshotId
		} else {

			snapshotInput := &ec2.CreateSnapshotInput{
				VolumeId: aws_sdk.String(ebsName),
				Description: aws_sdk.String(fmt.Sprintf("Created by stork for %v for PVC %v Namespace %v Volume: %v",
					backup.Name, pvc.Name, pvc.Namespace, ebsName)),
				TagSpecifications: []*ec2.TagSpecification{
					{
						ResourceType: aws_sdk.String(ec2.ResourceTypeSnapshot),
					},
				},
			}
			snapshotInput.TagSpecifications[0].Tags = make([]*ec2.Tag, 0)
			for k, v := range tags {
				snapshotInput.TagSpecifications[0].Tags = append(snapshotInput.TagSpecifications[0].Tags, &ec2.Tag{
					Key:   aws_sdk.String(k),
					Value: aws_sdk.String(v),
				})
			}

			// Pick up all tags from source that don't conflict with new ones we've
			// created
			sourceTags := make([]*ec2.Tag, 0)
			for _, tag := range ebsVolume.Tags {
				if _, present := tags[*tag.Key]; present {
					continue
				}
				sourceTags = append(sourceTags, tag)
			}
			snapshotInput.TagSpecifications[0].Tags = append(snapshotInput.TagSpecifications[0].Tags, sourceTags...)
			var snapshot *ec2.Snapshot
			var snapErr error
			err = wait.ExponentialBackoff(apiBackoff, func() (bool, error) {
				snapshot, snapErr = a.client.CreateSnapshot(snapshotInput)
				if snapErr != nil {
					if awsErr, ok := snapErr.(awserr.Error); ok {
						if awsErr.Code() != "SnapshotCreationPerVolumeRateExceeded" && awsErr.Code() != "InternalError" {
							return true, snapErr
						}
						log.ApplicationBackupLog(backup).Warnf("Retrying AWS snapshot for %v/%v : %v", pvc.Name, pvc.Namespace, snapErr)
					}
					return false, nil
				}
				return true, nil
			})
			if err != nil {
				if snapErr != nil {
					return nil, snapErr
				}
				return nil, err
			}

			volumeInfo.BackupID = *snapshot.SnapshotId
		}

	}
	return volumeInfos, nil
}

func (a *aws) getEBSVolumeID(pv *v1.PersistentVolume) string {
	var volumeID string
	if pv.Spec.AWSElasticBlockStore != nil {
		volumeID = pv.Spec.AWSElasticBlockStore.VolumeID
	} else if pv.Spec.CSI != nil {
		volumeID = pv.Spec.CSI.VolumeHandle
	}
	return regexp.MustCompile("vol-.*").FindString(volumeID)
}

func (a *aws) getEBSVolume(volumeID string, filters map[string]string) (*ec2.Volume, error) {
	input := &ec2.DescribeVolumesInput{}
	if volumeID != "" {
		input.VolumeIds = []*string{&volumeID}
	}
	if len(filters) > 0 {
		input.Filters = a.getFiltersFromMap(filters)
	}

	output, err := a.client.DescribeVolumes(input)
	if err != nil {
		return nil, err
	}

	if len(output.Volumes) != 1 {
		return nil, fmt.Errorf("received %v volumes for %v", len(output.Volumes), volumeID)
	}
	return output.Volumes[0], nil
}

func (a *aws) getEBSSnapshot(snapshotID string, filters map[string]string) (*ec2.Snapshot, error) {
	input := &ec2.DescribeSnapshotsInput{}
	if snapshotID != "" {
		input.SnapshotIds = []*string{&snapshotID}
	}

	if len(filters) > 0 {
		input.Filters = a.getFiltersFromMap(filters)
	}

	output, err := a.client.DescribeSnapshots(input)
	if err != nil {
		return nil, err
	}

	if len(output.Snapshots) != 1 {
		return nil, fmt.Errorf("received %v snapshots for %v", len(output.Snapshots), snapshotID)
	}
	return output.Snapshots[0], nil
}

func (a *aws) getFiltersFromMap(filters map[string]string) []*ec2.Filter {
	tagFilters := make([]*ec2.Filter, 0)
	for k, v := range filters {
		tagFilters = append(tagFilters, &ec2.Filter{
			Name:   aws_sdk.String(fmt.Sprintf("tag:%v", k)),
			Values: []*string{aws_sdk.String(v)},
		})
	}
	return tagFilters
}

func (a *aws) GetBackupStatus(backup *storkapi.ApplicationBackup) ([]*storkapi.ApplicationBackupVolumeInfo, error) {
	if a.client == nil {
		if err := a.Init(nil); err != nil {
			return nil, err
		}
	}

	volumeInfos := make([]*storkapi.ApplicationBackupVolumeInfo, 0)

	for _, vInfo := range backup.Status.Volumes {
		if vInfo.DriverName != driverName {
			continue
		}
		snapshot, err := a.getEBSSnapshot(vInfo.BackupID, nil)
		if err != nil {
			return nil, err
		}
		switch *snapshot.State {
		case "pending":
			vInfo.Status = storkapi.ApplicationBackupStatusInProgress
			vInfo.Reason = fmt.Sprintf("Volume backup in progress: %v (%v)", *snapshot.State, *snapshot.Progress)
		case "error":
			vInfo.Status = storkapi.ApplicationBackupStatusFailed
			vInfo.Reason = fmt.Sprintf("Backup failed for volume: %v", *snapshot.State)
		case "completed":
			vInfo.Status = storkapi.ApplicationBackupStatusSuccessful
			vInfo.Reason = "Backup successful for volume"
			vInfo.TotalSize = uint64(*snapshot.VolumeSize)
			vInfo.ActualSize = uint64(*snapshot.VolumeSize)
		}
		volumeInfos = append(volumeInfos, vInfo)
	}
	return volumeInfos, nil

}

func (a *aws) CancelBackup(backup *storkapi.ApplicationBackup) error {
	return a.DeleteBackup(backup)
}

func (a *aws) DeleteBackup(backup *storkapi.ApplicationBackup) error {
	if a.client == nil {
		if err := a.Init(nil); err != nil {
			return err
		}
	}

	for _, vInfo := range backup.Status.Volumes {
		if vInfo.DriverName != driverName {
			continue
		}
		input := &ec2.DeleteSnapshotInput{
			SnapshotId: aws_sdk.String(vInfo.BackupID),
		}

		_, err := a.client.DeleteSnapshot(input)
		if err != nil {
			// Do nothing if snapshot isn't found
			if awsErr, ok := err.(awserr.Error); ok {
				if awsErr.Code() == "InvalidSnapshot.NotFound" {
					continue
				}
			}
			return err
		}
	}
	return nil
}

func (a *aws) UpdateMigratedPersistentVolumeSpec(
	pv *v1.PersistentVolume,
) (*v1.PersistentVolume, error) {
	if pv.Spec.CSI != nil {
		pv.Spec.CSI.VolumeHandle = pv.Name
		return pv, nil
	}

	pv.Spec.AWSElasticBlockStore.VolumeID = pv.Name
	return pv, nil
}

func (a *aws) generatePVName() string {
	return pvNamePrefix + string(uuid.NewUUID())
}
func (a *aws) GetPreRestoreResources(
	*storkapi.ApplicationBackup,
	[]runtime.Unstructured,
) ([]runtime.Unstructured, error) {
	return nil, nil
}

func (a *aws) StartRestore(
	restore *storkapi.ApplicationRestore,
	volumeBackupInfos []*storkapi.ApplicationBackupVolumeInfo,
) ([]*storkapi.ApplicationRestoreVolumeInfo, error) {
	if a.client == nil {
		if err := a.Init(nil); err != nil {
			return nil, err
		}
	}

	volumeInfos := make([]*storkapi.ApplicationRestoreVolumeInfo, 0)
	for _, backupVolumeInfo := range volumeBackupInfos {
		volumeInfo := &storkapi.ApplicationRestoreVolumeInfo{}
		volumeInfo.PersistentVolumeClaim = backupVolumeInfo.PersistentVolumeClaim
		volumeInfo.SourceNamespace = backupVolumeInfo.Namespace
		volumeInfo.SourceVolume = backupVolumeInfo.Volume
		volumeInfo.DriverName = driverName
		volumeInfo.RestoreVolume = a.generatePVName()
		volumeInfos = append(volumeInfos, volumeInfo)

		tags := storkvolume.GetApplicationRestoreLabels(restore, volumeInfo)
		tags[nameTag] = volumeInfo.RestoreVolume

		// First check if we've already created a volume for this restore
		// operation
		if output, err := a.getEBSVolume("", tags); err == nil {
			volumeInfo.RestoreVolume = *output.VolumeId
		} else {
			if len(backupVolumeInfo.Zones) == 0 {
				return nil, fmt.Errorf("zone missing in backup for volume (%v) %v", backupVolumeInfo.Namespace, backupVolumeInfo.PersistentVolumeClaim)
			}
			ebsSnapshot, err := a.getEBSSnapshot(backupVolumeInfo.BackupID, nil)
			if err != nil {
				return nil, err
			}

			input := &ec2.CreateVolumeInput{
				SnapshotId:       aws_sdk.String(backupVolumeInfo.BackupID),
				AvailabilityZone: aws_sdk.String(backupVolumeInfo.Zones[0]),
				TagSpecifications: []*ec2.TagSpecification{
					{
						ResourceType: aws_sdk.String(ec2.ResourceTypeVolume),
					},
				},
			}

			input.TagSpecifications[0].Tags = make([]*ec2.Tag, 0)
			for k, v := range tags {
				input.TagSpecifications[0].Tags = append(input.TagSpecifications[0].Tags, &ec2.Tag{
					Key:   aws_sdk.String(k),
					Value: aws_sdk.String(v),
				})
			}
			// Pick up all tags from source that don't conflict with new ones we've
			// created
			sourceTags := make([]*ec2.Tag, 0)
			for _, tag := range ebsSnapshot.Tags {
				if _, present := tags[*tag.Key]; present {
					continue
				}
				sourceTags = append(sourceTags, tag)
			}
			input.TagSpecifications[0].Tags = append(input.TagSpecifications[0].Tags, sourceTags...)
			output, err := a.client.CreateVolume(input)
			if err != nil {
				return nil, err
			}
			volumeInfo.RestoreVolume = *output.VolumeId
		}
	}
	return volumeInfos, nil
}

func (a *aws) CancelRestore(*storkapi.ApplicationRestore) error {
	return nil
}

func (a *aws) GetRestoreStatus(restore *storkapi.ApplicationRestore) ([]*storkapi.ApplicationRestoreVolumeInfo, error) {
	if a.client == nil {
		if err := a.Init(nil); err != nil {
			return nil, err
		}
	}

	volumeInfos := make([]*storkapi.ApplicationRestoreVolumeInfo, 0)
	for _, vInfo := range restore.Status.Volumes {
		if vInfo.DriverName != driverName {
			continue
		}
		ebsVolume, err := a.getEBSVolume(vInfo.RestoreVolume, nil)
		if err != nil {
			if awsErr, ok := err.(awserr.Error); ok {
				if awsErr.Code() == "InvalidVolume.NotFound" {
					vInfo.Status = storkapi.ApplicationRestoreStatusFailed
					vInfo.Reason = "Restore failed for volume: NotFound"
					volumeInfos = append(volumeInfos, vInfo)
					continue
				}
			}

			return nil, err
		}
		switch *ebsVolume.State {
		default:
			fallthrough
		case "error", "deleting", "deleted":
			vInfo.Status = storkapi.ApplicationRestoreStatusFailed
			vInfo.Reason = fmt.Sprintf("Restore failed for volume: %v", *ebsVolume.State)
		case "creating":
			vInfo.Status = storkapi.ApplicationRestoreStatusInProgress
			vInfo.Reason = fmt.Sprintf("Volume restore in progress: %v", *ebsVolume.State)
		case "available", "in-use":
			vInfo.Status = storkapi.ApplicationRestoreStatusSuccessful
			vInfo.Reason = "Restore successful for volume"
			vInfo.TotalSize = uint64(*ebsVolume.Size)
		}
		volumeInfos = append(volumeInfos, vInfo)
	}

	return volumeInfos, nil
}

func (a *aws) InspectVolume(volumeID string) (*storkvolume.Info, error) {
	return nil, &errors.ErrNotSupported{}
}

func (a *aws) GetClusterID() (string, error) {
	return "", &errors.ErrNotSupported{}
}

func (a *aws) GetNodes() ([]*storkvolume.NodeInfo, error) {
	return nil, &errors.ErrNotSupported{}
}

func (a *aws) InspectNode(id string) (*storkvolume.NodeInfo, error) {
	return nil, &errors.ErrNotSupported{}
}

func (a *aws) GetPodVolumes(podSpec *v1.PodSpec, namespace string) ([]*storkvolume.Info, error) {
	return nil, &errors.ErrNotSupported{}
}

func (a *aws) GetSnapshotPlugin() snapshotVolume.Plugin {
	return nil
}

func (a *aws) GetSnapshotType(snap *snapv1.VolumeSnapshot) (string, error) {
	return "", &errors.ErrNotSupported{}
}

func (a *aws) GetVolumeClaimTemplates([]v1.PersistentVolumeClaim) (
	[]v1.PersistentVolumeClaim, error) {
	return nil, &errors.ErrNotSupported{}
}

func init() {
	a := &aws{}
	err := a.Init(nil)
	if err != nil {
		logrus.Debugf("Error init'ing aws driver: %v", err)
	}
	if err := storkvolume.Register(driverName, a); err != nil {
		logrus.Panicf("Error registering aws volume driver: %v", err)
	}
}
