package aws

import (
	"fmt"
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
	"github.com/libopenstorage/openstorage/pkg/units"
	storkvolume "github.com/libopenstorage/stork/drivers/volume"
	storkapi "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/libopenstorage/stork/pkg/errors"
	"github.com/libopenstorage/stork/pkg/k8sutils"
	"github.com/libopenstorage/stork/pkg/log"
	"github.com/portworx/sched-ops/k8s/core"
	"github.com/portworx/sched-ops/k8s/storage"
	storkops "github.com/portworx/sched-ops/k8s/stork"
	"github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/apimachinery/pkg/util/wait"
	k8shelper "k8s.io/component-helpers/storage/volume"
)

const (
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
	return storkvolume.AWSDriverName
}

func (a *aws) Stop() error {
	return nil
}

func (a *aws) OwnsPVCForBackup(
	coreOps core.Ops,
	pvc *v1.PersistentVolumeClaim,
	cmBackupType string,
	crBackupType string,
) bool {
	if cmBackupType == storkapi.ApplicationBackupGeneric {
		// If user has forced the backupType in config map, default to generic always
		return false
	}
	return a.OwnsPVC(coreOps, pvc)
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
		logrus.Tracef("Provisioner in Storageclass not AWS EBS: %v", provisioner)
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
		logrus.Tracef("Provisioner in Storageclass not AWS EBS: %v", provisioner)
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
	client, err := a.getAWSClient(backup.Spec.BackupLocation, backup.Namespace)
	if err != nil {
		return nil, err
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
		volumeInfo.DriverName = storkvolume.AWSDriverName

		pvName, err := core.Instance().GetVolumeForPersistentVolumeClaim(&pvc)
		if err != nil {
			return nil, fmt.Errorf("error getting PV name for PVC (%v/%v): %v", pvc.Namespace, pvc.Name, err)
		}
		pv, err := core.Instance().GetPersistentVolume(pvName)
		if err != nil {
			return nil, fmt.Errorf("error getting pv %v: %v", pvName, err)
		}
		ebsName := storkvolume.GetEBSVolumeID(pv)
		if ebsName == "" {
			return nil, fmt.Errorf("AWS EBS info not found in PV %v", pvName)
		}

		ebsVolume, err := storkvolume.GetEBSVolume(ebsName, nil, client)
		if err != nil {
			return nil, err
		}

		volume := pvc.Spec.VolumeName
		volumeInfo.Volume = volume
		volumeInfo.Zones = []string{*ebsVolume.AvailabilityZone}

		tags := storkvolume.GetApplicationBackupLabels(backup, &pvc)
		tags[nameTag] = "stork-snapshot-" + volume

		// First check if we've already created a snapshot for this volume
		if snapshot, err := a.getEBSSnapshot("", tags, client); err == nil {
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
				snapshot, snapErr = client.CreateSnapshot(snapshotInput)
				if snapErr != nil {
					if awsErr, ok := snapErr.(awserr.Error); ok {
						if !isExponentialError(awsErr) {
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
					if awsErr, ok := snapErr.(awserr.Error); ok && isExponentialError(awsErr) {
						return volumeInfos, &storkvolume.ErrStorageProviderBusy{Reason: awsErr.Message()}
					}
					return nil, snapErr
				}
				return nil, err
			}

			volumeInfo.BackupID = *snapshot.SnapshotId
		}
		volumeInfos = append(volumeInfos, volumeInfo)
	}
	return volumeInfos, nil
}

func (a *aws) getEBSSnapshot(snapshotID string, filters map[string]string, client *ec2.EC2) (*ec2.Snapshot, error) {
	input := &ec2.DescribeSnapshotsInput{}
	if snapshotID != "" {
		input.SnapshotIds = []*string{&snapshotID}
	}

	if len(filters) > 0 {
		input.Filters = a.getFiltersFromMap(filters)
	}

	output, err := client.DescribeSnapshots(input)
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
	client, err := a.getAWSClient(backup.Spec.BackupLocation, backup.Namespace)
	if err != nil {
		return nil, err
	}

	volumeInfos := make([]*storkapi.ApplicationBackupVolumeInfo, 0)

	for _, vInfo := range backup.Status.Volumes {
		if vInfo.DriverName != storkvolume.AWSDriverName {
			continue
		}
		snapshot, err := a.getEBSSnapshot(vInfo.BackupID, nil, client)
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
			// converting to bytes
			vInfo.TotalSize = uint64(*snapshot.VolumeSize) * units.GiB
			vInfo.ActualSize = uint64(*snapshot.VolumeSize) * units.GiB
		}
		volumeInfos = append(volumeInfos, vInfo)
	}
	return volumeInfos, nil

}

func (a *aws) CancelBackup(backup *storkapi.ApplicationBackup) error {
	_, err := a.DeleteBackup(backup)
	return err
}

func (a *aws) DeleteBackup(backup *storkapi.ApplicationBackup) (bool, error) {
	client, err := a.getAWSClient(backup.Spec.BackupLocation, backup.Namespace)
	if err != nil {
		return true, err
	}

	for _, vInfo := range backup.Status.Volumes {
		if vInfo.DriverName != storkvolume.AWSDriverName {
			continue
		}
		input := &ec2.DeleteSnapshotInput{
			SnapshotId: aws_sdk.String(vInfo.BackupID),
		}

		_, err := client.DeleteSnapshot(input)
		if err != nil {
			// Do nothing if snapshot isn't found
			if awsErr, ok := err.(awserr.Error); ok {
				if awsErr.Code() == "InvalidSnapshot.NotFound" {
					continue
				}
			}
			return true, err
		}
	}
	return true, nil
}

func (a *aws) UpdateMigratedPersistentVolumeSpec(
	pv *v1.PersistentVolume,
	vInfo *storkapi.ApplicationRestoreVolumeInfo,
	namespaceMapping map[string]string,
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
	*storkapi.ApplicationRestore,
	[]runtime.Unstructured,
	[]byte,
) ([]runtime.Unstructured, error) {
	return nil, nil
}

func (a *aws) StartRestore(
	restore *storkapi.ApplicationRestore,
	volumeBackupInfos []*storkapi.ApplicationBackupVolumeInfo,
	preRestoreObjects []runtime.Unstructured,
) ([]*storkapi.ApplicationRestoreVolumeInfo, error) {
	client, err := a.getAWSClient(restore.Spec.BackupLocation, restore.Namespace)
	if err != nil {
		return nil, err
	}

	volumeInfos := make([]*storkapi.ApplicationRestoreVolumeInfo, 0)
	for _, backupVolumeInfo := range volumeBackupInfos {
		volumeInfo := &storkapi.ApplicationRestoreVolumeInfo{}
		volumeInfo.PersistentVolumeClaim = backupVolumeInfo.PersistentVolumeClaim
		volumeInfo.PersistentVolumeClaimUID = backupVolumeInfo.PersistentVolumeClaimUID
		volumeInfo.SourceNamespace = backupVolumeInfo.Namespace
		volumeInfo.SourceVolume = backupVolumeInfo.Volume
		volumeInfo.DriverName = storkvolume.AWSDriverName
		volumeInfo.RestoreVolume = a.generatePVName()

		tags := storkvolume.GetApplicationRestoreLabels(restore, volumeInfo)
		tags[nameTag] = volumeInfo.RestoreVolume

		// First check if we've already created a volume for this restore
		// operation
		if output, err := storkvolume.GetEBSVolume("", tags, client); err == nil {
			volumeInfo.RestoreVolume = *output.VolumeId
		} else {
			if len(backupVolumeInfo.Zones) == 0 {
				return nil, fmt.Errorf("zone missing in backup for volume (%v) %v", backupVolumeInfo.Namespace, backupVolumeInfo.PersistentVolumeClaim)
			}
			ebsSnapshot, err := a.getEBSSnapshot(backupVolumeInfo.BackupID, nil, client)
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
			var createErr error
			var createVolume *ec2.Volume
			err = wait.ExponentialBackoff(apiBackoff, func() (bool, error) {
				createVolume, createErr = client.CreateVolume(input)
				if createErr != nil {
					if awsErr, ok := createErr.(awserr.Error); ok {
						if !isExponentialError(awsErr) {
							return true, createErr
						}
						log.ApplicationRestoreLog(restore).Warnf("Retrying AWS "+
							"volume restore for %v/%v : %v", volumeInfo.PersistentVolumeClaim, volumeInfo.SourceVolume, createErr)
					}
					return false, nil
				}
				return true, nil
			})
			if err != nil {
				if createErr != nil {
					if awsErr, ok := createErr.(awserr.Error); ok && isExponentialError(awsErr) {
						return volumeInfos, &storkvolume.ErrStorageProviderBusy{Reason: awsErr.Message()}
					}
					return nil, createErr
				}
				return nil, err
			}
			volumeInfo.RestoreVolume = *createVolume.VolumeId
			volumeInfos = append(volumeInfos, volumeInfo)
		}
	}
	return volumeInfos, nil
}

func (a *aws) CancelRestore(*storkapi.ApplicationRestore) error {
	return nil
}

func (a *aws) GetRestoreStatus(restore *storkapi.ApplicationRestore) ([]*storkapi.ApplicationRestoreVolumeInfo, error) {
	client, err := a.getAWSClient(restore.Spec.BackupLocation, restore.Namespace)
	if err != nil {
		return nil, err
	}

	volumeInfos := make([]*storkapi.ApplicationRestoreVolumeInfo, 0)
	for _, vInfo := range restore.Status.Volumes {
		if vInfo.DriverName != storkvolume.AWSDriverName {
			continue
		}
		if vInfo.Status == storkapi.ApplicationRestoreStatusSuccessful || vInfo.Status == storkapi.ApplicationRestoreStatusFailed || vInfo.Status == storkapi.ApplicationRestoreStatusRetained {
			volumeInfos = append(volumeInfos, vInfo)
			continue
		}
		ebsVolume, err := storkvolume.GetEBSVolume(vInfo.RestoreVolume, nil, client)
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
			// converting to bytes
			vInfo.TotalSize = uint64(*ebsVolume.Size) * units.GiB
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

func (a *aws) GetPodVolumes(podSpec *v1.PodSpec, namespace string, includePendingWFFC bool) ([]*storkvolume.Info, []*storkvolume.Info, error) {
	return nil, nil, &errors.ErrNotSupported{}
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

// CleanupBackupResources for specified backup
func (a *aws) CleanupBackupResources(*storkapi.ApplicationBackup) error {
	return nil
}

// CleanupBackupResources for specified restore
func (a *aws) CleanupRestoreResources(*storkapi.ApplicationRestore) error {
	return nil
}

// getAWSClientFromBackupLocation will return a client object using creds referred in backuplocation
func (a *aws) getAWSClientFromBackupLocation(backupLocationName, ns string) *ec2.EC2 {
	var client *ec2.EC2
	backupLocation, err := storkops.Instance().GetBackupLocation(backupLocationName, ns)
	if err != nil {
		logrus.Errorf("error getting backup location %s resource: %v", backupLocationName, err)
		return nil
	}
	metadata, err := cloud.NewMetadata()
	if err != nil {
		logrus.Errorf("error creating metadata instance: %v", err)
		return nil
	}
	if len(backupLocation.Cluster.SecretConfig) > 0 {
		s, err := session.NewSession(&aws_sdk.Config{
			Region:      aws_sdk.String(metadata.GetRegion()),
			Credentials: credentials.NewStaticCredentials(backupLocation.Cluster.AWSClusterConfig.AccessKeyID, backupLocation.Cluster.AWSClusterConfig.SecretAccessKey, ""),
		})
		if err != nil {
			logrus.Errorf("error creating aws client session for backuplocation %s: %v", backupLocationName, err)
			return nil
		}
		client = ec2.New(s)
	}
	return client
}

func (a *aws) getAWSClient(backupLocationName, ns string) (*ec2.EC2, error) {
	var client *ec2.EC2
	// if backuplocation has creds wrt the cluster, need to use that
	client = a.getAWSClientFromBackupLocation(backupLocationName, ns)
	if client == nil {
		if a.client == nil {
			if err := a.Init(nil); err != nil {
				return client, err
			}
		}
		client = a.client
	}
	return client, nil
}

// GetPodPatches returns driver-specific json patches to mutate the pod in a webhook
func (a *aws) GetPodPatches(podNamespace string, pod *v1.Pod) ([]k8sutils.JSONPatchOp, error) {
	return nil, nil
}

// GetCSIPodPrefix returns prefix for the csi pod names in the deployment
func (a *aws) GetCSIPodPrefix() (string, error) {
	return "", &errors.ErrNotSupported{}
}

func init() {
	a := &aws{}
	err := a.Init(nil)
	if err != nil {
		logrus.Debugf("Error init'ing aws driver: %v", err)
	}
	if err := storkvolume.Register(storkvolume.AWSDriverName, a); err != nil {
		logrus.Panicf("Error registering aws volume driver: %v", err)
	}
}

func isExponentialError(err error) bool {
	// Got the list of error codes from here
	// https://docs.aws.amazon.com/AWSEC2/latest/APIReference/errors-overview.html
	awsCodes := map[string]struct{}{
		"InternalError":                         {},
		"SnapshotCreationPerVolumeRateExceeded": {},
		"VolumeLimitExceeded":                   {},
		"AttachmentLimitExceeded":               {},
		"MaxIOPSLimitExceeded":                  {},
		"ResourceLimitExceeded":                 {},
		"RequestLimitExceeded":                  {},
		"SnapshotLimitExceeded":                 {},
		"TagLimitExceeded":                      {},
	}
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			if _, exist := awsCodes[awsErr.Code()]; exist {
				return true
			}
		}
	}
	return false
}
