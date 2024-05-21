package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	// ApplicationBackupResourceName is name for "applicationbackup" resource
	ApplicationBackupResourceName = "applicationbackup"
	// ApplicationBackupResourcePlural is plural for "applicationbackup" resource
	ApplicationBackupResourcePlural = "applicationbackups"
	// ApplicationBackupGeneric for using generic driver for backups/restore
	ApplicationBackupGeneric = "Generic"
	// GenericDriver is name for generic driver
	GenericDriver = "kdmp"
)

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ApplicationBackup represents applicationbackup object
type ApplicationBackup struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              ApplicationBackupSpec   `json:"spec"`
	Status            ApplicationBackupStatus `json:"status"`
}

// ApplicationBackupSpec is the spec used to backup applications
type ApplicationBackupSpec struct {
	Namespaces         []string                           `json:"namespaces"`
	BackupLocation     string                             `json:"backupLocation"`
	PlatformCredential string                             `json:"platformCredential"`
	RancherProjects    map[string]string                  `json:"rancherProjects"`
	Selectors          map[string]string                  `json:"selectors"`
	NamespaceSelector  string                             `json:"namespaceSelector"`
	PreExecRule        string                             `json:"preExecRule"`
	PostExecRule       string                             `json:"postExecRule"`
	ReclaimPolicy      ApplicationBackupReclaimPolicyType `json:"reclaimPolicy"`
	SkipServiceUpdate  bool                               `json:"skipServiceUpdate"`
	// Options to be passed in to the driver
	Options          map[string]string `json:"options"`
	IncludeResources []ObjectInfo      `json:"includeResources"`
	ResourceTypes    []string          `json:"resourceTypes"`
	BackupType       string            `json:"backupType"`
	// CSISnapshotClassMap is 1 to 1 Map of CSI Provisioner to its Corresponding VolumeSnapshotClass to be used for backup
	CSISnapshotClassMap map[string]string `json:"csiSnapshotClassMap"`
	// BackupObjectType set to All for Namespace backup, VirtualMachine for VM specific Backup
	BackupObjectType string `json:"backupObjectType"`
	// SkipAutoExecRules is false by default, if set true will skip auto exec rules for VM specific backup.
	// This field is unused for non VM specific Backup.
	SkipAutoExecRules bool `json:"skipAutoExecRules"`
	DirectKDMP        bool `json:"directKDMP"`
}

// ApplicationBackupReclaimPolicyType is the reclaim policy for the application backup
type ApplicationBackupReclaimPolicyType string

const (
	// ApplicationBackupReclaimPolicyDelete is to specify that the backup should
	// be deleted when the object is deleted
	ApplicationBackupReclaimPolicyDelete ApplicationBackupReclaimPolicyType = "Delete"
	// ApplicationBackupReclaimPolicyRetain is to specify that the backup should
	// be retained when the object is deleted
	ApplicationBackupReclaimPolicyRetain ApplicationBackupReclaimPolicyType = "Retain"
)

// ApplicationBackupStatus is the status of a application backup operation
type ApplicationBackupStatus struct {
	Stage                ApplicationBackupStageType       `json:"stage"`
	Status               ApplicationBackupStatusType      `json:"status"`
	Reason               string                           `json:"reason"`
	Resources            []*ApplicationBackupResourceInfo `json:"resources"`
	Volumes              []*ApplicationBackupVolumeInfo   `json:"volumes"`
	BackupPath           string                           `json:"backupPath"`
	TriggerTimestamp     metav1.Time                      `json:"triggerTimestamp"`
	LastUpdateTimestamp  metav1.Time                      `json:"lastUpdateTimestamp"`
	FinishTimestamp      metav1.Time                      `json:"finishTimestamp"`
	TotalSize            uint64                           `json:"totalSize"`
	ResourceCount        int                              `json:"resourceCount"`
	LargeResourceEnabled bool                             `json:"largeResourceEnabled"`
	FailedVolCount       int                              `json:"failedVolCount"`
}

// ObjectInfo contains info about an object being backed up or restored
type ObjectInfo struct {
	Name                    string `json:"name"`
	Namespace               string `json:"namespace"`
	metav1.GroupVersionKind `json:",inline"`
}

// ApplicationBackupResourceInfo is the info for the backup of a resource
type ApplicationBackupResourceInfo struct {
	ObjectInfo `json:",inline"`
	Status     ApplicationBackupStatusType `json:"status"`
	Reason     string                      `json:"reason"`
}

// This object is used in VolumeInfo for PSA enabled cluster to retain the runAsUser ID and runAsGroup ID used by
// Job pod(KDMP/NFS)during backup. We will use the same IDs to spin up Job Pods during restore.
type JobSecurityContext struct {
	RunAsUser  int64 `json:"runAsUser"`
	RunAsGroup int64 `json:"runAsGroup"`
}

// ApplicationBackupVolumeInfo is the info for the backup of a volume
type ApplicationBackupVolumeInfo struct {
	PersistentVolumeClaim    string                      `json:"persistentVolumeClaim"`
	PersistentVolumeClaimUID string                      `json:"persistentVolumeClaimUID"`
	Namespace                string                      `json:"namespace"`
	Volume                   string                      `json:"volume"`
	BackupID                 string                      `json:"backupID"`
	DriverName               string                      `json:"driverName"`
	Zones                    []string                    `json:"zones"`
	Status                   ApplicationBackupStatusType `json:"status"`
	Reason                   string                      `json:"reason"`
	Options                  map[string]string           `jons:"options"`
	TotalSize                uint64                      `json:"totalSize"`
	ActualSize               uint64                      `json:"actualSize"`
	StorageClass             string                      `json:"storageClass"`
	Provisioner              string                      `json:"provisioner"`
	VolumeSnapshot           string                      `json:"volumeSnapshot"`
	// It preserves the uid and gid of the pod that is run by the backup job
	// that helps in restore operation. this is required only when PSA is enforced.
	JobSecurityContext JobSecurityContext `json:",inline"`
}

// ApplicationBackupStatusType is the status of the application backup
type ApplicationBackupStatusType string

const (
	// ApplicationBackupStatusInitial is the initial state when backup is created
	ApplicationBackupStatusInitial ApplicationBackupStatusType = ""
	// ApplicationBackupStatusPending for when backup is still pending
	ApplicationBackupStatusPending ApplicationBackupStatusType = "Pending"
	// ApplicationBackupStatusInProgress for when backup is in progress
	ApplicationBackupStatusInProgress ApplicationBackupStatusType = "InProgress"
	// ApplicationBackupStatusFailed for when backup has failed
	ApplicationBackupStatusFailed ApplicationBackupStatusType = "Failed"
	// ApplicationBackupStatusPartialSuccess for when backup was partially successful
	ApplicationBackupStatusPartialSuccess ApplicationBackupStatusType = "PartialSuccess"
	// ApplicationBackupStatusSuccessful for when backup has completed successfully
	ApplicationBackupStatusSuccessful ApplicationBackupStatusType = "Successful"
)

// ApplicationBackupStageType is the stage of the backup
type ApplicationBackupStageType string

const (
	// ApplicationBackupStageInitial for when backup is created
	ApplicationBackupStageInitial ApplicationBackupStageType = ""
	// ApplicationBackupStageImportResource for when vm resources are imported
	ApplicationBackupStageImportResource ApplicationBackupStageType = "ImportResource"
	// ApplicationBackupStagePreExecRule for when the PreExecRule is being executed
	ApplicationBackupStagePreExecRule ApplicationBackupStageType = "PreExecRule"
	// ApplicationBackupStagePostExecRule for when the PostExecRule is being executed
	ApplicationBackupStagePostExecRule ApplicationBackupStageType = "PostExecRule"
	// ApplicationBackupStageVolumes for when volumes are being backed up
	ApplicationBackupStageVolumes ApplicationBackupStageType = "Volumes"
	// ApplicationBackupStageApplications for when applications are being backed up
	ApplicationBackupStageApplications ApplicationBackupStageType = "Applications"
	// ApplicationBackupStageFinal is the final stage for backup
	ApplicationBackupStageFinal ApplicationBackupStageType = "Final"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ApplicationBackupList is a list of ApplicationBackups
type ApplicationBackupList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []ApplicationBackup `json:"items"`
}

// CreateObjectsMap create a map of objects that are to be included in an
// operation. Allows quick lookup of objects
func CreateObjectsMap(
	includeObjects []ObjectInfo,
) map[ObjectInfo]bool {
	objectsMap := make(map[ObjectInfo]bool)
	for i := 0; i < len(includeObjects); i++ {
		if includeObjects[i].Group == "" {
			includeObjects[i].Group = "core"
		}
		objectsMap[includeObjects[i]] = true
	}
	return objectsMap
}
