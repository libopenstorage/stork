package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	// ApplicationBackupResourceName is name for "applicationbackup" resource
	ApplicationBackupResourceName = "applicationbackup"
	// ApplicationBackupResourcePlural is plural for "applicationbackup" resource
	ApplicationBackupResourcePlural = "applicationbackups"
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
	Namespaces     []string                           `json:"namespaces"`
	BackupLocation string                             `json:"backupLocation"`
	Selectors      map[string]string                  `json:"selectors"`
	PreExecRule    string                             `json:"preExecRule"`
	PostExecRule   string                             `json:"postExecRule"`
	ReclaimPolicy  ApplicationBackupReclaimPolicyType `json:"reclaimPolicy"`
	// Options to be passed in to the driver
	Options map[string]string `json:"options"`
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
	Stage               ApplicationBackupStageType       `json:"stage"`
	Status              ApplicationBackupStatusType      `json:"status"`
	Reason              string                           `json:"reason"`
	Resources           []*ApplicationBackupResourceInfo `json:"resources"`
	Volumes             []*ApplicationBackupVolumeInfo   `json:"volumes"`
	BackupPath          string                           `json:"backupPath"`
	TriggerTimestamp    metav1.Time                      `json:"triggerTimestamp"`
	LastUpdateTimestamp metav1.Time                      `json:"lastUpdateTimestamp"`
	FinishTimestamp     metav1.Time                      `json:"finishTimestamp"`
	Size                uint64                           `json:"size"`
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
}

// ApplicationBackupVolumeInfo is the info for the backup of a volume
type ApplicationBackupVolumeInfo struct {
	PersistentVolumeClaim string                      `json:"persistentVolumeClaim"`
	Namespace             string                      `json:"namespace"`
	Volume                string                      `json:"volume"`
	BackupID              string                      `json:"backupID"`
	DriverName            string                      `json:"driverName"`
	Zones                 []string                    `json:"zones"`
	Status                ApplicationBackupStatusType `json:"status"`
	Reason                string                      `json:"reason"`
	Options               map[string]string           `jons:"options"`
	Size                  uint64                      `json:"size"`
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
