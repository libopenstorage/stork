package v1alpha1

import (
	storkapi "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	// ResourceBackupResourceName is name for the ResourceBackup resource.
	ResourceBackupResourceName = "resourcebackup"
	// ResourceBackupResourcePlural is the name for list of ResourceBackup resources.
	ResourceBackupResourcePlural = "resourcebackups"
)

// ResourceBackupType defines a method of achieving Resource transfer.
type ResourceBackupType string

// ResourceBackupStatus defines a status of ResourceBackup.
type ResourceBackupStatus string

const (
	// ResourceBackupStatusInitial is the initial status of ResourceBackup. It indicates
	// that a volume Backup request has been received.
	ResourceBackupStatusInitial ResourceBackupStatus = "Initial"
	// ResourceBackupStatusPending when Resource Backup is pending and not started yet.
	ResourceBackupStatusPending ResourceBackupStatus = "Pending"
	// ResourceBackupStatusInProgress when Resource is being transferred.
	ResourceBackupStatusInProgress ResourceBackupStatus = "InProgress"
	// ResourceBackupStatusFailed when Resource transfer is failed.
	ResourceBackupStatusFailed ResourceBackupStatus = "Failed"
	// ResourceBackupStatusSuccessful when Resource has been transferred.
	ResourceBackupStatusSuccessful ResourceBackupStatus = "Successful"
	// ResourceBackupStatusPartialSuccess when Resource was partially successful
	ResourceBackupStatusPartialSuccess ResourceBackupStatus = "PartialSuccess"
)

// ResourceBackupProgressStatus overall resource backup/restore progress
type ResourceBackupProgressStatus struct {
	// ProgressPercentage is the progress of the command in percentage
	ProgressPercentage float64
	// Status status of resource export
	Status ResourceBackupStatus `json:"status,omitempty"`
	// Reason status reason
	Reason string `json:"reason,omitempty"`
	// Resources status of each resource being restore
	Resources []*ResourceRestoreResourceInfo `json:"resources"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ResourceBackup defines a spec for holding restore of resource status updated by NFS executor job
type ResourceBackup struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              ResourceBackupSpec `json:"spec"`
	// Type - Backup or Restore
	Type ResourceBackupType `json:"type,omitempty"`
	// Status Overall status
	Status ResourceBackupProgressStatus `json:"status,omitempty"`
	// RestoreCompleteList - restore complete volumeInfo
	RestoreCompleteList []*storkapi.ApplicationRestoreVolumeInfo `json:"restoreCompleteList,omitempty"`
}

// ResourceBackupSpec configuration parameters for ResourceBackup
type ResourceBackupSpec struct {
	// ObjRef here is backuplocation CR
	ObjRef ResourceBackupObjectReference `json:"source,omitempty"`
	// PVC obj ref - During restore of vols store the ref of pvc
	PVCObjRef ResourceBackupObjectReference `json:"pvcobj,omitempty"`
}

// ResourceBackupObjectReference contains enough information to let you inspect the referred object.
type ResourceBackupObjectReference struct {
	// API version of the referent.
	APIVersion string `json:"apiVersion,omitempty"`
	// Kind of the referent.
	// More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds
	Kind string `json:"kind,omitempty"`
	// Namespace of the referent.
	// More info: https://kubernetes.io/docs/concepts/overview/working-with-objects/namespaces/
	Namespace string `json:"namespace,omitempty"`
	// Name of the referent.
	// More info: https://kubernetes.io/docs/concepts/overview/working-with-objects/names/#names
	Name string `json:"name,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ResourceBackupList is a list of ResourceBackup resources.
type ResourceBackupList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metaResource,omitempty"`

	Items []ResourceBackup `json:"items"`
}

// ResourceBackupVolumeInfo is the info for the backup of a volume
type ResourceBackupVolumeInfo struct {
	PersistentVolumeClaim    string               `json:"persistentVolumeClaim"`
	PersistentVolumeClaimUID string               `json:"persistentVolumeClaimUID"`
	Namespace                string               `json:"namespace"`
	Volume                   string               `json:"volume"`
	BackupID                 string               `json:"backupID"`
	DriverName               string               `json:"driverName"`
	Zones                    []string             `json:"zones"`
	Status                   ResourceBackupStatus `json:"status"`
	Reason                   string               `json:"reason"`
	Options                  map[string]string    `json:"options"`
	TotalSize                uint64               `json:"totalSize"`
	ActualSize               uint64               `json:"actualSize"`
	StorageClass             string               `json:"storageClass"`
	Provisioner              string               `json:"provisioner"`
	VolumeSnapshot           string               `json:"volumeSnapshot"`
}

// ResourceRestoreVolumeInfo is the info for the restore of a volume
type ResourceRestoreVolumeInfo struct {
	PersistentVolumeClaim    string               `json:"persistentVolumeClaim"`
	PersistentVolumeClaimUID string               `json:"persistentVolumeClaimUID"`
	SourceNamespace          string               `json:"sourceNamespace"`
	SourceVolume             string               `json:"sourceVolume"`
	RestoreVolume            string               `json:"restoreVolume"`
	DriverName               string               `json:"driverName"`
	Zones                    []string             `json:"zones"`
	Status                   ResourceBackupStatus `json:"status"`
	Reason                   string               `json:"reason"`
	TotalSize                uint64               `json:"totalSize"`
	Options                  map[string]string    `json:"options"`
}
