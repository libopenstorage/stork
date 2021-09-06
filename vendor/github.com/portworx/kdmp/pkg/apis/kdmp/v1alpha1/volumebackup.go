package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	// VolumeBackupResourceName is name for the VolumeBackup resource.
	VolumeBackupResourceName = "volumebackup"
	// VolumeBackupResourcePlural is the name for list of VolumeBackup resources.
	VolumeBackupResourcePlural = "volumebackups"
)

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// VolumeBackup is the connector between the data transfer job and kdmp controller.
type VolumeBackup struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              VolumeBackupSpec   `json:"spec"`
	Status            VolumeBackupStatus `json:"status,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// VolumeBackupList is a list of VolumeBackup resources.
type VolumeBackupList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []VolumeBackup `json:"items"`
}

// VolumeBackupSpec defines configuration parameters for VolumeBackup.
type VolumeBackupSpec struct {
	// Repository is a name of the restic repository.
	// This name will be used if backuplocation repository is set too.
	Repository string `json:"repository,omitempty"`
	// BackupLocation contains a reference (name and namespace) to the object.
	BackupLocation DataExportObjectReference `json:"backupLocation,omitempty"`
}

// VolumeBackupStatus defines status for VolumeBackup.
type VolumeBackupStatus struct {
	// ProgressPercentage represents a backup progress.
	ProgressPercentage float64 `json:"progressPercentage,omitempty"`
	// TotalBytes is a backup size.
	TotalBytes uint64 `json:"totalBytes,omitempty"`
	// TotalBytesProcessed  represents a backup progress.
	TotalBytesProcessed uint64 `json:"totalBytesProcessed,omitempty"`
	// SnapshotID is a backup snapshot id.
	SnapshotID string `json:"snapshotID,omitempty"`
	// LastKnownError contains an error in case of failure.
	LastKnownError string `json:"lastKnownError,omitempty"`
}
