package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	// VolumeBackupDeleteResourceName is name for the VolumeBackupDelete resource.
	VolumeBackupDeleteResourceName = "volumebackupdelete"
	// VolumeBackupDeleteResourcePlural is the name for list of VolumeBackupDelete resources.
	VolumeBackupDeleteResourcePlural = "volumebackupdeletes"
)

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// VolumeBackupDelete is the CR for storing the volume delete job status
type VolumeBackupDelete struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              VolumeBackupDeleteSpec   `json:"spec"`
	Status            VolumeBackupDeleteStatus `json:"status,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// VolumeBackupDeleteList is a list of VolumeBackupDelete resources.
type VolumeBackupDeleteList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []VolumeBackupDelete `json:"items"`
}

// VolumeBackupDeleteSpec defines configuration parameters for VolumeBackupDelete.
type VolumeBackupDeleteSpec struct {
	PvcName    string `json:"pvcName,omitempty"`
	SnapshotID string `json:"snapshotID,omitempty"`
}

// VolumeBackupDeleteStatusType is the status of the volume delete job
type VolumeBackupDeleteStatusType string

const (
	// VolumeBackupDeleteStatusInitial - initial status
	VolumeBackupDeleteStatusInitial VolumeBackupDeleteStatusType = "Initial"
	// VolumeBackupDeleteStatusProgress - progress status
	VolumeBackupDeleteStatusProgress VolumeBackupDeleteStatusType = "Progress"
	// VolumeBackupDeleteStatusSuccess - Success status
	VolumeBackupDeleteStatusSuccess VolumeBackupDeleteStatusType = "Successful"
	// VolumeBackupDeleteStatusFailed - Fail status
	VolumeBackupDeleteStatusFailed VolumeBackupDeleteStatusType = "Failed"
)

// VolumeBackupDeleteStatus defines status for VolumeBackupDelete.
type VolumeBackupDeleteStatus struct {
	Status VolumeBackupDeleteStatusType `json:"status,omitempty"`
	Reason string                       `json:"reason,omitempty"`
}
