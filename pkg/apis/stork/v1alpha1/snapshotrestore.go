package v1alpha1

import (
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	// SnapshotRestoreResourceName is name for "volumesnapshotrestore" resource
	SnapshotRestoreResourceName = "volumesnapshotrestore"
	// SnapshotRestoreResourcePlural is plural for "volumesnapshotrestore" resource
	SnapshotRestoreResourcePlural = "volumesnapshotrestores"
)

// VolumeSnapshotRestoreSpec for in-place volume restore
type VolumeSnapshotRestoreSpec struct {
	// SourceName of snapshot
	SourceName string `json:"sourcename"`
	// SourceNameSpace is snapshot namespace
	SourceNamespace string `json:"sourcenamespace"`
	// GroupSnapshot true if snapshot volumegroupsnapshot
	GroupSnapshot bool `json:"groupsnapshot"`
	// DestinationPVC list to restore snapshot
	DestinationPVC map[string]string `json:"pvcs,omitempty"`
}

// SnapshotRestoreStatusType is the status of volume in-place restore
type SnapshotRestoreStatusType string

const (
	// VolumeSnapshotRestoreStatusInitial is the initial state when snapshot restore is initiated
	VolumeSnapshotRestoreStatusInitial SnapshotRestoreStatusType = ""
	// VolumeSnapshotRestoreStatusPending for when restore is in pending state
	VolumeSnapshotRestoreStatusPending SnapshotRestoreStatusType = "Pending"
	// VolumeSnapshotRestoreStatusRestore for when restore is in restore state
	VolumeSnapshotRestoreStatusRestore SnapshotRestoreStatusType = "Restore"
	// VolumeSnapshotRestoreStatusSuccessful for when restore is completed
	VolumeSnapshotRestoreStatusSuccessful SnapshotRestoreStatusType = "Successful"
	// VolumeSnapshotRestoreStatusInProgress for when restore is in progress
	VolumeSnapshotRestoreStatusInProgress SnapshotRestoreStatusType = "InProgress"
	// VolumeSnapshotRestoreStatusFailed for when restore failed
	VolumeSnapshotRestoreStatusFailed SnapshotRestoreStatusType = "Failed"
)

// RestoreStatus of volume
type RestoreStatus struct {
	// Status of volume restore
	Status SnapshotRestoreStatusType `json:"status"`
	// Volumes list of restore inforamtion
	Volumes []*RestoreVolumeInfo `json:"volumes"`
}

// RestoreVolumeInfo is the info for the restore of a volume
type RestoreVolumeInfo struct {
	Volume        string                    `json:"volume"`
	PVC           string                    `json:"pvc"`
	RestoreStatus SnapshotRestoreStatusType `json:"status"`
	Reason        string                    `json:"reason"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// VolumeSnapshotRestore crd spec for in-place restore of volume
type VolumeSnapshotRestore struct {
	meta.TypeMeta   `json:",inline"`
	meta.ObjectMeta `json:"metadata,omitempty"`
	Spec            VolumeSnapshotRestoreSpec `json:"spec"`
	Status          RestoreStatus             `json:"status"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// VolumeSnapshotRestoreList is list of snapshot restores
type VolumeSnapshotRestoreList struct {
	meta.TypeMeta `json:",inline"`
	meta.ListMeta `json:"metadata,omitempty"`
	Items         []VolumeSnapshotRestore `json:"items"`
}
