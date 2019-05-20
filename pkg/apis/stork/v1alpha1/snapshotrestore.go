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
	// VolumeSnapshotRestoreStatusReady for when restore is done
	VolumeSnapshotRestoreStatusReady SnapshotRestoreStatusType = "Ready"
	// VolumeSnapshotRestoreStatusError for when restore failed
	VolumeSnapshotRestoreStatusError SnapshotRestoreStatusType = "Error"
)

// VolumeSnapshotRestoreStageType is stage of volume in-place restore
type VolumeSnapshotRestoreStageType string

const (
	// VolumeSnapshotRestoreStageInitial is the initial stage when snapshot restore is initiated
	VolumeSnapshotRestoreStageInitial VolumeSnapshotRestoreStageType = ""
	// VolumeSnapshotRestoreStageRestore for when restore is in restore stage
	VolumeSnapshotRestoreStageRestore VolumeSnapshotRestoreStageType = "Restore"
	// VolumeSnapshotRestoreStageReady for when restore is completed
	VolumeSnapshotRestoreStageReady VolumeSnapshotRestoreStageType = "Ready"
	// VolumeSnapshotRestoreStageError for when restore failed
	VolumeSnapshotRestoreStageError VolumeSnapshotRestoreStageType = "Error"
)

// RestoreStatus of volume
type RestoreStatus struct {
	// Stage of volume snapshot restore
	Stage VolumeSnapshotRestoreStageType `json:"stage"`
	// Status of volume restore
	Status SnapshotRestoreStatusType `json:"status"`
	// Volumes list of restore inforamtion
	Volumes []*RestoreVolumeInfo `json:"volumes"`
}

// RestoreVolumeInfo is the info for the restore of a volume
type RestoreVolumeInfo struct {
	Volume        string                    `json:"volume"`
	PVC           string                    `json:"pvc`
	RestoreStatus SnapshotRestoreStatusType `json:"status"`
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
