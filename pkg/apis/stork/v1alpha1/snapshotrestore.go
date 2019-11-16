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
	SourceName string `json:"sourceName"`
	// SourceNameSpace is snapshot namespace
	SourceNamespace string `json:"sourceNamespace"`
	// GroupSnapshot true if snapshot volumegroupsnapshot
	GroupSnapshot bool `json:"groupSnapshot"`
	// DestinationPVC list to restore snapshot
	DestinationPVC map[string]string `json:"pvcs,omitempty"`
}

// VolumeSnapshotRestoreStatusType is the status of volume in-place restore
type VolumeSnapshotRestoreStatusType string

const (
	// VolumeSnapshotRestoreStatusInitial is the initial state when snapshot restore is initiated
	VolumeSnapshotRestoreStatusInitial VolumeSnapshotRestoreStatusType = ""
	// VolumeSnapshotRestoreStatusPending for when restore is in pending state
	VolumeSnapshotRestoreStatusPending VolumeSnapshotRestoreStatusType = "Pending"
	// VolumeSnapshotRestoreStatusStaged for when restore has been staged locally
	VolumeSnapshotRestoreStatusStaged VolumeSnapshotRestoreStatusType = "Staged"
	// VolumeSnapshotRestoreStatusSuccessful for when restore is completed
	VolumeSnapshotRestoreStatusSuccessful VolumeSnapshotRestoreStatusType = "Successful"
	// VolumeSnapshotRestoreStatusInProgress for when restore is in progress
	VolumeSnapshotRestoreStatusInProgress VolumeSnapshotRestoreStatusType = "InProgress"
	// VolumeSnapshotRestoreStatusFailed for when restore failed
	VolumeSnapshotRestoreStatusFailed VolumeSnapshotRestoreStatusType = "Failed"
)

// VolumeSnapshotRestoreStatus of volume
type VolumeSnapshotRestoreStatus struct {
	// Status of volume restore
	Status VolumeSnapshotRestoreStatusType `json:"status"`
	// Volumes list of volume restore information
	Volumes []*RestoreVolumeInfo `json:"volumes"`
}

// RestoreVolumeInfo is the info for the restore of a volume
type RestoreVolumeInfo struct {
	Volume        string                          `json:"volume"`
	PVC           string                          `json:"pvc"`
	Namespace     string                          `json:"namespace"`
	Snapshot      string                          `json:"snapshot"`
	RestoreStatus VolumeSnapshotRestoreStatusType `json:"status"`
	Reason        string                          `json:"reason"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// VolumeSnapshotRestore crd spec for in-place restore of volume
type VolumeSnapshotRestore struct {
	meta.TypeMeta   `json:",inline"`
	meta.ObjectMeta `json:"metadata,omitempty"`
	Spec            VolumeSnapshotRestoreSpec   `json:"spec"`
	Status          VolumeSnapshotRestoreStatus `json:"status"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// VolumeSnapshotRestoreList is list of snapshot restores
type VolumeSnapshotRestoreList struct {
	meta.TypeMeta `json:",inline"`
	meta.ListMeta `json:"metadata,omitempty"`
	Items         []VolumeSnapshotRestore `json:"items"`
}
