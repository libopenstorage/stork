package v1alpha1

import (
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// SnapshotRestoretatusType is the status of volume in-place restore
type SnapshotRestoreStatusType string

const (
	// SnapshotResourceName is name for "snapshotrestore" resource
	// change to volumesnapshotrestore
	SnapshotRestoreResourceName   = "volumesnaprestore"
	SnapshotRestoreResourcePlural = "volumesnaprestores"
	// SnapshotRestoreStatusInitial is the initial state when snapshot restore is initiated
	SnapshotRestoreStatusInitial SnapshotRestoreStatusType = ""
	// SnapshotRestoreStatusPending for when restore is in pending state
	SnapshotRestoreStatusPending SnapshotRestoreStatusType = "Pending"
	// SnapshotRestoreStatusReady for when restore is done
	SnapshotRestoreStatusReady SnapshotRestoreStatusType = "Done"
	// SnapshotRestoreStatusError for when restore failed
	SnapshotRestoreStatusError SnapshotRestoreStatusType = "Error"
)

type RestoreSpec struct {
	// source, type & name
	SourceName      string   `json:"sourcename"`
	SourceType      string   `json:"sourcetype"`
	SourceNamespace string   `json:"sourcenamespace"`
	DestinationPVC  []string `json:"pvcs"`
}

type RestoreStatus struct {
	Status SnapshotRestoreStatusType `json:"status"`
	// change it specific to restore
	Volumes []*RestoreVolumeInfo `json:"volumes"`
}

// RestoreVolumeInfo is the info for the restore of a volume
type RestoreVolumeInfo struct {
	Volume        string                    `json:"volume"`
	RestoreStatus SnapshotRestoreStatusType `json:"status"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// VolumeSnapshotRestore
// groupvolumerestore status of each  pvc restore
type SnapshotRestore struct {
	meta.TypeMeta   `json:",inline"`
	meta.ObjectMeta `json:"metadata,omitempty"`
	Spec            RestoreSpec   `json:"spec"`
	Status          RestoreStatus `json:"status"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// SnapshotRestoreList is list of snapshot restores
type SnapshotRestoreList struct {
	meta.TypeMeta `json:",inline"`
	meta.ListMeta `json:"metadata,omitempty"`
	Items         []SnapshotRestore `json:"items"`
}
