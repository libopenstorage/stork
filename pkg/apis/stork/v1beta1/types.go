package v1beta1

import meta "k8s.io/apimachinery/pkg/apis/meta/v1"

// +genclient
// +genclient:noStatus
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// GroupVolumeSnapshot represents a group snapshot
type GroupVolumeSnapshot struct {
	meta.TypeMeta   `json:",inline"`
	meta.ObjectMeta `json:"metadata,omitempty"`
	Spec            GroupVolumeSnapshotSpec `json:"spec"`
}

// GroupVolumeSnapshotSpec represents the spec for a group snapshot
type GroupVolumeSnapshotSpec struct {
	PreSnapshotRule  string            `json:"preSnapshotRule"`
	PostSnapshotRule string            `json:"postSnapshotRule"`
	PVCSelector      PVCSelectorSpec   `json:"pvcSelector"`
	Options          map[string]string `json:"options"`
}

// PVCSelectorSpec is the spec to select the PVCs for group snapshot
type PVCSelectorSpec struct {
	meta.LabelSelector
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// GroupVolumeSnapshotList is a list of group volume snapshots
type GroupVolumeSnapshotList struct {
	meta.TypeMeta `json:",inline"`
	meta.ListMeta `json:"metadata,omitempty"`

	Items []GroupVolumeSnapshot `json:"items"`
}
