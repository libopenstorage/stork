package v1alpha1

import (
	crdv1 "github.com/kubernetes-incubator/external-storage/snapshot/pkg/apis/crd/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	// GroupVolumeSnapshotResourceName is name for "groupvolumesnapshot" resource
	GroupVolumeSnapshotResourceName = "groupvolumesnapshot"
	// GroupVolumeSnapshotResourcePlural is plural for the "groupvolumesnapshot" resource
	GroupVolumeSnapshotResourcePlural = "groupvolumesnapshots"
)

// +genclient
// +genclient:noStatus
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// GroupVolumeSnapshot represents a group snapshot
type GroupVolumeSnapshot struct {
	meta.TypeMeta   `json:",inline"`
	meta.ObjectMeta `json:"metadata,omitempty"`
	Spec            GroupVolumeSnapshotSpec   `json:"spec"`
	Status          GroupVolumeSnapshotStatus `json:"status"`
}

// GroupVolumeSnapshotSpec represents the spec for a group snapshot
type GroupVolumeSnapshotSpec struct {
	// PreExecRule is the name of rule applied before taking the snapshot. The rule needs to be
	// in the same namespace as the group volumesnapshot
	PreExecRule string `json:"preExecRule"`
	// PreExecRule is the name of rule applied after taking the snapshot. The rule needs to be
	// in the same namespace as the group volumesnapshot
	PostExecRule string `json:"postExecRule"`
	// PVCSelector selects the PVCs that are part of the group snapshot
	PVCSelector PVCSelectorSpec `json:"pvcSelector"`
	// RestoreNamespaces is a list of namespaces to which the snapshots can be restored to
	RestoreNamespaces []string `json:"restoreNamespaces"`
	// MaxRetries is the number of times to retry the groupvolumesnapshot on failure. default: 0
	MaxRetries int `json:"maxRetries"`
	// Options are pass-through parameters that are passed to the driver handling the group snapshot
	Options map[string]string `json:"options"`
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

// GroupVolumeSnapshotStatus is status for the group snapshot
type GroupVolumeSnapshotStatus struct {
	Stage           GroupVolumeSnapshotStageType  `json:"stage"`
	Status          GroupVolumeSnapshotStatusType `json:"status"`
	NumRetries      int                           `json:"numRetries"`
	VolumeSnapshots []*VolumeSnapshotStatus       `json:"volumeSnapshots"`
}

// VolumeSnapshotStatus captures the status of a volume snapshot operation
type VolumeSnapshotStatus struct {
	VolumeSnapshotName string
	TaskID             string
	ParentVolumeID     string
	DataSource         *crdv1.VolumeSnapshotDataSource
	Conditions         []crdv1.VolumeSnapshotCondition
}

// GroupVolumeSnapshotStatusType is types of statuses of a group snapshot operation
type GroupVolumeSnapshotStatusType string

const (
	// GroupSnapshotInitial is when the group snapshot is created and no action has yet been performed
	GroupSnapshotInitial GroupVolumeSnapshotStatusType = ""
	// GroupSnapshotPending is when the group snapshot is in pending state waiting for another event
	GroupSnapshotPending GroupVolumeSnapshotStatusType = "Pending"
	// GroupSnapshotInProgress is when the group snapshot is in progress
	GroupSnapshotInProgress GroupVolumeSnapshotStatusType = "InProgress"
	// GroupSnapshotFailed is when the group snapshot has failed
	GroupSnapshotFailed GroupVolumeSnapshotStatusType = "Failed"
	// GroupSnapshotSuccessful is when the group snapshot has succeeded
	GroupSnapshotSuccessful GroupVolumeSnapshotStatusType = "Successful"
)

// GroupVolumeSnapshotStageType is the stage of the group snapshot
type GroupVolumeSnapshotStageType string

const (
	// GroupSnapshotStageInitial is when the group snapshot is just created
	GroupSnapshotStageInitial GroupVolumeSnapshotStageType = ""
	// GroupSnapshotStagePreChecks is when the group snapshot is going through prechecks
	GroupSnapshotStagePreChecks GroupVolumeSnapshotStageType = "PreChecks"
	// GroupSnapshotStagePreSnapshot is when the pre-snapshot rule is executing for the group snapshot
	GroupSnapshotStagePreSnapshot GroupVolumeSnapshotStageType = "PreSnapshot"
	// GroupSnapshotStageSnapshot is when the snapshots are being taken for the group snapshot
	GroupSnapshotStageSnapshot GroupVolumeSnapshotStageType = "Snapshot"
	// GroupSnapshotStagePostSnapshot is when the post-snapshot rule is executing for the group snapshot
	GroupSnapshotStagePostSnapshot GroupVolumeSnapshotStageType = "PostSnapshot"
	// GroupSnapshotStageFinal is when all stages are done for the group snapshot
	GroupSnapshotStageFinal GroupVolumeSnapshotStageType = "Final"
)
