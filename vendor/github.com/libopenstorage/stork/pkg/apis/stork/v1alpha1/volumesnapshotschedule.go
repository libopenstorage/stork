package v1alpha1

import (
	snapv1 "github.com/kubernetes-incubator/external-storage/snapshot/pkg/apis/crd/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	// VolumeSnapshotScheduleResourceName is name for "volumesnapshotschedule" resource
	VolumeSnapshotScheduleResourceName = "volumesnapshotschedule"
	// VolumeSnapshotScheduleResourcePlural is plural for "volumesnapshotschedule" resource
	VolumeSnapshotScheduleResourcePlural = "volumesnapshotschedules"
)

// VolumeSnapshotScheduleSpec is the spec used to schedule volumesnapshots
type VolumeSnapshotScheduleSpec struct {
	Template           VolumeSnapshotTemplateSpec `json:"template"`
	SchedulePolicyName string                     `json:"schedulePolicyName"`
	Suspend            *bool                      `json:"suspend"`
	ReclaimPolicy      ReclaimPolicyType          `json:"reclaimPolicy"`
	PreExecRule        string                     `json:"preExecRule"`
	PostExecRule       string                     `json:"postExecRule"`
}

// VolumeSnapshotTemplateSpec describes the data a VolumeSnapshot should have when created
// from a template
type VolumeSnapshotTemplateSpec struct {
	Spec snapv1.VolumeSnapshotSpec `json:"spec"`
}

// ReclaimPolicyType is the type of reclaim policy
type ReclaimPolicyType string

const (
	// ReclaimPolicyInvalid is an invalid schedule policy
	ReclaimPolicyInvalid ReclaimPolicyType = "Invalid"
	// ReclaimPolicyDelete is to specify that an object should be deleted
	ReclaimPolicyDelete ReclaimPolicyType = "Delete"
	// ReclaimPolicyRetain is to specify that an object should be retained
	ReclaimPolicyRetain ReclaimPolicyType = "Retain"
)

// VolumeSnapshotScheduleStatus is the status of a volumesnapshot schedule
type VolumeSnapshotScheduleStatus struct {
	Items map[SchedulePolicyType][]*ScheduledVolumeSnapshotStatus `json:"items"`
}

// ScheduledVolumeSnapshotStatus keeps track of the volumesnapshot that was triggered by a
// scheduled policy
type ScheduledVolumeSnapshotStatus struct {
	Name              string                             `json:"name"`
	CreationTimestamp meta.Time                          `json:"creationTimestamp"`
	FinishTimestamp   meta.Time                          `json:"finishTimestamp"`
	Status            snapv1.VolumeSnapshotConditionType `json:"status"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// VolumeSnapshotSchedule represents a scheduled volumesnapshot object
type VolumeSnapshotSchedule struct {
	meta.TypeMeta   `json:",inline"`
	meta.ObjectMeta `json:"metadata,omitempty"`
	Spec            VolumeSnapshotScheduleSpec   `json:"spec"`
	Status          VolumeSnapshotScheduleStatus `json:"status"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// VolumeSnapshotScheduleList is a list of VolumeSnapshotSchedules
type VolumeSnapshotScheduleList struct {
	meta.TypeMeta `json:",inline"`
	meta.ListMeta `json:"metadata,omitempty"`

	Items []VolumeSnapshotSchedule `json:"items"`
}
