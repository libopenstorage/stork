package v1alpha1

import (
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	// ApplicationBackupScheduleResourceName is name for "applicationbackupschedule" resource
	ApplicationBackupScheduleResourceName = "applicationbackupschedule"
	// ApplicationBackupScheduleResourcePlural is plural for "applicationbackupschedule" resource
	ApplicationBackupScheduleResourcePlural = "applicationbackupschedules"
)

// ApplicationBackupScheduleSpec is the spec used to schedule applicationbackups
type ApplicationBackupScheduleSpec struct {
	Template           ApplicationBackupTemplateSpec `json:"template"`
	SchedulePolicyName string                        `json:"schedulePolicyName"`
	Suspend            *bool                         `json:"suspend"`
	ReclaimPolicy      ReclaimPolicyType             `json:"reclaimPolicy"`
}

// ApplicationBackupTemplateSpec describes the data a ApplicationBackup should have when created
// from a template
type ApplicationBackupTemplateSpec struct {
	Spec ApplicationBackupSpec `json:"spec"`
}

// ApplicationBackupScheduleStatus is the status of a applicationbackup schedule
type ApplicationBackupScheduleStatus struct {
	Items map[SchedulePolicyType][]*ScheduledApplicationBackupStatus `json:"items"`
}

// ScheduledApplicationBackupStatus keeps track of the applicationbackup that was triggered by a
// scheduled policy
type ScheduledApplicationBackupStatus struct {
	Name              string                      `json:"name"`
	CreationTimestamp meta.Time                   `json:"creationTimestamp"`
	FinishTimestamp   meta.Time                   `json:"finishTimestamp"`
	Status            ApplicationBackupStatusType `json:"status"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ApplicationBackupSchedule represents a scheduled applicationbackup object
type ApplicationBackupSchedule struct {
	meta.TypeMeta   `json:",inline"`
	meta.ObjectMeta `json:"metadata,omitempty"`
	Spec            ApplicationBackupScheduleSpec   `json:"spec"`
	Status          ApplicationBackupScheduleStatus `json:"status"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ApplicationBackupScheduleList is a list of ApplicationBackupSchedules
type ApplicationBackupScheduleList struct {
	meta.TypeMeta `json:",inline"`
	meta.ListMeta `json:"metadata,omitempty"`

	Items []ApplicationBackupSchedule `json:"items"`
}
