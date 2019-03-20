package v1alpha1

import (
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	// MigrationScheduleResourceName is name for "migrationschedule" resource
	MigrationScheduleResourceName = "migrationschedule"
	// MigrationScheduleResourcePlural is plural for "migrationschedule" resource
	MigrationScheduleResourcePlural = "migrationschedules"
)

// MigrationScheduleSpec is the spec used to schedule migrations
type MigrationScheduleSpec struct {
	Template           MigrationTemplateSpec `json:"template"`
	SchedulePolicyName string                `json:"schedulePolicyName"`
	Suspend            *bool                 `json:"suspend"`
}

// MigrationTemplateSpec describes the data a Migration should have when created
// from a template
type MigrationTemplateSpec struct {
	Spec MigrationSpec `json:"spec"`
}

// MigrationScheduleStatus is the status of a migration schedule
type MigrationScheduleStatus struct {
	Items map[SchedulePolicyType][]*ScheduledMigrationStatus `json:"items"`
}

// ScheduledMigrationStatus keeps track of the migration that was triggered by a
// scheduled policy
type ScheduledMigrationStatus struct {
	Name              string              `json:"name"`
	CreationTimestamp meta.Time           `json:"creationTimestamp"`
	FinishTimestamp   meta.Time           `json:"finishTimestamp"`
	Status            MigrationStatusType `json:"status"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// MigrationSchedule represents a scheduled migration object
type MigrationSchedule struct {
	meta.TypeMeta   `json:",inline"`
	meta.ObjectMeta `json:"metadata,omitempty"`
	Spec            MigrationScheduleSpec   `json:"spec"`
	Status          MigrationScheduleStatus `json:"status"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// MigrationScheduleList is a list of MigrationSchedules
type MigrationScheduleList struct {
	meta.TypeMeta `json:",inline"`
	meta.ListMeta `json:"metadata,omitempty"`

	Items []MigrationSchedule `json:"items"`
}
