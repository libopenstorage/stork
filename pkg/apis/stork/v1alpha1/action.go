package v1alpha1

import (
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	// ActionResourceName is name for "Action" resource
	ActionResourceName = "action"
	// ActionResourcePlural is plural for "Action" resource
	ActionResourcePlural = "actions"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ActionList is a list of Actions
type ActionList struct {
	meta.TypeMeta `json:",inline"`
	meta.ListMeta `json:"metadata,omitempty"`
	Items         []Action `json:"items"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// Action represents a task that will be performed once
// It is similar to k8s Job
type Action struct {
	meta.TypeMeta   `json:",inline"`
	meta.ObjectMeta `json:"metadata,omitempty"`
	Spec            ActionSpec   `json:"spec"`
	Status          ActionStatus `json:"status"`
}

// ActionSpec specifies the type of Action
type ActionSpec struct {
	ActionType ActionType `json:"actionType"`
}

// ActionType lists the various actions that can be performed
type ActionType string

const (
	// to start apps on destination cluster
	ActionTypeFailover ActionType = "failover"
)

// ActionStatus is the current status of the Action
type ActionStatus string

const (
	// ActionStatusScheduled means Action is yet to start
	ActionStatusScheduled ActionStatus = "Scheduled"
	// ActionStatusInProgress means Action is in progress
	ActionStatusInProgress ActionStatus = "In-Progress"
	// ActionStatusFailed means that Action has failed
	ActionStatusFailed ActionStatus = "Failed"
	// ActionStatusSuccessful means Action has completed successfully
	ActionStatusSuccessful ActionStatus = "Successful"
)
