package v1alpha1

import (
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	// NamespacedActionResourceName is name for "NamespacedAction" resource
	NamespacedActionResourceName = "namespacedaction"
	// NamespacedActionResourcePlural is plural for "NamespacedAction" resource
	NamespacedActionResourcePlural = "namespacedactions"
	// NamespacedActionShortName is the short name for NamespacedAction
	NamespacedActionShortName = "nsa"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// NamespacedActionList is a list of NamespacedActions
type NamespacedActionList struct {
	meta.TypeMeta `json:",inline"`
	meta.ListMeta `json:"metadata,omitempty"`
	Items         []NamespacedAction `json:"items"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// NamespacedAction is a resource with which user can trigger various actions
type NamespacedAction struct {
	meta.TypeMeta   `json:",inline"`
	meta.ObjectMeta `json:"metadata,omitempty"`
	Spec            NamespacedActionSpec   `json:"spec"`
	Status          NamespacedActionStatus `json:"status"`
}

// NamespacedActionSpec is the action that the user wants to trigger, or nil if no action
type NamespacedActionSpec struct {
	Action NamespacedActionType `json:"action"`
}

// NamespacedActionType is the type of the NamespacedAction to perform
type NamespacedActionType string

const (
	NamespacedActionNil      NamespacedActionType = "nil"
	NamespacedActionFailover NamespacedActionType = "failover"
	NamespacedActionFailback NamespacedActionType = "failback"
)

// NamespacedActionStatus is the result of the past triggered actions
type NamespacedActionStatus struct {
	Items []*NamespacedActionStatusItem `json:"items"`
}

// NamespacedActionStatusItem keeps track of the actions, their timestamp and their result
type NamespacedActionStatusItem struct {
	Action    NamespacedActionType       `json:"action"`
	Timestamp meta.Time                  `json:"timestamp"`
	Status    NamespacedActionStatusType `json:"status"`
}

// NamespacedActionStatusType is the result of the action after it has completed
type NamespacedActionStatusType string

const (
	// NamespacedActionStatusFailed for when action has failed
	NamespacedActionStatusFailed NamespacedActionStatusType = "Failed"
	// NamespacedActionStatusSuccessful for when action has completed successfully
	NamespacedActionStatusSuccessful NamespacedActionStatusType = "Successful"
)
