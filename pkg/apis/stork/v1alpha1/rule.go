package v1alpha1

import (
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	// RuleActionCommand is a command action
	RuleActionCommand RuleActionType = "command"
)

// RuleActionType is a type for actions that are supported in a stork rule
type RuleActionType string

// +genclient
// +genclient:noStatus
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// Rule denotes an object to declare a rule that performs actions on pods
type Rule struct {
	meta.TypeMeta   `json:",inline"`
	meta.ObjectMeta `json:"metadata,omitempty"`
	Rules           []RuleItem `json:"rules"`
}

// RuleItem represents one items in a stork rule spec
type RuleItem struct {
	// PodSelector is a map of key value pairs that are used to select the pods using their labels
	PodSelector map[string]string `json:"podSelector"`
	// Container Name of the container in which to run the rule if there are
	// multiple containers in the pod
	Container string `json:"container"`
	// Actions are actions to be performed on the pods selected using the selector
	Actions []RuleAction `json:"actions"`
}

// RuleAction represents an action in a stork rule item
type RuleAction struct {
	// Type is a type of the stork rule action
	Type RuleActionType `json:"type"`
	// Background indicates that the action needs to be performed in the background
	// +optional
	Background bool `json:"background,omitempty"`
	// RunInSinglePod indicates that the action needs to be performed in a single pod
	//                from the list of pods that match the selector
	// +optional
	RunInSinglePod bool `json:"runInSinglePod,omitempty"`
	// Value is the actual action value for e.g the command to run
	Value string `json:"value"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// RuleList is a list of stork rules
type RuleList struct {
	meta.TypeMeta `json:",inline"`
	meta.ListMeta `json:"metadata,omitempty"`

	Items []Rule `json:"items"`
}
