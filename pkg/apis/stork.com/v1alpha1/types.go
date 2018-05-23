package v1alpha1

import (
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// StorkRuleActionType is a type for actions that are supported in a stork rule
type StorkRuleActionType string

const (
	// StorkRuleActionCommand is a command action
	StorkRuleActionCommand StorkRuleActionType = "command"
)

// +genclient
// +genclient:noStatus
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// StorkRule denotes an object to declare a rule that performs actions on pods
type StorkRule struct {
	meta.TypeMeta   `json:",inline"`
	meta.ObjectMeta `json:"metadata,omitempty"`
	Spec            []StorkRuleItem `json:"spec"`
}

// StorkRuleItem represents one items in a stork rule spec
type StorkRuleItem struct {
	// Selector is a map of key value pairs that are used to select the pods using their labels
	Selector map[string]string `json:"selector"`
	// Actions are actions to be performed on the pods selected using the selector
	Actions []StorkRuleAction `json:"actions"`
}

// StorkRuleAction represents an action in a stork rule item
type StorkRuleAction struct {
	// Type is a type of the stork rule action
	Type StorkRuleActionType `json:"type"`
	// Background indicates that the action needs to be performed in the background
	// +optional
	Background bool `json:"background,omitempty"`
	// RunOnSinglePod indicates that the action needs to be performed on a single pod
	//                from the list of pods that match the selector
	// +optional
	RunOnSinglePod bool `json:"runOnAllPods,omitempty"`
	// Value is the actual action value for e.g the command to run
	Value string `json:"value"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// StorkRuleList is a list of stork rules
type StorkRuleList struct {
	meta.TypeMeta `json:",inline"`
	meta.ListMeta `json:"metadata,omitempty"`

	Items []StorkRule `json:"items"`
}
