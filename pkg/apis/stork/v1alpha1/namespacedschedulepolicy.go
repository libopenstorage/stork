package v1alpha1

import (
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	// NamespacedSchedulePolicyResourceName is name for "namespacedschedulepolicy" resource
	NamespacedSchedulePolicyResourceName = "namespacedschedulepolicy"
	// NamespacedSchedulePolicyResourcePlural is plural for "namespacedschedulepolicy" resource
	NamespacedSchedulePolicyResourcePlural = "namespacedschedulepolicies"
)

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// NamespacedSchedulePolicy represents a policy for executing actions on a schedule
type NamespacedSchedulePolicy struct {
	*SchedulePolicy `json:",inline"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// NamespacedSchedulePolicyList is a list of namespaced schedule policies
type NamespacedSchedulePolicyList struct {
	meta.TypeMeta `json:",inline"`
	meta.ListMeta `json:"metadata,omitempty"`

	Items []SchedulePolicy `json:"items"`
}
