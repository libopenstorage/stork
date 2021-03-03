package v1alpha1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

const (
	// ApplicationRegistrationResourceName singuler name of Application Registration CR
	ApplicationRegistrationResourceName = "applicationregistration"
	// ApplicationRegistrationResourcePlural plural name of Application Registration CR
	ApplicationRegistrationResourcePlural = "applicationregistrations"
)

// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ApplicationRegistration to collect crd resources
type ApplicationRegistration struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Resources         []ApplicationResource `json:"resources"`
}

// ApplicationResource definations to collect resource and fields
type ApplicationResource struct {
	// ResourceKind crd kind
	// ResourceKind string `json:"resourceKind"`
	// CRDName to register CR on destination clusters
	metav1.GroupVersionKind `json:",inline"`
	// KeepStatus if set to true collect status
	// while doing backup/migration/restore etc
	KeepStatus bool `json:"keepStatus"`
	// SuspendOptions to disable CRD upon migration/restore/clone
	SuspendOptions SuspendOptions `json:"suspendOptions"`
	// PodsPath to help activate/deactivate crd upon migration
	PodsPath string `json:"podsPath"`
}

// SuspendOptions to disable CRD upon migration/restore/clone
type SuspendOptions struct {
	Path  string `json:"path"`
	Type  string `json:"type"`
	Value string `json:"value"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ApplicationRegistrationList is a list of ApplicationRegistration
type ApplicationRegistrationList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []ApplicationRegistration `json:"items"`
}
