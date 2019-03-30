package v1alpha1

import (
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	// ClusterDomainUpdateResourceName is name for "clusterdomainupdate" resource
	ClusterDomainUpdateResourceName = "clusterdomainupdate"
	// ClusterDomainUpdatePlural is plural for "clusterdomainupdate" resource
	ClusterDomainUpdatePlural = "clusterdomainupdates"
	// ClusterDomainUpdateShortName is the short name for clusterdomainupdate
	ClusterDomainUpdateShortName = "cdu"
)

// ClusterDomainUpdateSpec is the spec used to update a cluster domain
type ClusterDomainUpdateSpec struct {
	ClusterDomain string `json:"clusterdomain"`
	Active        bool   `json:"active"`
}

// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ClusterDomainUpdate indicates the update need to be done on a ClusterDomain
type ClusterDomainUpdate struct {
	meta.TypeMeta   `json:",inline"`
	meta.ObjectMeta `json:"metadata,omitempty"`
	Spec            ClusterDomainUpdateSpec   `json:"spec"`
	Status          ClusterDomainUpdateStatus `json:"status"`
}

// ClusterDomainUpdateStatus indicates the status of ClusterDomainUpdate resource
type ClusterDomainUpdateStatus struct {
	Status ClusterDomainUpdateStatusType `json:"status"`
	Reason string                        `json:"reason"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ClusterDomainUpdateList is a list of statuses for cluster domains
type ClusterDomainUpdateList struct {
	meta.TypeMeta `json:",inline"`
	meta.ListMeta `json:"metadata,omitempty"`
	Items         []ClusterDomainUpdate `json:"items"`
}

// ClusterDomainUpdateStatusType is the status of cluster domain update operation
type ClusterDomainUpdateStatusType string

const (
	// ClusterDomainUpdateStatusInitial is the initial state when clusterdomainsupdate is created
	ClusterDomainUpdateStatusInitial ClusterDomainUpdateStatusType = ""
	// ClusterDomainUpdateStatusPending is state when clusterdomainsupdate is still pending
	ClusterDomainUpdateStatusPending ClusterDomainUpdateStatusType = "Pending"
	// ClusterDomainUpdateStatusFailed is state when clusterdomainsupdate has failed
	ClusterDomainUpdateStatusFailed ClusterDomainUpdateStatusType = "Failed"
	// ClusterDomainUpdateStatusSuccessful is state when clusterdomainsupdate has completed successfully
	ClusterDomainUpdateStatusSuccessful ClusterDomainUpdateStatusType = "Successful"
)
