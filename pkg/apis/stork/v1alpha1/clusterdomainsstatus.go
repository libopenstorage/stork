package v1alpha1

import (
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	// ClusterDomainsStatusResourceName is name for "clusterdomainsstatus" resource
	ClusterDomainsStatusResourceName = "clusterdomainsstatus"
	// ClusterDomainsStatusPlural is plural for "clusterdomainsstatus" resource
	ClusterDomainsStatusPlural = "clusterdomainsstatuses"
	// ClusterDomainsStatusShortName is the shortname for "clusterdomainsstatus" resource
	ClusterDomainsStatusShortName = "cds"
)

// ClusterDomains provides a list of activated cluster domains and a list
// of inactive cluster domains
type ClusterDomains struct {
	Active   []string `json:"active"`
	Inactive []string `json:"inactive"`
}

// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ClusterDomainsStatus represents the status of all cluster domains
type ClusterDomainsStatus struct {
	meta.TypeMeta   `json:",inline"`
	meta.ObjectMeta `json:"metadata,omitempty"`
	Status          ClusterDomains `json:"status"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ClusterDomainsStatusList is a list of statuses for cluster domains
type ClusterDomainsStatusList struct {
	meta.TypeMeta `json:",inline"`
	meta.ListMeta `json:"metadata,omitempty"`
	Items         []ClusterDomainsStatus `json:"items"`
}
