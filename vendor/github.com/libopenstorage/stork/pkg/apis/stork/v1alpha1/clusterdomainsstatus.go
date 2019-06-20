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

// ClusterDomainState defines the state of the cluster domain
type ClusterDomainState string

const (
	// ClusterDomainActive indicates that the cluster domain is active
	ClusterDomainActive ClusterDomainState = "Active"

	// ClusterDomainInactive indicates that the cluster domain is inactive
	ClusterDomainInactive ClusterDomainState = "Inactive"
)

// ClusterDomainSyncStatus defines the current sync progress status of a cluster domain
type ClusterDomainSyncStatus string

const (
	// ClusterDomainSyncStatusInSync indicates the cluster domain is in sync
	ClusterDomainSyncStatusInSync ClusterDomainSyncStatus = "InSync"
	// ClusterDomainSyncStatusInProgress indicates the cluster domain sync is in progress
	ClusterDomainSyncStatusInProgress ClusterDomainSyncStatus = "SyncInProgress"
	// ClusterDomainSyncStatusNotInSync indicates the cluster domain is not in sync
	ClusterDomainSyncStatusNotInSync ClusterDomainSyncStatus = "NotInSync"
	// ClusterDomainSyncStatusUnknown indicates the cluster domain sync status is currently not known
	ClusterDomainSyncStatusUnknown ClusterDomainSyncStatus = "SyncStatusUnknown"
)

// ClusterDomains provides a list of activated cluster domains and a list
// of inactive cluster domains
type ClusterDomains struct {
	LocalDomain        string              `json:"localDomain"`
	ClusterDomainInfos []ClusterDomainInfo `json:"clusterDomainInfos"`
}

// ClusterDomainInfo provides more information about a cluster domain
type ClusterDomainInfo struct {
	Name       string                  `json:"name"`
	State      ClusterDomainState      `json:"state"`
	SyncStatus ClusterDomainSyncStatus `json:"syncStatus"`
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
