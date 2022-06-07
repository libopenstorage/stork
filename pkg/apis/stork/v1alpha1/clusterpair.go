package v1alpha1

import (
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/clientcmd/api"
)

const (
	// ClusterPairResourceName is name for "clusterpair" resource
	ClusterPairResourceName = "clusterpair"
	// ClusterPairResourcePlural is plural for "clusterpair" resource
	ClusterPairResourcePlural = "clusterpairs"
)

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ClusterPair represents pairing with other clusters
type ClusterPair struct {
	meta.TypeMeta   `json:",inline"`
	meta.ObjectMeta `json:"metadata,omitempty"`
	Spec            ClusterPairSpec   `json:"spec"`
	Status          ClusterPairStatus `json:"status,omitempty"`
}

// ClusterPairSpec is the spec to create the cluster pair
type ClusterPairSpec struct {
	Config  api.Config        `json:"config"`
	Options map[string]string `json:"options",yaml:"options"`
	// PlatformOptions are kubernetes platform provider related
	// options.
	PlatformOptions PlatformSpec `json:"platformOptions",yaml:"platformOptions"`
}

// ClusterPairStatusType is the status of the pair
type ClusterPairStatusType string

const (
	// ClusterPairStatusInitial is the initial state when pairing is created
	ClusterPairStatusInitial ClusterPairStatusType = ""
	// ClusterPairStatusPending for when pairing is still pending
	ClusterPairStatusPending ClusterPairStatusType = "Pending"
	// ClusterPairStatusReady for when pair is ready
	ClusterPairStatusReady ClusterPairStatusType = "Ready"
	// ClusterPairStatusError for when pairing is in error state
	ClusterPairStatusError ClusterPairStatusType = "Error"
	// ClusterPairStatusDegraded for when pairing is degraded
	ClusterPairStatusDegraded ClusterPairStatusType = "Degraded"
	// ClusterPairStatusDeleting for when pairing is being deleted
	ClusterPairStatusDeleting ClusterPairStatusType = "Deleting"
	// ClusterPairStatusNotProvided for when pairing information is not
	// provided
	ClusterPairStatusNotProvided ClusterPairStatusType = "NotProvided"
)

// ClusterPairStatus is the status of the cluster pair
type ClusterPairStatus struct {
	// Status of the pairing with the scheduler
	// +optional
	SchedulerStatus ClusterPairStatusType `json:"schedulerStatus"`
	// Status of pairing with the storage driver
	// +optional
	StorageStatus ClusterPairStatusType `json:"storageStatus"`
	// ID of the remote storage which is paired
	// +optional
	RemoteStorageID string `json:"remoteStorageId"`
}

// RancherSecret holds the reference to the api keys used to interact
// with a rancher cluster
type RancherSecret struct {
	// APIKeySecretName is the name of the kubernetes secret
	// that hosts the API key
	APIKeySecretName string `json:"apiKeySecretName"`
	// APIKeySecretNamespace is the namespace of the kubernetes secret
	// that hosts the API key
	APIKeySecretNamespace string `json:"apiKeySecretNamespace"`
}

// RancherSpec provides options for interacting with Rancher
type RancherSpec struct {
	// ProjectMappings allows a cluster pair to migrate namespaces between different
	// Rancher projects. The key in the map is the source project while the value
	// is the destination project. Specify this only if you have not provided
	// API key to stork for creating the project on the target cluster.
	ProjectMappings map[string]string `json:"projectMappings,omitempty"`

	// --- FUTURE Rancher Specs ---
	// SourceRancherSecret is the rancher secrets for source cluster
	// SourceRancherSecret RancherSecret `json:"sourceRancherSecret"`
	// DestinationRancherSecret is the rancher secrets for source cluster
	// DestinationRancherSecret RancherSecret `json:"destRancherSecret"`
	// SourceURL is the rancher source URL endpoint
	// SourceURL string `json: sourceURL`
	// Destination is the destination URL endpoint
	// Destination string `json: destinationURL`
}

// PlatformSpec provide options for interacting with kubernetes platform
// provider like: EKS / AKS / GKE / Rancher / Openshift
type PlatformSpec struct {
	// Rancher configuration
	Rancher *RancherSpec `json:"rancher,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ClusterPairList is a list of cluster pairs
type ClusterPairList struct {
	meta.TypeMeta `json:",inline"`
	meta.ListMeta `json:"metadata,omitempty"`

	Items []ClusterPair `json:"items"`
}
