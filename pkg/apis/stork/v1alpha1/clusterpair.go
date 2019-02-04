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
	Options map[string]string `json:"options"`
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

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ClusterPairList is a list of cluster pairs
type ClusterPairList struct {
	meta.TypeMeta `json:",inline"`
	meta.ListMeta `json:"metadata,omitempty"`

	Items []ClusterPair `json:"items"`
}
