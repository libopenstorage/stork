package v1

import (
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// PortworxDiagSpec is the spec used to define a portworx diag.
type PortworxDiagSpec struct {
	// Configuration for diags collection of the main Portworx component.
	Portworx *PortworxComponent `json:"portworx,omitempty"`
}

// Specifies the diags collection configuration for the Portworx component.
type PortworxComponent struct {
	// Nodes for which the diags need to be collected. If a volume selector is
	// also specified, then both the selectors will be honored and the selected
	// nodes will be a union of both selectors.
	NodeSelector NodeSelector `json:"nodes,omitempty"`
	// Volumes for which the diags need to be collected.
	VolumeSelector VolumeSelector `json:"volumes,omitempty"`
	// Generates the core dump as well when collecting the diags. Could be useful
	// to analyze the current state of the system.
	GenerateCore bool `json:"generateCore,omitempty"`
}

// Allows selecting all the nodes in the entire cluster or a specific set with
// node IDs or node label selectors. It will take a union of all the combinations
// and collect diags from those nodes.
type NodeSelector struct {
	// Select all nodes in the Portworx cluster. If set to true, other selectors are ignored.
	All bool `json:"all,omitempty"`
	// Ids of the nodes to be selected.
	IDs []string `json:"ids,omitempty"`
	// Labels of the volumes to be selected.
	Labels map[string]string `json:"labels,omitempty"`
}

// Allows selecting one or more volumes for which diags need to be  collected.
// It will take a union of volume IDs and volume labels and collect diags for
// all that match. Currently it will find the Portworx nodes that contain the
// replicas of the selected volumes and wherever the volume is attached or
// being consumed from.
type VolumeSelector struct {
	// Ids of the volumes to be selected.
	IDs []string `json:"ids,omitempty"`
	// Labels of the volumes to be selected.
	Labels map[string]string `json:"labels,omitempty"`
}

// PortworxDiagStatus is the status of a portworx diag.
type PortworxDiagStatus struct {
	// One word status of the entire diags collection job.
	Phase string `json:"phase,omitempty"`
	// UUID of the Portworx cluster. This is useful to find the uploaded diags.
	ClusterUUID string `json:"clusterUuid,omitempty"`
	// Status of the diags collection from all the selected nodes.
	NodeStatuses []NodeStatus `json:"nodes,omitempty"`
}

// Status of the diags collection from a single node.
type NodeStatus struct {
	// ID of the node for which the diag status is reported.
	NodeID string `json:"nodeId,omitempty"`
	// One word status of the diags collection on the node.
	Status string `json:"status,omitempty"`
	// Optional message used to give the reason for any failure.
	Message string `json:"message,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,shortName=pxdiag
// +kubebuilder:printcolumn:name="Status",type="string",JSONPath=".status.phase",description="Status of the Portworx diag collection."
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp",description="Age of the diag resource."

// PortworxDiag represents a portworx diag
type PortworxDiag struct {
	meta.TypeMeta   `json:",inline"`
	meta.ObjectMeta `json:"metadata,omitempty"`
	Spec            PortworxDiagSpec   `json:"spec,omitempty"`
	Status          PortworxDiagStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// PortworxDiagList is a list of portworx diags.
type PortworxDiagList struct {
	meta.TypeMeta `json:",inline"`
	meta.ListMeta `json:"metadata,omitempty"`
	Items         []PortworxDiag `json:"items"`
}

func init() {
	SchemeBuilder.Register(&PortworxDiag{}, &PortworxDiagList{})
}
