package v1beta2

import (
	"github.com/portworx/talisman/pkg/apis/portworx/v1beta1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +genclient
// +genclient:noStatus
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// VolumePlacementStrategy specifies a spec for volume placement in the cluster
type VolumePlacementStrategy struct {
	meta.TypeMeta   `json:",inline"`
	meta.ObjectMeta `json:"metadata,omitempty"`
	Spec            VolumePlacementSpec `json:"spec"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// VolumePlacementStrategyList is a list of VolumePlacementStrategy objects
type VolumePlacementStrategyList struct {
	meta.TypeMeta `json:",inline"`
	meta.ListMeta `json:"metadata,omitempty"`
	// Items are the list of volume placements strategy items
	Items []VolumePlacementStrategy `json:"items"`
}

// VolumePlacementSpec specifies a set of rules for volume placement in the cluster
type VolumePlacementSpec struct {
	// The spec defines a list of rules as part of the placement spec. All the rules specified will
	// be applied for volume placement.
	// Rules that have enforcement as "required" are strictly enforced while "preferred" are best effort.
	// In situations, where 2 or more rules conflict, the weight of the rules will dictate which wins.

	// ReplicaAffinity defines affinity rules between replicas within a volume
	ReplicaAffinity []*ReplicaPlacementSpec `json:"replicaAffinity,omitempty"`
	// ReplicaAntiAffinity defines anti-affinity rules between replicas within a volume
	ReplicaAntiAffinity []*ReplicaPlacementSpec `json:"replicaAntiAffinity,omitempty"`
	// VolumeAffinity defines affinity rules between volumes
	VolumeAffinity []*CommonPlacementSpec `json:"volumeAffinity,omitempty"`
	// VolumeAntiAffinity defines anti-affinity rules between volumes
	VolumeAntiAffinity []*CommonPlacementSpec `json:"volumeAntiAffinity,omitempty"`
}

// ReplicaPlacementSpec is the spec for replica affinity and anti-affinity
type ReplicaPlacementSpec struct {
	CommonPlacementSpec
	// AffectedReplicas defines the number of volume replicas affected by this rule. If not provided,
	// rule would affect all replicas
	// (optional)
	AffectedReplicas uint64 `json:"affected_replicas,omitempty"`
}

// CommonPlacementSpec is the spec that's common for replica and volume affinity and anti-affinity rules
type CommonPlacementSpec struct {
	// Weight defines the weight of the rule which allows to break the tie with other matching rules. A rule with
	// higher weight wins over a rule with lower weight.
	// (optional)
	Weight uint64 `json:"weight,omitempty"`
	// Enforcement specifies the rule enforcement policy. Can take values: required or preferred.
	// (optional)
	Enforcement v1beta1.EnforcementType `json:"enforcement,omitempty"`
	// TopologyKey key for the matching all segments of the cluster topology with the same key
	// e.g If the key is failure-domain.beta.kubernetes.io/zone, this should match all nodes with
	// the same value for this key (i.e in the same zone)
	TopologyKey string `json:"topologyKey,omitempty"`
	// MatchExpressions is a list of label selector requirements. The requirements are ANDed.
	MatchExpressions []*v1beta1.LabelSelectorRequirement `json:"matchExpressions,omitempty"`
}
