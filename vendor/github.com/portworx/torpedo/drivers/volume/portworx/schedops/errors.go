package schedops

import (
	"fmt"
	"strings"
)

// ErrFailedToCleanupVolume error type for orphan pods or unclean vol directories
type ErrFailedToCleanupVolume struct {
	// OrphanPods is a map of node to list of pod UIDs whose portworx volume dir is not deleted
	OrphanPods map[string][]string
	// DirtyVolPods is a map of node to list of pod UIDs which still has data written
	// under the volume mount point
	DirtyVolPods map[string][]string
}

func (e *ErrFailedToCleanupVolume) Error() string {
	var cause []string
	for node, pods := range e.OrphanPods {
		cause = append(cause, fmt.Sprintf("Failed to remove orphan volume dir on "+
			"node: %v for pods: %v", node, pods))
	}
	for node, pods := range e.DirtyVolPods {
		cause = append(cause, fmt.Sprintf("Failed to cleanup data under volume directory on "+
			"node: %v for pods: %v", node, pods))
	}
	return strings.Join(cause, ", ")
}

// ErrLabelMissingOnNode error type for missing volume labels on node
type ErrLabelMissingOnNode struct {
	// Label is the label string that is missing from the nodes
	Label string
	// Nodes is a list of node names which have missing labels for certain PVCs
	Nodes []string
}

func (e *ErrLabelMissingOnNode) Error() string {
	return fmt.Sprintf("Label %v missing on nodes %v", e.Label, e.Nodes)
}

// ErrLabelNotRemovedFromNode error type for stale volume labels on node
type ErrLabelNotRemovedFromNode struct {
	// Label is the label key that was not removed from the nodes
	Label string
	// Nodes is a list of node names which have stale volume related labels
	Nodes []string
}

func (e *ErrLabelNotRemovedFromNode) Error() string {
	return fmt.Sprintf("Label %v not removed from the nodes %v", e.Label, e.Nodes)
}
