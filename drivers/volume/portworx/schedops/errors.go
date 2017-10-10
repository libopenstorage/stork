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
