package volume

import (
	"fmt"
)

// ErrPVCPending PVC hasn't been bound yet
type ErrPVCPending struct {
	// Name of the PVC
	Name string
}

func (e *ErrPVCPending) Error() string {
	return fmt.Sprintf("PVC is pending for %v", e.Name)
}
