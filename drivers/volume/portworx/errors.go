package portworx

import (
	"fmt"
)

// ErrFailedToInspectVolme error type for failing to inspect a volume
type ErrFailedToInspectVolme struct {
	// ID is the ID/name of the volume that failed to inspect
	ID string
	// Cause is the underlying cause of the error
	Cause string
}

func (e *ErrFailedToInspectVolme) Error() string {
	return fmt.Sprintf("Failed to inspect volume: %v due to err: %v", e.ID, e.Cause)
}

// ErrFailedToGetNodes error type for failing to get the nodes where a driver
// is available
type ErrFailedToGetNodes struct {
	// Cause is the underlying cause of the error
	Cause string
}

func (e *ErrFailedToGetNodes) Error() string {
	return fmt.Sprintf("Failed to get nodes for the driver: %v", e.Cause)
}
