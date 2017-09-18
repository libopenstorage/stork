package portworx

import (
	"fmt"
	"github.com/portworx/torpedo/drivers/node"
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

func errFailedToInspectVolme(ID, key string, expected, actual interface{}) error {
	return &ErrFailedToInspectVolme{
		ID: ID,
		Cause: fmt.Sprintf("volume has invalid %v value. Expected:%#v Actual:%#v",
			key, expected, actual),
	}
}

// ErrFailedToWaitForPx error type for failing to wait for px to be up on a node
type ErrFailedToWaitForPx struct {
	// Node is the node on which px was waited upon
	Node node.Node
	// Cause is the underlying cause of the error
	Cause string
}

func (e *ErrFailedToWaitForPx) Error() string {
	return fmt.Sprintf("Failed to wait for px to be up on: %v due to err: %v", e.Node.Name, e.Cause)
}
