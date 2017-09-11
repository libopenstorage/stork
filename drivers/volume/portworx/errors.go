package portworx

import "fmt"

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
