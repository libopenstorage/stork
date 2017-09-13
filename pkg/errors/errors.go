package errors

import "fmt"

// ErrNotFound error type for objects not found
type ErrNotFound struct {
	// UID unique object identifier.
	ID string
	// Type of the object which wasn't found
	Type string
}

func (e *ErrNotFound) Error() string {
	return fmt.Sprintf("%v with UID/Name: %v not found", e.Type, e.ID)
}

// ErrValidateVol is error type when a volume fails validation
type ErrValidateVol struct {
	// UID unique object identifier.
	ID string
	// Error is the underlying error
	Cause string
}

func (e *ErrValidateVol) Error() string {
	return fmt.Sprintf("Failed to validate volumes for spec: %v Err: %v", e.ID, e.Cause)
}

// ErrNotSupported is error type when an operation is not supposed
type ErrNotSupported struct {
	Type      string
	Operation interface{}
}

func (e *ErrNotSupported) Error() string {
	return fmt.Sprintf("%v %v is not supported", e.Type, e.Operation)
}
