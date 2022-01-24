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

// ErrNotImplemented error type for methods not implemented
type ErrNotImplemented struct {
}

func (e *ErrNotImplemented) Error() string {
	return "Method not implemented"
}

// ErrNotSupported error type for methods not supported
type ErrNotSupported struct {
	// Feature which is not supported
	Feature string
	// Reason why feature is not supported (optional)
	Reason string
}

func (e *ErrNotSupported) Error() string {
	return fmt.Sprintf("%v not supported. Reason: %v", e.Feature, e.Reason)
}
