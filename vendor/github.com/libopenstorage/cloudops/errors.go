package cloudops

import "fmt"

// Custom storage operation error codes.
const (
	_ = iota + 5000
	// ErrVolDetached is code for a volume is detached on the instance
	ErrVolDetached
	// ErrVolInval is the code for a invalid volume
	ErrVolInval
	// ErrVolAttachedOnRemoteNode is code when a volume is not attached locally
	// but attached on a remote node
	ErrVolAttachedOnRemoteNode
	// ErrVolNotFound is code when a volume is not found
	ErrVolNotFound
	// ErrInvalidDevicePath is code when a volume/disk has invalid device path
	ErrInvalidDevicePath
	// ErrExponentialTimeout is code when all the retries with exponential backoff have exhausted
	ErrExponentialTimeout
	// ErrDiskGreaterOrEqualToExpandSize is code when a volume/disk expansion call fails
	// as the given disk is already at a size greater than or equal to requested size
	ErrDiskGreaterOrEqualToExpandSize
)

// ErrNotSupported is the error type for unsupported operations
type ErrNotSupported struct {
	// Operation is the operation not being supported
	Operation string
	// Reason is an optional reason for not supporting the operation
	Reason string
}

func (e *ErrNotSupported) Error() string {
	errString := fmt.Sprintf("Operation: %s is not supported", e.Operation)
	if len(e.Reason) > 0 {
		errString = fmt.Sprintf("%s. Reason: %s", errString, e.Reason)
	}

	return errString
}

// StorageError error returned for storage operations
type StorageError struct {
	// Code is one of storage operation driver error codes.
	Code int
	// Msg is human understandable error message.
	Msg string
	// Instance provides more information on the error.
	Instance string
}

// NewStorageError creates a new custom storage error instance
func NewStorageError(code int, msg string, instance string) error {
	return &StorageError{Code: code, Msg: msg, Instance: instance}
}

func (e *StorageError) Error() string {
	return e.Msg
}

// ErrNoInstanceGroup is returned when instance doesn't belong to an instance group
type ErrNoInstanceGroup struct {
	// Reason is an optional reason for not belong to an instance group
	Reason string
}

func (e *ErrNoInstanceGroup) Error() string {
	errString := "Instance doesn't belong to an instance group"
	if len(e.Reason) > 0 {
		errString = fmt.Sprintf("%s. Reason: %s", errString, e.Reason)
	}

	return errString
}

// ErrInvalidStoragePoolUpdateRequest is returned when an unsupported or invalid request
// is sent to get the updated storage config on an instance
type ErrInvalidStoragePoolUpdateRequest struct {
	// Request is the request that caused the invalid error
	Request *StoragePoolUpdateRequest
	// Reason is the reason why the request was invalid
	Reason string
}

func (e *ErrInvalidStoragePoolUpdateRequest) Error() string {
	return fmt.Sprintf("Invalid request to update storage on instance due to: %s Request: %v",
		e.Reason, e.Request)
}

// ErrCurrentCapacityHigherThanDesired is returned when the current capacity of the instance is already higher than
// the desired capacity
type ErrCurrentCapacityHigherThanDesired struct {
	// Current is the current capacity
	Current uint64
	// Desired is the desired new capacity
	Desired uint64
}

func (e *ErrCurrentCapacityHigherThanDesired) Error() string {
	return fmt.Sprintf("current capacity (%d) is higher than desired capacity: %d", e.Current, e.Desired)
}
