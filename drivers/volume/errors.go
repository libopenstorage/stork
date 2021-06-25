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

// ErrStorageProviderBusy is returned when the storage provider
// cannot perform the requested operation
type ErrStorageProviderBusy struct {
	// Reason for the error
	Reason string
}

func (e *ErrStorageProviderBusy) Error() string {
	return fmt.Sprintf("storage provider busy: %v", e.Reason)
}

// ErrBackupExists is returned when the backup already exists
type ErrBackupExists struct {
	// UID of the backup
	UID string
}

func (e *ErrBackupExists) Error() string {
	return fmt.Sprintf("backup with uid %v already exists", e.UID)
}
