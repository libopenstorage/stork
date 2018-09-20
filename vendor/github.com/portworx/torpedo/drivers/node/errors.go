package node

import (
	"fmt"
)

// ErrFailedToTestConnection error type when failing to test connection
type ErrFailedToTestConnection struct {
	Node  Node
	Cause string
}

func (e *ErrFailedToTestConnection) Error() string {
	return fmt.Sprintf("Failed to test connnection to %v. Cause: %v", e.Node.Name, e.Cause)
}

// ErrFailedToRebootNode error type when failing to reboot a node
type ErrFailedToRebootNode struct {
	Node  Node
	Cause string
}

func (e *ErrFailedToRebootNode) Error() string {
	return fmt.Sprintf("Failed to reboot node: %v. Cause: %v", e.Node.Name, e.Cause)
}

// ErrFailedToShutdownNode error type when failing to shutdown the node
type ErrFailedToShutdownNode struct {
	Node  Node
	Cause string
}

func (e *ErrFailedToShutdownNode) Error() string {
	return fmt.Sprintf("Failed to shutdown node: %v. Cause: %v", e.Node.Name, e.Cause)
}

// ErrFailedToFindFileOnNode error type when failing to run find on the node
type ErrFailedToFindFileOnNode struct {
	Node  Node
	Cause string
}

func (e *ErrFailedToFindFileOnNode) Error() string {
	return fmt.Sprintf("Failed to find file on: %v. Cause: %v", e.Node.Name, e.Cause)
}

// ErrFailedToRunSystemctlOnNode error type when failing to run systemctl on the node
type ErrFailedToRunSystemctlOnNode struct {
	Node  Node
	Cause string
}

func (e *ErrFailedToRunSystemctlOnNode) Error() string {
	return fmt.Sprintf("Failed to run systemctl command on: %v. Cause: %v", e.Node.Name, e.Cause)
}

// ErrFailedToRunCommand error type when failing to run command
type ErrFailedToRunCommand struct {
	Addr  string
	Cause string
}

func (e *ErrFailedToRunCommand) Error() string {
	return fmt.Sprintf("Failed to run command on: %v. Cause: %v", e.Addr, e.Cause)
}

// ErrFailedToYankDrive error type when we fail to simulate drive failure
type ErrFailedToYankDrive struct {
	Node  Node
	Cause string
}

func (e *ErrFailedToYankDrive) Error() string {
	return fmt.Sprintf("Failed to yank a drive on: %v. Cause: %v", e.Node.Name, e.Cause)
}

// ErrFailedToRecoverDrive error type when we fail to simulate drive failure
type ErrFailedToRecoverDrive struct {
	Node  Node
	Cause string
}

func (e *ErrFailedToRecoverDrive) Error() string {
	return fmt.Sprintf("Failed to recover a drive on: %v. Cause: %v", e.Node.Name, e.Cause)
}

// ErrFailedToSystemCheck error type when we fail to check for core files
type ErrFailedToSystemCheck struct {
	Node  Node
	Cause string
}

func (e *ErrFailedToSystemCheck) Error() string {
	return fmt.Sprintf("System check failed on: %v. Cause: %v", e.Node.Name, e.Cause)
}
