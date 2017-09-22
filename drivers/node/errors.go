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

// ErrFailedToRunCommand error type when failing to run command
type ErrFailedToRunCommand struct {
	Addr  string
	Cause string
}

func (e *ErrFailedToRunCommand) Error() string {
	return fmt.Sprintf("Failed to run command on: %v. Cause: %v", e.Addr, e.Cause)
}
