package portworx

import (
	"fmt"

	"github.com/portworx/torpedo/drivers/node"
)

// ErrFailedToInspectVolume error type for failing to inspect a volume
type ErrFailedToInspectVolume struct {
	// ID is the ID/name of the volume that failed to inspect
	ID string
	// Cause is the underlying cause of the error
	Cause string
}

func (e *ErrFailedToInspectVolume) Error() string {
	return fmt.Sprintf("Failed to inspect volume: %v due to err: %v", e.ID, e.Cause)
}

func errFailedToInspectVolume(ID, key string, expected, actual interface{}) error {
	return &ErrFailedToInspectVolume{
		ID: ID,
		Cause: fmt.Sprintf("volume has invalid %v value. Expected:%#v Actual:%#v",
			key, expected, actual),
	}
}

// ErrFailedToDeleteVolume error type for failing to delete a volume
type ErrFailedToDeleteVolume struct {
	// ID is the ID/name of the volume that failed to delete
	ID string
	// Cause is the underlying cause of the error
	Cause string
}

func (e *ErrFailedToDeleteVolume) Error() string {
	return fmt.Sprintf("Failed to delete volume: %v due to err: %v", e.ID, e.Cause)
}

// ErrFailedToWaitForPx error type for failing to wait for PX to be up on a node
type ErrFailedToWaitForPx struct {
	// Node is the node on which PX was waited upon
	Node node.Node
	// Cause is the underlying cause of the error
	Cause string
}

func (e *ErrFailedToWaitForPx) Error() string {
	return fmt.Sprintf("Failed to wait for px status on: %v due to err: %v", e.Node.Name, e.Cause)
}

// ErrFailedToUpgradeVolumeDriver error type for failed volume driver upgrade
type ErrFailedToUpgradeVolumeDriver struct {
	// Version is the new image used to upgrade the volume driver
	Version string
	// Cause is the underlying cause of the error
	Cause string
}

func (e *ErrFailedToUpgradeVolumeDriver) Error() string {
	return fmt.Sprintf("Failed to upgrade volume driver to version: %v due to err: %v", e.Version, e.Cause)
}

// ErrFailedToRecoverDriver error type for failing to recover PX on a node
type ErrFailedToRecoverDriver struct {
	// Node is the node on which PX failed to recover on
	Node node.Node
	// Cause is the underlying cause of the error
	Cause string
}

func (e *ErrFailedToRecoverDriver) Error() string {
	return fmt.Sprintf("Failed to wait for px to be up on: %v due to err: %v", e.Node.Name, e.Cause)
}

// ErrFailedToSetReplicationFactor error type for failing to set replication factor to given value
type ErrFailedToSetReplicationFactor struct {
	// ID is the ID/name of the volume for which we could not set the replication factor
	ID string
	// Cause is the underlying cause of the error
	Cause string
}

func (e *ErrFailedToSetReplicationFactor) Error() string {
	return fmt.Sprintf("Failed to set replication factor of the volume: %v due to err: %v", e.ID, e.Cause)
}

// ErrFailedToGetReplicationFactor error type for failing to get/query the current replication factor
type ErrFailedToGetReplicationFactor struct {
	// ID is the ID/name of the volume for which we could not get the replication factor
	ID string
	// Cause is the underlying cause of the error
	Cause string
}

func (e *ErrFailedToGetReplicationFactor) Error() string {
	return fmt.Sprintf("Failed to get replication factor of the volume: %v due to err: %v", e.ID, e.Cause)
}

// ErrFailedToGetAggregationLevel error type for failing to get/query the aggregation level
type ErrFailedToGetAggregationLevel struct {
	// ID is the ID/name of the volume for which we could not get the aggregation level
	ID string
	// Cause is the underlying cause of the error
	Cause string
}

func (e *ErrFailedToGetAggregationLevel) Error() string {
	return fmt.Sprintf("Failed to get aggregation level of the volume: %v due to err: %v", e.ID, e.Cause)
}
