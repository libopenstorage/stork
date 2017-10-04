package k8s

import (
	"fmt"

	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/scheduler/spec"
)

// ErrNodeNotReady error type when a k8s node is not ready
type ErrNodeNotReady struct {
	// Node is not which is not ready
	Node node.Node
	// Cause is the underlying cause of the error
	Cause string
}

func (e *ErrNodeNotReady) Error() string {
	return fmt.Sprintf("Node: %v is not ready due to err: %v", e.Node.Name, e.Cause)
}

// ErrFailedToScheduleApp error type for failing to schedule an app
type ErrFailedToScheduleApp struct {
	// App is the app that failed to schedule
	App *spec.AppSpec
	// Cause is the underlying cause of the error
	Cause string
}

func (e *ErrFailedToScheduleApp) Error() string {
	return fmt.Sprintf("Failed to schedule app: %v due to err: %v", e.App.Key, e.Cause)
}

// ErrFailedToDestroyApp error type for failing to destroy an app
type ErrFailedToDestroyApp struct {
	// App is the app that failed to destroy
	App *spec.AppSpec
	// Cause is the underlying cause of the error
	Cause string
}

func (e *ErrFailedToDestroyApp) Error() string {
	return fmt.Sprintf("Failed to destory app: %v due to err: %v", e.App.Key, e.Cause)
}

// ErrFailedToDestroyStorage error type for failing to destroy an app's storage
type ErrFailedToDestroyStorage struct {
	// App is the app that failed to destroy
	App *spec.AppSpec
	// Cause is the underlying cause of the error
	Cause string
}

func (e *ErrFailedToDestroyStorage) Error() string {
	return fmt.Sprintf("Failed to destory storage for app: %v due to err: %v", e.App.Key, e.Cause)
}

// ErrFailedToValidateStorage error type for failing to validate an app's storage
type ErrFailedToValidateStorage struct {
	// App is the app that failed to destroy
	App *spec.AppSpec
	// Cause is the underlying cause of the error
	Cause string
}

func (e *ErrFailedToValidateStorage) Error() string {
	return fmt.Sprintf("Failed to validate storage for app: %v due to err: %v", e.App.Key, e.Cause)
}

// ErrFailedToValidateApp error type for failing to validate an app
type ErrFailedToValidateApp struct {
	// App is the app that failed to destroy
	App *spec.AppSpec
	// Cause is the underlying cause of the error
	Cause string
}

func (e *ErrFailedToValidateApp) Error() string {
	return fmt.Sprintf("Failed to validate app: %v due to err: %v", e.App.Key, e.Cause)
}

// ErrFailedToGetAppStatus error type for failing to get app's status
type ErrFailedToGetAppStatus struct {
	// App is the app for which we want to get the status
	App *spec.AppSpec
	// Cause is the underlying cause of the error
	Cause string
}

func (e *ErrFailedToGetAppStatus) Error() string {
	return fmt.Sprintf("Failed to get status of app: %v due to err: %v", e.App.Key, e.Cause)
}

// ErrFailedToValidateAppDestroy error type for failing to validate destory of an app
type ErrFailedToValidateAppDestroy struct {
	// App is the app that failed to destroy
	App *spec.AppSpec
	// Cause is the underlying cause of the error
	Cause string
}

func (e *ErrFailedToValidateAppDestroy) Error() string {
	return fmt.Sprintf("Failed to validate destroy of app: %v due to err: %v", e.App.Key, e.Cause)
}

// ErrFailedToGetNodesForApp error type for failing to get nodes on which app is running
type ErrFailedToGetNodesForApp struct {
	// App is the app that failed to get to get nodes
	App *spec.AppSpec
	// Cause is the underlying cause of the error
	Cause string
}

func (e *ErrFailedToGetNodesForApp) Error() string {
	return fmt.Sprintf("Failed to get nodes of app: %v due to err: %v", e.App.Key, e.Cause)
}

// ErrFailedToDeleteTasks error type for failing to delete the tasks for an app
type ErrFailedToDeleteTasks struct {
	// App is the app for which we failed to delete the tasks
	App *spec.AppSpec
	// Cause is the underlying cause of the error
	Cause string
}

func (e *ErrFailedToDeleteTasks) Error() string {
	return fmt.Sprintf("Failed to delete tasks of app: %v due to err: %v", e.App.Key, e.Cause)
}

// ErrFailedToGetVolumesForApp error type for failing to get an app's volumes
type ErrFailedToGetVolumesForApp struct {
	// App is the app that has volumes we want to get
	App *spec.AppSpec
	// Cause is the underlying cause of the error
	Cause string
}

func (e *ErrFailedToGetVolumesForApp) Error() string {
	return fmt.Sprintf("Failed to get volumes for app: %v due to err: %v", e.App.Key, e.Cause)
}

// ErrFailedToGetVolumesParameters error type for failing to get an app's volume paramters
type ErrFailedToGetVolumesParameters struct {
	// App is the app for which we failed to get volume parameters
	App *spec.AppSpec
	// Cause is the underlying cause of the error
	Cause string
}

func (e *ErrFailedToGetVolumesParameters) Error() string {
	return fmt.Sprintf("Failed to get volume parameters for app: %v due to err: %v", e.App.Key, e.Cause)
}

// ErrFailedToGetPvcStatus error type for failing to get the status of a pvc
type ErrFailedToGetPvcStatus struct {
	// App whose persistent volume claim status couldn't be obtained
	App *spec.AppSpec
	// Cause is the underlying cause of the error
	Cause string
}

func (e *ErrFailedToGetPvcStatus) Error() string {
	return fmt.Sprintf("Failed to get status of pvc: %v due to err: %v", e.App.Key, e.Cause)
}

// ErrFailedToGetScParams error type for failing to get the parameters of a storage class
type ErrFailedToGetScParams struct {
	// App whose storage class params couldn't be obtained
	App *spec.AppSpec
	// Cause is the underlying cause of the error
	Cause string
}

func (e *ErrFailedToGetScParams) Error() string {
	return fmt.Sprintf("Failed to get params of storage class: %v due to err: %v", e.App.Key, e.Cause)
}
