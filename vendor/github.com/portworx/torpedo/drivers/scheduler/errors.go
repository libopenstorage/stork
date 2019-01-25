package scheduler

import (
	"fmt"
	"reflect"

	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/scheduler/spec"
)

// ErrNodeNotReady error type when a node is not ready
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
	// App is the app whose storage validation failed
	App *spec.AppSpec
	// Cause is the underlying cause of the error
	Cause string
}

func (e *ErrFailedToValidateStorage) Error() string {
	return fmt.Sprintf("Failed to validate storage for app: %v due to err: %v", e.App.Key, e.Cause)
}

// ErrFailedToGetStorage error type for failing to get an app's storage
type ErrFailedToGetStorage struct {
	// App is the app whose storage could not be retrieved
	App *spec.AppSpec
	// Cause is the underlying cause of the error
	Cause string
}

func (e *ErrFailedToGetStorage) Error() string {
	return fmt.Sprintf("Failed to get storage for app: %v due to err: %v", e.App.Key, e.Cause)
}

// ErrFailedToResizeStorage error type for failing to update an app's storage
type ErrFailedToResizeStorage struct {
	// App is the app whose storage could not be retrieved
	App *spec.AppSpec
	// Cause is the underlying cause of the error
	Cause string
}

func (e *ErrFailedToResizeStorage) Error() string {
	return fmt.Sprintf("Failed to resize storage for app: %v due to err: %v", e.App.Key, e.Cause)
}

// ErrFailedToValidateApp error type for failing to validate an app
type ErrFailedToValidateApp struct {
	// App is the app whose validation failed
	App *spec.AppSpec
	// Cause is the underlying cause of the error
	Cause string
}

func (e *ErrFailedToValidateApp) Error() string {
	return fmt.Sprintf("Failed to validate app: %v due to err: %v", e.App.Key, e.Cause)
}

// ErrFailedToSchedulePod error type for failing to schedule a pod
type ErrFailedToSchedulePod struct {
	// Pod is the pod that failed to schedule
	App *spec.AppSpec
	// Cause is the underlying cause of the error
	Cause string
}

func (e *ErrFailedToSchedulePod) Error() string {
	return fmt.Sprintf("Failed to schedule app: %v due to err: %v", e.App.Key, e.Cause)
}

// ErrFailedToValidatePod error type for failing to validate a pod
type ErrFailedToValidatePod struct {
	// App is the app whose validation failed
	App *spec.AppSpec
	// Cause is the underlying cause of the error
	Cause string
}

func (e *ErrFailedToValidatePod) Error() string {
	return fmt.Sprintf("Failed to validate pod: %v due to err: %v", e.App.Key, e.Cause)
}

// ErrFailedToDestroyPod error type for failing to validate destory of a pod
type ErrFailedToDestroyPod struct {
	// App is the app that failed to destroy
	App *spec.AppSpec
	// Cause is the underlying cause of the error
	Cause string
}

func (e *ErrFailedToDestroyPod) Error() string {
	return fmt.Sprintf("Failed to validate destroy of pod: %v due to err: %v", e.App.Key, e.Cause)
}

// ErrFailedToGetPodStatus error type for failing to get pod's status
type ErrFailedToGetPodStatus struct {
	// App is the app for which we want to get the status
	App *spec.AppSpec
	// Cause is the underlying cause of the error
	Cause string
}

func (e *ErrFailedToGetPodStatus) Error() string {
	return fmt.Sprintf("Failed to get status of pod: %v due to err: %v", e.App.Key, e.Cause)
}

// ErrFailedToValidatePodDestroy error type for failing to validate destroy of an pod
type ErrFailedToValidatePodDestroy struct {
	// App is the app that failed to destroy
	App *spec.AppSpec
	// Cause is the underlying cause of the error
	Cause string
}

func (e *ErrFailedToValidatePodDestroy) Error() string {
	return fmt.Sprintf("Failed to validate destroy of pod: %v due to err: %v", e.App.Key, e.Cause)
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

// ErrFailedToGetVolumeParameters error type for failing to get an app's volume paramters
type ErrFailedToGetVolumeParameters struct {
	// App is the app for which we failed to get volume parameters
	App *spec.AppSpec
	// Cause is the underlying cause of the error
	Cause string
}

func (e *ErrFailedToGetVolumeParameters) Error() string {
	return fmt.Sprintf("Failed to get volume parameters for app: %v due to err: %v", e.App.Key, e.Cause)
}

// ErrFailedToGetStorageStatus error type for failing to get the status of the app's storage
type ErrFailedToGetStorageStatus struct {
	// App whose storage status couldn't be obtained
	App *spec.AppSpec
	// Cause is the underlying cause of the error
	Cause string
}

func (e *ErrFailedToGetStorageStatus) Error() string {
	return fmt.Sprintf("Failed to get storage status for: %v due to err: %v", e.App.Key, e.Cause)
}

// ErrFailedToDeleteVolumeDirForPod error type for failing to delete volume dir path for pods
type ErrFailedToDeleteVolumeDirForPod struct {
	// App is the app whose volume directories are not deleted
	App *spec.AppSpec
	// Cause is the underlying cause of the error
	Cause string
}

func (e *ErrFailedToDeleteVolumeDirForPod) Error() string {
	return fmt.Sprintf("Failed to delete volume directory for app: %v due to err: %v", e.App.Key, e.Cause)
}

// ErrFailedToUpdateApp error type for failing to update an app
type ErrFailedToUpdateApp struct {
	// App is the app whose validation failed
	App *spec.AppSpec
	// Cause is the underlying cause of the error
	Cause string
}

func (e *ErrFailedToUpdateApp) Error() string {
	return fmt.Sprintf("Failed to update app: %v due to err: %v", e.App.Key, e.Cause)
}

// ErrFailedToStopSchedOnNode error type when fail to stop scheduler service on the node
type ErrFailedToStopSchedOnNode struct {
	// Node where the service is not stopped
	Node node.Node
	// SystemService responsible for scheduling
	SystemService string
	// Cause is the underlying cause of the error
	Cause string
}

func (e *ErrFailedToStopSchedOnNode) Error() string {
	return fmt.Sprintf("Failed to stop scheduler service %v on node: %v due to err: %v", e.SystemService, e.Node, e.Cause)
}

// ErrFailedToStartSchedOnNode error type when fail to start scheduler service on the node
type ErrFailedToStartSchedOnNode struct {
	// Node where the service is not starting
	Node node.Node
	// SystemService responsible for scheduling
	SystemService string
	// Cause is the underlying cause of the error
	Cause string
}

func (e *ErrFailedToStartSchedOnNode) Error() string {
	return fmt.Sprintf("Failed to start scheduler service %v on node: %v due to err: %v", e.SystemService, e.Node, e.Cause)
}

// ErrFailedToValidateCustomSpec error type when CRD objects does not applied successfully
type ErrFailedToValidateCustomSpec struct {
	// Name of CRD object
	Name string
	// Cause is the underlying cause of the error
	Cause string
	// Type is the underlying type of CRD objects
	Type interface{}
}

func (e *ErrFailedToValidateCustomSpec) Error() string {
	return fmt.Sprintf("Failed to validate custom spec : %v of type %v due to err: %v", e.Name, reflect.TypeOf(e.Type), e.Cause)
}
