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

// ErrFailedToGetSnapShotData error type for failing to get snapshotdata of the volume
type ErrFailedToGetSnapShotData struct {
	// App is the app for which we failed to get volume parameters
	App *spec.AppSpec
	// Cause is the underlying cause of the error
	Cause string
}

func (e *ErrFailedToGetSnapShotData) Error() string {
	return fmt.Sprintf("Failed to get snapshot data for app: %v due to err: %v", e.App.Key, e.Cause)
}

// ErrFailedToGetSnapShot error type for failing to get snapshot of the volume
type ErrFailedToGetSnapShot struct {
	// App is the app for which we failed to get volume parameters
	App *spec.AppSpec
	// Cause is the underlying cause of the error
	Cause string
}

func (e *ErrFailedToGetSnapShot) Error() string {
	return fmt.Sprintf("Failed to get snapshot for app: %v due to err: %v", e.App.Key, e.Cause)
}

// ErrFailedToGetSnapShotDataName error type for failing to get snapshotdata name of the snapshot
type ErrFailedToGetSnapShotDataName struct {
	// App is the app for which we failed to get volume parameters
	App *spec.AppSpec
	// Cause is the underlying cause of the error
	Cause string
}

func (e *ErrFailedToGetSnapShotDataName) Error() string {
	return fmt.Sprintf("Failed to get snapshot data name for app: %v due to err: %v", e.App.Key, e.Cause)
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

// ErrFailedToDecommissionNode error type when fail to decommission a node
type ErrFailedToDecommissionNode struct {
	// Node where the service is not starting
	Node node.Node
	// Cause is the underlying cause of the error
	Cause string
}

func (e *ErrFailedToDecommissionNode) Error() string {
	return fmt.Sprintf("Failed to decommission node: %v due to err: %v", e.Node, e.Cause)
}

// ErrFailedToGetConfigMap error type for failing to get config map
type ErrFailedToGetConfigMap struct {
	// Name of config map
	Name string
	// Cause is the underlying cause of the error
	Cause string
}

func (e *ErrFailedToGetConfigMap) Error() string {
	return fmt.Sprintf("Failed to get config map: %s due to err: %v", e.Name, e.Cause)
}

// ErrFailedToAddLabelOnNode error type for failing to add label on node
type ErrFailedToAddLabelOnNode struct {
	// Key is the label key
	Key string
	// Value is the label value
	Value string
	// Node is the node where label should be added
	Node node.Node
	// Cause is the underlying cause of the error
	Cause string
}

func (e *ErrFailedToAddLabelOnNode) Error() string {
	return fmt.Sprintf("Failed to add label: %s=%s on node %v due to err: %v", e.Key, e.Value, e.Node, e.Cause)
}

// ErrFailedToRemoveLabelOnNode error type for failing to remove label on node
type ErrFailedToRemoveLabelOnNode struct {
	// Key is the label key
	Key string
	// Node is the node where label should be added
	Node node.Node
	// Cause is the underlying cause of the error
	Cause string
}

func (e *ErrFailedToRemoveLabelOnNode) Error() string {
	return fmt.Sprintf("Failed to remove label: %s on node: %v due to err: %v", e.Key, e.Node, e.Cause)
}

// ErrFailedToGetCustomSpec error type for failing to get config map
type ErrFailedToGetCustomSpec struct {
	// Name of config map
	Name string
	// Cause is the underlying cause of the error
	Cause string
	// Type is the underlying type of CRD objects
	Type interface{}
}

func (e *ErrFailedToGetCustomSpec) Error() string {
	return fmt.Sprintf("Failed to get custom spec: %s due to err: %v", e.Name, e.Cause)
}

// ErrFailedToGetSecret error when we are unable to get the defined secret
type ErrFailedToGetSecret struct {
	// App is the spec for which we want to get the secret
	App *spec.AppSpec
	// Cause is the underlying cause of the error
	Cause string
}

func (e *ErrFailedToGetSecret) Error() string {
	return fmt.Sprintf("Failed to get Secret : %v due to err: %v", e.App.Key, e.Cause)
}

// ErrFailedToGetEvents error when we are unable to get events
type ErrFailedToGetEvents struct {
	// Type is the resource type which we want to get the events
	Type string
	// Name of object
	Name string
	// Cause is the underlying cause of the error
	Cause string
}

func (e *ErrFailedToGetEvents) Error() string {
	return fmt.Sprintf("Failed to get Events for: [%v]%v due to err: %v", e.Type, e.Name, e.Cause)
}

// ErrFailedToDeleteNode error when we are unable to delete node
type ErrFailedToDeleteNode struct {
	// Node which failed to delete
	Node node.Node

	// Cause is the underlying cause of the error
	Cause string
}

func (e *ErrFailedToDeleteNode) Error() string {
	return fmt.Sprintf("Failed to Delete a Node : [%v] due to err: %v", e.Node.Name, e.Cause)
}

// ErrFailedToGetNode error when we are unable to get a new node
type ErrFailedToGetNode struct {
	// Cause is the underlying cause of the error
	Cause string
}

func (e *ErrFailedToGetNode) Error() string {
	return fmt.Sprintf("Failed to Get a Node due to err: %v", e.Cause)
}

// ErrFailedToUpdateNodeList error when we are unable to update a new node in node list
type ErrFailedToUpdateNodeList struct {
	// Node which failed to update
	Node string

	// Cause is the underlying cause of the error
	Cause string
}

func (e *ErrFailedToUpdateNodeList) Error() string {
	return fmt.Sprintf("Failed to Update a Node : [%v] due to err: %v", e.Node, e.Cause)
}

// ErrFailedToBringUpNode error when node failed to be up
type ErrFailedToBringUpNode struct {
	// Node which failed to start
	Node string

	// Cause is the underlying cause of the error
	Cause error
}

func (e *ErrFailedToBringUpNode) Error() string {
	return fmt.Sprintf("Failed to bring up a Node : [%v] due to err: %v", e.Node, e.Cause)
}

// ErrFailedToValidateTopologyLabel error when failed to validate topology label in namespace
type ErrFailedToValidateTopologyLabel struct {
	// Pod is the pod that failed to schedule
	NameSpace string
	// Cause is the underlying cause of the error
	Cause error
}

func (e *ErrFailedToValidateTopologyLabel) Error() string {
	return fmt.Sprintf("Failed to validate topology label in namespace: %v due to err: %v", e.NameSpace, e.Cause)
}

// ErrTopologyLabelMismatch  error when pod schedule on a node where label not matched
type ErrTopologyLabelMismatch struct {
	// Pod is the pod that failed to schedule
	PodName string
	// Cause is the underlying cause of the error
	Cause string
}

func (e *ErrTopologyLabelMismatch) Error() string {
	return fmt.Sprintf("Failed to match topology label for pod: %v due to err: %v", e.PodName, e.Cause)
}

// ErrFailedToCreateSnapshot error when snapshot create is failed
type ErrFailedToCreateSnapshot struct {
	// PvcName is name of the pvc for which snapshot create failed
	PvcName string
	// Cause is the underlying cause of the error
	Cause error
}

func (e *ErrFailedToCreateSnapshot) Error() string {
	return fmt.Sprintf("Failed to create snapshot for volume: %v due to err: %v", e.PvcName, e.Cause)
}

// ErrFailedToCreateCsiSnapshots error when snapshot create is failed
type ErrFailedToCreateCsiSnapshots struct {
	// App specs
	App *spec.AppSpec
	// Cause is the underlying cause of the error
	Cause string
}

func (e *ErrFailedToCreateCsiSnapshots) Error() string {
	return fmt.Sprintf("Failed to create snapshot for App: %v due to err: %v", e.App, e.Cause)
}

// ErrFailedToCreateSnapshotClass error when snapshot create is failed
type ErrFailedToCreateSnapshotClass struct {
	// Name is name of the snapshot class which failed to create
	Name string
	// Cause is the underlying cause of the error
	Cause error
}

func (e *ErrFailedToCreateSnapshotClass) Error() string {
	return fmt.Sprintf("Failed to create snapshot for volume: %v due to err: %v", e.Name, e.Cause)
}

// ErrFailedToGetSnapshotList error when snapshot create is failed
type ErrFailedToGetSnapshotList struct {
	// Name is name of the namespace
	Name string
	// Cause is the underlying cause of the error
	Cause error
}

func (e *ErrFailedToGetSnapshotList) Error() string {
	return fmt.Sprintf("Failed to list snapshot in namespace: %v due to err: %v", e.Name, e.Cause)
}

// ErrFailedToValidateSnapshot error is failed to validate the snapshot for a volume
type ErrFailedToValidateSnapshot struct {
	// Name is name of the PVC
	Name string
	// Cause is the underlying cause of the error
	Cause error
}

func (e *ErrFailedToValidateSnapshot) Error() string {
	return fmt.Sprintf("Failed to validate snapshot for volume: %v due to err: %v", e.Name, e.Cause)
}

// ErrFailedToValidateCsiSnapshots error is failed to validate the snapshot for a volume
type ErrFailedToValidateCsiSnapshots struct {
	// AppSpec
	App *spec.AppSpec
	// Cause is the underlying cause of the error
	Cause string
}

func (e *ErrFailedToValidateCsiSnapshots) Error() string {
	return fmt.Sprintf("Failed to validate Csi snapshot for App: %v due to err: %v", e.App.Key, e.Cause)
}

// ErrFailedToValidatePvc error is failed to validate PVC
type ErrFailedToValidatePvc struct {
	// Name is the name of the PVC
	Name string
	// Cause is the underlying cause of the error
	Cause error
}

func (e *ErrFailedToValidatePvc) Error() string {
	return fmt.Sprintf("Failed to validate PVC: %v due to err: %v", e.Name, e.Cause)
}

// ErrFailedToValidatePvcAfterRestore error is failed to validate PVC
type ErrFailedToValidatePvcAfterRestore struct {
	// Name is the name of the PVC
	App *spec.AppSpec
	// Cause is the underlying cause of the error
	Cause string
}

func (e *ErrFailedToValidatePvcAfterRestore) Error() string {
	return fmt.Sprintf("Failed to validate PVC for App: %v due to err: %v", e.App, e.Cause)
}

// ErrFailedToRestore error is failed to restore from snapshot
type ErrFailedToRestore struct {
	// Name is the name of the PVC
	App *spec.AppSpec
	// Cause is the underlying cause of the error
	Cause string
}

func (e *ErrFailedToRestore) Error() string {
	return fmt.Sprintf("Failed to validate PVC for App: %v due to err: %v", e.App, e.Cause)
}

// ErrFailedToDeleteSnapshot error is failed to restore from snapshot
type ErrFailedToDeleteSnapshot struct {
	// Name is the name of the namespace
	Name string
	// Cause is the underlying cause of the error
	Cause error
}

func (e *ErrFailedToDeleteSnapshot) Error() string {
	return fmt.Sprintf("Failed to delete snapshot in namespace: %v due to err: %v", e.Name, e.Cause)
}
