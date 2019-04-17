package v1alpha1

import (
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	// ApplicationCloneResourceName is the name for the application clone resource
	ApplicationCloneResourceName = "applicationclone"
	// ApplicationCloneResourcePlural is the name in plural for the application clone resources
	ApplicationCloneResourcePlural = "applicationclones"
)

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ApplicationClone represents the cloning of application in different namespaces
type ApplicationClone struct {
	meta.TypeMeta   `json:",inline"`
	meta.ObjectMeta `json:"metadata,omitempty"`
	Spec            ApplicationCloneSpec   `json:"spec"`
	Status          ApplicationCloneStatus `json:"status,omitempty"`
}

// ApplicationCloneSpec defines the spec to create an application clone
type ApplicationCloneSpec struct {
	// SourceNamespace can be optional, and we can check in the code that we are using
	// the same namespace in which we are creating the cloning object
	// +optional
	SourceNamespace string `json:"sourceNamespace,omitempty"`
	// DestinationNamespace is a must parameter to tell the cloning object
	// where to place the application objects
	DestinationNamespace string `json:"destinationNamespace"`
	// Selectors for label on objects
	Selectors    map[string]string `json:"selectors"`
	PreExecRule  string            `json:"preExecRule"`
	PostExecRule string            `json:"postExecRule"`
	// ReplacePolicy to decide how to react when a object conflict occurs in the cloning process
	ReplacePolicy ApplicationCloneReplacePolicyType `json:"replacePolicy"`
}

// ApplicationCloneStatus defines the status of the clone
type ApplicationCloneStatus struct {
	// Status of the cloning process
	Status ApplicationCloneStatusType `json:"status"`
	// Stage of the cloning process
	Stage           ApplicationCloneStageType       `json:"stage"`
	Resources       []*ApplicationCloneResourceInfo `json:"resources"`
	Volumes         []*ApplicationCloneVolumeInfo   `json:"volumes"`
	FinishTimestamp meta.Time                       `json:"finishTimestamp"`
}

// ApplicationCloneResourceInfo is the info for the cloning of a resource
type ApplicationCloneResourceInfo struct {
	Name                  string                     `json:"name"`
	Reason                string                     `json:"reason"`
	Status                ApplicationCloneStatusType `json:"status"`
	meta.GroupVersionKind `json:",inline"`
}

// ApplicationCloneVolumeInfo is the info for the cloning of a volume
type ApplicationCloneVolumeInfo struct {
	PersistentVolumeClaim string                     `json:"persistentVolumeClaim"`
	Volume                string                     `json:"volume"`
	ClonedVolume          string                     `json:"clonedVolume"`
	Status                ApplicationCloneStatusType `json:"status"`
	Reason                string                     `json:"reason"`
}

// ApplicationCloneStatusType defines status of the application being cloned
type ApplicationCloneStatusType string

const (
	// ApplicationCloneStatusInitial initial state when the cloning will start
	ApplicationCloneStatusInitial ApplicationCloneStatusType = ""
	// ApplicationCloneStatusPending when cloning is still pending
	ApplicationCloneStatusPending ApplicationCloneStatusType = "Pending"
	// ApplicationCloneStatusInProgress cloning in progress
	ApplicationCloneStatusInProgress ApplicationCloneStatusType = "InProgress"
	// ApplicationCloneStatusFailed when cloning has failed
	ApplicationCloneStatusFailed ApplicationCloneStatusType = "Failed"
	// ApplicationCloneStatusSuccess when cloning was a success
	ApplicationCloneStatusSuccess ApplicationCloneStatusType = "Success"
	// ApplicationCloneStatusPartialSuccess when cloning was only partially successful
	ApplicationCloneStatusPartialSuccess ApplicationCloneStatusType = "PartialSuccess"
)

// ApplicationCloneStageType defines the stage of the cloning process
type ApplicationCloneStageType string

const (
	// ApplicationCloneStageInitial when the cloning was started
	ApplicationCloneStageInitial ApplicationCloneStageType = ""
	// ApplicationCloneStagePreExecRule stage when pre-exec rules are being executed
	ApplicationCloneStagePreExecRule ApplicationCloneStageType = "PreExecRule"
	// ApplicationCloneStagePostExecRule stage when post-exec rules are being executed
	ApplicationCloneStagePostExecRule ApplicationCloneStageType = "PostExecRule"
	// ApplicationCloneStageVolumeClone stage where the volumes are being cloned
	ApplicationCloneStageVolumeClone ApplicationCloneStageType = "VolumeClone"
	// ApplicationCloneStageApplicationClone stage when applications are being cloned
	ApplicationCloneStageApplicationClone ApplicationCloneStageType = "ApplicationClone"
	// ApplicationCloneStageDone stage when the cloning is done
	ApplicationCloneStageDone ApplicationCloneStageType = "Done"
)

// ApplicationCloneReplacePolicyType defines the policy for objects that might already exist in
// the destination namespace, should they be replaced, deleted, retained or overwritten
type ApplicationCloneReplacePolicyType string

const (
	// ApplicationCloneReplacePolicyDelete will delete any conflicts in objects in the
	// destination namespace
	ApplicationCloneReplacePolicyDelete ApplicationCloneReplacePolicyType = "Delete"
	// ApplicationCloneReplacePolicyRetain will retain any conflicts and not change/clone
	ApplicationCloneReplacePolicyRetain ApplicationCloneReplacePolicyType = "Retain"
	// ApplicationCloneReplacePolicyFail will trigger a clone failure on conflicts
	ApplicationCloneReplacePolicyFail ApplicationCloneReplacePolicyType = "Fail"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ApplicationCloneList is a list of ApplicationClones
type ApplicationCloneList struct {
	meta.TypeMeta `json:",inline"`
	meta.ListMeta `json:"metadata,omitempty"`
	Items         []ApplicationClone `json:"items"`
}
