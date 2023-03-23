package v1alpha1

import (
	storkapi "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	// ResourceExportResourceName is name for the ResourceExport resource.
	ResourceExportResourceName = "resourceexport"
	// ResourceExportResourcePlural is the name for list of ResourceExport resources.
	ResourceExportResourcePlural = "resourceexports"
	// ResourceExportNFS resource export provided by nfs path
	ResourceExportNFS ResourceExportType = "nfs"
)

// ResourceExportType defines a method of achieving Resource transfer.
type ResourceExportType string

// ResourceExportStatus defines a status of ResourceExport.
type ResourceExportStatus string

// ResourceExportStage is the stage of the ResourceExport
type ResourceExportStage string

// ResourceRestoreStatus defines the status of Resource after applying the spec during restore.
type ResourceRestoreStatus string

// ObjectInfo contains info about an object being backed up or restored
type ObjectInfo struct {
	Name                    string `json:"name"`
	Namespace               string `json:"namespace"`
	metav1.GroupVersionKind `json:",inline"`
}

// ResourceRestoreResourceInfo is the info for the restore of a resource
type ResourceRestoreResourceInfo struct {
	ObjectInfo `json:",inline"`
	Status     ResourceRestoreStatus `json:"status"`
	Reason     string                `json:"reason"`
}

const (
	// ResourceRestoreStatusFailed Restore Failed
	ResourceRestoreStatusFailed ResourceRestoreStatus = "Failed"
	// ResourceRestoreStatusRetained Restore Retained
	ResourceRestoreStatusRetained ResourceRestoreStatus = "Retained"
	// ResourceRestoreStatusSuccessful Restore Successful
	ResourceRestoreStatusSuccessful ResourceRestoreStatus = "Successful"
	// ResourceRestoreStatusInProgress Restore InProgress
	ResourceRestoreStatusInProgress ResourceRestoreStatus = "InProgress"
)

const (
	// ResourceExportStatusInitial is the initial status of ResourceExport. It indicates
	// that a volume export request has been received.
	ResourceExportStatusInitial ResourceExportStatus = "Initial"
	// ResourceExportStatusPending when Resource export is pending and not started yet.
	ResourceExportStatusPending ResourceExportStatus = "Pending"
	// ResourceExportStatusInProgress when Resource is being transferred.
	ResourceExportStatusInProgress ResourceExportStatus = "InProgress"
	// ResourceExportStatusFailed when Resource transfer is failed.
	ResourceExportStatusFailed ResourceExportStatus = "Failed"
	// ResourceExportStatusSuccessful when Resource has been transferred.
	ResourceExportStatusSuccessful ResourceExportStatus = "Successful"
)

const (
	// ResourceExportBackup backup op for resource upload
	ResourceExportBackup ResourceExportType = "nfs"
)

const (
	// ResourceExportStageInitial is the initial stage for ResourceExport
	ResourceExportStageInitial ResourceExportStage = "Initial"
	// ResourceExportStageInProgress is the InProgress stage for ResourceExport
	ResourceExportStageInProgress ResourceExportStage = "InProgress"
	// ResourceExportStageFailed is the Failed stage for ResourceExport
	ResourceExportStageFailed ResourceExportStage = "Failed"
	// ResourceExportStageSuccessful is the Successful stage for ResourceExport
	ResourceExportStageSuccessful ResourceExportStage = "Successful"
	// ResourceExportStageFinal is the Final stage for ResourceExport
	ResourceExportStageFinal ResourceExportStage = "Final"
)

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ResourceExport defines a spec for restoring resources to NFS target
type ResourceExport struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              ResourceExportSpec `json:"spec"`
	// Status Overall status
	Status ResourceStatus `json:"status,omitempty"`
	// RestoreCompleteList - restore complete volumeInfo
	RestoreCompleteList []*storkapi.ApplicationRestoreVolumeInfo `json:"restoreCompleteList,omitempty"`
}

// ResourceExportSpec configuration parameters for ResourceExport
type ResourceExportSpec struct {
	// Type - Backup or Restore
	Type ResourceExportType `json:"type,omitempty"`
	// TriggeredFrom is to know which module is created the resourceExport CR.
	// The intention is to know from where to get the nfs executor image
	TriggeredFrom   string `json:"triggerFrom,omitempty"`
	TriggeredFromNs string `json:"triggerFromNs,omitempty"`
	// Source here is applicationBackup CR for backup
	Source ResourceExportObjectReference `json:"source,omitempty"`
	// Destination is the ref to BL CR
	Destination ResourceExportObjectReference `json:"destination,omitempty"`
}

// ResourceStatus overall resource backup/restore progress
type ResourceStatus struct {
	// Status status of resource export
	Status ResourceExportStatus `json:"status,omitempty"`
	// Reason status reason
	Reason string `json:"reason,omitempty"`
	// TransferID job transfer ID
	TransferID string `json:"transferID,omitempty"`
	// Stage resource export stage
	Stage ResourceExportStage `json:"stage,omitempty"`
	// Resources status of each resource being restore
	Resources []*ResourceRestoreResourceInfo `json:"resources"`
}

// ResourceExportObjectReference contains enough information to let you inspect the referred object.
type ResourceExportObjectReference struct {
	// API version of the referent.
	APIVersion string `json:"apiVersion,omitempty"`
	// Kind of the referent.
	// More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds
	Kind string `json:"kind,omitempty"`
	// Namespace of the referent.
	// More info: https://kubernetes.io/docs/concepts/overview/working-with-objects/namespaces/
	Namespace string `json:"namespace,omitempty"`
	// Name of the referent.
	// More info: https://kubernetes.io/docs/concepts/overview/working-with-objects/names/#names
	Name string `json:"name,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ResourceExportList is a list of ResourceExport resources.
type ResourceExportList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metaResource,omitempty"`

	Items []ResourceExport `json:"items"`
}
