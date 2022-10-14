package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	// DataExportResourceName is name for the DataExport resource.
	DataExportResourceName = "dataexport"
	// DataExportResourcePlural is the name for list of DataExport resources.
	DataExportResourcePlural = "dataexports"
)

// DataExportType defines a method of achieving data transfer.
type DataExportType string

const (
	// DataExportRsync means that data will be copied between two PVCs directly.
	// Rsync supports both local and remote file copy.
	DataExportRsync DataExportType = "rsync"
)

// DataExportStatus defines a status of DataExport.
type DataExportStatus string

const (
	// DataExportStatusInitial is the initial status of DataExport. It indicates
	// that a volume export request has been received.
	DataExportStatusInitial DataExportStatus = "Initial"
	// DataExportStatusPending when data export is pending and not started yet.
	DataExportStatusPending DataExportStatus = "Pending"
	// DataExportStatusInProgress when data is being transferred.
	DataExportStatusInProgress DataExportStatus = "InProgress"
	// DataExportStatusFailed when data transfer is failed.
	DataExportStatusFailed DataExportStatus = "Failed"
	// DataExportStatusSuccessful when data has been transferred.
	DataExportStatusSuccessful DataExportStatus = "Successful"
)

// DataExportStage defines different stages for DataExport when its Status changes
// from Initial to Failed/Successful.
type DataExportStage string

const (
	// DataExportStageInitial is the starting point for DataExport.
	DataExportStageInitial DataExportStage = "Initial"
	// DataExportStageSnapshotScheduled if a driver support this stage, it means a snapshot
	// is being taken of the source PVC which will be used to transfer data with rsync.
	DataExportStageSnapshotScheduled DataExportStage = "SnapshotScheduled"
	// DataExportStageSnapshotInProgress if a driver supports this stage, it means a data
	// is processing.
	DataExportStageSnapshotInProgress DataExportStage = "SnapshotInProgress"
	// DataExportStageTransferScheduled when the rsync daemon and pod are currently being
	// scheduled by Kubernetes.
	DataExportStageTransferScheduled DataExportStage = "TransferScheduled"
	// DataExportStageTransferInProgress when rsync is in progress and is transferring data
	// between the two PVCs.
	DataExportStageTransferInProgress DataExportStage = "TransferInProgress"
	// DataExportStageFinal when rsync is completed.
	DataExportStageFinal DataExportStage = "Final"
)

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// DataExport defines a spec for importing of application data from a non Portworx PVC
// (source) to a PVC backed by Portworx.
type DataExport struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              DataExportSpec `json:"spec"`
	Status            ExportStatus   `json:"status"`
}

// DataExportSpec defines configuration parameters for DataExport.
type DataExportSpec struct {
	Type        DataExportType        `json:"type,omitempty"`
	ClusterPair string                `json:"clusterPair,omitempty"`
	Source      DataExportSource      `json:"source,omitempty"`
	Destination DataExportDestination `json:"destination,omitempty"`
}

// DataExportSource defines a PVC name and namespace that should be processed.
type DataExportSource struct {
	PersistentVolumeClaim *corev1.PersistentVolumeClaim `json:"persistentVolumeClaim,omitempty"`
}

// DataExportDestination defines a backend for data transfer.
type DataExportDestination struct {
	// PersistentVolumeClaim defines a PVC backend for data transfer. If provided PVC doesn't exist
	// a new one will be created using the spec configuration.
	PersistentVolumeClaim *corev1.PersistentVolumeClaim `json:"persistentVolumeClaim,omitempty"`
}

// ExportStatus indicates a current state of the data transfer.
type ExportStatus struct {
	Stage              DataExportStage  `json:"stage,omitempty"`
	Status             DataExportStatus `json:"status,omitempty"`
	Reason             string           `json:"reason,omitempty"`
	ProgressPercentage int              `json:"progressPercentage,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// DataExportList is a list of DataExport resources.
type DataExportList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []DataExport `json:"items"`
}
