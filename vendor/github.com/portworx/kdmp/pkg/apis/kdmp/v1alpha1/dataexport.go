package v1alpha1

import (
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
	// DataExportRestic means that data will be backed up to or restored from a restic
	// repository.
	DataExportRestic DataExportType = "restic"
	// DataExportKopia means that data will be backed up to or restored from a kopia
	// repository.
	DataExportKopia DataExportType = "kopia"
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
	// DataExportStageSnapshotRestore if a driver supports this stage, it means a PVC is
	// creating from a snapshot.
	DataExportStageSnapshotRestore DataExportStage = "SnapshotRestore"
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
	Type                 DataExportType            `json:"type,omitempty"`
	ClusterPair          string                    `json:"clusterPair,omitempty"`
	SnapshotStorageClass string                    `json:"snapshotStorageClass,omitempty"`
	Source               DataExportObjectReference `json:"source,omitempty"`
	Destination          DataExportObjectReference `json:"destination,omitempty"`
}

// DataExportObjectReference contains enough information to let you inspect the referred object.
type DataExportObjectReference struct {
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

// ExportStatus indicates a current state of the data transfer.
type ExportStatus struct {
	Stage                DataExportStage  `json:"stage,omitempty"`
	Status               DataExportStatus `json:"status,omitempty"`
	Reason               string           `json:"reason,omitempty"`
	SnapshotID           string           `json:"snapshotID,omitempty"`
	SnapshotNamespace    string           `json:"snapshotNamespace,omitempty"`
	SnapshotPVCName      string           `json:"snapshotPVCName,omitempty"`
	SnapshotPVCNamespace string           `json:"snapshotPVCNamespace,omitempty"`
	TransferID           string           `json:"transferID,omitempty"`
	ProgressPercentage   int              `json:"progressPercentage,omitempty"`
	Size                 uint64           `json:"size,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// DataExportList is a list of DataExport resources.
type DataExportList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []DataExport `json:"items"`
}
