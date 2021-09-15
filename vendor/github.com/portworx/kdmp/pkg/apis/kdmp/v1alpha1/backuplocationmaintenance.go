package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	// BackupLocationMaintenanceName is name for the BackupLocationMaintenance resource.
	BackupLocationMaintenanceName = "backupLocationMaintenance"
	// BackupLocationMaintenancePlural is the name for list of BackupLocationMaintenance resources.
	BackupLocationMaintenancePlural = "backupLocationMaintenances"
)

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// BackupLocationMaintenance is the place holder for repository maintenance status
// that belongs to the given backuplocation.
type BackupLocationMaintenance struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              BackupLocationMaintenanceSpec   `json:"spec"`
	Status            BackupLocationMaintenanceStatus `json:"status,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// BackupLocationMaintenanceList is a list of BackupLocationMaintenance resources.
type BackupLocationMaintenanceList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []BackupLocationMaintenance `json:"items"`
}

// BackupLocationMaintenanceSpec defines configuration parameters for BackupLocationMaintenanceSpec.
type BackupLocationMaintenanceSpec struct {
	// backuplocation name, this can be CR name or object name
	BackuplocationName string
	// secret name that holds the crdentials of the backuplocation.
	CredSecret string
}

// BackupLocationMaintenanceStatus defines  the status for BackupLocationMaintenance
type BackupLocationMaintenanceStatus struct {
	RepoStatus []*RepoMaintenanceStatus
}

// RepoMaintenanceStatusType is the status of the repository maintenance run.
type RepoMaintenanceStatusType string

const (
	// RepoMaintenanceStatusSuccess - Success status
	RepoMaintenanceStatusSuccess RepoMaintenanceStatusType = "Successful"
	// RepoMaintenanceStatusFailed - Fail status
	RepoMaintenanceStatusFailed RepoMaintenanceStatusType = "Failed"
)

// RepoMaintenanceStatus defines status for RepoMaintenanceStatus.
type RepoMaintenanceStatus struct {
	// RepoName - name of the repository
	RepoName string `json:"repoName"`
	// LastRunTimestamp - last maintenance run timestamp
	LastRunTimestamp metav1.Time `json:"lastRunTimestamp"`
	// Status - last maintenance run status
	Status RepoMaintenanceStatusType `json:"status"`
}
