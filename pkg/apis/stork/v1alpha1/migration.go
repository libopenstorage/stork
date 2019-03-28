package v1alpha1

import (
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	// MigrationResourceName is name for "migration" resource
	MigrationResourceName = "migration"
	// MigrationResourcePlural is plural for "migration" resource
	MigrationResourcePlural = "migrations"
)

// MigrationSpec is the spec used to migrate apps between clusterpairs
type MigrationSpec struct {
	ClusterPair       string            `json:"clusterPair"`
	Namespaces        []string          `json:"namespaces"`
	IncludeResources  *bool             `json:"includeResources"`
	IncludeVolumes    *bool             `json:"includeVolumes"`
	StartApplications *bool             `json:"startApplications"`
	Selectors         map[string]string `json:"selectors"`
	PreExecRule       string            `json:"preExecRule"`
	PostExecRule      string            `json:"postExecRule"`
}

// MigrationStatus is the status of a migration operation
type MigrationStatus struct {
	Stage           MigrationStageType  `json:"stage"`
	Status          MigrationStatusType `json:"status"`
	Resources       []*ResourceInfo     `json:"resources"`
	Volumes         []*VolumeInfo       `json:"volumes"`
	FinishTimestamp meta.Time           `json:"finishTimestamp"`
}

// ResourceInfo is the info for the migration of a resource
type ResourceInfo struct {
	Name                  string `json:"name"`
	Namespace             string `json:"namespace"`
	meta.GroupVersionKind `json:",inline"`
	Status                MigrationStatusType `json:"status"`
	Reason                string              `json:"reason"`
}

// VolumeInfo is the info for the migration of a volume
type VolumeInfo struct {
	PersistentVolumeClaim string              `json:"persistentVolumeClaim"`
	Namespace             string              `json:"namespace"`
	Volume                string              `json:"volume"`
	Status                MigrationStatusType `json:"status"`
	Reason                string              `json:"reason"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// Migration represents migration status
type Migration struct {
	meta.TypeMeta   `json:",inline"`
	meta.ObjectMeta `json:"metadata,omitempty"`
	Spec            MigrationSpec   `json:"spec"`
	Status          MigrationStatus `json:"status"`
}

// MigrationStatusType is the status of the migration
type MigrationStatusType string

const (
	// MigrationStatusInitial is the initial state when migration is created
	MigrationStatusInitial MigrationStatusType = ""
	// MigrationStatusPending for when migration is still pending
	MigrationStatusPending MigrationStatusType = "Pending"
	// MigrationStatusCaptured for when migration specs have been captured
	MigrationStatusCaptured MigrationStatusType = "Captured"
	// MigrationStatusInProgress for when migration is in progress
	MigrationStatusInProgress MigrationStatusType = "InProgress"
	// MigrationStatusFailed for when migration has failed
	MigrationStatusFailed MigrationStatusType = "Failed"
	// MigrationStatusPartialSuccess for when migration was partially successful
	MigrationStatusPartialSuccess MigrationStatusType = "PartialSuccess"
	// MigrationStatusSuccessful for when migration has completed successfully
	MigrationStatusSuccessful MigrationStatusType = "Successful"
)

// MigrationStageType is the stage of the migration
type MigrationStageType string

const (
	// MigrationStageInitial for when migration is created
	MigrationStageInitial MigrationStageType = ""
	// MigrationStagePreExecRule for when the PreExecRule is being executed
	MigrationStagePreExecRule MigrationStageType = "PreExecRule"
	// MigrationStagePostExecRule for when the PostExecRule is being executed
	MigrationStagePostExecRule MigrationStageType = "PostExecRule"
	// MigrationStageVolumes for when volumes are being migrated
	MigrationStageVolumes MigrationStageType = "Volumes"
	// MigrationStageApplications for when applications are being migrated
	MigrationStageApplications MigrationStageType = "Applications"
	// MigrationStageFinal is the final stage for migration
	MigrationStageFinal MigrationStageType = "Final"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// MigrationList is a list of Migrations
type MigrationList struct {
	meta.TypeMeta `json:",inline"`
	meta.ListMeta `json:"metadata,omitempty"`

	Items []Migration `json:"items"`
}
