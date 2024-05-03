package v1alpha1

import (
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	// ActionResourceName is name for "Action" resource
	ActionResourceName = "action"
	// ActionResourcePlural is plural for "Action" resource
	ActionResourcePlural = "actions"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ActionList is a list of Actions
type ActionList struct {
	meta.TypeMeta `json:",inline"`
	meta.ListMeta `json:"metadata,omitempty"`
	Items         []Action `json:"items"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// Action represents a task that will be performed once
// It is similar to k8s Job
type Action struct {
	meta.TypeMeta   `json:",inline"`
	meta.ObjectMeta `json:"metadata,omitempty"`
	Spec            ActionSpec   `json:"spec"`
	Status          ActionStatus `json:"status"`
}

// ActionSpec specifies the type of Action
type ActionSpec struct {
	ActionType      ActionType      `json:"actionType"`
	ActionParameter ActionParameter `json:"actionParameter"`
}

// ActionType lists the various actions that can be performed
type ActionType string

const (
	// ActionTypeNearSyncFailover action type to start apps on destination cluster in nearsync DR case
	ActionTypeNearSyncFailover ActionType = "nearsyncFailover"
	// ActionTypeFailover action type to start apps in destination cluster for sync and async DRs
	ActionTypeFailover ActionType = "failover"
	// ActionTypeFailback action type to migrate back to source and start apps in source
	ActionTypeFailback ActionType = "failback"
)

// ActionParameter lists the parameters required to perform the action
type ActionParameter ActionParameterItem

type ActionParameterItem struct {
	FailoverParameter FailoverParameter `json:"failoverParameter,omitempty"`
	FailbackParameter FailbackParameter `json:"failbackParameter,omitempty"`
}

type FailoverParameter struct {
	FailoverNamespaces         []string `json:"failoverNamespaces"`
	MigrationScheduleReference string   `json:"migrationScheduleReference"`
	SkipSourceOperations       *bool    `json:"skipSourceOperations"`
}

type FailbackParameter struct {
	FailbackNamespaces         []string `json:"failbackNamespaces"`
	MigrationScheduleReference string   `json:"migrationScheduleReference"`
}

// ActionStatusType is the current status of the Action
type ActionStatusType string

const (
	// ActionStatusInitial means Action CR has been created
	ActionStatusInitial ActionStatusType = ""
	// ActionStatusScheduled means Action is yet to start
	ActionStatusScheduled ActionStatusType = "Scheduled"
	// ActionStatusInProgress means Action is in progress
	ActionStatusInProgress ActionStatusType = "In-Progress"
	// ActionStatusFailed means that Action has failed
	ActionStatusFailed ActionStatusType = "Failed"
	// ActionStatusSuccessful means Action has completed successfully
	ActionStatusSuccessful ActionStatusType = "Successful"
	// ActionStatusRollbackSuccessful means Rollback has completed successfully
	ActionStatusRollbackSuccessful ActionStatusType = "RollbackSuccessful"
	// ActionStatusRollbackInProgress means Rollback failed
	ActionStatusRollbackFailed ActionStatusType = "RollbackFailed"
)

// ActionStageType is the stage of the action
type ActionStageType string

const (
	// ActionStageInitial for when action is created
	ActionStageInitial ActionStageType = ""
	// ActionStageScaleDownDestination for scaling down apps in destination
	ActionStageScaleDownDestination ActionStageType = "ScaleDownDestination"
	// ActionStageScaleDownSource for scaling down apps in source
	ActionStageScaleDownSource ActionStageType = "ScaleDownSource"
	// ActionStageScaleUpDestination for scaling apps in destination
	ActionStageScaleUpDestination ActionStageType = "ScaleUpDestination"
	// ActionStageScaleUpSource for scaling apps in source
	ActionStageScaleUpSource ActionStageType = "ScaleUpSource"
	// ActionStageLastMileMigration for doing a last migration before failover/failback to ensure data integrity
	ActionStageLastMileMigration ActionStageType = "LastMileMigration"
	// ActionStageWaitAfterScaleDown for waiting after scaling down of apps
	ActionStageWaitAfterScaleDown ActionStageType = "WaitAfterScaleDown"
	// ActionStageFinal is the final stage for action
	ActionStageFinal ActionStageType = "Final"
)

// ActionStatus is the status of action operation
type ActionStatus struct {
	Stage           ActionStageType  `json:"stage"`
	Status          ActionStatusType `json:"status"`
	FinishTimestamp meta.Time        `json:"finishTimestamp"`
	Summary         *ActionSummary   `json:"summary"`
	Reason          string           `json:"reason"`
}

// ActionSummary lists the summary of the action
type ActionSummary ActionSummaryItem

// ActionSummaryItem has summary for each action type
type ActionSummaryItem struct {
	FailoverSummaryItem []*FailoverSummary `json:"failoverSummary,omitempty"`
	FailbackSummaryItem []*FailbackSummary `json:"failbackSummary,omitempty"`
}

type FailoverSummary struct {
	Namespace string           `json:"namespace"`
	Status    ActionStatusType `json:"status"`
	Reason    string           `json:"reason"`
}
type FailbackSummary struct {
	Namespace string           `json:"namespace"`
	Status    ActionStatusType `json:"status"`
	Reason    string           `json:"reason"`
}
