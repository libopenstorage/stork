package v1alpha1

import (
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type (
	// LabelSelectorOperator is the set of operators that can be used in a selector requirement.
	LabelSelectorOperator string
	// ActionApprovalState is the enum for approval states that an object can take for it's actions
	ActionApprovalState string
)

const (
	// AutopilotRuleResourceName is the name of the singular AutopilotRule objects
	AutopilotRuleResourceName = "autopilotrule"
	// AutopilotRuleObjectResourceName is the name of the singular AutopilotObject objects
	AutopilotRuleObjectResourceName = "autopilotruleobject"
	// AutopilotRuleResourceShortName is the short name for AutopilotRule objects
	AutopilotRuleResourceShortName = "ar"
	// AutopilotRuleObjectResourceShortName is the short name for AutopilotRuleObject objects
	AutopilotRuleObjectResourceShortName = "aro"

	// AutopilotRuleResourcePlural is the name of the plural AutopilotRule objects
	AutopilotRuleResourcePlural = "autopilotrules"
	// AutopilotRuleObjectResourcePlural is the name of the plural AutopilotRuleObject objects
	AutopilotRuleObjectResourcePlural = "autopilotruleobjects"

	// LabelSelectorOpIn is operator where the key must have one of the values
	LabelSelectorOpIn LabelSelectorOperator = "In"
	// LabelSelectorOpNotIn is operator where the key must not have any of the values
	LabelSelectorOpNotIn LabelSelectorOperator = "NotIn"
	// LabelSelectorOpExists is operator where the key must exist
	LabelSelectorOpExists LabelSelectorOperator = "Exists"
	// LabelSelectorOpDoesNotExist is operator where the key must not exist
	LabelSelectorOpDoesNotExist LabelSelectorOperator = "DoesNotExist"
	// LabelSelectorOpGt is operator where the key must be greater than the values
	LabelSelectorOpGt LabelSelectorOperator = "Gt"
	// LabelSelectorOpGtEq is operator where the key must be greater than or equal to the values
	LabelSelectorOpGtEq LabelSelectorOperator = "GtEq"
	// LabelSelectorOpLt is operator where the key must be less than the values
	LabelSelectorOpLt LabelSelectorOperator = "Lt"
	// LabelSelectorOpLtEq is operator where the key must be less than or equal to the values
	LabelSelectorOpLtEq LabelSelectorOperator = "LtEq"
	// LabelSelectorOpNotInRange will compare if the value is not in the range given by first 2 values
	LabelSelectorOpNotInRange LabelSelectorOperator = "NotInRange"
	// LabelSelectorOpInRange will compare if the value is in the range given by first 2 values
	LabelSelectorOpInRange LabelSelectorOperator = "InRange"
	// ApprovalStatePending means the action has not been yet approved
	ApprovalStatePending ActionApprovalState = "pending"
	// ApprovalStateApproved means the action has been approved
	ApprovalStateApproved ActionApprovalState = "approved"
	// ApprovalStateDeclined  means the action has been declined
	ApprovalStateDeclined ActionApprovalState = "declined"
)

// LabelSelectorRequirement is a selector that contains values, a key, and an operator that
// relates the key and values.
type LabelSelectorRequirement struct {
	// key is the label key that the selector applies to.
	// +patchMergeKey=key
	// +patchStrategy=merge
	Key string `json:"key,omitempty"`
	// KeyAlias is an alias known to autopilot that can be used instead of supplying the key
	// To view supported aliases, refer to documentation at https://docs.portworx.com/portworx-install-with-kubernetes/autopilot/
	KeyAlias string `json:"keyAlias,omitempty"`
	// operator represents a key's relationship to a set of values.
	// Valid operators are In, NotIn, Exists, DoesNotExist, Lt and Gt.
	Operator LabelSelectorOperator `json:"operator"`
	// values is an array of string values. If the operator is In or NotIn,
	// the values array must be non-empty. If the operator is Exists or DoesNotExist,
	// the values array must be empty. This array is replaced during a strategic
	// merge patch.
	// +optional
	Values []string `json:"values"`
}

// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// AutopilotRule represents pairing with other clusters
type AutopilotRule struct {
	meta.TypeMeta   `json:",inline"`
	meta.ObjectMeta `json:"metadata,omitempty"`
	Spec            AutopilotRuleSpec `json:"spec"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// AutopilotRuleList is a list of AutopilotRules in Kubernetes
type AutopilotRuleList struct {
	meta.TypeMeta `json:",inline"`
	meta.ListMeta `json:"metadata,omitempty"`

	Items []AutopilotRule `json:"items"`
}

// AutopilotRuleSpec is the spec to create the cluster pair
type AutopilotRuleSpec struct {
	// Weight defines the weight of the rule which allows to break the tie with other conflicting policies. A rule with
	// higher weight wins over one with lower weight.
	// (optional)
	Weight int64 `json:"weight,omitempty"`
	// PollInterval defined the interval in seconds at which the conditions for the
	// rule are queried from the monitoring provider
	PollInterval int64 `json:"pollInterval,omitempty"`
	// Enforcement specifies the enforcement type for rule. Can take values: required or preferred.
	// (optional)
	Enforcement EnforcementType `json:"enforcement,omitempty"`
	// Selector allows to select the objects that are relevant with this rule using label selection
	Selector RuleObjectSelector `json:"selector"`
	// NamespaceSelector allows to select namespaces affecting the rule by labels:w
	NamespaceSelector RuleObjectSelector `json:"namespaceSelector"`
	// Conditions are the conditions to check on the rule objects
	Conditions RuleConditions `json:"conditions"`
	// Actions are the actions to run for the rule when the conditions are met
	Actions []*RuleAction `json:"actions"`
	// ActionsCoolDownPeriod is the duration in seconds for which autopilot will not
	// re-trigger any actions once they have been executed.
	ActionsCoolDownPeriod int64 `json:"actionsCoolDownPeriod,omitempty"`
}

// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// AutopilotRuleObject represents a particular object that is being monitored by autopilot.
type AutopilotRuleObject struct {
	meta.TypeMeta   `json:",inline"`
	meta.ObjectMeta `json:"metadata,omitempty"`
	// Spec is the spec of the autopilot rule object
	Spec AutopilotRuleObjectSpec `json:"spec,omitempty"`
	// Status is the status of an object monitored by an autopilot rule
	Status AutopilotRuleObjectStatus `json:"status,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// AutopilotRuleObjectList is a list of AutopilotRule objects in Kubernetes
type AutopilotRuleObjectList struct {
	meta.TypeMeta `json:",inline"`
	meta.ListMeta `json:"metadata,omitempty"`

	Items []AutopilotRuleObject `json:"items"`
}

// AutopilotRuleObjectSpec represents the spec of the autopilot object
type AutopilotRuleObjectSpec struct {
	// ActionApprovals allows users to set the approval states for actions pending for the object
	ActionApprovals []*AutopilotActionApproval `json:"actionApprovals,omitempty"`
}

// AutopilotActionApproval stores the state related to approval of an action
type AutopilotActionApproval struct {
	// Annotations are annotation for the action approval
	Annotations map[string]string `json:"annotations,omitempty"`
	// State is the current approval state of the action approval
	State ActionApprovalState `json:"state,omitempty"`
	// Action is the action that needs/needed approval
	Action AutopilotActionPreview `json:"action,omitempty"`
}

// AutopilotActionPreview is a preview of an action and the expected result of it before it gets executed
type AutopilotActionPreview struct {
	// ObjectMetadata is the metadata for the object on which the action will be performed
	ObjectMetadata ActionPreviewObjectMetadata `json:"objectMetadata,omitempty"`
	RuleAction
	// ExpectedResult is a user friendly description of the outcome of executing the action
	ExpectedResult string `json:"expectedResult,omitempty"`
}

// ActionPreviewObjectMetadata is metadata for an object inside an action preview
type ActionPreviewObjectMetadata struct {
	meta.ObjectMeta
	// Type is the object type
	Type string `json:"type,omitempty"`
}

// AutopilotRuleObjectStatus represents the status of an autopilot object
type AutopilotRuleObjectStatus struct {
	// Items contains list of recent status items for an autopilot object
	Items []*AutopilotRuleObjectStatusItem `json:"items,omitempty"`
}

// AutopilotRuleObjectStatusItem is a single status item of an autopilot object
type AutopilotRuleObjectStatusItem struct {
	// LastProcessTimestamp was the last time the object was processed
	LastProcessTimestamp meta.Time `json:"lastProcessTimestamp"`
	// State of the object
	State RuleState `json:"state"`
	// Message is the user friendly status
	Message string `json:"message"`
	// TODO add NextProcessTimestamp
}

// RuleState is the type for the state of a rule
type RuleState string

const (
	// RuleStateInit is the initial state of the rule where monitorign has not yet begin
	RuleStateInit RuleState = "Initializing"
	// RuleStateNormal is when the rule is being monitored and is in normal state
	RuleStateNormal RuleState = "Normal"
	// RuleStateTriggered is when the rule has it's conditions met
	RuleStateTriggered RuleState = "Triggered"
	// RuleStateActionAwaitingApproval is when a rule is waiting approval from a user to proceed with it's actions
	RuleStateActionAwaitingApproval RuleState = "ActionAwaitingApproval"
	// RuleStateActiveActionsPending is when the rule has it's conditions met but the actions are
	// not being performed yet.
	RuleStateActiveActionsPending RuleState = "ActiveActionsPending"
	// RuleStateActiveActionsTaken is when the rule has it's actions already taken
	// but still hasn't moved out of active status
	RuleStateActiveActionsTaken RuleState = "ActiveActionsTaken"
	// RuleStateActionsDeclined is when action was intentionally declined by autopilot
	RuleStateActionsDeclined RuleState = "ActionsDeclined"
	// RuleStateActiveActionsInProgress is when the rule is active and has met its
	// conditions and there is an on going action on the object.
	RuleStateActiveActionsInProgress RuleState = "ActiveActionsInProgress"
)

// RuleStatusObjectKey is a type to use as key for rule object statuses
type RuleStatusObjectKey string

// RuleObjectSelector defines an object for the rule
type RuleObjectSelector struct {
	// LabelSelector selects the rule objects
	meta.LabelSelector
}

// RuleConditions defines the conditions for the rule
type RuleConditions struct {
	// Expressions are the actual rule conditions
	Expressions []*LabelSelectorRequirement `json:"expressions,omitempty"`
	// For is the duration in seconds for which the conditions must hold true
	For int64 `json:"for,omitempty"`
	// RequiredMatches is the number of expressions above that should match for the RuleCondition to be considered
	// as triggered. Default is 0, which means all expressions need to match
	RequiredMatches uint64 `json:"requiredMatches,omitempty"`
	// Type is the condition type
	// If not provided, the controller for the CRD will pick the default type
	Type AutopilotRuleConditionType `json:"type,omitempty"`
	// Provider is an optional provider for the above condition type
	// If not provided, the controller for the CRD will pick the default provider
	Provider string `json:"provider,omitempty"`
}

// RuleAction defines an action for the rule
type RuleAction struct {
	// ObjectName is the name of the rule
	Name string `json:"name"`
	// Params are the opaque paramters that will be used for the above action
	Params map[string]string `json:"params"`
}

// AutopilotRuleStatusType is the type for rule statuses
type AutopilotRuleStatusType string

const (
	// AutopilotRuleConditonMet is for when the conditions in rule are met
	AutopilotRuleConditonMet AutopilotRuleStatusType = "ConditionMet"
	// AutopilotRuleActionFailed is when an action for a rule has failed
	AutopilotRuleActionFailed AutopilotRuleStatusType = "ActionFailed"
	// AutopilotRuleActionTriggered is when an action for a rule has triggerred
	AutopilotRuleActionTriggered AutopilotRuleStatusType = "ActionTriggered"
	// AutopilotRuleActionSuccessful is when an action for a rule is successful
	AutopilotRuleActionSuccessful AutopilotRuleStatusType = "ActionSuccessful"
)

// AutopilotRuleConditionType defines the type of a condition in a rule
type AutopilotRuleConditionType string

const (
	// RuleConditionMetrics is a monitoring type of condition in a rule
	RuleConditionMetrics AutopilotRuleConditionType = "monitoring"
)

func init() {
	SchemeBuilder.Register(&AutopilotRule{}, &AutopilotRuleObject{}, &AutopilotRuleList{}, &AutopilotRuleObjectList{})
}
