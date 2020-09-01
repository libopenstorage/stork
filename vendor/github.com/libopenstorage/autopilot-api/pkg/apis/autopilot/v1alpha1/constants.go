package v1alpha1

// EnforcementType Defines the types of enforcement on the given policy
type EnforcementType string

const (
	// ApprovalRequired specifies that all actions for the rule will require an approval
	ApprovalRequired EnforcementType = "approvalRequired"
)
