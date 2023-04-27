package testrail

import "strconv"

// Plan represents a Plan
type Plan struct {
	AssignedToID       int     `json:"assignedto_id"`
	BlockedCount       int     `json:"blocked_count"`
	CompletedOn        int     `json:"completed_on"`
	CreatedBy          int     `json:"created_by"`
	CreatedOn          int     `json:"created_on"`
	Description        string  `json:"description"`
	Entries            []Entry `json:"entries"`
	FailedCount        int     `json:"failed_count"`
	ID                 int     `json:"id"`
	IsCompleted        bool    `json:"is_completed"`
	MilestoneID        int     `json:"milestone_id"`
	Name               string  `json:"name"`
	PassedCount        int     `json:"passed_count"`
	ProjectID          int     `json:"project_id"`
	RetestCount        int     `json:"retest_count"`
	UntestedCount      int     `json:"untested_count"`
	URL                string  `json:"url"`
	CustomStatus1Count int     `json:"custom_status1_count"`
	CustomStatus2Count int     `json:"custom_status2_count"`
	CustomStatus3Count int     `json:"custom_status3_count"`
	CustomStatus4Count int     `json:"custom_status4_count"`
	CustomStatus5Count int     `json:"custom_status5_count"`
	CustomStatus6Count int     `json:"custom_status6_count"`
	CustomStatus7Count int     `json:"custom_status7_count"`
}

// Entry represents the entry a Plan can have
type Entry struct {
	ID      string `json:"id"`
	Name    string `json:"name"`
	Runs    []Run  `json:"runs"`
	SuiteID int    `json:"suite_id"`
}

// RequestFilterForPlan represents the filters
// usable to get the plan
type RequestFilterForPlan struct {
	CreatedAfter  string `json:"created_after"`
	CreatedBefore string `json:"created_before"`
	CreatedBy     []int  `json:"created_by"`
	IsCompleted   *bool  `json:"is_completed"`
	Limit         *int   `json:"limit"`
	Offset        *int   `json:"offset"`
	MilestoneID   []int  `json:"milestone_id"`
}

// SendablePlan represents a Plan
// that can be created or updated via the api
type SendablePlan struct {
	Name        string          `json:"name,omitempty"`
	Description string          `json:"description,omitempty"`
	MilestoneID int             `json:"milestone_id,omitempty"`
	Entries     []SendableEntry `json:"entries,omitempty"`
}

// SendableEntry represents an Entry
// that can be created or updated via the api
type SendableEntry struct {
	SuiteID      int           `json:"suite_id"`
	Name         string        `json:"name,omitempty"`
	AssignedtoID int           `json:"assignedto_id,omitempty"`
	IncludeAll   bool          `json:"include_all"`
	CaseIDs      []int         `json:"case_ids,omitempty"`
	ConfigIDs    []int         `json:"config_ids,omitempty"`
	Runs         []SendableRun `json:"runs,omitempty"`
}

// GetPlan returns the existing plan planID
func (c *Client) GetPlan(planID int) (Plan, error) {
	returnPlan := Plan{}
	err := c.sendRequest("GET", "get_plan/"+strconv.Itoa(planID), nil, &returnPlan)
	return returnPlan, err
}

// GetPlans returns the list of plans for the project projectID
// validating the filters
func (c *Client) GetPlans(projectID int, filters ...RequestFilterForPlan) ([]Plan, error) {
	uri := "get_plans/" + strconv.Itoa(projectID)
	if len(filters) > 0 {
		uri = applyFiltersForPlan(uri, filters[0])
	}

	var err error
	returnPlans := []Plan{}
	if c.useBetaApi {
		err = c.sendRequestBeta("GET", uri, nil, &returnPlans, "plans")
	} else {
		err = c.sendRequest("GET", uri, nil, &returnPlans)
	}

	return returnPlans, err
}

// AddPlan creates a new plan on project projectID and returns it
func (c *Client) AddPlan(projectID int, newPlan SendablePlan) (Plan, error) {
	createdPlan := Plan{}
	err := c.sendRequest("POST", "add_plan/"+strconv.Itoa(projectID), newPlan, &createdPlan)
	return createdPlan, err
}

// AddPlanEntry creates a new entry on plan planID and returns it
func (c *Client) AddPlanEntry(planID int, newEntry SendableEntry) (Entry, error) {
	createdEntry := Entry{}
	err := c.sendRequest("POST", "add_plan_entry/"+strconv.Itoa(planID), newEntry, &createdEntry)
	return createdEntry, err
}

// UpdatePlan updates the existing plan planID and returns it
func (c *Client) UpdatePlan(planID int, updates SendablePlan) (Plan, error) {
	updatedPlan := Plan{}
	err := c.sendRequest("POST", "update_plan/"+strconv.Itoa(planID), updates, &updatedPlan)
	return updatedPlan, err
}

// UpdatePlanEntry updates the entry entryID on plan planID and returns it
func (c *Client) UpdatePlanEntry(planID int, entryID string, updates SendableEntry) (Entry, error) {
	uri := "update_plan_entry/" + strconv.Itoa(planID) + "/" + entryID
	updatedEntry := Entry{}
	err := c.sendRequest("POST", uri, updates, &updatedEntry)
	return updatedEntry, err
}

// ClosePlan closes the plan planID and returns it
func (c *Client) ClosePlan(planID int) (Plan, error) {
	deletedPlan := Plan{}
	err := c.sendRequest("POST", "close_plan/"+strconv.Itoa(planID), nil, &deletedPlan)
	return deletedPlan, err
}

// DeletePlan deletes the plan planID
func (c *Client) DeletePlan(planID int) error {
	return c.sendRequest("POST", "delete_plan/"+strconv.Itoa(planID), nil, nil)
}

// DeletePlanEntry delete the entry entryID on plan planID
func (c *Client) DeletePlanEntry(planID int, entryID string) error {
	uri := "delete_plan_entry/" + strconv.Itoa(planID) + "/" + entryID
	return c.sendRequest("POST", uri, nil, nil)
}

// applyFiltersForPlan go through each possible filters and create the
// uri for the wanted ones
func applyFiltersForPlan(uri string, filters RequestFilterForPlan) string {
	if filters.CreatedAfter != "" {
		uri = uri + "&created_after=" + filters.CreatedAfter
	}
	if filters.CreatedBefore != "" {
		uri = uri + "&created_before=" + filters.CreatedBefore
	}
	if len(filters.CreatedBy) != 0 {
		uri = applySpecificFilter(uri, "created_by", filters.CreatedBy)
	}
	if len(filters.MilestoneID) != 0 {
		uri = applySpecificFilter(uri, "milestone_id", filters.MilestoneID)
	}
	if filters.IsCompleted != nil {
		uri = uri + "&is_completed=" + btoitos(*filters.IsCompleted)
	}
	if filters.Limit != nil {
		uri = uri + "&limit=" + strconv.Itoa(*filters.Limit)
	}
	if filters.Offset != nil {
		uri = uri + "&offset=" + strconv.Itoa(*filters.Offset)
	}

	return uri
}
