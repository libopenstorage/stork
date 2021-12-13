package testrail

import "strconv"

// Run represents a Run
type Run struct {
	AssignedToID       int    `json:"assignedto_id"`
	BlockedCount       int    `json:"blocked_count"`
	CompletedOn        int    `json:"completed_on"`
	Config             string `json:"config"`
	ConfigIDs          []int  `json:"config_ids"`
	CreatedBy          int    `json:"created_by"`
	CreatedOn          int    `json:"created_on"`
	Description        string `json:"description"`
	EntryID            string `json:"entry_id"`
	EntryIndex         int    `json:"entry_index"`
	FailedCount        int    `json:"failed_count"`
	ID                 int    `json:"id"`
	IncludeAll         bool   `json:"include_all"`
	IsCompleted        bool   `json:"is_completed"`
	MilestoneID        int    `json:"milestone_id"`
	Name               string `json:"name"`
	PassedCount        int    `json:"passed_count"`
	PlanID             int    `json:"plan_id"`
	ProjectID          int    `json:"project_id"`
	RetestCount        int    `json:"retest_count"`
	SuiteID            int    `json:"suite_id"`
	UntestedCount      int    `json:"untested_count"`
	URL                string `json:"url"`
	CustomStatus1Count int    `json:"custom_status1_count"`
	CustomStatus2Count int    `json:"custom_status2_count"`
	CustomStatus3Count int    `json:"custom_status3_count"`
	CustomStatus4Count int    `json:"custom_status4_count"`
	CustomStatus5Count int    `json:"custom_status5_count"`
	CustomStatus6Count int    `json:"custom_status6_count"`
	CustomStatus7Count int    `json:"custom_status7_count"`
}

// RequestFilterForRun represents the filters
// usable to get the run
type RequestFilterForRun struct {
	CreatedAfter  string `json:"created_after,omitempty"`
	CreatedBefore string `json:"created_before,omitempty"`
	CreatedBy     []int  `json:"created_by,omitempty"`
	IsCompleted   *bool  `json:"is_completed,omitempty"`
	Limit         *int   `json:"limit,omitempty"`
	Offset        *int   `json:"offset, omitempty"`
	MilestoneID   []int  `json:"milestone_id,omitempty"`
	SuiteID       []int  `json:"suite_id,omitempty"`
}

// SendableRun represents a Run
// that can be created via the api
type SendableRun struct {
	SuiteID      int    `json:"suite_id"`
	Name         string `json:"name,omitempty"`
	Description  string `json:"description,omitempty"`
	MilestoneID  int    `json:"milestone_id,omitempty"`
	AssignedToID int    `json:"assignedto_id,omitempty"`
	IncludeAll   *bool  `json:"include_all,omitempty"`
	CaseIDs      []int  `json:"case_ids,omitempty"`
}

// UpdatableRun represents a Run
// that can be updated via the api
type UpdatableRun struct {
	Name        string `json:"name,omitempty"`
	Description string `json:"description,omitempty"`
	MilestoneID int    `json:"milestone_id,omitempty"`
	IncludeAll  *bool  `json:"include_all,omitempty"`
	CaseIDs     []int  `json:"case_ids,omitempty"`
}

// GetRun returns the run runID
func (c *Client) GetRun(runID int) (Run, error) {
	returnRun := Run{}
	err := c.sendRequest("GET", "get_run/"+strconv.Itoa(runID), nil, &returnRun)
	return returnRun, err
}

// GetRuns returns the list of runs of projectID
// validating the filters
func (c *Client) GetRuns(projectID int, filters ...RequestFilterForRun) ([]Run, error) {
	uri := "get_runs/" + strconv.Itoa(projectID)
	if len(filters) > 0 {
		uri = applyFiltersForRuns(uri, filters[0])
	}

	returnRun := []Run{}
	var err error
	if c.useBetaApi {
		err = c.sendRequestBeta("GET", uri, nil, &returnRun, "runs")
	} else {
		err = c.sendRequest("GET", uri, nil, &returnRun)
	}
	return returnRun, err
}

// AddRun creates a new run on projectID and returns it
func (c *Client) AddRun(projectID int, newRun SendableRun) (Run, error) {
	createdRun := Run{}
	err := c.sendRequest("POST", "add_run/"+strconv.Itoa(projectID), newRun, &createdRun)
	return createdRun, err
}

// UpdateRun updates the run runID and returns it
func (c *Client) UpdateRun(runID int, update UpdatableRun) (Run, error) {
	updatedRun := Run{}
	err := c.sendRequest("POST", "update_run/"+strconv.Itoa(runID), update, &updatedRun)
	return updatedRun, err
}

// CloseRun closes the run runID,
// archives its tests and results
// and returns it
func (c *Client) CloseRun(runID int) (Run, error) {
	closedRun := Run{}
	err := c.sendRequest("POST", "close_run/"+strconv.Itoa(runID), nil, &closedRun)
	return closedRun, err
}

// DeleteRun delete the run runID
func (c *Client) DeleteRun(runID int) error {
	return c.sendRequest("POST", "delete_run/"+strconv.Itoa(runID), nil, nil)
}

// applyFiltersForRuns go through each possible filters and create the
// uri for the wanted ones
func applyFiltersForRuns(uri string, filters RequestFilterForRun) string {
	if filters.CreatedAfter != "" {
		uri = uri + "&created_after=" + filters.CreatedAfter
	}
	if filters.CreatedBefore != "" {
		uri = uri + "&created_before=" + filters.CreatedBefore
	}
	if len(filters.CreatedBy) != 0 {
		uri = applySpecificFilter(uri, "created_by", filters.CreatedBy)
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
	if len(filters.MilestoneID) != 0 {
		uri = applySpecificFilter(uri, "milestone_id", filters.MilestoneID)
	}
	if len(filters.SuiteID) != 0 {
		uri = applySpecificFilter(uri, "suite_id", filters.SuiteID)
	}

	return uri
}
