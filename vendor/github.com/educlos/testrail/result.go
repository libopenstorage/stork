package testrail

import "strconv"

// Result represents a Test Case result
type Result struct {
	AssignedtoID int       `json:"assignedto_id"`
	Comment      string    `json:"comment"`
	CreatedBy    int       `json:"created_by"`
	CreatedOn    timestamp `json:"created_on"`
	Defects      string    `json:"defects"`
	Elapsed      timespan  `json:"elapsed"`
	ID           int       `json:"id"`
	StatusID     int       `json:"status_id"`
	TestID       int       `json:"test_id"`
	Version      string    `json:"version"`

	customResult
}

// CustomStepResult represents the custom steps
// results a Result can have
type CustomStepResult struct {
	Content  string `json:"content"`
	Expected string `json:"expected"`
	Actual   string `json:"actual"`
	StatusID int    `json:"status_id"`
}

// RequestFilterForCaseResults represents the filters
// usable to get the test case results
type RequestFilterForCaseResults struct {
	Limit    *int  `json:"limit,omitempty"`
	Offest   *int  `json:"offset, omitempty"`
	StatusID []int `json:"status_id,omitempty"`
}

// RequestFilterForRunResults represents the filters
// usable to get the run results
type RequestFilterForRunResults struct {
	CreatedAfter  string `json:"created_after,omitempty"`
	CreatedBefore string `json:"created_before,omitempty"`
	CreatedBy     []int  `json:"created_by,omitempty"`
	Limit         *int   `json:"limit,omitempty"`
	Offest        *int   `json:"offset, omitempty"`
	StatusID      []int  `json:"status_id,omitempty"`
}

// SendableResult represents a Test Case result
// that can be created or updated via the api
type SendableResult struct {
	StatusID     int      `json:"status_id,omitempty"`
	Comment      string   `json:"comment,omitempty"`
	Version      string   `json:"version,omitempty"`
	Elapsed      timespan `json:"elapsed,omitempty"`
	Defects      string   `json:"defects,omitempty"`
	AssignedToID int      `json:"assignedto_id,omitempty"`

	customResult
}

// SendableResults represents a list of run results
// that can be created or updated via the api
type SendableResults struct {
	Results []Results `json:"results"`
}

// Results represents a run result
// that can be created or updated via the api
type Results struct {
	TestID int `json:"test_id"`
	SendableResult
}

// Results represents a run result
// that can be created or updated via the api
type ResultsForCase struct {
	CaseID int `json:"case_id"`
	SendableResult
}

// SendableResultsForCase represents a Test Case result
// that can be created or updated via the api
type SendableResultsForCase struct {
	Results []ResultsForCase `json:"results"`
}

// GetResults returns a list of results for the test testID
// validating the filters
func (c *Client) GetResults(testID int, filters ...RequestFilterForCaseResults) ([]Result, error) {
	returnResults := []Result{}
	uri := "get_results/" + strconv.Itoa(testID)

	if len(filters) > 0 {
		uri = applyFiltersForCaseResults(uri, filters[0])
	}
	var err error
	if c.useBetaApi {
		err = c.sendRequestBeta("GET", uri, nil, &returnResults, "results")
	} else {
		err = c.sendRequest("GET", uri, nil, &returnResults)
	}
	return returnResults, err
}

// GetResultsForCase returns a list of results for the case caseID
// on run runID validating the filters
func (c *Client) GetResultsForCase(runID, caseID int, filters ...RequestFilterForCaseResults) ([]Result, error) {
	returnResults := []Result{}
	uri := "get_results_for_case/" + strconv.Itoa(runID) + "/" + strconv.Itoa(caseID)

	if len(filters) > 0 {
		uri = applyFiltersForCaseResults(uri, filters[0])
	}
	var err error
	if c.useBetaApi {
		err = c.sendRequestBeta("GET", uri, nil, &returnResults, "results")
	} else {
		err = c.sendRequest("GET", uri, nil, &returnResults)
	}
	return returnResults, err
}

// GetResultsForRun returns a list of results for the run runID
// validating the filters
func (c *Client) GetResultsForRun(runID int, filters ...RequestFilterForRunResults) ([]Result, error) {
	returnResults := []Result{}
	uri := "get_results_for_run/" + strconv.Itoa(runID)

	if len(filters) > 0 {
		uri = applyFiltersForRunResults(uri, filters[0])
	}
	var err error
	if c.useBetaApi {
		err = c.sendRequestBeta("GET", uri, nil, &returnResults, "results")
	} else {
		err = c.sendRequest("GET", uri, nil, &returnResults)
	}
	return returnResults, err
}

// AddResult adds a new result, comment or assigns a test to testID
func (c *Client) AddResult(testID int, newResult SendableResult) (Result, error) {
	createdResult := Result{}
	err := c.sendRequest("POST", "add_result/"+strconv.Itoa(testID), newResult, &createdResult)
	return createdResult, err
}

// AddResultForCase adds a new result, comment or assigns a test to the case caseID on run runID
func (c *Client) AddResultForCase(runID, caseID int, newResult SendableResult) (Result, error) {
	createdResult := Result{}
	uri := "add_result_for_case/" + strconv.Itoa(runID) + "/" + strconv.Itoa(caseID)
	err := c.sendRequest("POST", uri, newResult, &createdResult)
	return createdResult, err
}

// AddResults adds new results, comment or assigns tests to runID
func (c *Client) AddResults(runID int, newResult SendableResults) ([]Result, error) {
	createdResult := []Result{}
	err := c.sendRequest("POST", "add_results/"+strconv.Itoa(runID), newResult, &createdResult)
	return createdResult, err
}

// AddResultsForCases adds new results, comments or assigns tests to run runID
// each result being assigned to a test case
func (c *Client) AddResultsForCases(runID int, newResult SendableResultsForCase) ([]Result, error) {
	createdResult := []Result{}
	err := c.sendRequest("POST", "add_results_for_cases/"+strconv.Itoa(runID), newResult, &createdResult)
	return createdResult, err
}

// applyFiltersForCaseResults go through each possible filters and create the
// uri for the wanted ones
func applyFiltersForCaseResults(uri string, filters RequestFilterForCaseResults) string {
	if filters.Limit != nil {
		uri = uri + "&limit=" + strconv.Itoa(*filters.Limit)
	}
	if filters.Offest != nil {
		uri = uri + "&offset=" + strconv.Itoa(*filters.Offest)
	}
	if len(filters.StatusID) != 0 {
		uri = applySpecificFilter(uri, "status_id", filters.StatusID)
	}

	return uri
}

// applyFiltersForCaseResults go through each possible filters and create the
// uri for the wanted ones
func applyFiltersForRunResults(uri string, filters RequestFilterForRunResults) string {
	if filters.CreatedAfter != "" {
		uri = uri + "&created_after=" + filters.CreatedAfter
	}
	if filters.CreatedBefore != "" {
		uri = uri + "&created_before=" + filters.CreatedBefore
	}
	if len(filters.CreatedBy) != 0 {
		uri = applySpecificFilter(uri, "created_by", filters.CreatedBy)
	}
	if filters.Limit != nil {
		uri = uri + "&limit=" + strconv.Itoa(*filters.Limit)
	}
	if filters.Offest != nil {
		uri = uri + "&offset=" + strconv.Itoa(*filters.Offest)
	}
	if len(filters.StatusID) != 0 {
		uri = applySpecificFilter(uri, "status_id", filters.StatusID)
	}

	return uri
}
