package testrail

import "fmt"

// Case represents a Test Case
type Case struct {
	CreatedBy            int          `json:"created_by"`
	CreatedOn            int          `json:"created_on"`
	CustomExpected       string       `json:"custom_expected"`
	CustomPreconds       string       `json:"custom_preconds"`
	CustomSteps          string       `json:"custom_steps"`
	CustomStepsSeparated []CustomStep `json:"custom_steps_separated"`
	Estimate             string       `json:"estimate"`
	EstimateForecast     string       `json:"estimate_forecast"`
	ID                   int          `json:"id"`
	MilestoneID          int          `json:"milestone_id"`
	PriorityID           int          `json:"priority_id"`
	Refs                 string       `json:"refs"`
	SectionID            int          `json:"section_id"`
	SuiteID              int          `json:"suite_id"`
	Title                string       `json:"title"`
	TypeID               int          `json:"type_id"`
	UpdatedBy            int          `json:"updated_by"`
	UdpatedOn            int          `json:"updated_on"`
}

// CustomStep represents the custom steps
// a Test Case can have
type CustomStep struct {
	Content  string `json:"content"`
	Expected string `json:"expected"`
}

// RequestFilterForCases represents the filters
// usable to get the test cases
type RequestFilterForCases struct {
	CreatedAfter  string `json:"created_after"`
	CreatedBefore string `json:"created_before"`
	CreatedBy     []int  `json:"created_by"`
	MilestoneID   []int  `json:"milestone_id"`
	PriorityID    []int  `json:"priority_id"`
	TypeID        []int  `json:"type_id"`
	UpdatedAfter  string `json:"updated_after"`
	UpdatedBefore string `json:"updated_before"`
	UpdatedBy     []int  `json:"updated_by"`
}

// SendableCase represents a Test Case
// that can be created or updated via the api
type SendableCase struct {
	Title           string       `json:"title"`
	TypeID          int          `json:"type_id,omitempty"`
	PriorityID      int          `json:"priority_id,omitempty"`
	Estimate        string       `json:"estimate,omitempty"`
	MilestoneID     int          `json:"milestone_id,omitempty"`
	Refs            string       `json:"refs,omitempty"`
	Checkbox        bool         `json:"custom_checkbox,omitempty"`
	Date            string       `json:"custom_date,omitempty"`
	Dropdown        int          `json:"custom_dropdown,omitempty"`
	Integer         int          `json:"custom_integer,omitempty"`
	Milestone       int          `json:"custom_milestone,omitempty"`
	MultiSelect     []int        `json:"custom_multi-select,omitempty"`
	Steps           []CustomStep `json:"custom_steps,omitempty"`
	String          string       `json:"custom_string,omitempty"`
	TemplateId      int          `json:"template_id,omitempty"`
	TestDescription string       `json:"custom_test_description,omitempty"`
	Text            string       `json:"custom_text,omitempty"`
	URL             string       `json:"custom_url,omitempty"`
	User            int          `json:"custom_user,omitempty"`
}

// GetCase returns the existing Test Case caseID
func (c *Client) GetCase(caseID int) (Case, error) {
	returnCase := Case{}
	err := c.sendRequest("GET", fmt.Sprintf("get_case/%d", caseID), nil, &returnCase)
	return returnCase, err
}

// GetCases returns a list of Test Cases on project projectID
// for a Test Suite suiteID
// or for specific section sectionID in a Test Suite
func (c *Client) GetCases(projectID, suiteID int, sectionID ...int) ([]Case, error) {
	uri := fmt.Sprintf("get_cases/%d&suite_id=%d", projectID, suiteID)
	if len(sectionID) > 0 {
		uri = fmt.Sprintf("%s&section_id=%d", uri, sectionID[0])
	}

	returnCases := []Case{}
	var err error
	if c.useBetaApi {
		err = c.sendRequestBeta("GET", uri, nil, &returnCases, "cases")
	} else {
		err = c.sendRequest("GET", uri, nil, &returnCases)
	}
	return returnCases, err
}

// GetCasesWithCustomFields returns a interface that can be mapped to an array that contains custom fields
// on project projectID for a Test Suite suiteID
// or for specific section sectionID in a Test Suite
func (c *Client) GetCasesWithCustomFields(projectID, suiteID int, customArray interface{}, sectionID ...int) error {
	uri := fmt.Sprintf("get_cases/%d&suite_id=%d", projectID, suiteID)
	if len(sectionID) > 0 {
		uri = fmt.Sprintf("%s&section_id=%d", uri, sectionID[0])
	}

	var err error
	if c.useBetaApi {
		err = c.sendRequestBeta("GET", uri, nil, &customArray, "cases")
	} else {
		err = c.sendRequest("GET", uri, nil, &customArray)
	}
	return err
}

// GetCasesWithFilters returns a list of Test Cases on project projectID
// for a Test Suite suiteID validating the filters
func (c *Client) GetCasesWithFilters(projectID, suiteID int, filters ...RequestFilterForCases) ([]Case, error) {
	uri := fmt.Sprintf("get_cases/%d&suite_id=%d", projectID, suiteID)
	if len(filters) > 0 {
		uri = applyFiltersForCase(uri, filters[0])
	}

	returnCases := []Case{}
	fmt.Println(uri)
	err := c.sendRequest("GET", uri, nil, &returnCases)
	return returnCases, err
}

// AddCase creates a new Test Case newCase and returns it
func (c *Client) AddCase(sectionID int, newCase SendableCase) (Case, error) {
	createdCase := Case{}
	err := c.sendRequest("POST", fmt.Sprintf("add_case/%d", sectionID), newCase, &createdCase)
	return createdCase, err
}

// UpdateCase updates an existing Test Case caseID and returns it
func (c *Client) UpdateCase(caseID int, updates SendableCase) (Case, error) {
	updatedCase := Case{}
	err := c.sendRequest("POST", fmt.Sprintf("update_case/%d", caseID), updates, &updatedCase)
	return updatedCase, err
}

// DeleteCase deletes the existing Test Case caseID
func (c *Client) DeleteCase(caseID int) error {
	return c.sendRequest("POST", fmt.Sprintf("delete_case/%d", caseID), nil, nil)
}

// applyFiltersForCase go through each possible filters and create the
// uri for the wanted ones
func applyFiltersForCase(uri string, filters RequestFilterForCases) string {
	if filters.CreatedAfter != "" {
		uri = fmt.Sprintf("%s&created_after=%s", uri, filters.CreatedAfter)
	}
	if filters.CreatedBefore != "" {
		uri = fmt.Sprintf("%s&created_before=%s", uri, filters.CreatedBefore)
	}
	if len(filters.CreatedBy) != 0 {
		uri = applySpecificFilter(uri, "created_by", filters.CreatedBy)
	}
	if len(filters.MilestoneID) != 0 {
		uri = applySpecificFilter(uri, "milestone_id", filters.MilestoneID)
	}
	if len(filters.PriorityID) != 0 {
		uri = applySpecificFilter(uri, "priority_id", filters.PriorityID)
	}
	if len(filters.TypeID) != 0 {
		uri = applySpecificFilter(uri, "type_id", filters.TypeID)
	}
	if filters.UpdatedAfter != "" {
		uri = fmt.Sprintf("%s&updated_after=%s", uri, filters.UpdatedAfter)
	}
	if filters.UpdatedBefore != "" {
		uri = fmt.Sprintf("%s&updated_before=%s", uri, filters.UpdatedBefore)
	}
	if len(filters.UpdatedBy) != 0 {
		uri = applySpecificFilter(uri, "updated_by", filters.UpdatedBy)
	}

	return uri
}
