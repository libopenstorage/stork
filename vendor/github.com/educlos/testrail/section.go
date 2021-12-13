package testrail

import "strconv"

// Section represents a Test Suite Section
type Section struct {
	Depth        int    `json:"depth"`
	Description  string `json:"description"`
	DisplayOrder int    `json:"display_order"`
	ID           int    `json:"id"`
	ParentID     int    `json:"parent_id"`
	Name         string `json:"name"`
	SuiteID      int    `json:"suite_id"`
}

// SendableSection represents a Test Suite Section
// that can be created via the api
type SendableSection struct {
	Description string `json:"description,omitempty"`
	SuiteID     int    `json:"suite_id,omitempty"`
	ParentID    int    `json:"parent_id,omitempty"`
	Name        string `json:"name"`
}

// UpdatableSection represents a Test Suite Section
// that can be updated via the api
type UpdatableSection struct {
	Description string `json:"description,omitempty"`
	Name        string `json:"name,omitempty"`
}

// GetSection returns the section sectionID
func (c *Client) GetSection(sectionID int) (Section, error) {
	returnSection := Section{}
	err := c.sendRequest("GET", "get_section/"+strconv.Itoa(sectionID), nil, &returnSection)
	return returnSection, err
}

// GetSections returns the list of sections of projectID
// present in suite suiteID, if specified
func (c *Client) GetSections(projectID int, suiteID ...int) ([]Section, error) {
	returnSection := []Section{}
	uri := "get_sections/" + strconv.Itoa(projectID)

	if len(suiteID) > 0 {
		uri = uri + "&suite_id=" + strconv.Itoa(suiteID[0])
	}
	var err error
	if c.useBetaApi {
		err = c.sendRequestBeta("GET", uri, nil, &returnSection, "sections")
	} else {
		err = c.sendRequest("GET", uri, nil, &returnSection)
	}
	return returnSection, err
}

// AddSection creates a new section on projectID and returns it
func (c *Client) AddSection(projectID int, newSection SendableSection) (Section, error) {
	createdSection := Section{}
	err := c.sendRequest("POST", "add_section/"+strconv.Itoa(projectID), newSection, &createdSection)
	return createdSection, err
}

// UpdateSection updates the section sectionID and returns it
func (c *Client) UpdateSection(sectionID int, update UpdatableSection) (Section, error) {
	updatedSection := Section{}
	err := c.sendRequest("POST", "update_section/"+strconv.Itoa(sectionID), update, &updatedSection)
	return updatedSection, err
}

// DeleteSection deletes the section sectionID
func (c *Client) DeleteSection(sectionID int) error {
	return c.sendRequest("POST", "delete_section/"+strconv.Itoa(sectionID), nil, nil)
}
