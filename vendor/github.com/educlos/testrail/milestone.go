package testrail

import (
	"strconv"
)

// Milestone represents a Milestone
type Milestone struct {
	CompletedOn int    `json:"completed_on"`
	Description string `json:"description"`
	DueOn       int    `json:"due_on"`
	ID          int    `json:"id"`
	IsCompleted bool   `json:"is_completed"`
	Name        string `json:"name"`
	ProjectID   int    `json:"project_id"`
	URL         string `json:"url"`
}

// SendableMilestone represents a Milestone
// that can be created or updated via the api
type SendableMilestone struct {
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
	DueOn       int    `json:"due_on,omitempty"`
}

// GetMilestone returns the existing milestone milestoneID
func (c *Client) GetMilestone(milestoneID int) (Milestone, error) {
	returnMilestone := Milestone{}
	err := c.sendRequest("GET", "get_milestone/"+strconv.Itoa(milestoneID), nil, &returnMilestone)
	return returnMilestone, err
}

// GetMilestones returns the list of milestones for the project projectID
// can be filtered by completed status of the milestones
func (c *Client) GetMilestones(projectID int, isCompleted ...bool) ([]Milestone, error) {
	uri := "get_milestones/" + strconv.Itoa(projectID)
	if len(isCompleted) > 0 {
		uri = uri + "&is_completed=" + btoitos(isCompleted[0])
	}
	var err error
	returnMilestones := []Milestone{}

	if c.useBetaApi {
		err = c.sendRequestBeta("GET", uri, nil, &returnMilestones, "milestones")
	} else {
		err = c.sendRequest("GET", uri, nil, &returnMilestones)
	}

	return returnMilestones, err
}

// AddMilestone creates a new milestone on project projectID and returns it
func (c *Client) AddMilestone(projectID int, newMilestone SendableMilestone) (Milestone, error) {
	createdMilestone := Milestone{}
	err := c.sendRequest("POST", "add_milestone/"+strconv.Itoa(projectID), newMilestone, &createdMilestone)
	return createdMilestone, err
}

// UpdateMilestone updates the existing milestone milestoneID an returns it
func (c *Client) UpdateMilestone(milestoneID int, updates SendableMilestone) (Milestone, error) {
	updatedMilestone := Milestone{}
	err := c.sendRequest("POST", "update_milestone/"+strconv.Itoa(milestoneID), updates, &updatedMilestone)
	return updatedMilestone, err
}

// DeleteMilestone deletes the milestone milestoneID
func (c *Client) DeleteMilestone(milestoneID int) error {
	return c.sendRequest("POST", "delete_milestone/"+strconv.Itoa(milestoneID), nil, nil)
}
