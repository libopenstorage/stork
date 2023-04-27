package testrail

import (
	"fmt"
	"strconv"
)

// Project represents a Project
type Project struct {
	Announcement     string `json:"announcement"`
	CompletedOn      int    `json:"completed_on"`
	ID               int    `json:"id"`
	IsCompleted      bool   `json:"is_completed"`
	Name             string `json:"name"`
	ShowAnnouncement bool   `json:"show_announcement"`
	URL              string `json:"url"`
}

// SendableProject represents a Project
// that can be created or updated via the ap
type SendableProject struct {
	Name             string `json:"name"`
	Announcement     string `json:"announcement,omitempty"`
	ShowAnnouncement bool   `json:"show_announcement,omitempty"`
	SuiteMode        int    `json:"suite_mode,omitempty"`
}

// GetProject returns the existing project projectID
func (c *Client) GetProject(projectID int) (Project, error) {
	returnProject := Project{}
	err := c.sendRequest("GET", "get_project/"+strconv.Itoa(projectID), nil, &returnProject)
	return returnProject, err
}

// GetProjects returns a list available projects
// can be filtered by completed status of the project
func (c *Client) GetProjects(isCompleted ...bool) ([]Project, error) {
	uri := "get_projects"
	if len(isCompleted) > 0 {
		uri = uri + "&is_completed=" + btoitos(isCompleted[0])
	}

	returnProjects := []Project{}
	var err error
	if c.useBetaApi {
		err = c.sendRequestBeta("GET", uri, nil, &returnProjects, "projects")
	} else {
		err = c.sendRequest("GET", uri, nil, &returnProjects)
	}
	return returnProjects, err
}

// AddProject creates a new project and return its
func (c *Client) AddProject(newProject SendableProject) (Project, error) {
	createdProject := Project{}
	err := c.sendRequest("POST", "add_project", newProject, &createdProject)
	return createdProject, err
}

// UpdateProject updates the existing project projectID and returns it
func (c *Client) UpdateProject(projectID int, updates SendableProject, isCompleted ...bool) (Project, error) {
	updatedProject := Project{}
	uri := "update_project/" + strconv.Itoa(projectID)
	if len(isCompleted) > 0 {
		uri = uri + "&is_completed=" + btoitos(isCompleted[0])
	}

	fmt.Println(uri)
	err := c.sendRequest("POST", uri, updates, &updatedProject)
	return updatedProject, err
}

// DeleteProject deletes the project projectID
func (c *Client) DeleteProject(projectID int) error {
	return c.sendRequest("POST", "delete_project/"+strconv.Itoa(projectID), nil, nil)
}
