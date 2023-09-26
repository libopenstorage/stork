package api

import (
	"fmt"
	status "net/http"

	pds "github.com/portworx/pds-api-go-client/pds/v1alpha1"
	"github.com/portworx/torpedo/pkg/log"
)

// Project struct
type Project struct {
	apiClient *pds.APIClient
}

// GetprojectsList return pds projects models.
func (project *Project) GetprojectsList(tenantID string) ([]pds.ModelsProject, error) {
	projectClient := project.apiClient.ProjectsApi
	log.Info("Get list of Projects.")
	ctx, err := GetContext()
	if err != nil {
		return nil, fmt.Errorf("Error in getting context for api call: %v\n", err)
	}
	projectsModel, res, err := projectClient.ApiTenantsIdProjectsGet(ctx, tenantID).Execute()
	if err != nil && res.StatusCode != status.StatusOK {
		return nil, fmt.Errorf("Error when calling `ApiTenantsIdProjectsGet`: %v\n.Full HTTP response: %v", err, res)
	}
	return projectsModel.GetData(), nil
}

// Getproject return project model.
func (project *Project) Getproject(projectID string) (*pds.ModelsProject, error) {
	projectClient := project.apiClient.ProjectsApi
	log.Info("Get the project details.")
	ctx, err := GetContext()
	if err != nil {
		return nil, fmt.Errorf("Error in getting context for api call: %v\n", err)
	}
	projectModel, res, err := projectClient.ApiProjectsIdGet(ctx, projectID).Execute()
	if err != nil && res.StatusCode != status.StatusOK {
		return nil, fmt.Errorf("Error when calling `ApiProjectsIdGet`: %v\n.Full HTTP response: %v", err, res)
	}
	return projectModel, nil
}
