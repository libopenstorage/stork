package api

import (
	status "net/http"

	pds "github.com/portworx/pds-api-go-client/pds/v1alpha1"
	"github.com/portworx/torpedo/drivers/pds/pdsutils"
	log "github.com/sirupsen/logrus"
)

// Project struct
type Project struct {
	apiClient *pds.APIClient
}

// GetprojectsList return pds projects models.
func (project *Project) GetprojectsList(tenantID string) ([]pds.ModelsProject, error) {
	projectClient := project.apiClient.ProjectsApi
	log.Info("Get list of Projects.")
	ctx, err := pdsutils.GetContext()
	if err != nil {
		log.Errorf("Error in getting context for api call: %v\n", err)
		return nil, err
	}
	projectsModel, res, err := projectClient.ApiTenantsIdProjectsGet(ctx, tenantID).Execute()

	if err != nil && res.StatusCode != status.StatusOK {
		log.Errorf("Error when calling `ApiTenantsIdProjectsGet``: %v\n", err)
		log.Errorf("Full HTTP response: %v\n", res)
		return nil, err
	}
	return projectsModel.GetData(), nil
}

// Getproject return project model.
func (project *Project) Getproject(projectID string) (*pds.ModelsProject, error) {
	projectClient := project.apiClient.ProjectsApi
	log.Info("Get the project details.")
	ctx, err := pdsutils.GetContext()
	if err != nil {
		log.Errorf("Error in getting context for api call: %v\n", err)
		return nil, err
	}
	projectModel, res, err := projectClient.ApiProjectsIdGet(ctx, projectID).Execute()

	if err != nil && res.StatusCode != status.StatusOK {
		log.Errorf("Error when calling `ApiProjectsIdGet``: %v\n", err)
		log.Errorf("Full HTTP response: %v\n", res)
		return nil, err
	}
	return projectModel, nil
}
