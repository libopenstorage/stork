package api

import (
	status "net/http"

	pds "github.com/portworx/pds-api-go-client/pds/v1alpha1"
	"github.com/portworx/torpedo/drivers/pds/pdsutils"
	"github.com/portworx/torpedo/pkg/log"
)

// DeploymentTarget struct
type DeploymentTarget struct {
	apiClient *pds.APIClient
}

// ListDeploymentTargetsBelongsToTenant return deployment targets models for a tenant.
func (dt *DeploymentTarget) ListDeploymentTargetsBelongsToTenant(tenantID string) ([]pds.ModelsDeploymentTarget, error) {
	dtClient := dt.apiClient.DeploymentTargetsApi
	ctx, err := pdsutils.GetContext()
	if err != nil {
		log.Errorf("Error in getting context for api call: %v\n", err)
		return nil, err
	}
	dtModels, res, err := dtClient.ApiTenantsIdDeploymentTargetsGet(ctx, tenantID).Execute()

	if err != nil && res.StatusCode != status.StatusOK {
		log.Errorf("Error when calling `ApiTenantsIdDeploymentTargetsGet``: %v\n", err)
		log.Errorf("Full HTTP response: %v\n", res)
		return nil, err
	}
	return dtModels.GetData(), nil
}

// ListDeploymentTargetsBelongsToProject return deployment targets models for a project.
func (dt *DeploymentTarget) ListDeploymentTargetsBelongsToProject(projectID string) ([]pds.ModelsDeploymentTarget, error) {
	dtClient := dt.apiClient.DeploymentTargetsApi
	ctx, err := pdsutils.GetContext()
	if err != nil {
		log.Errorf("Error in getting context for api call: %v\n", err)
		return nil, err
	}
	dtModels, res, err := dtClient.ApiProjectsIdDeploymentTargetsGet(ctx, projectID).Execute()

	if err != nil && res.StatusCode != status.StatusOK {
		log.Errorf("Error when calling `ApiProjectsIdDeploymentTargetsGet``: %v\n", err)
		log.Errorf("Full HTTP response: %v\n", res)
		return nil, err
	}
	return dtModels.GetData(), nil
}

// GetTarget return deployment target model.
func (dt *DeploymentTarget) GetTarget(targetID string) (*pds.ModelsDeploymentTarget, error) {
	dtClient := dt.apiClient.DeploymentTargetsApi
	log.Infof("Get cluster details having uuid - %v", targetID)
	ctx, err := pdsutils.GetContext()
	if err != nil {
		log.Errorf("Error in getting context for api call: %v\n", err)
		return nil, err
	}
	dtModel, res, err := dtClient.ApiDeploymentTargetsIdGet(ctx, targetID).Execute()
	if err != nil && res.StatusCode != status.StatusOK {
		log.Errorf("Error when calling `ApiDeploymentTargetsIdGet``: %v\n", err)
		log.Errorf("Full HTTP response: %v\n", res)
		return nil, err
	}
	return dtModel, nil
}

// UpdateTarget return updated deployment target model.
func (dt *DeploymentTarget) UpdateTarget(targetID string, name string) (*pds.ModelsDeploymentTarget, error) {
	dtClient := dt.apiClient.DeploymentTargetsApi
	log.Infof("Get cluster details having uuid - %v", targetID)
	upateRequest := pds.ControllersUpdateDeploymentTargetRequest{Name: &name}
	ctx, err := pdsutils.GetContext()
	if err != nil {
		log.Errorf("Error in getting context for api call: %v\n", err)
		return nil, err
	}
	dtModel, res, err := dtClient.ApiDeploymentTargetsIdPut(ctx, targetID).Body(upateRequest).Execute()
	if err != nil && res.StatusCode != status.StatusOK {
		log.Errorf("Error when calling `ApiDeploymentTargetsIdPut``: %v\n", err)
		log.Errorf("Full HTTP response: %v\n", res)
		return nil, err
	}
	return dtModel, nil
}

// DeleteTarget delete the deployment target and return status.
func (dt *DeploymentTarget) DeleteTarget(targetID string) (*status.Response, error) {
	dtClient := dt.apiClient.DeploymentTargetsApi
	log.Infof("Get cluster details having uuid - %v", targetID)
	ctx, err := pdsutils.GetContext()
	if err != nil {
		log.Errorf("Error in getting context for api call: %v\n", err)
		return nil, err
	}
	res, err := dtClient.ApiDeploymentTargetsIdDelete(ctx, targetID).Execute()
	if err != nil && res.StatusCode != status.StatusOK {
		log.Errorf("Error when calling `ApiDeploymentTargetsIdDelete``: %v\n", err)
		log.Errorf("Full HTTP response: %v\n", res)
		return nil, err
	}
	return res, nil
}
