// Package api comprises of all the components and associated CRUD functionality
package api

import (
	status "net/http"

	pds "github.com/portworx/pds-api-go-client/pds/v1alpha1"
	"github.com/portworx/torpedo/drivers/pds/pdsutils"
	"github.com/portworx/torpedo/pkg/log"
)

// AppConfigTemplate struct
type AppConfigTemplate struct {
	apiClient *pds.APIClient
}

// ListTemplates return app configuration templates model.
func (at *AppConfigTemplate) ListTemplates(tenantID string) ([]pds.ModelsApplicationConfigurationTemplate, error) {
	atClient := at.apiClient.ApplicationConfigurationTemplatesApi
	ctx, err := pdsutils.GetContext()
	if err != nil {
		log.Errorf("Error in getting context for api call: %v\n", err)
		return nil, err
	}
	atModel, res, err := atClient.ApiTenantsIdApplicationConfigurationTemplatesGet(ctx, tenantID).Execute()

	if err != nil && res.StatusCode != status.StatusOK {
		log.Errorf("Error when calling `ApiTenantsIdApplicationConfigurationTemplatesGet``: %v\n", err)
		log.Errorf("Full HTTP response: %v\n", res)
		return nil, err
	}
	return atModel.GetData(), nil
}

// GetTemplate return app configurationtemplate model.
func (at *AppConfigTemplate) GetTemplate(templateID string) (*pds.ModelsApplicationConfigurationTemplate, error) {
	atClient := at.apiClient.ApplicationConfigurationTemplatesApi
	log.Infof("Get list of storage templates for tenant ID - %v", templateID)
	ctx, err := pdsutils.GetContext()
	if err != nil {
		log.Errorf("Error in getting context for api call: %v\n", err)
		return nil, err
	}
	atModel, res, err := atClient.ApiApplicationConfigurationTemplatesIdGet(ctx, templateID).Execute()

	if err != nil && res.StatusCode != status.StatusOK {
		log.Errorf("Error when calling `ApiApplicationConfigurationTemplatesIdGet``: %v\n", err)
		log.Errorf("Full HTTP response: %v\n", res)
		return nil, err
	}
	return atModel, nil
}

// CreateTemplate function create app configuration template and return pds application template model.
func (at *AppConfigTemplate) CreateTemplate(tenantID string, dataServiceID string, name string, data []pds.ModelsConfigItem) (*pds.ModelsApplicationConfigurationTemplate, error) {
	atClient := at.apiClient.ApplicationConfigurationTemplatesApi
	log.Info("Create new resource template.")
	createRequest := pds.ControllersCreateApplicationConfigurationTemplatesRequest{ConfigItems: data, DataServiceId: &dataServiceID, Name: &name}
	ctx, err := pdsutils.GetContext()
	if err != nil {
		log.Errorf("Error in getting context for api call: %v\n", err)
		return nil, err
	}
	atModel, res, err := atClient.ApiTenantsIdApplicationConfigurationTemplatesPost(ctx, tenantID).Body(createRequest).Execute()

	if err != nil && res.StatusCode != status.StatusOK {
		log.Errorf("Error when calling `ApiTenantsIdApplicationConfigurationTemplatesPost``: %v\n", err)
		log.Errorf("Full HTTP response: %v\n", res)
		return nil, err
	}
	return atModel, nil
}

// UpdateTemplate function return updated pds application template model.
func (at *AppConfigTemplate) UpdateTemplate(templateID string, deployTime bool, key string, value string, name string) (*pds.ModelsApplicationConfigurationTemplate, error) {
	atClient := at.apiClient.ApplicationConfigurationTemplatesApi
	log.Info("Create new resource template.")
	data := []pds.ModelsConfigItem{{DeployTime: &deployTime, Key: &key, Value: &value}}
	updateRequest := pds.ControllersUpdateApplicationConfigurationTemplateRequest{ConfigItems: data, Name: &name}
	ctx, err := pdsutils.GetContext()
	if err != nil {
		log.Errorf("Error in getting context for api call: %v\n", err)
		return nil, err
	}
	atModel, res, err := atClient.ApiApplicationConfigurationTemplatesIdPut(ctx, templateID).Body(updateRequest).Execute()

	if err != nil && res.StatusCode != status.StatusOK {
		log.Errorf("Error when calling `ApiApplicationConfigurationTemplatesIdPut``: %v\n", err)
		log.Errorf("Full HTTP response: %v\n", res)
		return nil, err
	}
	return atModel, nil
}

// DeleteTemplate function delete app configuration template and return the api statusresponse.
func (at *AppConfigTemplate) DeleteTemplate(templateID string) (*status.Response, error) {
	atClient := at.apiClient.ApplicationConfigurationTemplatesApi
	log.Infof("Get list of storage templates for tenant ID - %v", templateID)
	ctx, err := pdsutils.GetContext()
	if err != nil {
		log.Errorf("Error in getting context for api call: %v\n", err)
		return nil, err
	}
	res, err := atClient.ApiApplicationConfigurationTemplatesIdDelete(ctx, templateID).Execute()

	if err != nil && res.StatusCode != status.StatusOK {
		log.Errorf("Error when calling `ApiApplicationConfigurationTemplatesIdDelete``: %v\n", err)
		log.Errorf("Full HTTP response: %v\n", res)
		return nil, err
	}
	return res, nil
}
