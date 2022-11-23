package api

import (
	status "net/http"

	pds "github.com/portworx/pds-api-go-client/pds/v1alpha1"
	"github.com/portworx/torpedo/drivers/pds/pdsutils"
	"github.com/portworx/torpedo/pkg/log"
)

// DefaultTemplates struct
type DefaultTemplates struct {
	apiClient *pds.APIClient
}

// ListApplicationConfigurationTemplates func
func (ds *DefaultTemplates) ListApplicationConfigurationTemplates() ([]pds.ModelsApplicationConfigurationTemplate, error) {
	dsClient := ds.apiClient.DefaultTemplatesApi
	ctx, err := pdsutils.GetContext()
	if err != nil {
		log.Errorf("Error in getting context for api call: %v\n", err)
		return nil, err
	}
	dsModels, res, err := dsClient.ApiDefaultTemplatesApplicationConfigurationGet(ctx).Execute()

	if res.StatusCode != status.StatusOK {
		log.Errorf("Error when calling `ApiDataServicesGet``: %v\n", err)
		log.Errorf("Full HTTP response: %v\n", res)
	}
	return dsModels.GetData(), err
}

// ListResourceSettingTemplates func
func (ds *DefaultTemplates) ListResourceSettingTemplates() ([]pds.ModelsResourceSettingsTemplate, error) {
	dsClient := ds.apiClient.DefaultTemplatesApi
	ctx, err := pdsutils.GetContext()
	if err != nil {
		log.Errorf("Error in getting context for api call: %v\n", err)
		return nil, err
	}
	dsModels, res, err := dsClient.ApiDefaultTemplatesResourceSettingsGet(ctx).Execute()

	if res.StatusCode != status.StatusOK {
		log.Errorf("Error when calling `ApiDataServicesGet``: %v\n", err)
		log.Errorf("Full HTTP response: %v\n", res)
	}
	return dsModels.GetData(), err
}

// ListStorageOptionsTemplates func
func (ds *DefaultTemplates) ListStorageOptionsTemplates() ([]pds.ModelsStorageOptionsTemplate, error) {
	dsClient := ds.apiClient.DefaultTemplatesApi
	ctx, err := pdsutils.GetContext()
	if err != nil {
		log.Errorf("Error in getting context for api call: %v\n", err)
		return nil, err
	}
	dsModels, res, err := dsClient.ApiDefaultTemplatesStorageOptionsGet(ctx).Execute()

	if res.StatusCode != status.StatusOK {
		log.Errorf("Error when calling `ApiDataServicesGet``: %v\n", err)
		log.Errorf("Full HTTP response: %v\n", res)
	}
	return dsModels.GetData(), err
}
