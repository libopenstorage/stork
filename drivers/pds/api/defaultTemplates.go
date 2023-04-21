package api

import (
	"fmt"
	status "net/http"

	pds "github.com/portworx/pds-api-go-client/pds/v1alpha1"
	"github.com/portworx/torpedo/drivers/pds/pdsutils"
	"github.com/portworx/torpedo/pkg/log"
)

// DefaultTemplates struct
type DefaultTemplates struct {
	apiClient *pds.APIClient
}

// ListApplicationConfigurationTemplates returns application configuration templates for the given account
func (ds *DefaultTemplates) ListApplicationConfigurationTemplates(tenantID string) ([]pds.ModelsApplicationConfigurationTemplate, error) {
	dsClient := ds.apiClient.ApplicationConfigurationTemplatesApi
	ctx, err := pdsutils.GetContext()
	if err != nil {
		return nil, fmt.Errorf("failed to get context, Err %v", err)
	}
	dsModels, res, err := dsClient.ApiTenantsIdApplicationConfigurationTemplatesGet(ctx, tenantID).Execute()

	if err != nil {
		return nil, fmt.Errorf("failed to get application configuration templates for given tenant, Error: %v %v", err, res)
	}
	if res.StatusCode != status.StatusOK {
		log.Errorf("Error when calling `ApiDataServicesGet``: %v\n", err)
		log.Errorf("Full HTTP response: %v\n", res)
	}
	return dsModels.GetData(), err
}

// ListResourceSettingTemplates returns resource setting templates for the given account
func (ds *DefaultTemplates) ListResourceSettingTemplates(tenantID string) ([]pds.ModelsResourceSettingsTemplate, error) {
	dsClient := ds.apiClient.ResourceSettingsTemplatesApi
	ctx, err := pdsutils.GetContext()
	if err != nil {
		return nil, fmt.Errorf("Error in getting context for api call: %v\n", err)
	}
	dsModels, res, err := dsClient.ApiTenantsIdResourceSettingsTemplatesGet(ctx, tenantID).Execute()

	if res.StatusCode != status.StatusOK {
		log.Errorf("Error when calling `ApiDataServicesGet``: %v\n", err)
		log.Errorf("Full HTTP response: %v\n", res)
	}
	return dsModels.GetData(), err
}

// ListStorageOptionsTemplates returns storage options templates for the given account
func (ds *DefaultTemplates) ListStorageOptionsTemplates(tenantID string) ([]pds.ModelsStorageOptionsTemplate, error) {
	dsClient := ds.apiClient.StorageOptionsTemplatesApi
	ctx, err := pdsutils.GetContext()
	if err != nil {
		log.Errorf("Error in getting context for api call: %v\n", err)
		return nil, err
	}
	dsModels, res, err := dsClient.ApiTenantsIdStorageOptionsTemplatesGet(ctx, tenantID).Execute()

	if res.StatusCode != status.StatusOK {
		log.Errorf("Error when calling `ApiDataServicesGet``: %v\n", err)
		log.Errorf("Full HTTP response: %v\n", res)
	}
	return dsModels.GetData(), err
}
