// Package api comprises of all the components and associated CRUD functionality
package api

import (
	status "net/http"

	pds "github.com/portworx/pds-api-go-client/pds/v1alpha1"
	"github.com/portworx/torpedo/drivers/pds/pdsutils"
	"github.com/portworx/torpedo/pkg/log"
)

// ResourceSettingsTemplate struct
type ResourceSettingsTemplate struct {
	apiClient *pds.APIClient
}

// ListTemplates return pds resource setting templates models.
func (rt *ResourceSettingsTemplate) ListTemplates(tenantID string) ([]pds.ModelsResourceSettingsTemplate, error) {
	rtClient := rt.apiClient.ResourceSettingsTemplatesApi
	ctx, err := pdsutils.GetContext()
	if err != nil {
		log.Errorf("Error in getting context for api call: %v\n", err)
		return nil, err
	}
	rtModel, res, err := rtClient.ApiTenantsIdResourceSettingsTemplatesGet(ctx, tenantID).Execute()

	if err != nil && res.StatusCode != status.StatusOK {
		log.Errorf("Error when calling `ApiTenantsIdResourceSettingsTemplatesGet``: %v\n", err)
		log.Errorf("Full HTTP response: %v\n", res)
		return nil, err
	}
	return rtModel.GetData(), nil
}

// GetTemplate return pds resource setting template model.
func (rt *ResourceSettingsTemplate) GetTemplate(templateID string) (*pds.ModelsResourceSettingsTemplate, error) {
	rtClient := rt.apiClient.ResourceSettingsTemplatesApi
	ctx, err := pdsutils.GetContext()
	if err != nil {
		log.Errorf("Error in getting context for api call: %v\n", err)
		return nil, err
	}
	rtModel, res, err := rtClient.ApiResourceSettingsTemplatesIdGet(ctx, templateID).Execute()

	if err != nil && res.StatusCode != status.StatusOK {
		log.Errorf("Error when calling `ApiResourceSettingsTemplatesIdGet``: %v\n", err)
		log.Errorf("Full HTTP response: %v\n", res)
		return nil, err
	}
	return rtModel, nil
}

// CreateTemplate return newly created pds resource setting template model.
func (rt *ResourceSettingsTemplate) CreateTemplate(tenantID string, cpuLimit string, cpuRequest string, dataServiceID string, memoryLimit string, memoryRequest string, name string, storageRequest string) (*pds.ModelsResourceSettingsTemplate, error) {
	rtClient := rt.apiClient.ResourceSettingsTemplatesApi
	createRequest := pds.ControllersCreateResourceSettingsTemplateRequest{CpuLimit: &cpuLimit, CpuRequest: &cpuRequest, DataServiceId: &dataServiceID, MemoryLimit: &memoryLimit, MemoryRequest: &memoryRequest, Name: &name, StorageRequest: &storageRequest}
	ctx, err := pdsutils.GetContext()
	if err != nil {
		log.Errorf("Error in getting context for api call: %v\n", err)
		return nil, err
	}
	rtModel, res, err := rtClient.ApiTenantsIdResourceSettingsTemplatesPost(ctx, tenantID).Body(createRequest).Execute()

	if err != nil && res.StatusCode != status.StatusOK {
		log.Errorf("Error when calling `ApiTenantsIdResourceSettingsTemplatesPost``: %v\n", err)
		log.Errorf("Full HTTP response: %v\n", res)
		return nil, err
	}
	return rtModel, nil
}

// UpdateTemplate return updated created pds resource setting template model.
func (rt *ResourceSettingsTemplate) UpdateTemplate(templateID string, cpuLimit string, cpuRequest string, memoryLimit string, memoryRequest string, name string, storageRequest string) (*pds.ModelsResourceSettingsTemplate, error) {
	rtClient := rt.apiClient.ResourceSettingsTemplatesApi
	updateRequest := pds.ControllersUpdateResourceSettingsTemplateRequest{CpuLimit: &cpuLimit, CpuRequest: &cpuRequest, MemoryLimit: &memoryLimit, MemoryRequest: &memoryRequest, Name: &name, StorageRequest: &storageRequest}
	ctx, err := pdsutils.GetContext()
	if err != nil {
		log.Errorf("Error in getting context for api call: %v\n", err)
		return nil, err
	}
	rtModel, res, err := rtClient.ApiResourceSettingsTemplatesIdPut(ctx, templateID).Body(updateRequest).Execute()

	if err != nil && res.StatusCode != status.StatusOK {
		log.Errorf("Error when calling `ApiResourceSettingsTemplatesIdPut``: %v\n", err)
		log.Errorf("Full HTTP response: %v\n", res)
		return nil, err
	}
	return rtModel, nil
}

// DeleteTemplate delete resource setting template and return status.
func (rt *ResourceSettingsTemplate) DeleteTemplate(templateID string) (*status.Response, error) {
	rtClient := rt.apiClient.ResourceSettingsTemplatesApi
	ctx, err := pdsutils.GetContext()
	if err != nil {
		log.Errorf("Error in getting context for api call: %v\n", err)
		return nil, err
	}
	res, err := rtClient.ApiResourceSettingsTemplatesIdDelete(ctx, templateID).Execute()

	if err != nil && res.StatusCode != status.StatusOK {
		log.Errorf("Error when calling `ApiResourceSettingsTemplatesIdDelete``: %v\n", err)
		log.Errorf("Full HTTP response: %v\n", res)
		return nil, err
	}
	return res, nil
}
