package api

import (
	"fmt"
	status "net/http"

	pds "github.com/portworx/pds-api-go-client/pds/v1alpha1"
	"github.com/portworx/torpedo/pkg/log"
)

// StorageSettingsTemplate struct
type StorageSettingsTemplate struct {
	apiClient *pds.APIClient
}

// ListTemplates return storage options templates models.
func (st *StorageSettingsTemplate) ListTemplates(tenantID string) ([]pds.ModelsStorageOptionsTemplate, error) {
	stClient := st.apiClient.StorageOptionsTemplatesApi
	log.Infof("Get list of storage templates for tenant ID - %v", tenantID)
	ctx, err := GetContext()
	if err != nil {
		return nil, fmt.Errorf("Error in getting context for api call: %v\n", err)
	}
	pdsStorageTemplates, res, err := stClient.ApiTenantsIdStorageOptionsTemplatesGet(ctx, tenantID).Execute()
	if err != nil && res.StatusCode != status.StatusOK {
		return nil, fmt.Errorf("Error when calling `ApiTenantsIdStorageOptionsTemplatesGet`: %v\n.Full HTTP response: %v", err, res)
	}
	return pdsStorageTemplates.GetData(), nil
}

// GetTemplate return storage options templates models.
func (st *StorageSettingsTemplate) GetTemplate(templateID string) (*pds.ModelsStorageOptionsTemplate, error) {
	stClient := st.apiClient.StorageOptionsTemplatesApi
	log.Infof("Get storage template details for UUID - %v", templateID)
	ctx, err := GetContext()
	if err != nil {
		return nil, fmt.Errorf("Error in getting context for api call: %v\n", err)
	}
	stModel, res, err := stClient.ApiStorageOptionsTemplatesIdGet(ctx, templateID).Execute()
	if err != nil && res.StatusCode != status.StatusOK {
		return nil, fmt.Errorf("Error when calling `ApiStorageOptionsTemplatesIdGet`: %v\n.Full HTTP response: %v", err, res)
	}
	return stModel, nil
}

// CreateTemplate return newly created storage option template model.
func (st *StorageSettingsTemplate) CreateTemplate(tenantID string, fg bool, fs string, name string, provisioner string, repl int32, secure bool) (*pds.ModelsStorageOptionsTemplate, error) {
	stClient := st.apiClient.StorageOptionsTemplatesApi
	log.Info("Create new storage template.")
	createRequest := pds.ControllersCreateStorageOptionsTemplateRequest{Fg: &fg, Fs: &fs, Name: &name, Provisioner: &provisioner, Repl: &repl, Secure: &secure}
	ctx, err := GetContext()
	if err != nil {
		return nil, fmt.Errorf("Error in getting context for api call: %v\n", err)
	}
	stModel, res, err := stClient.ApiTenantsIdStorageOptionsTemplatesPost(ctx, tenantID).Body(createRequest).Execute()
	if err != nil && res.StatusCode != status.StatusOK {
		return nil, fmt.Errorf("Error when calling `ApiTenantsIdStorageOptionsTemplatesPost`: %v\n.Full HTTP response: %v", err, res)
	}
	return stModel, nil
}

// UpdateTemplate return updatedd@12 storage option template model.
func (st *StorageSettingsTemplate) UpdateTemplate(templateID string, fg bool, fs string, name string, repl int32, secure bool) (*pds.ModelsStorageOptionsTemplate, error) {
	stClient := st.apiClient.StorageOptionsTemplatesApi
	log.Info("Create new storage template.")
	updateRequest := pds.ControllersUpdateStorageOptionsTemplateRequest{Fg: &fg, Fs: &fs, Name: &name, Repl: &repl, Secure: &secure}
	ctx, err := GetContext()
	if err != nil {
		return nil, fmt.Errorf("Error in getting context for api call: %v\n", err)
	}
	stModel, res, err := stClient.ApiStorageOptionsTemplatesIdPut(ctx, templateID).Body(updateRequest).Execute()
	if err != nil && res.StatusCode != status.StatusOK {
		return nil, fmt.Errorf("Error when calling `ApiStorageOptionsTemplatesIdPut`: %v\n.Full HTTP response: %v", err, res)
	}
	return stModel, nil
}

// DeleteTemplate delete the storage option template and return the status
func (st *StorageSettingsTemplate) DeleteTemplate(templateID string) (*status.Response, error) {
	stClient := st.apiClient.StorageOptionsTemplatesApi
	log.Infof("Delete strogae template: %v", templateID)
	ctx, err := GetContext()
	if err != nil {
		return nil, fmt.Errorf("Error in getting context for api call: %v\n", err)
	}
	res, err := stClient.ApiStorageOptionsTemplatesIdDelete(ctx, templateID).Execute()
	if err != nil && res.StatusCode != status.StatusOK {
		return nil, fmt.Errorf("Error when calling `ApiStorageOptionsTemplatesIdDelete`: %v\n.Full HTTP response: %v", err, res)
	}
	return res, nil
}
