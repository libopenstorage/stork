package api

import (
	status "net/http"

	pds "github.com/portworx/pds-api-go-client/pds/v1alpha1"
	"github.com/portworx/torpedo/drivers/pds/pdsutils"
	"github.com/portworx/torpedo/pkg/log"
)

// ServiceAccount struct
type ServiceAccount struct {
	apiClient *pds.APIClient
}

// ListServiceAccounts return service accounts models for a tenant.
func (sa *ServiceAccount) ListServiceAccounts(tenantID string) ([]pds.ModelsServiceAccount, error) {
	saClient := sa.apiClient.ServiceAccountsApi
	ctx, err := pdsutils.GetContext()
	if err != nil {
		log.Errorf("Error in getting context for api call: %v\n", err)
		return nil, err
	}
	saModels, res, err := saClient.ApiTenantsIdServiceAccountsGet(ctx, tenantID).Execute()
	if err != nil && res.StatusCode != status.StatusOK {
		log.Errorf("Error when calling `ApiTenantsIdServiceAccountsGet``: %v\n", err)
		log.Errorf("Full HTTP response: %v\n", res)
		return nil, err
	}
	return saModels.GetData(), nil
}

// GetServiceAccount return service account model.
func (sa *ServiceAccount) GetServiceAccount(serviceAccountID string) (*pds.ControllersServiceAccountResponse, error) {
	saClient := sa.apiClient.ServiceAccountsApi
	ctx, err := pdsutils.GetContext()
	if err != nil {
		log.Errorf("Error in getting context for api call: %v\n", err)
		return nil, err
	}
	saModel, res, err := saClient.ApiServiceAccountsIdGet(ctx, serviceAccountID).Execute()
	if err != nil && res.StatusCode != status.StatusOK {
		log.Errorf("Error when calling `ApiServiceAccountsIdGet``: %v\n", err)
		log.Errorf("Full HTTP response: %v\n", res)
		return nil, err
	}
	return saModel, nil
}

// CreateServiceAccountToken return newly created service account.
func (sa *ServiceAccount) CreateServiceAccountToken(tenantID string, name string) (*pds.ModelsServiceAccount, error) {
	saClient := sa.apiClient.ServiceAccountsApi
	createRequest := pds.ControllersCreateServiceAccountRequest{Name: &name}
	ctx, err := pdsutils.GetContext()
	if err != nil {
		log.Errorf("Error in getting context for api call: %v\n", err)
		return nil, err
	}
	saModel, res, err := saClient.ApiTenantsIdServiceAccountsPost(ctx, tenantID).Body(createRequest).Execute()

	if err != nil && res.StatusCode != status.StatusOK {
		log.Errorf("Error when calling `ApiServiceAccountsIdTokenGet``: %v\n", err)
		log.Errorf("Full HTTP response: %v\n", res)
		return nil, err
	}
	return saModel, nil
}

// GetServiceAccountToken return service account token.
func (sa *ServiceAccount) GetServiceAccountToken(serviceAccountID string) (*pds.ControllersServiceAccountTokenResponse, error) {
	saClient := sa.apiClient.ServiceAccountsApi
	ctx, err := pdsutils.GetContext()
	if err != nil {
		log.Errorf("Error in getting context for api call: %v\n", err)
		return nil, err
	}
	saModel, res, err := saClient.ApiServiceAccountsIdTokenGet(ctx, serviceAccountID).Execute()

	if err != nil && res.StatusCode != status.StatusOK {
		log.Errorf("Error when calling `ApiServiceAccountsIdTokenGet``: %v\n", err)
		log.Errorf("Full HTTP response: %v\n", res)
		return nil, err
	}
	return saModel, nil
}

// DeleteServiceAccount delete service account and return status.
func (sa *ServiceAccount) DeleteServiceAccount(serviceAccountID string) (*status.Response, error) {
	saClient := sa.apiClient.ServiceAccountsApi
	ctx, err := pdsutils.GetContext()
	if err != nil {
		log.Errorf("Error in getting context for api call: %v\n", err)
		return nil, err
	}
	res, err := saClient.ApiServiceAccountsIdDelete(ctx, serviceAccountID).Execute()
	if err != nil && res.StatusCode != status.StatusOK {
		log.Errorf("Error when calling `ApiServiceAccountsIdDelete``: %v\n", err)
		log.Errorf("Full HTTP response: %v\n", res)
		return nil, err
	}
	return res, nil
}
