package api

import (
	"fmt"
	status "net/http"

	pds "github.com/portworx/pds-api-go-client/pds/v1alpha1"
	"github.com/portworx/torpedo/drivers/pds/pdsutils"
	"github.com/portworx/torpedo/pkg/log"
)

// Tenant struct
type Tenant struct {
	apiClient *pds.APIClient
}

// GetTenantsList return pds tenants models.
func (tenant *Tenant) GetTenantsList(accountID string) ([]pds.ModelsTenant, error) {
	tenantClient := tenant.apiClient.TenantsApi
	log.Info("Get list of tenants.")
	ctx, err := pdsutils.GetContext()
	if err != nil {
		return nil, fmt.Errorf("Error in getting context for api call: %v\n", err)
	}
	tenantsModel, res, err := tenantClient.ApiAccountsIdTenantsGet(ctx, accountID).Execute()
	if err != nil && res.StatusCode != status.StatusOK {
		return nil, fmt.Errorf("Error when calling `ApiAccountsIdTenantsGet`: %v\n.Full HTTP response: %v", err, res)
	}
	return tenantsModel.GetData(), nil
}

// GetTenant return tenant model.
func (tenant *Tenant) GetTenant(tenantID string) (*pds.ModelsTenant, error) {
	tenantClient := tenant.apiClient.TenantsApi
	log.Info("Get tenant.")
	ctx, err := pdsutils.GetContext()
	if err != nil {
		return nil, fmt.Errorf("Error in getting context for api call: %v\n", err)
	}
	tenantModel, res, err := tenantClient.ApiTenantsIdGet(ctx, tenantID).Execute()
	if err != nil && res.StatusCode != status.StatusOK {
		return nil, fmt.Errorf("Error when calling `ApiTenantsIdGet`: %v\n.Full HTTP response: %v", err, res)
	}
	return tenantModel, nil
}

// GetDNS return DNS details for the tenant.
func (tenant *Tenant) GetDNS(tenantID string) (*pds.ModelsDNSDetails, error) {
	tenantClient := tenant.apiClient.TenantsApi
	log.Info("Get tenant.")
	ctx, err := pdsutils.GetContext()
	if err != nil {
		return nil, fmt.Errorf("Error in getting context for api call: %v\n", err)
	}
	tenantDNSModel, res, err := tenantClient.ApiTenantsIdDnsDetailsGet(ctx, tenantID).Execute()
	if err != nil && res.StatusCode != status.StatusOK {
		return nil, fmt.Errorf("Error when calling `ApiTenantsIdDnsDetailsGet`: %v\n.Full HTTP response: %v", err, res)
	}
	return tenantDNSModel, nil
}
