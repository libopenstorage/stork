package api

import (
	"fmt"
	pds "github.com/portworx/pds-api-go-client/pds/v1alpha1"
	"net/http"
	status "net/http"
)

// Whoami struct
type Whoami struct {
	apiClient *pds.APIClient
}

// WhoAmI Fetches the details of the current calling actor (user or service account)
func (whoami *Whoami) WhoAmI() (*pds.ControllersWhoAmIResponse, *http.Response, error) {
	whoamiClient := whoami.apiClient.WhoAmIApi
	ctx, err := GetContext()
	if err != nil {
		return nil, nil, fmt.Errorf("Error in getting context for api call: %v\n", err)
	}
	whoamiResp, httpResp, err := whoamiClient.ApiWhoamiGet(ctx).Execute()
	if err != nil && httpResp.StatusCode != status.StatusOK {
		return nil, nil, fmt.Errorf("Error when calling `ApiTenantsIdGet`: %v\n.Full HTTP response: %v", err, httpResp)
	}

	return whoamiResp, httpResp, nil
}
