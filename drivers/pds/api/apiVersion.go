// Package api comprises of all the components and associated CRUD functionality
package api

import (
	"fmt"
	status "net/http"

	pds "github.com/portworx/pds-api-go-client/pds/v1alpha1"
	"github.com/portworx/torpedo/drivers/pds/pdsutils"
)

// PDSVersion struct
type PDSVersion struct {
	apiClient *pds.APIClient
}

// GetHelmChartVersion function return latest pds helm chart version.
func (v *PDSVersion) GetHelmChartVersion() (string, error) {
	versionClient := v.apiClient.APIVersionApi
	ctx, err := pdsutils.GetContext()
	if err != nil {
		return "", fmt.Errorf("Error in getting context for api call: %v\n", err)
	}
	versionModel, res, err := versionClient.ApiVersionGet(ctx).Execute()
	if res.StatusCode != status.StatusOK {
		return "", fmt.Errorf("Error when calling `ApiVersionGet`: %v\n.Full HTTP response: %v", err, res)
	}
	return versionModel.GetHelmChartVersion(), err
}
