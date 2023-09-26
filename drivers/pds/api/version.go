// Package api comprises of all the components and associated CRUD functionality
package api

import (
	"fmt"
	status "net/http"

	pds "github.com/portworx/pds-api-go-client/pds/v1alpha1"
)

// Version struct
type Version struct {
	apiClient *pds.APIClient
}

// ListDataServiceVersions return pds versions models.
func (v *Version) ListDataServiceVersions(dataServiceID string) ([]pds.ModelsVersion, error) {
	versionClient := v.apiClient.VersionsApi
	ctx, err := GetContext()
	if err != nil {
		return nil, fmt.Errorf("Error in getting context for api call: %v\n", err)
	}
	versionModels, res, err := versionClient.ApiDataServicesIdVersionsGet(ctx, dataServiceID).Execute()
	if err != nil && res.StatusCode != status.StatusOK {
		return nil, fmt.Errorf("Error when calling `ApiDataServicesIdVersionsGet`: %v\n.Full HTTP response: %v", err, res)
	}
	return versionModels.GetData(), err
}

// GetVersion return pds version model.
func (v *Version) GetVersion(versionID string) (*pds.ModelsVersion, error) {
	versionClient := v.apiClient.VersionsApi
	ctx, err := GetContext()
	if err != nil {
		return nil, fmt.Errorf("Error in getting context for api call: %v\n", err)
	}
	versionModel, res, err := versionClient.ApiVersionsIdGet(ctx, versionID).Execute()
	if err != nil && res.StatusCode != status.StatusOK {
		return nil, fmt.Errorf("Error when calling `ApiVersionsIdGet`: %v\n.Full HTTP response: %v", err, res)
	}
	return versionModel, err
}
