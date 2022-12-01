// Package api comprises of all the components and associated CRUD functionality
package api

import (
	status "net/http"

	pds "github.com/portworx/pds-api-go-client/pds/v1alpha1"
	"github.com/portworx/torpedo/drivers/pds/pdsutils"
	"github.com/portworx/torpedo/pkg/log"
)

// Version struct
type Version struct {
	apiClient *pds.APIClient
}

// ListDataServiceVersions return pds versions models.
func (v *Version) ListDataServiceVersions(dataServiceID string) ([]pds.ModelsVersion, error) {
	versionClient := v.apiClient.VersionsApi
	ctx, err := pdsutils.GetContext()
	if err != nil {
		log.Errorf("Error in getting context for api call: %v\n", err)
		return nil, err
	}
	versionModels, res, err := versionClient.ApiDataServicesIdVersionsGet(ctx, dataServiceID).Execute()
	if res.StatusCode != status.StatusOK {
		log.Errorf("Error when calling `ApiDataServicesIdVersionsGet``: %v\n", err)
		log.Errorf("Full HTTP response: %v\n", res)
	}
	return versionModels.GetData(), err
}

// GetVersion return pds version model.
func (v *Version) GetVersion(versionID string) (*pds.ModelsVersion, error) {
	versionClient := v.apiClient.VersionsApi
	ctx, err := pdsutils.GetContext()
	if err != nil {
		log.Errorf("Error in getting context for api call: %v\n", err)
		return nil, err
	}
	versionModel, res, err := versionClient.ApiVersionsIdGet(ctx, versionID).Execute()
	if res.StatusCode != status.StatusOK {
		log.Errorf("Error when calling `ApiVersionsIdGet``: %v\n", err)
		log.Errorf("Full HTTP response: %v\n", res)
	}
	return versionModel, err
}
