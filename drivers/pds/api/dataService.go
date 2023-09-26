package api

import (
	"fmt"
	status "net/http"

	pds "github.com/portworx/pds-api-go-client/pds/v1alpha1"
)

// DataService struct
type DataService struct {
	apiClient *pds.APIClient
}

// ListDataServices return data services models.
func (ds *DataService) ListDataServices() ([]pds.ModelsDataService, error) {
	dsClient := ds.apiClient.DataServicesApi
	ctx, err := GetContext()
	if err != nil {
		return nil, fmt.Errorf("Error in getting context for api call: %v\n", err)
	}
	dsModels, res, err := dsClient.ApiDataServicesGet(ctx).Execute()
	if err != nil && res.StatusCode != status.StatusOK {
		return nil, fmt.Errorf("Error when calling `ApiDataServicesGet`: %v\n.Full HTTP response: %v", err, res)
	}
	return dsModels.GetData(), err
}

// GetDataService return data service model.
func (ds *DataService) GetDataService(dataServiceID string) (*pds.ModelsDataService, error) {
	dsClient := ds.apiClient.DataServicesApi
	ctx, err := GetContext()
	if err != nil {
		return nil, fmt.Errorf("Error in getting context for api call: %v\n", err)
	}
	dsModel, res, err := dsClient.ApiDataServicesIdGet(ctx, dataServiceID).Execute()
	if err != nil && res.StatusCode != status.StatusOK {
		return nil, fmt.Errorf("Error when calling `ApiDataServicesIdGet`: %v\n.Full HTTP response: %v", err, res)
	}
	return dsModel, err
}
