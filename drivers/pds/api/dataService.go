package api

import (
	status "net/http"

	pds "github.com/portworx/pds-api-go-client/pds/v1alpha1"
	"github.com/portworx/torpedo/drivers/pds/pdsutils"
	log "github.com/sirupsen/logrus"
)

// DataService struct
type DataService struct {
	apiClient *pds.APIClient
}

// ListDataServices return data services models.
func (ds *DataService) ListDataServices() ([]pds.ModelsDataService, error) {
	dsClient := ds.apiClient.DataServicesApi
	ctx, err := pdsutils.GetContext()
	if err != nil {
		log.Errorf("Error in getting context for api call: %v\n", err)
		return nil, err
	}
	dsModels, res, err := dsClient.ApiDataServicesGet(ctx).Execute()

	if res.StatusCode != status.StatusOK {
		log.Errorf("Error when calling `ApiDataServicesGet``: %v\n", err)
		log.Errorf("Full HTTP response: %v\n", res)
	}
	return dsModels.GetData(), err
}

// GetDataService return data service model.
func (ds *DataService) GetDataService(dataServiceID string) (*pds.ModelsDataService, error) {
	dsClient := ds.apiClient.DataServicesApi
	ctx, err := pdsutils.GetContext()
	if err != nil {
		log.Errorf("Error in getting context for api call: %v\n", err)
		return nil, err
	}
	dsModel, res, err := dsClient.ApiDataServicesIdGet(ctx, dataServiceID).Execute()

	if res.StatusCode != status.StatusOK {
		log.Errorf("Error when calling `ApiDataServicesIdGet``: %v\n", err)
		log.Errorf("Full HTTP response: %v\n", res)
	}
	return dsModel, err
}
