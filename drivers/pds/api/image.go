package api

import (
	"fmt"
	status "net/http"

	pds "github.com/portworx/pds-api-go-client/pds/v1alpha1"
	"github.com/portworx/torpedo/drivers/pds/pdsutils"
)

// Image struct
type Image struct {
	apiClient *pds.APIClient
}

// ListImages return images models for given version.
func (img *Image) ListImages(versionID string) ([]pds.ModelsImage, error) {
	imgClient := img.apiClient.ImagesApi
	ctx, err := pdsutils.GetContext()
	if err != nil {
		return nil, fmt.Errorf("Error in getting context for api call: %v\n", err)
	}
	imgModels, res, err := imgClient.ApiVersionsIdImagesGet(ctx, versionID).Execute()
	if err != nil && res.StatusCode != status.StatusOK {
		return nil, fmt.Errorf("Error when calling `ApiDeploymentsIdBackupsGet`: %v\n.Full HTTP response: %v", err, res)
	}
	return imgModels.GetData(), err
}

// GetImage return image model.
func (img *Image) GetImage(imageID string) (*pds.ModelsImage, error) {
	imgClient := img.apiClient.ImagesApi
	ctx, err := pdsutils.GetContext()
	if err != nil {
		return nil, fmt.Errorf("Error in getting context for api call: %v\n", err)
	}
	imgModel, res, err := imgClient.ApiImagesIdGet(ctx, imageID).Execute()
	if err != nil && res.StatusCode != status.StatusOK {
		return nil, fmt.Errorf("Error when calling `ApiDeploymentsIdBackupsGet`: %v\n.Full HTTP response: %v", err, res)
	}
	return imgModel, err
}
