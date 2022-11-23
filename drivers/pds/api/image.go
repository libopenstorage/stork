package api

import (
	status "net/http"

	pds "github.com/portworx/pds-api-go-client/pds/v1alpha1"
	"github.com/portworx/torpedo/drivers/pds/pdsutils"
	"github.com/portworx/torpedo/pkg/log"
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
		log.Errorf("Error in getting context for api call: %v\n", err)
		return nil, err
	}
	imgModels, res, err := imgClient.ApiVersionsIdImagesGet(ctx, versionID).Execute()

	if res.StatusCode != status.StatusOK {
		log.Errorf("Error when calling `ApiVersionsIdImagesGet``: %v\n", err)
		log.Errorf("Full HTTP response: %v\n", res)
	}
	return imgModels.GetData(), err
}

// GetImage return image model.
func (img *Image) GetImage(imageID string) (*pds.ModelsImage, error) {
	imgClient := img.apiClient.ImagesApi
	ctx, err := pdsutils.GetContext()
	if err != nil {
		log.Errorf("Error in getting context for api call: %v\n", err)
		return nil, err
	}
	imgModel, res, err := imgClient.ApiImagesIdGet(ctx, imageID).Execute()

	if res.StatusCode != status.StatusOK {
		log.Errorf("Error when calling `ApiImagesIdGet``: %v\n", err)
		log.Errorf("Full HTTP response: %v\n", res)
	}
	return imgModel, err
}
