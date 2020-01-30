package dataexport

import "github.com/libopenstorage/stork/pkg/dataexport/controllers"

// Controller is a kubernetes controller for DataExport resource.
type Controller struct {
}

// New returns an instance of DataExport controller.
func New() *Controller {
	return &Controller{}
}

// Init configures DataExport controller.
func (c Controller) Init() error {
	deController, err := controllers.NewDataExportController()
	if err != nil {
		return err
	}
	if err = deController.Init(); err != nil {
		return err
	}

	jobController, err := controllers.NewJobController()
	if err != nil {
		return err
	}
	if err = jobController.Init(); err != nil {
		return err
	}

	return nil
}
