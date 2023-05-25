package pds

import (
	"fmt"
	pds "github.com/portworx/pds-api-go-client/pds/v1alpha1"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/portworx/torpedo/pkg/errors"
)

type Driver interface {
	DeployPDSDataservices() ([]*pds.ModelsDeployment, error)
	CreateSchedulerContextForPDSApps([]*pds.ModelsDeployment) []*scheduler.Context
}

var (
	pdsschedulers = make(map[string]Driver)
)

// Get returns a registered scheduler test provider.
func Get(name string) (Driver, error) {
	if d, ok := pdsschedulers[name]; ok {
		return d, nil
	}
	return nil, &errors.ErrNotFound{
		ID:   name,
		Type: "PdsDriver",
	}
}

func Register(name string, d Driver) error {
	if _, ok := pdsschedulers[name]; !ok {
		pdsschedulers[name] = d
	} else {
		return fmt.Errorf("pds driver: %s is already registered", name)
	}
	return nil
}
