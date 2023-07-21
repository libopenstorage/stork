package pds

import (
	"fmt"
	pds "github.com/portworx/pds-api-go-client/pds/v1alpha1"
	pdsapi "github.com/portworx/torpedo/drivers/pds/api"
	pdscontrolplane "github.com/portworx/torpedo/drivers/pds/controlplane"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/portworx/torpedo/pkg/errors"
	"github.com/portworx/torpedo/pkg/log"
	"net/url"
)

type LoadGenParams struct {
	LoadGenDepName    string
	PdsDeploymentName string
	Namespace         string
	FailOnError       string
	Mode              string
	TableName         string
	NumOfRows         string
	Iterations        string
	Timeout           string //example 60s
	ReplacePassword   string
	ClusterMode       string
	Replicas          int32
}

type Driver interface {
	DeployPDSDataservices() ([]*pds.ModelsDeployment, error)
	CreateSchedulerContextForPDSApps(pdsApps []*pds.ModelsDeployment) ([]*scheduler.Context, error)
	ValidateDataServiceDeployment(deployment *pds.ModelsDeployment, namespace string) error
	GenerateWorkload(pdsDeployment *pds.ModelsDeployment, wkloadGenParams LoadGenParams) (string, error)
}

var (
	pdsschedulers = make(map[string]Driver)
)

func InitPdsApiComponents(ControlPlaneURL string) (*pdsapi.Components, *pdscontrolplane.ControlPlane, error) {
	log.InfoD("Initializing Api components")
	apiConf := pds.NewConfiguration()
	endpointURL, err := url.Parse(ControlPlaneURL)
	if err != nil {
		return nil, nil, err
	}
	apiConf.Host = endpointURL.Host
	apiConf.Scheme = endpointURL.Scheme

	apiClient := pds.NewAPIClient(apiConf)
	components := pdsapi.NewComponents(apiClient)
	controlplane := pdscontrolplane.NewControlPlane(ControlPlaneURL, components)

	return components, controlplane, nil
}

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
