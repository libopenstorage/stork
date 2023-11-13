package pds

import (
	"fmt"
	pds "github.com/portworx/pds-api-go-client/pds/v1alpha1"
	pdsapi "github.com/portworx/torpedo/drivers/pds/api"
	pdscontrolplane "github.com/portworx/torpedo/drivers/pds/controlplane"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/portworx/torpedo/pkg/errors"
	"github.com/portworx/torpedo/pkg/log"
	v1 "k8s.io/api/apps/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"net/url"
	"os"
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

	//DeployPDSDataservices Deploys the given PDS dataservice and retruns the models deployment object
	DeployPDSDataservices() ([]*pds.ModelsDeployment, error)

	//CreateSchedulerContextForPDSApps Creates Context for the pds deployed applications
	CreateSchedulerContextForPDSApps(pdsApps []*pds.ModelsDeployment) ([]*scheduler.Context, error)

	//ValidateDataServiceDeployment Validate the PDS deployments
	ValidateDataServiceDeployment(deployment *pds.ModelsDeployment, namespace string) error

	//InsertDataAndReturnChecksum Inserts data and returns md5 hash for the data inserted
	InsertDataAndReturnChecksum(pdsDeployment *pds.ModelsDeployment, wkloadGenParams LoadGenParams) (string, *v1.Deployment, error)

	//ReadDataAndReturnChecksum Reads data and returns md5 hash for the data
	ReadDataAndReturnChecksum(pdsDeployment *pds.ModelsDeployment, wkloadGenParams LoadGenParams) (string, *v1.Deployment, error)
}

var (
	pdsschedulers = make(map[string]Driver)
)

func GetK8sContext() (*kubernetes.Clientset, *rest.Config, error) {
	kubeconfigPath := os.Getenv("KUBECONFIG")

	// Build the client configuration from the kubeconfig file.
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfigPath)
	if err != nil {
		return nil, nil, fmt.Errorf("Error creating client configuration from kubeconfig: %v\n", err)
	}
	// Create the Kubernetes client using the configuration.
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, nil, fmt.Errorf("Error creating clientset: %v\n", err)
	}
	return clientset, config, nil

}

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
