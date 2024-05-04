package anthos

import (
	"context"
	"fmt"
	"os"
	"sync"

	"github.com/portworx/sched-ops/k8s/common"
	gkeonprem "google.golang.org/api/gkeonprem/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	"k8s.io/client-go/dynamic"
)

var (
	instance Ops
	once     sync.Once
)

const (
	GroupName = "cluster.k8s.io"
)

// Ops is an interface to perform kubernetes related operation in Anthos Cluster.
type Ops interface {
	// OnpremOps is an interface to perform Anthos onprem operations
	OnpremOps
	// ClusterOps is an interface to perform anthos cluster operations
	ClusterOps
	// SetConfig sets the config and resets the client
	SetConfig(config *rest.Config)
}

// Client is a wrapper for the anthos resource interfaces.
type Client struct {
	config                                                *rest.Config
	dynamicClient                                         dynamic.Interface
	kube                                                  kubernetes.Interface
	service                                               *gkeonprem.Service
	projectLocationService                                *gkeonprem.ProjectsLocationsService
	projectLocationVmwareClusterService                   *gkeonprem.ProjectsLocationsVmwareClustersService
	projectLocationBareMetalClusterService                *gkeonprem.ProjectsLocationsBareMetalAdminClustersService
	projectsLocationsVmwareClustersVmwareNodePoolsService *gkeonprem.ProjectsLocationsVmwareClustersVmwareNodePoolsService
}

// Instance returns a singleton instance of the client.
func Instance() Ops {
	once.Do(func() {
		if instance == nil {
			instance = &Client{}
		}
	})
	return instance
}

// SetInstance replaces the instance with the provided one. Should be used only for testing purposes.
func SetInstance(i Ops) {
	instance = i
}

// New builds a new client.
func New(kube kubernetes.Interface, service *gkeonprem.Service) *Client {
	return &Client{
		kube:    kube,
		service: service,
	}
}

// NewForConfig builds a new client for the given config.
func NewForConfig(c *rest.Config) (*Client, error) {
	kube, err := kubernetes.NewForConfig(c)
	if err != nil {
		return nil, err
	}

	service, err := gkeonprem.NewService(context.Background())
	if err != nil {
		return nil, err
	}
	return &Client{
		kube:    kube,
		service: service,
	}, nil
}

// NewInstanceFromConfigFile returns new instance of client by using given
// config file
func NewInstanceFromConfigFile(config string) (Ops, error) {
	newInstance := &Client{}
	err := newInstance.loadClientFromKubeconfig(config)
	if err != nil {
		return nil, err
	}
	return newInstance, nil
}

// SetConfig sets the config and resets the client
func (c *Client) SetConfig(cfg *rest.Config) {
	c.config = cfg
	c.kube = nil
	c.service = &gkeonprem.Service{}
	c.dynamicClient = nil
}

// initClient the k8s client if uninitialized
func (c *Client) initClient() error {
	if c.kube != nil && c.service != nil && c.dynamicClient != nil {
		return nil
	}

	return c.setClient()
}

// setClient instantiates a client.
func (c *Client) setClient() error {
	var err error

	if c.config != nil {
		if err = c.loadClient(); err != nil {
			return err
		}
	} else {
		kubeconfig := os.Getenv("KUBECONFIG")
		if len(kubeconfig) > 0 {
			if err = c.loadClientFromKubeconfig(kubeconfig); err != nil {
				return err
			}
		} else {
			if err = c.loadClientFromServiceAccount(); err != nil {
				return err
			}
		}
	}
	if c.dynamicClient, err = dynamic.NewForConfig(c.config); err != nil {
		return err
	}
	return err
}

// loadClientFromServiceAccount loads a k8s client from a ServiceAccount specified in the pod running px
func (c *Client) loadClientFromServiceAccount() error {
	config, err := rest.InClusterConfig()
	if err != nil {
		return err
	}
	c.config = config
	return c.loadClient()
}

func (c *Client) loadClientFromKubeconfig(kubeconfig string) error {
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		return err
	}
	c.config = config
	return c.loadClient()
}

func (c *Client) loadClient() error {
	if c.config == nil {
		return fmt.Errorf("rest config is not provided")
	}
	var err error
	err = common.SetRateLimiter(c.config)
	if err != nil {
		return err
	}
	c.kube, err = kubernetes.NewForConfig(c.config)
	if err != nil {
		return err
	}
	ctx := context.Background()
	c.service, err = gkeonprem.NewService(ctx)
	if err != nil {
		return fmt.Errorf("unable to create a gke onprem service. Err: %s", err)
	}
	c.projectLocationService = gkeonprem.NewProjectsLocationsService(c.service)
	c.projectLocationVmwareClusterService = gkeonprem.NewProjectsLocationsVmwareClustersService(c.service)
	c.projectLocationBareMetalClusterService = gkeonprem.NewProjectsLocationsBareMetalAdminClustersService(c.service)
	c.projectsLocationsVmwareClustersVmwareNodePoolsService = gkeonprem.NewProjectsLocationsVmwareClustersVmwareNodePoolsService(c.service)

	return nil
}
