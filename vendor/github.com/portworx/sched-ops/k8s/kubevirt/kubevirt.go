package kubevirt

import (
	"fmt"
	"os"
	"sync"

	"github.com/portworx/sched-ops/k8s/common"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	kubecli "kubevirt.io/client-go/kubecli"
)

var (
	instance Ops
	once     sync.Once
)

// Ops is an interface to perform kubernetes related operations on the core resources.
type Ops interface {
	GetKubevirtClient() kubecli.KubevirtClient
	VirtualMachineOps
	VirtualMachineInstanceOps

	// SetConfig sets the config and resets the client
	SetConfig(config *rest.Config)

	// GetVersion gets the version of kubevirt control plane
	GetVersion() (string, error)
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

// New creates a new client.
func new(kv kubecli.KubevirtClient) *Client {
	return &Client{
		kubevirt: kv,
	}
}

// NewForConfig creates a new client for the given config.
func NewForConfig(c *rest.Config) (*Client, error) {
	kv, err := kubecli.GetKubevirtClientFromRESTConfig(c)
	if err != nil {
		return nil, err
	}

	return &Client{
		kubevirt: kv,
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

// Client is a wrapper for the kubevirt client Ops
type Client struct {
	config   *rest.Config
	kubevirt kubecli.KubevirtClient
}

// SetConfig sets the config and resets the client
func (c *Client) SetConfig(cfg *rest.Config) {
	c.config = cfg
	c.kubevirt = nil
}

// GetVersion gets the version of kubevirt control plane
func (c *Client) GetVersion() (string, error) {
	if err := c.initClient(); err != nil {
		return "", err
	}
	versionInfo, err := c.kubevirt.ServerVersion().Get()
	if err != nil {
		return "", err
	}
	version := versionInfo.String()
	return version, nil
}

// initClient the k8s client if uninitialized
func (c *Client) initClient() error {
	if c.kubevirt != nil {
		return nil
	}

	return c.setClient()
}

// setClient instantiates a client.
func (c *Client) setClient() error {
	var err error

	if c.config != nil {
		err = c.loadClient()
	} else {
		kubeconfig := os.Getenv("KUBECONFIG")
		if len(kubeconfig) > 0 {
			err = c.loadClientFromKubeconfig(kubeconfig)
		} else {
			err = c.loadClientFromServiceAccount()
		}

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

// loadClientFromKubeconfig loads a k8s client from a kubeconfig file
func (c *Client) loadClientFromKubeconfig(kubeconfig string) error {
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		return err
	}

	c.config = config
	return c.loadClient()
}

// loadClient loads the k8s client
func (c *Client) loadClient() error {
	if c.config == nil {
		return fmt.Errorf("rest config is not provided")
	}

	var err error
	err = common.SetRateLimiter(c.config)
	if err != nil {
		return err
	}
	c.kubevirt, err = kubecli.GetKubevirtClientFromRESTConfig(c.config)
	if err != nil {
		return err
	}

	return nil
}

// GetKubevirtClient get kubevirt client
func (c *Client) GetKubevirtClient() kubecli.KubevirtClient {
	if err := c.initClient(); err != nil {
		return nil
	}
	return c.kubevirt
}
