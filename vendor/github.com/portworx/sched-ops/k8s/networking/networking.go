package networking

import (
	"fmt"
	"os"
	"sync"

	networkingv1betaclient "k8s.io/client-go/kubernetes/typed/networking/v1beta1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

var (
	instance Ops
	once     sync.Once
)

// Ops is an interface to perform kubernetes related operations on the networking resources.
type Ops interface {
	IngressOps

	// SetConfig sets the config and resets the client
	SetConfig(config *rest.Config)
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

// New builds a new networking client.
func New(networking networkingv1betaclient.NetworkingV1beta1Interface) *Client {
	return &Client{
		networking: networking,
	}
}

// NewForConfig builds a new networking client for the given config.
func NewForConfig(c *rest.Config) (*Client, error) {
	networking, err := networkingv1betaclient.NewForConfig(c)
	if err != nil {
		return nil, err
	}

	return &Client{
		networking: networking,
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

// Client provides a wrapper for the kubernetes networking client.
type Client struct {
	config     *rest.Config
	networking networkingv1betaclient.NetworkingV1beta1Interface
}

// SetConfig sets the config and resets the client
func (c *Client) SetConfig(cfg *rest.Config) {
	c.config = cfg
	c.networking = nil
}

// initClient the k8s client if uninitialized
func (c *Client) initClient() error {
	if c.networking != nil {
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

	c.networking, err = networkingv1betaclient.NewForConfig(c.config)
	if err != nil {
		return err
	}

	return nil
}
