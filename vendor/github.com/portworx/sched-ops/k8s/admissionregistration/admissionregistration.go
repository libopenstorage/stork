package admissionregistration

import (
	"fmt"
	"os"
	"sync"

	apiadmissionsclient "k8s.io/client-go/kubernetes/typed/admissionregistration/v1beta1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

var (
	instance Ops
	once     sync.Once
)

// Ops is an interface to the admission client wrapper.
type Ops interface {
	MutatingWebhookConfigurationOps

	// SetConfig sets the config and resets the client.
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

// New builds a new admissionregistration client.
func New(client apiadmissionsclient.AdmissionregistrationV1beta1Interface) *Client {
	return &Client{
		admission: client,
	}
}

// NewForConfig builds a new admissionregistration client for the given config.
func NewForConfig(c *rest.Config) (*Client, error) {
	client, err := apiadmissionsclient.NewForConfig(c)
	if err != nil {
		return nil, err
	}

	return &Client{
		admission: client,
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

// Client provides a wrapper for kubernetes admission interface.
type Client struct {
	config    *rest.Config
	admission apiadmissionsclient.AdmissionregistrationV1beta1Interface
}

// SetConfig sets the config and resets the client.
func (c *Client) SetConfig(cfg *rest.Config) {
	c.config = cfg
	c.admission = nil
}

// initClient the k8s client if uninitialized
func (c *Client) initClient() error {
	if c.admission != nil {
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

	c.admission, err = apiadmissionsclient.NewForConfig(c.config)
	if err != nil {
		return err
	}

	return nil
}
