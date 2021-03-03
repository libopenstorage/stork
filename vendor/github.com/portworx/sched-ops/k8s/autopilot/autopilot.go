package autopilot

import (
	"fmt"
	"os"
	"sync"

	autopilotclientset "github.com/libopenstorage/autopilot-api/pkg/client/clientset/versioned"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

var (
	instance Ops
	once     sync.Once
)

// Ops provides an interface to Autopilot operations.
type Ops interface {
	RuleOps
	RuleObjectOps
	ActionApprovalInterface
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

// New builds a new autopilot client.
func New(c autopilotclientset.Interface) *Client {
	return &Client{
		autopilot: c,
	}
}

// NewForConfig builds a new autopilot client for the given config.
func NewForConfig(cfg *rest.Config) (*Client, error) {
	client, err := autopilotclientset.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}

	return &Client{
		autopilot: client,
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

// Client provides a wrapper for the autopilot client.
type Client struct {
	config    *rest.Config
	autopilot autopilotclientset.Interface
}

// SetConfig sets the config and resets the client.
func (c *Client) SetConfig(cfg *rest.Config) {
	c.config = cfg
	c.autopilot = nil
}

// initClient the k8s client if uninitialized
func (c *Client) initClient() error {
	if c.autopilot != nil {
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

	c.autopilot, err = autopilotclientset.NewForConfig(c.config)
	if err != nil {
		return err
	}

	return nil
}
