package kubevirtdynamic

import (
	"fmt"
	"os"
	"sync"

	"github.com/portworx/sched-ops/k8s/common"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

var (
	instance Ops
	once     sync.Once
)

// Ops is an interface to perform generic Object operations
type Ops interface {
	VirtualMachineInstanceOps

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

// SetInstance replaces the instance with the provided one. Should be used only
// for testing purposes.
func SetInstance(i Ops) {
	instance = i
}

// New builds a new client.
func New(client dynamic.Interface) *Client {
	return &Client{
		client: client,
	}
}

// NewForConfig builds a new client for the given config.
func NewForConfig(c *rest.Config) (*Client, error) {
	client, err := dynamic.NewForConfig(c)
	if err != nil {
		return nil, err
	}

	return &Client{
		client: client,
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

// Client is a wrapper for the kubernetes dynamic client.
type Client struct {
	config *rest.Config
	client dynamic.Interface
}

// SetConfig sets the config and resets the client
func (c *Client) SetConfig(cfg *rest.Config) {
	c.config = cfg
	c.client = nil
}

// initClient the k8s client if uninitialized
func (c *Client) initClient() error {
	if c.client != nil {
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
	err = common.SetRateLimiter(c.config)
	if err != nil {
		return err
	}

	c.client, err = dynamic.NewForConfig(c.config)
	if err != nil {
		return err
	}

	return nil
}

// unstructuredGetValString returns a string value for the specified key from the map
func (c *Client) unstructuredGetValString(data map[string]interface{}, key string) (string, bool, error) {
	rawVal, ok := data[key]
	if !ok {
		return "", false, nil
	}
	val, ok := rawVal.(string)
	if !ok {
		return "", false, fmt.Errorf(
			"wrong type for key %q in unstructured map: expected string, actual %T", key, rawVal)
	}
	return val, true, nil
}

// unstructuredGetValString returns an int64 value for the specified key from the map
func (c *Client) unstructuredGetValInt64(data map[string]interface{}, key string) (int64, bool, error) {
	rawVal, ok := data[key]
	if !ok {
		return 0, false, nil
	}
	val, ok := rawVal.(int64)
	if !ok {
		return 0, false, fmt.Errorf("wrong type for key %q in unstructured map: expected int64, actual %T", key, rawVal)
	}
	return val, true, nil
}

// unstructuredFindKeyValInt64 scans the specified slice of maps looking for a map that contains the specified key/value pair.
// The slice members are expected to be of type map[string]interface{}.
// Returns the map if found.
func (c *Client) unstructuredFindKeyValString(
	data []interface{}, key, val string,
) (map[string]interface{}, error) {
	for _, rawMap := range data {
		typedMap, ok := rawMap.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("wrong type of element in slice: expected map[string]interface{}, actual %T", rawMap)
		}
		mapVal, found, err := c.unstructuredGetValString(typedMap, key)
		if err != nil {
			return nil, fmt.Errorf("failed to get key %q in map in the slice", key)
		} else if !found {
			continue
		}
		if mapVal == val {
			return typedMap, nil
		}
	}
	return nil, nil
}

// Similar to the above but with int64 value for the string key.
func (c *Client) unstructuredFindKeyValInt64(
	data []interface{}, key string, val int64,
) (map[string]interface{}, error) {
	for _, rawMap := range data {
		typedMap, ok := rawMap.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("wrong type of element in slice: expected map[string]interface{}, actual %T", rawMap)
		}
		mapVal, found, err := c.unstructuredGetValInt64(typedMap, key)
		if err != nil {
			return nil, fmt.Errorf("failed to get key %q in map in the slice", key)
		} else if !found {
			continue
		}
		if mapVal == val {
			return typedMap, nil
		}
	}
	return nil, nil
}
