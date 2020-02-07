package dynamic

import (
	"fmt"
	"os"
	"strings"
	"sync"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
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
	// GetObject returns the latest object given a generic Object
	GetObject(object runtime.Object) (runtime.Object, error)
	// UpdateObject updates a generic Object
	UpdateObject(object runtime.Object) (runtime.Object, error)
	// ListObjects returns a list of generic Objects using the options
	ListObjects(options *metav1.ListOptions, namespace string) (*unstructured.UnstructuredList, error)

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

// GetObject returns the latest object given a generic Object
func (c *Client) GetObject(object runtime.Object) (runtime.Object, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}

	client, err := c.getDynamicClient(object)
	if err != nil {
		return nil, err
	}

	metadata, err := meta.Accessor(object)
	if err != nil {
		return nil, err
	}
	return client.Get(metadata.GetName(), metav1.GetOptions{}, "")
}

// UpdateObject updates a generic Object
func (c *Client) UpdateObject(object runtime.Object) (runtime.Object, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}

	unstructured, ok := object.(*unstructured.Unstructured)
	if !ok {
		return nil, fmt.Errorf("Unable to cast object to unstructured: %v", object)
	}

	client, err := c.getDynamicClient(object)
	if err != nil {
		return nil, err
	}

	return client.Update(unstructured, metav1.UpdateOptions{}, "")
}

// ListObjects returns a list of generic Objects using the options
func (c *Client) ListObjects(options *metav1.ListOptions, namespace string) (*unstructured.UnstructuredList, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}

	var client dynamic.ResourceInterface
	gvk := schema.FromAPIVersionAndKind(options.APIVersion, options.Kind)
	resourceInterface := c.client.Resource(gvk.GroupVersion().WithResource(strings.ToLower(gvk.Kind) + "s"))

	if namespace != "" {
		client = resourceInterface.Namespace(namespace)
	} else {
		client = resourceInterface
	}

	return client.List(*options)
}

func (c *Client) getDynamicClient(object runtime.Object) (dynamic.ResourceInterface, error) {

	objectType, err := meta.TypeAccessor(object)
	if err != nil {
		return nil, err
	}

	metadata, err := meta.Accessor(object)
	if err != nil {
		return nil, err
	}

	resourceInterface := c.client.Resource(object.GetObjectKind().GroupVersionKind().GroupVersion().WithResource(strings.ToLower(objectType.GetKind()) + "s"))
	if metadata.GetNamespace() == "" {
		return resourceInterface, nil
	}
	return resourceInterface.Namespace(metadata.GetNamespace()), nil
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

	c.client, err = dynamic.NewForConfig(c.config)
	if err != nil {
		return err
	}

	return nil
}
