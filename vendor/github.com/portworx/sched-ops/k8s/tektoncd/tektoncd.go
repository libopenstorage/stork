package tektoncd

import (
	"fmt"
	"github.com/portworx/sched-ops/k8s/common"
	"github.com/tektoncd/pipeline/pkg/client/clientset/versioned"
	v1 "github.com/tektoncd/pipeline/pkg/client/clientset/versioned/typed/pipeline/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"os"
	"sync"
)

var (
	instance Ops
	once     sync.Once
)

// Ops is an interface to perform kubernetes related operations on the core resources.
type Ops interface {
	taskOps
	taskRunOps
	pipelineOps
	pipelineRunOps
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

// New creates a new client.
func new(tc v1.TaskInterface, tp v1.PipelineInterface, trc v1.TaskRunInterface, tpr v1.PipelineRunInterface) *Client {
	return &Client{
		V1TaskClient:        tc,
		V1PipelineClient:    tp,
		V1TaskRunClient:     trc,
		V1PipelineRunClient: tpr,
	}
}

// NewForConfig creates a new client for the given config.
func NewForConfig(c *rest.Config) (*Client, error) {
	_, err := v1.NewForConfig(c)
	if err != nil {
		return nil, err
	}
	cs, err := versioned.NewForConfig(c)
	return &Client{
		V1TaskClient:        cs.TektonV1().Tasks(""),
		V1PipelineClient:    cs.TektonV1().Pipelines(""),
		V1TaskRunClient:     cs.TektonV1().TaskRuns(""),
		V1PipelineRunClient: cs.TektonV1().PipelineRuns(""),
	}, nil
}

// NewInstanceFromConfigFile returns new instance of client by using given
// config file
func NewInstanceFromConfigFile(config string) (Ops, error) {
	newInstance := &Client{}
	err := newInstance.loadClientFromKubeconfig(config, "")
	if err != nil {
		return nil, err
	}
	return newInstance, nil
}

// Client is a wrapper for the tekton client Ops
type Client struct {
	config              *rest.Config
	V1PipelineClient    v1.PipelineInterface
	V1TaskClient        v1.TaskInterface
	V1TaskRunClient     v1.TaskRunInterface
	V1PipelineRunClient v1.PipelineRunInterface
}

// SetConfig sets the config and resets the client
func (c *Client) SetConfig(cfg *rest.Config) {
	c.config = cfg
	c.V1PipelineClient = nil
	c.V1TaskClient = nil
	c.V1TaskRunClient = nil
	c.V1PipelineRunClient = nil
}

// initClient the k8s client if uninitialized
func (c *Client) initClient(namespace string) error {
	// As the client needs to be intialized for each namespace created and passed
	return c.setClient(namespace)
}

// setClient instantiates a client.
func (c *Client) setClient(namespace string) error {
	var err error

	if c.config != nil {
		err = c.loadClient(namespace)
	} else {
		kubeconfig := os.Getenv("KUBECONFIG")
		if len(kubeconfig) > 0 {
			err = c.loadClientFromKubeconfig(kubeconfig, namespace)
		} else {
			err = c.loadClientFromServiceAccount(namespace)
		}
	}
	return err
}

// loadClientFromServiceAccount loads a k8s client from a ServiceAccount specified in the pod running px
func (c *Client) loadClientFromServiceAccount(namespace string) error {
	config, err := rest.InClusterConfig()
	if err != nil {
		return err
	}

	c.config = config
	return c.loadClient(namespace)
}

// loadClientFromKubeconfig loads a k8s client from a kubeconfig file
func (c *Client) loadClientFromKubeconfig(kubeconfig string, namespace string) error {
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		return err
	}

	c.config = config
	return c.loadClient(namespace)
}

// loadClient loads the k8s client
func (c *Client) loadClient(namespace string) error {
	if c.config == nil {
		return fmt.Errorf("rest config is not provided")
	}

	var err error
	err = common.SetRateLimiter(c.config)
	if err != nil {
		return err
	}
	cs, err := versioned.NewForConfig(c.config)
	if err != nil {
		return err
	}
	c.V1PipelineClient = cs.TektonV1().Pipelines(namespace)
	c.V1TaskClient = cs.TektonV1().Tasks(namespace)
	c.V1TaskRunClient = cs.TektonV1().TaskRuns(namespace)
	c.V1PipelineRunClient = cs.TektonV1().PipelineRuns(namespace)

	return nil
}
