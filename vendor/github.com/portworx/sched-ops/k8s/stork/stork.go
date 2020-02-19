package stork

import (
	"fmt"
	"os"
	"sync"

	snapclient "github.com/kubernetes-incubator/external-storage/snapshot/pkg/client"
	storkclientset "github.com/libopenstorage/stork/pkg/client/clientset/versioned"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

var (
	instance Ops
	once     sync.Once

	deleteForegroundPolicy = metav1.DeletePropagationForeground
)

// Ops is an interface to Stork operations.
type Ops interface {
	SnapshotScheduleOps
	GroupSnapshotOps
	RuleOps
	ClusterPairOps
	MigrationOps
	ClusterDomainsOps
	SchedulePolicyOps
	BackupLocationOps
	ApplicationBackupRestoreOps
	ApplicationCloneOps
	VolumeSnapshotRestoreOps

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

// New creates a new stork client.
func New(kube kubernetes.Interface, stork storkclientset.Interface, snap rest.Interface) *Client {
	return &Client{
		kube:  kube,
		stork: stork,
		snap:  snap,
	}
}

// NewForConfig creates a new stork client for the given config.
func NewForConfig(c *rest.Config) (*Client, error) {
	kclient, err := kubernetes.NewForConfig(c)
	if err != nil {
		return nil, err
	}

	storkClient, err := storkclientset.NewForConfig(c)
	if err != nil {
		return nil, err
	}

	return &Client{
		kube:  kclient,
		stork: storkClient,
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

// Client is a wrapper for the stork operator client.
type Client struct {
	config *rest.Config
	kube   kubernetes.Interface
	stork  storkclientset.Interface
	snap   rest.Interface
}

// SetConfig sets the config and resets the client
func (c *Client) SetConfig(cfg *rest.Config) {
	c.config = cfg

	c.kube = nil
	c.stork = nil
	c.snap = nil
}

// initClient initialize stork clients.
func (c *Client) initClient() error {
	if c.stork != nil && c.kube != nil {
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

	c.kube, err = kubernetes.NewForConfig(c.config)
	if err != nil {
		return err
	}

	c.stork, err = storkclientset.NewForConfig(c.config)
	if err != nil {
		return err
	}

	c.snap, _, err = snapclient.NewClient(c.config)
	if err != nil {
		return err
	}

	return nil
}
