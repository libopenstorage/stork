package stork

import (
	"fmt"
	"os"
	"sync"

	snapclient "github.com/kubernetes-incubator/external-storage/snapshot/pkg/client"
	storkv1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	storkclientset "github.com/libopenstorage/stork/pkg/client/clientset/versioned"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
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
	ApplicationRegistrationOps

	// SetConfig sets the config and resets the client
	SetConfig(config *rest.Config)

	// WatchStorkResources sets up and return resource watch
	WatchStorkResources(runtime.Object) (watch.Interface, error)
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

// WatchStorkResources sets up and return resource watch
func (c *Client) WatchStorkResources(object runtime.Object) (watch.Interface, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	metadata, err := meta.Accessor(object)
	if err != nil {
		fmt.Printf("Watch dynamic get meta %v\n", err)
		return nil, err
	}

	listOptions := metav1.ListOptions{
		FieldSelector: fields.OneTermEqualSelector("metadata.name", metadata.GetName()).String(),
		Watch:         true,
	}
	var watchInterface watch.Interface
	if obj, ok := object.(*storkv1.ApplicationBackup); ok {
		watchInterface, err = c.stork.StorkV1alpha1().ApplicationBackups(obj.Namespace).Watch(listOptions)
	} else if obj, ok := object.(*storkv1.ApplicationBackupSchedule); ok {
		watchInterface, err = c.stork.StorkV1alpha1().ApplicationBackupSchedules(obj.Namespace).Watch(listOptions)
	} else if obj, ok := object.(*storkv1.ApplicationRestore); ok {
		watchInterface, err = c.stork.StorkV1alpha1().ApplicationRestores(obj.Namespace).Watch(listOptions)
	} else if obj, ok := object.(*storkv1.ApplicationClone); ok {
		watchInterface, err = c.stork.StorkV1alpha1().ApplicationClones(obj.Namespace).Watch(listOptions)
	} else if obj, ok := object.(*storkv1.ClusterPair); ok {
		watchInterface, err = c.stork.StorkV1alpha1().ClusterPairs(obj.Namespace).Watch(listOptions)
	} else if _, ok := object.(*storkv1.ClusterDomainsStatus); ok {
		watchInterface, err = c.stork.StorkV1alpha1().ClusterDomainsStatuses().Watch(listOptions)
	} else if obj, ok := object.(*storkv1.ApplicationBackup); ok {
		watchInterface, err = c.stork.StorkV1alpha1().ApplicationBackups(obj.Namespace).Watch(listOptions)
	} else if obj, ok := object.(*storkv1.Migration); ok {
		watchInterface, err = c.stork.StorkV1alpha1().Migrations(obj.Namespace).Watch(listOptions)
	} else if obj, ok := object.(*storkv1.MigrationSchedule); ok {
		watchInterface, err = c.stork.StorkV1alpha1().MigrationSchedules(obj.Namespace).Watch(listOptions)
	} else if obj, ok := object.(*storkv1.VolumeSnapshotRestore); ok {
		watchInterface, err = c.stork.StorkV1alpha1().VolumeSnapshotRestores(obj.Namespace).Watch(listOptions)
	} else {
		return nil, fmt.Errorf("unsupported object, %v", object)
	}

	if err != nil {
		return nil, err
	}
	return watchInterface, nil
}
