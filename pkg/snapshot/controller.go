package snapshot

import (
	"fmt"
	"sync"
	"time"

	"github.com/kubernetes-incubator/external-storage/lib/controller"
	"github.com/kubernetes-incubator/external-storage/snapshot/pkg/client"
	snapshotcontroller "github.com/kubernetes-incubator/external-storage/snapshot/pkg/controller/snapshot-controller"
	snapshotvolume "github.com/kubernetes-incubator/external-storage/snapshot/pkg/volume"
	"github.com/libopenstorage/stork/drivers/volume"
	"github.com/portworx/sched-ops/k8s"
	log "github.com/sirupsen/logrus"
	apiextensionsclient "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

const (
	snapshotProvisionerName               = "stork-snapshot"
	snapshotProvisionerID                 = "stork"
	provisionerIDAnn                      = "snapshotProvisionerIdentity"
	defaultSyncDuration     time.Duration = 60 * time.Second
	validateCrdInterval     time.Duration = 5 * time.Second
	validateCrdTimeout      time.Duration = 1 * time.Minute
)

// Controller Snapshot Controller
type Controller struct {
	Driver      volume.Driver
	lock        sync.Mutex
	started     bool
	stopChannel chan struct{}
}

// GetProvisionerName Gets the name of the provisioner
func GetProvisionerName() string {
	return snapshotProvisionerName
}

// Start Starts the snapshot controller
func (c *Controller) Start() error {
	c.lock.Lock()
	defer c.lock.Unlock()

	if c.started {
		return fmt.Errorf("Extender has already been started")
	}
	config, err := rest.InClusterConfig()
	if err != nil {
		return err
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return err
	}

	if clientset == nil {
		return k8s.ErrK8SApiAccountNotSet
	}

	aeclientset, err := apiextensionsclient.NewForConfig(config)
	if err != nil {
		return err
	}

	snapshotClient, snapshotScheme, err := client.NewClient(config)
	if err != nil {
		return err
	}

	log.Infof("Registering CRDs")
	err = client.CreateCRD(aeclientset)
	if err != nil {
		return err
	}

	err = client.WaitForSnapshotResource(snapshotClient)
	if err != nil {
		return err
	}

	plugins := make(map[string]snapshotvolume.Plugin)
	plugins[c.Driver.String()] = c.Driver.GetSnapshotPlugin()

	snapController := snapshotcontroller.NewSnapshotController(snapshotClient, snapshotScheme,
		clientset, &plugins, defaultSyncDuration)

	c.stopChannel = make(chan struct{})

	snapController.Run(c.stopChannel)

	serverVersion, err := clientset.Discovery().ServerVersion()
	if err != nil {
		log.Fatalf("Error getting server version: %v", err)
	}

	snapProvisioner := newSnapshotProvisioner(clientset, snapshotClient, plugins, snapshotProvisionerID)

	provisioner := controller.NewProvisionController(
		clientset,
		snapshotProvisionerName,
		snapProvisioner,
		serverVersion.GitVersion,
	)
	// stork already does leader elction, don't need it for each controller
	if err := controller.LeaderElection(false)(provisioner); err != nil {
		log.Errorf("failed to disable leader election for snapshot controller: %v", err)
		return err
	}

	go provisioner.Run(c.stopChannel)

	if err := performRuleRecovery(); err != nil {
		log.Errorf("failed to perform recovery for snapshot rules due to: %v", err)
		return err
	}

	c.started = true
	return nil
}

// Stop Stops the snapshot controller
func (c *Controller) Stop() error {
	c.lock.Lock()
	defer c.lock.Unlock()

	if !c.started {
		return fmt.Errorf("Extender has not been started")
	}

	close(c.stopChannel)

	c.started = false
	return nil
}
