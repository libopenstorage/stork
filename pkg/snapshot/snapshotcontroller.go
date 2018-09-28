package snapshotcontroller

import (
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/kubernetes-incubator/external-storage/lib/controller"
	"github.com/kubernetes-incubator/external-storage/snapshot/pkg/client"
	snapshotcontroller "github.com/kubernetes-incubator/external-storage/snapshot/pkg/controller/snapshot-controller"
	snapshotvolume "github.com/kubernetes-incubator/external-storage/snapshot/pkg/volume"
	"github.com/libopenstorage/stork/drivers/volume"
	stork "github.com/libopenstorage/stork/pkg/apis/stork"
	storkapi "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/libopenstorage/stork/pkg/snapshot/rule"
	"github.com/portworx/sched-ops/k8s"
	log "github.com/sirupsen/logrus"
	apiextensionsv1beta1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1beta1"
	apiextensionsclient "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

const (
	snapshotProvisionerName               = "stork-snapshot"
	snapshotProvisionerID                 = "stork"
	provisionerIDAnn                      = "snapshotProvisionerIdentity"
	defaultSyncDuration     time.Duration = 60 * time.Second
)

// SnapshotController Snapshot Controller
type SnapshotController struct {
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
func (s *SnapshotController) Start() error {
	s.lock.Lock()
	defer s.lock.Unlock()

	if s.started {
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

	storkRuleResource := k8s.CustomResource{
		Name:    "rule",
		Plural:  "rules",
		Group:   stork.GroupName,
		Version: stork.Version,
		Scope:   apiextensionsv1beta1.NamespaceScoped,
		Kind:    reflect.TypeOf(storkapi.Rule{}).Name(),
	}

	err = k8s.Instance().CreateCRD(storkRuleResource)
	if err != nil {
		if !errors.IsAlreadyExists(err) {
			return fmt.Errorf("Failed to create CRD due to: %v", err)
		}
	}

	err = k8s.Instance().ValidateCRD(storkRuleResource)
	if err != nil {
		return fmt.Errorf("Failed to validate stork rules CRD due to: %v", err)
	}

	err = client.WaitForSnapshotResource(snapshotClient)
	if err != nil {
		return err
	}

	plugins := make(map[string]snapshotvolume.Plugin)
	plugins[s.Driver.String()] = s.Driver.GetSnapshotPlugin()

	snapController := snapshotcontroller.NewSnapshotController(snapshotClient, snapshotScheme,
		clientset, &plugins, defaultSyncDuration)

	s.stopChannel = make(chan struct{})

	snapController.Run(s.stopChannel)

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
	go provisioner.Run(s.stopChannel)

	if err := rule.PerformRuleRecovery(); err != nil {
		log.Errorf("failed to perform recovery for snapshot rules due to: %v", err)
		return err
	}

	s.started = true
	return nil
}

// Stop Stops the snapshot controller
func (s *SnapshotController) Stop() error {
	s.lock.Lock()
	defer s.lock.Unlock()

	if !s.started {
		return fmt.Errorf("Extender has not been started")
	}

	close(s.stopChannel)

	s.started = false
	return nil
}
