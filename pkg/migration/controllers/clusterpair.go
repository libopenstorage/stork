package controllers

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"github.com/libopenstorage/stork/drivers/volume"
	"github.com/libopenstorage/stork/pkg/apis/stork"
	stork_api "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/libopenstorage/stork/pkg/controller"
	"github.com/operator-framework/operator-sdk/pkg/sdk"
	"github.com/portworx/sched-ops/k8s"
	"k8s.io/api/core/v1"
	apiextensionsv1beta1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1beta1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/record"
)

const (
	validateCRDInterval time.Duration = 5 * time.Second
	validateCRDTimeout  time.Duration = 1 * time.Minute
)

// ClusterPairController controller to watch over ClusterPair
type ClusterPairController struct {
	Driver   volume.Driver
	Recorder record.EventRecorder
}

// Init initialize the cluster pair controller
func (c *ClusterPairController) Init() error {
	err := c.createCRD()
	if err != nil {
		return err
	}

	return controller.Register(
		&schema.GroupVersionKind{
			Group:   stork.GroupName,
			Version: stork_api.SchemeGroupVersion.Version,
			Kind:    reflect.TypeOf(stork_api.ClusterPair{}).Name(),
		},
		"",
		resyncPeriod,
		c)
}

// Handle updates for ClusterPair objects
func (c *ClusterPairController) Handle(ctx context.Context, event sdk.Event) error {
	switch o := event.Object.(type) {
	case *stork_api.ClusterPair:

		clusterPair := o
		if event.Deleted {
			if clusterPair.Status.RemoteStorageID != "" {
				return c.Driver.DeletePair(clusterPair)
			}
		}

		if len(clusterPair.Spec.Options) == 0 {
			clusterPair.Status.StorageStatus = stork_api.ClusterPairStatusNotProvided
			c.Recorder.Event(clusterPair,
				v1.EventTypeNormal,
				string(clusterPair.Status.StorageStatus),
				"Skipping storage pairing since no storage options provided")
			err := sdk.Update(clusterPair)
			if err != nil {
				return err
			}
		} else {
			if clusterPair.Status.StorageStatus != stork_api.ClusterPairStatusReady {
				remoteID, err := c.Driver.CreatePair(clusterPair)
				if err != nil {
					clusterPair.Status.StorageStatus = stork_api.ClusterPairStatusError
					c.Recorder.Event(clusterPair,
						v1.EventTypeWarning,
						string(clusterPair.Status.StorageStatus),
						err.Error())
				} else {
					clusterPair.Status.StorageStatus = stork_api.ClusterPairStatusReady
					c.Recorder.Event(clusterPair,
						v1.EventTypeNormal,
						string(clusterPair.Status.StorageStatus),
						"Storage successfully paired")
					clusterPair.Status.RemoteStorageID = remoteID
				}
				err = sdk.Update(clusterPair)
				if err != nil {
					return err
				}
			}
		}
		if clusterPair.Status.SchedulerStatus != stork_api.ClusterPairStatusReady {
			remoteConfig, err := getClusterPairSchedulerConfig(clusterPair.Name, clusterPair.Namespace)
			if err != nil {
				return err
			}

			client, err := kubernetes.NewForConfig(remoteConfig)
			if err != nil {
				return err
			}
			if _, err = client.ServerVersion(); err != nil {
				clusterPair.Status.SchedulerStatus = stork_api.ClusterPairStatusError
				c.Recorder.Event(clusterPair,
					v1.EventTypeWarning,
					string(clusterPair.Status.SchedulerStatus),
					err.Error())
			} else {
				clusterPair.Status.SchedulerStatus = stork_api.ClusterPairStatusReady
				c.Recorder.Event(clusterPair,
					v1.EventTypeNormal,
					string(clusterPair.Status.SchedulerStatus),
					"Scheduler successfully paired")
			}
			err = sdk.Update(clusterPair)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func getClusterPairSchedulerConfig(clusterPairName string, namespace string) (*restclient.Config, error) {
	clusterPair, err := k8s.Instance().GetClusterPair(clusterPairName, namespace)
	if err != nil {
		return nil, fmt.Errorf("error getting clusterpair: %v", err)
	}
	remoteClientConfig := clientcmd.NewNonInteractiveClientConfig(
		clusterPair.Spec.Config,
		clusterPair.Spec.Config.CurrentContext,
		&clientcmd.ConfigOverrides{},
		clientcmd.NewDefaultClientConfigLoadingRules())
	return remoteClientConfig.ClientConfig()
}

func getClusterPairStorageStatus(clusterPairName string, namespace string) (stork_api.ClusterPairStatusType, error) {
	clusterPair, err := k8s.Instance().GetClusterPair(clusterPairName, namespace)
	if err != nil {
		return stork_api.ClusterPairStatusInitial, fmt.Errorf("error getting clusterpair: %v", err)
	}
	return clusterPair.Status.StorageStatus, nil
}

func getClusterPairSchedulerStatus(clusterPairName string, namespace string) (stork_api.ClusterPairStatusType, error) {
	clusterPair, err := k8s.Instance().GetClusterPair(clusterPairName, namespace)
	if err != nil {
		return stork_api.ClusterPairStatusInitial, fmt.Errorf("error getting clusterpair: %v", err)
	}
	return clusterPair.Status.SchedulerStatus, nil
}

func (c *ClusterPairController) createCRD() error {
	resource := k8s.CustomResource{
		Name:    stork_api.ClusterPairResourceName,
		Plural:  stork_api.ClusterPairResourcePlural,
		Group:   stork.GroupName,
		Version: stork_api.SchemeGroupVersion.Version,
		Scope:   apiextensionsv1beta1.NamespaceScoped,
		Kind:    reflect.TypeOf(stork_api.ClusterPair{}).Name(),
	}
	err := k8s.Instance().CreateCRD(resource)
	if err != nil && !errors.IsAlreadyExists(err) {
		return err
	}

	return k8s.Instance().ValidateCRD(resource, validateCRDTimeout, validateCRDInterval)
}
