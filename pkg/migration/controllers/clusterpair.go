package controllers

import (
	"context"
	"fmt"
	"reflect"

	"github.com/libopenstorage/stork/drivers/volume"
	stork "github.com/libopenstorage/stork/pkg/apis/stork"
	storkv1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
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

//ClusterPairController pair
type ClusterPairController struct {
	Driver   volume.Driver
	Recorder record.EventRecorder
}

//Init init
func (c *ClusterPairController) Init() error {
	err := c.createCRD()
	if err != nil {
		return err
	}

	return controller.Register(
		&schema.GroupVersionKind{
			Group:   stork.GroupName,
			Version: stork.Version,
			Kind:    reflect.TypeOf(storkv1.ClusterPair{}).Name(),
		},
		"",
		resyncPeriod,
		c)
}

// Handle updates for ClusterPair objects
func (c *ClusterPairController) Handle(ctx context.Context, event sdk.Event) error {
	switch o := event.Object.(type) {
	case *storkv1.ClusterPair:

		clusterPair := o
		if event.Deleted {
			if clusterPair.Status.RemoteStorageID != "" {
				return c.Driver.DeletePair(clusterPair)
			}
		}

		if clusterPair.Status.StorageStatus != storkv1.ClusterPairStatusReady {
			remoteID, err := c.Driver.CreatePair(clusterPair)
			if err != nil {
				clusterPair.Status.StorageStatus = storkv1.ClusterPairStatusError
				c.Recorder.Event(clusterPair,
					v1.EventTypeWarning,
					string(clusterPair.Status.StorageStatus),
					err.Error())
			} else {
				clusterPair.Status.StorageStatus = storkv1.ClusterPairStatusReady
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
		if clusterPair.Status.SchedulerStatus != storkv1.ClusterPairStatusReady {
			remoteConfig, err := getClusterPairSchedulerConfig(clusterPair.Name)
			if err != nil {
				return err
			}

			client, err := kubernetes.NewForConfig(remoteConfig)
			if err != nil {
				return err
			}
			if _, err = client.ServerVersion(); err != nil {
				clusterPair.Status.SchedulerStatus = storkv1.ClusterPairStatusError
				c.Recorder.Event(clusterPair,
					v1.EventTypeWarning,
					string(clusterPair.Status.SchedulerStatus),
					err.Error())
			} else {
				clusterPair.Status.SchedulerStatus = storkv1.ClusterPairStatusReady
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

func getClusterPairSchedulerConfig(clusterPairName string) (*restclient.Config, error) {
	clusterPair, err := k8s.Instance().GetClusterPair(clusterPairName)
	if err != nil {
		return nil, fmt.Errorf("error getting clusterpair: %v", err)
	}
	remoteClientConfig := clientcmd.NewNonInteractiveClientConfig(
		clusterPair.Spec.Config,
		clusterPair.Spec.Config.CurrentContext,
		&clientcmd.ConfigOverrides{},
		nil)
	return remoteClientConfig.ClientConfig()
}

func getClusterPairStorageStatus(clusterPairName string) (storkv1.ClusterPairStatusType, error) {
	clusterPair, err := k8s.Instance().GetClusterPair(clusterPairName)
	if err != nil {
		return storkv1.ClusterPairStatusInitial, fmt.Errorf("error getting clusterpair: %v", err)
	}
	return clusterPair.Status.StorageStatus, nil
}

func getClusterPairSchedulerStatus(clusterPairName string) (storkv1.ClusterPairStatusType, error) {
	clusterPair, err := k8s.Instance().GetClusterPair(clusterPairName)
	if err != nil {
		return storkv1.ClusterPairStatusInitial, fmt.Errorf("error getting clusterpair: %v", err)
	}
	return clusterPair.Status.SchedulerStatus, nil
}

func (c *ClusterPairController) createCRD() error {
	resource := k8s.CustomResource{
		Name:    storkv1.ClusterPairResourceName,
		Plural:  storkv1.ClusterPairResourcePlural,
		Group:   stork.GroupName,
		Version: stork.Version,
		Scope:   apiextensionsv1beta1.ClusterScoped,
		Kind:    reflect.TypeOf(storkv1.ClusterPair{}).Name(),
	}
	err := k8s.Instance().CreateCRD(resource)
	if err != nil && !errors.IsAlreadyExists(err) {
		return err
	}

	return k8s.Instance().ValidateCRD(resource)
}
