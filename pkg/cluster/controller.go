package cluster

import (
	"context"
	"reflect"
	"time"

	stork "github.com/libopenstorage/stork/pkg/apis/stork"
	storkv1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/libopenstorage/stork/pkg/controller"
	"github.com/operator-framework/operator-sdk/pkg/sdk"
	"github.com/portworx/sched-ops/k8s"
	"github.com/sirupsen/logrus"
	apiextensionsv1beta1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1beta1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/record"
)

const (
	validateCRDInterval time.Duration = 5 * time.Second
	validateCRDTimeout  time.Duration = 1 * time.Minute
	resyncPeriod                      = 30 * time.Second
)

// Controller storage cluster controller
type Controller struct {
	Recorder record.EventRecorder
}

// Init initialize the storage cluster controller
func (c *Controller) Init() error {
	err := c.createCRD()
	if err != nil {
		return err
	}

	return controller.Register(
		&schema.GroupVersionKind{
			Group:   stork.GroupName,
			Version: storkv1.SchemeGroupVersion.Version,
			Kind:    reflect.TypeOf(storkv1.StorageCluster{}).Name(),
		},
		"",
		resyncPeriod,
		c)
}

// Handle updates the cluster about the changes in the StorageCluster CRD
func (c *Controller) Handle(ctx context.Context, event sdk.Event) error {
	switch obj := event.Object.(type) {
	case *storkv1.StorageCluster:
		storageCluster := obj
		logrus.Infof("storage cluster: %v", storageCluster)
		// TODO: Take some action
	}
	return nil
}

// createCRD creates the CRD for StorageCluster object
func (c *Controller) createCRD() error {
	resource := k8s.CustomResource{
		Name:       storkv1.StorageClusterResourceName,
		Plural:     storkv1.StorageClusterResourcePlural,
		Group:      stork.GroupName,
		Version:    storkv1.SchemeGroupVersion.Version,
		Scope:      apiextensionsv1beta1.ClusterScoped,
		Kind:       reflect.TypeOf(storkv1.StorageCluster{}).Name(),
		ShortNames: []string{storkv1.StorageClusterShortName},
	}
	err := k8s.Instance().CreateCRD(resource)
	if err != nil && !errors.IsAlreadyExists(err) {
		return err
	}

	return k8s.Instance().ValidateCRD(resource, validateCRDTimeout, validateCRDInterval)
}
