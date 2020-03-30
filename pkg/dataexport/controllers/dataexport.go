package controllers

import (
	"context"
	"reflect"
	"time"

	"github.com/libopenstorage/stork/pkg/apis/stork"
	storkapi "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/libopenstorage/stork/pkg/controller"
	"github.com/operator-framework/operator-sdk/pkg/sdk"
	"github.com/portworx/sched-ops/k8s"
	corev1 "k8s.io/api/core/v1"
	apiextensionsv1beta1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1beta1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// DataExportController is a k8s controller that handles DataExport resources.
type DataExportController struct {
}

// NewDataExportController returns a new instance of the controller.
func NewDataExportController() (*DataExportController, error) {
	return &DataExportController{}, nil
}

// Init ensures that CustomResourceDefinition for DataExport exists in a kubernetes
// cluster and registers the DataExport controller.
func (c *DataExportController) Init() error {
	if err := c.createCRD(); err != nil {
		return err
	}

	return controller.Register(
		&schema.GroupVersionKind{
			Group:   stork.GroupName,
			Version: storkapi.SchemeGroupVersion.Version,
			Kind:    reflect.TypeOf(storkapi.DataExport{}).Name(),
		},
		corev1.NamespaceAll,
		5*time.Minute,
		c,
	)
}

// Handle performs volumes checks and data transfer due to DataExport configuration.
func (c *DataExportController) Handle(ctx context.Context, event sdk.Event) error {
	return Sync(ctx, event)
}

func (c *DataExportController) createCRD() error {
	resource := k8s.CustomResource{
		Name:    storkapi.DataExportResourceName,
		Plural:  storkapi.DataExportResourcePlural,
		Group:   stork.GroupName,
		Version: storkapi.SchemeGroupVersion.Version,
		Scope:   apiextensionsv1beta1.NamespaceScoped,
		Kind:    reflect.TypeOf(storkapi.DataExport{}).Name(),
	}
	err := k8s.Instance().CreateCRD(resource)
	if err != nil && !errors.IsAlreadyExists(err) {
		return err
	}

	return k8s.Instance().ValidateCRD(resource, 10*time.Second, 2*time.Minute)
}
