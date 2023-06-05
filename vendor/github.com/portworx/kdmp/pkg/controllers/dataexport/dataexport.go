package dataexport

import (
	"context"
	"reflect"

	"github.com/libopenstorage/stork/pkg/controllers"
	"github.com/libopenstorage/stork/pkg/snapshotter"
	kdmpapi "github.com/portworx/kdmp/pkg/apis/kdmp/v1alpha1"
	kdmpcontroller "github.com/portworx/kdmp/pkg/controllers"
	"github.com/portworx/kdmp/pkg/utils"
	"github.com/portworx/kdmp/pkg/version"
	"github.com/portworx/sched-ops/k8s/apiextensions"
	"github.com/portworx/sched-ops/k8s/kdmp"
	"github.com/sirupsen/logrus"
	apiextensionsv1beta1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1beta1"
	"k8s.io/apimachinery/pkg/api/errors"
	runtimeclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

// Controller is a k8s controller that handles DataExport resources.
type Controller struct {
	client      runtimeclient.Client
	snapshotter snapshotter.Snapshotter
}

// NewController returns a new instance of the controller.
func NewController(mgr manager.Manager) (*Controller, error) {
	return &Controller{
		client:      mgr.GetClient(),
		snapshotter: snapshotter.NewDefaultSnapshotter(),
	}, nil
}

// Init Initialize the application backup controller
func (c *Controller) Init(mgr manager.Manager) error {
	err := c.createCRD()
	if err != nil {
		return err
	}

	// Create a new controller
	ctrl, err := controller.New("data-export-controller", mgr, controller.Options{
		Reconciler:              c,
		MaxConcurrentReconciles: 10,
	})
	if err != nil {
		return err
	}

	// Watch for changes to primary resource
	return ctrl.Watch(&source.Kind{Type: &kdmpapi.DataExport{}}, &handler.EnqueueRequestForObject{})
}

// Reconcile reads that state of the cluster for an object and makes changes based on the state read
// and what is in the Spec.
//
// The Controller will requeue the Request to be processed again if the returned error is non-nil or
// Result.Requeue is true, otherwise upon completion it will remove the work from the queue.
func (c *Controller) Reconcile(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
	logrus.Tracef("Reconciling DataExport %s/%s", request.Namespace, request.Name)

	dataExport, err := kdmp.Instance().GetDataExport(request.Name, request.Namespace)
	if err != nil {
		if errors.IsNotFound(err) {
			// Request object not found, could have been deleted after reconcile request.
			// Owned objects are automatically garbage collected. For additional cleanup logic use finalizers.
			// Return and don't requeue
			return reconcile.Result{}, nil
		}
		// Error reading the object - requeue the request.
		return reconcile.Result{RequeueAfter: kdmpcontroller.RequeuePeriod}, nil
	}

	if !controllers.ContainsFinalizer(dataExport, kdmpcontroller.CleanupFinalizer) {
		controllers.SetFinalizer(dataExport, kdmpcontroller.CleanupFinalizer)
		return reconcile.Result{Requeue: true}, c.client.Update(context.TODO(), dataExport)
	}

	requeue, err := c.sync(context.TODO(), dataExport)
	if err != nil {
		logrus.Errorf("kdmp controller: %s/%s: %s", request.Namespace, request.Name, err)
		return reconcile.Result{RequeueAfter: kdmpcontroller.RequeuePeriod}, nil
	}
	if requeue {
		return reconcile.Result{RequeueAfter: kdmpcontroller.RequeuePeriod}, nil
	}

	return reconcile.Result{RequeueAfter: kdmpcontroller.ResyncPeriod}, nil
}

func (c *Controller) createCRD() error {
	// volumebackups is used by this controller - ensure it's registered
	vb := apiextensions.CustomResource{
		Name:    kdmpapi.VolumeBackupResourceName,
		Plural:  kdmpapi.VolumeBackupResourcePlural,
		Group:   kdmpapi.SchemeGroupVersion.Group,
		Version: kdmpapi.SchemeGroupVersion.Version,
		Scope:   apiextensionsv1beta1.NamespaceScoped,
		Kind:    reflect.TypeOf(kdmpapi.VolumeBackup{}).Name(),
	}

	requiresV1, err := version.RequiresV1Registration()
	if err != nil {
		return err
	}
	if requiresV1 {
		err := utils.CreateCRD(vb)
		if err != nil && !errors.IsAlreadyExists(err) {
			return err
		}
		if err := apiextensions.Instance().ValidateCRD(vb.Plural+"."+vb.Group, kdmpcontroller.ValidateCRDTimeout, kdmpcontroller.ValidateCRDInterval); err != nil {
			return err
		}
	} else {
		err = apiextensions.Instance().CreateCRDV1beta1(vb)
		if err != nil && !errors.IsAlreadyExists(err) {
			return err
		}
		if err := apiextensions.Instance().ValidateCRDV1beta1(vb, kdmpcontroller.ValidateCRDTimeout, kdmpcontroller.ValidateCRDInterval); err != nil {
			return err
		}
	}

	resource := apiextensions.CustomResource{
		Name:    kdmpapi.DataExportResourceName,
		Plural:  kdmpapi.DataExportResourcePlural,
		Group:   kdmpapi.SchemeGroupVersion.Group,
		Version: kdmpapi.SchemeGroupVersion.Version,
		Scope:   apiextensionsv1beta1.NamespaceScoped,
		Kind:    reflect.TypeOf(kdmpapi.DataExport{}).Name(),
	}

	if requiresV1 {
		err := utils.CreateCRD(resource)
		if err != nil && !errors.IsAlreadyExists(err) {
			return err
		}
		if err := apiextensions.Instance().ValidateCRD(resource.Plural+"."+vb.Group, kdmpcontroller.ValidateCRDTimeout, kdmpcontroller.ValidateCRDInterval); err != nil {
			return err
		}
	} else {
		err = apiextensions.Instance().CreateCRDV1beta1(resource)
		if err != nil && !errors.IsAlreadyExists(err) {
			return err
		}
		if err := apiextensions.Instance().ValidateCRDV1beta1(resource, kdmpcontroller.ValidateCRDTimeout, kdmpcontroller.ValidateCRDInterval); err != nil {
			return err
		}
	}
	return nil
}
