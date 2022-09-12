package resourceexport

import (
	"context"
	"reflect"

	"github.com/libopenstorage/stork/pkg/controllers"
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

// Controller is a k8s controller that handles ResourceExport resources.
type Controller struct {
	client runtimeclient.Client
}

// NewController returns a new instance of the controller.
func NewController(mgr manager.Manager) (*Controller, error) {
	return &Controller{
		client: mgr.GetClient(),
	}, nil
}

// Init Initialize the application backup controller
func (c *Controller) Init(mgr manager.Manager) error {
	err := c.createCRD()
	if err != nil {
		return err
	}

	// Create a new controller
	ctrl, err := controller.New("resource-export-controller", mgr, controller.Options{
		Reconciler:              c,
		MaxConcurrentReconciles: 10,
	})
	if err != nil {
		return err
	}

	// Watch for changes to primary resource
	return ctrl.Watch(&source.Kind{Type: &kdmpapi.ResourceExport{}}, &handler.EnqueueRequestForObject{})
}

// Reconcile reads that state of the cluster for an object and makes changes based on the state read
// and what is in the Spec.
func (c *Controller) Reconcile(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
	logrus.Debugf("Reconciling ResourceExport %s/%s", request.Namespace, request.Name)

	restoreExport, err := kdmp.Instance().GetResourceExport(request.Name, request.Namespace)
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

	if !controllers.ContainsFinalizer(restoreExport, kdmpcontroller.CleanupFinalizer) {
		controllers.SetFinalizer(restoreExport, kdmpcontroller.CleanupFinalizer)
		return reconcile.Result{Requeue: true}, c.client.Update(context.TODO(), restoreExport)
	}

	requeue, err := c.process(context.TODO(), restoreExport)
	if err != nil {
		logrus.Errorf("fail to execute sync function for restoreExport CR %v: %v", restoreExport.Name, err)
		return reconcile.Result{RequeueAfter: kdmpcontroller.RequeuePeriod}, nil
	}
	if requeue {
		return reconcile.Result{RequeueAfter: kdmpcontroller.RequeuePeriod}, nil
	}

	return reconcile.Result{RequeueAfter: kdmpcontroller.ResyncPeriod}, nil
}

func (c *Controller) createCRD() error {
	requiresV1, err := version.RequiresV1Registration()
	if err != nil {
		return err
	}
	// resourceexport is used by this controller - ensure it's registered
	re := apiextensions.CustomResource{
		Name:    kdmpapi.ResourceExportResourceName,
		Plural:  kdmpapi.ResourceExportResourcePlural,
		Group:   kdmpapi.SchemeGroupVersion.Group,
		Version: kdmpapi.SchemeGroupVersion.Version,
		Scope:   apiextensionsv1beta1.NamespaceScoped,
		Kind:    reflect.TypeOf(kdmpapi.ResourceExport{}).Name(),
	}

	if requiresV1 {
		err := utils.CreateCRD(re)
		if err != nil && !errors.IsAlreadyExists(err) {
			return err
		}
		if err := apiextensions.Instance().ValidateCRD(re.Plural+"."+re.Group, kdmpcontroller.ValidateCRDTimeout, kdmpcontroller.ValidateCRDInterval); err != nil {
			return err
		}
	} else {
		err = apiextensions.Instance().CreateCRDV1beta1(re)
		if err != nil && !errors.IsAlreadyExists(err) {
			return err
		}
		if err := apiextensions.Instance().ValidateCRDV1beta1(re, kdmpcontroller.ValidateCRDTimeout, kdmpcontroller.ValidateCRDInterval); err != nil {
			return err
		}
	}

	return nil
}
