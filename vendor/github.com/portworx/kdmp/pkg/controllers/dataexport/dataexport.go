package dataexport

import (
	"context"
	"reflect"
	"time"

	"github.com/libopenstorage/stork/pkg/controllers"
	kdmpapi "github.com/portworx/kdmp/pkg/apis/kdmp/v1alpha1"
	"github.com/portworx/sched-ops/k8s/apiextensions"
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

var (
	resyncPeriod = 10 * time.Second

	cleanupFinalizer = "kdmp.portworx.com/finalizer-cleanup"
)

// Controller is a k8s controller that handles DataExport resources.
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
		logrus.Infof("unable to registered data export crds: %v", err)
		return err
	}

	// Create a new controller
	ctrl, err := controller.New("data-export-controller", mgr, controller.Options{
		Reconciler:              c,
		MaxConcurrentReconciles: 10,
	})
	if err != nil {
		logrus.Infof("unable to start reconsiler for data export: %v", err)
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
//
func (c *Controller) Reconcile(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
	logrus.Debugf("Reconciling DataExport %s/%s", request.Namespace, request.Name)

	dataExport := &kdmpapi.DataExport{}
	err := c.client.Get(context.TODO(), request.NamespacedName, dataExport)
	if err != nil {
		if errors.IsNotFound(err) {
			// Request object not found, could have been deleted after reconcile request.
			// Owned objects are automatically garbage collected. For additional cleanup logic use finalizers.
			// Return and don't requeue
			return reconcile.Result{}, nil
		}
		// Error reading the object - requeue the request.
		return reconcile.Result{RequeueAfter: 2 * time.Second}, err
	}

	if !controllers.ContainsFinalizer(dataExport, cleanupFinalizer) {
		controllers.SetFinalizer(dataExport, cleanupFinalizer)
		return reconcile.Result{Requeue: true}, c.client.Update(context.TODO(), dataExport)
	}

	requeue, err := c.sync(context.TODO(), dataExport)
	if err != nil {
		logrus.Errorf("kdmp controller: %s/%s: %s", request.Namespace, request.Name, err)
		return reconcile.Result{RequeueAfter: 2 * time.Second}, err
	}
	if requeue {
		return reconcile.Result{Requeue: requeue}, nil
	}

	return reconcile.Result{RequeueAfter: resyncPeriod}, nil
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
	err := apiextensions.Instance().CreateCRDV1beta1(vb)
	if err != nil && !errors.IsAlreadyExists(err) {
		return err
	}
	if err := apiextensions.Instance().ValidateCRDV1beta1(vb, 10*time.Second, 2*time.Minute); err != nil {
		return err
	}

	resource := apiextensions.CustomResource{
		Name:    kdmpapi.DataExportResourceName,
		Plural:  kdmpapi.DataExportResourcePlural,
		Group:   kdmpapi.SchemeGroupVersion.Group,
		Version: kdmpapi.SchemeGroupVersion.Version,
		Scope:   apiextensionsv1beta1.NamespaceScoped,
		Kind:    reflect.TypeOf(kdmpapi.DataExport{}).Name(),
	}
	err = apiextensions.Instance().CreateCRDV1beta1(resource)
	if err != nil && !errors.IsAlreadyExists(err) {
		return err
	}

	return apiextensions.Instance().ValidateCRDV1beta1(resource, 10*time.Second, 2*time.Minute)
}
