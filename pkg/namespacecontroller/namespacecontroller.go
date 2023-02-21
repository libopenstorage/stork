package namespacecontroller

import (
	"context"

	"github.com/libopenstorage/stork/drivers/volume"
	"github.com/libopenstorage/stork/pkg/controllers"
	"github.com/portworx/sched-ops/k8s/core"
	"github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/tools/record"
	"k8s.io/kubectl/pkg/cmd/util"
	runtimeclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// New creates a new instance of NamespaceController
// TODO: rename variable names
func NewNamespaceController(mgr manager.Manager, d volume.Driver, r record.EventRecorder) *NamespaceController {
	return &NamespaceController{
		client:    mgr.GetClient(),
		volDriver: d,
		recorder:  r,
	}
}

type NamespaceController struct {
	client    runtimeclient.Client
	volDriver volume.Driver
	recorder  record.EventRecorder
}

func (ns *NamespaceController) Init(mgr manager.Manager) error {
	return controllers.RegisterTo(mgr, "namespace-controller", ns, &corev1.Namespace{})
}

// Reconcile handles snapshot schedule updates for persistent volume claims.
func (ns *NamespaceController) Reconcile(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {

	// TODO: Extract this out to a method / class
	namespace := &corev1.Namespace{}
	err := ns.client.Get(context.TODO(), request.NamespacedName, namespace)
	if err != nil {
		if errors.IsNotFound(err) {
			// Request object not found, could have been deleted after reconcile request.
			// Owned objects are automatically garbage collected. For additional cleanup logic use finalizers.
			// Return and don't requeue
			return reconcile.Result{}, nil
		}
		// Error reading the object - requeue the request.
		return reconcile.Result{RequeueAfter: controllers.DefaultRequeueError}, err
	}

	if _, ok := namespace.Labels["failover"]; ok {
		logrus.Infof("pkg/namespacecontroller.go: [detected failover label] namespace labels: %v", namespace.Labels)
		delete(namespace.Labels, "failover")
		namespace.Labels["promote"] = "true"
		_, err = core.Instance().UpdateNamespace(namespace)
		if err != nil {
			util.CheckErr(err)
			return reconcile.Result{}, err
		}
		logrus.Infof("pkg/namespacecontroller.go: [after adding promote] namespace labels: %v", namespace.Labels)
		ns.volDriver.ActivateMigration(namespace.GetNamespace())
	}
	return reconcile.Result{RequeueAfter: controllers.DefaultRequeue}, nil
}
