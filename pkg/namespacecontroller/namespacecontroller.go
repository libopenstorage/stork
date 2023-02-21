package namespacecontroller

import (
	"context"
	"os"

	"github.com/libopenstorage/stork/drivers/volume"
	"github.com/libopenstorage/stork/pkg/controllers"
	"github.com/libopenstorage/stork/pkg/storkctl"
	"github.com/portworx/sched-ops/k8s/core"
	"github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/client-go/tools/record"
	"k8s.io/kubectl/pkg/cmd/util"
	runtimeclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

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

func (ns *NamespaceController) Reconcile(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {

	namespace := &corev1.Namespace{}
	err := ns.client.Get(context.TODO(), request.NamespacedName, namespace)
	if err != nil {
		if errors.IsNotFound(err) {
			return reconcile.Result{}, nil
		}
		// Error reading the object - requeue the request.
		return reconcile.Result{RequeueAfter: controllers.DefaultRequeueError}, err
	}

	if _, ok := namespace.Labels["failover"]; ok {
		delete(namespace.Labels, "failover")
		_, err = core.Instance().UpdateNamespace(namespace)
		if err != nil {
			util.CheckErr(err)
			return reconcile.Result{}, err
		}
		ns.volDriver.ActivateMigration(namespace.Name)
		logrus.Infof("NamespaceController.Reconcile(d) %v", namespace.Name)

		ioStreams := genericclioptions.IOStreams{In: os.Stdin, Out: os.Stdout, ErrOut: os.Stdout}
		factory := storkctl.NewFactory()
		config, err := factory.GetConfig()
		if err != nil {
			util.CheckErr(err)
			return reconcile.Result{}, err
		}
		storkctl.UpdateStatefulSets(namespace.Name, true, ioStreams)
		storkctl.UpdateDeployments(namespace.Name, true, ioStreams)
		storkctl.UpdateDeploymentConfigs(namespace.Name, true, ioStreams)
		storkctl.UpdateIBPObjects("IBPPeer", namespace.Name, true, ioStreams)
		storkctl.UpdateIBPObjects("IBPCA", namespace.Name, true, ioStreams)
		storkctl.UpdateIBPObjects("IBPOrderer", namespace.Name, true, ioStreams)
		storkctl.UpdateIBPObjects("IBPConsole", namespace.Name, true, ioStreams)
		storkctl.UpdateVMObjects("VirtualMachine", namespace.Name, true, ioStreams)
		storkctl.UpdateCRDObjects(namespace.Name, true, ioStreams, config)
		storkctl.UpdateCronJobObjects(namespace.Name, true, ioStreams)
	}

	return reconcile.Result{RequeueAfter: controllers.DefaultRequeue}, nil
}
