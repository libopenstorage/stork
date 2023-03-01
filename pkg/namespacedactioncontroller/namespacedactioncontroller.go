package namespacedaction

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"github.com/libopenstorage/stork/drivers/volume"
	storkv1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/libopenstorage/stork/pkg/controllers"
	"github.com/libopenstorage/stork/pkg/k8sutils"
	"github.com/libopenstorage/stork/pkg/resourceutils"
	"github.com/libopenstorage/stork/pkg/schedule"
	"github.com/portworx/sched-ops/k8s/apiextensions"
	"github.com/sirupsen/logrus"
	apiextensionsv1beta1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1beta1"
	"k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/record"
	runtimeclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const (
	validateCRDInterval time.Duration = 5 * time.Second
	validateCRDTimeout  time.Duration = 1 * time.Minute
)

func NewNamespacedActionController(mgr manager.Manager, d volume.Driver, r record.EventRecorder) *NamespacedActionController {
	config, err := rest.InClusterConfig()
	if err != nil {
		logrus.Fatalf("Error getting cluster config: %v", err)
	}
	return &NamespacedActionController{
		client:    mgr.GetClient(),
		volDriver: d,
		recorder:  r,
		config:    config,
	}
}

type NamespacedActionController struct {
	client    runtimeclient.Client
	volDriver volume.Driver
	recorder  record.EventRecorder
	config    *rest.Config
}

func (ns *NamespacedActionController) Init(mgr manager.Manager) error {
	logrus.Infof("[DEBUG] NamespacedActionController Init")
	err := ns.createCRD()
	if err != nil {
		return err
	}
	return controllers.RegisterTo(mgr, "namespace-action-controller", ns, &storkv1.NamespacedAction{})
}

func (ns *NamespacedActionController) Reconcile(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
	logrus.Infof("[DEBUG] NamespacedActionController Reconcile")

	namespacedAction := &storkv1.NamespacedAction{}
	err := ns.client.Get(context.TODO(), request.NamespacedName, namespacedAction)
	if err != nil {
		if errors.IsNotFound(err) {
			return reconcile.Result{}, nil
		}
		// Error reading the object - requeue the request.
		return reconcile.Result{RequeueAfter: controllers.DefaultRequeueError}, err
	}

	if err = ns.handle(context.TODO(), namespacedAction); err != nil {
		logrus.Errorf("%s: %s: %s", reflect.TypeOf(ns), namespacedAction.Name, err)
		return reconcile.Result{RequeueAfter: controllers.DefaultRequeueError}, err
	}

	return reconcile.Result{RequeueAfter: controllers.DefaultRequeue}, nil
}

func (ns *NamespacedActionController) handle(ctx context.Context, namespacedAction *storkv1.NamespacedAction) error {
	action := namespacedAction.Spec.Action
	switch action {
	case storkv1.NamespacedActionNil:
		return nil
	case storkv1.NamespacedActionFailover:
		logrus.Infof("[DEBUG] NamespacedActionController handle: Failover")
		resourceutils.ScaleReplicas(namespacedAction.Namespace, true, printFunc, ns.config)
		namespacedAction.Spec.Action = storkv1.NamespacedActionNil
	case storkv1.NamespacedActionFailback:
		logrus.Infof("[DEBUG] NamespacedActionController handle: Failback")
		namespacedAction.Spec.Action = storkv1.NamespacedActionNil
	default:
		return fmt.Errorf("Invalid value received for NamespacedAction.Spec.Action!")
	}
	return ns.createNamespacedActionStatusItem(namespacedAction, action, storkv1.NamespacedActionStatusSuccessful)
}

func printFunc(msg, stream string) {
	switch stream {
	case "out":
		logrus.Infof(msg)
	case "err":
		logrus.Errorf(msg)
	default:
		logrus.Errorf("printFunc received invalid stream")
		logrus.Errorf(msg)
	}
}

func (ns *NamespacedActionController) createNamespacedActionStatusItem(
	namespacedAction *storkv1.NamespacedAction,
	action storkv1.NamespacedActionType,
	status storkv1.NamespacedActionStatusType) error {
	if namespacedAction.Status.Items == nil {
		namespacedAction.Status.Items = make([]*storkv1.NamespacedActionStatusItem, 0)
	}
	namespacedAction.Status.Items = append(
		namespacedAction.Status.Items,
		&storkv1.NamespacedActionStatusItem{
			Action:    action,
			Timestamp: meta.NewTime(schedule.GetCurrentTime()),
			Status:    status,
		})
	return ns.client.Update(context.TODO(), namespacedAction)
}

func (c *NamespacedActionController) createCRD() error {
	resource := apiextensions.CustomResource{
		Name:    storkv1.NamespacedActionResourceName,
		Plural:  storkv1.NamespacedActionResourcePlural,
		Group:   storkv1.SchemeGroupVersion.Group,
		Version: storkv1.SchemeGroupVersion.Version,
		Scope:   apiextensionsv1beta1.NamespaceScoped,
		Kind:    reflect.TypeOf(storkv1.NamespacedAction{}).Name(),
	}
	return k8sutils.CreateCRD(resource, validateCRDInterval, validateCRDTimeout)
}
