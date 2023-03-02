package action

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"github.com/libopenstorage/stork/drivers/volume"
	storkv1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/libopenstorage/stork/pkg/controllers"
	"github.com/libopenstorage/stork/pkg/k8sutils"
	"github.com/portworx/sched-ops/k8s/apiextensions"
	storkops "github.com/portworx/sched-ops/k8s/stork"
	"github.com/sirupsen/logrus"
	apiextensionsv1beta1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1beta1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/tools/record"
	runtimeclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const (
	validateCRDInterval time.Duration = 5 * time.Second
	validateCRDTimeout  time.Duration = 1 * time.Minute
)

func NewActionController(mgr manager.Manager, d volume.Driver, r record.EventRecorder) *ActionController {
	return &ActionController{
		client:    mgr.GetClient(),
		volDriver: d,
		recorder:  r,
	}
}

type ActionController struct {
	client    runtimeclient.Client
	volDriver volume.Driver
	recorder  record.EventRecorder
}

func (ac *ActionController) Init(mgr manager.Manager) error {
	err := ac.createCRD()
	if err != nil {
		return err
	}
	return controllers.RegisterTo(mgr, "action-controller", ac, &storkv1.Action{})
}

func (ac *ActionController) Reconcile(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
	action := &storkv1.Action{}
	err := ac.client.Get(context.TODO(), request.NamespacedName, action)
	if err != nil {
		if errors.IsNotFound(err) {
			return reconcile.Result{}, nil
		}
		// Error reading the object - requeue the request.
		return reconcile.Result{RequeueAfter: controllers.DefaultRequeueError}, err
	}

	if err = ac.handle(context.TODO(), action); err != nil {
		logrus.Errorf("%s: %s: %s", reflect.TypeOf(ac), action.Name, err)
		return reconcile.Result{RequeueAfter: controllers.DefaultRequeueError}, err
	}

	return reconcile.Result{RequeueAfter: controllers.DefaultRequeue}, nil
}

func (ac *ActionController) handle(ctx context.Context, action *storkv1.Action) error {
	ac.updateStatus(action, storkv1.ActionStatusInProgress)
	switch action.Spec.ActionType {
	case storkv1.ActionTypeFailover:
		logrus.Infof("[DEBUG] Action handle: Failover")
		resourceutils.ScaleReplicas(action.Namespace, true, printFunc, ac.config)
		// (dgoel) what happens if action completes but not able to update status value
		// should all the actions be idempotent?
		ac.updateStatus(action, storkv1.ActionStatusSuccessful)
	default:
		ac.updateStatus(action, storkv1.ActionStatusFailed)
		return fmt.Errorf("Invalid value received for Action.Spec.ActionType!")
	}
	return nil
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

func (ac *ActionController) updateStatus(action *storkv1.Action, actionStatus storkv1.ActionStatus) {
	action.Status = actionStatus
	_, err := storkops.Instance().UpdateAction(action)
	if err != nil {
		logrus.Errorf("Failed to update Action status: %v/%v", action.Name, actionStatus)
	}
}

func (ac *ActionController) createCRD() error {
	resource := apiextensions.CustomResource{
		Name:    storkv1.ActionResourceName,
		Plural:  storkv1.ActionResourcePlural,
		Group:   storkv1.SchemeGroupVersion.Group,
		Version: storkv1.SchemeGroupVersion.Version,
		Scope:   apiextensionsv1beta1.NamespaceScoped,
		Kind:    reflect.TypeOf(storkv1.Action{}).Name(),
	}
	return k8sutils.CreateCRD(resource, validateCRDInterval, validateCRDTimeout)
}
