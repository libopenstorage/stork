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
	"github.com/libopenstorage/stork/pkg/log"
	"github.com/libopenstorage/stork/pkg/resourceutils"
	"github.com/portworx/sched-ops/k8s/apiextensions"
	storkops "github.com/portworx/sched-ops/k8s/stork"
	"github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	apiextensionsv1beta1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1beta1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/record"
	runtimeclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const (
	validateCRDInterval time.Duration = 5 * time.Second
	validateCRDTimeout  time.Duration = 1 * time.Minute
	actionExpiryTime    time.Duration = 24 * time.Hour
)

func NewActionController(mgr manager.Manager, d volume.Driver, r record.EventRecorder) *ActionController {
	config, err := rest.InClusterConfig()
	if err != nil {
		logrus.Fatalf("error getting cluster config: %v", err)
	}
	return &ActionController{
		client:    mgr.GetClient(),
		volDriver: d,
		recorder:  r,
		config:    config,
	}
}

type ActionController struct {
	client    runtimeclient.Client
	volDriver volume.Driver
	recorder  record.EventRecorder
	config    *rest.Config
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
		return reconcile.Result{RequeueAfter: controllers.DefaultRequeueError}, err
	}

	if action.Status == storkv1.ActionStatusScheduled {
		if err = ac.handle(context.TODO(), action); err != nil {
			log.ActionLog(action).Errorf("ActionController handle failed with %s", err)
			ac.recorder.Event(action, v1.EventTypeWarning, "ActionController", err.Error())
			return reconcile.Result{RequeueAfter: controllers.DefaultRequeueError}, err
		}
	}

	ac.pruneActions(action.Namespace)

	return reconcile.Result{RequeueAfter: controllers.DefaultRequeue}, nil
}

func (ac *ActionController) handle(ctx context.Context, action *storkv1.Action) error {
	ac.updateStatus(action, storkv1.ActionStatusInProgress)
	switch action.Spec.ActionType {
	case storkv1.ActionTypeFailover:
		log.ActionLog(action).Info("started failover")
		err := ac.volDriver.Failover(action)
		if err != nil {
			ac.updateStatus(action, storkv1.ActionStatusFailed)
			return err
		}
		resourceutils.ScaleReplicas(action.Namespace, true, ac.printFunc(action, "ScaleReplicas"), ac.config)
		ac.updateStatus(action, storkv1.ActionStatusSuccessful)
	default:
		ac.updateStatus(action, storkv1.ActionStatusFailed)
		return fmt.Errorf("invalid value received for Action.Spec.ActionType")
	}
	return nil
}

func (ac *ActionController) printFunc(action *storkv1.Action, reason string) func(string, string) {
	return func(msg, stream string) {
		actionLog := log.ActionLog(action)
		switch stream {
		case "out":
			actionLog.Infof(msg)
			ac.recorder.Event(action, v1.EventTypeNormal, reason, msg)
		case "err":
			actionLog.Errorf(msg)
			ac.recorder.Event(action, v1.EventTypeWarning, reason, msg)
		default:
			actionLog.Errorf("printFunc received invalid stream")
			actionLog.Errorf(msg)
			ac.recorder.Event(action, v1.EventTypeWarning, reason, msg)
		}
	}
}

func (ac *ActionController) updateStatus(action *storkv1.Action, actionStatus storkv1.ActionStatus) {
	action.Status = actionStatus
	err := ac.client.Update(context.TODO(), action)
	if err != nil {
		log.ActionLog(action).Errorf("failed to update action status to %v with error %v", action.Status, err)
	}
}

// delete any action older than actionExpiryTime
func (ac *ActionController) pruneActions(namespace string) {
	actionList, err := storkops.Instance().ListActions(namespace)
	if err != nil {
		logrus.Errorf("call to fetch ActionList to prune older actions failed with error %v", err)
	}
	for _, action := range actionList.Items {
		if time.Now().Sub(action.CreationTimestamp.Local()) >= actionExpiryTime {
			err = storkops.Instance().DeleteAction(action.Name, namespace)
			if err != nil {
				log.ActionLog(&action).Errorf("received error when deleting expired action %v", err)
			}
		}
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
