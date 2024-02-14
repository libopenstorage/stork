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
	validateCRDInterval          time.Duration = 5 * time.Second
	validateCRDTimeout           time.Duration = 1 * time.Minute
	actionExpiryTime             time.Duration = 24 * time.Hour
	latestMigrationThresholdTime time.Duration = 1 * time.Hour
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

	if action.Status.Stage == storkv1.ActionStageFinal {
		// Will not requeue once action is in final stage
		return reconcile.Result{}, nil
	}

	if !controllers.ContainsFinalizer(action, controllers.FinalizerCleanup) {
		controllers.SetFinalizer(action, controllers.FinalizerCleanup)
		return reconcile.Result{Requeue: true}, ac.client.Update(context.TODO(), action)
	}

	if err = ac.handle(context.TODO(), action); err != nil {
		log.ActionLog(action).Errorf("ActionController handle failed with %s", err)
		ac.recorder.Event(action, v1.EventTypeWarning, "ActionController", err.Error())
		return reconcile.Result{RequeueAfter: controllers.DefaultRequeueError}, err
	}

	if action.Spec.ActionType == storkv1.ActionTypeNearSyncFailover {
		ac.pruneActionIfExpired(action)
	}

	return reconcile.Result{RequeueAfter: controllers.DefaultRequeue}, nil
}

func (ac *ActionController) handle(ctx context.Context, action *storkv1.Action) error {
	if action.DeletionTimestamp != nil {
		if action.GetFinalizers() != nil {
			controllers.RemoveFinalizer(action, controllers.FinalizerCleanup)
			return ac.client.Update(ctx, action)
		}
		return nil
	}

	switch action.Spec.ActionType {
	case storkv1.ActionTypeNearSyncFailover:
		ac.updateStatus(action, storkv1.ActionStatusInProgress)
		if action.Status.Status == storkv1.ActionStatusScheduled {
			log.ActionLog(action).Info("started failover")
			err := ac.volDriver.Failover(action)
			if err != nil {
				ac.updateStatus(action, storkv1.ActionStatusFailed)
				return err
			}
			resourceutils.ScaleReplicas(action.Namespace, true, ac.printFunc(action, "ScaleReplicas"), ac.config)
			ac.updateStatus(action, storkv1.ActionStatusSuccessful)
		}
	case storkv1.ActionTypeFailback:
		log.ActionLog(action).Infof("in stage %s", action.Status.Stage)
		switch action.Status.Stage {
		case storkv1.ActionStageInitial:
			ac.verifyMigrationScheduleBeforeFailback(action)
		case storkv1.ActionStageScaleDownDestination:
			ac.performDeactivateFailBackStage(action)
		case storkv1.ActionStageFinal:
			return nil
		}
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

func (ac *ActionController) updateStatus(action *storkv1.Action, actionStatus storkv1.ActionStatusType) {
	action.Status.Status = actionStatus
	err := ac.client.Update(context.TODO(), action)
	if err != nil {
		log.ActionLog(action).Errorf("failed to update action status to %v with error %v", action.Status, err)
	}
}

func (ac *ActionController) updateAction(action *storkv1.Action) error {
	err := ac.client.Update(context.TODO(), action)
	if err != nil {
		log.ActionLog(action).Errorf("failed to update action status to %v with error %v", action.Status, err)
		return err
	}
	return nil
}

// delete any action older than actionExpiryTime
func (ac *ActionController) pruneActionIfExpired(action *storkv1.Action) {
	if action.Status.Status == storkv1.ActionStatusFailed || action.Status.Status == storkv1.ActionStatusSuccessful {
		if time.Since(action.CreationTimestamp.Local()) >= actionExpiryTime {
			err := storkops.Instance().DeleteAction(action.Name, action.Namespace)
			if err != nil {
				log.ActionLog(action).Errorf("received error when deleting expired action %v", err)
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

// getLatestMigrationStatus returns the a migrationschedule's latest migration's policy type and status
func getLatestMigrationPolicyAndStatus(migrationSchedule storkv1.MigrationSchedule) (storkv1.SchedulePolicyType, *storkv1.ScheduledMigrationStatus) {
	var lastMigrationStatus *storkv1.ScheduledMigrationStatus
	var schedulePolicyType storkv1.SchedulePolicyType
	for schedulePolicyType, policyMigration := range migrationSchedule.Status.Items {
		if len(policyMigration) > 0 {
			lastMigrationStatus = policyMigration[len(policyMigration)-1]
			return schedulePolicyType, lastMigrationStatus
		}
	}
	return schedulePolicyType, lastMigrationStatus
}

func isMigrationComplete(status storkv1.MigrationStatusType) bool {
	if status == storkv1.MigrationStatusPending ||
		status == storkv1.MigrationStatusInProgress {
		return false
	}
	return true
}
