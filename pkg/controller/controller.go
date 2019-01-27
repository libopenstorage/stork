package controller

import (
	"fmt"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/config"
	controllersdk "sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/runtime/signals"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

type controllerNotInitError struct {
}

func (c *controllerNotInitError) Error() string {
	return "Contoller has not been initialized"
}

// Controller to track updates for objects
type controller struct {
	manager    manager.Manager
	controller controllersdk.Controller
	sync.Mutex
	started     bool
	reconcilers map[string][]reconcile.Func
}

var controllerInst *controller

// Init the controller
func Init() error {
	if controllerInst != nil {
		return nil
	}
	cfg, err := config.GetConfig()
	if err != nil {
		return err
	}
	controllerInst = &controller{}
	controllerInst.manager, err = manager.New(cfg, manager.Options{})
	if err != nil {
		return err
	}

	//controllerInst.controller, err = controllersdk.New("stork-controller", controllerInst.manager, controllersdk.Options{
	//	Reconciler: controllerInst,
	//})
	if err != nil {
		return err
	}
	//controllerInst.handlers = make(map[string][]reconcile.Func)
	//sdk.Handle(controllerInst)
	return nil
}

// Run the controller
func Run() error {
	if controllerInst == nil {
		return &controllerNotInitError{}
	}
	controllerInst.Lock()
	defer controllerInst.Unlock()
	err := controllerInst.controller.Start(signals.SetupSignalHandler())
	if err != nil {
		return nil
	}
	controllerInst.started = true
	return nil
}

// Register to get callbacks for updates to objects
// All handlers need to be registered before calling Run()
func Register(
	object runtime.Object,
	reconciler reconcile.Reconciler,
	namespace string,
	resyncPeriod time.Duration,
) error {
	if controllerInst == nil {
		return &controllerNotInitError{}
	}
	controllerInst.Lock()
	defer controllerInst.Unlock()
	if controllerInst.started {
		return fmt.Errorf("Can't register new handlers after starting controller")
	}

	objectType := object.GetObjectKind().GroupVersionKind().String()
	logrus.Debugf("Registering controller for %v", objectType)
	c, err := controllersdk.New(objectType+"-controller", controllerInst.manager, controllersdk.Options{
		Reconciler: reconciler,
	})
	if err != nil {
		return err
	}
	err = c.Watch(
		&source.Kind{
			Type: object,
		},
		&handler.EnqueueRequestForObject{})
	if err != nil {
		return err
	}
	/*
		// Only add Watch if we aren't already watching for the object already.
		// resyncPeriod will be ignored for second call if different
		if controllerInst.reconcilers[objectType] == nil {
			err := controllerInst.controller.Watch(
				&source.Kind{
					Type: object,
				},
				&handler.EnqueueRequestForObject{})
			if err != nil {
				return err
			}
			controllerInst.reconcilers[objectType] = make([]reconcile.Func, 0)
		}
		controllerInst.reconcilers[objectType] = append(controllerInst.reconcilers[objectType], reconciler)
	*/
	logrus.Debugf("Registered controller for %v", objectType)
	return nil
}

/*
// Handle handles updates for registered types
func (c *controller) Reconcile(request reconcile.Request) (reconcile.Result, error) {
	//func (c *controller) Handle(ctx context.Context, event sdk.Event) error {
	gkv := event.Object.GetObjectKind().GroupVersionKind().String()
	var firstErr error
	if reconcilers, ok := c.reconcilers[gkv]; ok {
		for _, reconciler := range reconcilers {
			err := reconciler.Reconcile(request)
			// Keep track of the first error
			if err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}
*/
