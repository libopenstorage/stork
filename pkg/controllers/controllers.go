package controllers

import (
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

const (
	// FinalizerCleanup is a kubernetes finalizer for stork controllers.
	FinalizerCleanup = "stork.libopenstorage.org/finalizer-cleanup"

	// DefaultRequeue is a reconcile period for a resource on success.
	DefaultRequeue = 10 * time.Second

	// DefaultRequeueError is a reconcile period for a resource on error.
	DefaultRequeueError = 2 * time.Second
)

// RegisterTo creates a new controller for a provided config and registers it to the controller manager.
func RegisterTo(mgr manager.Manager, name string, r reconcile.Reconciler, watchedObjects ...runtime.Object) error {
	// Create a new controller
	c, err := controller.New(name, mgr, controller.Options{
		Reconciler:              r,
		MaxConcurrentReconciles: 10,
	})
	if err != nil {
		return err
	}

	// Watch for changes to primary resource
	for _, obj := range watchedObjects {
		if err = c.Watch(&source.Kind{Type: obj}, &handler.EnqueueRequestForObject{}); err != nil {
			return err
		}
	}

	return nil
}
