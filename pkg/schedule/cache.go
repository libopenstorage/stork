package schedule

import (
	"fmt"
	"time"

	stork_api "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	storkclientset "github.com/libopenstorage/stork/pkg/client/clientset/versioned"
	storkops "github.com/portworx/sched-ops/k8s/stork"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
)

var store cache.Store
var controller cache.Controller

func startSchedulePolicyCache() error {
	resyncPeriod := 30 * time.Second

	config, err := rest.InClusterConfig()
	if err != nil {
		return fmt.Errorf("error getting cluster config: %v", err)
	}

	storkClient, err := storkclientset.NewForConfig(config)
	if err != nil {
		return fmt.Errorf("error getting client, %v", err)
	}

	restClient := storkClient.StorkV1alpha1().RESTClient()

	watchlist := cache.NewListWatchFromClient(restClient, stork_api.SchedulePolicyResourcePlural, v1.NamespaceAll, fields.Everything())
	lw := &cache.ListWatch{
		ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
			return watchlist.List(options)
		},
		WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
			return watchlist.Watch(options)
		},
	}
	store, controller = cache.NewInformer(lw, &stork_api.SchedulePolicy{}, resyncPeriod,
		cache.ResourceEventHandlerFuncs{},
	)

	go controller.Run(wait.NeverStop)
	return nil
}

// getSchedulePolicy gets the schedule policy from a cached store
func getSchedulePolicy(name string) (*stork_api.SchedulePolicy, error) {
	// Won't enable for UTs since ListWatcher doesn't work with fake client
	// https://github.com/kubernetes/client-go/issues/352
	if store == nil {
		return storkops.Instance().GetSchedulePolicy(name)
	}

	obj, _, err := store.GetByKey(name)
	if err != nil {
		return nil, err
	}
	return obj.(*stork_api.SchedulePolicy), nil
}
