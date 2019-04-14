package initializer

import (
	"fmt"
	"sync"
	"time"

	"github.com/libopenstorage/stork/drivers/volume"
	"github.com/sirupsen/logrus"
	appv1 "k8s.io/api/apps/v1"
	appv1beta1 "k8s.io/api/apps/v1beta1"
	appv1beta2 "k8s.io/api/apps/v1beta2"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
)

const (
	storkInitializerName = "stork.initializer.kubernetes.io"
	storkSchedulerName   = "stork"
	defaultSchedulerName = "default-scheduler"
)

// Initializer Kubernetes object initializer
type Initializer struct {
	Driver      volume.Driver
	lock        sync.Mutex
	started     bool
	stopChannel chan struct{}
}

// Start Starts the Initializer
func (i *Initializer) Start() error {
	i.lock.Lock()
	defer i.lock.Unlock()

	if i.started {
		return fmt.Errorf("Initializer has already been started")
	}

	config, err := rest.InClusterConfig()
	if err != nil {
		return fmt.Errorf("error getting cluster config: %v", err)
	}

	k8sClient, err := kubernetes.NewForConfig(config)
	if err != nil {
		return fmt.Errorf("error getting client, %v", err)
	}

	restClient := k8sClient.AppsV1beta1().RESTClient()
	err = i.startInitializerController(restClient, k8sClient, "deployments", &appv1beta1.Deployment{})
	if err != nil {
		return fmt.Errorf("error creating init controller for deployments: %v", err)
	}
	err = i.startInitializerController(restClient, k8sClient, "statefulsets", &appv1beta1.StatefulSet{})
	if err != nil {
		return fmt.Errorf("error creating init controller for statefulsets: %v", err)
	}

	restClient = k8sClient.AppsV1beta2().RESTClient()
	err = i.startInitializerController(restClient, k8sClient, "deployments", &appv1beta2.Deployment{})
	if err != nil {
		return fmt.Errorf("error creating init controller for deployments: %v", err)
	}
	err = i.startInitializerController(restClient, k8sClient, "statefulsets", &appv1beta2.StatefulSet{})
	if err != nil {
		return fmt.Errorf("error creating init controller for statefulsets: %v", err)
	}

	restClient = k8sClient.AppsV1().RESTClient()
	err = i.startInitializerController(restClient, k8sClient, "deployments", &appv1.Deployment{})
	if err != nil {
		return fmt.Errorf("error creating init controller for deployments: %v", err)
	}
	err = i.startInitializerController(restClient, k8sClient, "statefulsets", &appv1.StatefulSet{})
	if err != nil {
		return fmt.Errorf("error creating init controller for statefulsets: %v", err)
	}
	i.started = true
	return nil
}

func (i *Initializer) startInitializerController(
	restClient rest.Interface,
	k8sClient *kubernetes.Clientset,
	resource string,
	objType runtime.Object,
) error {
	resyncPeriod := 30 * time.Second

	i.stopChannel = make(chan struct{})
	watchlist := cache.NewListWatchFromClient(restClient, resource, v1.NamespaceAll, fields.Everything())
	includeUninitializedWatchlist := &cache.ListWatch{
		ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
			options.IncludeUninitialized = true
			return watchlist.List(options)
		},
		WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
			options.IncludeUninitialized = true
			return watchlist.Watch(options)
		},
	}

	_, initController := cache.NewInformer(includeUninitializedWatchlist, objType, resyncPeriod,
		cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				err := i.initializeObject(obj, k8sClient)
				if err != nil {
					logrus.Errorf("error initializing %v: %v", resource, err)
				}
			},
		},
	)

	go initController.Run(i.stopChannel)
	return nil
}

// Stop Stops the initializer
func (i *Initializer) Stop() error {
	i.lock.Lock()
	defer i.lock.Unlock()

	if !i.started {
		return fmt.Errorf("Initializer has not been started")
	}

	close(i.stopChannel)
	i.started = false
	return nil
}

func (i *Initializer) initializeObject(obj interface{}, clientset *kubernetes.Clientset) error {
	switch obj := obj.(type) {
	case *appv1.StatefulSet:
		return i.initializeStatefulSetV1(obj, clientset)
	case *appv1beta1.StatefulSet:
		return i.initializeStatefulSetV1Beta1(obj, clientset)
	case *appv1beta2.StatefulSet:
		return i.initializeStatefulSetV1Beta2(obj, clientset)
	case *appv1.Deployment:
		return i.initializeDeploymentV1(obj, clientset)
	case *appv1beta1.Deployment:
		return i.initializeDeploymentV1Beta1(obj, clientset)
	case *appv1beta2.Deployment:
		return i.initializeDeploymentV1Beta2(obj, clientset)
	default:
		return fmt.Errorf("unsupported app type: %v", obj)
	}
}
