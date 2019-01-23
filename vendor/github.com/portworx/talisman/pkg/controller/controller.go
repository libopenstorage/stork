package controller

import (
	"fmt"
	"reflect"
	"time"

	portworx "github.com/portworx/talisman/pkg/apis/portworx"
	api "github.com/portworx/talisman/pkg/apis/portworx/v1beta1"
	clientset "github.com/portworx/talisman/pkg/client/clientset/versioned"
	"github.com/portworx/talisman/pkg/client/clientset/versioned/scheme"
	samplescheme "github.com/portworx/talisman/pkg/client/clientset/versioned/scheme"
	informers "github.com/portworx/talisman/pkg/client/informers/externalversions"
	listers "github.com/portworx/talisman/pkg/client/listers/portworx/v1beta1"
	"github.com/portworx/talisman/pkg/cluster/px"
	"github.com/portworx/talisman/pkg/crd"
	"github.com/sirupsen/logrus"
	"k8s.io/api/core/v1"
	apiextensionsv1beta1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1beta1"
	apiextensionsclient "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	kwatch "k8s.io/apimachinery/pkg/watch"
	kubeinformers "k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
)

const controllerAgentName = "talisman"

const (
	// SuccessSynced is used as part of the Event 'reason' when a Cluster is synced
	SuccessSynced = "Synced"
	// ErrResourceExists is used as part of the Event 'reason' when a Cluster fails
	// to sync due to a Deployment of the same name already existing.
	ErrResourceExists = "ErrResourceExists"

	// MessageResourceExists is the message used for Events when a resource
	// fails to sync due to a Deployment already existing
	MessageResourceExists = "Resource %q already exists and is not managed by Cluster"
	// MessageResourceSynced is the message used for an Event fired when a Cluster
	// is synced successfully
	MessageResourceSynced = "Cluster synced successfully"
)

// Controller is the controller implementation for Cluster resources
type Controller struct {
	// kubeclientset is a standard kubernetes clientset
	kubeclientset kubernetes.Interface
	// pxoperatorclientset is a clientset for our own API group
	pxoperatorclientset clientset.Interface

	apiExtClientset apiextensionsclient.Interface

	clustersLister  listers.ClusterLister
	clustersSynced  cache.InformerSynced
	clusterProvider px.Cluster
	// workqueue is a rate limited work queue. This is used to queue work to be
	// processed instead of performing it as soon as a change happens. This
	// means we can ensure we only process a fixed amount of resources at a
	// time, and makes it easy to ensure we are never processing the same item
	// simultaneously in two different workers.
	workqueue workqueue.RateLimitingInterface
	// recorder is an event recorder for recording Event resources to the
	// Kubernetes API.
	recorder  record.EventRecorder
	resources []crd.CustomResource
}

type event struct {
	key       string
	eventType kwatch.EventType
}

// New returns a new controller for managing portworx clusters
func New(
	kubeclientset kubernetes.Interface,
	pxoperatorclientset clientset.Interface,
	apiExtClientset apiextensionsclient.Interface,
	kubeInformerFactory kubeinformers.SharedInformerFactory,
	pxoperatorInformerFactory informers.SharedInformerFactory) *Controller {

	// obtain references to shared index informers for the PX cluster types
	pxInformer := pxoperatorInformerFactory.Portworx().V1beta1().Clusters()

	// Add portworx types to the default Kubernetes Scheme so Events can be
	// logged for them
	if err := samplescheme.AddToScheme(scheme.Scheme); err != nil {
		logrus.Fatalf("failed to add scheme due to: %v", err)
	}

	resources := []crd.CustomResource{
		{
			Name:    "cluster",
			Plural:  "clusters",
			Group:   portworx.GroupName,
			Version: portworx.Version,
			Scope:   apiextensionsv1beta1.NamespaceScoped,
			Kind:    reflect.TypeOf(api.Cluster{}).Name(),
		},
	}

	clusterProvider, err := px.NewPXClusterProvider(
		"", /* currently, we don't support docker registry secret through operator */
		"" /* kubeconfig will always be in cluster config for operator*/)
	if err != nil {
		logrus.Fatalf("failed to fetch cluster provider. Err: %v", err)
	}

	controller := &Controller{
		kubeclientset:       kubeclientset,
		pxoperatorclientset: pxoperatorclientset,
		apiExtClientset:     apiExtClientset,
		clustersLister:      pxInformer.Lister(),
		clustersSynced:      pxInformer.Informer().HasSynced,
		clusterProvider:     clusterProvider,
		workqueue: workqueue.NewNamedRateLimitingQueue(
			workqueue.DefaultControllerRateLimiter(), "Clusters"),
		recorder:  CreateRecorder(kubeclientset, controllerAgentName, ""),
		resources: resources,
	}

	logrus.Info("Setting up event handlers")

	// Set up an event handler for when Cluster resources change
	pxInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    controller.onAddEvent,
		UpdateFunc: controller.onUpdateEvent,
		DeleteFunc: controller.onDeleteEvent,
	})

	return controller
}

func (c *Controller) onAddEvent(obj interface{}) {
	logrus.Infof("[debug] On add event: %v", obj)
	c.handleObject(obj, kwatch.Added)
}

func (c *Controller) onUpdateEvent(oldObj, newObj interface{}) {
	logrus.Infof("[debug] On update event: %v", newObj)
	c.handleObject(newObj, kwatch.Modified)
}

func (c *Controller) onDeleteEvent(obj interface{}) {
	logrus.Infof("[debug] On delete event: %v", obj)
	c.handleObject(obj, kwatch.Deleted)
}

// handleObject will take any resource implementing metav1.Object and attempt
// to find the Cluster resource that 'owns' it. It does this by looking at the
// objects metadata.ownerReferences field for an appropriate OwnerReference.
// It then enqueues that Cluster resource to be processed. If the object does not
// have an appropriate OwnerReference, it will simply be skipped.
func (c *Controller) handleObject(obj interface{}, eventType kwatch.EventType) {
	var object metav1.Object
	var ok bool
	if object, ok = obj.(metav1.Object); !ok {
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			runtime.HandleError(fmt.Errorf("error decoding object, invalid type"))
			return
		}
		object, ok = tombstone.Obj.(metav1.Object)
		if !ok {
			runtime.HandleError(fmt.Errorf("error decoding object tombstone, invalid type"))
			return
		}
		logrus.Infof("Recovered deleted object '%s' from tombstone", object.GetName())
	}

	logrus.Infof("Processing object: %s", object.GetName())
	cluster, err := c.clustersLister.Clusters(object.GetNamespace()).Get(object.GetName())
	if err != nil {
		logrus.Infof("ignoring orphaned object '%s' of cluster '%s'", object.GetSelfLink(), object.GetName())
		return
	}

	c.enqueueCluster(cluster, eventType)
	return
}

// enqueueCluster takes a Cluster resource and converts it into a namespace/name
// string which is then put onto the work queue. This method should *not* be
// passed resources of any type other than Cluster.
func (c *Controller) enqueueCluster(obj interface{}, eventType kwatch.EventType) {
	var key string
	var err error
	if key, err = cache.MetaNamespaceKeyFunc(obj); err != nil {
		runtime.HandleError(err)
		return
	}

	c.workqueue.AddRateLimited(event{
		key:       key,
		eventType: eventType,
	})
}

// Run will set up the event handlers for types we are interested in, as well
// as syncing informer caches and starting workers. It will block until stopCh
// is closed, at which point it will shutdown the workqueue and wait for
// workers to finish processing their current work items.
func (c *Controller) Run(threadiness int, stopCh <-chan struct{}) error {
	defer runtime.HandleCrash()
	defer c.workqueue.ShutDown()

	logrus.Infof("Registering custom resources: %#v", c.resources)
	ctx := crd.Context{
		Clientset:             c.kubeclientset,
		APIExtensionClientset: c.apiExtClientset,
		Interval:              500 * time.Millisecond,
		Timeout:               60 * time.Second,
	}
	err := crd.CreateCRD(ctx, c.resources)
	if err != nil {
		logrus.Fatalf("failed to create CRD. Err: %v", err)
	}

	// Start the informer factories to begin populating the informer caches
	logrus.Info("Starting controller")

	// Wait for the caches to be synced before starting workers
	logrus.Info("Waiting for informer caches to sync")
	if ok := cache.WaitForCacheSync(stopCh, c.clustersSynced); !ok {
		return fmt.Errorf("failed to wait for caches to sync")
	}

	logrus.Info("Starting workers")
	// Launch workers to process Cluster resources
	for i := 0; i < threadiness; i++ {
		go wait.Until(c.runWorker, time.Second, stopCh)
	}

	logrus.Info("Started workers")
	<-stopCh
	logrus.Info("Shutting down workers")

	return nil
}

// runWorker is a long-running function that will continually call the
// processNextWorkItem function in order to read and process a message on the
// workqueue.
func (c *Controller) runWorker() {
	for c.processNextWorkItem() {
	}
}

// processNextWorkItem will read a single work item off the workqueue and
// attempt to process it, by calling the sync.
func (c *Controller) processNextWorkItem() bool {
	obj, shutdown := c.workqueue.Get()

	if shutdown {
		return false
	}

	// We wrap this block in a func so we can defer c.workqueue.Done.
	err := func(obj interface{}) error {
		// We call Done here so the workqueue knows we have finished
		// processing this item. We also must remember to call Forget if we
		// do not want this work item being re-queued. For example, we do
		// not call Forget if a transient error occurs, instead the item is
		// put back on the workqueue and attempted again after a back-off
		// period.
		defer c.workqueue.Done(obj)
		var ev event
		var ok bool
		// We expect 'event' to come off the workqueue. The key in these events
		// are of the form namespace/name. We do this as the delayed nature of the
		// workqueue means the items in the informer cache may actually be
		// more up to date that when the item was initially put onto the
		// workqueue.
		if ev, ok = obj.(event); !ok {
			// As the item in the workqueue is actually invalid, we call
			// Forget here else we'd go into a loop of attempting to
			// process a work item that is invalid.
			c.workqueue.Forget(obj)
			runtime.HandleError(fmt.Errorf("expected type event in workqueue but got %#v", obj))
			return nil
		}

		// Run the sync, passing it the namespace/name string of the
		// custom resource (e.g PX cluster) to be synced.
		if err := c.sync(ev); err != nil {
			return fmt.Errorf("error syncing '%v': %s", ev, err.Error())
		}
		// Finally, if no error occurs we Forget this item so it does not
		// get queued again until another change happens.
		c.workqueue.Forget(obj)
		logrus.Infof("Successfully synced '%v'", ev)
		return nil
	}(obj)

	if err != nil {
		runtime.HandleError(err)
		return true
	}

	return true
}

// sync compares the actual state with the desired, and attempts to
// converge the two. It then updates the Status block of the Cluster resource
// with the current status of the resource.
func (c *Controller) sync(ev event) error {
	// Convert the namespace/name string into a distinct namespace and name
	namespace, name, err := cache.SplitMetaNamespaceKey(ev.key)
	if err != nil {
		runtime.HandleError(fmt.Errorf("invalid resource key: %s", ev.key))
		return nil
	}

	cluster, err := c.clustersLister.Clusters(namespace).Get(name)
	if err != nil {
		// The Cluster resource may no longer exist, in which case we stop
		// processing.
		if errors.IsNotFound(err) {
			runtime.HandleError(fmt.Errorf("cluster '%s:%s' in work queue no longer exists", namespace, name))
			return nil
		}

		return err
	}

	switch ev.eventType {
	case kwatch.Added:
		err = c.clusterProvider.Create(cluster)
	case kwatch.Modified:
		err = c.clusterProvider.Upgrade(cluster, &px.UpgradeOptions{})
	case kwatch.Deleted:
		err = c.clusterProvider.Delete(cluster, &px.DeleteOptions{})
	default:
		err = fmt.Errorf("unsupported event type for cluster: %s", cluster.Name)
	}

	// If an error occurs during Update, we'll requeue the item so we can
	// attempt processing again later. This could have been caused by a
	// temporary network failure, or any other transient reason.
	if err != nil {
		return err
	}

	// TODO implemented the update of the 'cluster' object here
	// Finally, we update the status block of the Cluster resource to reflect the
	// current state of the world
	err = c.updateClusterStatus(cluster)
	if err != nil {
		return err
	}

	c.recorder.Event(cluster, v1.EventTypeNormal, SuccessSynced, MessageResourceSynced)
	return nil
}

// TODO fix the signature based on px objects
func (c *Controller) updateClusterStatus(cluster *api.Cluster) error {
	// NEVER modify objects from the store. It's a read-only, local cache.
	// You can use DeepCopy() to make a deep copy of original object and modify this copy
	// Or create a copy manually for better performance
	clusterCopy := cluster.DeepCopy()

	// TODO perform additional operations to fetch status. Refer to sample, etcd and rook operators

	_, err := c.pxoperatorclientset.Portworx().Clusters(cluster.Namespace).Update(clusterCopy)
	return err
}

// CreateRecorder creates a event recorder
func CreateRecorder(kubecli kubernetes.Interface, name, namespace string) record.EventRecorder {
	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(logrus.Infof)
	eventBroadcaster.StartRecordingToSink(&v1core.EventSinkImpl{
		Interface: v1core.New(kubecli.Core().RESTClient()).Events(namespace)})
	return eventBroadcaster.NewRecorder(scheme.Scheme, v1.EventSource{Component: name})
}
