package controller

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/libopenstorage/openstorage/pkg/correlation"
	crdv1alpha1 "github.com/portworx/px-object-controller/client/apis/objectservice/v1alpha1"
	clientset "github.com/portworx/px-object-controller/client/clientset/versioned"
	"github.com/portworx/px-object-controller/client/clientset/versioned/scheme"
	bucketscheme "github.com/portworx/px-object-controller/client/clientset/versioned/scheme"
	informers "github.com/portworx/px-object-controller/client/informers/externalversions"
	bucketlisters "github.com/portworx/px-object-controller/client/listers/objectservice/v1alpha1"
	corev1 "k8s.io/client-go/kubernetes/typed/core/v1"

	"github.com/portworx/px-object-controller/pkg/client"
	v1 "k8s.io/api/core/v1"
	k8s_errors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
)

const (
	componentNameController = correlation.Component("pkg/controller")
)

var (
	logrus = correlation.NewPackageLogger(componentNameController)
)

// Config represents a configuration for creating a controller server
type Config struct {
	SdkEndpoint        string
	ResyncPeriod       time.Duration
	RetryIntervalStart time.Duration
	RetryIntervalMax   time.Duration
}

// Controller represents a controller server
type Controller struct {
	config *Config

	k8sBucketClient clientset.Interface
	k8sClient       kubernetes.Interface
	bucketClient    *client.Client
	eventRecorder   record.EventRecorder
	objectFactory   informers.SharedInformerFactory

	bucketQueue        workqueue.RateLimitingInterface
	bucketLister       bucketlisters.PXBucketClaimLister
	bucketListerSynced cache.InformerSynced
	bucketStore        cache.Store

	accessQueue        workqueue.RateLimitingInterface
	accessLister       bucketlisters.PXBucketAccessLister
	accessListerSynced cache.InformerSynced
	accessStore        cache.Store
}

// New returns a new controller server
func New(cfg *Config) (*Controller, error) {

	// Get Openstorage Bucket SDK Client
	sdkBucketClient := client.NewClient(client.Config{
		SdkEndpoint: cfg.SdkEndpoint,
	})

	// Get general k8s clients
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, err
	}
	k8sClient, err := kubernetes.NewForConfig(config)
	if err != nil {
		logrus.Fatalf("failed to create leaderelection client: %v", err)
	}
	k8sBucketClient, err := clientset.NewForConfig(config)
	if err != nil {
		return nil, err
	}

	// Create new controller
	ctrl := &Controller{
		config:          cfg,
		k8sBucketClient: k8sBucketClient,
		k8sClient:       k8sClient,
		bucketClient:    sdkBucketClient,
	}

	// Create factory and informers
	factory := informers.NewSharedInformerFactory(k8sBucketClient, cfg.ResyncPeriod)
	bucketInformer := factory.Object().V1alpha1().PXBucketClaims()
	bucketInformer.Informer().AddEventHandlerWithResyncPeriod(
		cache.ResourceEventHandlerFuncs{
			AddFunc:    func(obj interface{}) { ctrl.enqueueBucketWork(obj) },
			UpdateFunc: func(oldObj, newObj interface{}) { ctrl.enqueueBucketWork(newObj) },
			DeleteFunc: func(obj interface{}) { ctrl.enqueueBucketWork(obj) },
		},
		ctrl.config.ResyncPeriod,
	)
	accessInformer := factory.Object().V1alpha1().PXBucketAccesses()
	accessInformer.Informer().AddEventHandlerWithResyncPeriod(
		cache.ResourceEventHandlerFuncs{
			AddFunc:    func(obj interface{}) { ctrl.enqueueAccessWork(obj) },
			UpdateFunc: func(oldObj, newObj interface{}) { ctrl.enqueueAccessWork(newObj) },
			DeleteFunc: func(obj interface{}) { ctrl.enqueueAccessWork(obj) },
		},
		ctrl.config.ResyncPeriod,
	)

	// Assign bucket CR listers and informers
	bucketRateLimiter := workqueue.NewItemExponentialFailureRateLimiter(ctrl.config.RetryIntervalStart, ctrl.config.RetryIntervalMax)
	ctrl.objectFactory = factory
	ctrl.bucketStore = cache.NewStore(cache.DeletionHandlingMetaNamespaceKeyFunc)
	ctrl.bucketLister = bucketInformer.Lister()
	ctrl.bucketListerSynced = bucketInformer.Informer().HasSynced
	ctrl.bucketQueue = workqueue.NewNamedRateLimitingQueue(bucketRateLimiter, "px-object-controller-bucket")

	// Assign access CR listers and informers
	accessRateLimiter := workqueue.NewItemExponentialFailureRateLimiter(ctrl.config.RetryIntervalStart, ctrl.config.RetryIntervalMax)
	ctrl.accessStore = cache.NewStore(cache.DeletionHandlingMetaNamespaceKeyFunc)
	ctrl.accessLister = accessInformer.Lister()
	ctrl.accessListerSynced = accessInformer.Informer().HasSynced
	ctrl.accessQueue = workqueue.NewNamedRateLimitingQueue(accessRateLimiter, "px-object-controller-access")

	// Broadcaster setup
	broadcaster := record.NewBroadcaster()
	broadcaster.StartLogging(logrus.Infof)
	broadcaster.StartRecordingToSink(&corev1.EventSinkImpl{Interface: k8sClient.CoreV1().Events(v1.NamespaceAll)})
	ctrl.eventRecorder = broadcaster.NewRecorder(scheme.Scheme, v1.EventSource{Component: "px-object-controller"})
	bucketscheme.AddToScheme(scheme.Scheme)

	return ctrl, nil
}

// Run starts the Px Object Service controller
func (ctrl *Controller) Run(workers int, stopCh chan struct{}) {
	ctrl.objectFactory.Start(stopCh)

	for i := 0; i < workers; i++ {
		go wait.Until(ctrl.bucketWorker, 0, stopCh)
		go wait.Until(ctrl.accessWorker, 0, stopCh)
	}

	<-stopCh
}

// bucketWorker is the main worker for PXBucketClaims.
func (ctrl *Controller) bucketWorker() {
	keyObj, quit := ctrl.bucketQueue.Get()
	if quit {
		return
	}
	defer ctrl.bucketQueue.Done(keyObj)
	ctx := correlation.WithCorrelationContext(context.Background(), "px-object-controller/pkg/controller")

	if err := ctrl.processBucket(ctx, keyObj.(string)); err != nil {
		// Rather than wait for a full resync, re-add the key to the
		// queue to be processed.
		ctrl.bucketQueue.AddRateLimited(keyObj)
		logrus.WithContext(ctx).Infof("Failed to sync bucket %q, will retry again: %v", keyObj.(string), err)
	} else {
		// Finally, if no error occurs we Forget this item so it does not
		// get queued again until another change happens.
		ctrl.bucketQueue.Forget(keyObj)
	}
}

func (ctrl *Controller) processBucket(ctx context.Context, key string) error {
	logrus.WithContext(ctx).Infof("syncBucketClaimByKey[%s]", key)

	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	logrus.WithContext(ctx).Infof("processBucket: bucket namespace [%s] name [%s]", namespace, name)
	if err != nil {
		logrus.WithContext(ctx).Errorf("error getting namespace & name of bucketclaim %q to get bucketclaim from informer: %v", key, err)
		return nil
	}
	bucketClaim, err := ctrl.bucketLister.PXBucketClaims(namespace).Get(name)
	if err == nil && bucketClaim.ObjectMeta.DeletionTimestamp == nil {
		var bucketClass *crdv1alpha1.PXBucketClass
		if bucketClaim.Spec.BucketClassName != "" {
			bucketClass, err = ctrl.k8sBucketClient.ObjectV1alpha1().PXBucketClasses().Get(ctx, bucketClaim.Spec.BucketClassName, metav1.GetOptions{})
			if err != nil {
				ctrl.eventRecorder.Event(bucketClaim, v1.EventTypeWarning, "CreateBucketError", fmt.Sprintf("failed to get bucket class %v", key))
				return err
			}
		} else {
			errMsg := fmt.Sprintf("PXBucketClaim %v must reference a PXBucketClass", key)
			ctrl.eventRecorder.Event(bucketClaim, v1.EventTypeWarning, "CreateBucketError", errMsg)
			return errors.New(errMsg)
		}
		ctx, err := ctrl.setupContextFromClass(ctx, bucketClass)
		if err != nil {
			ctrl.eventRecorder.Event(bucketClaim, v1.EventTypeWarning, "CreateBucketError", fmt.Sprintf("invalid bucketclass: %v", err))
			return err
		}

		if bucketClaim.Status != nil && bucketClaim.Status.Provisioned {
			logrus.WithContext(ctx).Infof("bucketclaim %q already provisioned", key)
			_, err := ctrl.storeBucketUpdate(bucketClaim)
			return err
		}

		logrus.WithContext(ctx).Infof("Creating bucketclaim %q", key)
		return ctrl.createBucket(ctx, bucketClaim, bucketClass)
	}
	if err != nil && !k8s_errors.IsNotFound(err) {
		logrus.WithContext(ctx).Infof("error getting bucketclaim %q from informer: %v", key, err)
		return err
	}
	// The bucketclaim is not in informer cache, the event must have been "delete"
	bcObj, found, err := ctrl.bucketStore.GetByKey(key)
	if err != nil {
		logrus.WithContext(ctx).Infof("error getting bucketclaim %q from cache: %v", key, err)
		return nil
	}
	if !found {
		// The controller has already processed the delete event and
		// deleted the bucketclaim from its cache
		logrus.WithContext(ctx).Infof("deletion of bucketclaim %q was already processed", key)
		return nil
	}
	bucketclaim, ok := bcObj.(*crdv1alpha1.PXBucketClaim)
	if !ok {
		logrus.WithContext(ctx).Errorf("expected bc, got %+v", bcObj)
		return nil
	}
	ctx = ctrl.setupContextFromValue(ctx, bucketclaim.Status.BackendType)

	logrus.WithContext(ctx).Infof("deleting bucketclaim %q with driver %s", key, bucketclaim.Status.BackendType)
	ctrl.deleteBucket(ctx, bucketclaim)

	return nil
}

// enqueueBucketClaimWork adds bucketclaim to given work queue.
func (ctrl *Controller) enqueueBucketWork(obj interface{}) {
	// Beware of "xxx deleted" events
	if unknown, ok := obj.(cache.DeletedFinalStateUnknown); ok && unknown.Obj != nil {
		obj = unknown.Obj
	}
	if bucket, ok := obj.(*crdv1alpha1.PXBucketClaim); ok {
		objName, err := cache.DeletionHandlingMetaNamespaceKeyFunc(bucket)
		if err != nil {
			logrus.Errorf("failed to get key from object: %v, %v", err, bucket)
			return
		}
		logrus.Infof("enqueued %q for sync", objName)
		ctrl.bucketQueue.Add(objName)
	}
}

// bucketWorker is the main worker for PXBucketClaims.
func (ctrl *Controller) accessWorker() {
	keyObj, quit := ctrl.accessQueue.Get()
	if quit {
		return
	}
	defer ctrl.accessQueue.Done(keyObj)
	ctx := correlation.WithCorrelationContext(context.Background(), "px-object-controller/pkg/controller")

	if err := ctrl.processAccess(ctx, keyObj.(string)); err != nil {
		// Rather than wait for a full resync, re-add the key to the
		// queue to be processed.
		ctrl.accessQueue.AddRateLimited(keyObj)
		logrus.WithContext(ctx).Infof("Failed to sync bucket access %q, will retry again: %v", keyObj.(string), err)
	} else {
		// Finally, if no error occurs we Forget this item so it does not
		// get queued again until another change happens.
		ctrl.accessQueue.Forget(keyObj)
	}
}

func (ctrl *Controller) processAccess(ctx context.Context, key string) error {
	logrus.WithContext(ctx).Infof("syncBucketAccessByKey[%s]", key)

	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	logrus.WithContext(ctx).Infof("processBucket: bucket access namespace [%s] name [%s]", namespace, name)
	if err != nil {
		logrus.WithContext(ctx).Errorf("error getting namespace & name of bucketaccess %q to get bucketaccess from informer: %v", key, err)
		return nil
	}
	bucketAccess, err := ctrl.accessLister.PXBucketAccesses(namespace).Get(name)
	if err == nil && bucketAccess.ObjectMeta.DeletionTimestamp == nil {
		var bucketClass *crdv1alpha1.PXBucketClass
		if bucketAccess.Spec.BucketClassName != "" {
			bucketClass, err = ctrl.k8sBucketClient.ObjectV1alpha1().PXBucketClasses().Get(ctx, bucketAccess.Spec.BucketClassName, metav1.GetOptions{})
			if err != nil {
				return err
			}
		} else {
			return errors.New("PXBucketAccess must reference a PXBucketClass")
		}
		ctx, err := ctrl.setupContextFromClass(ctx, bucketClass)
		if err != nil {
			return err
		}

		if bucketAccess.Status != nil && bucketAccess.Status.AccessGranted {
			logrus.WithContext(ctx).Infof("access already granted to bucket %s for bucket access %s", bucketAccess.Status.BucketId, key)
			_, err = ctrl.storeAccessUpdate(bucketAccess)
			return err
		}

		var bucketID string
		if bucketAccess.Spec.BucketClaimName != "" {
			pbc, err := ctrl.k8sBucketClient.ObjectV1alpha1().PXBucketClaims(bucketAccess.Namespace).Get(ctx, bucketAccess.Spec.BucketClaimName, metav1.GetOptions{})
			if err != nil {
				errMsg := fmt.Sprintf("failed to get bucketclaim %s", bucketAccess.Spec.BucketClaimName)
				ctrl.eventRecorder.Event(bucketAccess, v1.EventTypeWarning, "GrantAccessError", errMsg)
				return err
			}
			if pbc.Status == nil {
				errMsg := fmt.Sprintf("bucket claim %s exists but is not yet provisioned", bucketAccess.Spec.BucketClaimName)
				ctrl.eventRecorder.Event(bucketAccess, v1.EventTypeWarning, "GrantAccessError", errMsg)
				return fmt.Errorf(errMsg)
			}

			bucketID = getBucketID(pbc)
		}
		if bucketAccess.Spec.ExistingBucketId != "" {
			bucketID = bucketAccess.Spec.ExistingBucketId
		}

		logrus.WithContext(ctx).Infof("Creating bucketaccess %q for bucket ID %v", key, bucketID)
		return ctrl.createAccess(ctx, bucketAccess, bucketClass, bucketID)
	}
	if err != nil && !k8s_errors.IsNotFound(err) {
		logrus.WithContext(ctx).Infof("error getting bucketaccess %q from informer: %v", key, err)
		return err
	}
	// The bucketaccess is not in informer cache, the event must have been "delete"
	bacObj, found, err := ctrl.accessStore.GetByKey(key)
	if err != nil {
		logrus.WithContext(ctx).Infof("error getting bucketaccess %q from cache: %v", key, err)
		return nil
	}
	if !found {
		// The controller has already processed the delete event and
		// deleted the bucketaccess from its cache
		logrus.WithContext(ctx).Infof("deletion of bucketaccess %q was already processed", key)
		return nil
	}
	bucketaccess, ok := bacObj.(*crdv1alpha1.PXBucketAccess)
	if !ok {
		logrus.WithContext(ctx).Errorf("expected bc, got %+v", bacObj)
		return nil
	}
	ctx = ctrl.setupContextFromValue(ctx, bucketaccess.Status.BackendType)

	logrus.WithContext(ctx).Infof("deleting bucketaccess %q", key)
	return ctrl.revokeAccess(ctx, bucketaccess)
}

// enqueueBucketClaimWork adds bucketclaim to given work queue.
func (ctrl *Controller) enqueueAccessWork(obj interface{}) {
	// Beware of "xxx deleted" events
	if unknown, ok := obj.(cache.DeletedFinalStateUnknown); ok && unknown.Obj != nil {
		obj = unknown.Obj
	}
	if access, ok := obj.(*crdv1alpha1.PXBucketAccess); ok {
		objName, err := cache.DeletionHandlingMetaNamespaceKeyFunc(access)
		if err != nil {
			logrus.Errorf("failed to get key from object: %v, %v", err, access)
			return
		}
		logrus.Infof("enqueued %q for sync", objName)
		ctrl.accessQueue.Add(objName)
	}
}
