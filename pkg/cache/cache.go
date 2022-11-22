package cache

import (
	"fmt"
	stork "github.com/libopenstorage/stork/pkg/apis/stork"
	storkv1alpha1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	k8s_errors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	restclient "k8s.io/client-go/rest"
	kcache "k8s.io/client-go/tools/cache"
	"sync"
	"time"
)

const (
	defaultResyncPeriod = 30 * time.Second
)

var (
	scResource     = schema.GroupVersionResource{Group: storagev1.GroupName, Version: storagev1.SchemeGroupVersion.Version, Resource: "storageclasses"}
	appRegResource = schema.GroupVersionResource{Group: stork.GroupName, Version: "v1alpha1", Resource: storkv1alpha1.ApplicationRegistrationResourcePlural}
)

// SharedInformerCache  is an eventually consistent cache. The cache interface
// provides APIs to fetch specific k8s objects from the cache. Only a subset
// of k8s objects are currently managed by this cache.
// DO NOT USE it when you need the latest and accurate copy of a CR.
type SharedInformerCache interface {
	// GetStorageClass returns the storage class if present in the cache.
	GetStorageClass(storageClassName string) (*storagev1.StorageClass, error)

	// GetStorageClassForPVC returns the storage class for the provided PVC if present in cache
	GetStorageClassForPVC(pvc *corev1.PersistentVolumeClaim) (*storagev1.StorageClass, error)

	// ListStorageClasses returns a list of storage classes from the cache
	ListStorageClasses() (*storagev1.StorageClassList, error)

	// GetApplicationRegistration returns the ApplicationRegistration CR from the cache
	GetApplicationRegistration(name string) (*storkv1alpha1.ApplicationRegistration, error)

	// ListApplicationRegistrations lists the application registration CRs from the cache
	ListApplicationRegistrations() (*storkv1alpha1.ApplicationRegistrationList, error)
}

type cache struct {
	scInformer     kcache.SharedIndexInformer
	appRegInformer kcache.SharedIndexInformer
}

var (
	cacheLock           sync.Mutex
	sharedInformerCache *cache
)

func CreateSharedInformerCache(config *restclient.Config) error {
	cacheLock.Lock()
	defer cacheLock.Unlock()
	if sharedInformerCache != nil {
		return fmt.Errorf("shared informer cache already initialized")
	}

	dynInterface, err := dynamic.NewForConfig(config)
	if err != nil {
		return err
	}
	dynInformer := dynamicinformer.NewDynamicSharedInformerFactory(dynInterface, defaultResyncPeriod)
	scInformer := dynInformer.ForResource(scResource).Informer()
	appRegInformer := dynInformer.ForResource(appRegResource).Informer()

	scInformer.AddEventHandler(kcache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			sc, ok := obj.(*storagev1.StorageClass)
			if ok {
				logrus.Infof("Added a new storage class to cache: %v (%v)", sc.Name, sc.Provisioner)
			}
		},
	})

	appRegInformer.AddEventHandler(kcache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			appReg, ok := obj.(*storkv1alpha1.ApplicationRegistration)
			if ok {
				logrus.Infof("Added a new application registration to cache: %v (%v)", appReg.Name, appReg.GroupVersionKind())
			}
		},
	})

	defer utilruntime.HandleCrash()

	stopper := make(chan struct{})

	go scInformer.Run(stopper)
	go appRegInformer.Run(stopper)

	// wait for the caches to synchronize before starting the worker
	if !kcache.WaitForCacheSync(stopper, scInformer.HasSynced) {
		err := fmt.Errorf("timed out waiting for caches to sync")
		utilruntime.HandleError(err)
		return err
	}

	if !kcache.WaitForCacheSync(stopper, appRegInformer.HasSynced) {
		err := fmt.Errorf("timed out waiting for caches to sync")
		utilruntime.HandleError(err)
		return err
	}

	sharedInformerCache = &cache{
		scInformer:     scInformer,
		appRegInformer: appRegInformer,
	}
	return nil
}

func Instance() SharedInformerCache {
	cacheLock.Lock()
	defer cacheLock.Unlock()
	return sharedInformerCache
}

// GetStorageClass returns the storage class if present in the cache.
func (c *cache) GetStorageClass(storageClassName string) (*storagev1.StorageClass, error) {
	obj, exists, err := c.scInformer.GetStore().GetByKey(storageClassName)
	if err != nil {
		return nil, err
	} else if !exists {
		return nil, k8s_errors.NewNotFound(storagev1.Resource("storageclass"), storageClassName)
	}
	sc := storagev1.StorageClass{}
	unstructuredObj, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return nil, fmt.Errorf("failed to parse object into storage class")
	}
	if err := runtime.DefaultUnstructuredConverter.
		FromUnstructured(unstructuredObj.UnstructuredContent(), &sc); err != nil {
		return nil, fmt.Errorf("failed to parse object into storage class: %v", obj)
	}

	// Return a copy of the object
	return sc.DeepCopy(), nil
}

// ListStorageClasses returns a list of storage classes from the cache
func (c *cache) ListStorageClasses() (*storagev1.StorageClassList, error) {
	objList := c.scInformer.GetStore().List()
	scList := storagev1.StorageClassList{}
	for _, obj := range objList {
		sc := storagev1.StorageClass{}
		unstructuredObj, ok := obj.(*unstructured.Unstructured)
		if !ok {
			return nil, fmt.Errorf("failed to parse object into storage class")
		}
		if err := runtime.DefaultUnstructuredConverter.
			FromUnstructured(unstructuredObj.UnstructuredContent(), &sc); err != nil {
			return nil, fmt.Errorf("failed to parse object into storage class: %v", obj)
		}

		// Add a copy of the object to the list
		scList.Items = append(scList.Items, *sc.DeepCopy())
	}
	return &scList, nil
}

// GetStorageClassForPVC returns the storage class for the provided PVC if present in cache
func (c *cache) GetStorageClassForPVC(pvc *corev1.PersistentVolumeClaim) (*storagev1.StorageClass, error) {
	var scName string
	if pvc.Spec.StorageClassName != nil && len(*pvc.Spec.StorageClassName) > 0 {
		scName = *pvc.Spec.StorageClassName
	} else {
		scName = pvc.Annotations[corev1.BetaStorageClassAnnotation]
	}

	if len(scName) == 0 {
		return nil, fmt.Errorf("PVC: %s does not have a storage class", pvc.Name)
	}
	return c.GetStorageClass(scName)
}

// GetApplicationRegistration returns the ApplicationRegistration CR from the cache
func (c *cache) GetApplicationRegistration(name string) (*storkv1alpha1.ApplicationRegistration, error) {
	obj, exists, err := c.appRegInformer.GetStore().GetByKey(name)
	if err != nil {
		return nil, err
	} else if !exists {
		return nil, k8s_errors.NewNotFound(storkv1alpha1.Resource("applicationregistration"), name)
	}
	appReg := storkv1alpha1.ApplicationRegistration{}
	unstructuredObj, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return nil, fmt.Errorf("failed to parse object into application registration")
	}
	if err := runtime.DefaultUnstructuredConverter.
		FromUnstructured(unstructuredObj.UnstructuredContent(), &appReg); err != nil {
		return nil, fmt.Errorf("failed to parse object into application registration: %v", obj)
	}
	// Return a copy of the object
	return appReg.DeepCopy(), nil
}

// ListApplicationRegistrations lists the application registration CRs from the cache
func (c *cache) ListApplicationRegistrations() (*storkv1alpha1.ApplicationRegistrationList, error) {
	objList := c.appRegInformer.GetStore().List()
	appRegList := storkv1alpha1.ApplicationRegistrationList{}
	for _, obj := range objList {
		appReg := storkv1alpha1.ApplicationRegistration{}
		unstructuredObj, ok := obj.(*unstructured.Unstructured)
		if !ok {
			return nil, fmt.Errorf("failed to parse object into application registration")
		}
		if err := runtime.DefaultUnstructuredConverter.
			FromUnstructured(unstructuredObj.UnstructuredContent(), &appReg); err != nil {
			return nil, fmt.Errorf("failed to parse object into application registration: %v", obj)
		}
		// Add a copy of the object to the list
		appRegList.Items = append(appRegList.Items, *appReg.DeepCopy())
	}
	return &appRegList, nil
}
