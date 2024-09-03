package cache

import (
	"context"
	"fmt"
	"sync"

	storkv1alpha1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/client-go/rest"
	clientCache "k8s.io/client-go/tools/cache"
	controllercache "sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
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

	// ListTransformedPods lists the all the Pods running in a node from the cache after applying TransformFunc
	ListTransformedPods(nodeName string) (*corev1.PodList, error)

	// WatchPods registers the pod event handlers with the informer cache
	WatchPods(fn func(object interface{})) error

	// GetPersistentVolumeClaim returns the PersistentVolumeClaim in a namespace from the cache after applying TransformFunc
	GetPersistentVolumeClaim(pvcName string, namespace string) (*corev1.PersistentVolumeClaim, error)
}

type cache struct {
	controllerCache controllercache.Cache
}

var (
	cacheLock sync.Mutex
	instance  SharedInformerCache

	cacheNotInitializedErr = "shared informer cache has not been initialized yet"
)

func CreateSharedInformerCache(mgr manager.Manager) error {
	cacheLock.Lock()
	defer cacheLock.Unlock()
	if instance != nil {
		return fmt.Errorf("shared informer cache already initialized")
	}
	config, err := rest.InClusterConfig()
	if err != nil {
		return err
	}

	// Define a transform map to modify the informer's objects before they're added to the cache.
	transformMap := controllercache.TransformByObject{
		&corev1.Pod{}: func(obj interface{}) (interface{}, error) {
			podResource, ok := obj.(*corev1.Pod)
			if !ok {
				return nil, fmt.Errorf("unexpected object type: %T", obj)
			}
			currPod := corev1.Pod{}
			currPod.Name = podResource.Name
			currPod.Namespace = podResource.Namespace

			// Only store volumes which we care about.
			for _, podVolume := range podResource.Spec.Volumes {
				if podVolume.PersistentVolumeClaim != nil {
					currPod.Spec.Volumes = append(currPod.Spec.Volumes, podVolume)
				} else if podVolume.PortworxVolume != nil {
					currPod.Spec.Volumes = append(currPod.Spec.Volumes, podVolume)
				} else if podVolume.Ephemeral != nil {
					currPod.Spec.Volumes = append(currPod.Spec.Volumes, podVolume)
				} else if podVolume.CSI != nil {
					currPod.Spec.Volumes = append(currPod.Spec.Volumes, podVolume)
				}
			}

			currPod.ObjectMeta.DeletionTimestamp = podResource.ObjectMeta.DeletionTimestamp

			currPod.Spec.Containers = podResource.Spec.Containers
			currPod.Spec.NodeName = podResource.Spec.NodeName

			currPod.Status.Conditions = podResource.Status.Conditions
			currPod.Status.Reason = podResource.Status.Reason
			currPod.Status.Phase = podResource.Status.Phase
			currPod.Status.HostIP = podResource.Status.HostIP
			return &currPod, nil
		},

		&corev1.PersistentVolumeClaim{}: func(obj interface{}) (interface{}, error) {
			pvc, ok := obj.(*corev1.PersistentVolumeClaim)
			if !ok {
				return nil, fmt.Errorf("unexpected object type: %T", obj)
			}
			currPVC := corev1.PersistentVolumeClaim{}
			currPVC.Name = pvc.Name
			currPVC.Namespace = pvc.Namespace

			currPVC.Annotations = pvc.Annotations

			currPVC.Spec = pvc.Spec
			currPVC.Status = pvc.Status
			return &currPVC, nil
		},
	}

	sharedInformerCache := &cache{}
	// Set the global instance
	instance = sharedInformerCache
	sharedInformerCache.controllerCache, err = controllercache.New(config, controllercache.Options{
		Scheme:            mgr.GetScheme(),
		TransformByObject: transformMap,
	})
	if err != nil {
		logrus.Errorf("error creating shared informer cache: %v", err)
		return err
	}
	// indexing pods by nodeName so that we can list all pods running on a node
	err = sharedInformerCache.controllerCache.IndexField(context.Background(), &corev1.Pod{}, "spec.nodeName", func(obj client.Object) []string {
		podObject, ok := obj.(*corev1.Pod)
		if !ok {
			return []string{}
		}
		return []string{podObject.Spec.NodeName}
	})
	if err != nil {
		logrus.Errorf("error indexing field spec.nodeName for pods: %v", err)
		return err
	}
	go sharedInformerCache.controllerCache.Start(context.Background())

	synced := sharedInformerCache.controllerCache.WaitForCacheSync(context.Background())
	if !synced {
		return fmt.Errorf("error syncing the shared informer cache")
	}
	return nil
}

func Instance() SharedInformerCache {
	cacheLock.Lock()
	defer cacheLock.Unlock()
	return instance
}

// Only used for UTs
func SetTestInstance(s SharedInformerCache) {
	cacheLock.Lock()
	defer cacheLock.Unlock()
	instance = s
}

// GetStorageClass returns the storage class if present in the cache.
func (c *cache) GetStorageClass(storageClassName string) (*storagev1.StorageClass, error) {
	if c == nil || c.controllerCache == nil {
		return nil, fmt.Errorf(cacheNotInitializedErr)
	}
	sc := &storagev1.StorageClass{}
	if err := c.controllerCache.Get(context.Background(), client.ObjectKey{Name: storageClassName}, sc); err != nil {
		return nil, err
	}
	return sc, nil
}

// ListStorageClasses returns a list of storage classes from the cache
func (c *cache) ListStorageClasses() (*storagev1.StorageClassList, error) {
	if c == nil || c.controllerCache == nil {
		return nil, fmt.Errorf(cacheNotInitializedErr)
	}
	scList := &storagev1.StorageClassList{}
	if err := c.controllerCache.List(context.Background(), scList); err != nil {
		return nil, err
	}
	return scList, nil
}

// GetStorageClassForPVC returns the storage class for the provided PVC if present in cache
func (c *cache) GetStorageClassForPVC(pvc *corev1.PersistentVolumeClaim) (*storagev1.StorageClass, error) {
	if c == nil || c.controllerCache == nil {
		return nil, fmt.Errorf(cacheNotInitializedErr)
	}
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
	if c == nil || c.controllerCache == nil {
		return nil, fmt.Errorf(cacheNotInitializedErr)
	}
	appReg := &storkv1alpha1.ApplicationRegistration{}
	if err := c.controllerCache.Get(context.Background(), client.ObjectKey{Name: name}, appReg); err != nil {
		return nil, err
	}
	return appReg, nil
}

// ListApplicationRegistrations lists the application registration CRs from the cache
func (c *cache) ListApplicationRegistrations() (*storkv1alpha1.ApplicationRegistrationList, error) {
	if c == nil || c.controllerCache == nil {
		return nil, fmt.Errorf(cacheNotInitializedErr)
	}
	appRegList := &storkv1alpha1.ApplicationRegistrationList{}
	if err := c.controllerCache.List(context.Background(), appRegList); err != nil {
		return nil, err
	}
	return appRegList, nil
}

// ListTransformedPods lists the all the Pods from the cache after applying TransformFunc
func (c *cache) ListTransformedPods(nodeName string) (*corev1.PodList, error) {
	if c == nil || c.controllerCache == nil {
		return nil, fmt.Errorf(cacheNotInitializedErr)
	}
	podList := &corev1.PodList{}
	fieldSelector := &client.ListOptions{
		FieldSelector: fields.SelectorFromSet(fields.Set{"spec.nodeName": nodeName}),
	}
	if err := c.controllerCache.List(context.Background(), podList, fieldSelector); err != nil {
		return nil, err
	}
	return podList, nil
}

// WatchPods uses handlers for different pod events with shared informers.
func (c *cache) WatchPods(fn func(object interface{})) error {
	informer, err := c.controllerCache.GetInformer(context.Background(), &corev1.Pod{})
	if err != nil {
		logrus.WithError(err).Error("error getting the informer for pods")
		return err
	}

	informer.AddEventHandler(clientCache.ResourceEventHandlerFuncs{
		AddFunc: fn,
		UpdateFunc: func(oldObj, newObj interface{}) {
			// Only considering the new pod object
			fn(newObj)
		},
		DeleteFunc: fn,
	})

	return nil
}

// GetPersistentVolumeClaim returns the transformed PersistentVolumeClaim if present in the cache.
func (c *cache) GetPersistentVolumeClaim(pvcName string, namespace string) (*corev1.PersistentVolumeClaim, error) {
	if c == nil || c.controllerCache == nil {
		return nil, fmt.Errorf(cacheNotInitializedErr)
	}
	pvc := &corev1.PersistentVolumeClaim{}
	if err := c.controllerCache.Get(context.Background(), client.ObjectKey{Name: pvcName, Namespace: namespace}, pvc); err != nil {
		return nil, err
	}
	return pvc, nil
}
