package controllers

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"github.com/libopenstorage/stork/drivers/volume"
	stork_api "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/libopenstorage/stork/pkg/controllers"
	"github.com/libopenstorage/stork/pkg/k8sutils"
	"github.com/libopenstorage/stork/pkg/version"
	"github.com/portworx/sched-ops/k8s/apiextensions"
	"github.com/portworx/sched-ops/k8s/core"
	storkops "github.com/portworx/sched-ops/k8s/stork"
	"github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	apiextensionsv1beta1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1beta1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/record"
	runtimeclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const (
	validateCRDInterval time.Duration = 5 * time.Second
	validateCRDTimeout  time.Duration = 1 * time.Minute
)

// NewClusterPair creates a new instance of ClusterPairController.
func NewClusterPair(mgr manager.Manager, d volume.Driver, r record.EventRecorder) *ClusterPairController {
	return &ClusterPairController{
		client:    mgr.GetClient(),
		volDriver: d,
		recorder:  r,
	}
}

// ClusterPairController controller to watch over ClusterPair
type ClusterPairController struct {
	client runtimeclient.Client

	volDriver volume.Driver
	recorder  record.EventRecorder
}

// Init initialize the cluster pair controller
func (c *ClusterPairController) Init(mgr manager.Manager) error {
	err := c.createCRD()
	if err != nil {
		return err
	}

	return controllers.RegisterTo(mgr, "cluster-pair-controller", c, &stork_api.ClusterPair{})
}

// Reconcile manages ClusterPair resources.
func (c *ClusterPairController) Reconcile(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
	logrus.Tracef("Reconciling ClusterPair %s/%s", request.Namespace, request.Name)

	// Fetch the ApplicationBackup instance
	backup := &stork_api.ClusterPair{}
	err := c.client.Get(context.TODO(), request.NamespacedName, backup)
	if err != nil {
		if errors.IsNotFound(err) {
			// Request object not found, could have been deleted after reconcile request.
			// Owned objects are automatically garbage collected. For additional cleanup logic use finalizers.
			// Return and don't requeue
			return reconcile.Result{}, nil
		}
		// Error reading the object - requeue the request.
		return reconcile.Result{RequeueAfter: controllers.DefaultRequeueError}, err
	}

	if !controllers.ContainsFinalizer(backup, controllers.FinalizerCleanup) {
		controllers.SetFinalizer(backup, controllers.FinalizerCleanup)
		return reconcile.Result{Requeue: true}, c.client.Update(context.TODO(), backup)
	}

	if err = c.handle(context.TODO(), backup); err != nil {
		logrus.Errorf("%s: %s/%s: %s", reflect.TypeOf(c), backup.Namespace, backup.Name, err)
		return reconcile.Result{RequeueAfter: controllers.DefaultRequeueError}, err
	}

	return reconcile.Result{RequeueAfter: controllers.DefaultRequeue}, nil
}

func (c *ClusterPairController) handle(ctx context.Context, clusterPair *stork_api.ClusterPair) error {
	if clusterPair.DeletionTimestamp != nil {
		if controllers.ContainsFinalizer(clusterPair, controllers.FinalizerCleanup) {
			if err := c.cleanup(clusterPair); err != nil {
				logrus.Errorf("%s: %s", reflect.TypeOf(c), err)
			}
		}

		if clusterPair.GetFinalizers() != nil {
			controllers.RemoveFinalizer(clusterPair, controllers.FinalizerCleanup)
			return c.client.Update(ctx, clusterPair)
		}

		return nil
	}

	if _, ok := clusterPair.Spec.Options["token"]; !ok {
		clusterPair.Status.StorageStatus = stork_api.ClusterPairStatusNotProvided
		c.recorder.Event(clusterPair,
			v1.EventTypeNormal,
			string(clusterPair.Status.StorageStatus),
			"Skipping storage pairing since no storage options provided")
		err := c.client.Update(context.TODO(), clusterPair)
		if err != nil {
			return err
		}
	} else {
		if clusterPair.Status.StorageStatus != stork_api.ClusterPairStatusReady {
			remoteID, err := c.volDriver.CreatePair(clusterPair)
			if err != nil {
				clusterPair.Status.StorageStatus = stork_api.ClusterPairStatusError
				c.recorder.Event(clusterPair,
					v1.EventTypeWarning,
					string(clusterPair.Status.StorageStatus),
					err.Error())
			} else {
				clusterPair.Status.StorageStatus = stork_api.ClusterPairStatusReady
				c.recorder.Event(clusterPair,
					v1.EventTypeNormal,
					string(clusterPair.Status.StorageStatus),
					"Storage successfully paired")
				clusterPair.Status.RemoteStorageID = remoteID
			}
			err = c.client.Update(context.TODO(), clusterPair)
			if err != nil {
				return err
			}
		}
	}
	if clusterPair.Status.SchedulerStatus != stork_api.ClusterPairStatusReady {
		clusterPair.Status.SchedulerStatus = stork_api.ClusterPairStatusError
		remoteConfig, err := getClusterPairSchedulerConfig(clusterPair.Name, clusterPair.Namespace)
		if err != nil {
			return err
		}

		client, err := kubernetes.NewForConfig(remoteConfig)
		if err != nil {
			return err
		}
		if _, err = client.ServerVersion(); err != nil {
			c.recorder.Event(clusterPair,
				v1.EventTypeWarning,
				string(clusterPair.Status.SchedulerStatus),
				err.Error())
			return c.client.Update(context.TODO(), clusterPair)
		}
		if err := c.createBackupLocationOnRemote(remoteConfig, clusterPair); err != nil {
			c.recorder.Event(clusterPair,
				v1.EventTypeWarning,
				string(clusterPair.Status.SchedulerStatus),
				err.Error())
			return c.client.Update(context.TODO(), clusterPair)
		}
		clusterPair.Status.SchedulerStatus = stork_api.ClusterPairStatusReady
		c.recorder.Event(clusterPair,
			v1.EventTypeNormal,
			string(clusterPair.Status.SchedulerStatus),
			"Scheduler successfully paired")

		err = c.client.Update(context.TODO(), clusterPair)
		if err != nil {
			return err
		}
	}

	return nil
}

func getClusterPairSchedulerConfig(clusterPairName string, namespace string) (*restclient.Config, error) {
	clusterPair, err := storkops.Instance().GetClusterPair(clusterPairName, namespace)
	if err != nil {
		return nil, fmt.Errorf("error getting clusterpair (%v/%v): %v", namespace, clusterPairName, err)
	}
	remoteClientConfig := clientcmd.NewNonInteractiveClientConfig(
		clusterPair.Spec.Config,
		clusterPair.Spec.Config.CurrentContext,
		&clientcmd.ConfigOverrides{},
		clientcmd.NewDefaultClientConfigLoadingRules())
	return remoteClientConfig.ClientConfig()
}

func getClusterPairStorageStatus(clusterPairName string, namespace string) (stork_api.ClusterPairStatusType, error) {
	clusterPair, err := storkops.Instance().GetClusterPair(clusterPairName, namespace)
	if err != nil {
		return stork_api.ClusterPairStatusInitial, fmt.Errorf("error getting clusterpair: %v", err)
	}
	return clusterPair.Status.StorageStatus, nil
}

func getClusterPairSchedulerStatus(clusterPairName string, namespace string) (stork_api.ClusterPairStatusType, error) {
	clusterPair, err := storkops.Instance().GetClusterPair(clusterPairName, namespace)
	if err != nil {
		return stork_api.ClusterPairStatusInitial, fmt.Errorf("error getting clusterpair: %v", err)
	}
	return clusterPair.Status.SchedulerStatus, nil
}

func (c *ClusterPairController) cleanup(clusterPair *stork_api.ClusterPair) error {
	if clusterPair.Status.RemoteStorageID != "" {
		return c.volDriver.DeletePair(clusterPair)
	}
	return nil
}

func (c *ClusterPairController) createCRD() error {
	resource := apiextensions.CustomResource{
		Name:    stork_api.ClusterPairResourceName,
		Plural:  stork_api.ClusterPairResourcePlural,
		Group:   stork_api.SchemeGroupVersion.Group,
		Version: stork_api.SchemeGroupVersion.Version,
		Scope:   apiextensionsv1beta1.NamespaceScoped,
		Kind:    reflect.TypeOf(stork_api.ClusterPair{}).Name(),
	}
	ok, err := version.RequiresV1Registration()
	if err != nil {
		return err
	}
	if ok {
		err := k8sutils.CreateCRD(resource)
		if err != nil && !errors.IsAlreadyExists(err) {
			return err
		}
		return apiextensions.Instance().ValidateCRD(resource.Plural+"."+resource.Group, validateCRDTimeout, validateCRDInterval)
	}
	err = apiextensions.Instance().CreateCRDV1beta1(resource)
	if err != nil && !errors.IsAlreadyExists(err) {
		return err
	}
	return apiextensions.Instance().ValidateCRDV1beta1(resource, validateCRDTimeout, validateCRDInterval)
}

func (c *ClusterPairController) createBackupLocationOnRemote(remoteConfig *restclient.Config, clusterPair *stork_api.ClusterPair) error {
	if bkpl, ok := clusterPair.Spec.Options[stork_api.BackupLocationResourceName]; ok {
		remoteClient, err := storkops.NewForConfig(remoteConfig)
		if err != nil {
			return err
		}
		client, err := kubernetes.NewForConfig(remoteConfig)
		if err != nil {
			return err
		}
		ns, err := core.Instance().GetNamespace(clusterPair.Namespace)
		if err != nil {
			return err
		}
		// Don't create if the namespace already exists on the remote cluster
		_, err = client.CoreV1().Namespaces().Get(context.TODO(), clusterPair.Namespace, metav1.GetOptions{})
		if err != nil {
			// create namespace on destination cluster
			_, err = client.CoreV1().Namespaces().Create(context.TODO(), &v1.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name:        ns.Name,
					Labels:      ns.Labels,
					Annotations: ns.Annotations,
				},
			}, metav1.CreateOptions{})
			if err != nil && !errors.IsAlreadyExists(err) {
				return err
			}
		}
		// create backuplocation on destination cluster
		resp, err := storkops.Instance().GetBackupLocation(bkpl, clusterPair.GetNamespace())
		if err != nil {
			return err
		}
		resp.ResourceVersion = ""
		if _, err := remoteClient.CreateBackupLocation(resp); err != nil && !errors.IsAlreadyExists(err) {
			return err
		}
	}
	return nil
}
