package controllers

import (
	"context"
	"fmt"
	"reflect"
	"strings"
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
	"k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/record"
	runtimeclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const (
	validateCRDInterval    time.Duration = 5 * time.Second
	validateCRDTimeout     time.Duration = 1 * time.Minute
	storkCreatedAnnotation               = "stork.libopenstorage.org/created-by-stork"
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

	// Fetch the ClusterPair instance
	clusterPair := &stork_api.ClusterPair{}
	err := c.client.Get(context.TODO(), request.NamespacedName, clusterPair)
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

	if !controllers.ContainsFinalizer(clusterPair, controllers.FinalizerCleanup) {
		controllers.SetFinalizer(clusterPair, controllers.FinalizerCleanup)
		return reconcile.Result{Requeue: true}, c.client.Update(context.TODO(), clusterPair)
	}

	if err = c.handle(context.TODO(), clusterPair); err != nil {
		logrus.Errorf("%s: %s/%s: %s", reflect.TypeOf(c), clusterPair.Namespace, clusterPair.Name, err)
		return reconcile.Result{RequeueAfter: controllers.DefaultRequeueError}, err
	}

	return reconcile.Result{RequeueAfter: controllers.DefaultRequeue}, nil
}

func (c *ClusterPairController) handle(ctx context.Context, clusterPair *stork_api.ClusterPair) error {
	if clusterPair.DeletionTimestamp != nil {
		if controllers.ContainsFinalizer(clusterPair, controllers.FinalizerCleanup) {
			if err := c.cleanup(clusterPair); err != nil {
				logrus.Errorf("%s: %s", reflect.TypeOf(c), err)
				c.recorder.Event(
					clusterPair,
					v1.EventTypeWarning,
					string(stork_api.ClusterPairStatusDeleting),
					fmt.Sprintf("Cluster Pair delete failed: %v", err.Error()),
				)
				// Do not delete the cluster pair CR
				return nil
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
		return stork_api.ClusterPairStatusInitial, fmt.Errorf("error getting clusterpair %v (%v): %v", clusterPairName, namespace, err)
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
	skipDelete := false
	dependentClusterPairs := make([]string, 0)
	if clusterPair.Status.RemoteStorageID != "" {
		// verify if any other cluster pair using the same RemoteStorageID
		cpList, err := storkops.Instance().ListClusterPairs("")
		if err != nil {
			return err
		}
		for _, cp := range cpList.Items {
			// No need to handle current reconciled clusterpair
			// Since clusterPair will have deleteTimeStamp set and will be ignored
			if cp.Status.RemoteStorageID == clusterPair.Status.RemoteStorageID &&
				cp.DeletionTimestamp == nil {
				skipDelete = true
				dependentClusterPairs = append(dependentClusterPairs, fmt.Sprintf("%s/%s", cp.Namespace, cp.Name))
			}
		}
	}

	// Following is a work around to avoid deleting the earliest clusterpair as the later ones depend on it.
	// The following code block can be removed once each clusterpair has its own corresponding pair info in px.
	if _, ok := clusterPair.Spec.Options["backuplocation"]; ok && skipDelete {
		referenced, err := c.isClusterPairReferenced(clusterPair)
		if err != nil {
			return fmt.Errorf("failed to get clusterpair reference, error: %v", err)
		}
		if referenced {
			return fmt.Errorf("other clusterpairs %v are dependent on this cluster pair and the objectstore location objects created by it", dependentClusterPairs)
		}
	}

	// Delete the backuplocation and secret associated with clusterpair as part of the delete
	if backuplocationName, ok := clusterPair.Spec.Options["backuplocation"]; ok {
		bl, err := storkops.Instance().GetBackupLocation(backuplocationName, clusterPair.Namespace)
		if err != nil && !errors.IsNotFound(err) {
			logrus.Errorf("fetching backuplocation %s in ns %s failed: %v", backuplocationName, clusterPair.Namespace, err)
			return err
		}
		if err == nil && bl.Annotations[storkCreatedAnnotation] == "true" {
			secret, err := core.Instance().GetSecret(bl.Location.SecretConfig, bl.Namespace)
			if err != nil && errors.IsNotFound(err) {
				logrus.Errorf("fetching secret %s in ns %s failed: %v", bl.Location.SecretConfig, bl.Namespace, err)
			}
			if err == nil && secret.Annotations[storkCreatedAnnotation] == "true" {
				err := core.Instance().DeleteSecret(bl.Location.SecretConfig, bl.Namespace)
				if err != nil && !errors.IsNotFound(err) {
					logrus.Errorf("deleting secret %s in ns %s failed: %v", bl.Location.SecretConfig, bl.Namespace, err)
				}
			}
			err = storkops.Instance().DeleteBackupLocation(bl.Name, bl.Namespace)
			if err != nil && !errors.IsNotFound(err) {
				logrus.Errorf("deleting backuplocation %s in ns %s failed: %v", backuplocationName, bl.Namespace, err)
			}
		}
	}

	if !skipDelete && clusterPair.Status.RemoteStorageID != "" {
		return c.volDriver.DeletePair(clusterPair)
	}

	return nil
}

func (c *ClusterPairController) isClusterPairReferenced(clusterPair *stork_api.ClusterPair) (bool, error) {
	pairInfo, err := c.volDriver.GetPair(clusterPair.Status.RemoteStorageID)
	if err != nil {
		return false, err
	}
	if pairInfo == nil {
		return false, fmt.Errorf("clusterpair info not found")
	}
	if credName, ok := pairInfo.Options["CredName"]; ok {
		if blName, exists := clusterPair.Spec.Options["backuplocation"]; exists {
			// sample example of credName in px is like "CredName":"k8s/ns/backuplocation"
			expectedCredName := strings.Join([]string{"k8s", clusterPair.Namespace, blName}, "/")
			if credName == expectedCredName {
				return true, nil
			}
		}
	}
	return false, nil
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
		err := k8sutils.CreateCRDV1(resource)
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
