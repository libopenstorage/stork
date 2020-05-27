package controllers

import (
	"context"
	"fmt"
	"reflect"

	"github.com/libopenstorage/stork/drivers/volume"
	storkv1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/libopenstorage/stork/pkg/controllers"
	"github.com/libopenstorage/stork/pkg/log"
	"github.com/portworx/sched-ops/k8s/apiextensions"
	storkops "github.com/portworx/sched-ops/k8s/stork"
	"github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	apiextensionsv1beta1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1beta1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/tools/record"
	runtimeclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// NewClusterDomainUpdate creates a new instance of ClusterDomainUpdateController.
func NewClusterDomainUpdate(mgr manager.Manager, d volume.Driver, r record.EventRecorder) *ClusterDomainUpdateController {
	return &ClusterDomainUpdateController{
		client:    mgr.GetClient(),
		volDriver: d,
		recorder:  r,
	}
}

// ClusterDomainUpdateController clusterdomainupdate controller
type ClusterDomainUpdateController struct {
	client runtimeclient.Client

	volDriver volume.Driver
	recorder  record.EventRecorder
}

// Init initialize the clusterdomainupdate controller
func (c *ClusterDomainUpdateController) Init(mgr manager.Manager) error {
	err := c.createCRD()
	if err != nil {
		return err
	}

	return controllers.RegisterTo(mgr, "cluster-domain-update-controller", c, &storkv1.ClusterDomainUpdate{})
}

// Reconcile updates ClusterDomainUpdate resources.
func (c *ClusterDomainUpdateController) Reconcile(request reconcile.Request) (reconcile.Result, error) {
	logrus.Tracef("Reconciling ClusterDomainUpdate %s/%s", request.Namespace, request.Name)

	// Fetch the ApplicationBackup instance
	clusterDomainUpdate := &storkv1.ClusterDomainUpdate{}
	err := c.client.Get(context.TODO(), request.NamespacedName, clusterDomainUpdate)
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

	if err = c.handle(context.TODO(), clusterDomainUpdate); err != nil {
		logrus.Errorf("%s: %s/%s: %s", reflect.TypeOf(c), clusterDomainUpdate.Namespace, clusterDomainUpdate.Name, err)
		return reconcile.Result{RequeueAfter: controllers.DefaultRequeueError}, err
	}

	return reconcile.Result{RequeueAfter: controllers.DefaultRequeue}, nil
}

func (c *ClusterDomainUpdateController) handle(ctx context.Context, clusterDomainUpdate *storkv1.ClusterDomainUpdate) error {
	if clusterDomainUpdate.DeletionTimestamp != nil {
		return nil
	}

	switch clusterDomainUpdate.Status.Status {
	case storkv1.ClusterDomainUpdateStatusInitial:
		var (
			action string
			err    error
		)
		if clusterDomainUpdate.Spec.Active {
			action = "activate"
			err = c.volDriver.ActivateClusterDomain(clusterDomainUpdate)
		} else {
			action = "deactivate"
			err = c.volDriver.DeactivateClusterDomain(clusterDomainUpdate)
		}
		if err != nil {
			err = fmt.Errorf("unable to %v cluster domain: %v", action, err)
			log.ClusterDomainUpdateLog(clusterDomainUpdate).Errorf(err.Error())
			clusterDomainUpdate.Status.Status = storkv1.ClusterDomainUpdateStatusFailed
			clusterDomainUpdate.Status.Reason = err.Error()
			c.recorder.Event(
				clusterDomainUpdate,
				v1.EventTypeWarning,
				string(storkv1.ClusterDomainUpdateStatusFailed),
				err.Error(),
			)

		} else {
			clusterDomainUpdate.Status.Status = storkv1.ClusterDomainUpdateStatusSuccessful
		}

		err = c.client.Update(context.Background(), clusterDomainUpdate)
		if err != nil {
			log.ClusterDomainUpdateLog(clusterDomainUpdate).Errorf("Error updating ClusterDomainUpdate: %v", err)
			return err
		}
		// Do a dummy update on the cluster domain status so that it queries
		// the storage driver and gets updated too
		if clusterDomainUpdate.Status.Status == storkv1.ClusterDomainUpdateStatusSuccessful {
			cdsList, err := storkops.Instance().ListClusterDomainStatuses()
			if err != nil {
				return err
			}
			for _, cds := range cdsList.Items {
				_, err := storkops.Instance().UpdateClusterDomainsStatus(&cds)
				if err != nil {
					return err
				}
			}

		}
		return nil
	case storkv1.ClusterDomainUpdateStatusFailed, storkv1.ClusterDomainUpdateStatusSuccessful:
		return nil
	default:
		log.ClusterDomainUpdateLog(clusterDomainUpdate).Errorf("Invalid status for cluster domain update: %v", clusterDomainUpdate.Status.Status)
	}

	return nil
}

// createCRD creates the CRD for ClusterDomainsStatus object
func (c *ClusterDomainUpdateController) createCRD() error {
	resource := apiextensions.CustomResource{
		Name:       storkv1.ClusterDomainUpdateResourceName,
		Plural:     storkv1.ClusterDomainUpdatePlural,
		Group:      storkv1.SchemeGroupVersion.Group,
		Version:    storkv1.SchemeGroupVersion.Version,
		Scope:      apiextensionsv1beta1.ClusterScoped,
		Kind:       reflect.TypeOf(storkv1.ClusterDomainUpdate{}).Name(),
		ShortNames: []string{storkv1.ClusterDomainUpdateShortName},
	}
	err := apiextensions.Instance().CreateCRD(resource)
	if err != nil && !errors.IsAlreadyExists(err) {
		return err
	}

	return apiextensions.Instance().ValidateCRD(resource, validateCRDTimeout, validateCRDInterval)
}
