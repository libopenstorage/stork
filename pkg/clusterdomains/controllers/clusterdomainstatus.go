package controllers

import (
	"context"
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"time"

	"github.com/libopenstorage/stork/drivers/volume"
	storkv1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/libopenstorage/stork/pkg/controllers"
	"github.com/libopenstorage/stork/pkg/k8sutils"
	"github.com/libopenstorage/stork/pkg/version"
	"github.com/portworx/sched-ops/k8s/apiextensions"
	storkops "github.com/portworx/sched-ops/k8s/stork"
	"github.com/portworx/sched-ops/task"
	"github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	apiextensionsv1beta1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1beta1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	runtimeclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const (
	validateCRDInterval    time.Duration = 5 * time.Second
	validateCRDTimeout     time.Duration = 1 * time.Minute
	createCdsTimeout                     = 30 * time.Minute
	createCdsRetryInterval               = 10 * time.Second
)

var (
	clusterIDRegex = regexp.MustCompile("[^a-zA-Z0-9-.]+")
)

// NewClusterDomainsStatus creates a new instance of ClusterDomainsStatusController.
func NewClusterDomainsStatus(mgr manager.Manager, d volume.Driver, r record.EventRecorder) *ClusterDomainsStatusController {
	return &ClusterDomainsStatusController{
		client:    mgr.GetClient(),
		volDriver: d,
		Recorder:  r,
	}
}

// ClusterDomainsStatusController clusterdomainsstatus controller
type ClusterDomainsStatusController struct {
	client runtimeclient.Client

	volDriver volume.Driver
	Recorder  record.EventRecorder
}

// Init initialize the clusterdomainsstatus controller
func (c *ClusterDomainsStatusController) Init(mgr manager.Manager) error {
	err := c.createCRD()
	if err != nil {
		return err
	}

	go func() { c.createClusterDomainsStatusObject() }()

	return controllers.RegisterTo(mgr, "clusters-domains-status-controller", c, &storkv1.ClusterDomainsStatus{})
}

// Reconcile updates the cluster about the changes in the ClusterDomainsStatus CRD.
func (c *ClusterDomainsStatusController) Reconcile(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
	logrus.Tracef("Reconciling ClusterDomainsStatus %s/%s", request.Namespace, request.Name)

	// Fetch the ApplicationBackup instance
	apiClusterDomainsStatus := &storkv1.ClusterDomainsStatus{}
	err := c.client.Get(context.TODO(), request.NamespacedName, apiClusterDomainsStatus)
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

	if err = c.handle(context.TODO(), apiClusterDomainsStatus); err != nil {
		logrus.Errorf("%s: %s/%s: %s", reflect.TypeOf(c), apiClusterDomainsStatus.Namespace, apiClusterDomainsStatus.Name, err)
		return reconcile.Result{RequeueAfter: controllers.DefaultRequeueError}, err
	}

	return reconcile.Result{RequeueAfter: controllers.DefaultRequeue}, nil
}

// Handle updates the cluster about the changes in the ClusterDomainsStatus CRD
func (c *ClusterDomainsStatusController) handle(ctx context.Context, apiClusterDomainsStatus *storkv1.ClusterDomainsStatus) error {
	if apiClusterDomainsStatus.DeletionTimestamp != nil {
		go func() { c.createClusterDomainsStatusObject() }()
	}

	currentClusterDomains, err := c.volDriver.GetClusterDomains()
	updated := false
	if err != nil {
		c.Recorder.Event(
			apiClusterDomainsStatus,
			v1.EventTypeWarning,
			err.Error(),
			"Failed to update ClusterDomainsStatuses",
		)
		logrus.Errorf("Failed to get cluster domain info: %v", err)
	} else {

		if currentClusterDomains.LocalDomain != apiClusterDomainsStatus.Status.LocalDomain {
			updated = true
		} else if len(currentClusterDomains.ClusterDomainInfos) != len(apiClusterDomainsStatus.Status.ClusterDomainInfos) {
			updated = true
		} else {
			// check if the domain infos match
			for _, apiDomainInfo := range apiClusterDomainsStatus.Status.ClusterDomainInfos {
				if !c.doesClusterDomainInfoMatch(apiDomainInfo, currentClusterDomains.ClusterDomainInfos) {
					updated = true
					break
				}
			}
		}
	}
	if updated {
		apiClusterDomainsStatus.Status = *currentClusterDomains
		return c.client.Update(context.TODO(), apiClusterDomainsStatus)
	}

	return nil
}

func (c *ClusterDomainsStatusController) doesClusterDomainInfoMatch(
	clusterDomainInfo storkv1.ClusterDomainInfo,
	currentClusterDomainInfos []storkv1.ClusterDomainInfo,
) bool {
	var (
		isMatch bool
	)
	for _, currentClusterDomainInfo := range currentClusterDomainInfos {
		if currentClusterDomainInfo.Name == clusterDomainInfo.Name {
			if clusterDomainInfo.State == currentClusterDomainInfo.State &&
				clusterDomainInfo.SyncStatus == currentClusterDomainInfo.SyncStatus {
				isMatch = true
			}
			break
		}
	}
	return isMatch
}

func (c *ClusterDomainsStatusController) createClusterDomainsStatusObject() {
	t := func() (interface{}, bool, error) {

		clusterID, err := c.volDriver.GetClusterID()
		if err != nil {
			return nil, true, fmt.Errorf("failed to create cluster "+
				"domains status object for driver %v: %v", c.volDriver.String(), err)
		}
		clusterDomainStatus := &storkv1.ClusterDomainsStatus{
			ObjectMeta: metav1.ObjectMeta{
				Name: getNameForClusterDomainsStatus(clusterID),
			},
		}
		if _, err := storkops.Instance().CreateClusterDomainsStatus(clusterDomainStatus); err != nil && !errors.IsAlreadyExists(err) {
			return nil, true, fmt.Errorf("failed to create cluster domain"+
				" status object for driver %v: %v", c.volDriver.String(), err)
		}
		return nil, false, nil
	}

	if _, err := task.DoRetryWithTimeout(t, createCdsTimeout, createCdsRetryInterval); err != nil {
		logrus.Errorf("Failed to start the task for creating cluster domain status object: %v", err)
	}

}

// createCRD creates the CRD for ClusterDomainsStatus object
func (c *ClusterDomainsStatusController) createCRD() error {
	resource := apiextensions.CustomResource{
		Name:       storkv1.ClusterDomainsStatusResourceName,
		Plural:     storkv1.ClusterDomainsStatusPlural,
		Group:      storkv1.SchemeGroupVersion.Group,
		Version:    storkv1.SchemeGroupVersion.Version,
		Scope:      apiextensionsv1beta1.ClusterScoped,
		Kind:       reflect.TypeOf(storkv1.ClusterDomainsStatus{}).Name(),
		ShortNames: []string{storkv1.ClusterDomainsStatusShortName},
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

func getNameForClusterDomainsStatus(clusterID string) string {
	clusterDomainsStatusName := strings.ToLower(clusterID)
	return clusterIDRegex.ReplaceAllString(clusterDomainsStatusName, "-")
}
