package controllers

import (
	"context"
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"time"

	"github.com/libopenstorage/stork/drivers/volume"
	"github.com/libopenstorage/stork/pkg/apis/stork"
	storkv1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/libopenstorage/stork/pkg/controller"
	"github.com/operator-framework/operator-sdk/pkg/sdk"
	"github.com/portworx/sched-ops/k8s"
	"github.com/portworx/sched-ops/task"
	"github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	apiextensionsv1beta1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1beta1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/record"
)

const (
	validateCRDInterval    time.Duration = 5 * time.Second
	validateCRDTimeout     time.Duration = 1 * time.Minute
	resyncPeriod                         = 1 * time.Minute
	createCdsTimeout                     = 30 * time.Minute
	createCdsRetryInterval               = 10 * time.Second
)

var (
	clusterIDRegex = regexp.MustCompile("[^a-zA-Z0-9-.]+")
)

// ClusterDomainsStatusController clusterdomainsstatus controller
type ClusterDomainsStatusController struct {
	Driver   volume.Driver
	Recorder record.EventRecorder
}

// Init initialize the clusterdomainsstatus controller
func (c *ClusterDomainsStatusController) Init() error {
	err := c.createCRD()
	if err != nil {
		return err
	}

	if err := controller.Register(
		&schema.GroupVersionKind{
			Group:   stork.GroupName,
			Version: storkv1.SchemeGroupVersion.Version,
			Kind:    reflect.TypeOf(storkv1.ClusterDomainsStatus{}).Name(),
		},
		"",
		resyncPeriod,
		c,
	); err != nil {
		return err
	}

	go func() { c.createClusterDomainsStatusObject() }()

	return nil
}

// Handle updates the cluster about the changes in the ClusterDomainsStatus CRD
func (c *ClusterDomainsStatusController) Handle(ctx context.Context, event sdk.Event) error {
	switch obj := event.Object.(type) {
	case *storkv1.ClusterDomainsStatus:
		clusterDomainsStatus := obj
		if event.Deleted {
			go func() { c.createClusterDomainsStatusObject() }()
		}
		clusterDomainsInfo, err := c.Driver.GetClusterDomains()
		updated := false
		if err != nil {
			c.Recorder.Event(
				clusterDomainsStatus,
				v1.EventTypeWarning,
				err.Error(),
				fmt.Sprintf("Failed to update ClusterDomainsStatuses"),
			)
			logrus.Errorf("Failed to get cluster domain info: %v", err)
		} else {
			if len(clusterDomainsInfo.Active) != len(clusterDomainsStatus.Status.Active) ||
				len(clusterDomainsInfo.Inactive) != len(clusterDomainsStatus.Status.Inactive) {
				updated = true
			} else {
				// Check active list
				if !c.doListsMatch(clusterDomainsInfo.Active, clusterDomainsStatus.Status.Active) ||
					!c.doListsMatch(clusterDomainsInfo.Inactive, clusterDomainsStatus.Status.Inactive) {
					updated = true
				}
			}
		}
		if updated {
			clusterDomainsStatus.Status.Active = clusterDomainsInfo.Active
			clusterDomainsStatus.Status.Inactive = clusterDomainsInfo.Inactive
			if err := sdk.Update(clusterDomainsStatus); err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *ClusterDomainsStatusController) doListsMatch(domainListSDK, domainListCRD []string) bool {
	for _, sdkDomain := range domainListSDK {
		for _, crdDomain := range domainListCRD {
			if crdDomain == sdkDomain {
				continue
			}
			// could not find a match
			return false
		}
	}
	return true
}

func (c *ClusterDomainsStatusController) createClusterDomainsStatusObject() {
	t := func() (interface{}, bool, error) {

		clusterID, err := c.Driver.GetClusterID()
		if err != nil {
			return nil, true, fmt.Errorf("failed to create cluster "+
				"domains status object for driver %v: %v", c.Driver.String(), err)
		}
		clusterDomainStatus := &storkv1.ClusterDomainsStatus{
			ObjectMeta: metav1.ObjectMeta{
				Name: getNameForClusterDomainsStatus(clusterID),
			},
		}
		if _, err := k8s.Instance().CreateClusterDomainsStatus(clusterDomainStatus); err != nil && !errors.IsAlreadyExists(err) {
			return nil, true, fmt.Errorf("failed to create cluster domain"+
				" status object for driver %v: %v", c.Driver.String(), err)
		}
		return nil, false, nil
	}

	if _, err := task.DoRetryWithTimeout(t, createCdsTimeout, createCdsRetryInterval); err != nil {
		logrus.Errorf("Failed to start the task for creating cluster domain status object: %v", err)
	}

}

// createCRD creates the CRD for ClusterDomainsStatus object
func (c *ClusterDomainsStatusController) createCRD() error {
	resource := k8s.CustomResource{
		Name:       storkv1.ClusterDomainsStatusResourceName,
		Plural:     storkv1.ClusterDomainsStatusPlural,
		Group:      stork.GroupName,
		Version:    storkv1.SchemeGroupVersion.Version,
		Scope:      apiextensionsv1beta1.ClusterScoped,
		Kind:       reflect.TypeOf(storkv1.ClusterDomainsStatus{}).Name(),
		ShortNames: []string{storkv1.ClusterDomainsStatusShortName},
	}
	err := k8s.Instance().CreateCRD(resource)
	if err != nil && !errors.IsAlreadyExists(err) {
		return err
	}

	return k8s.Instance().ValidateCRD(resource, validateCRDTimeout, validateCRDInterval)
}

func getNameForClusterDomainsStatus(clusterID string) string {
	clusterDomainsStatusName := strings.ToLower(clusterID)
	return clusterIDRegex.ReplaceAllString(clusterDomainsStatusName, "-")
}
