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
	"github.com/portworx/sched-ops/k8s/apiextensions"
	storkops "github.com/portworx/sched-ops/k8s/stork"
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
		apiClusterDomainsStatus := obj
		if event.Deleted {
			go func() { c.createClusterDomainsStatusObject() }()
		}
		currentClusterDomains, err := c.Driver.GetClusterDomains()
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
			if err := sdk.Update(apiClusterDomainsStatus); err != nil {
				return err
			}
		}
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
		if _, err := storkops.Instance().CreateClusterDomainsStatus(clusterDomainStatus); err != nil && !errors.IsAlreadyExists(err) {
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
	resource := apiextensions.CustomResource{
		Name:       storkv1.ClusterDomainsStatusResourceName,
		Plural:     storkv1.ClusterDomainsStatusPlural,
		Group:      stork.GroupName,
		Version:    storkv1.SchemeGroupVersion.Version,
		Scope:      apiextensionsv1beta1.ClusterScoped,
		Kind:       reflect.TypeOf(storkv1.ClusterDomainsStatus{}).Name(),
		ShortNames: []string{storkv1.ClusterDomainsStatusShortName},
	}
	err := apiextensions.Instance().CreateCRD(resource)
	if err != nil && !errors.IsAlreadyExists(err) {
		return err
	}

	return apiextensions.Instance().ValidateCRD(resource, validateCRDTimeout, validateCRDInterval)
}

func getNameForClusterDomainsStatus(clusterID string) string {
	clusterDomainsStatusName := strings.ToLower(clusterID)
	return clusterIDRegex.ReplaceAllString(clusterDomainsStatusName, "-")
}
