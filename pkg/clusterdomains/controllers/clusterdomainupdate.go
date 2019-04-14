package controllers

import (
	"context"
	"fmt"
	"reflect"

	"github.com/libopenstorage/stork/drivers/volume"
	"github.com/libopenstorage/stork/pkg/apis/stork"
	storkv1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/libopenstorage/stork/pkg/controller"
	"github.com/libopenstorage/stork/pkg/log"
	"github.com/operator-framework/operator-sdk/pkg/sdk"
	"github.com/portworx/sched-ops/k8s"
	v1 "k8s.io/api/core/v1"
	apiextensionsv1beta1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1beta1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/record"
)

// ClusterDomainUpdateController clusterdomainupdate controller
type ClusterDomainUpdateController struct {
	Driver   volume.Driver
	Recorder record.EventRecorder
}

// Init initialize the clusterdomainupdate controller
func (c *ClusterDomainUpdateController) Init() error {
	err := c.createCRD()
	if err != nil {
		return err
	}

	return controller.Register(
		&schema.GroupVersionKind{
			Group:   stork.GroupName,
			Version: storkv1.SchemeGroupVersion.Version,
			Kind:    reflect.TypeOf(storkv1.ClusterDomainUpdate{}).Name(),
		},
		"",
		resyncPeriod,
		c)
}

// Handle updates the cluster about the changes in the ClusterDomainsStatus CRD
func (c *ClusterDomainUpdateController) Handle(ctx context.Context, event sdk.Event) error {
	switch obj := event.Object.(type) {
	case *storkv1.ClusterDomainUpdate:
		clusterDomainUpdate := obj
		if event.Deleted {
			// No op
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
				err = c.Driver.ActivateClusterDomain(clusterDomainUpdate)
			} else {
				action = "deactivate"
				err = c.Driver.DeactivateClusterDomain(clusterDomainUpdate)
			}
			if err != nil {
				err = fmt.Errorf("unable to %v cluster domain: %v", action, err)
				log.ClusterDomainUpdateLog(clusterDomainUpdate).Errorf(err.Error())
				clusterDomainUpdate.Status.Status = storkv1.ClusterDomainUpdateStatusFailed
				clusterDomainUpdate.Status.Reason = err.Error()
				c.Recorder.Event(
					clusterDomainUpdate,
					v1.EventTypeWarning,
					string(storkv1.ClusterDomainUpdateStatusFailed),
					err.Error(),
				)

			} else {
				clusterDomainUpdate.Status.Status = storkv1.ClusterDomainUpdateStatusSuccessful
			}

			err = sdk.Update(clusterDomainUpdate)
			if err != nil {
				log.ClusterDomainUpdateLog(clusterDomainUpdate).Errorf("Error updating ClusterDomainUpdate: %v", err)
				return err
			}
			return nil
		case storkv1.ClusterDomainUpdateStatusFailed, storkv1.ClusterDomainUpdateStatusSuccessful:
			return nil
		default:
			log.ClusterDomainUpdateLog(clusterDomainUpdate).Errorf("Invalid status for cluster domain update: %v", clusterDomainUpdate.Status.Status)
		}

	}
	return nil
}

// createCRD creates the CRD for ClusterDomainsStatus object
func (c *ClusterDomainUpdateController) createCRD() error {
	resource := k8s.CustomResource{
		Name:       storkv1.ClusterDomainUpdateResourceName,
		Plural:     storkv1.ClusterDomainUpdatePlural,
		Group:      stork.GroupName,
		Version:    storkv1.SchemeGroupVersion.Version,
		Scope:      apiextensionsv1beta1.ClusterScoped,
		Kind:       reflect.TypeOf(storkv1.ClusterDomainUpdate{}).Name(),
		ShortNames: []string{storkv1.ClusterDomainUpdateShortName},
	}
	err := k8s.Instance().CreateCRD(resource)
	if err != nil && !errors.IsAlreadyExists(err) {
		return err
	}

	return k8s.Instance().ValidateCRD(resource, validateCRDTimeout, validateCRDInterval)
}
