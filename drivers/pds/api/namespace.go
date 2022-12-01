// Package api comprises of all the components and associated CRUD functionality
package api

import (
	status "net/http"

	pds "github.com/portworx/pds-api-go-client/pds/v1alpha1"
	"github.com/portworx/torpedo/drivers/pds/pdsutils"
	"github.com/portworx/torpedo/pkg/log"
)

// Namespace struct
type Namespace struct {
	apiClient *pds.APIClient
}

// ListNamespaces return namespaces models in a target cluster.
func (ns *Namespace) ListNamespaces(targetID string) ([]pds.ModelsNamespace, error) {
	nsClient := ns.apiClient.NamespacesApi
	ctx, err := pdsutils.GetContext()
	if err != nil {
		log.Errorf("Error in getting context for api call: %v\n", err)
		return nil, err
	}
	nsModels, res, err := nsClient.ApiDeploymentTargetsIdNamespacesGet(ctx, targetID).Execute()

	if res.StatusCode != status.StatusOK {
		log.Errorf("Error when calling `ApiDeploymentTargetsIdNamespacesGet``: %v\n", err)
		log.Errorf("Full HTTP response: %v\n", res)
	}
	return nsModels.GetData(), err
}

// CreateNamespace return newly created namespaces model in the target cluster.
func (ns *Namespace) CreateNamespace(targetID string, name string) (*pds.ModelsNamespace, error) {
	nsClient := ns.apiClient.NamespacesApi

	createRequest := pds.ControllersCreateNamespace{Name: &name}
	ctx, err := pdsutils.GetContext()
	if err != nil {
		log.Errorf("Error in getting context for api call: %v\n", err)
		return nil, err
	}
	nsModel, res, err := nsClient.ApiDeploymentTargetsIdNamespacesPost(ctx, targetID).Body(createRequest).Execute()

	if err != nil && res.StatusCode != status.StatusOK {
		log.Errorf("Error when calling `ApiDeploymentTargetsIdNamespacesPost``: %v\n", err)
		log.Errorf("Full HTTP response: %v\n", res)
		return nil, err
	}
	return nsModel, nil
}

// GetNamespace return namespaces model in the target cluster.
func (ns *Namespace) GetNamespace(namespaceID string) (*pds.ModelsNamespace, error) {
	nsClient := ns.apiClient.NamespacesApi
	ctx, err := pdsutils.GetContext()
	if err != nil {
		log.Errorf("Error in getting context for api call: %v\n", err)
		return nil, err
	}
	nsModel, res, err := nsClient.ApiNamespacesIdGet(ctx, namespaceID).Execute()

	if err != nil && res.StatusCode != status.StatusOK {
		log.Errorf("Error when calling `ApiNamespacesIdGet``: %v\n", err)
		log.Errorf("Full HTTP response: %v\n", res)
		return nil, err
	}
	return nsModel, nil
}

// DeleteNamespace delete the namespace and return status.
func (ns *Namespace) DeleteNamespace(namespaceID string) (*status.Response, error) {
	nsClient := ns.apiClient.NamespacesApi
	ctx, err := pdsutils.GetContext()
	if err != nil {
		log.Errorf("Error in getting context for api call: %v\n", err)
		return nil, err
	}
	res, err := nsClient.ApiNamespacesIdDelete(ctx, namespaceID).Execute()
	if err != nil && res.StatusCode != status.StatusOK {
		log.Errorf("Error when calling `ApiNamespacesIdDelete``: %v\n", err)
		log.Errorf("Full HTTP response: %v\n", res)
		return nil, err
	}
	return res, nil
}
