// Package api comprises of all the components and associated CRUD functionality
package api

import (
	"fmt"
	status "net/http"

	pds "github.com/portworx/pds-api-go-client/pds/v1alpha1"
	"github.com/portworx/torpedo/drivers/pds/pdsutils"
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
		return nil, fmt.Errorf("Error in getting context for api call: %v\n", err)
	}
	nsModels, res, err := nsClient.ApiDeploymentTargetsIdNamespacesGet(ctx, targetID).Execute()
	if err != nil && res.StatusCode != status.StatusOK {
		return nil, fmt.Errorf("Error when calling `ApiDeploymentTargetsIdNamespacesGet`: %v\n.Full HTTP response: %v", err, res)
	}
	return nsModels.GetData(), err
}

// CreateNamespace return newly created namespaces model in the target cluster.
func (ns *Namespace) CreateNamespace(targetID string, name string) (*pds.ModelsNamespace, error) {
	nsClient := ns.apiClient.NamespacesApi
	createRequest := pds.ControllersCreateNamespace{Name: &name}
	ctx, err := pdsutils.GetContext()
	if err != nil {
		return nil, fmt.Errorf("Error in getting context for api call: %v\n", err)
	}
	nsModel, res, err := nsClient.ApiDeploymentTargetsIdNamespacesPost(ctx, targetID).Body(createRequest).Execute()
	if err != nil && res.StatusCode != status.StatusOK {
		return nil, fmt.Errorf("Error when calling `ApiDeploymentTargetsIdNamespacesPost`: %v\n.Full HTTP response: %v", err, res)
	}
	return nsModel, nil
}

// GetNamespace return namespaces model in the target cluster.
func (ns *Namespace) GetNamespace(namespaceID string) (*pds.ModelsNamespace, error) {
	nsClient := ns.apiClient.NamespacesApi
	ctx, err := pdsutils.GetContext()
	if err != nil {
		return nil, fmt.Errorf("Error in getting context for api call: %v\n", err)
	}
	nsModel, res, err := nsClient.ApiNamespacesIdGet(ctx, namespaceID).Execute()
	if err != nil && res.StatusCode != status.StatusOK {
		return nil, fmt.Errorf("Error when calling `ApiNamespacesIdGet`: %v\n.Full HTTP response: %v", err, res)
	}
	return nsModel, nil
}

// DeleteNamespace delete the namespace and return status.
func (ns *Namespace) DeleteNamespace(namespaceID string) (*status.Response, error) {
	nsClient := ns.apiClient.NamespacesApi
	ctx, err := pdsutils.GetContext()
	if err != nil {
		return nil, fmt.Errorf("Error in getting context for api call: %v\n", err)
	}
	res, err := nsClient.ApiNamespacesIdDelete(ctx, namespaceID).Execute()
	if err != nil && res.StatusCode != status.StatusOK {
		return nil, fmt.Errorf("Error when calling `ApiNamespacesIdDelete`: %v\n.Full HTTP response: %v", err, res)
	}
	return res, nil
}
