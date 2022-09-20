// Package api comprises of all the components and associated CRUD functionality
package api

import (
	status "net/http"

	pds "github.com/portworx/pds-api-go-client/pds/v1alpha1"
	"github.com/portworx/torpedo/drivers/pds/pdsutils"
	log "github.com/sirupsen/logrus"
)

// DataServiceDeployment struct
type DataServiceDeployment struct {
	apiClient *pds.APIClient
}

// ListDeployments return deployments models for a given project.
func (ds *DataServiceDeployment) ListDeployments(projectID string) ([]pds.ModelsDeployment, error) {
	dsClient := ds.apiClient.DeploymentsApi
	ctx, err := pdsutils.GetContext()
	if err != nil {
		log.Errorf("Error in getting context for api call: %v\n", err)
		return nil, err
	}

	dsModels, res, err := dsClient.ApiProjectsIdDeploymentsGet(ctx, projectID).Execute()

	if res.StatusCode != status.StatusOK {
		log.Errorf("Error when calling `ApiProjectsIdDeploymentsGet``: %v\n", err)
		log.Errorf("Full HTTP response: %v\n", res)
	}
	return dsModels.GetData(), err
}

// CreateDeployment return newly created deployment model.
func (ds *DataServiceDeployment) CreateDeployment(projectID string, deploymentTargetID string, dnsZone string, name string, namespaceID string, appConfigID string, imageID string, nodeCount int32, serviceType string, resourceTemplateID string, storageTemplateID string) (*pds.ModelsDeployment, error) {
	dsClient := ds.apiClient.DeploymentsApi
	createRequest := pds.ControllersCreateProjectDeployment{
		ApplicationConfigurationTemplateId: &appConfigID,
		DeploymentTargetId:                 &deploymentTargetID,
		DnsZone:                            &dnsZone,
		ImageId:                            &imageID,
		Name:                               &name,
		NamespaceId:                        &namespaceID,
		NodeCount:                          &nodeCount,
		ResourceSettingsTemplateId:         &resourceTemplateID,
		ServiceType:                        &serviceType,
		StorageOptionsTemplateId:           &storageTemplateID,
	}
	ctx, err := pdsutils.GetContext()
	if err != nil {
		log.Errorf("Error in getting context for api call: %v\n", err)
		return nil, err
	}
	dsModel, res, err := dsClient.ApiProjectsIdDeploymentsPost(ctx, projectID).Body(createRequest).Execute()

	if res.StatusCode != status.StatusOK {
		log.Errorf("Error when calling `ApiProjectsIdDeploymentsPost``: %v\n", err)
		log.Errorf("Full HTTP response: %v\n", res)
	}
	return dsModel, err
}

// CreateDeploymentWithScehduleBackup return newly created deployment model with schedule backup enabled.
func (ds *DataServiceDeployment) CreateDeploymentWithScehduleBackup(projectID string, deploymentTargetID string, dnsZone string, name string, namespaceID string, appConfigID string, imageID string, nodeCount int32, serviceType string, resourceTemplateID string, storageTemplateID string, backupPolicyID string, backupTargetID string) (*pds.ModelsDeployment, error) {
	dsClient := ds.apiClient.DeploymentsApi
	scheduledBackup := pds.ControllersCreateDeploymentScheduledBackup{
		BackupPolicyId: &backupPolicyID,
		BackupTargetId: &backupTargetID,
	}
	createRequest := pds.ControllersCreateProjectDeployment{
		ApplicationConfigurationTemplateId: &appConfigID,
		DeploymentTargetId:                 &deploymentTargetID,
		DnsZone:                            &dnsZone,
		ImageId:                            &imageID,
		Name:                               &name,
		NamespaceId:                        &namespaceID,
		NodeCount:                          &nodeCount,
		ResourceSettingsTemplateId:         &resourceTemplateID,
		ScheduledBackup:                    &scheduledBackup,
		ServiceType:                        &serviceType,
		StorageOptionsTemplateId:           &storageTemplateID,
	}
	ctx, err := pdsutils.GetContext()
	if err != nil {
		log.Errorf("Error in getting context for api call: %v\n", err)
		return nil, err
	}
	dsModel, res, err := dsClient.ApiProjectsIdDeploymentsPost(ctx, projectID).Body(createRequest).Execute()

	if res.StatusCode != status.StatusOK {
		log.Errorf("Error when calling `ApiProjectsIdDeploymentsPost``: %v\n", err)
		log.Errorf("Full HTTP response: %v\n", res)
	}
	return dsModel, err
}

// GetDeployment return deployment model.
func (ds *DataServiceDeployment) GetDeployment(deploymentID string) (*pds.ModelsDeployment, error) {
	dsClient := ds.apiClient.DeploymentsApi
	ctx, err := pdsutils.GetContext()
	if err != nil {
		log.Errorf("Error in getting context for api call: %v\n", err)
		return nil, err
	}
	dsModel, res, err := dsClient.ApiDeploymentsIdGet(ctx, deploymentID).Execute()
	if res.StatusCode != status.StatusOK {
		log.Errorf("Error when calling `ApiDeploymentsIdGet``: %v\n", err)
		log.Errorf("Full HTTP response: %v\n", res)
	}
	return dsModel, err
}

// GetDeploymentSatus return deployment status.
func (ds *DataServiceDeployment) GetDeploymentSatus(deploymentID string) (*pds.ControllersStatusResponse, *status.Response, error) {
	dsClient := ds.apiClient.DeploymentsApi
	ctx, err := pdsutils.GetContext()
	if err != nil {
		log.Errorf("Error in getting context for api call: %v\n", err)
		return nil, nil, err
	}
	dsModel, res, err := dsClient.ApiDeploymentsIdStatusGet(ctx, deploymentID).Execute()

	if res.StatusCode != status.StatusOK {
		log.Errorf("Error when calling `ApiDeploymentsIdStatusGet``: %v\n", err)
		log.Errorf("Full HTTP response: %v\n", res)
	}
	return dsModel, res, err
}

// GetDeploymentEvents return events on the given deployment.
func (ds *DataServiceDeployment) GetDeploymentEvents(deploymentID string) (*pds.ControllersEventsResponse, error) {
	dsClient := ds.apiClient.DeploymentsApi
	ctx, err := pdsutils.GetContext()
	if err != nil {
		log.Errorf("Error in getting context for api call: %v\n", err)
		return nil, err
	}
	dsModel, res, err := dsClient.ApiDeploymentsIdEventsGet(ctx, deploymentID).Execute()

	if res.StatusCode != status.StatusOK {
		log.Errorf("Error when calling `ApiDeploymentsIdEventsGet``: %v\n", err)
		log.Errorf("Full HTTP response: %v\n", res)
	}
	return dsModel, err
}

// GetDeploymentCredentials return deployment credentials.
func (ds *DataServiceDeployment) GetDeploymentCredentials(deploymentID string) (*pds.DeploymentsCredentials, error) {
	dsClient := ds.apiClient.DeploymentsApi
	ctx, err := pdsutils.GetContext()
	if err != nil {
		log.Errorf("Error in getting context for api call: %v\n", err)
		return nil, err
	}
	dsModel, res, err := dsClient.ApiDeploymentsIdCredentialsGet(ctx, deploymentID).Execute()

	if res.StatusCode != status.StatusOK {
		log.Errorf("Error when calling `ApiDeploymentsIdCredentialsGet``: %v\n", err)
		log.Errorf("Full HTTP response: %v\n", res)
	}
	return dsModel, err
}

// GetConnectionDetails return connection details for the given deployment.
func (ds *DataServiceDeployment) GetConnectionDetails(deploymentID string) (pds.DeploymentsConnectionDetails, error) {
	dsClient := ds.apiClient.DeploymentsApi
	ctx, err := pdsutils.GetContext()
	if err != nil {
		log.Errorf("Error in getting context for api call: %v\n", err)
		return pds.DeploymentsConnectionDetails{}, err
	}
	dsModel, res, err := dsClient.ApiDeploymentsIdConnectionInfoGet(ctx, deploymentID).Execute()

	if res.StatusCode != status.StatusOK {
		log.Errorf("Error when calling `ApiDeploymentsIdConnectionInfoGet``: %v\n", err)
		log.Errorf("Full HTTP response: %v\n", res)
	}
	return dsModel.GetConnectionDetails(), err
}

// DeleteDeployment delete deployment and return status.
func (ds *DataServiceDeployment) DeleteDeployment(deploymentID string) (*status.Response, error) {
	dsClient := ds.apiClient.DeploymentsApi
	ctx, err := pdsutils.GetContext()
	if err != nil {
		log.Errorf("Error in getting context for api call: %v\n", err)
		return nil, err
	}
	res, err := dsClient.ApiDeploymentsIdDelete(ctx, deploymentID).Execute()
	if res.StatusCode != status.StatusAccepted {
		log.Errorf("Error when calling `ApiDeploymentsIdDelete``: %v\n", err)
		log.Errorf("Full HTTP response: %v\n", res)
	}
	return res, err
}
