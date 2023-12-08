// Package api comprises of all the components and associated CRUD functionality
package api

import (
	"fmt"
	status "net/http"

	pds "github.com/portworx/pds-api-go-client/pds/v1alpha1"
	"github.com/portworx/torpedo/pkg/log"
)

// DataServiceDeployment struct
type DataServiceDeployment struct {
	apiClient *pds.APIClient
}

var ServiceIdFlag = false

// ListDeployments return deployments models for a given project.
func (ds *DataServiceDeployment) ListDeployments(projectID string) ([]pds.ModelsDeployment, error) {
	dsClient := ds.apiClient.DeploymentsApi
	ctx, err := GetContext()
	if err != nil {
		return nil, fmt.Errorf("Error in getting context for api call: %v\n", err)
	}
	dsModels, res, err := dsClient.ApiProjectsIdDeploymentsGet(ctx, projectID).Execute()
	if err != nil && res.StatusCode != status.StatusOK {
		return nil, fmt.Errorf("Error when calling `ApiProjectsIdDeploymentsGet`: %v\n.Full HTTP response: %v", err, res)
	}
	return dsModels.GetData(), nil
}

// CreateDeployment return newly created deployment model.
func (ds *DataServiceDeployment) CreateDeployment(projectID string, deploymentTargetID string, dnsZone string, name string, namespaceID string, appConfigID string, imageID string, nodeCount int32, serviceType string, resourceTemplateID string, storageTemplateID string, enableTLS bool) (*pds.ModelsDeployment, error) {
	dsClient := ds.apiClient.DeploymentsApi
	var createRequest pds.RequestsCreateProjectDeploymentRequest
	if enableTLS {
		createRequest = pds.RequestsCreateProjectDeploymentRequest{
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
			TlsEnabled:                         &enableTLS,
		}
	} else {
		createRequest = pds.RequestsCreateProjectDeploymentRequest{
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
	}

	ctx, err := GetContext()
	if err != nil {
		return nil, fmt.Errorf("Error in getting context for api call: %v\n", err)
	}
	dsModel, res, err := dsClient.ApiProjectsIdDeploymentsPost(ctx, projectID).Body(createRequest).Execute()
	if err != nil && res.StatusCode != status.StatusOK {
		return nil, fmt.Errorf("Error when calling `ApiProjectsIdDeploymentsPost`: %v\n.Full HTTP response: %v", err, res)
	}
	return dsModel, err
}
func (ds *DataServiceDeployment) CreateDeploymentWithRbac(deploymentTargetID string, dnsZone string, name string, namespaceID string, appConfigID string, imageID string, nodeCount int32, serviceType string, resourceTemplateID string, storageTemplateID string, tlsEnable bool) (*pds.ModelsDeployment, error) {
	dsClient := ds.apiClient.DeploymentsApi
	createRequest := pds.RequestsCreateProjectDeploymentRequest{
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
		TlsEnabled:                         &tlsEnable,
	}
	ctx, err := GetContext()
	if err != nil {
		return nil, fmt.Errorf("Error in getting context for api call: %v\n", err)
	}

	dsModel, res, err := dsClient.ApiDeploymentsPost(ctx).Body(createRequest).Execute()
	if err != nil && res.StatusCode != status.StatusOK {
		return nil, fmt.Errorf("Error when calling `ApiDeploymentsPost`: %v\n.Full HTTP response: %v", err, res)
	}
	return dsModel, err
}

// CreateDeploymentWithScheduleBackup return newly created deployment model with schedule backup enabled.
func (ds *DataServiceDeployment) CreateDeploymentWithScheduleBackup(projectID string, deploymentTargetID string, dnsZone string, name string, namespaceID string, appConfigID string, imageID string, nodeCount int32, serviceType string, resourceTemplateID string, storageTemplateID string, backupPolicyID string, backupTargetID string) (*pds.ModelsDeployment, error) {
	dsClient := ds.apiClient.DeploymentsApi
	scheduledBackup := pds.RequestsDeploymentScheduledBackup{
		BackupPolicyId: &backupPolicyID,
		BackupTargetId: &backupTargetID,
	}
	createRequest := pds.RequestsCreateProjectDeploymentRequest{
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
	ctx, err := GetContext()
	if err != nil {
		log.Errorf("Error in getting context for api call: %v\n", err)
		return nil, fmt.Errorf("Error in getting context for api call: %v\n", err)
	}
	dsModel, res, err := dsClient.ApiProjectsIdDeploymentsPost(ctx, projectID).Body(createRequest).Execute()
	if err != nil && res.StatusCode != status.StatusOK {
		return nil, fmt.Errorf("Error when calling `ApiProjectsIdDeploymentsPost`: %v\n.Full HTTP response: %v", err, res)
	}
	return dsModel, err
}

// GetDeployment return deployment model.
func (ds *DataServiceDeployment) GetDeployment(deploymentID string) (*pds.ModelsDeployment, error) {
	dsClient := ds.apiClient.DeploymentsApi
	ctx, err := GetContext()
	if err != nil {
		return nil, fmt.Errorf("Error in getting context for api call: %v\n", err)
	}
	dsModel, res, err := dsClient.ApiDeploymentsIdGet(ctx, deploymentID).Execute()
	if err != nil && res.StatusCode != status.StatusOK {
		return nil, fmt.Errorf("Error when calling `ApiDeploymentsIdGet`: %v\n.Full HTTP response: %v", err, res)
	}
	return dsModel, err
}

// GetDeploymentStatus return deployment status.
func (ds *DataServiceDeployment) GetDeploymentStatus(deploymentID string) (*pds.ServiceDeploymentStatus, *status.Response, error) {
	dsClient := ds.apiClient.DeploymentsApi
	ctx, err := GetContext()
	if err != nil {
		return nil, nil, fmt.Errorf("Error in getting context for api call: %v\n", err)
	}
	dsModel, res, err := dsClient.ApiDeploymentsIdStatusGet(ctx, deploymentID).Execute()
	if err != nil && res.StatusCode != status.StatusOK {
		return nil, nil, fmt.Errorf("Error when calling `ApiDeploymentsIdStatusGet`: %v\n.Full HTTP response: %v", err, res)
	}
	return dsModel, res, err
}

// GetDeploymentCredentials return deployment credentials.
func (ds *DataServiceDeployment) GetDeploymentCredentials(deploymentID string) (*pds.DeploymentsCredentials, error) {
	dsClient := ds.apiClient.DeploymentsApi
	ctx, err := GetContext()
	if err != nil {
		return nil, fmt.Errorf("Error in getting context for api call: %v\n", err)
	}
	dsModel, res, err := dsClient.ApiDeploymentsIdCredentialsGet(ctx, deploymentID).Execute()
	if err != nil && res.StatusCode != status.StatusOK {
		return nil, fmt.Errorf("Error when calling `ApiDeploymentsIdCredentialsGet`: %v\n.Full HTTP response: %v", err, res)
	}
	return dsModel, err
}

// UpdateDeploymentWithTls updates the deployment with TLS
func (ds *DataServiceDeployment) UpdateDeploymentWithTls(deploymentID string, appConfigID string, imageID string,
	nodeCount int32, resourceTemplateID string, enableTLS bool) (*pds.ModelsDeployment, error) {
	dsClient := ds.apiClient.DeploymentsApi
	ctx, err := GetContext()
	if err != nil {
		return nil, fmt.Errorf("Error in getting context for api call: %v\n", err)
	}
	createRequest := pds.RequestsUpdateDeploymentRequest{
		ApplicationConfigurationTemplateId: &appConfigID,
		ImageId:                            &imageID,
		NodeCount:                          &nodeCount,
		ResourceSettingsTemplateId:         &resourceTemplateID,
		TlsEnabled:                         &enableTLS,
	}
	log.InfoD("Starting to update the deployment ... ")
	dsModel, res, err := dsClient.ApiDeploymentsIdPut(ctx, deploymentID).Body(createRequest).Execute()
	if err != nil && res.StatusCode != status.StatusOK {
		return nil, fmt.Errorf("Error when calling `ApiDeploymentsIdPut`: %v\n.Full HTTP response: %v", err, res)
	}
	return dsModel, err
}

// UpdateDeployment func
func (ds *DataServiceDeployment) UpdateDeployment(deploymentID string, appConfigID string, imageID string, nodeCount int32, resourceTemplateID string,
	appConfigOverride map[string]string) (*pds.ModelsDeployment, error) {
	dsClient := ds.apiClient.DeploymentsApi
	ctx, err := GetContext()
	if err != nil {
		return nil, fmt.Errorf("Error in getting context for api call: %v\n", err)
	}
	createRequest := pds.RequestsUpdateDeploymentRequest{
		ApplicationConfigurationOverrides:  &appConfigOverride,
		ApplicationConfigurationTemplateId: &appConfigID,
		ImageId:                            &imageID,
		NodeCount:                          &nodeCount,
		ResourceSettingsTemplateId:         &resourceTemplateID,
	}
	log.InfoD("Starting to update the deployment ... ")
	dsModel, res, err := dsClient.ApiDeploymentsIdPut(ctx, deploymentID).Body(createRequest).Execute()
	if err != nil && res.StatusCode != status.StatusOK {
		return nil, fmt.Errorf("Error when calling `ApiDeploymentsIdPut`: %v\n.Full HTTP response: %v", err, res)
	}
	return dsModel, err
}

// GetConnectionDetails return connection details for the given deployment.
func (ds *DataServiceDeployment) GetConnectionDetails(deploymentID string) (pds.DeploymentsConnectionDetails, map[string]interface{}, error) {
	dsClient := ds.apiClient.DeploymentsApi
	ctx, err := GetContext()
	if err != nil {
		return pds.DeploymentsConnectionDetails{}, nil, fmt.Errorf("Error in getting context for api call: %v\n", err)
	}
	dsModel, res, err := dsClient.ApiDeploymentsIdConnectionInfoGet(ctx, deploymentID).Execute()
	if err != nil && res.StatusCode != status.StatusOK {
		return pds.DeploymentsConnectionDetails{}, nil, fmt.Errorf("Error when calling `ApiDeploymentsIdConnectionInfoGet`: %v\n.Full HTTP response: %v", err, res)
	}
	return dsModel.GetConnectionDetails(), dsModel.GetClusterDetails(), err
}

// DeleteDeployment delete deployment and return status.
func (ds *DataServiceDeployment) DeleteDeployment(deploymentID string) (*status.Response, error) {
	dsClient := ds.apiClient.DeploymentsApi
	ctx, err := GetContext()
	if err != nil {
		return nil, fmt.Errorf("Error in getting context for api call: %v\n", err)
	}
	res, err := dsClient.ApiDeploymentsIdDelete(ctx, deploymentID).Execute()
	if err != nil && res.StatusCode != status.StatusOK {
		return nil, fmt.Errorf("Error when calling `ApiDeploymentsIdDelete`: %v\n.Full HTTP response: %v", err, res)
	}
	return res, err
}
