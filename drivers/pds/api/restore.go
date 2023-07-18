package api

import (
	"fmt"
	status "net/http"

	pds "github.com/portworx/pds-api-go-client/pds/v1alpha1"
	"github.com/portworx/torpedo/drivers/pds/pdsutils"
)

// Restore struct
type Restore struct {
	apiClient *pds.APIClient
}

// GetRestore return pds restore model.
func (restore *Restore) GetRestore(restoreID string) (*pds.ModelsRestore, error) {
	restoreClient := restore.apiClient.RestoresApi
	ctx, err := pdsutils.GetContext()
	if err != nil {
		return nil, fmt.Errorf("Error in getting context for api call: %v\n", err)
	}
	restoreModel, res, err := restoreClient.ApiRestoresIdGet(ctx, restoreID).Execute()
	if err != nil && res.StatusCode != status.StatusOK {
		return nil, fmt.Errorf("Error when calling `ApiRestoresIdGet`: %v\n.Full HTTP response: %v", err, res)
	}
	return restoreModel, err
}

// RestoreToNewDeployment restore as new data service and return the newly create restore model.
func (restore *Restore) RestoreToNewDeployment(backupJobId, name, deploymentTargetId, namespaceId string) (*pds.ModelsRestore, error) {
	restoreClient := restore.apiClient.RestoresApi
	createRequest := pds.RequestsCreateRestoreRequest{
		DeploymentTargetId: &deploymentTargetId,
		Name:               &name,
		NamespaceId:        &namespaceId,
	}
	ctx, err := pdsutils.GetContext()
	if err != nil {
		return nil, fmt.Errorf("Error in getting context for api call: %v\n", err)
	}
	restoreModel, res, err := restoreClient.ApiBackupJobsIdRestorePost(ctx, backupJobId).Body(createRequest).Execute()
	if err != nil && res.StatusCode != status.StatusOK {
		return nil, fmt.Errorf("Error when calling `ApiDeploymentsIdRestoresPost`: %v\n.Full HTTP response: %v", err, res)
	}
	return restoreModel, err
}

// RetryRestoreToNewDeployment retry restore as new data service and return the newly create restore model.
func (restore *Restore) RetryRestoreToNewDeployment(backupJobId, name, deploymentTargetId, namespaceId string) (*pds.ModelsRestore, error) {
	restoreClient := restore.apiClient.RestoresApi
	createRequest := pds.RequestsCreateRestoreRequest{
		DeploymentTargetId: &deploymentTargetId,
		Name:               &name,
		NamespaceId:        &namespaceId,
	}
	ctx, err := pdsutils.GetContext()
	if err != nil {
		return nil, fmt.Errorf("Error in getting context for api call: %v\n", err)
	}
	restoreModel, res, err := restoreClient.ApiRestoresIdRetryPost(ctx, backupJobId).Body(createRequest).Execute()
	if err != nil && res.StatusCode != status.StatusOK {
		return nil, fmt.Errorf("Error when calling `ApiDeploymentsIdRestoresPost`: %v\n.Full HTTP response: %v", err, res)
	}
	return restoreModel, err
}

// GetRestorabilityMatrix return the restore matrix
func (restore *Restore) GetRestorabilityMatrix() (*map[string][]pds.ServiceRestoreCompatibilityCondition, error) {
	restoreClient := restore.apiClient.RestoresApi
	ctx, err := pdsutils.GetContext()
	if err != nil {
		return nil, fmt.Errorf("Error in getting context for api call: %v\n", err)
	}
	restoreMatrix, res, err := restoreClient.ApiRestoresRestorabilityMatrixGet(ctx).Execute()
	if err != nil {
		return nil, fmt.Errorf("Error when calling `ApiRestoresIdDelete`: %v\n.Full HTTP response: %v", err, res)
	}
	return restoreMatrix, nil
}
