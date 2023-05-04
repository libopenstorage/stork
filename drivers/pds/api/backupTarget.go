package api

import (
	"fmt"
	status "net/http"

	pds "github.com/portworx/pds-api-go-client/pds/v1alpha1"
	"github.com/portworx/torpedo/drivers/pds/pdsutils"
)

// BackupTarget struct
type BackupTarget struct {
	apiClient *pds.APIClient
}

// ListBackupTarget return backup targets models.
func (backupTarget *BackupTarget) ListBackupTarget(tenantID string) ([]pds.ModelsBackupTarget, error) {
	backupTargetClient := backupTarget.apiClient.BackupTargetsApi
	ctx, err := pdsutils.GetContext()
	if err != nil {
		return nil, fmt.Errorf("Error in getting context for api call: %v\n", err)
	}
	backupTargetModels, res, err := backupTargetClient.ApiTenantsIdBackupTargetsGet(ctx, tenantID).Execute()
	if res.StatusCode != status.StatusOK {
		return nil, fmt.Errorf("Error when calling `ApiTenantsIdBackupTargetsGet`: %v\n.Full HTTP response: %v", err, res)
	}
	return backupTargetModels.GetData(), err
}

// LisBackupsStateBelongToBackupTarget return backup targets state models w.r.t to particular target.
func (backupTarget *BackupTarget) LisBackupsStateBelongToBackupTarget(backuptargetID string) ([]pds.ModelsBackupTargetState, error) {
	backupTargetClient := backupTarget.apiClient.BackupTargetsApi
	ctx, err := pdsutils.GetContext()
	if err != nil {
		return nil, fmt.Errorf("Error in getting context for api call: %v\n", err)
	}
	backupTargetModel, res, err := backupTargetClient.ApiBackupTargetsIdStatesGet(ctx, backuptargetID).Execute()
	if res.StatusCode != status.StatusOK {
		return nil, fmt.Errorf("Error when calling `ApiBackupTargetsIdStatesGet`: %v\n.Full HTTP response: %v", err, res)
	}
	return backupTargetModel.GetData(), err
}

// GetBackupTarget return backup target model.
func (backupTarget *BackupTarget) GetBackupTarget(backupTargetID string) (*pds.ModelsBackupTarget, error) {
	backupTargetClient := backupTarget.apiClient.BackupTargetsApi
	ctx, err := pdsutils.GetContext()
	if err != nil {
		return nil, fmt.Errorf("Error in getting context for api call: %v\n", err)
	}
	backupTargetModel, res, err := backupTargetClient.ApiBackupTargetsIdGet(ctx, backupTargetID).Execute()
	if res.StatusCode != status.StatusOK {
		return nil, fmt.Errorf("Error when called `ApiBackupTargetsIdGet`, Full HTTP response: %v\n", res)
	}
	return backupTargetModel, err
}

// CreateBackupTarget return newly created backup target model.
func (backupTarget *BackupTarget) CreateBackupTarget(tenantID string, name string, backupCredentialsID string, bucket string, region string, backupType string) (*pds.ModelsBackupTarget, error) {
	backupTargetClient := backupTarget.apiClient.BackupTargetsApi
	createRequest := pds.ControllersCreateTenantBackupTarget{
		BackupCredentialsId: &backupCredentialsID,
		Bucket:              &bucket,
		Name:                &name,
		Region:              &region,
		Type:                &backupType,
	}
	ctx, err := pdsutils.GetContext()
	if err != nil {
		return nil, fmt.Errorf("Error in getting context for api call: %v\n", err)
	}
	backupTargetModel, _, err := backupTargetClient.ApiTenantsIdBackupTargetsPost(ctx, tenantID).Body(createRequest).Execute()
	if err != nil {
		return nil, fmt.Errorf("error when called `ApiTenantsIdBackupTargetsPost` to create backup target - %v", err)
	}
	return backupTargetModel, err
}

// UpdateBackupTarget return updated backup target model.
func (backupTarget *BackupTarget) UpdateBackupTarget(backupTaregetID string, name string) (*pds.ModelsBackupTarget, error) {
	backupTargetClient := backupTarget.apiClient.BackupTargetsApi
	updateRequest := pds.ControllersUpdateBackupTargetRequest{
		Name: &name,
	}
	ctx, err := pdsutils.GetContext()
	if err != nil {
		return nil, fmt.Errorf("Error in getting context for api call: %v\n", err)
	}
	backupTargetModel, res, err := backupTargetClient.ApiBackupTargetsIdPut(ctx, backupTaregetID).Body(updateRequest).Execute()
	if res.StatusCode != status.StatusOK {
		return nil, fmt.Errorf("Error when calling `ApiBackupTargetsIdPut`: %v\n.Full HTTP response: %v", err, res)
	}
	return backupTargetModel, err

}

// SyncToBackupLocation returned synced backup target model.
func (backupTarget *BackupTarget) SyncToBackupLocation(backupTaregetID string) (*status.Response, error) {
	backupTargetClient := backupTarget.apiClient.BackupTargetsApi
	ctx, err := pdsutils.GetContext()
	if err != nil {
		return nil, fmt.Errorf("Error in getting context for api call: %v\n", err)
	}
	res, err := backupTargetClient.ApiBackupTargetsIdRetryPost(ctx, backupTaregetID).Execute()
	if res.StatusCode != status.StatusOK {
		return nil, fmt.Errorf("Error when calling `ApiBackupTargetsIdRetryPost`: %v\n.Full HTTP response: %v", err, res)
	}
	return res, err
}

// DeleteBackupTarget delete backup target and return status.
func (backupTarget *BackupTarget) DeleteBackupTarget(backupTargetID string) (*status.Response, error) {
	backupTargetClient := backupTarget.apiClient.BackupTargetsApi
	ctx, err := pdsutils.GetContext()
	if err != nil {
		return nil, fmt.Errorf("Error in getting context for api call: %v\n", err)
	}
	res, err := backupTargetClient.ApiBackupTargetsIdDelete(ctx, backupTargetID).Force("true").Execute()
	if err != nil {
		return nil, err
	}
	return res, nil
}
