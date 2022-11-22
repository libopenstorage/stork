package api

import (
	status "net/http"

	pds "github.com/portworx/pds-api-go-client/pds/v1alpha1"
	"github.com/portworx/torpedo/drivers/pds/pdsutils"
	log "github.com/sirupsen/logrus"
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
		log.Errorf("Error in getting context for api call: %v\n", err)
		return nil, err
	}
	backupTargetModels, res, err := backupTargetClient.ApiTenantsIdBackupTargetsGet(ctx, tenantID).Execute()

	if res.StatusCode != status.StatusOK {
		log.Errorf("Error when calling `ApiTenantsIdBackupTargetsGet``: %v\n", err)
		log.Errorf("Full HTTP response: %v\n", res)
	}
	return backupTargetModels.GetData(), err
}

// LisBackupsStateBelongToBackupTarget return backup targets state models w.r.t to particular target.
func (backupTarget *BackupTarget) LisBackupsStateBelongToBackupTarget(backuptargetID string) ([]pds.ModelsBackupTargetState, error) {
	backupTargetClient := backupTarget.apiClient.BackupTargetsApi
	ctx, err := pdsutils.GetContext()
	if err != nil {
		log.Errorf("Error in getting context for api call: %v\n", err)
		return nil, err
	}
	backupTargetModel, res, err := backupTargetClient.ApiBackupTargetsIdStatesGet(ctx, backuptargetID).Execute()

	if res.StatusCode != status.StatusOK {
		log.Errorf("Error when calling `ApiBackupTargetsIdStatesGet``: %v\n", err)
		log.Errorf("Full HTTP response: %v\n", res)
	}
	return backupTargetModel.GetData(), err
}

// GetBackupTarget return backup target model.
func (backupTarget *BackupTarget) GetBackupTarget(backupTargetID string) (*pds.ModelsBackupTarget, error) {
	backupTargetClient := backupTarget.apiClient.BackupTargetsApi
	ctx, err := pdsutils.GetContext()
	if err != nil {
		log.Errorf("Error in getting context for api call: %v\n", err)
		return nil, err
	}
	backupTargetModel, res, err := backupTargetClient.ApiBackupTargetsIdGet(ctx, backupTargetID).Execute()

	if res.StatusCode != status.StatusOK {
		log.Errorf("Error when calling `ApiBackupTargetsIdGet``: %v\n", err)
		log.Errorf("Full HTTP response: %v\n", res)
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
		log.Errorf("Error in getting context for api call: %v\n", err)
		return nil, err
	}
	backupTargetModel, res, err := backupTargetClient.ApiTenantsIdBackupTargetsPost(ctx, tenantID).Body(createRequest).Execute()
	if res.StatusCode != status.StatusOK {
		log.Errorf("Error when calling `ApiTenantsIdBackupTargetsPost``: %v\n", err)
		log.Errorf("Full HTTP response: %v\n", res)
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
		log.Errorf("Error in getting context for api call: %v\n", err)
		return nil, err
	}
	backupTargetModel, res, err := backupTargetClient.ApiBackupTargetsIdPut(ctx, backupTaregetID).Body(updateRequest).Execute()
	if res.StatusCode != status.StatusOK {
		log.Errorf("Error when calling `ApiBackupTargetsIdPut``: %v\n", err)
		log.Errorf("Full HTTP response: %v\n", res)
	}
	return backupTargetModel, err

}

// SyncToBackupLocation returned synced backup target model.
func (backupTarget *BackupTarget) SyncToBackupLocation(backupTaregetID string, name string) (*status.Response, error) {
	backupTargetClient := backupTarget.apiClient.BackupTargetsApi
	ctx, err := pdsutils.GetContext()
	if err != nil {
		log.Errorf("Error in getting context for api call: %v\n", err)
		return nil, err
	}
	res, err := backupTargetClient.ApiBackupTargetsIdRetryPost(ctx, backupTaregetID).Execute()
	if res.StatusCode != status.StatusOK {
		log.Errorf("Error when calling `ApiBackupTargetsIdPut``: %v\n", err)
		log.Errorf("Full HTTP response: %v\n", res)
	}
	return res, err
}

// DeleteBackupTarget delete backup target and return status.
func (backupTarget *BackupTarget) DeleteBackupTarget(backupTaregetID string) (*status.Response, error) {
	backupTargetClient := backupTarget.apiClient.BackupTargetsApi
	ctx, err := pdsutils.GetContext()
	if err != nil {
		log.Errorf("Error in getting context for api call: %v\n", err)
		return nil, err
	}
	res, err := backupTargetClient.ApiBackupTargetsIdDelete(ctx, backupTaregetID).Execute()
	if err != nil && res.StatusCode != status.StatusOK {
		log.Errorf("Error when calling `ApiBackupTargetsIdDelete``: %v\n", err)
		log.Errorf("Full HTTP response: %v\n", res)
		return nil, err
	}
	return res, nil
}
