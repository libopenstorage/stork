package api

import (
	"fmt"
	status "net/http"

	pds "github.com/portworx/pds-api-go-client/pds/v1alpha1"
	"github.com/portworx/torpedo/drivers/pds/pdsutils"
)

// Backup struct
type Backup struct {
	apiClient *pds.APIClient
}

// ListBackup return pds backup models.
func (backup *Backup) ListBackup(deploymentID string) ([]pds.ModelsBackup, error) {
	backupClient := backup.apiClient.BackupsApi
	ctx, err := pdsutils.GetContext()
	if err != nil {
		return nil, fmt.Errorf("Error in getting context for api call: %v\n", err)
	}
	backupModels, res, err := backupClient.ApiDeploymentsIdBackupsGet(ctx, deploymentID).Execute()
	if err != nil && res.StatusCode != status.StatusOK {
		return nil, fmt.Errorf("Error when calling `ApiDeploymentsIdBackupsGet`: %v\n.Full HTTP response: %v", err, res)
	}
	return backupModels.GetData(), err
}

// ListBackupsBelongToTarget return pds backup models specific to a backup target.
func (backup *Backup) ListBackupsBelongToTarget(backupTargetID string) ([]pds.ModelsBackup, error) {
	backupClient := backup.apiClient.BackupsApi
	ctx, err := pdsutils.GetContext()
	if err != nil {
		return nil, fmt.Errorf("Error in getting context for api call: %v\n", err)
	}
	backupModels, res, err := backupClient.ApiBackupTargetsIdBackupsGet(ctx, backupTargetID).Execute()
	if err != nil && res.StatusCode != status.StatusOK {
		return nil, fmt.Errorf("Error when calling `ApiBackupTargetsIdBackupsGet`: %v\n.Full HTTP response: %v", err, res)
	}
	return backupModels.GetData(), err
}

// GetBackup return pds backup model.
func (backup *Backup) GetBackup(backupID string) (*pds.ModelsBackup, error) {
	backupClient := backup.apiClient.BackupsApi
	ctx, err := pdsutils.GetContext()
	if err != nil {
		return nil, fmt.Errorf("Error in getting context for api call: %v\n", err)
	}
	backupModel, res, err := backupClient.ApiBackupsIdGet(ctx, backupID).Execute()
	if err != nil && res.StatusCode != status.StatusOK {
		return nil, fmt.Errorf("Error when calling `ApiBackupsIdGet`: %v\n.Full HTTP response: %v", err, res)
	}
	return backupModel, err
}

// CreateBackup create adhoc/schedule backup and return the newly create backup model.
func (backup *Backup) CreateBackup(deploymentID string, backupTargetID string, isAdhoc bool) (*pds.ModelsBackup, error) {
	backupClient := backup.apiClient.BackupsApi
	backupType := "adhoc"
	if !isAdhoc {
		backupType = "scheduled"
	}
	backupLevel := "snapshot"
	createRequest := pds.ControllersCreateDeploymentBackup{
		BackupLevel:    &backupLevel,
		BackupTargetId: &backupTargetID,
		BackupType:     &backupType,
	}
	ctx, err := pdsutils.GetContext()
	if err != nil {
		return nil, fmt.Errorf("Error in getting context for api call: %v\n", err)
	}
	backupModel, res, err := backupClient.ApiDeploymentsIdBackupsPost(ctx, deploymentID).Body(createRequest).Execute()
	if err != nil && res.StatusCode != status.StatusOK {
		return nil, fmt.Errorf("Error when calling `ApiDeploymentsIdBackupsPost`: %v\n.Full HTTP response: %v", err, res)
	}
	return backupModel, err
}

// UpdateBackup return updated backup model.
func (backup *Backup) UpdateBackup(backupID string, jobHistoryLimit int32) (*pds.ModelsBackup, error) {
	backupClient := backup.apiClient.BackupsApi
	updateRequest := pds.ControllersUpdateBackupRequest{
		JobHistoryLimit: &jobHistoryLimit,
	}
	ctx, err := pdsutils.GetContext()
	if err != nil {
		return nil, fmt.Errorf("Error in getting context for api call: %v\n", err)
	}
	backupTargetModel, res, err := backupClient.ApiBackupsIdPut(ctx, backupID).Body(updateRequest).Execute()
	if err != nil && res.StatusCode != status.StatusOK {
		return nil, fmt.Errorf("Error when calling `ApiBackupsIdPut`: %v\n.Full HTTP response: %v", err, res)
	}
	return backupTargetModel, err
}

// DeleteBackupJobs delete the backup job and return the status.
func (backup *Backup) DeleteBackupJobs(backupID string, jobName string) (*status.Response, error) {
	backupClient := backup.apiClient.BackupsApi
	ctx, err := pdsutils.GetContext()
	if err != nil {
		return nil, fmt.Errorf("Error in getting context for api call: %v\n", err)
	}
	res, err := backupClient.ApiBackupsIdJobsNameDelete(ctx, backupID, jobName).Execute()
	if err != nil && res.StatusCode != status.StatusOK {
		return nil, fmt.Errorf("Error when calling `ApiBackupsIdJobsNameDelete`: %v\n.Full HTTP response: %v", err, res)
	}
	return res, nil
}

// DeleteBackup delete the backup and return the status.
func (backup *Backup) DeleteBackup(backupID string) (*status.Response, error) {
	backupClient := backup.apiClient.BackupsApi
	ctx, err := pdsutils.GetContext()
	if err != nil {
		return nil, fmt.Errorf("Error in getting context for api call: %v\n", err)
	}
	res, err := backupClient.ApiBackupsIdDelete(ctx, backupID).Execute()
	if err != nil && res.StatusCode != status.StatusOK {
		return nil, fmt.Errorf("Error when calling `ApiBackupsIdDelete`: %v\n.Full HTTP response: %v", err, res)
	}
	return res, nil
}
