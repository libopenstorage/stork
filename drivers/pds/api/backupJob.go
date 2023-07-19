package api

import (
	"fmt"
	status "net/http"

	pds "github.com/portworx/pds-api-go-client/pds/v1alpha1"
	"github.com/portworx/torpedo/drivers/pds/pdsutils"
)

// BackupJob struct
type BackupJob struct {
	apiClient *pds.APIClient
}

// ListBackupJobs return back up jobs models.
func (backupJob *BackupJob) ListBackupJobs(backupID string) ([]pds.ModelsBackupJobStatusResponse, error) {
	backupJobClient := backupJob.apiClient.BackupJobsApi
	ctx, err := pdsutils.GetContext()
	if err != nil {
		return nil, fmt.Errorf("Error in getting context for api call: %v\n", err)
	}
	backupJobModels, res, err := backupJobClient.ApiBackupsIdJobsGet(ctx, backupID).Execute()

	if res.StatusCode != status.StatusOK {
		return nil, fmt.Errorf("Error when calling `ApiBackupsIdJobsGet`: %v\n.Full HTTP response: %v", err, res)
	}
	return backupJobModels.GetData(), err
}

// ListBackupJobsBelongToDeployment return back up jobs models associated to deployment.
func (backupJob *BackupJob) ListBackupJobsBelongToDeployment(projectID, deploymentID string) ([]pds.ModelsBackupJob, error) {
	backupJobClient := backupJob.apiClient.BackupJobsApi
	ctx, err := pdsutils.GetContext()
	if err != nil {
		return nil, fmt.Errorf("Error in getting context for api call: %v\n", err)
	}
	backupJobModel, res, err := backupJobClient.ApiProjectsIdBackupJobsGet(ctx, projectID).DeploymentId(deploymentID).Execute()
	if err != nil && res.StatusCode != status.StatusOK {
		return nil, fmt.Errorf("Error when calling `ApiBackupsIdJobsGet`: %v\n.Full HTTP response: %v", err, res)
	}
	return backupJobModel.GetData(), err
}

// GetBackupJob return backup job model.
func (backupJob *BackupJob) GetBackupJob(backupJobID string) (*pds.ModelsBackupJob, error) {
	backupJobClient := backupJob.apiClient.BackupJobsApi
	ctx, err := pdsutils.GetContext()
	if err != nil {
		return nil, fmt.Errorf("Error in getting context for api call: %v\n", err)
	}
	backupJobModel, res, err := backupJobClient.ApiBackupJobsIdGet(ctx, backupJobID).Execute()

	if res.StatusCode != status.StatusOK {
		return nil, fmt.Errorf("Error when calling `ApiBackupJobsIdGet`: %v\n.Full HTTP response: %v", err, res)
	}
	return backupJobModel, err
}

// DeleteBackupJob delete deployment and return status.
func (backupJob *BackupJob) DeleteBackupJob(backupJobID string) (*status.Response, error) {
	backupJobClient := backupJob.apiClient.BackupJobsApi
	ctx, err := pdsutils.GetContext()
	if err != nil {
		return nil, fmt.Errorf("Error in getting context for api call: %v\n", err)
	}
	res, err := backupJobClient.ApiBackupJobsIdDelete(ctx, backupJobID).Execute()
	if err != nil {
		return nil, fmt.Errorf("Error when calling `ApiDeploymentsIdDelete`: %v\n.Full HTTP response: %v", err, res)
	}
	return res, err
}
