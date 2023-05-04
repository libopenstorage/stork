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
func (backupJob *BackupJob) ListBackupJobs(backupID string) ([]pds.ControllersBackupJobStatus, error) {
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
