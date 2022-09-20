package api

import (
	status "net/http"

	pds "github.com/portworx/pds-api-go-client/pds/v1alpha1"
	"github.com/portworx/torpedo/drivers/pds/pdsutils"
	log "github.com/sirupsen/logrus"
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
		log.Errorf("Error in getting context for api call: %v\n", err)
		return nil, err
	}
	backupJobModels, res, err := backupJobClient.ApiBackupsIdJobsGet(ctx, backupID).Execute()

	if res.StatusCode != status.StatusOK {
		log.Errorf("Error when calling `ApiBackupsIdJobsGet``: %v\n", err)
		log.Errorf("Full HTTP response: %v\n", res)
	}
	return backupJobModels.GetData(), err
}

// GetBackupJob return backup job model.
func (backupJob *BackupJob) GetBackupJob(backupJobID string) (*pds.ModelsBackupJob, error) {
	backupJobClient := backupJob.apiClient.BackupJobsApi
	ctx, err := pdsutils.GetContext()
	if err != nil {
		log.Errorf("Error in getting context for api call: %v\n", err)
		return nil, err
	}
	backupJobModel, res, err := backupJobClient.ApiBackupJobsIdGet(ctx, backupJobID).Execute()

	if res.StatusCode != status.StatusOK {
		log.Errorf("Error when calling `ApiBackupJobsIdGet``: %v\n", err)
		log.Errorf("Full HTTP response: %v\n", res)
	}
	return backupJobModel, err
}
