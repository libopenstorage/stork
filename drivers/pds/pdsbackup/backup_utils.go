package pdsbackup

import (
	"fmt"
	"k8s.io/apimachinery/pkg/util/wait"
	status "net/http"
	"net/url"
	"os"
	"strings"
	"time"

	pds "github.com/portworx/pds-api-go-client/pds/v1alpha1"
	pdsapi "github.com/portworx/torpedo/drivers/pds/api"
	"github.com/portworx/torpedo/drivers/pds/pdsutils"
	"github.com/portworx/torpedo/pkg/log"
)

const (
	bucketName           = "pds-qa-automation"
	awsS3endpoint        = "s3.amazonaws.com"
	bkpTimeOut           = 30 * time.Minute
	bkpTimeInterval      = 60 * time.Second
	bkpMaxtimeInterval   = 10 * time.Minute
	BACKUP_JOB_SUCCEEDED = "Succeeded"
)

// BackupClient struct
type BackupClient struct {
	controlPlaneURL    string
	Components         *pdsapi.Components
	AWSStorageClient   *awsStorageClient
	AzureStorageClient *azureStorageClient
	GCPStorageClient   *gcpStorageClient
}

// CreateAwsS3BackupCredsAndTarget create backup creds,bucket and target.
func (backupClient *BackupClient) CreateAwsS3BackupCredsAndTarget(tenantId, name, deploymentTargetId string) (*pds.ModelsBackupTarget, error) {
	log.Info("Add AWS S3 backup credentials")
	akid := backupClient.AWSStorageClient.accessKey
	skid := backupClient.AWSStorageClient.secretKey
	region := backupClient.AWSStorageClient.region
	log.Debugf("Creating backup %s credentials", name)
	backupCred, err := backupClient.Components.BackupCredential.CreateS3BackupCredential(tenantId, name, akid, awsS3endpoint, skid)
	if err != nil {
		return nil, fmt.Errorf("Error in adding the backup credentials to PDS , Err: %v ", err)
	}
	log.Infof("Backup Credential %v created successfully.", backupCred.GetName())
	log.Info("Create S3 bucket on AWS cloud.")
	err = backupClient.AWSStorageClient.createBucket()
	if err != nil {
		return nil, fmt.Errorf("Failed while creating S3 bucket, Err: %v ", err)
	}
	log.Infof("Adding backup target {Name: %v} to PDS.", name)
	backupTarget, err := backupClient.Components.BackupTarget.CreateBackupTarget(tenantId, name, backupCred.GetId(), bucketName, region, "s3")
	time.Sleep(bkpTimeInterval)
	if err != nil {
		return nil, fmt.Errorf("Failed to create AWS S3 backup target, Err: %v ", err)
	}
	res, err := backupClient.Components.BackupTarget.SyncToBackupLocation(backupTarget.GetId())
	if err != nil || res.StatusCode != status.StatusOK {
		return nil, fmt.Errorf("failed to sync to AWS S3 backup target. Err: %v, Http response: %v", err, res)
	}
	backupStates, err := backupClient.Components.BackupTarget.LisBackupsStateBelongToBackupTarget(backupTarget.GetId())
	if err != nil {
		return nil, fmt.Errorf("Failed to create AWS S3 backup target, Err: %v ", err)
	}
	for _, backupState := range backupStates {
		if deploymentTargetId == backupState.GetDeploymentTargetId() {
			if backupState.GetState() == "successful" {
				log.Infof("Synced successfully to the target cluster and created px credentials, PxCredentialsName: %v", backupState.GetPxCredentialsName())
			} else {
				return nil, fmt.Errorf("state: %v, error message: %v, error details: %v", backupState.GetState(), backupState.GetErrorMessage(), backupState.GetErrorDetails())
			}
		}
	}
	return backupTarget, nil
}

// CreateAzureBackupCredsAndTarget create backup creds,bucket and target.
func (backupClient *BackupClient) CreateAzureBackupCredsAndTarget(tenantId, name string) (*pds.ModelsBackupTarget, error) {
	// Pre-req: Azure bucket should be created by name pds-automation-1
	log.Info("Add Azure backup creadentials")
	accountKey := backupClient.AzureStorageClient.accountKey
	accountName := backupClient.AzureStorageClient.accountName
	backupCred, err := backupClient.Components.BackupCredential.CreateAzureBackupCredential(tenantId, name, accountKey, accountName)
	if err != nil {
		return nil, fmt.Errorf("Error in adding the backup credentials to PDS , Err: %v ", err)
	}
	log.Info("Create Azure containers for backup.")
	err = backupClient.AzureStorageClient.createBucket()
	if err != nil {
		return nil, fmt.Errorf("Failed while creating Azure container, Err: %v ", err)
	}
	log.Infof("Adding backup target {Name: %v} to PDS.", name)
	backupTarget, err := backupClient.Components.BackupTarget.CreateBackupTarget(tenantId, name, backupCred.GetId(), bucketName, "", "azure")
	if err != nil {
		return nil, fmt.Errorf("Failed to create Azure backup target, Err: %v ", err)
	}
	log.Infof("[Backup Target: %v]Syncing to target clusters", name)
	return backupTarget, nil
}

// CreateGcpBackupCredsAndTarget create backup creds,bucket and target.
func (backupClient *BackupClient) CreateGcpBackupCredsAndTarget(tenantId, name string) (*pds.ModelsBackupTarget, error) {
	log.Info("Create google storage for adding GCP backup target to control plane.")
	err := backupClient.GCPStorageClient.createBucket()
	if err != nil {
		return nil, fmt.Errorf("Failed while creating bucket, Err: %v ", err)
	}
	log.Info("Add GCP backup credentials")
	projectId := backupClient.GCPStorageClient.projectId
	key := "GCP_JSON_PATH"
	gcpJsonKey, isExist := os.LookupEnv(key)
	if !isExist {
		return nil, fmt.Errorf("environment var: %v doesn't exist", key)
	}
	backupCred, err := backupClient.Components.BackupCredential.CreateGoogleCredential(tenantId, name, projectId, gcpJsonKey)
	if err != nil {
		return nil, fmt.Errorf("Error in adding the backup credentials to PDS , Err: %v ", err)
	}
	log.Infof("Adding backup target {Name: %v} to PDS.", name)
	backupTarget, err := backupClient.Components.BackupTarget.CreateBackupTarget(tenantId, name, backupCred.GetId(), bucketName, "", "google")
	if err != nil {
		return nil, fmt.Errorf("Failed to create google backup target, Err: %v ", err)
	}
	return backupTarget, nil

}

func (backupClient *BackupClient) GetAllBackUpTargets(projectID, bkptargetPrefix string) ([]pds.ModelsBackupTarget, error) {
	log.Info("Get all backup targets matches to a prefix")
	var bkpTargets []pds.ModelsBackupTarget
	allbkpTargets, err := backupClient.Components.BackupTarget.ListBackupTargetBelongsToProject(projectID)
	if err != nil {
		return nil, err
	}

	for index, bkpTarget := range allbkpTargets {
		if strings.Contains(bkpTarget.GetName(), bkptargetPrefix) {
			log.Debugf("backuptarget name %s", bkpTarget.GetName())
			bkpTargets = append(bkpTargets, allbkpTargets[index])
		}
	}

	return bkpTargets, nil
}

// DeleteAwsS3BackupCredsAndTarget delete backup creds,bucket and target.
func (backupClient *BackupClient) DeleteAwsS3BackupCredsAndTarget(backupTargetId string) error {
	log.Info("Delete S3 bucket from AWS cloud.")
	err := backupClient.AWSStorageClient.DeleteBucket()
	if err != nil {
		return fmt.Errorf("Failed to delete S3 bucket %s, Err: %v ", bucketName, err)
	}
	log.Info("Removing S3 backup creadentials and target from PDS.")
	backupTarget, err := backupClient.Components.BackupTarget.GetBackupTarget(backupTargetId)
	if err != nil {
		return fmt.Errorf("Failed while fetching the backup target details, Err: %v ", err)
	}
	credId := backupTarget.GetBackupCredentialsId()
	log.Infof("Deleting backup target {Name: %v} to PDS.", backupTarget.GetName())
	_, err = backupClient.Components.BackupTarget.DeleteBackupTarget(backupTargetId)
	if err != nil {
		return fmt.Errorf("Failed to delete AWS S3 backup target, Err: %v ", err)
	}
	waitErr := wait.Poll(bkpTimeInterval, 1*time.Minute, func() (bool, error) {
		model, bkpErr := backupClient.Components.BackupTarget.GetBackupTarget(backupTargetId)
		if model != nil {
			log.Info(model.GetName())
			return false, bkpErr
		}
		if bkpErr != nil && strings.Contains(bkpErr.Error(), "not found") {
			return true, nil
		}
		return false, bkpErr
	})
	if waitErr != nil {
		return fmt.Errorf("error occured while polling for deleting backuptarget : %v", err)
	}
	_, err = backupClient.Components.BackupCredential.DeleteBackupCredential(credId)
	if err != nil {
		return fmt.Errorf("Failed to delete AWS S3 backup credentials, Err: %v ", err)
	}
	return nil
}

// DeleteAzureBackupCredsAndTarget delete backup creds,bucket and target.
func (backupClient *BackupClient) DeleteAzureBackupCredsAndTarget(backupTargetId string) error {
	log.Info("Delete Azure blob storage")
	err := backupClient.AzureStorageClient.DeleteBucket()
	if err != nil {
		return fmt.Errorf("Failed to delete  azure blob %s, Err: %v ", bucketName, err)
	}
	log.Info("Removing Azure backup credentials and target from PDS.")
	backupTarget, err := backupClient.Components.BackupTarget.GetBackupTarget(backupTargetId)
	if err != nil {
		return fmt.Errorf("Failed while fetching the backup target details, Err: %v ", err)
	}
	credId := backupTarget.GetBackupCredentialsId()
	log.Infof("Deleting backup target {Name: %v} to PDS.", backupTarget.GetName())
	_, err = backupClient.Components.BackupTarget.DeleteBackupTarget(backupTargetId)
	if err != nil {
		return fmt.Errorf("Failed to delete Azure backup target, Err: %v ", err)
	}
	waitErr := wait.Poll(bkpTimeInterval, bkpMaxtimeInterval, func() (bool, error) {
		model, bkpErr := backupClient.Components.BackupTarget.GetBackupTarget(backupTargetId)
		if model != nil {
			log.Info(model.GetName())
			return false, bkpErr
		}
		if bkpErr != nil && strings.Contains(bkpErr.Error(), "not found") {
			return true, nil
		}
		return false, bkpErr
	})
	if waitErr != nil {
		return fmt.Errorf("error occured while polling for deleting backuptarget : %v", err)
	}
	_, err = backupClient.Components.BackupCredential.DeleteBackupCredential(credId)
	if err != nil {
		return fmt.Errorf("Failed to delete Azure backup credentials, Err: %v ", err)
	}
	return nil
}

// DeleteGoogleBackupCredsAndTarget delete backup creds,bucket and target.
func (backupClient *BackupClient) DeleteGoogleBackupCredsAndTarget(backupTargetId string) error {
	log.Info("Deleting google storage.")
	err := backupClient.GCPStorageClient.DeleteBucket()
	if err != nil {
		return fmt.Errorf("Failed to delete bucket %s, Err: %v ", bucketName, err)
	}
	log.Info("Removing Google backup credentials and target from PDS.")
	backupTarget, err := backupClient.Components.BackupTarget.GetBackupTarget(backupTargetId)
	if err != nil {
		return fmt.Errorf("Unable to fetch backup target details using uuid - %v, Err: %v ", backupTargetId, err)
	}
	credId := backupTarget.GetBackupCredentialsId()
	log.Infof("Deleting backup target {Name: %v} to PDS.", backupTarget.GetName())
	_, err = backupClient.Components.BackupTarget.DeleteBackupTarget(backupTargetId)
	if err != nil {
		return fmt.Errorf("Failed to delete Google backup target, Err: %v ", err)
	}
	waitErr := wait.Poll(bkpTimeInterval, bkpMaxtimeInterval, func() (bool, error) {
		model, bkpErr := backupClient.Components.BackupTarget.GetBackupTarget(backupTargetId)
		if model != nil {
			log.Info(model.GetName())
			return false, bkpErr
		}
		if bkpErr != nil && strings.Contains(bkpErr.Error(), "not found") {
			return true, nil
		}
		return false, bkpErr
	})
	if waitErr != nil {
		return fmt.Errorf("error occured while polling for deleting backuptarget : %v", err)
	}
	_, err = backupClient.Components.BackupCredential.DeleteBackupCredential(credId)
	if err != nil {
		return fmt.Errorf("Failed to delete Google backup credentials, Err: %v ", err)
	}
	return nil
}

// GetAllBackupSupportedDataServices get all the backup supported data services and returns the map
func (backupClient *BackupClient) GetAllBackupSupportedDataServices() (map[string]string, error) {
	dataServiceNameIDMap := make(map[string]string)
	dataService, err := backupClient.Components.DataService.ListDataServices()
	if err != nil {
		return nil, err
	}
	for _, ds := range dataService {
		log.Infof("DS- %v", ds.GetName())
		log.Infof("HasFullBackup- %v, Coming soon - %v", *ds.HasFullBackup, *ds.ComingSoon)
		if *ds.HasFullBackup && !*ds.ComingSoon {
			dataServiceNameIDMap[ds.GetName()] = ds.GetId()
		}
	}
	for key, value := range dataServiceNameIDMap {
		log.Infof("Data service: %v ID: %v", key, value)
	}
	return dataServiceNameIDMap, nil
}

// TriggerAndValidateAdhocBackup triggers the adhoc backup for given ds and store at the given backup target and validate them
func (backupClient *BackupClient) TriggerAndValidateAdhocBackup(deploymentID string, backupTargetID string, backupType string) error {
	var bkpJobs []pds.ModelsBackupJobStatusResponse
	bkpObj, err := backupClient.Components.Backup.CreateBackup(deploymentID, backupTargetID, true)
	if err != nil {
		return fmt.Errorf("failed while creating adhoc backup. Err: %v", err)
	}
	log.Infof("Created adhoc backup. Details: deployment- %v,backup type - %v, backup resource name: %v, backupObj id: %v", bkpObj.GetDeploymentName(),
		bkpObj.GetBackupType(), bkpObj.GetClusterResourceName(), bkpObj.GetId())

	waitErr := wait.Poll(bkpTimeInterval, bkpMaxtimeInterval, func() (bool, error) {
		bkpJobs, err = backupClient.Components.BackupJob.ListBackupJobs(bkpObj.GetId())
		if err != nil {
			return false, err
		}
		log.Infof("[Backup job: %v] Status: %v", bkpJobs[0].GetName(), bkpJobs[0].GetStatus())
		if bkpJobs[0].GetStatus() == "Succeeded" {
			return true, nil
		} else {
			return false, nil
		}
	})
	if waitErr != nil {
		return fmt.Errorf("error occured while polling the status of backup job object. Err:%v", waitErr)
	}

	log.Infof("Created adhoc backup successfully for %v,"+
		" backup job: %v, backup job creation time: %v, backup job completion time: %v",
		bkpObj.GetClusterResourceName(), bkpJobs[0].GetName(), bkpJobs[0].GetStartTime(), bkpJobs[0].GetCompletionTime())
	return nil
}

// InitializePdsBackup to create backup creds/targets.
func InitializePdsBackup() (*BackupClient, error) {
	// TODO: Reuse PDSInit func
	envVars := pdsutils.BackupEnvVariables()
	apiConf := pds.NewConfiguration()
	endpointURL, err := url.Parse(envVars.PDSControlPlaneURL)
	if err != nil {
		return nil, fmt.Errorf("an Error Occured while parsing the URL %v", err)
	}
	apiConf.Host = endpointURL.Host
	apiConf.Scheme = endpointURL.Scheme

	apiClient := pds.NewAPIClient(apiConf)
	Components := pdsapi.NewComponents(apiClient)

	backupClient := &BackupClient{
		controlPlaneURL: envVars.PDSControlPlaneURL,
		Components:      Components,
		AWSStorageClient: &awsStorageClient{accessKey: envVars.PDSAwsAccessKey,
			secretKey: envVars.PDSAwsSecretKey, region: envVars.PDSAwsRegion},
		AzureStorageClient: &azureStorageClient{accountName: envVars.PDSAzureStorageAccountName,
			accountKey: envVars.PDSAzurePrimaryAccountKey},
		GCPStorageClient: &gcpStorageClient{projectId: envVars.PDSGcpProjectId},
	}
	return backupClient, nil
}
