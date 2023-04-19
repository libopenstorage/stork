package pdsbackup

import (
	"fmt"
	"k8s.io/apimachinery/pkg/util/wait"
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
	bucketName         = "pds-qa-automation"
	awsS3endpoint      = "s3.amazonaws.com"
	bkpTimeOut         = 30 * time.Minute
	bkpTimeInterval    = 10 * time.Second
	bkpMaxtimeInterval = 60 * time.Second
)

// BackupClient struct
type BackupClient struct {
	controlPlaneURL    string
	components         *pdsapi.Components
	awsStorageClient   *awsStorageClient
	azureStorageClient *azureStorageClient
	gcpStorageClient   *gcpStorageClient
}

// CreateAwsS3BackupCredsAndTarget create backup creds,bucket and target.
func (backupClient *BackupClient) CreateAwsS3BackupCredsAndTarget(tenantId, name string) (*pds.ModelsBackupTarget, error) {
	log.Info("Add AWS S3 backup credentials")
	akid := backupClient.awsStorageClient.accessKey
	skid := backupClient.awsStorageClient.secretKey
	region := backupClient.awsStorageClient.region
	backupCred, err := backupClient.components.BackupCredential.CreateS3BackupCredential(tenantId, name, akid, awsS3endpoint, skid)
	if err != nil {
		return nil, fmt.Errorf("Error in adding the backup credentials to PDS , Err: %v ", err)
	}
	log.Infof("Backup Credential %v created successfully.", backupCred.GetName())
	log.Info("Create S3 bucket on AWS cloud.")
	err = backupClient.awsStorageClient.createBucket(bucketName)
	if err != nil {
		return nil, fmt.Errorf("Failed while creating S3 bucket, Err: %v ", err)
	}
	log.Infof("Adding backup target {Name: %v} to PDS.", name)
	backupTarget, err := backupClient.components.BackupTarget.CreateBackupTarget(tenantId, name, backupCred.GetId(), bucketName, region, "s3")
	if err != nil {

		return nil, fmt.Errorf("Failed to create AWS S3 backup target, Err: %v ", err)
	}
	return backupTarget, nil
}

// CreateAzureBackupCredsAndTarget create backup creds,bucket and target.
func (backupClient *BackupClient) CreateAzureBackupCredsAndTarget(tenantId, name string) (*pds.ModelsBackupTarget, error) {
	// Pre-req: Azure bucket should be created by name pds-automation-1
	log.Info("Add Azure backup creadentials")
	accountKey := backupClient.azureStorageClient.accountKey
	accountName := backupClient.azureStorageClient.accountName
	backupCred, err := backupClient.components.BackupCredential.CreateAzureBackupCredential(tenantId, name, accountKey, accountName)
	if err != nil {
		return nil, fmt.Errorf("Error in adding the backup credentials to PDS , Err: %v ", err)
	}
	log.Info("Create Azure containers for backup.")
	err = backupClient.azureStorageClient.createBucket(bucketName)
	if err != nil {
		return nil, fmt.Errorf("Failed while creating Azure container, Err: %v ", err)
	}
	log.Infof("Adding backup target {Name: %v} to PDS.", name)
	backupTarget, err := backupClient.components.BackupTarget.CreateBackupTarget(tenantId, name, backupCred.GetId(), bucketName, "", "azure")
	if err != nil {
		return nil, fmt.Errorf("Failed to create Azure backup target, Err: %v ", err)
	}
	log.Infof("[Backup Target: %v]Syncing to target clusters", name)
	return backupTarget, nil
}

// CreateGcpBackupCredsAndTarget create backup creds,bucket and target.
func (backupClient *BackupClient) CreateGcpBackupCredsAndTarget(tenantId, name string) (*pds.ModelsBackupTarget, error) {
	log.Info("Create google storage for adding GCP backup target to control plane.")
	err := backupClient.gcpStorageClient.createBucket(bucketName)
	if err != nil {
		return nil, fmt.Errorf("Failed while creating bucket, Err: %v ", err)
	}
	log.Info("Add GCP backup credentials")
	projectId := backupClient.gcpStorageClient.projectId
	key := "GCP_JSON_PATH"
	gcpJsonKey, isExist := os.LookupEnv(key)
	if !isExist {
		return nil, fmt.Errorf("environment var: %v doesn't exist", key)
	}
	backupCred, err := backupClient.components.BackupCredential.CreateGoogleCredential(tenantId, name, projectId, gcpJsonKey)
	if err != nil {
		return nil, fmt.Errorf("Error in adding the backup credentials to PDS , Err: %v ", err)
	}
	log.Infof("Adding backup target {Name: %v} to PDS.", name)
	backupTarget, err := backupClient.components.BackupTarget.CreateBackupTarget(tenantId, name, backupCred.GetId(), bucketName, "", "google")
	if err != nil {
		return nil, fmt.Errorf("Failed to create google backup target, Err: %v ", err)
	}
	return backupTarget, nil

}

// DeleteAwsS3BackupCredsAndTarget delete backup creds,bucket and target.
func (backupClient *BackupClient) DeleteAwsS3BackupCredsAndTarget(backupTargetId string) error {
	log.Info("Removing S3 backup creadentials and target from PDS.")
	backupTarget, err := backupClient.components.BackupTarget.GetBackupTarget(backupTargetId)
	if err != nil {
		return fmt.Errorf("Failed while fetching the backup target details, Err: %v ", err)
	}
	credId := backupTarget.GetBackupCredentialsId()
	log.Infof("Deleting backup target {Name: %v} to PDS.", backupTarget.GetName())
	_, err = backupClient.components.BackupTarget.DeleteBackupTarget(backupTargetId)
	if err != nil {
		return fmt.Errorf("Failed to delete AWS S3 backup target, Err: %v ", err)
	}
	waitErr := wait.Poll(bkpTimeInterval, bkpMaxtimeInterval, func() (bool, error) {
		model, bkpErr := backupClient.components.BackupTarget.GetBackupTarget(backupTargetId)
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
	_, err = backupClient.components.BackupCredential.DeleteBackupCredential(credId)
	if err != nil {
		return fmt.Errorf("Failed to delete AWS S3 backup credentials, Err: %v ", err)
	}
	log.Info("Delete S3 bucket from AWS cloud.")
	err = backupClient.awsStorageClient.deleteBucket(bucketName)
	if err != nil {
		return fmt.Errorf("Failed to delete S3 bucket %s, Err: %v ", bucketName, err)
	}
	return nil
}

// DeleteAzureBackupCredsAndTarget delete backup creds,bucket and target.
func (backupClient *BackupClient) DeleteAzureBackupCredsAndTarget(backupTargetId string) error {
	log.Info("Removing Azure backup credentials and target from PDS.")
	backupTarget, err := backupClient.components.BackupTarget.GetBackupTarget(backupTargetId)
	if err != nil {
		return fmt.Errorf("Failed while fetching the backup target details, Err: %v ", err)
	}
	credId := backupTarget.GetBackupCredentialsId()
	log.Infof("Deleting backup target {Name: %v} to PDS.", backupTarget.GetName())
	_, err = backupClient.components.BackupTarget.DeleteBackupTarget(backupTargetId)
	if err != nil {
		return fmt.Errorf("Failed to delete Azure backup target, Err: %v ", err)
	}
	waitErr := wait.Poll(bkpTimeInterval, bkpMaxtimeInterval, func() (bool, error) {
		model, bkpErr := backupClient.components.BackupTarget.GetBackupTarget(backupTargetId)
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
	_, err = backupClient.components.BackupCredential.DeleteBackupCredential(credId)
	if err != nil {
		return fmt.Errorf("Failed to delete Azure backup credentials, Err: %v ", err)
	}

	log.Info("Delete Azure blob storage")
	err = backupClient.azureStorageClient.deleteBucket(bucketName)
	if err != nil {
		return fmt.Errorf("Failed to delete  azure blob %s, Err: %v ", bucketName, err)
	}
	return nil
}

// DeleteGoogleBackupCredsAndTarget delete backup creds,bucket and target.
func (backupClient *BackupClient) DeleteGoogleBackupCredsAndTarget(backupTargetId string) error {
	log.Info("Removing Google backup credentials and target from PDS.")
	backupTarget, err := backupClient.components.BackupTarget.GetBackupTarget(backupTargetId)
	if err != nil {
		return fmt.Errorf("Unable to fetch backup target details using uuid - %v, Err: %v ", backupTargetId, err)
	}
	credId := backupTarget.GetBackupCredentialsId()
	log.Infof("Deleting backup target {Name: %v} to PDS.", backupTarget.GetName())
	_, err = backupClient.components.BackupTarget.DeleteBackupTarget(backupTargetId)
	if err != nil {
		return fmt.Errorf("Failed to delete Google backup target, Err: %v ", err)
	}
	waitErr := wait.Poll(bkpTimeInterval, bkpMaxtimeInterval, func() (bool, error) {
		model, bkpErr := backupClient.components.BackupTarget.GetBackupTarget(backupTargetId)
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
	_, err = backupClient.components.BackupCredential.DeleteBackupCredential(credId)
	if err != nil {
		return fmt.Errorf("Failed to delete Google backup credentials, Err: %v ", err)
	}
	log.Info("Deleting google storage.")
	err = backupClient.gcpStorageClient.deleteBucket(bucketName)
	if err != nil {
		return fmt.Errorf("Failed to delete bucket %s, Err: %v ", bucketName, err)
	}
	return nil
}

// InitializePdsBackup to create backup creds/targets.
func InitializePdsBackup() (*BackupClient, error) {
	envVars := pdsutils.BackupEnvVariables()
	apiConf := pds.NewConfiguration()
	endpointURL, err := url.Parse(envVars.PDSControlPlaneURL)
	if err != nil {
		return nil, fmt.Errorf("an Error Occured while parsing the URL %v", err)
	}
	apiConf.Host = endpointURL.Host
	apiConf.Scheme = endpointURL.Scheme

	apiClient := pds.NewAPIClient(apiConf)
	components := pdsapi.NewComponents(apiClient)

	backupClient := &BackupClient{
		controlPlaneURL: envVars.PDSControlPlaneURL,
		components:      components,
		awsStorageClient: &awsStorageClient{accessKey: envVars.PDSAwsAccessKey,
			secretKey: envVars.PDSAwsSecretKey, region: envVars.PDSAwsRegion},
		azureStorageClient: &azureStorageClient{accountName: envVars.PDSAzureStorageAccountName,
			accountKey: envVars.PDSAzurePrimaryAccountKey},
		gcpStorageClient: &gcpStorageClient{projectId: envVars.PDSGcpProjectId},
	}
	return backupClient, nil
}
