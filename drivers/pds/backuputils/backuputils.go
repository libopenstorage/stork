package backuputils

import (
	"net/url"
	"os"

	pds "github.com/portworx/pds-api-go-client/pds/v1alpha1"
	pdsapi "github.com/portworx/torpedo/drivers/pds/api"
	"github.com/portworx/torpedo/drivers/pds/pdsutils"
	log "github.com/sirupsen/logrus"
)

const (
	bucketName            = "pds-automation-1"
	awsS3endpoint         = "s3.amazonaws.com"
	backupJobHistoryLimit = 30
	sleepTime             = 30
)

// BackupClient struct
type BackupClient struct {
	controlPlaneURL           string
	components                *pdsapi.Components
	awsStorageClient          *awsStorageClient
	azureStorageClient        *azureStorageClient
	gcpStorageClient          *gcpStorageClient
	s3CompatibleStorageClient *s3CompatibleStorageClient
}

// CreateAdhocBackup creates the adhoc backup.
func (backupClient *BackupClient) CreateAdhocBackup(deploymentID, backupTargetID string) (*pds.ModelsBackup, error) {
	deployment, err := backupClient.components.DataServiceDeployment.GetDeployment(deploymentID)
	if err != nil {
		log.Errorf("Unable to fetch info about the deployment, Err: %v ", err)
		return nil, err
	}
	backupTarget, err := backupClient.components.BackupTarget.GetBackupTarget(backupTargetID)
	if err != nil {
		log.Errorf("Unable to fetch info about the backup target, Err: %v ", err)
		return nil, err
	}
	backup, err := backupClient.components.Backup.CreateBackup(deploymentID, backupTargetID, backupJobHistoryLimit, true)
	if err != nil {
		log.Errorf("Error while performing adhoc backup for the deployment: {%v}, backuptarget: {%v}, \n Err: %v ", deployment.GetName(), backupTarget.GetName(), err)
		return nil, err
	}
	// Implement verification whether backup job is succededed.
	return backup, nil
}

// CreateScehduledBackup creates the adhoc backup.
func (backupClient *BackupClient) CreateScehduledBackup(deploymentID, backupTargetID string) (*pds.ModelsBackup, error) {
	deployment, err := backupClient.components.DataServiceDeployment.GetDeployment(deploymentID)
	if err != nil {
		log.Errorf("Unable to fetch info about the deployment, Err: %v ", err)
		return nil, err
	}
	backupTarget, err := backupClient.components.BackupTarget.GetBackupTarget(backupTargetID)
	if err != nil {
		log.Errorf("Unable to fetch info about the backup target, Err: %v ", err)
		return nil, err
	}
	backup, err := backupClient.components.Backup.CreateBackup(deploymentID, backupTargetID, backupJobHistoryLimit, false)
	if err != nil {
		log.Errorf("Error while performing adhoc backup for the deployment: {%v}, backuptarget: {%v}, \n Err: %v ", deployment.GetName(), backupTarget.GetName(), err)
		return nil, err
	}
	// Implement verification whether backup job is succededed.
	return backup, nil
}

// CreateAwsS3BackupCredsAndTarget create backup creds,bucket and target.
func (backupClient *BackupClient) CreateAwsS3BackupCredsAndTarget(tenantId, name string) (*pds.ModelsBackupTarget, error) {
	log.Info("Add AWS S3 backup creadentials")
	akid := backupClient.awsStorageClient.accessKey
	skid := backupClient.awsStorageClient.secretKey
	region := backupClient.awsStorageClient.region
	backupCred, err := backupClient.components.BackupCredential.CreateS3BackupCredential(tenantId, name, akid, awsS3endpoint, skid)

	if err != nil {
		log.Errorf("Error in adding the backup credentials to PDS , Err: %v ", err)
		return nil, err
	}
	log.Info("Create S3 bucket on AWS cloud.")
	err = backupClient.awsStorageClient.createBucket(bucketName)
	if err != nil {
		log.Errorf("Failed while creating S3 bucket, Err: %v ", err)
		return nil, err
	}
	log.Infof("Adding backup target {Name: %v} to PDS.", name)
	backupTarget, err := backupClient.components.BackupTarget.CreateBackupTarget(tenantId, name, backupCred.GetId(), bucketName, region, "s3")
	if err != nil {
		log.Errorf("Failed to create AWS S3 backup target, Err: %v ", err)
		return nil, err
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
		log.Errorf("Error in adding the backup credentials to PDS , Err: %v ", err)
		return nil, err
	}

	log.Infof("Adding backup target {Name: %v} to PDS.", name)
	backupTarget, err := backupClient.components.BackupTarget.CreateBackupTarget(tenantId, name, backupCred.GetId(), bucketName, "", "azure")
	if err != nil {
		log.Errorf("Failed to create Azure backup target, Err: %v ", err)
		return nil, err
	}
	return backupTarget, nil

}

// CreateGcpBackupCredsAndTarget create backup creds,bucket and target.
func (backupClient *BackupClient) CreateGcpBackupCredsAndTarget(tenantId, name string) (*pds.ModelsBackupTarget, error) {
	log.Info("Add GCP backup creadentials")
	projectId := backupClient.gcpStorageClient.projectId
	jsongPath := backupClient.gcpStorageClient.jsongPath
	contents, err := os.ReadFile(jsongPath)
	if err != nil {
		log.Infof("File reading error. Err: %v", err)
		return nil, err
	}
	backupCred, err := backupClient.components.BackupCredential.CreateGoogleCredential(tenantId, name, projectId, string(contents))

	if err != nil {
		log.Errorf("Error in adding the backup credentials to PDS , Err: %v ", err)
		return nil, err
	}
	log.Info("Create google storage.")
	err = backupClient.gcpStorageClient.createBucket(bucketName)
	if err != nil {
		log.Errorf("Failed while creating bucket, Err: %v ", err)
		return nil, err
	}
	log.Infof("Adding backup target {Name: %v} to PDS.", name)
	backupTarget, err := backupClient.components.BackupTarget.CreateBackupTarget(tenantId, name, backupCred.GetId(), bucketName, "", "google")
	if err != nil {
		log.Errorf("Failed to create google backup target, Err: %v ", err)
		return nil, err
	}
	return backupTarget, nil

}

// CreateS3CompatibleBackupCredsAndTarget create backup creds,bucket and target.
func (backupClient *BackupClient) CreateS3CompatibleBackupCredsAndTarget(tenantId, name string) (*pds.ModelsBackupTarget, error) {
	log.Info("Add S3 compatible backup creadentials")
	akid := backupClient.s3CompatibleStorageClient.accessKey
	skid := backupClient.s3CompatibleStorageClient.secretKey
	region := backupClient.s3CompatibleStorageClient.region
	endpoint := backupClient.s3CompatibleStorageClient.endpoint
	backupCred, err := backupClient.components.BackupCredential.CreateS3CompatibleBackupCredential(tenantId, name, akid, endpoint, skid)

	if err != nil {
		log.Errorf("Error in adding the backup credentials to PDS , Err: %v ", err)
		return nil, err
	}
	log.Info("Create S3 compatible bucket.")
	err = backupClient.s3CompatibleStorageClient.createBucket(bucketName)
	if err != nil {
		log.Errorf("Failed while creating S3 bucket, Err: %v ", err)
		return nil, err
	}
	log.Infof("Adding backup target {Name: %v} to PDS.", name)
	backupTarget, err := backupClient.components.BackupTarget.CreateBackupTarget(tenantId, name, backupCred.GetId(), bucketName, region, "s3-compatible")
	if err != nil {
		log.Errorf("Failed to create AWS S3 backup target, Err: %v ", err)
		return nil, err
	}
	return backupTarget, nil
}

// DeleteAwsS3BackupCredsAndTarget delete backup creds,bucket and target.
func (backupClient *BackupClient) DeleteAwsS3BackupCredsAndTarget(backupTargetId string) error {
	log.Info("Removing S3 backup creadentials and target from PDS.")
	backupTarget, _ := backupClient.components.BackupTarget.GetBackupTarget(backupTargetId)
	credId := backupTarget.GetBackupCredentialsId()
	log.Infof("Deleting backup target {Name: %v} to PDS.", backupTarget.GetName())
	_, err := backupClient.components.BackupTarget.DeleteBackupTarget(backupTargetId)
	backupTarget.GetBackupCredentialsId()
	if err != nil {
		log.Errorf("Failed to delete AWS S3 backup target, Err: %v ", err)
		return err
	}
	_, err = backupClient.components.BackupCredential.DeleteBackupCredential(credId)
	if err != nil {
		log.Errorf("Failed to delete AWS S3 backup credentials, Err: %v ", err)
		return err
	}
	log.Info("Delete S3 bucket from AWS cloud.")
	err = backupClient.awsStorageClient.deleteBucket(bucketName)
	if err != nil {
		log.Errorf("Failed to delete S3 bucket %s, Err: %v ", bucketName, err)
		return err
	}

	return nil

}

// DeleteAzureBackupCredsAndTarget delete backup creds,bucket and target.
func (backupClient *BackupClient) DeleteAzureBackupCredsAndTarget(backupTargetId string) error {
	log.Info("Removing Azure backup creadentials and target from PDS.")
	backupTarget, _ := backupClient.components.BackupTarget.GetBackupTarget(backupTargetId)
	credId := backupTarget.GetBackupCredentialsId()
	log.Infof("Deleting backup target {Name: %v} to PDS.", backupTarget.GetName())
	_, err := backupClient.components.BackupTarget.DeleteBackupTarget(backupTargetId)
	backupTarget.GetBackupCredentialsId()
	if err != nil {
		log.Errorf("Failed to delete Azure backup target, Err: %v ", err)
		return err
	}
	_, err = backupClient.components.BackupCredential.DeleteBackupCredential(credId)
	if err != nil {
		log.Errorf("Failed to delete Azure backup credentials, Err: %v ", err)
		return err
	}

	return nil

}

// DeleteGoogleBackupCredsAndTarget delete backup creds,bucket and target.
func (backupClient *BackupClient) DeleteGoogleBackupCredsAndTarget(backupTargetId string) error {
	log.Info("Removing Google backup creadentials and target from PDS.")
	backupTarget, _ := backupClient.components.BackupTarget.GetBackupTarget(backupTargetId)
	credId := backupTarget.GetBackupCredentialsId()
	log.Infof("Deleting backup target {Name: %v} to PDS.", backupTarget.GetName())
	_, err := backupClient.components.BackupTarget.DeleteBackupTarget(backupTargetId)
	backupTarget.GetBackupCredentialsId()
	if err != nil {
		log.Errorf("Failed to delete Google backup target, Err: %v ", err)
		return err
	}
	_, err = backupClient.components.BackupCredential.DeleteBackupCredential(credId)
	if err != nil {
		log.Errorf("Failed to delete Google backup credentials, Err: %v ", err)
		return err
	}
	log.Info("Deleting google storage.")
	err = backupClient.gcpStorageClient.deleteBucket(bucketName)
	if err != nil {
		log.Errorf("Failed to delete bucket %s, Err: %v ", bucketName, err)
		return err
	}
	return nil
}

// DeleteS3CompatibleBackupCredsAndTarget delete backup creds,bucket and target.
func (backupClient *BackupClient) DeleteS3CompatibleBackupCredsAndTarget(backupTargetId string) error {
	log.Info("Removing S3-Compatible backup creadentials and target from PDS.")
	backupTarget, _ := backupClient.components.BackupTarget.GetBackupTarget(backupTargetId)
	credId := backupTarget.GetBackupCredentialsId()
	log.Infof("Deleting backup target {Name: %v} to PDS.", backupTarget.GetName())
	_, err := backupClient.components.BackupTarget.DeleteBackupTarget(backupTargetId)
	backupTarget.GetBackupCredentialsId()
	if err != nil {
		log.Errorf("Failed to delete S3-Compatible backup target, Err: %v ", err)
		return err
	}
	_, err = backupClient.components.BackupCredential.DeleteBackupCredential(credId)
	if err != nil {
		log.Errorf("Failed to delete S3-Compatible backup credentials, Err: %v ", err)
		return err
	}
	log.Info("Deleting S3-Compatible storage.")
	err = backupClient.s3CompatibleStorageClient.deleteBucket(bucketName)
	if err != nil {
		log.Errorf("Failed to delete bucket %s, Err: %v ", bucketName, err)
		return err
	}
	return nil
}

// ConfigurePdsBackup to create backup creds/targets.
func ConfigurePdsBackup() (*BackupClient, error) {

	envVars := pdsutils.BackupEnvVariables()
	apiConf := pds.NewConfiguration()
	endpointURL, err := url.Parse(envVars.PDSControlPlaneURL)
	if err != nil {
		log.Errorf("An Error Occured while parsing the URL %v", err)
		return nil, err
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
		gcpStorageClient: &gcpStorageClient{projectId: envVars.PDSGcpProjectId,
			jsongPath: envVars.PDSGcpJsonPath},
		s3CompatibleStorageClient: &s3CompatibleStorageClient{accessKey: envVars.PDSMinioAccessKey,
			secretKey: envVars.PDSMinioSecretKey, region: envVars.PDSMinioRegion},
	}
	return backupClient, nil
}
