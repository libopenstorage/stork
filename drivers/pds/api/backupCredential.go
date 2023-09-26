package api

import (
	"fmt"
	status "net/http"

	pds "github.com/portworx/pds-api-go-client/pds/v1alpha1"
	"github.com/portworx/torpedo/pkg/log"
)

// BackupCredential struct
type BackupCredential struct {
	apiClient *pds.APIClient
}

// ListBackupCredentials return backup credentials model.
func (backupCredential *BackupCredential) ListBackupCredentials(tenantID string) ([]pds.ModelsBackupCredentials, error) {
	backupClient := backupCredential.apiClient.BackupCredentialsApi
	ctx, err := GetContext()
	if err != nil {
		return nil, fmt.Errorf("Error in getting context for api call: %v\n", err)
	}
	backupModels, res, err := backupClient.ApiTenantsIdBackupCredentialsGet(ctx, tenantID).Execute()

	if res.StatusCode != status.StatusOK {
		return nil, fmt.Errorf("Error when calling `ApiTenantsIdBackupCredentialsGet`: %v\n.Full HTTP response: %v", err, res)
	}
	return backupModels.GetData(), err
}

// GetBackupCredential return back upo credential model.
func (backupCredential *BackupCredential) GetBackupCredential(backupCredID string) (*pds.ModelsBackupCredentials, error) {
	backupClient := backupCredential.apiClient.BackupCredentialsApi
	ctx, err := GetContext()
	if err != nil {
		return nil, fmt.Errorf("Error in getting context for api call: %v\n", err)
	}
	backupModel, res, err := backupClient.ApiBackupCredentialsIdGet(ctx, backupCredID).Execute()

	if res.StatusCode != status.StatusOK {
		return nil, fmt.Errorf("Error when calling `ApiBackupCredentialsIdGet`: %v\n.Full HTTP response: %v", err, res)
	}
	return backupModel, err
}

// CreateAzureBackupCredential func
func (backupCredential *BackupCredential) CreateAzureBackupCredential(tenantID string, name string, accountKey string, accountName string) (*pds.ModelsBackupCredentials, error) {
	backupClient := backupCredential.apiClient.BackupCredentialsApi
	azureCredsModel := pds.ModelsAzureCredentials{
		AccountKey:  &accountKey,
		AccountName: &accountName,
	}
	controllerCreds := pds.ControllersCredentials{
		Azure: &azureCredsModel,
	}
	createRequest := pds.ControllersCreateBackupCredentialsRequest{
		Credentials: &controllerCreds,
		Name:        &name,
	}
	ctx, err := GetContext()
	if err != nil {
		return nil, fmt.Errorf("Error in getting context for api call: %v\n", err)
	}
	backupModel, res, err := backupClient.ApiTenantsIdBackupCredentialsPost(ctx, tenantID).Body(createRequest).Execute()
	if res.StatusCode != status.StatusOK {
		return nil, fmt.Errorf("Error when calling `ApiTenantsIdBackupCredentialsPost`: %v\n.Full HTTP response: %v", err, res)
	}
	return backupModel, err

}

// CreateS3BackupCredential func
func (backupCredential *BackupCredential) CreateS3BackupCredential(tenantID string, name string, accessKey string, endpoint string, secretKey string) (*pds.ModelsBackupCredentials, error) {
	backupClient := backupCredential.apiClient.BackupCredentialsApi
	s3CredsModel := pds.ModelsS3Credentials{
		AccessKey: &accessKey,
		Endpoint:  &endpoint,
		SecretKey: &secretKey,
	}
	controllerCreds := pds.ControllersCredentials{
		S3: &s3CredsModel,
	}
	createRequest := pds.ControllersCreateBackupCredentialsRequest{
		Credentials: &controllerCreds,
		Name:        &name,
	}
	ctx, err := GetContext()
	log.Infof("Ctx: %v", ctx)
	if err != nil {
		return nil, fmt.Errorf("Error in getting context for api call: %v\n", err)
	}
	backupModel, res, err := backupClient.ApiTenantsIdBackupCredentialsPost(ctx, tenantID).Body(createRequest).Execute()

	if err != nil {
		return nil, fmt.Errorf("error when called `ApiTenantsIdBackupCredentialsPost` to create credentials - %v", err)
	}
	if res.StatusCode != status.StatusOK {
		return nil, fmt.Errorf("response code is not 200(OK) when called `ApiTenantsIdBackupCredentialsPost. Full response code %v", res)
	}
	return backupModel, err
}

// CreateS3CompatibleBackupCredential func
func (backupCredential *BackupCredential) CreateS3CompatibleBackupCredential(tenantID string, name string, accessKey string, endpoint string, secretKey string) (*pds.ModelsBackupCredentials, error) {
	backupClient := backupCredential.apiClient.BackupCredentialsApi
	s3CompatibleCredsModel := pds.ModelsS3CompatibleCredentials{
		AccessKey: &accessKey,
		Endpoint:  &endpoint,
		SecretKey: &secretKey,
	}
	controllerCreds := pds.ControllersCredentials{
		S3Compatible: &s3CompatibleCredsModel,
	}
	createRequest := pds.ControllersCreateBackupCredentialsRequest{
		Credentials: &controllerCreds,
		Name:        &name,
	}
	ctx, err := GetContext()
	if err != nil {
		return nil, fmt.Errorf("Error in getting context for api call: %v\n", err)
	}
	backupModel, res, err := backupClient.ApiTenantsIdBackupCredentialsPost(ctx, tenantID).Body(createRequest).Execute()
	if err != nil {
		return nil, fmt.Errorf("error when called `ApiTenantsIdBackupCredentialsPost` to create credentials - %v", err)
	}
	if res.StatusCode != status.StatusOK {
		return nil, fmt.Errorf("response code is not 200(OK) when called `ApiTenantsIdBackupCredentialsPost. Full response code %v", res)
	}
	return backupModel, err

}

// CreateGoogleCredential func
func (backupCredential *BackupCredential) CreateGoogleCredential(tenantID string, name string, GcpProjectId string, jsonkey string) (*pds.ModelsBackupCredentials, error) {
	backupClient := backupCredential.apiClient.BackupCredentialsApi
	gcpCredsModel := pds.ModelsGoogleCredentials{
		JsonKey:   &jsonkey,
		ProjectId: &GcpProjectId,
	}
	controllerCreds := pds.ControllersCredentials{
		Google: &gcpCredsModel,
	}
	createRequest := pds.ControllersCreateBackupCredentialsRequest{
		Credentials: &controllerCreds,
		Name:        &name,
	}
	ctx, err := GetContext()
	if err != nil {
		return nil, fmt.Errorf("Error in getting context for api call: %v\n", err)
	}
	backupModel, res, err := backupClient.ApiTenantsIdBackupCredentialsPost(ctx, tenantID).Body(createRequest).Execute()
	if err != nil {
		return nil, err
	}
	if res.StatusCode != status.StatusOK {
		return nil, fmt.Errorf("response code is not 200(OK) when called `ApiTenantsIdBackupCredentialsPost. Full response code %v", res)
	}
	return backupModel, err

}

// UpdateAzureBackupCredential func
func (backupCredential *BackupCredential) UpdateAzureBackupCredential(backupCredsID string, name string, accountKey string, accountName string) (*pds.ModelsBackupCredentials, error) {
	backupClient := backupCredential.apiClient.BackupCredentialsApi
	azureCredsModel := pds.ModelsAzureCredentials{
		AccountKey:  &accountKey,
		AccountName: &accountName,
	}
	controllerCreds := pds.ControllersCredentials{
		Azure: &azureCredsModel,
	}
	updateRequest := pds.ControllersUpdateBackupCredentialsRequest{
		Credentials: &controllerCreds,
		Name:        &name,
	}
	ctx, err := GetContext()
	if err != nil {
		return nil, fmt.Errorf("Error in getting context for api call: %v\n", err)
	}
	backupModel, res, err := backupClient.ApiBackupCredentialsIdPut(ctx, backupCredsID).Body(updateRequest).Execute()
	if res.StatusCode != status.StatusOK {
		return nil, fmt.Errorf("Error when calling `ApiBackupCredentialsIdPut`: %v\n.Full HTTP response: %v", err, res)
	}
	return backupModel, err

}

// UpdateS3BackupCredential func
func (backupCredential *BackupCredential) UpdateS3BackupCredential(backupCredsID string, name string, accessKey string, endpoint string, secretKey string) (*pds.ModelsBackupCredentials, error) {
	backupClient := backupCredential.apiClient.BackupCredentialsApi
	s3CredsModel := pds.ModelsS3Credentials{
		AccessKey: &accessKey,
		Endpoint:  &endpoint,
		SecretKey: &secretKey,
	}
	controllerCreds := pds.ControllersCredentials{
		S3: &s3CredsModel,
	}
	updateRequest := pds.ControllersUpdateBackupCredentialsRequest{
		Credentials: &controllerCreds,
		Name:        &name,
	}
	ctx, err := GetContext()
	if err != nil {
		return nil, fmt.Errorf("Error in getting context for api call: %v\n", err)
	}
	backupModel, res, err := backupClient.ApiBackupCredentialsIdPut(ctx, backupCredsID).Body(updateRequest).Execute()
	if res.StatusCode != status.StatusOK {
		return nil, fmt.Errorf("Error when calling `ApiBackupCredentialsIdPut`: %v\n.Full HTTP response: %v", err, res)
	}
	return backupModel, err

}

// UpdateS3CompatibleBackupCredential func
func (backupCredential *BackupCredential) UpdateS3CompatibleBackupCredential(backupCredsID string, name string, accessKey string, endpoint string, secretKey string) (*pds.ModelsBackupCredentials, error) {
	backupClient := backupCredential.apiClient.BackupCredentialsApi
	s3CompatibleCredsModel := pds.ModelsS3CompatibleCredentials{
		AccessKey: &accessKey,
		Endpoint:  &endpoint,
		SecretKey: &secretKey,
	}
	controllerCreds := pds.ControllersCredentials{
		S3Compatible: &s3CompatibleCredsModel,
	}
	updateRequest := pds.ControllersUpdateBackupCredentialsRequest{
		Credentials: &controllerCreds,
		Name:        &name,
	}
	ctx, err := GetContext()
	if err != nil {
		return nil, fmt.Errorf("Error in getting context for api call: %v\n", err)
	}
	backupModel, res, err := backupClient.ApiBackupCredentialsIdPut(ctx, backupCredsID).Body(updateRequest).Execute()
	if res.StatusCode != status.StatusOK {
		return nil, fmt.Errorf("Error when calling `ApiBackupCredentialsIdPut`: %v\n.Full HTTP response: %v", err, res)
	}
	return backupModel, err

}

// UpdateGoogleBackupCredential func
func (backupCredential *BackupCredential) UpdateGoogleBackupCredential(backupCredsID string, name string, GcpProjectId string, jsonkey string) (*pds.ModelsBackupCredentials, error) {
	backupClient := backupCredential.apiClient.BackupCredentialsApi
	gcpCredsModel := pds.ModelsGoogleCredentials{
		JsonKey:   &GcpProjectId,
		ProjectId: &jsonkey,
	}
	controllerCreds := pds.ControllersCredentials{
		Google: &gcpCredsModel,
	}
	updateRequest := pds.ControllersUpdateBackupCredentialsRequest{
		Credentials: &controllerCreds,
		Name:        &name,
	}
	ctx, err := GetContext()
	if err != nil {
		return nil, fmt.Errorf("Error in getting context for api call: %v\n", err)
	}
	backupModel, res, err := backupClient.ApiBackupCredentialsIdPut(ctx, backupCredsID).Body(updateRequest).Execute()
	if err != nil {
		return nil, fmt.Errorf("Error when calling `ApiBackupCredentialsIdPut``: %v\n", err)
	}
	if res.StatusCode != status.StatusOK {
		return nil, fmt.Errorf("Error when calling `ApiBackupCredentialsIdPut``: %v\n", err)
	}
	return backupModel, err
}

// DeleteBackupCredential func
func (backupCredential *BackupCredential) DeleteBackupCredential(backupCredsID string) (*status.Response, error) {
	backupClient := backupCredential.apiClient.BackupCredentialsApi
	ctx, err := GetContext()
	if err != nil {
		return nil, fmt.Errorf("Error in getting context for api call: %v\n", err)
	}
	res, err := backupClient.ApiBackupCredentialsIdDelete(ctx, backupCredsID).Execute()
	if err != nil {
		return nil, err
	}
	return res, nil
}

// GetCloudCredentials func
func (backupCredential *BackupCredential) GetCloudCredentials(backupCredsID string) (*pds.ControllersPartialCredentials, error) {
	backupClient := backupCredential.apiClient.BackupCredentialsApi
	ctx, err := GetContext()
	if err != nil {
		return nil, fmt.Errorf("Error in getting context for api call: %v\n", err)
	}
	cloudCredsModel, res, err := backupClient.ApiBackupCredentialsIdCredentialsGet(ctx, backupCredsID).Execute()
	if res.StatusCode != status.StatusOK {
		return nil, fmt.Errorf("Error when calling `ApiBackupCredentialsIdCredentialsGet`: %v\n.Full HTTP response: %v", err, res)
	}
	return cloudCredsModel, err
}
