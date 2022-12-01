package api

import (
	status "net/http"

	pds "github.com/portworx/pds-api-go-client/pds/v1alpha1"
	"github.com/portworx/torpedo/drivers/pds/pdsutils"
	"github.com/portworx/torpedo/pkg/log"
)

// BackupPolicy struct
type BackupPolicy struct {
	apiClient *pds.APIClient
}

// ListBackupPolicy return backup policy models.
func (backupPolicy *BackupPolicy) ListBackupPolicy(tenantID string) ([]pds.ModelsBackupPolicy, error) {
	backupClient := backupPolicy.apiClient.BackupPoliciesApi
	ctx, err := pdsutils.GetContext()
	if err != nil {
		log.Errorf("Error in getting context for api call: %v\n", err)
		return nil, err
	}
	backupModels, res, err := backupClient.ApiTenantsIdBackupPoliciesGet(ctx, tenantID).Execute()

	if res.StatusCode != status.StatusOK {
		log.Errorf("Error when calling `ApiTenantsIdBackupPoliciesGet``: %v\n", err)
		log.Errorf("Full HTTP response: %v\n", res)
	}
	return backupModels.GetData(), err
}

// GetBackupPolicy return backup policy model.
func (backupPolicy *BackupPolicy) GetBackupPolicy(backupCredID string) (*pds.ModelsBackupPolicy, error) {
	backupClient := backupPolicy.apiClient.BackupPoliciesApi
	ctx, err := pdsutils.GetContext()
	if err != nil {
		log.Errorf("Error in getting context for api call: %v\n", err)
		return nil, err
	}
	backupPolicyModel, res, err := backupClient.ApiBackupPoliciesIdGet(ctx, backupCredID).Execute()

	if res.StatusCode != status.StatusOK {
		log.Errorf("Error when calling `ApiBackupPoliciesIdGet``: %v\n", err)
		log.Errorf("Full HTTP response: %v\n", res)
	}
	return backupPolicyModel, err
}

// CreateBackupPolicy return newly created backup policy model.
func (backupPolicy *BackupPolicy) CreateBackupPolicy(tenantID string, name string, retentionCount int32, scheduleCronExpression string, backupType string) (*pds.ModelsBackupPolicy, error) {
	backupClient := backupPolicy.apiClient.BackupPoliciesApi
	modelBackupSchedule := []pds.ModelsBackupSchedule{{
		RetentionCount: &retentionCount,
		Schedule:       &scheduleCronExpression,
		Type:           &backupType,
	}}
	createRequest := pds.ControllersCreateBackupPolicyRequest{
		Schedules: modelBackupSchedule,
		Name:      &name,
	}
	ctx, err := pdsutils.GetContext()
	if err != nil {
		log.Errorf("Error in getting context for api call: %v\n", err)
		return nil, err
	}
	backupPolicyModel, res, err := backupClient.ApiTenantsIdBackupPoliciesPost(ctx, tenantID).Body(createRequest).Execute()
	if res.StatusCode != status.StatusOK {
		log.Errorf("Error when calling `ApiTenantsIdBackupPoliciesPost``: %v\n", err)
		log.Errorf("Full HTTP response: %v\n", res)
	}
	return backupPolicyModel, err

}

// UpdateBackupPolicy return updated backup policy model.
func (backupPolicy *BackupPolicy) UpdateBackupPolicy(backupCredsID string, name string, retentionCount int32, scheduleCronExpression string, backupType string) (*pds.ModelsBackupPolicy, error) {
	backupClient := backupPolicy.apiClient.BackupPoliciesApi
	modelBackupSchedule := []pds.ModelsBackupSchedule{{
		RetentionCount: &retentionCount,
		Schedule:       &scheduleCronExpression,
		Type:           &backupType,
	}}
	updateRequest := pds.ControllersUpdateBackupPolicyRequest{
		Schedules: modelBackupSchedule,
		Name:      &name,
	}
	ctx, err := pdsutils.GetContext()
	if err != nil {
		log.Errorf("Error in getting context for api call: %v\n", err)
		return nil, err
	}
	backupPolicyModel, res, err := backupClient.ApiBackupPoliciesIdPut(ctx, backupCredsID).Body(updateRequest).Execute()
	if res.StatusCode != status.StatusOK {
		log.Errorf("Error when calling `ApiBackupPoliciesIdPut``: %v\n", err)
		log.Errorf("Full HTTP response: %v\n", res)
	}
	return backupPolicyModel, err

}

// DeleteBackupPolicy delete backup policy.
func (backupPolicy *BackupPolicy) DeleteBackupPolicy(backupCredsID string) (*status.Response, error) {
	backupClient := backupPolicy.apiClient.BackupPoliciesApi
	ctx, err := pdsutils.GetContext()
	if err != nil {
		log.Errorf("Error in getting context for api call: %v\n", err)
		return nil, err
	}
	res, err := backupClient.ApiBackupPoliciesIdDelete(ctx, backupCredsID).Execute()
	if err != nil && res.StatusCode != status.StatusOK {
		log.Errorf("Error when calling `ApiBackupPoliciesIdDelete``: %v\n", err)
		log.Errorf("Full HTTP response: %v\n", res)
		return nil, err
	}
	return res, nil
}
