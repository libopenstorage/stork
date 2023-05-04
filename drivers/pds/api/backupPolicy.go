package api

import (
	"fmt"
	status "net/http"

	pds "github.com/portworx/pds-api-go-client/pds/v1alpha1"
	"github.com/portworx/torpedo/drivers/pds/pdsutils"
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
		return nil, fmt.Errorf("Error in getting context for api call: %v\n", err)
	}
	backupModels, res, err := backupClient.ApiTenantsIdBackupPoliciesGet(ctx, tenantID).Execute()

	if err != nil && res.StatusCode != status.StatusOK {
		return nil, fmt.Errorf("Error when calling `ApiTenantsIdBackupPoliciesGet`: %v\n.Full HTTP response: %v", err, res)
	}
	return backupModels.GetData(), err
}

// GetBackupPolicy return backup policy model.
func (backupPolicy *BackupPolicy) GetBackupPolicy(backupCredID string) (*pds.ModelsBackupPolicy, error) {
	backupClient := backupPolicy.apiClient.BackupPoliciesApi
	ctx, err := pdsutils.GetContext()
	if err != nil {
		return nil, fmt.Errorf("Error in getting context for api call: %v\n", err)
	}
	backupPolicyModel, res, err := backupClient.ApiBackupPoliciesIdGet(ctx, backupCredID).Execute()

	if err != nil && res.StatusCode != status.StatusOK {
		return nil, fmt.Errorf("Error when calling `ApiBackupPoliciesIdGet`: %v\n.Full HTTP response: %v", err, res)
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
		return nil, fmt.Errorf("Error in getting context for api call: %v\n", err)
	}
	backupPolicyModel, res, err := backupClient.ApiTenantsIdBackupPoliciesPost(ctx, tenantID).Body(createRequest).Execute()
	if err != nil && res.StatusCode != status.StatusOK {
		return nil, fmt.Errorf("Error when calling `ApiTenantsIdBackupPoliciesPost`: %v\n.Full HTTP response: %v", err, res)
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
		return nil, fmt.Errorf("Error in getting context for api call: %v\n", err)
	}
	backupPolicyModel, res, err := backupClient.ApiBackupPoliciesIdPut(ctx, backupCredsID).Body(updateRequest).Execute()
	if err != nil && res.StatusCode != status.StatusOK {
		return nil, fmt.Errorf("Error when calling `ApiBackupPoliciesIdPut`: %v\n.Full HTTP response: %v", err, res)
	}
	return backupPolicyModel, err

}

// DeleteBackupPolicy delete backup policy.
func (backupPolicy *BackupPolicy) DeleteBackupPolicy(backupCredsID string) (*status.Response, error) {
	backupClient := backupPolicy.apiClient.BackupPoliciesApi
	ctx, err := pdsutils.GetContext()
	if err != nil {
		return nil, fmt.Errorf("Error in getting context for api call: %v\n", err)
	}
	res, err := backupClient.ApiBackupPoliciesIdDelete(ctx, backupCredsID).Execute()
	if err != nil && res.StatusCode != status.StatusOK {
		return nil, fmt.Errorf("Error when calling `ApiBackupPoliciesIdDelete`: %v\n.Full HTTP response: %v", err, res)
	}
	return res, nil
}
