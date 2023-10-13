package api

import (
	"fmt"
	"github.com/portworx/torpedo/pkg/log"
	status "net/http"

	pds "github.com/portworx/pds-api-go-client/pds/v1alpha1"
)

// IamRoleBindings struct
type IamRoleBindings struct {
	apiClient *pds.APIClient
}

type NamespaceRoles struct {
	roles       pds.ModelsBinding
	resourceIds []string
	roleName    string
}

// ListIamRoleBindings return service identities models for a project.
func (iam *IamRoleBindings) ListIamRoleBindings(accountId string) ([]pds.ModelsIAM, error) {
	iamClient := iam.apiClient.IAMApi
	ctx, err := GetContext()
	if err != nil {
		return nil, fmt.Errorf("Error in getting context for api call: %v\n", err)
	}
	iamModels, res, err := iamClient.ApiAccountsIdIamGet(ctx, accountId).Execute()
	if err != nil && res.StatusCode != status.StatusOK {
		return nil, fmt.Errorf("Error when calling `ApiAccountsIdIamGet`: %v\n.Full HTTP response: %v", err, res)
	}
	return iamModels, nil
}

// CreateIamRoleBinding returns newly create IAM RoleBinding object
func (iam *IamRoleBindings) CreateIamRoleBinding(accountId string, actorId string, namespaceRoles []pds.ModelsBinding) (*pds.ModelsIAM, error) {
	iamClient := iam.apiClient.IAMApi

	createRequest := pds.RequestsIAMRequest{
		ActorId: actorId,
		Data: pds.ModelsAccessPolicy{
			Namespace: namespaceRoles},
	}
	ctx, err := GetContext()
	if err != nil {
		return nil, fmt.Errorf("Error in getting context for api call: %v\n", err)
	}
	iamModels, res, err := iamClient.ApiAccountsIdIamPost(ctx, accountId).Body(createRequest).Execute()
	if err != nil && res.StatusCode != status.StatusOK {
		return nil, fmt.Errorf("Error when calling `ApiAccountsIdIamPost`: %v\n.Full HTTP response: %v", err, res)
	}
	return iamModels, nil
}

func (iam *IamRoleBindings) UpdateIamRoleBindings(accountId string, actorId string, namespaceRoles []pds.ModelsBinding) (*pds.ModelsIAM, error) {
	iamClient := iam.apiClient.IAMApi
	updateRequest := pds.RequestsIAMRequest{
		ActorId: actorId,
		Data: pds.ModelsAccessPolicy{
			Namespace: namespaceRoles},
	}
	ctx, err := GetContext()
	if err != nil {
		return nil, fmt.Errorf("Error in getting context for api call: %v\n", err)
	}
	iamModels, res, err := iamClient.ApiAccountsIdIamPut(ctx, accountId).Body(updateRequest).Execute()
	if err != nil && res.StatusCode != status.StatusOK {
		return nil, fmt.Errorf("Error when calling `ApiAccountsIdIamPut`: %v\n.Full HTTP response: %v", err, res)
	}
	log.InfoD("Successfully updated the IAM Roles")
	return iamModels, nil
}

// GetIamRoleBindingByID return IAM RoleBinding model.
func (iam *IamRoleBindings) GetIamRoleBindingByID(iamId string, actorId string) (*pds.ModelsIAM, error) {
	iamClient := iam.apiClient.IAMApi
	ctx, err := GetContext()
	if err != nil {
		return nil, fmt.Errorf("Error in getting context for api call: %v\n", err)
	}
	iamModel, res, err := iamClient.ApiAccountsIdIamActorIdGet(ctx, iamId, actorId).Execute()
	if err != nil && res.StatusCode != status.StatusOK {
		return nil, fmt.Errorf("Error when calling `ApiAccountsIdIamActorIdGet`: %v\n.Full HTTP response: %v", err, res)
	}
	return iamModel, nil
}

// DeleteIamRoleBinding delete IAM RoleBinding and return status.
func (iam *IamRoleBindings) DeleteIamRoleBinding(accountId string, actorId string) (*status.Response, error) {
	iamClient := iam.apiClient.IAMApi
	ctx, err := GetContext()
	if err != nil {
		return nil, fmt.Errorf("Error in getting context for api call: %v\n", err)
	}
	res, err := iamClient.ApiAccountsIdIamActorIdDelete(ctx, accountId, actorId).Execute()
	if err != nil && res.StatusCode != status.StatusOK {
		return nil, fmt.Errorf("Error when calling `ApiAccountsIdIamActorIdDelete`: %v\n.Full HTTP response: %v", err, res)
	}
	return res, nil
}
