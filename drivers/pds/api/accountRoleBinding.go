// Package api comprises of all the components and associated CRUD functionality
package api

import (
	"fmt"
	status "net/http"

	pds "github.com/portworx/pds-api-go-client/pds/v1alpha1"
	"github.com/portworx/torpedo/drivers/pds/pdsutils"
	"github.com/portworx/torpedo/pkg/log"
)

// AccountRoleBinding struct
type AccountRoleBinding struct {
	apiClient *pds.APIClient
}

// ListAccountsRoleBindings function return pds account role bindings model.
func (accountRoleBinding *AccountRoleBinding) ListAccountsRoleBindings(accountID string) ([]pds.ModelsLegacyAccountBinding, error) {
	client := accountRoleBinding.apiClient.AccountRoleBindingsApi
	log.Info("List Account Role Bindings.")
	ctx, err := pdsutils.GetContext()
	if err != nil {
		return nil, fmt.Errorf("Error in getting context for api call: %v\n", err)
	}
	accountRoleBindings, res, err := client.ApiAccountsIdRoleBindingsGet(ctx, accountID).Execute()
	if err != nil && res.StatusCode != status.StatusOK {
		return nil, fmt.Errorf("Error when calling `ApiAccountsIdRoleBindingsGet``: %v\n.Full HTTP response: %v", err, res)
	}
	return accountRoleBindings.GetData(), nil
}

// ListAccountRoleBindingsOfUser function return pds account role bindings model for a given user.
func (accountRoleBinding *AccountRoleBinding) ListAccountRoleBindingsOfUser(userID string) ([]pds.ModelsLegacyAccountBinding, error) {
	client := accountRoleBinding.apiClient.AccountRoleBindingsApi
	ctx, err := pdsutils.GetContext()
	if err != nil {
		return nil, fmt.Errorf("Error in getting context for api call: %v\n", err)
	}
	accRoleModels, res, err := client.ApiUsersIdAccountRoleBindingsGet(ctx, userID).Execute()
	if err != nil && res.StatusCode != status.StatusOK {
		return nil, fmt.Errorf("Error when calling `ApiUsersIdAccountRoleBindingsGet`: %v\n.Full HTTP response: %v", err, res)
	}
	return accRoleModels.GetData(), nil
}

// UpdateAccountRoleBinding function return the updated pds account role binding model.
func (accountRoleBinding *AccountRoleBinding) UpdateAccountRoleBinding(accountID string, actorID string, actorType string, roleName string) (*pds.ModelsLegacyAccountBinding, error) {
	client := accountRoleBinding.apiClient.AccountRoleBindingsApi
	updateReq := pds.RequestsPutLegacyBindingRequest{ActorId: &actorID, ActorType: &actorType, RoleName: &roleName}
	log.Info("Get list of Accounts.")
	ctx, err := pdsutils.GetContext()
	if err != nil {
		return nil, fmt.Errorf("Error in getting context for api call: %v\n", err)
	}
	accRoleBinding, res, err := client.ApiAccountsIdRoleBindingsPut(ctx, accountID).Body(updateReq).Execute()
	if err != nil && res.StatusCode != status.StatusOK {
		return nil, fmt.Errorf("Error when calling `ApiAccountsIdRoleBindingsPut`: %v\n.Full HTTP response: %v", err, res)
	}
	return accRoleBinding, nil
}

// AddUser function add new user with admin/nonadmin role.
func (accountRoleBinding *AccountRoleBinding) AddUser(accountID string, email string, isAdmin bool) error {
	client := accountRoleBinding.apiClient.AccountRoleBindingsApi
	rBinding := "account-reader"
	if isAdmin {
		rBinding = "account-admin"
	}
	invitationRequest := pds.RequestsInvitationAccountRequest{Email: email, RoleName: rBinding}
	log.Info("Get list of Accounts.")
	ctx, err := pdsutils.GetContext()
	if err != nil {
		return fmt.Errorf("Error in getting context for api call: %v\n", err)
	}
	res, err := client.ApiAccountsIdInvitationsPost(ctx, accountID).Body(invitationRequest).Execute()
	if err != nil && res.StatusCode != status.StatusOK {
		return fmt.Errorf("Error when calling `ApiAccountsIdInvitationsPost`: %v\n.Full HTTP response: %v", err, res)
	}
	return nil
}

// RemoveUser function delete the user from given account.
func (accountRoleBinding *AccountRoleBinding) RemoveUser(userID string) (*status.Response, error) {
	client := accountRoleBinding.apiClient.AccountRoleBindingsApi
	ctx, err := pdsutils.GetContext()
	if err != nil {
		return nil, fmt.Errorf("error in getting context for api call: %v", err)
	}
	res, err := client.ApiAccountsIdRoleBindingsDelete(ctx, userID).Execute()
	if err != nil {
		return nil, fmt.Errorf("error while removing the user: %v", err)
	}
	return res, nil
}
