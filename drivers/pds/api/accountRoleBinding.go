// Package api comprises of all the components and associated CRUD functionality
package api

import (
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
func (accountRoleBinding *AccountRoleBinding) ListAccountsRoleBindings(accountID string) ([]pds.ModelsAccountRoleBinding, error) {
	client := accountRoleBinding.apiClient.AccountRoleBindingsApi
	log.Info("List Account Role Bindings.")
	ctx, err := pdsutils.GetContext()
	if err != nil {
		log.Errorf("Error in getting context for api call: %v\n", err)
		return nil, err
	}
	accountRoleBindings, res, err := client.ApiAccountsIdRoleBindingsGet(ctx, accountID).Execute()
	if err != nil && res.StatusCode != status.StatusOK {
		log.Errorf("Error when calling `ApiAccountsIdRoleBindingsGet``: %v\n", err)
		log.Errorf("Full HTTP response: %v\n", res)
		return nil, err
	}
	return accountRoleBindings.GetData(), nil
}

// ListAccountRoleBindingsOfUser function return pds account role bindings model for a given user.
func (accountRoleBinding *AccountRoleBinding) ListAccountRoleBindingsOfUser(userID string) ([]pds.ModelsAccountRoleBinding, error) {
	client := accountRoleBinding.apiClient.AccountRoleBindingsApi
	ctx, err := pdsutils.GetContext()
	if err != nil {
		log.Errorf("Error in getting context for api call: %v\n", err)
		return nil, err
	}
	accRoleModels, res, err := client.ApiUsersIdAccountRoleBindingsGet(ctx, userID).Execute()
	if err != nil && res.StatusCode != status.StatusOK {
		log.Errorf("Error when calling `ApiUsersIdAccountRoleBindingsGet``: %v\n", err)
		log.Errorf("Full HTTP response: %v\n", res)
		return nil, err
	}
	return accRoleModels.GetData(), nil
}

// UpdateAccountRoleBinding function return the updated pds account role binding model.
func (accountRoleBinding *AccountRoleBinding) UpdateAccountRoleBinding(accountID string, actorID string, actorType string, roleName string) (*pds.ModelsAccountRoleBinding, error) {
	client := accountRoleBinding.apiClient.AccountRoleBindingsApi
	updateReq := pds.ControllersUpsertAccountRoleBindingRequest{ActorId: &actorID, ActorType: &actorType, RoleName: &roleName}
	log.Info("Get list of Accounts.")
	ctx, err := pdsutils.GetContext()
	if err != nil {
		log.Errorf("Error in getting context for api call: %v\n", err)
		return nil, err
	}
	accRoleBinding, res, err := client.ApiAccountsIdRoleBindingsPut(ctx, accountID).Body(updateReq).Execute()
	if err != nil && res.StatusCode != status.StatusOK {
		log.Errorf("Error when calling `ApiAccountsIdRoleBindingsPut``: %v\n", err)
		log.Errorf("Full HTTP response: %v\n", res)
		return nil, err
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
	invitationRequest := pds.ControllersInvitationRequest{Email: &email, RoleName: &rBinding}
	log.Info("Get list of Accounts.")
	ctx, err := pdsutils.GetContext()
	if err != nil {
		log.Errorf("Error in getting context for api call: %v\n", err)
		return err
	}
	res, err := client.ApiAccountsIdInvitationsPost(ctx, accountID).Body(invitationRequest).Execute()
	if err != nil && res.StatusCode != status.StatusOK {
		log.Errorf("Error when calling `ApiAccountsIdInvitationsPost``: %v\n", err)
		log.Errorf("Full HTTP response: %v\n", res)
		return err
	}
	return nil
}
