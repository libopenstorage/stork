// Package api comprises of all the components and associated CRUD functionality
package api

import (
	status "net/http"

	pds "github.com/portworx/pds-api-go-client/pds/v1alpha1"
	"github.com/portworx/torpedo/drivers/pds/pdsutils"

	"github.com/portworx/torpedo/pkg/log"
)

// Account struct
type Account struct {
	apiClient *pds.APIClient
}

// GetAccountsList return pds accounts model.
func (account *Account) GetAccountsList() ([]pds.ModelsAccount, error) {
	client := account.apiClient.AccountsApi
	log.Info("Get list of Accounts.")
	ctx, err := pdsutils.GetContext()
	if err != nil {
		log.Errorf("Error in getting context for api call: %v\n", err)
		return nil, err
	}
	accountsModel, res, err := client.ApiAccountsGet(ctx).Execute()

	if err != nil && res.StatusCode != status.StatusOK {
		log.Errorf("Error when calling `ApiAccountsGet``: %v\n", err)
		log.Errorf("Full HTTP response: %v\n", res)
		return nil, err
	}
	return accountsModel.GetData(), nil
}

// GetAccount return pds account model.
func (account *Account) GetAccount(accountID string) (*pds.ModelsAccount, error) {
	client := account.apiClient.AccountsApi
	log.Infof("Get the account detail having UUID: %v", accountID)
	ctx, err := pdsutils.GetContext()
	if err != nil {
		log.Errorf("Error in getting context for api call: %v\n", err)
		return nil, err
	}
	accountModel, res, err := client.ApiAccountsIdGet(ctx, accountID).Execute()

	if err != nil && res.StatusCode != status.StatusOK {
		log.Errorf("Full HTTP response: %v\n", res)
		log.Errorf("Error when calling `ApiAccountsIdGet``: %v\n", err)
		return nil, err
	}
	return accountModel, nil
}

// GetAccountUsers return pds user model.
func (account *Account) GetAccountUsers(accountID string) ([]pds.ModelsUser, error) {
	client := account.apiClient.AccountsApi
	accountInfo, _ := account.GetAccount(accountID)
	log.Infof("Get the users belong to the account having name: %v", accountInfo.GetName())
	ctx, err := pdsutils.GetContext()
	if err != nil {
		log.Errorf("Error in getting context for api call: %v\n", err)
		return nil, err
	}
	usersModel, res, err := client.ApiAccountsIdUsersGet(ctx, accountID).Execute()
	if err != nil && res.StatusCode != status.StatusOK {
		log.Errorf("Full HTTP response: %v\n", res)
		log.Errorf("Error when calling `ApiAccountsIdUsersGet``: %v\n", err)
		return nil, err
	}
	return usersModel.GetData(), nil
}

// AcceptEULA function accept the license for newly created account.
func (account *Account) AcceptEULA(accountID string, eulaVersion string) error {
	client := account.apiClient.AccountsApi
	accountInfo, _ := account.GetAccount(accountID)
	log.Infof("Get the users belong to the account having name: %v", accountInfo.GetName())
	ctx, err := pdsutils.GetContext()
	if err != nil {
		log.Errorf("Error in getting context for api call: %v\n", err)
		return err
	}
	updateRequest := pds.ControllersAcceptEULARequest{
		Version: &eulaVersion,
	}
	res, err := client.ApiAccountsIdEulaPut(ctx, accountID).Body(updateRequest).Execute()
	if err != nil && res.StatusCode != status.StatusOK {
		log.Errorf("Full HTTP response: %v\n", res)
		log.Errorf("Error when calling `ApiAccountsIdUsersGet``: %v\n", err)
	}
	return err
}
