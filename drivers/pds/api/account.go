// Package api comprises of all the components and associated CRUD functionality
package api

import (
	"fmt"
	status "net/http"

	pds "github.com/portworx/pds-api-go-client/pds/v1alpha1"
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
	ctx, err := GetContext()
	if err != nil {
		return nil, fmt.Errorf("Error in getting context for api call: %v\n", err)
	}
	accountsModel, res, err := client.ApiAccountsGet(ctx).Execute()

	if err != nil && res.StatusCode != status.StatusOK {
		return nil, fmt.Errorf("Error when calling `ApiAccountsGet``: %v\n.Full HTTP response: %v", err, res)
	}
	return accountsModel.GetData(), nil
}

// GetAccount return pds account model.
func (account *Account) GetAccount(accountID string) (*pds.ModelsAccount, error) {
	client := account.apiClient.AccountsApi
	log.Infof("Get the account detail having UUID: %v", accountID)
	ctx, err := GetContext()
	if err != nil {
		return nil, fmt.Errorf("Error in getting context for api call: %v\n", err)
	}
	accountModel, res, err := client.ApiAccountsIdGet(ctx, accountID).Execute()

	if err != nil && res.StatusCode != status.StatusOK {
		return nil, fmt.Errorf("Error when calling `ApiAccountsIdGet`: %v\n.Full HTTP response: %v", err, res)
	}
	return accountModel, nil
}

// GetAccountUsers return pds user model.
func (account *Account) GetAccountUsers(accountID string) ([]pds.ModelsUser, error) {
	client := account.apiClient.AccountsApi
	accountInfo, _ := account.GetAccount(accountID)
	log.Infof("Get the users belong to the account having name: %v", accountInfo.GetName())
	ctx, err := GetContext()
	if err != nil {
		return nil, fmt.Errorf("Error in getting context for api call: %v\n", err)
	}
	usersModel, res, err := client.ApiAccountsIdUsersGet(ctx, accountID).Execute()
	if err != nil && res.StatusCode != status.StatusOK {
		return nil, fmt.Errorf("Error when called `ApiAccountsIdUsersGet`: %v\n.Full HTTP response: %v", err, res)
	}
	return usersModel.GetData(), nil
}

// AcceptEULA function accept the license for newly created account.
func (account *Account) AcceptEULA(accountID string, eulaVersion string) error {
	client := account.apiClient.AccountsApi
	accountInfo, _ := account.GetAccount(accountID)
	log.Infof("Get the users belong to the account having name: %v", accountInfo.GetName())
	ctx, err := GetContext()
	if err != nil {
		return fmt.Errorf("Error in getting context for api call: %v\n", err)
	}
	updateRequest := pds.ControllersAcceptEULARequest{
		Version: &eulaVersion,
	}
	res, err := client.ApiAccountsIdEulaPut(ctx, accountID).Body(updateRequest).Execute()
	if err != nil && res.StatusCode != status.StatusOK {
		return fmt.Errorf("Error when called `ApiAccountsIdEulaPut`: %v\n.Full HTTP response: %v", err, res)
	}
	return err
}
