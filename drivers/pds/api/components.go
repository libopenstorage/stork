// Package api comprises of all the components and associated CRUD functionality
package api

import (
	pds "github.com/portworx/pds-api-go-client/pds/v1alpha1"
)

// Components struct comprise of all the component stuct to leverage the usage of the functionality and act as entry point.
type Components struct {
	Account                  *Account
	Tenant                   *Tenant
	Project                  *Project
	AccountRoleBinding       *AccountRoleBinding
	AppConfigTemplate        *AppConfigTemplate
	DataService              *DataService
	DataServiceDeployment    *DataServiceDeployment
	DeploymentTarget         *DeploymentTarget
	Image                    *Image
	Namespace                *Namespace
	ResourceSettingsTemplate *ResourceSettingsTemplate
	StorageSettingsTemplate  *StorageSettingsTemplate
	Version                  *Version
	Backup                   *Backup
	BackupCredential         *BackupCredential
	BackupJob                *BackupJob
	BackupTarget             *BackupTarget
	BackupPolicy             *BackupPolicy
	APIVersion               *PDSVersion
	ServiceAccount           *ServiceAccount
}

// NewComponents create a struct literal that can be leveraged to call all the components functions.
func NewComponents(apiClient *pds.APIClient) *Components {
	return &Components{
		Account: &Account{
			apiClient: apiClient,
		},
		Tenant: &Tenant{
			apiClient: apiClient,
		},
		Project: &Project{

			apiClient: apiClient,
		},
		AccountRoleBinding: &AccountRoleBinding{
			apiClient: apiClient,
		},
		AppConfigTemplate: &AppConfigTemplate{
			apiClient: apiClient,
		},
		DataService: &DataService{
			apiClient: apiClient,
		},
		DataServiceDeployment: &DataServiceDeployment{
			apiClient: apiClient,
		},
		DeploymentTarget: &DeploymentTarget{
			apiClient: apiClient,
		},
		Image: &Image{
			apiClient: apiClient,
		},
		Namespace: &Namespace{
			apiClient: apiClient,
		},
		Version: &Version{
			apiClient: apiClient,
		},
		StorageSettingsTemplate: &StorageSettingsTemplate{
			apiClient: apiClient,
		},
		ResourceSettingsTemplate: &ResourceSettingsTemplate{
			apiClient: apiClient,
		},
		Backup: &Backup{
			apiClient: apiClient,
		},
		BackupCredential: &BackupCredential{
			apiClient: apiClient,
		},
		BackupJob: &BackupJob{
			apiClient: apiClient,
		},
		BackupPolicy: &BackupPolicy{
			apiClient: apiClient,
		},
		BackupTarget: &BackupTarget{
			apiClient: apiClient,
		},
		APIVersion: &PDSVersion{
			apiClient: apiClient,
		},
		ServiceAccount: &ServiceAccount{
			apiClient: apiClient,
		},
	}
}
