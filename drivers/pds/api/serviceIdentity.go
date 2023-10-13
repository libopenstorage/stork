package api

import (
	"fmt"
	pds "github.com/portworx/pds-api-go-client/pds/v1alpha1"
	"github.com/portworx/torpedo/pkg/log"
	"math/rand"
	status "net/http"
	"strconv"
)

var (
	iamRB   *IamRoleBindings
	SiToken string
)

// ServiceIdentity struct
type ServiceIdentity struct {
	apiClient *pds.APIClient
}

// ListServiceIdentities return service identities models for a project.
func (si *ServiceIdentity) ListServiceIdentities(tenantID string) ([]pds.ModelsServiceIdentity, error) {
	siClient := si.apiClient.ServiceIdentityApi
	ctx, err := GetContext()
	if err != nil {
		return nil, fmt.Errorf("Error in getting context for api call: %v\n", err)
	}
	siModels, res, err := siClient.ApiAccountsIdServiceIdentityGet(ctx, tenantID).Execute()
	if err != nil && res.StatusCode != status.StatusOK {
		return nil, fmt.Errorf("Error when calling `ApiAccountsIdServiceIdentityGet`: %v\n.Full HTTP response: %v", err, res)
	}
	return siModels.GetData(), nil
}

// CreateServiceIdentity returns newly create service identity object
func (si *ServiceIdentity) CreateServiceIdentity(accountID string, name string) (*pds.ModelsServiceIdentityWithToken, error) {
	siClient := si.apiClient.ServiceIdentityApi
	createRequest := pds.RequestsServiceIdentityRequest{Name: name, Enabled: true}
	ctx, err := GetContext()
	if err != nil {
		return nil, fmt.Errorf("Error in getting context for api call: %v\n", err)
	}
	siModels, res, err := siClient.ApiAccountsIdServiceIdentityPost(ctx, accountID).Body(createRequest).Execute()
	if err != nil && res.StatusCode != status.StatusOK {
		return nil, fmt.Errorf("Error when calling `ApiAccountsIdServiceIdentityPost`: %v\n.Full HTTP response: %v", err, res)
	}
	log.InfoD("Successfully created ServiceIdentity with Name- %v", name)
	return siModels, nil
}

// GetServiceIdentityByID return service identity model.
func (si *ServiceIdentity) GetServiceIdentityByID(serviceIdentityID string) (*pds.ModelsServiceIdentity, error) {
	siClient := si.apiClient.ServiceIdentityApi
	ctx, err := GetContext()
	if err != nil {
		return nil, fmt.Errorf("Error in getting context for api call: %v\n", err)
	}
	siModel, res, err := siClient.ApiServiceIdentityIdGet(ctx, serviceIdentityID).Execute()
	if err != nil && res.StatusCode != status.StatusOK {
		return nil, fmt.Errorf("Error when calling `ApiServiceIdentityIdGet`: %v\n.Full HTTP response: %v", err, res)
	}
	return siModel, nil
}

// DeleteServiceIdentity delete service identity and return status.
func (si *ServiceIdentity) DeleteServiceIdentity(serviceIdentityID string) (*status.Response, error) {
	siClient := si.apiClient.ServiceIdentityApi
	ctx, err := GetContext()
	if err != nil {
		return nil, fmt.Errorf("Error in getting context for api call: %v\n", err)
	}
	res, err := siClient.ApiServiceIdentityIdDelete(ctx, serviceIdentityID).Execute()
	if err != nil && res.StatusCode != status.StatusOK {
		return nil, fmt.Errorf("Error when calling `ApiServiceIdentityIdDelete`: %v\n.Full HTTP response: %v", err, res)
	}
	return res, nil
}

// GenerateServiceIdentityToken generates new JWT token for a service Identity
func (si *ServiceIdentity) GenerateServiceIdentityToken(clientId string, clientToken string) (string, error) {
	siClient := si.apiClient.ServiceIdentityApi
	createRequest := pds.ControllersGenerateTokenRequest{ClientId: &clientId,
		ClientToken: &clientToken}
	ctx, err := GetContext()
	if err != nil {
		return "", fmt.Errorf("Error in getting context for api call: %v\n", err)
	}
	siModel, res, err := siClient.ServiceIdentityGenerateTokenPost(ctx).Body(createRequest).Execute()
	if err != nil && res.StatusCode != status.StatusOK {
		return "", fmt.Errorf("Error when calling `ServiceIdentityGenerateTokenPost`: %v\n.Full HTTP response: %v", err, res)
	}
	siToken := siModel.GetToken()
	log.InfoD("SiToken token is %v", SiToken)
	return siToken, nil
}

// GetServiceIdentityToken regenerates/ returns the created JWT token for the serviceIdentity
func (si *ServiceIdentity) GetServiceIdentityToken(siId string) (*pds.ModelsServiceIdentityWithToken, error) {
	siClient := si.apiClient.ServiceIdentityApi
	ctx, err := GetContext()
	if err != nil {
		return nil, fmt.Errorf("Error in getting context for api call: %v\n", err)
	}
	saModel, res, err := siClient.ApiServiceIdentityIdRegenerateGet(ctx, siId).Execute()
	if err != nil && res.StatusCode != status.StatusOK {
		return nil, fmt.Errorf("Error when calling `ApiServiceIdentityIdRegenerateGet`: %v\n.Full HTTP response: %v", err, res)
	}
	return saModel, nil
}

func (si *ServiceIdentity) CreateIAMRoleBindingsWithSi(actorID string, accountID string, nsRoles []pds.ModelsBinding, siToken string) (*pds.ModelsIAM, error) {

	iamRB = &IamRoleBindings{
		apiClient: si.apiClient,
	}
	iamModels, err := iamRB.CreateIamRoleBinding(accountID, actorID, nsRoles)
	if err != nil {
		return nil, fmt.Errorf("error generating service identity token for serviceId- %v", iamModels.Id)
	}
	return iamModels, nil
}

func (si *ServiceIdentity) CreateAndGetServiceIdentityToken(accountID string) (string, string, error) {
	siName := "sin" + strconv.Itoa(rand.Int())
	siModels, err := si.CreateServiceIdentity(accountID, siName)
	if err != nil {
		return "", "", fmt.Errorf("error generating service identity token for serviceId- %v", siModels.Name)
	}
	clientID := *siModels.ClientId
	clientToken := *siModels.ClientToken
	siToken, err := si.GenerateServiceIdentityToken(clientID, clientToken)
	if err != nil {
		return "", "", fmt.Errorf("error generating service identity token for serviceId- %v", siModels.Name)
	}
	actorId := *siModels.Id
	return actorId, siToken, nil
}

func (si *ServiceIdentity) ReturnServiceIdToken() string {
	return SiToken
}
func (si *ServiceIdentity) GenerateServiceTokenAndSetAuthContext(actorId string) (string, error) {

	tokenModel, err := si.GetServiceIdentityToken(actorId)
	if err != nil {
		return "", fmt.Errorf("error while regenarting and fetching clientid and client token %v", actorId)
	}
	newClient := tokenModel.GetClientId()
	newClientToken := tokenModel.GetClientToken()
	regeneratedToken, err := si.GenerateServiceIdentityToken(newClient, newClientToken)
	if err != nil {
		return "", fmt.Errorf("error while regenarting and fetching new Si token %v", actorId)
	}
	SiToken = regeneratedToken
	log.InfoD("Successfully Generated the SiToken- [%v] for ActorId- [%v]", SiToken, actorId)
	return regeneratedToken, nil
}
