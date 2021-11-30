package backup

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	k8s "github.com/portworx/sched-ops/k8s/core"
	"github.com/portworx/sched-ops/task"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/metadata"
)

// PxCentralAdminPwd password of PxCentralAdminUser
var PxCentralAdminPwd string

const (
	// PxCentralAdminUser px central admin
	PxCentralAdminUser = "px-central-admin"
	// PxCentralAdminSecretName secret for PxCentralAdminUser
	PxCentralAdminSecretName = "px-central-admin"
	// PxCentralAdminSecretNamespace namespace of PxCentralAdminSecretName
	PxCentralAdminSecretNamespace = "px-backup"
	// keycloakEndPoint Endpoint for keycloak
	keycloakEndPoint = "pxcentral-keycloak-http:80"
	/// httpTimeout timeout for http request
	httpTimeout = 1 * time.Minute
)

const (
	// AuthTokenType of incoming auth token
	AuthTokenType = "bearer"
	// AuthHeader incoming auth request
	AuthHeader = "authorization"
	// OrgToken key
	OrgToken = "PX_BACKUP_ORG_TOKEN"
	// AdminTokenSecretName which has admin user jwt token information
	AdminTokenSecretName = "px-backup-admin-secret"
	// AdminTokenSecretNamespace which has admin user jwt token information
	AdminTokenSecretNamespace = "px-backup"

	defaultWaitTimeout  time.Duration = 30 * time.Second
	defaultWaitInterval time.Duration = 5 * time.Second
	// pxBackupNamespace where px backup is running
	pxBackupNamespace = "PX_BACKUP_NAMESPACE"
)

type tokenResponse struct {
	AccessToken string `json:"access_token"`
}

// Doc ref: https://www.keycloak.org/docs-api/5.0/rest-api/index.html#_rolerepresentation
// Not all the fields are used and below is sample response obtained from keycloak
// {
//        "id": "12bfd2ee-bd3d-4260-809b-c288669ed5b1",
//        "name": "px-backup-app.user",
//        "description": "Portworx px-backup-app.user user role",
//        "composite": false,
//        "clientRole": false,
//        "containerId": "master"
//    },

// KeycloakRoleRepresentation role repsetaton struct
type KeycloakRoleRepresentation struct {
	ID          string                       `json:"id"`
	Name        string                       `json:"name"`
	Description string                       `json:"description"`
	Composite   bool                         `json:"composite"`
	ClientRole  bool                         `json:"clientRole"`
	ContainerID string                       `json:"containerId"`
	Attributes  map[string]string            `json:"attributes"`
	Composites  RoleRespresentationComposite `json:"composites"`
}

// RoleRespresentationComposite composite role rep
type RoleRespresentationComposite struct {
	Client map[string]string `json:"client"`
	Realm  []string          `json:"realm"`
}

// KeycloakUserRepresentation user representation
type KeycloakUserRepresentation struct {
	ID            string                    `json:"id"`
	Name          string                    `json:"username"`
	FirstName     string                    `json:"firstName"`
	LastName      string                    `json:"lastName"`
	EmailVerified bool                      `json:"emailVerified"`
	Enabled       bool                      `json:"enabled"`
	Email         string                    `json:"email"`
	Credentials   []KeycloakUserCredentials `json:"credentials"`
}

// KeycloakUserCredentials user credentials
type KeycloakUserCredentials struct {
	// Type is "password"
	Type string `json:"type"`
	// Temporary is the password temporary
	Temporary bool `json:"temporary"`
	// Value password for the user
	Value string `json:"value"`
}

// KeycloakGroupRepresentation group representation
type KeycloakGroupRepresentation struct {
	Name      string   `json:"name"`
	ID        string   `json:"id"`
	Path      string   `json:"path"`
	SubGroups []string `json:"subGroups"`
}

// KeycloakGroupAdd adding group
type KeycloakGroupAdd struct {
	Name string `json:"name"`
}

// KeycloakGroupToUser representation of adding group to user
type KeycloakGroupToUser struct {
	UserID  string `json:"userId"`
	GroupID string `json:"groupId"`
	Realm   string `json:"realm"`
}

// GetPxBackupNamespace returns namespace of px-backup deployment.
func GetPxBackupNamespace() string {
	ns := os.Getenv(pxBackupNamespace)
	if ns == "" {
		return AdminTokenSecretNamespace
	}
	return ns
}

// GetToken fetches JWT token for given user credentials
func GetToken(userName, password string) (string, error) {
	fn := "GetToken"
	values := make(url.Values)
	values.Set("client_id", "pxcentral")
	values.Set("username", userName)
	values.Set("password", password)
	values.Set("grant_type", "password")
	values.Set("token-duration", "365d")
	reqURL := fmt.Sprintf("http://%s/auth/realms/master/protocol/openid-connect/token", keycloakEndPoint)
	method := "POST"
	headers := make(http.Header)
	headers.Add("Content-Type", "application/x-www-form-urlencoded")
	response, err := processHTTPRequest(method, reqURL, headers, strings.NewReader(values.Encode()))
	if err != nil {
		logrus.Errorf("%s: %v", fn, err)
		return "", err
	}

	token := &tokenResponse{}
	err = json.Unmarshal(response, &token)
	if err != nil {
		logrus.Errorf("%s: %v", fn, err)
		return "", err
	}

	return token.AccessToken, nil
}

// GetCommonHTTPHeaders populates http header
func GetCommonHTTPHeaders(userName, password string) (http.Header, error) {
	fn := "GetCommonHTTPHeaders"
	token, err := GetToken(userName, password)
	if err != nil {
		logrus.Errorf("%s: %v", fn, err)
		return nil, err
	}
	headers := make(http.Header)
	headers.Add("Authorization", fmt.Sprintf("Bearer %v", token))
	headers.Add("Content-Type", "application/json")

	return headers, nil
}

// GetPxCentralAdminPwd fetches password from PxCentralAdminUser from secret
func GetPxCentralAdminPwd() (string, error) {

	secret, err := k8s.Instance().GetSecret(PxCentralAdminSecretName, GetPxBackupNamespace())
	if err != nil {
		return "", err
	}

	PxCentralAdminPwd := string(secret.Data["credential"])
	if PxCentralAdminPwd == "" {
		return "", fmt.Errorf("px-central-admin secret is empty")
	}

	return PxCentralAdminPwd, nil
}

// GetAllRoles lists all the available role in keycloak
func GetAllRoles() ([]KeycloakRoleRepresentation, error) {
	fn := "GetAllRoles"
	headers, err := GetCommonHTTPHeaders(PxCentralAdminUser, PxCentralAdminPwd)
	if err != nil {
		logrus.Errorf("%s: %v", fn, err)
		return nil, err
	}
	reqURL := fmt.Sprintf("http://%s/auth/admin/realms/master/roles", keycloakEndPoint)
	method := "GET"
	response, err := processHTTPRequest(method, reqURL, headers, nil)
	if err != nil {
		logrus.Errorf("%s: %v", fn, err)
		return nil, err
	}
	var roles []KeycloakRoleRepresentation
	err = json.Unmarshal(response, &roles)
	if err != nil {
		logrus.Errorf("%s: %v", fn, err)
		return nil, err
	}

	return roles, nil
}

// GetRoleID gets role ID for a given role
func GetRoleID(role string) (string, error) {
	fn := "GetRoleID"
	// Fetch all roles
	roles, err := GetAllRoles()
	if err != nil {
		logrus.Errorf("%s: %v", fn, err)
		return "", err
	}
	// Now fetch the current role ID
	var clientID string
	for _, r := range roles {
		if r.Name == role {
			clientID = r.ID
			break
		}
	}

	return clientID, nil
}

// GetUserRole fetches roles for a given user
func GetUserRole(userName string) error {
	fn := "GetUserRole"
	// First fetch all users to get the client id for the client
	headers, err := GetCommonHTTPHeaders(PxCentralAdminUser, PxCentralAdminPwd)
	if err != nil {
		logrus.Errorf("%s: %v", fn, err)
		return err
	}

	reqURL := fmt.Sprintf("http://%s/auth/admin/realms/master/users", keycloakEndPoint)
	method := "GET"
	response, err := processHTTPRequest(method, reqURL, headers, nil)
	if err != nil {
		return err
	}
	var users []KeycloakUserRepresentation
	err = json.Unmarshal(response, &users)
	if err != nil {
		logrus.Errorf("%s: %v", fn, err)
		return err
	}

	var clientID string
	for _, user := range users {
		if user.Name == userName {
			clientID = user.ID
			break
		}
	}
	// Now fetch all the roles for the fetched client ID
	reqURL = fmt.Sprintf("http://%s/auth/admin/realms/master/users/%s/role-mappings/realm", keycloakEndPoint, clientID)
	method = "GET"
	response, err = processHTTPRequest(method, reqURL, headers, nil)
	if err != nil {
		return err
	}
	var r []KeycloakRoleRepresentation
	err = json.Unmarshal(response, &r)
	if err != nil {
		logrus.Errorf("%s: %v", fn, err)
		return err
	}

	return nil
}

// FetchIDOfUser fetches ID for a given user
func FetchIDOfUser(userName string) (string, error) {
	fn := "FetchIDOfUser"
	// First fetch all users to get the client id for the client
	headers, err := GetCommonHTTPHeaders(PxCentralAdminUser, PxCentralAdminPwd)
	if err != nil {
		logrus.Errorf("%s: %v", fn, err)
		return "", err
	}
	reqURL := fmt.Sprintf("http://%s/auth/admin/realms/master/users", keycloakEndPoint)
	method := "GET"
	response, err := processHTTPRequest(method, reqURL, headers, nil)
	if err != nil {
		logrus.Errorf("%s: %v", fn, err)
		return "", err
	}
	var users []KeycloakUserRepresentation
	err = json.Unmarshal(response, &users)
	if err != nil {
		logrus.Errorf("%s: %v", fn, err)
		return "", err
	}

	var clientID string
	for _, user := range users {
		if user.Name == userName {
			clientID = user.ID
			break
		}
	}

	return clientID, nil
}

// AddRoleToUser assigning a given role to a existing user
func AddRoleToUser(userName string, role string, description string) error {
	fn := "AddRoleToUser"
	// First fetch the client ID of the user
	clientID, err := FetchIDOfUser(userName)
	if err != nil {
		logrus.Errorf("%s: %v", fn, err)
		return err
	}
	// Fetch the role ID
	roleID, err := GetRoleID(role)
	if err != nil {
		logrus.Errorf("%s: %v", fn, err)
		return err
	}

	// Frame the role struct to be assigned
	var kRoles []KeycloakRoleRepresentation
	kRole := KeycloakRoleRepresentation{
		ID:          roleID,
		ClientRole:  false,
		Composite:   false,
		ContainerID: "master",
		Description: description,
		Name:        role,
	}
	kRoles = append(kRoles, kRole)
	roleBytes, err := json.Marshal(&kRoles)
	if err != nil {
		logrus.Errorf("%s: %v", fn, err)
		return err
	}
	reqURL := fmt.Sprintf("http://%s/auth/admin/realms/master/users/%s/role-mappings/realm", keycloakEndPoint, clientID)
	method := "POST"
	headers, err := GetCommonHTTPHeaders(PxCentralAdminUser, PxCentralAdminPwd)
	if err != nil {
		logrus.Errorf("%s: %v", fn, err)
		return err
	}
	_, err = processHTTPRequest(method, reqURL, headers, strings.NewReader(string(roleBytes)))
	if err != nil {
		logrus.Errorf("%s: %v", fn, err)
		return err
	}

	return nil
}

// DeleteRoleFromUser deleting role from a user
func DeleteRoleFromUser(userName string, role string, description string) error {
	fn := "DeleteRoleFromUser"
	// First fetch the user ID of the user
	clientID, err := FetchIDOfUser(userName)
	if err != nil {
		logrus.Errorf("%s: %v", fn, err)
		return err
	}
	// Fetch the role ID
	roleID, err := GetRoleID(role)
	if err != nil {
		logrus.Errorf("%s: %v", fn, err)
		return err
	}

	// Frame the role struct to be assigned
	var kRoles []KeycloakRoleRepresentation
	kRole := KeycloakRoleRepresentation{
		ID:          roleID,
		ClientRole:  false,
		Composite:   false,
		ContainerID: "master",
		Description: description,
		Name:        role,
	}
	kRoles = append(kRoles, kRole)
	roleBytes, err := json.Marshal(&kRoles)
	if err != nil {
		logrus.Errorf("%s: %v", fn, err)
		return err
	}
	reqURL := fmt.Sprintf("http://%s/auth/admin/realms/master/users/%s/role-mappings/realm", keycloakEndPoint, clientID)
	method := "DELETE"
	headers, err := GetCommonHTTPHeaders(PxCentralAdminUser, PxCentralAdminPwd)
	if err != nil {
		return err
	}
	_, err = processHTTPRequest(method, reqURL, headers, strings.NewReader(string(roleBytes)))
	if err != nil {
		logrus.Errorf("%s: %v", fn, err)
		return err
	}

	return nil
}

// AddUser adds a new user
func AddUser(userName, firstName, lastName, email, password string) error {
	fn := "AddUser"
	reqURL := fmt.Sprintf("http://%s/auth/admin/realms/master/users", keycloakEndPoint)
	method := "POST"
	headers, err := GetCommonHTTPHeaders(PxCentralAdminUser, PxCentralAdminPwd)
	if err != nil {
		logrus.Errorf("%s: %v", fn, err)
		return err
	}
	userRep := KeycloakUserRepresentation{
		Name:      userName,
		FirstName: firstName,
		LastName:  lastName,
		Email:     email,
		Enabled:   true,
		Credentials: []KeycloakUserCredentials{
			{
				Type:      "password",
				Temporary: false,
				Value:     password,
			},
		},
	}
	userBytes, err := json.Marshal(&userRep)
	if err != nil {
		logrus.Errorf("%s: %v", fn, err)
		return err
	}
	_, err = processHTTPRequest(method, reqURL, headers, strings.NewReader(string(userBytes)))
	if err != nil {
		logrus.Errorf("%s: %v", fn, err)
		return err
	}

	return nil
}

// GetPxCentralAdminToken gets token for "px-central-admin"
func GetPxCentralAdminToken() (string, error) {
	token, err := GetToken(PxCentralAdminUser, PxCentralAdminPwd)
	if err != nil {
		return "", err
	}

	return token, nil
}

// GetCtxWithToken getx ctx with passed token
func GetCtxWithToken(token string) context.Context {
	ctx := context.Background()
	md := metadata.New(map[string]string{
		AuthHeader: AuthTokenType + " " + token,
	})
	ctx = metadata.NewOutgoingContext(ctx, md)

	return ctx
}

// GetPxCentralAdminCtx fetch px-central-admin context
func GetPxCentralAdminCtx() (context.Context, error) {
	token, err := GetPxCentralAdminToken()
	if err != nil {
		return nil, err
	}

	ctx := GetCtxWithToken(token)

	return ctx, nil
}

// UpdatePxBackupAdminSecret updating "px-backup-admin-secret" token with
// "px-central-admin" token
func UpdatePxBackupAdminSecret() error {
	pxCentralAdminToken, err := GetPxCentralAdminToken()
	if err != nil {
		return err
	}

	secret, err := k8s.Instance().GetSecret(AdminTokenSecretName, AdminTokenSecretNamespace)
	if err != nil {
		return err
	}
	// Now update the token into "AdminTokenSecretName"
	secret.Data[OrgToken] = ([]byte(pxCentralAdminToken))
	_, err = k8s.Instance().UpdateSecret(secret)
	if err != nil {
		return err
	}

	return nil
}

// GetAdminCtxFromSecret with provided name and namespace
func GetAdminCtxFromSecret() (context.Context, error) {
	err := UpdatePxBackupAdminSecret()
	if err != nil {
		return nil, err
	}

	secret, err := k8s.Instance().GetSecret(AdminTokenSecretName, AdminTokenSecretNamespace)
	if err != nil {
		return nil, err
	}

	token := string(secret.Data[OrgToken])
	if token == "" {
		return nil, fmt.Errorf("admin token is empty")
	}
	ctx := GetCtxWithToken(token)

	return ctx, nil
}

// GetAllGroups fetches all available groups
func GetAllGroups() ([]KeycloakGroupRepresentation, error) {
	fn := "GetAllGroups"
	headers, err := GetCommonHTTPHeaders(PxCentralAdminUser, PxCentralAdminPwd)
	if err != nil {
		logrus.Errorf("%s: %v", fn, err)
		return nil, err
	}
	reqURL := fmt.Sprintf("http://%s/auth/admin/realms/master/groups", keycloakEndPoint)
	method := "GET"
	response, err := processHTTPRequest(method, reqURL, headers, nil)
	if err != nil {
		logrus.Errorf("%s: %v", fn, err)
		return nil, err
	}
	var groups []KeycloakGroupRepresentation
	err = json.Unmarshal(response, &groups)
	if err != nil {
		logrus.Errorf("%s: %v", fn, err)
		return nil, err
	}
	logrus.Debugf("list of groups : %v", groups)
	return groups, nil
}

// AddGroup adds a new group
func AddGroup(group string) error {
	fn := "AddGroup"
	reqURL := fmt.Sprintf("http://%s/auth/admin/realms/master/groups", keycloakEndPoint)
	method := "POST"
	headers, err := GetCommonHTTPHeaders(PxCentralAdminUser, PxCentralAdminPwd)
	if err != nil {
		logrus.Errorf("%s: %v", fn, err)
		return err
	}
	groups := KeycloakGroupAdd{
		Name: group,
	}

	userBytes, err := json.Marshal(&groups)
	if err != nil {
		logrus.Errorf("%s: %v", fn, err)
		return err
	}
	_, err = processHTTPRequest(method, reqURL, headers, strings.NewReader(string(userBytes)))
	if err != nil {
		logrus.Errorf("%s: %v", fn, err)
		return err
	}

	return nil
}

// AddGroupToUser add group to a user
func AddGroupToUser(user, group string) error {
	fn := "AddGroupToUser"
	groupID, err := FetchIDOfGroup(group)
	if err != nil {
		return err
	}

	userID, err := FetchIDOfUser(user)
	if err != nil {
		return err
	}

	reqURL := fmt.Sprintf("http://%s/auth/admin/realms/master/users/%s/groups/%s", keycloakEndPoint, userID, groupID)
	method := "PUT"
	headers, err := GetCommonHTTPHeaders(PxCentralAdminUser, PxCentralAdminPwd)
	if err != nil {
		logrus.Errorf("%s: %v", fn, err)
		return err
	}
	data := KeycloakGroupToUser{
		UserID:  userID,
		GroupID: groupID,
		Realm:   "master",
	}

	dataBytes, err := json.Marshal(&data)
	if err != nil {
		logrus.Errorf("%s: %v", fn, err)
		return err
	}
	_, err = processHTTPRequest(method, reqURL, headers, strings.NewReader(string(dataBytes)))
	if err != nil {
		logrus.Errorf("%s: %v", fn, err)
		return err
	}

	return nil
}

// FetchIDOfGroup fetched ID of a group
func FetchIDOfGroup(name string) (string, error) {
	groups, err := GetAllGroups()
	if err != nil {
		return "", nil
	}

	var groupID string
	for _, group := range groups {
		if group.Name == name {
			groupID = group.ID
			break
		}
	}

	return groupID, nil
}

// FetchUserDetailsFromID fetches user name and email ID
func FetchUserDetailsFromID(userID string) (string, string, error) {
	fn := "FetchUserDetailsFromID"

	// First fetch all users to get the client id for the client
	headers, err := GetCommonHTTPHeaders(PxCentralAdminUser, PxCentralAdminPwd)
	if err != nil {
		logrus.Errorf("%s: %v", fn, err)
		return "", "", err
	}
	var userName string
	var email string
	f := func() (interface{}, bool, error) {
		reqURL := fmt.Sprintf("http://%s/auth/admin/realms/master/users", keycloakEndPoint)
		method := "GET"
		response, err := processHTTPRequest(method, reqURL, headers, nil)
		if err != nil {
			logrus.Errorf("%s: %v", fn, err)
			return nil, true, err
		}
		var users []KeycloakUserRepresentation
		err = json.Unmarshal(response, &users)
		if err != nil {
			logrus.Errorf("%s: %v", fn, err)
			return nil, true, err
		}

		for _, user := range users {
			if user.ID == userID {
				userName = user.Name
				email = user.Email
				break
			}
		}
		if userName == "" || email == "" {
			// In some case there might be no error but we got empty user name/email, retry again
			return nil, true, fmt.Errorf("got emptry user/email, retrying again")
		}

		return nil, false, nil
	}

	_, err = task.DoRetryWithTimeout(f, defaultWaitTimeout, defaultWaitInterval)
	if err != nil {
		return "", "", fmt.Errorf("failed to fetch user name/email: [%v]", err)
	}

	return userName, email, nil
}

func processHTTPRequest(
	method string,
	url string,
	headers http.Header,
	body io.Reader,
) ([]byte, error) {
	httpRequest, err := http.NewRequest(method, url, body)
	if err != nil {
		return nil, err
	}
	httpRequest.Header = headers
	client := &http.Client{
		Timeout: httpTimeout,
	}
	httpResponse, err := client.Do(httpRequest)
	if err != nil {
		return nil, err
	}
	defer func() {
		err := httpResponse.Body.Close()
		if err != nil {
			logrus.Debugf("Error closing http response body: %v", err)
		}
	}()

	return ioutil.ReadAll(httpResponse.Body)
}
