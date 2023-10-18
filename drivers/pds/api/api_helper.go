package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/onsi/ginkgo"
	"github.com/portworx/torpedo/drivers/pds/parameters"
	"github.com/portworx/torpedo/pkg/log"
	"io/ioutil"
	"os"
	"strings"

	"net/http"

	"net/url"

	pds "github.com/portworx/pds-api-go-client/pds/v1alpha1"
)

const (
	// Control plane environment variables
	envControlPlaneURL = "CONTROL_PLANE_URL"
	envUsername        = "PDS_USERNAME"
	envPassword        = "PDS_PASSWORD"
	envPDSClientSecret = "PDS_CLIENT_SECRET"
	envPDSClientID     = "PDS_CLIENT_ID"
	envPDSISSUERURL    = "PDS_ISSUER_URL"
)

// BearerToken struct
type BearerToken struct {
	AccessToken  string `json:"access_token"`
	IDToken      string `json:"id_token"`
	TokenType    string `json:"token_type"`
	ExpiresIn    uint64 `json:"expires_in"`
	RefreshToken string `json:"refresh_token"`
}

var (
	customParams *parameters.Customparams
	siID         *ServiceIdentity
)

// GetContext return context for api call.
func GetContext() (context.Context, error) {
	var token string
	currentTestDescription := ginkgo.CurrentGinkgoTestDescription()
	testName := strings.Split(currentTestDescription.FullTestText, " ")[0]
	serviceIdFlag := customParams.ReturnServiceIdentityFlag()
	PDSControlPlaneURL := os.Getenv("CONTROL_PLANE_URL")
	endpointURL, err := url.Parse(PDSControlPlaneURL)
	if err != nil {
		return nil, fmt.Errorf("Unable to connect to ControlPlane URL: %v\n", err)
	}
	apiConf := pds.NewConfiguration()
	apiConf.Host = endpointURL.Host
	apiConf.Scheme = endpointURL.Scheme
	if serviceIdFlag == true && strings.Contains(testName, "ServiceIdentity") {
		serviceIdToken := siID.ReturnServiceIdToken()
		if serviceIdToken == "" {
			token, err = getBearerToken()
		} else {
			token = serviceIdToken
			log.InfoD("ServiceIdentity Token being used")
		}

	} else {
		token, err = getBearerToken()
		if err != nil {
			return nil, err
		}
	}
	ctx := context.WithValue(context.Background(), pds.ContextAPIKeys, map[string]pds.APIKey{"ApiKeyAuth": {Key: token, Prefix: "Bearer"}})
	return ctx, nil
}

func getBearerToken() (string, error) {
	username := os.Getenv(envUsername)
	password := os.Getenv(envPassword)
	clientID := os.Getenv(envPDSClientID)
	clientSecret := os.Getenv(envPDSClientSecret)
	issuerURL := os.Getenv(envPDSISSUERURL)
	url := fmt.Sprintf("%s/protocol/openid-connect/token", issuerURL)
	grantType := "password"

	postBody, err := json.Marshal(map[string]string{
		"grant_type":    grantType,
		"client_id":     clientID,
		"client_secret": clientSecret,
		"username":      username,
		"password":      password,
	})
	if err != nil {
		return "", err
	}

	requestBody := bytes.NewBuffer(postBody)
	resp, err := http.Post(url, "application/json", requestBody)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	//Read the response body
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	var bearerToken = new(BearerToken)
	err = json.Unmarshal(body, &bearerToken)
	if err != nil {
		return "", err
	}

	return bearerToken.AccessToken, nil

}
