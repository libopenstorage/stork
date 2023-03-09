package pdsutils

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/portworx/torpedo/pkg/log"

	"net/http"

	"net/url"

	pds "github.com/portworx/pds-api-go-client/pds/v1alpha1"
)

// BearerToken struct
type BearerToken struct {
	AccessToken  string `json:"access_token"`
	IDToken      string `json:"id_token"`
	TokenType    string `json:"token_type"`
	ExpiresIn    uint64 `json:"expires_in"`
	RefreshToken string `json:"refresh_token"`
}

// GetContext return context for api call.
func GetContext() (context.Context, error) {
	envVars := MustHaveEnvVariables()

	endpointURL, err := url.Parse(envVars.PDSControlPlaneURL)
	if err != nil {
		log.Errorf("Unable to access the URL: %s", envVars.PDSControlPlaneURL)
		return nil, err
	}
	apiConf := pds.NewConfiguration()
	apiConf.Host = endpointURL.Host
	apiConf.Scheme = endpointURL.Scheme
	token, err := getBearerToken()
	if err != nil {
		return nil, err
	}
	ctx := context.WithValue(context.Background(), pds.ContextAPIKeys, map[string]pds.APIKey{"ApiKeyAuth": {Key: token, Prefix: "Bearer"}})
	return ctx, nil
}

// getBearerToken fetches the token.
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
