package flasharray

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/portworx/torpedo/pkg/log"
	"io/ioutil"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"strings"
	"time"
)

// supportedRestVersions is used to negotiate the API version to use
var supportedRestVersions = [...]string{"2.2", "2.3", "2.4", "2.5", "2.6", "2.7", "2.8"}

type Client struct {
	MgmtIp      string
	ApiToken    string
	UserName    string
	Password    string
	RestVersion string
	UserAgent   string
	AuthToken   string
	Kwargs      map[string]string

	// Client object defined here
	client  *http.Client
	Volumes *VolumeServices
}

// Type supported is used for retrieving the support API versions from the Flash Array
type supported struct {
	Versions []string `json:"version"`
}

// Type auth is used to for the API token used in API authentication
type ApiToken struct {
	Token string `json:"api_token,omitempty"`
}

func (c *Client) CreateClientInstance() {
	// Create Client Interface
	cookieJar, _ := cookiejar.New(nil)
	transport := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	c.client = &http.Client{Transport: transport, Jar: cookieJar}
}

func NewClient(mgmtIp string, apiToken string, userName string, password string,
	restVersion string, verifyHTTPS bool, sslCert bool,
	userAgent string, kwargs map[string]string) (*Client, error) {

	err := checkAuth(apiToken, userName, password)
	if err != nil {
		return nil, err
	}

	c := &Client{MgmtIp: mgmtIp,
		ApiToken:    apiToken,
		UserName:    userName,
		Password:    password,
		RestVersion: restVersion,
		UserAgent:   userAgent}

	requestKwargs := setDefaultRequestKwargs(kwargs, verifyHTTPS, sslCert)
	c.Kwargs = requestKwargs

	authToken, err := c.getAuthToken(restVersion)
	if err != nil {
		return nil, err
	}
	c.AuthToken = authToken
	// Create Client Instance
	c.CreateClientInstance()

	// Authenticate to the API and store the session
	err = c.login()
	if err != nil {
		return nil, err
	}

	// Initialize services
	c.InitializeServices()

	return c, err

}

// checkAuth validates
func checkAuth(apiToken string, username string, password string) error {

	if apiToken == "" && (username == "" && password == "") {
		err := errors.New("[error] Must specify API token or both username and password")
		return err
	}

	if apiToken != "" && (username != "" && password != "") {
		err := errors.New("specify only API token or both username and password")
		return err
	}

	return nil
}

// Authenticate to the API and store the session
func (c *Client) login() error {
	authURL := c.formatPath("auth/session", true)
	data := map[string]string{"api_token": c.ApiToken}
	jsonValue, _ := json.Marshal(data)
	_, err := c.client.Post(authURL, "application/json", bytes.NewBuffer(jsonValue))
	if err != nil {
		return err
	}
	return nil
}

func (c *Client) Do(req *http.Request, v interface{}) (*http.Response, error) {
	log.Infof("\nRequest [%v]\n", req)
	resp, err := c.client.Do(req)
	if err != nil {
		log.Infof("Do request failed")
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("error getting auth-token,response status is [%d]", resp.StatusCode)

	}
	if err := validateResponse(resp); err != nil {
		return resp, err
	}

	bodyBytes, _ := ioutil.ReadAll(resp.Body)
	bodyString := string(bodyBytes)
	err = json.Unmarshal([]byte(fmt.Sprintf("[%v]", bodyString)), v)

	if err != nil {
		return nil, err
	}
	return resp, nil

}

func (c *Client) NewRequest(method string, path string, params map[string]string, data interface{}) (*http.Request, error) {

	var fpath string
	if strings.HasPrefix(path, "http") {
		fpath = path
	} else {
		fpath = c.formatPath(path, false)
	}
	bodyReader := bytes.NewReader([]byte{})
	baseURL, err := url.Parse(fpath)
	if err != nil {
		return nil, err
	}
	if params != nil {
		ps := url.Values{}
		for k, v := range params {
			log.Infof("[DEBUG] key: %s, value: %s \n", k, v)
			ps.Set(k, v)
		}
		baseURL.RawQuery = ps.Encode()
	}

	log.Infof("Base URL [%v]", baseURL.String())

	req, err := http.NewRequest(method, baseURL.String(), bodyReader)
	if err != nil {
		return nil, err
	}
	if data != nil {
		jsonString, err := json.Marshal(data)
		if err != nil {
			return nil, err
		}
		req, err = http.NewRequest(method, baseURL.String(), bytes.NewBuffer(jsonString))
		if err != nil {
			return nil, err
		}
	}

	log.Infof("Adding auth token [%v]", c.AuthToken)
	req.Header.Add("x-auth-token", fmt.Sprintf("%v", c.AuthToken))
	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("Accept", "application/json")
	if c.UserAgent != "" {
		req.Header.Add("User-Agent", c.UserAgent)
	}

	return req, err
}

func (c *Client) NewGetRequests(path string, params map[string]string, data interface{}) (*http.Request, error) {
	method := "GET"
	httpRequest, err := c.NewRequest(method, path, params, data)
	if err != nil {
		return nil, err
	}
	return httpRequest, nil
}

func (c *Client) getAuthToken(restVersion string) (string, error) {

	authURL, err := url.Parse(c.formatPath(fmt.Sprintf("api/%v/login", restVersion), true))
	if err != nil {
		return "", err
	}
	log.Infof("Auth URL [%v]", authURL.String())

	bodyReader := bytes.NewReader([]byte{})
	request, err := http.NewRequest("POST", authURL.String(), bodyReader)
	if err != nil {
		return "", err
	}
	log.Infof("API Token [%v]", c.ApiToken)

	request.Header.Add("Content-Type", "application/json")
	request.Header.Add("api-token", c.ApiToken)

	log.Infof(fmt.Sprintf("API Token [%v]", c.ApiToken))

	tempClient := &http.Client{
		// http.Client doesn't set the default Timeout,
		// so it will be blocked forever without Timeout setting
		Timeout: time.Second * time.Duration(10),
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
	}
	httpResponse, err := tempClient.Do(request)
	if err != nil {
		return "", err
	}

	// Response processing
	defer httpResponse.Body.Close()
	if httpResponse.StatusCode != http.StatusOK {
		return "", fmt.Errorf("error getting auth-token,response status is [%d]", httpResponse.StatusCode)

	}
	_, err = ioutil.ReadAll(httpResponse.Body)
	if err != nil {
		return "", err
	}

	authToken := httpResponse.Header.Get("x-auth-token")
	log.Infof("Login Success!")
	return authToken, nil

}

// formatPath returns the formated string to be used for the base URL in
// all API calls
func (c *Client) formatPath(path string, ignoreRestVersion bool) string {
	formatPath := ""
	if ignoreRestVersion {
		formatPath = fmt.Sprintf("https://%s/%s", c.MgmtIp, path)
	} else {
		formatPath = fmt.Sprintf("https://%s/api/%s/%s", c.MgmtIp, c.RestVersion, path)
	}
	log.Infof(formatPath)
	return formatPath
}

// getJSON is just a helper function that creates and retrieves information
// from the Flash Array before the actual session is established.
// Right now, its just grabbing the supported API versions.  I should
// probably find a more graceful way to accomplish this.
func getJSON(uri string, target interface{}) error {
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	var c = &http.Client{Timeout: 10 * time.Second, Transport: tr}
	r, err := c.Get(uri)
	if err != nil {
		return err
	}
	defer r.Body.Close()

	return json.NewDecoder(r.Body).Decode(target)
}

func (c *Client) InitializeServices() *Client {

	// Initialize all services created here
	c.Volumes = &VolumeServices{client: c}
	return c
}

// validateResponse checks that the http response is within the 200 range.
// Some functionality needs to be added here to check for some specific errors,
// and probably add the equivlents to PureError and PureHTTPError from the Python
// REST client.
func validateResponse(r *http.Response) error {
	if c := r.StatusCode; 200 <= c && c <= 299 {
		return nil
	}

	bodyBytes, _ := ioutil.ReadAll(r.Body)
	bodyString := string(bodyBytes)
	return fmt.Errorf("Response code: %d, ResponeBody: %s", r.StatusCode, bodyString)
}

// setDefaultRequestKwargs sets default request kwargs if not provided.
func setDefaultRequestKwargs(requestKwargs map[string]string, verifyHTTPS, sslCert bool) map[string]string {
	if requestKwargs == nil {
		requestKwargs = make(map[string]string)
	}

	if _, ok := requestKwargs["verify"]; !ok {
		if sslCert && verifyHTTPS {
			requestKwargs["verify"] = "false"
		} else {
			requestKwargs["verify"] = "true"
		}
	}
	return requestKwargs
}

// getRestVersion retrieves and verifies the REST API version.
func getRestVersion(restVersion string, target string) (string, error) {
	if restVersion != "" {
		if err := checkRestVersion(restVersion, target); err != nil {
			return "", err
		}
	} else {
		r, err := chooseRestVersion(target)
		if err != nil {
			return "", err
		}
		restVersion = r
	}
	log.Infof("Selected rest Version is [%v]", restVersion)
	return restVersion, nil
}

// checkRestVersion checks if the specified REST API version is supported by the FlashArray and the library.
func checkRestVersion(version string, target string) error {
	// Construct the URL for checking supported API versions
	checkURL := fmt.Sprintf("https://%s/api/api_version", target)
	log.Infof(fmt.Sprintf("URL is [%v]", checkURL))
	// Retrieve supported API versions from the FlashArray
	supported := &supported{}
	err := getJSON(checkURL, supported)
	if err != nil {
		return err
	}

	// Check if the specified version is supported by the FlashArray
	arraySupported := false
	for _, v := range supported.Versions {
		if version == v {
			arraySupported = true
			break
		}
	}
	if !arraySupported {
		return errors.New("[error] Array is incompatible with REST API version " + version)
	}

	// Check if the specified version is supported by the library
	librarySupported := false
	for _, v := range supportedRestVersions {
		if version == v {
			librarySupported = true
			break
		}
	}
	if !librarySupported {
		return errors.New("[error] Library is incompatible with REST API version " + version)
	}

	return nil
}

// chooseRestVersion negotiates the highest REST API version supported by the library and the FlashArray.
func chooseRestVersion(target string) (string, error) {
	// Construct the URL for checking supported API versions
	checkURL := fmt.Sprintf("https://%s/api/api_version", target)

	// Retrieve supported API versions from the FlashArray
	supported := &supported{}
	err := getJSON(checkURL, supported)
	if err != nil {
		return "", err
	}

	// Find the highest supported API version
	for i := len(supported.Versions) - 1; i >= 0; i-- {
		for _, version := range supportedRestVersions {
			if supported.Versions[i] == version {
				return version, nil
			}
		}
	}

	// If no compatible version found, return an error
	return "", errors.New("[error] Array is incompatible with all supported REST API versions")
}
