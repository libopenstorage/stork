// A REST client to interact with LINSTOR's REST API
// Copyright (C) LINBIT HA-Solutions GmbH
// All Rights Reserved.
// Author: Roland Kammerer <roland.kammerer@linbit.com>
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package client

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"

	"github.com/moul/http2curl"
	"golang.org/x/time/rate"
)

// Client is a struct representing a LINSTOR REST client.
type Client struct {
	httpClient *http.Client
	baseURL    *url.URL
	basicAuth  *BasicAuthCfg
	lim        *rate.Limiter
	log        interface{} // must be either Logger or LeveledLogger

	Nodes                  *NodeService
	ResourceDefinitions    *ResourceDefinitionService
	Resources              *ResourceService
	ResourceGroups         *ResourceGroupService
	StoragePoolDefinitions *StoragePoolDefinitionService
	Encryption             *EncryptionService
}

// Logger represents a standard logger interface
type Logger interface {
	Printf(string, ...interface{})
}

// LeveledLogger interface implements the basic methods that a logger library needs
type LeveledLogger interface {
	Errorf(string, ...interface{})
	Infof(string, ...interface{})
	Debugf(string, ...interface{})
	Warnf(string, ...interface{})
}

type BasicAuthCfg struct {
	Username, Password string
}

// const errors as in https://dave.cheney.net/2016/04/07/constant-errors
type clientError string

func (e clientError) Error() string { return string(e) }

const (
	// NotFoundError is the error type returned in case of a 404 error. This is required to test for this kind of error.
	NotFoundError = clientError("404 Not Found")
	// Name of the environment variable that stores the certificate used for TLS client authentication
	UserCertEnv = "LS_USER_CERTIFICATE"
	// Name of the environment variable that stores the key used for TLS client authentication
	UserKeyEnv = "LS_USER_KEY"
	// Name of the environment variable that stores the certificate authority for the LINSTOR HTTPS API
	RootCAEnv = "LS_ROOT_CA"
	// Name of the environment variable that holds the URL(s) of LINSTOR controllers
	ControllerUrlEnv = "LS_CONTROLLERS"
	// Name of the environment variable that holds the username for authentication
	UsernameEnv = "LS_USERNAME"
	// Name of the environment variable that holds the password for authentication
	PasswordEnv = "LS_PASSWORD"
)

// For example:
// u, _ := url.Parse("http://somehost:3370")
// c, _ := linstor.NewClient(linstor.BaseURL(u))

// Option configures a LINSTOR Client
type Option func(*Client) error

// BaseURL is a client's option to set the baseURL of the REST client.
func BaseURL(URL *url.URL) Option {
	return func(c *Client) error {
		c.baseURL = URL
		return nil
	}
}

// BasicAuth is a client's option to set username and password for the REST client.
func BasicAuth(basicauth *BasicAuthCfg) Option {
	return func(c *Client) error {
		c.basicAuth = basicauth
		return nil
	}
}

// HTTPClient is a client's option to set a specific http.Client.
func HTTPClient(httpClient *http.Client) Option {
	return func(c *Client) error {
		c.httpClient = httpClient
		return nil
	}
}

// Log is a client's option to set a Logger
func Log(logger interface{}) Option {
	return func(c *Client) error {
		switch logger.(type) {
		case Logger, LeveledLogger, nil:
			c.log = logger
		default:
			return errors.New("Invalid logger type, expected Logger or LeveledLogger")
		}
		return nil
	}
}

// Limit is the client's option to set number of requests per second and
// max number of bursts.
func Limit(r rate.Limit, b int) Option {
	return func(c *Client) error {
		if b == 0 && r != rate.Inf {
			return fmt.Errorf("invalid rate limit, burst must not be zero for non-unlimted rates")
		}
		c.lim = rate.NewLimiter(r, b)
		return nil
	}
}

// buildHttpClient constructs an HTTP client which will be used to connect to
// the LINSTOR controller. It recongnizes some environment variables which can
// be used to configure the HTTP client at runtime. If an invalid key or
// certificate is passed, an error is returned.
// If none or not all of the environment variables are passed, the default
// client is used as a fallback.
func buildHttpClient() (*http.Client, error) {
	certPEM, cert := os.LookupEnv(UserCertEnv)
	keyPEM, key := os.LookupEnv(UserKeyEnv)
	caPEM, ca := os.LookupEnv(RootCAEnv)

	if key != cert {
		return nil, fmt.Errorf("'%s', '%s': specify both or none", UserKeyEnv, UserCertEnv)
	}

	if !cert && !key && !ca {
		// Non of the special variables was set -> if TLS is used, default configuration can be used
		return http.DefaultClient, nil
	}

	tlsConfig := &tls.Config{}

	if ca {
		caPool := x509.NewCertPool()
		ok := caPool.AppendCertsFromPEM([]byte(caPEM))
		if !ok {
			return nil, fmt.Errorf("failed to get a valid certificate from '%s'", RootCAEnv)
		}
		tlsConfig.RootCAs = caPool
	}

	if key && cert {
		keyPair, err := tls.X509KeyPair([]byte(certPEM), []byte(keyPEM))
		if err != nil {
			return nil, fmt.Errorf("failed to load keys: %w", err)
		}
		tlsConfig.Certificates = append(tlsConfig.Certificates, keyPair)
	}

	return &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: tlsConfig,
		},
	}, nil
}

// Return the default scheme to access linstor
// If one of the HTTPS environment variables is set, will return "https".
// If not, will return "http"
func defaultScheme() string {
	_, ca := os.LookupEnv(RootCAEnv)
	_, cert := os.LookupEnv(UserCertEnv)
	_, key := os.LookupEnv(UserKeyEnv)
	if ca || cert || key {
		return "https"
	}
	return "http"
}

// Return the default port to access linstor.
// Defaults are:
// "https": 3371
// "http":  3370
func defaultPort(scheme string) string {
	if scheme == "https" {
		return "3371"
	}
	return "3370"
}

// NewClient takes an arbitrary number of options and returns a Client or an error.
// It recognizes several environment variables which can be used to configure
// the client at runtime:
//
// - LS_CONTROLLERS: a comma-separated list of LINSTOR controllers to connect to.
// Currently, golinstor will only use the first one.
//
// - LS_USERNAME, LS_PASSWORD: can be used to authenticate against the LINSTOR
// controller using HTTP basic authentication.
//
// - LS_USER_CERTIFICATE, LS_USER_KEY, LS_ROOT_CA: can be used to enable TLS on
// the HTTP client, enabling encrypted communication with the LINSTOR controller.
//
// Options passed to NewClient take precedence over options passed in via
// environment variables.
func NewClient(options ...Option) (*Client, error) {
	httpClient, err := buildHttpClient()
	if err != nil {
		return nil, fmt.Errorf("failed to build http client: %w", err)
	}

	controllers := os.Getenv(ControllerUrlEnv)
	if controllers == "" {
		controllers = "localhost"
	}

	// only use the first entry
	urlString := strings.Split(controllers, ",")[0]

	// Check scheme
	urlSplit := strings.Split(urlString, "://")

	if len(urlSplit) == 1 {
		urlSplit = []string{defaultScheme(), urlSplit[0]}
	}
	if len(urlSplit) != 2 {
		return nil, fmt.Errorf("URL with multiple scheme separators. parts: %v", urlSplit)
	}
	scheme, endpoint := urlSplit[0], urlSplit[1]
	if scheme == "linstor" {
		scheme = defaultScheme()
	}

	// Check port
	endpointSplit := strings.Split(endpoint, ":")
	if len(endpointSplit) == 1 {
		endpointSplit = []string{endpointSplit[0], defaultPort(scheme)}
	}
	if len(endpointSplit) != 2 {
		return nil, fmt.Errorf("URL with multiple port separators. parts: %v", endpointSplit)
	}
	host, port := endpointSplit[0], endpointSplit[1]

	baseURL, err := url.Parse(fmt.Sprintf("%s://%s:%s", scheme, host, port))
	if err != nil {
		return nil, err
	}

	c := &Client{
		httpClient: httpClient,
		baseURL:    baseURL,
		basicAuth: &BasicAuthCfg{
			Username: os.Getenv(UsernameEnv),
			Password: os.Getenv(PasswordEnv),
		},
		lim: rate.NewLimiter(rate.Inf, 0),
		log: log.New(os.Stdout, "", 0),
	}

	c.Nodes = &NodeService{client: c}
	c.ResourceDefinitions = &ResourceDefinitionService{client: c}
	c.Resources = &ResourceService{client: c}
	c.Encryption = &EncryptionService{client: c}
	c.ResourceGroups = &ResourceGroupService{client: c}
	c.StoragePoolDefinitions = &StoragePoolDefinitionService{client: c}

	for _, opt := range options {
		if err := opt(c); err != nil {
			return nil, err
		}
	}

	return c, nil
}

func (c *Client) newRequest(method, path string, body interface{}) (*http.Request, error) {
	rel, err := url.Parse(path)
	if err != nil {
		return nil, err
	}
	u := c.baseURL.ResolveReference(rel)

	var buf io.ReadWriter
	if body != nil {
		buf = new(bytes.Buffer)
		err := json.NewEncoder(buf).Encode(body)
		if err != nil {
			return nil, err
		}
		switch l := c.log.(type) {
		case LeveledLogger:
			l.Debugf("%s", buf)
		case Logger:
			l.Printf("[DEBUG] %s", body)
		}
	}

	req, err := http.NewRequest(method, u.String(), buf)
	if err != nil {
		return nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Accept", "application/json")
	// req.Header.Set("User-Agent", c.UserAgent)
	username := c.basicAuth.Username
	if username != "" {
		req.SetBasicAuth(username, c.basicAuth.Password)
	}

	return req, nil
}

func (c *Client) curlify(req *http.Request) (string, error) {
	cc, err := http2curl.GetCurlCommand(req)
	if err != nil {
		return "", err
	}
	return cc.String(), nil
}

func (c *Client) logCurlify(req *http.Request) {
	var msg string
	if curl, err := c.curlify(req); err != nil {
		msg = err.Error()
	} else {
		msg = curl
	}

	switch l := c.log.(type) {
	case LeveledLogger:
		l.Debugf("%s", msg)
	case Logger:
		l.Printf("[DEBUG] %s", msg)
	}
}

func (c *Client) do(ctx context.Context, req *http.Request, v interface{}) (*http.Response, error) {
	if err := c.lim.Wait(ctx); err != nil {
		return nil, err
	}
	req = req.WithContext(ctx)

	c.logCurlify(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 400 {
		msg := fmt.Sprintf("Status code not within 200 to 400, but %d (%s)\n",
			resp.StatusCode, http.StatusText(resp.StatusCode))
		switch l := c.log.(type) {
		case LeveledLogger:
			l.Debugf("%s", msg)
		case Logger:
			l.Printf("[DEBUG] %s", msg)
		}
		if resp.StatusCode == 404 {
			return nil, NotFoundError
		}

		var rets ApiCallError
		if err = json.NewDecoder(resp.Body).Decode(&rets); err != nil {
			return nil, err
		}
		return nil, rets
	}

	if v != nil {
		err = json.NewDecoder(resp.Body).Decode(v)
	}
	return resp, err
}

// Higer Leve Abstractions

func (c *Client) doGET(ctx context.Context, url string, ret interface{}, opts ...*ListOpts) (*http.Response, error) {

	u, err := addOptions(url, genOptions(opts...))
	if err != nil {
		return nil, err
	}

	req, err := c.newRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}
	return c.do(ctx, req, ret)
}

func (c *Client) doPOST(ctx context.Context, url string, body interface{}) (*http.Response, error) {
	req, err := c.newRequest("POST", url, body)
	if err != nil {
		return nil, err
	}

	return c.do(ctx, req, nil)
}

func (c *Client) doPUT(ctx context.Context, url string, body interface{}) (*http.Response, error) {
	req, err := c.newRequest("PUT", url, body)
	if err != nil {
		return nil, err
	}

	return c.do(ctx, req, nil)
}

func (c *Client) doPATCH(ctx context.Context, url string, body interface{}) (*http.Response, error) {
	req, err := c.newRequest("PATCH", url, body)
	if err != nil {
		return nil, err
	}

	return c.do(ctx, req, nil)
}

func (c *Client) doDELETE(ctx context.Context, url string, body interface{}) (*http.Response, error) {
	req, err := c.newRequest("DELETE", url, body)
	if err != nil {
		return nil, err
	}

	return c.do(ctx, req, nil)
}

func (c *Client) doOPTIONS(ctx context.Context, url string, ret interface{}, body interface{}) (*http.Response, error) {
	req, err := c.newRequest("OPTIONS", url, body)
	if err != nil {
		return nil, err
	}

	return c.do(ctx, req, ret)
}

// ApiCallRc represents the struct returned by LINSTOR, when accessing its REST API.
type ApiCallRc struct {
	// A masked error number
	RetCode int64  `json:"ret_code"`
	Message string `json:"message"`
	// Cause of the error
	Cause string `json:"cause,omitempty"`
	// Details to the error message
	Details string `json:"details,omitempty"`
	// Possible correction options
	Correction string `json:"correction,omitempty"`
	// List of error report ids related to this api call return code.
	ErrorReportIds []string `json:"error_report_ids,omitempty"`
	// Map of objection that have been involved by the operation.
	ObjRefs map[string]string `json:"obj_refs,omitempty"`
}

func (rc *ApiCallRc) String() string {
	s := fmt.Sprintf("Message: '%s'", rc.Message)
	if rc.Cause != "" {
		s += fmt.Sprintf("; Cause: '%s'", rc.Cause)
	}
	if rc.Details != "" {
		s += fmt.Sprintf("; Details: '%s'", rc.Details)
	}
	if rc.Correction != "" {
		s += fmt.Sprintf("; Correction: '%s'", rc.Correction)
	}
	if len(rc.ErrorReportIds) > 0 {
		s += fmt.Sprintf("; Reports: '[%s]'", strings.Join(rc.ErrorReportIds, ","))
	}

	return s
}

// DeleteProps is a slice of properties to delete.
type DeleteProps []string

// OverrideProps is a map of properties to modify (key/value pairs)
type OverrideProps map[string]string

// Namespaces to delete
type DeleteNamespaces []string

// GenericPropsModify is a struct combining DeleteProps and OverrideProps
type GenericPropsModify struct {
	DeleteProps      DeleteProps      `json:"delete_props,omitempty"`
	OverrideProps    OverrideProps    `json:"override_props,omitempty"`
	DeleteNamespaces DeleteNamespaces `json:"delete_namespaces,omitempty"`
}
