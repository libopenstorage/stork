package restutil

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"io"
	"net/http"
	neturl "net/url"
	"time"
)

// Auth basic auth details for API
type Auth struct {
	Username string
	Password string
}

const (
	defaultRestTimeOut = 10 * time.Second
)

// Get rest get call
func Get(url string, auth *Auth, headers map[string]string) ([]byte, int, error) {

	respBody, respStatusCode, err := getResponse(http.MethodGet, url, nil, auth, headers)
	if err != nil {
		return nil, 0, err
	}
	return respBody, respStatusCode, nil
}

// POST rest post call
func POST(url string, payload interface{}, auth *Auth, headers map[string]string) ([]byte, int, error) {

	respBody, respStatusCode, err := getResponse(http.MethodPost, url, payload, auth, headers)
	if err != nil {
		return nil, 0, err
	}
	return respBody, respStatusCode, nil

}

// PUT rest put call
func PUT(url string, payload interface{}, auth *Auth, headers map[string]string) ([]byte, int, error) {

	respBody, respStatusCode, err := getResponse(http.MethodPut, url, payload, auth, headers)
	if err != nil {
		return nil, 0, err
	}
	return respBody, respStatusCode, nil

}

// DELETE rest delete call
func DELETE(url string, payload interface{}, auth *Auth, headers map[string]string) ([]byte, int, error) {
	respBody, respStatusCode, err := getResponse(http.MethodDelete, url, payload, auth, headers)
	if err != nil {
		return nil, 0, err
	}
	return respBody, respStatusCode, nil

}

func validateURL(url string) error {
	_, err := neturl.ParseRequestURI(url)
	return err
}

func getResponse(httpMethod, url string, payload interface{}, auth *Auth, headers map[string]string) ([]byte, int, error) {
	var err error
	err = validateURL(url)
	if err != nil {
		return nil, 0, err
	}
	var req *http.Request
	if payload != nil {
		var j []byte
		j, err = json.Marshal(payload)

		if err != nil {
			return nil, 0, err
		}
		req, err = http.NewRequest(httpMethod, url, bytes.NewBuffer(j))
	} else {
		req, err = http.NewRequest(httpMethod, url, nil)
	}
	if err != nil {
		return nil, 0, err
	}
	setBasicAuthAndHeaders(req, auth, headers)
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	client := &http.Client{
		Timeout:   defaultRestTimeOut,
		Transport: tr,
	}
	var resp *http.Response
	resp, err = client.Do(req)
	if err != nil {
		return nil, 0, err
	}

	respBody, err := getBody(resp.Body)
	if err != nil {
		return nil, 0, err
	}
	err = resp.Body.Close()
	return respBody, resp.StatusCode, err
}

func setBasicAuthAndHeaders(req *http.Request, auth *Auth, headers map[string]string) *http.Request {

	//Setting basic auth
	if auth != nil {
		req.SetBasicAuth(auth.Username, auth.Password)
	}
	//Setting headers
	req.Header.Set("Content-Type", "application/json")
	if len(headers) > 0 {
		for k, v := range headers {

			req.Header.Set(k, v)

		}
	}
	return req
}

func getBody(rBody io.ReadCloser) ([]byte, error) {
	respBody, err := io.ReadAll(rBody)
	if err != nil {
		return nil, err
	}
	return respBody, nil
}
