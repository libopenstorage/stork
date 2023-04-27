package netutil

import (
	"crypto/tls"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"strconv"
)

type HttpRequest struct {
	Method   string
	Url      string
	Content  string
	Auth     string
	Body     io.Reader
	Insecure bool
}

// MakeURL makes URL based on params taking into consideration IPv6
func MakeURL(urlPrefix, ip string, port int) string {
	return urlPrefix + net.JoinHostPort(ip, strconv.Itoa(port))
}

// DoRequest sends HTTP API request
func DoRequest(request HttpRequest) ([]byte, error) {
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: request.Insecure},
	}
	client := &http.Client{Transport: tr}

	req, err := http.NewRequest(request.Method, request.Url, request.Body)
	if err != nil {
		return nil, fmt.Errorf("Failed to make request: %+v, Err: %v", req, err)
	}

	if request.Content != "" {
		req.Header.Set("Content-Type", request.Content)
	}

	if request.Auth != "" {
		req.Header.Set("Authorization", request.Auth)
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("Failed to send request, Err: %v %v", resp, err)
	}
	defer resp.Body.Close()

	htmlData, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("Failed to read response: %+v", resp.Body)
	}
	return htmlData, nil
}
