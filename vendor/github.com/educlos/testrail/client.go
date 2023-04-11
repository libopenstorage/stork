// testrail provides a go api for testrail
package testrail

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
)

// A Client stores the client informations
// and implement all the api functions
// to communicate with testrail
type Client struct {
	url        string
	username   string
	password   string
	httpClient *http.Client
	useBetaApi bool
}

// NewClient returns a new client
// with the given credential
// for the given testrail domain
func NewClient(url, username, password string, useBetaApi ...bool) (c *Client) {
	_useBetaApi := false
	if len(useBetaApi) > 0 {
		_useBetaApi = useBetaApi[0]
	}
	return NewCustomClient(url, username, password, nil, _useBetaApi)
}

// NewClient returns a new client with
// with the given credential
// for the given testrail domain
// and custom http Client
func NewCustomClient(url, username, password string, customHttpClient *http.Client, useBetaApi ...bool) (c *Client) {
	c = &Client{}
	c.username = username
	c.password = password

	c.url = url
	if !strings.HasSuffix(c.url, "/") {
		c.url += "/"
	}
	c.url += "index.php?/api/v2/"

	if customHttpClient != nil {
		c.httpClient = customHttpClient
	} else {
		c.httpClient = &http.Client{}
	}

	if len(useBetaApi) > 0 {
		c.useBetaApi = useBetaApi[0]
	}

	return
}

// sendRequest sends a request of type "method"
// to the url "client.url+uri" and with optional data "data"
// Returns an error if any and the optional data "v"
func (c *Client) sendRequest(method, uri string, data, v interface{}) error {
	var body io.Reader
	if data != nil {
		jsonReq, err := json.Marshal(data)
		if err != nil {
			return fmt.Errorf("marshaling data: %s", err)
		}

		body = bytes.NewBuffer(jsonReq)
	}

	req, err := http.NewRequest(method, c.url+uri, body)
	if err != nil {
		return err
	}

	req.SetBasicAuth(c.username, c.password)
	req.Header.Add("Accept", "application/json")
	req.Header.Add("Content-Type", "application/json")

	if c.useBetaApi {
		req.Header.Add("x-api-ident", "beta")
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	jsonCnt, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("reading: %s", err)
	}

	if resp.StatusCode >= http.StatusBadRequest {
		return fmt.Errorf("response: status: %q, body: %s", resp.Status, jsonCnt)
	}

	if v != nil {
		err = json.Unmarshal(jsonCnt, v)
		if err != nil {
			return fmt.Errorf("unmarshaling response: %s", err)
		}
	}

	return nil
}

type Links struct {
	Next *string `json:"next"`
	Prev *string `json:"prev"`
}

func (c *Client) sendRequestBeta(method, uri string, data, v interface{}, itemsKeyName string) error {
	var wraperMap map[string]json.RawMessage
	var returnItems []interface{}
	var tempItems []interface{}
	var links Links

	err := c.sendRequest("GET", uri, nil, &wraperMap)
	if err != nil {
		return err
	}

	json.Unmarshal(wraperMap[itemsKeyName], &tempItems)
	json.Unmarshal(wraperMap["_links"], &links)

	returnItems = append(returnItems, tempItems...)

	for err == nil && links.Next != nil {
		nextUri := strings.TrimPrefix(*links.Next, "/api/v2/")
		err = c.sendRequest("GET", nextUri, nil, &wraperMap)
		if err == nil {
			json.Unmarshal(wraperMap[itemsKeyName], &tempItems)
			json.Unmarshal(wraperMap["_links"], &links)
			returnItems = append(returnItems, tempItems...)
		} else {
			return err
		}
	}

	jsonAll, _ := json.Marshal(returnItems)
	json.Unmarshal(jsonAll, v)

	return nil
}
