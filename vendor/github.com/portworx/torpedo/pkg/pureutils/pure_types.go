package pureutils

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"github.com/hashicorp/go-version"
	"github.com/portworx/torpedo/pkg/log"
	"github.com/portworx/torpedo/pkg/units"
	"io/ioutil"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/portworx/sched-ops/k8s/core"
)

const (
	PureSecretName      = "px-pure-secret"
	PureS3SecretName    = "px-pure-secret-fbs3"
	PureJSONKey         = "pure.json"
	NonPxPureSecretName = "px-non-pure-secret"
	nonPxPureJsonKey    = "non-pure.json"

	PureSecretArrayZoneLabel = "topology.portworx.io/zone"

	CloudDriveFAMgmtLabel = "portworx.io/flasharray-management-endpoint"
)

type FlashArrayEntry struct {
	APIToken     string            `json:"APIToken"`
	MgmtEndPoint string            `json:"MgmtEndPoint"`
	Labels       map[string]string `json:"Labels"`
}

type FlashBladeEntry struct {
	MgmtEndPoint string            `json:"MgmtEndPoint"`
	NFSEndPoint  string            `json:"NFSEndPoint"`
	APIToken     string            `json:"APIToken"`
	Labels       map[string]string `json:"Labels"`
}

type PXPureSecret struct {
	Arrays []FlashArrayEntry `json:"FlashArrays"`
	Blades []FlashBladeEntry `json:"FlashBlades"`
}

type FlashBladeS3Entry struct {
	ObjectStoreEndpoint string `json:"ObjectStoreEndpoint"`
	S3AccessKey         string `json:"S3AccessKey"`
	S3SecretKey         string `json:"S3SecretKey"`
}

type versionInfo struct {
	Versions []string `json:"version"`
}

type PXPureS3Secret struct {
	Blades []FlashBladeS3Entry `json:"FlashBlades"`
}

func (p *PXPureSecret) GetArrayToZoneMap() map[string]string {
	arrayEndpointToZoneMap := map[string]string{}
	for _, array := range p.Arrays {
		if len(array.Labels) == 0 {
			arrayEndpointToZoneMap[array.MgmtEndPoint] = ""
			continue
		}
		zoneLabel, ok := array.Labels[PureSecretArrayZoneLabel]
		if !ok {
			arrayEndpointToZoneMap[array.MgmtEndPoint] = ""
			continue
		}
		arrayEndpointToZoneMap[array.MgmtEndPoint] = zoneLabel
	}
	return arrayEndpointToZoneMap
}

func GetNonPxPureSecret(namespace string) (PXPureSecret, error) {
	conf := PXPureSecret{}
	err := GetFAPureSecret(&conf, NonPxPureSecretName, namespace)
	return conf, err
}

func GetPXPureSecret(namespace string) (PXPureSecret, error) {
	conf := PXPureSecret{}
	err := GetPureSecret(&conf, PureSecretName, namespace)
	return conf, err
}

func GetS3Secret(namespace string) (PXPureS3Secret, error) {
	conf := PXPureS3Secret{}
	err := GetPureSecret(&conf, PureS3SecretName, namespace)
	return conf, err
}

func GetFAPureSecret(output interface{}, name, namespace string) error {
	secret, err := core.Instance().GetSecret(name, namespace)
	if err != nil {
		return fmt.Errorf("failed to retrieve secret '%s' from namespace '%s': %v", name, namespace, err)
	}

	var pureConnectionJSON []byte
	var ok bool

	if pureConnectionJSON, ok = secret.Data[nonPxPureJsonKey]; !ok {
		return fmt.Errorf("secret '%s' is missing field '%s'", name, nonPxPureJsonKey)
	}

	err = json.Unmarshal(pureConnectionJSON, output)
	if err != nil {
		return fmt.Errorf("error unmarshaling pure config file from secret '%s': %v", name, err)
	}

	return nil
}

func GetPureSecret(output interface{}, name, namespace string) error {
	secret, err := core.Instance().GetSecret(name, namespace)
	if err != nil {
		return fmt.Errorf("failed to retrieve secret '%s' from namespace '%s': %v", name, namespace, err)
	}

	var pureConnectionJSON []byte
	var ok bool

	if pureConnectionJSON, ok = secret.Data[PureJSONKey]; !ok {
		return fmt.Errorf("secret '%s' is missing field '%s'", name, PureJSONKey)
	}

	err = json.Unmarshal(pureConnectionJSON, output)
	if err != nil {
		return fmt.Errorf("error unmarshaling pure config file from secret '%s': %v", name, err)
	}

	return nil
}

func getPureFAVolumesJsonData(mgmtEndpoint, apiToken string) (string, error) {
	maxVer, err := getMaxVersion(mgmtEndpoint)
	if err != nil {
		return "", err
	}
	resVersion := "1.6"

	v1, err := version.NewVersion(maxVer)
	if err != nil {
		return "", err
	}
	v2, err := version.NewVersion("1.6")
	if err != nil {
		return "", err
	}
	v3, err := version.NewVersion("2.2")
	if err != nil {
		return "", err
	}

	if v1.LessThan(v2) {
		resVersion = "1.6"
	} else if v1.LessThan(v3) {
		resVersion = "1.17"
	} else {
		resVersion = "2.2"
	}

	authToken, err := getAuthToken(mgmtEndpoint, apiToken)
	if err != nil {
		return "", err
	}

	tempClient := &http.Client{
		// http.Client doesn't set the default Timeout,
		// so it will be blocked forever without Timeout setting
		Timeout: time.Second * time.Duration(20),
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
	}

	endpoint := fmt.Sprintf("https://%s/api/%s", mgmtEndpoint, resVersion)
	apiEndpoint := "/volumes"

	faUrl := endpoint + apiEndpoint

	// Create an HTTP request
	req, err := http.NewRequest("GET", faUrl, nil)
	if err != nil {
		return "", err
	}
	// Set the API token in the Authorization header
	//req.Header.Set("Authorization", "Bearer "+authToken)
	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("x-auth-token", authToken)

	resp, err := tempClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	// Read the response body
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	jsonData := string(body)
	return jsonData, nil
}

func GetPureFAVolumes(mgmtEndpoint, apiToken string) ([]string, error) {

	jsonData, err := getPureFAVolumesJsonData(mgmtEndpoint, apiToken)
	if err != nil {
		return nil, fmt.Errorf("error getting volumes json data, Err : %v", err)
	}
	// Decode the JSON string into a map
	var data map[string]interface{}
	err = json.Unmarshal([]byte(jsonData), &data)
	if err != nil {
		return nil, fmt.Errorf("error decoding JSON: %v", err)
	}

	// Extract names from the "items" array
	items, ok := data["items"].([]interface{})
	if !ok {
		return nil, fmt.Errorf("error accessing 'items' array")
	}

	var faVolsList []string
	for _, item := range items {
		if itemMap, ok := item.(map[string]interface{}); ok {
			if name, ok := itemMap["name"].(string); ok {
				faVolsList = append(faVolsList, name)
			}
		}
	}

	return faVolsList, nil

}

func GetPureFAVolumeSize(volName, mgmtEndpoint, apiToken string) (uint64, error) {

	jsonData, err := getPureFAVolumesJsonData(mgmtEndpoint, apiToken)
	if err != nil {
		return 0, fmt.Errorf("error getting volumes json data, Err : %v", err)
	}
	// Decode the JSON string into a map
	var data map[string]interface{}
	err = json.Unmarshal([]byte(jsonData), &data)
	if err != nil {
		return 0, fmt.Errorf("error decoding JSON: %v", err)
	}

	// Extract names from the "items" array
	items, ok := data["items"].([]interface{})
	if !ok {
		return 0, fmt.Errorf("error accessing 'items' array")
	}

	var volSize float64 = -1

	for _, item := range items {
		if itemMap, ok := item.(map[string]interface{}); ok {
			if name, ok := itemMap["name"].(string); ok {
				if strings.Contains(name, volName) {
					log.Infof("itemMap: [%v]", itemMap)
					if space, ok := itemMap["space"].(map[string]interface{}); ok {
						log.Infof("space: [%v]", space)
						if totalProv, ok := space["total_provisioned"].(float64); ok {
							log.Infof("totalProv: [%v]", totalProv)
							volSize = totalProv / units.GiB
						}
					}
				}
			}
		}
	}

	return uint64(volSize), err

}

func getMaxVersion(mgmtEndpoint string) (string, error) {
	tempClient := &http.Client{
		// http.Client doesn't set the default Timeout,
		// so it will be blocked forever without Timeout setting
		Timeout: time.Second * time.Duration(10),
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
	}

	resp, err := tempClient.Get(fmt.Sprintf("https://%s/api/api_version", mgmtEndpoint))

	if err != nil {
		return "", err
	}

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("response status is [%d]", resp.StatusCode)

	}
	respbody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to convert response body to bytes, error: [%v]", err)
	}
	info := versionInfo{}
	if err := json.Unmarshal([]byte(respbody), &info); err != nil {
		return "", fmt.Errorf("failed to unmarshal array API versions!, error: [%v]", err)

	}
	if len(info.Versions) == 0 {
		return "", fmt.Errorf("array API version array is empty")

	}
	versions := info.Versions
	// sort version response to make sure we get the latest version.
	sortedVersions, err := versionLess(versions)
	if err != nil {
		return "", err
	}

	return sortedVersions[len(sortedVersions)-1], nil
}

func versionLess(versions []string) ([]string, error) {

	// Convert string versions to []*version.Version
	versionObjects := make([]*version.Version, len(versions))
	for i, v := range versions {
		parsedVersion, err := version.NewVersion(v)
		if err != nil {
			return nil, err
		}
		versionObjects[i] = parsedVersion
	}

	// Sort []*version.Version
	sort.Sort(version.Collection(versionObjects))

	// Convert back to string slice
	sortedVersions := make([]string, len(versionObjects))
	for i, v := range versionObjects {
		sortedVersions[i] = v.Original()
	}
	return sortedVersions, nil

}

func getAuthToken(mgmtEndpoint, apiToken string) (string, error) {

	url := fmt.Sprintf("https://%s/api/2.2/login", mgmtEndpoint)
	bodyReader := bytes.NewReader([]byte{})
	request, err := http.NewRequest("POST", url, bodyReader)
	if err != nil {
		return "", err
	}

	request.Header.Add("Content-Type", "application/json")
	request.Header.Add("api-token", apiToken)

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
	log.Info("Login Success!")

	return authToken, nil

}
