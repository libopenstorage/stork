package pureutils

import (
	"encoding/json"
	"fmt"

	"github.com/portworx/sched-ops/k8s/core"
)

const (
	PureSecretName   = "px-pure-secret"
	PureS3SecretName = "px-pure-secret-fbs3"
	PureJSONKey      = "pure.json"

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
