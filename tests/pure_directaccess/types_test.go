package puredirectaccess

import (
	"encoding/json"

	"github.com/portworx/sched-ops/k8s/core"
	"github.com/sirupsen/logrus"
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

func GetPXPureSecret() (PXPureSecret, error) {
	conf := PXPureSecret{}
	err := GetSecret(&conf, pureSecretName, secretNamespace)
	return conf, err
}

func GetS3Secret() (PXPureS3Secret, error) {
	conf := PXPureS3Secret{}
	err := GetSecret(&conf, pureS3SecretName, secretNamespace)
	return conf, err
}

func GetSecret(output interface{}, name, namespace string) error {
	secret, err := core.Instance().GetSecret(name, namespace)
	if err != nil {
		logrus.Errorf("failed to retrieve secret '%s' from namespace '%s'", name, namespace)
		return err
	}

	var pureConnectionJSON []byte
	var ok bool

	if pureConnectionJSON, ok = secret.Data[pureJSONKey]; !ok {
		logrus.Errorf("Secret '%s' is missing field '%s'", name, pureJSONKey)
		return err
	}

	err = json.Unmarshal(pureConnectionJSON, output)
	if err != nil {
		logrus.Errorf("Error unmarshaling config file from secret '%s': %v", name, err)
		return err
	}

	return nil
}
