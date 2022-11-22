package pdsutils

import (
	"os"

	log "github.com/sirupsen/logrus"
)

const (
	envControlPlaneURL         = "CONTROL_PLANE_URL"
	envPDSTestAccountName      = "TEST_ACCOUNT_NAME"
	envTargetKubeconfig        = "TARGET_KUBECONFIG"
	envUsername                = "PDS_USERNAME"
	envPassword                = "PDS_PASSWORD"
	envPDSClientSecret         = "PDS_CLIENT_SECRET"
	envPDSClientID             = "PDS_CLIENT_ID"
	envPDSISSUERURL            = "PDS_ISSUER_URL"
	envClusterType             = "CLUSTER_TYPE"
	envAwsAccessKey            = "AWS_ACCESS_KEY"
	envAwsSecretKey            = "AWS_SECRET_KEY"
	envAwsRegion               = "AWS_REGION"
	envAzureStorageAccountName = "AZURE_STORAGE_ACCOUNT_NAME"
	envAzurePrimaryAccountKey  = "AZURE_ACCOUNT_KEY"
	envGcpProjectId            = "GCP_PROJECT_ID"
	envGcpJsonPath             = "GCP_JSON_PATH"
	envMinioAccessKey          = "MINIO_ACCESS_KEY"
	envMinioSecretKey          = "MINIO_SECRET_KEY"
	envMinioEndpoint           = "MINIO_ENDPOINT"
	envMinioRegion             = "MINIO_REGION"
)

// Environment struct
type Environment struct {
	PDSControlPlaneURL         string
	PDSTestAccountName         string
	PDSTargetKUBECONFIG        string
	PDSUsername                string
	PDSPassword                string
	PDSIssuerURL               string
	PDSClientID                string
	PDSClientSecret            string
	PDSTargetClusterType       string
	PDSAwsAccessKey            string
	PDSAwsSecretKey            string
	PDSAwsRegion               string
	PDSAzureStorageAccountName string
	PDSAzurePrimaryAccountKey  string
	PDSGcpProjectId            string
	PDSGcpJsonPath             string
	PDSMinioAccessKey          string
	PDSMinioSecretKey          string
	PDSMinioEndpoint           string
	PDSMinioRegion             string
}

// MustHaveEnvVariables return emnvironment variables.
func MustHaveEnvVariables() Environment {
	return Environment{
		PDSControlPlaneURL: mustGetEnvVariable(envControlPlaneURL),
		PDSUsername:        mustGetEnvVariable(envUsername),
		PDSPassword:        mustGetEnvVariable(envPassword),
		PDSIssuerURL:       mustGetEnvVariable(envPDSISSUERURL),
		PDSClientID:        mustGetEnvVariable(envPDSClientID),
		PDSClientSecret:    mustGetEnvVariable(envPDSClientSecret),
	}
}

// BackupEnvVariables return emnvironment variables specific to backup.
func BackupEnvVariables() Environment {
	return Environment{
		PDSControlPlaneURL:         mustGetEnvVariable(envControlPlaneURL),
		PDSAwsAccessKey:            mustGetEnvVariable(envAwsAccessKey),
		PDSAwsSecretKey:            mustGetEnvVariable(envAwsSecretKey),
		PDSAwsRegion:               mustGetEnvVariable(envAwsRegion),
		PDSAzureStorageAccountName: mustGetEnvVariable(envAzureStorageAccountName),
		PDSAzurePrimaryAccountKey:  mustGetEnvVariable(envAzurePrimaryAccountKey),
		PDSGcpProjectId:            mustGetEnvVariable(envGcpProjectId),
		PDSGcpJsonPath:             mustGetEnvVariable(envGcpJsonPath),
		PDSMinioAccessKey:          mustGetEnvVariable(envMinioAccessKey),
		PDSMinioSecretKey:          mustGetEnvVariable(envMinioSecretKey),
		PDSMinioEndpoint:           mustGetEnvVariable(envMinioEndpoint),
		PDSMinioRegion:             mustGetEnvVariable(envMinioRegion),
	}
}

// mustGetEnvVariable return environment variable.
func mustGetEnvVariable(key string) string {
	value, isExist := os.LookupEnv(key)
	if !isExist {
		log.Panicf("Key: %v doesn't exist", key)
	}
	return value
}
