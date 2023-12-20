package pdsutils

import (
	"os"

	"github.com/portworx/torpedo/pkg/log"
)

const (
	// Backup environment variable
	envAwsAccessKey            = "AWS_ACCESS_KEY_ID"
	envAwsSecretKey            = "AWS_SECRET_ACCESS_KEY"
	envAwsRegion               = "AWS_REGION"
	envMinioAccessKey          = "AWS_MINIO_ACCESS_KEY_ID"
	envMinioSecretKey          = "AWS_MINIO_SECRET_ACCESS_KEY"
	envMinioRegion             = "AWS_MINIO_REGION"
	envMinioEndPoint           = "AWS_MINIO_ENDPOINT"
	envAzureStorageAccountName = "AZURE_ACCOUNT_NAME"
	envAzurePrimaryAccountKey  = "AZURE_ACCOUNT_KEY"
	envGcpProjectId            = "GCP_PROJECT_ID"
	envGcpJsonPath             = "GCP_JSON_PATH"
	// Control plane environment variables
	envControlPlaneURL    = "CONTROL_PLANE_URL"
	envUsername           = "PDS_USERNAME"
	envPassword           = "PDS_PASSWORD"
	envPDSClientSecret    = "PDS_CLIENT_SECRET"
	envPDSClientID        = "PDS_CLIENT_ID"
	envPDSISSUERURL       = "PDS_ISSUER_URL"
	envClusterType        = "CLUSTER_TYPE"
	envPDSTestAccountName = "TEST_ACCOUNT_NAME"
	envTargetKubeconfig   = "TARGET_KUBECONFIG"
	// Restore environment variable
	envOcpRestoreTargetCluster     = "PDS_OCP_TARGET_CLUSTER"
	envVanillaRestoreTargetCluster = "PDS_VANILLA_TARGET_CLUSTER"
	envRkeRestoreTargetCluster     = "PDS_RKE_TARGET_CLUSTER"
	envAnthosRestoreTargetCluster  = "PDS_ANTHOS_TARGET_CLUSTER"
	envRestoreTargetCluster        = "PDS_RESTORE_TARGET_CLUSTER"
)

// Environment struct for PDS test execution
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

type RestoreEnvironment struct {
	PDSControlPlaneURL      string
	PDSRestoreTarget        string
	PDSOcpRestoreTarget     string
	PDSAnthosRestoreTarget  string
	PDSRkeRestoreTarget     string
	PDSVanillaRestoreTarget string
}

// BackupEnvVariables return environment variables specific to backup.
func BackupEnvVariables() Environment {
	return Environment{
		PDSControlPlaneURL:         mustGetEnvVariable(envControlPlaneURL),
		PDSAwsAccessKey:            mustGetEnvVariable(envAwsAccessKey),
		PDSAwsSecretKey:            mustGetEnvVariable(envAwsSecretKey),
		PDSAwsRegion:               mustGetEnvVariable(envAwsRegion),
		PDSAzureStorageAccountName: mustGetEnvVariable(envAzureStorageAccountName),
		PDSAzurePrimaryAccountKey:  mustGetEnvVariable(envAzurePrimaryAccountKey),
		PDSGcpProjectId:            mustGetEnvVariable(envGcpProjectId),
		PDSMinioAccessKey:          mustGetEnvVariable(envMinioAccessKey),
		PDSMinioSecretKey:          mustGetEnvVariable(envMinioSecretKey),
		PDSMinioRegion:             mustGetEnvVariable(envMinioRegion),
		PDSMinioEndpoint:           mustGetEnvVariable(envMinioEndPoint),
	}
}

// BackupEnvVariables return environment variables specific to backup.
func RestoreEnvVariables() RestoreEnvironment {
	return RestoreEnvironment{
		PDSControlPlaneURL: mustGetEnvVariable(envControlPlaneURL),
		PDSRestoreTarget:   mustGetEnvVariable(envRestoreTargetCluster),
	}
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

// mustGetEnvVariable return environment variable.
func mustGetEnvVariable(key string) string {
	value, isExist := os.LookupEnv(key)
	if !isExist {
		log.Panicf("Key: %v doesn't exist", key)
	}
	return value
}
