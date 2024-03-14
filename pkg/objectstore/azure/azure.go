package azure

import (
	"context"
	"fmt"
	"net/url"
	"os"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-blob-go/azblob"
	az_autorest "github.com/Azure/go-autorest/autorest/azure"
	stork_api "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/libopenstorage/stork/pkg/objectstore/common"
	"github.com/sirupsen/logrus"
	"gocloud.dev/blob"
	"gocloud.dev/blob/azureblob"
)

const (
	azureEnv = "AZURE_ENVIRONMENT"
)

func getPipeline(backupLocation *stork_api.BackupLocation) (pipeline.Pipeline, error) {
	accountName := azureblob.AccountName(backupLocation.Location.AzureConfig.StorageAccountName)
	accountKey := azureblob.AccountKey(backupLocation.Location.AzureConfig.StorageAccountKey)
	credential, err := azureblob.NewCredential(accountName, accountKey)
	if err != nil {
		return nil, err
	}
	return azureblob.NewPipeline(credential, azblob.PipelineOptions{}), nil
}

// GetBucket gets a reference to the bucket for that backup location
func GetBucket(backupLocation *stork_api.BackupLocation) (*blob.Bucket, error) {
	accountName := azureblob.AccountName(backupLocation.Location.AzureConfig.StorageAccountName)
	pipeline, err := getPipeline(backupLocation)
	if err != nil {
		return nil, err
	}
	urlSuffix := fmt.Sprintf("blob.%s", getAzureURLSuffix())

	opts := azureblob.Options{
		StorageDomain: azureblob.StorageDomain(urlSuffix),
	}
	return azureblob.OpenBucket(context.Background(), pipeline, accountName, backupLocation.Location.Path, &opts)
}

func getAzureURLSuffix() string {
	azureEnv := os.Getenv(azureEnv)
	if azureEnv == "" {
		return az_autorest.PublicCloud.StorageEndpointSuffix
	}
	azureClEnv, err := az_autorest.EnvironmentFromName(azureEnv)
	if err != nil {
		logrus.Errorf("Failed to get azure cl env for:%v, err:%v", azureEnv, err)
		return az_autorest.PublicCloud.StorageEndpointSuffix
	}
	// Endpoint suffux based on current cloud type
	return azureClEnv.StorageEndpointSuffix
}

// CreateBucket creates a bucket for the bucket location
func CreateBucket(backupLocation *stork_api.BackupLocation) error {
	accountName := azureblob.AccountName(backupLocation.Location.AzureConfig.StorageAccountName)
	pipeline, err := getPipeline(backupLocation)
	if err != nil {
		return err
	}
	urlSuffix := getAzureURLSuffix()
	url, err := url.Parse(fmt.Sprintf("https://%s.blob.%s", accountName, urlSuffix))
	logrus.Infof("azure - CreateBucket ---------->  url %v -- err %v", url, err)
	if err != nil {
		return err
	}

	_, err = azblob.NewServiceURL(*url, pipeline).
		NewContainerURL(backupLocation.Location.Path).
		Create(context.Background(), azblob.Metadata{}, azblob.PublicAccessNone)
	if err != nil {
		if azblobErr, ok := err.(azblob.StorageError); ok {
			if azblobErr.ServiceCode() == azblob.ServiceCodeContainerAlreadyExists {
				return nil
			}
		}
	}
	return err
}

// GetObjLockInfo fetches the object lock configuration of a bucket
func GetObjLockInfo(backupLocation *stork_api.BackupLocation) (*common.ObjLockInfo, error) {
	logrus.Infof("object lock is not supported for azure provider")
	return &common.ObjLockInfo{}, nil
}
