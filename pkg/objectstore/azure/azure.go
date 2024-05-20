package azure

import (
	"context"
	"fmt"
	"net/url"
	"os"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/storage/armstorage"
	"github.com/Azure/azure-storage-blob-go/azblob"
	az_autorest "github.com/Azure/go-autorest/autorest/azure"
	stork_api "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/libopenstorage/stork/pkg/objectstore/common"
	"github.com/sirupsen/logrus"
	"gocloud.dev/blob"
	"gocloud.dev/blob/azureblob"
)

const (
	azureEnvKey = "AZURE_ENVIRONMENT"
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

func getAzureURLSuffix(backupLocation *stork_api.BackupLocation) (string, error) {
	// Give first preference to environment variable setting.
	azureEnv := os.Getenv(azureEnvKey)
	if len(azureEnv) == 0 {
		// If env variable is not set, check the azure environment vaue from backuplocation CR.
		if len(backupLocation.Location.AzureConfig.Environment) != 0 {
			azureEnv = string(backupLocation.Location.AzureConfig.Environment)
			logrus.Infof("Received azure environment type %s from backup location cr", azureEnv)
		} else {
			logrus.Infof("Both BL Cr environment type and azureEnv are empty, setting storage domain to default i.e Public")
			return az_autorest.PublicCloud.StorageEndpointSuffix, nil
		}
	} else {
		logrus.Infof("Received azure environment type as environment variable which has higher priority than environment type passed down from backup location cr %s", azureEnv)
	}

	azureClientEnv, err := az_autorest.EnvironmentFromName(azureEnv)
	if err != nil {
		logrus.Errorf("Failed to get azure client env for:%v, err:%v", azureEnv, err)
		return "", err
	}
	// Endpoint suffix based on current cloud type
	return azureClientEnv.StorageEndpointSuffix, nil
}

// GetBucket gets a reference to the bucket for that backup location
func GetBucket(backupLocation *stork_api.BackupLocation) (*blob.Bucket, error) {
	accountName := azureblob.AccountName(backupLocation.Location.AzureConfig.StorageAccountName)
	pipeline, err := getPipeline(backupLocation)
	if err != nil {
		return nil, err
	}
	urlSuffix, err := getAzureURLSuffix(backupLocation)
	if err != nil {
		return nil, err
	}

	urlSuffix = fmt.Sprintf("blob.%s", urlSuffix)
	logrus.Debugf("azure - GetBucket - urlSuffix %v", urlSuffix)
	opts := azureblob.Options{
		StorageDomain: azureblob.StorageDomain(urlSuffix),
	}
	return azureblob.OpenBucket(context.Background(), pipeline, accountName, backupLocation.Location.Path, &opts)
}

// CreateBucket creates a bucket for the bucket location
func CreateBucket(backupLocation *stork_api.BackupLocation) error {
	accountName := azureblob.AccountName(backupLocation.Location.AzureConfig.StorageAccountName)
	pipeline, err := getPipeline(backupLocation)
	if err != nil {
		return err
	}
	urlSuffix, err := getAzureURLSuffix(backupLocation)
	if err != nil {
		return err
	}

	url, err := url.Parse(fmt.Sprintf("https://%s.blob.%s", accountName, urlSuffix))
	if err != nil {
		return err
	}
	logrus.Debugf("azure - CreateBucket - url %v", url)
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
	fn := "GetObjLockInfo"
	objLockInfo := &common.ObjLockInfo{}
	accountName := azureblob.AccountName(backupLocation.Location.AzureConfig.StorageAccountName)
	pipeline, err := getPipeline(backupLocation)
	if err != nil {
		logrus.Errorf("%v: backuplocation: [%v/%v] getPipeline failed: %v", fn, backupLocation.Namespace, backupLocation.Name, err)
		return nil, err
	}
	urlSuffix, err := getAzureURLSuffix(backupLocation)
	if err != nil {
		logrus.Errorf("%v: backuplocation: [%v/%v] getAzureURLSuffix failed: %v", fn, backupLocation.Namespace, backupLocation.Name, err)
		return nil, err
	}
	url, err := url.Parse(fmt.Sprintf("https://%s.blob.%s", accountName, urlSuffix))
	if err != nil {
		logrus.Errorf("%v: backuplocation: [%v/%v] url.Parse failed: %v", fn, backupLocation.Namespace, backupLocation.Name, err)
		return nil, err
	}
	leaseCondition := azblob.LeaseAccessConditions{}
	response, err := azblob.NewServiceURL(*url, pipeline).
		NewContainerURL(backupLocation.Location.Path).
		GetProperties(context.Background(), leaseCondition)
	if err != nil {
		logrus.Errorf("%v: backuplocation: [%v/%v] getProperties of the container %v failed %v", fn, backupLocation.Namespace, backupLocation.Name, backupLocation.Location.Path, err)
		return nil, err
	}
	if response.IsImmutableStorageWithVersioningEnabled() == "true" {
		objLockInfo.LockEnabled = true
		logrus.Infof("%v: backuplocation: [%v/%v] Immutability policy is enabled on container %v", fn, backupLocation.Namespace, backupLocation.Name, backupLocation.Location.Path)

		cred, err := azidentity.NewClientSecretCredential(backupLocation.Cluster.AzureClusterConfig.TenantID,
			backupLocation.Cluster.AzureClusterConfig.ClientID,
			backupLocation.Cluster.AzureClusterConfig.ClientSecret, nil)
		if err != nil {
			logrus.Errorf("%v: backuplocation: [%v/%v] failed in creating azure client with the given credential", fn, backupLocation.Namespace, backupLocation.Name)
			return nil, err
		}
		ctx := context.Background()
		clientFactory, err := armstorage.NewClientFactory(backupLocation.Cluster.AzureClusterConfig.SubscriptionID, cred, nil)
		if err != nil {
			logrus.Errorf("%v: backuplocation: [%v/%v] failed to create client: %v", fn, backupLocation.Namespace, backupLocation.Name, err)
			return nil, err
		}
		res, err := clientFactory.NewBlobContainersClient().GetImmutabilityPolicy(ctx,
			backupLocation.Location.AzureConfig.ResourceGroupName,
			backupLocation.Location.AzureConfig.StorageAccountName,
			backupLocation.Location.Path,
			&armstorage.BlobContainersClientGetImmutabilityPolicyOptions{IfMatch: nil})
		if err != nil {
			logrus.Errorf("%v: failed to  get immutability policy for the backuplocation [%v/%v] : %v", fn, backupLocation.Namespace, backupLocation.Name, err)
			return nil, err
		}
		properties := res.ImmutabilityPolicy.Properties
		objLockInfo.RetentionPeriodDays = int64(*properties.ImmutabilityPeriodSinceCreationInDays)
	} else {
		logrus.Infof("%v: backuplocation: [%v/%v] Immutability policy is disabled on container %v", fn, backupLocation.Namespace, backupLocation.Name, backupLocation.Location.Path)
		objLockInfo.LockEnabled = false
	}
	return objLockInfo, nil
}
