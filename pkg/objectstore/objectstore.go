package objectstore

import (
	"context"
	"fmt"

	"cloud.google.com/go/storage"
	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	stork_api "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"gocloud.dev/blob"
	"gocloud.dev/blob/azureblob"
	"gocloud.dev/blob/gcsblob"
	"gocloud.dev/blob/s3blob"
	"gocloud.dev/gcp"
	"golang.org/x/oauth2/google"
	//	"google.golang.org/api/option"
)

// GetBucket gets the bucket handle for the given backup location
func GetBucket(backupLocation *stork_api.BackupLocation) (*blob.Bucket, error) {
	if backupLocation == nil {
		return nil, fmt.Errorf("nil backupLocation")
	}

	switch backupLocation.Location.Type {
	case stork_api.BackupLocationGoogle:
		return getGoogleBucket(backupLocation)
	case stork_api.BackupLocationAzure:
		return getAzureBucket(backupLocation)
	case stork_api.BackupLocationS3:
		return getS3Bucket(backupLocation)
	default:
		return nil, fmt.Errorf("invalid backupLocation type: %v", backupLocation.Location.Type)
	}
}

func getS3Bucket(backupLocation *stork_api.BackupLocation) (*blob.Bucket, error) {
	sess, err := session.NewSession(&aws.Config{
		Endpoint: aws.String(backupLocation.Location.S3Config.Endpoint),
		Credentials: credentials.NewStaticCredentials(backupLocation.Location.S3Config.AccessKeyID,
			backupLocation.Location.S3Config.SecretAccessKey, ""),
		Region:           aws.String(backupLocation.Location.S3Config.Region),
		DisableSSL:       aws.Bool(backupLocation.Location.S3Config.DisableSSL),
		S3ForcePathStyle: aws.Bool(true),
	})
	if err != nil {
		return nil, err
	}
	return s3blob.OpenBucket(context.Background(), sess, backupLocation.Location.Path, nil)
}

func getAzureBucket(backupLocation *stork_api.BackupLocation) (*blob.Bucket, error) {
	accountName := azureblob.AccountName(backupLocation.Location.AzureConfig.StorageAccountName)
	accountKey := azureblob.AccountKey(backupLocation.Location.AzureConfig.StorageAccountKey)
	credential, err := azureblob.NewCredential(accountName, accountKey)
	if err != nil {
		return nil, err
	}
	pipeline := azureblob.NewPipeline(credential, azblob.PipelineOptions{})
	return azureblob.OpenBucket(context.Background(), pipeline, accountName, backupLocation.Location.Path, nil)
}

func getGoogleBucket(backupLocation *stork_api.BackupLocation) (*blob.Bucket, error) {
	conf, err := google.JWTConfigFromJSON([]byte(backupLocation.Location.GoogleConfig.AccountKey), storage.ScopeFullControl)
	if err != nil {
		return nil, err
	}

	ctx := context.Background()
	client, err := gcp.NewHTTPClient(
		gcp.DefaultTransport(),
		conf.TokenSource(ctx))
	if err != nil {
		return nil, err
	}

	return gcsblob.OpenBucket(ctx, client, backupLocation.Location.Path, nil)
}
