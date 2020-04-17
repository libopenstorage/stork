package google

import (
	"context"
	"net/http"

	"cloud.google.com/go/storage"
	stork_api "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"gocloud.dev/blob"
	"gocloud.dev/blob/gcsblob"
	"gocloud.dev/gcp"
	"golang.org/x/oauth2/google"
	"golang.org/x/oauth2/jwt"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
)

func getConfig(backupLocation *stork_api.BackupLocation) (*jwt.Config, error) {
	return google.JWTConfigFromJSON(
		[]byte(backupLocation.Location.GoogleConfig.AccountKey),
		storage.ScopeFullControl)
}

// GetBucket gets a reference to the bucket for that backup location
func GetBucket(backupLocation *stork_api.BackupLocation) (*blob.Bucket, error) {
	conf, err := getConfig(backupLocation)
	if err != nil {
		return nil, err
	}
	client, err := gcp.NewHTTPClient(
		gcp.DefaultTransport(),
		conf.TokenSource(context.Background()))
	if err != nil {
		return nil, err
	}

	return gcsblob.OpenBucket(context.Background(), client, backupLocation.Location.Path, nil)
}

// CreateBucket creates a bucket for the bucket location
func CreateBucket(backupLocation *stork_api.BackupLocation) error {
	conf, err := getConfig(backupLocation)
	if err != nil {
		return err
	}
	ctx := context.Background()
	client, err := storage.NewClient(ctx, option.WithTokenSource(conf.TokenSource(ctx)))
	if err != nil {
		return err
	}
	err = client.Bucket(backupLocation.Location.Path).
		Create(ctx, backupLocation.Location.GoogleConfig.ProjectID, nil)
	if err != nil {
		if googleErr, ok := err.(*googleapi.Error); ok {
			if googleErr.Code == http.StatusConflict {
				return nil
			}
		}
	}
	return err
}
