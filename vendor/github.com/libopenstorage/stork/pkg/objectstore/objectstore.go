package objectstore

import (
	"fmt"

	stork_api "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/libopenstorage/stork/pkg/objectstore/azure"
	"github.com/libopenstorage/stork/pkg/objectstore/google"
	"github.com/libopenstorage/stork/pkg/objectstore/s3"
	"gocloud.dev/blob"
)

// GetBucket gets the bucket handle for the given backup location
func GetBucket(backupLocation *stork_api.BackupLocation) (*blob.Bucket, error) {
	if backupLocation == nil {
		return nil, fmt.Errorf("nil backupLocation")
	}

	switch backupLocation.Location.Type {
	case stork_api.BackupLocationGoogle:
		return google.GetBucket(backupLocation)
	case stork_api.BackupLocationAzure:
		return azure.GetBucket(backupLocation)
	case stork_api.BackupLocationS3:
		return s3.GetBucket(backupLocation)
	default:
		return nil, fmt.Errorf("invalid backupLocation type: %v", backupLocation.Location.Type)
	}
}

// CreateBucket gets the bucket handle for the given backup location
func CreateBucket(backupLocation *stork_api.BackupLocation) error {
	if backupLocation == nil {
		return fmt.Errorf("nil backupLocation")
	}

	switch backupLocation.Location.Type {
	case stork_api.BackupLocationGoogle:
		return google.CreateBucket(backupLocation)
	case stork_api.BackupLocationAzure:
		return azure.CreateBucket(backupLocation)
	case stork_api.BackupLocationS3:
		return s3.CreateBucket(backupLocation)
	default:
		return fmt.Errorf("invalid backupLocation type: %v", backupLocation.Location.Type)
	}
}
