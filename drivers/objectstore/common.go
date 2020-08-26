package objectstore

import (
	"context"
	"fmt"
	"io"

	stork_api "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	stork_objectstore "github.com/libopenstorage/stork/pkg/objectstore"
	"github.com/portworx/sched-ops/task"
	"gocloud.dev/blob"
)

// DefaultDriver implements defaults for Driver interface
type DefaultDriver struct {
}

func (d *DefaultDriver) String() string {
	return ""
}

// ValidateBackupsDeletedFromCloud checks it given backups are deleted from the cloud
func (d *DefaultDriver) ValidateBackupsDeletedFromCloud(backupLocation *stork_api.BackupLocation, backupPath string) error {
	t := func() (interface{}, bool, error) {
		bucket, err := stork_objectstore.GetBucket(backupLocation)
		if err != nil {
			return "", false, fmt.Errorf("failed to get bucket %s in namespace: %s.Bucket: %v is present", backupLocation.Location.Path, backupLocation.Namespace, bucket)
		}
		iterator := bucket.List(&blob.ListOptions{
			Prefix:    backupPath,
			Delimiter: "/",
		})
		backups := make(map[string]bool)
		for {
			object, err := iterator.Next(context.TODO())
			if err == io.EOF {
				break
			}
			if err != nil {
				return "", false, fmt.Errorf("Failed to iterate: %v", err)
			}
			if object.IsDir {
				backups[object.Key] = true
			}
		}
		if len(backups) > 0 {
			return "", true, fmt.Errorf("failed to delete backups in namespace: %s", backupLocation.Namespace)
		}
		return "", false, nil
	}
	_, err := task.DoRetryWithTimeout(t, defaultTimeout, defaultRetryInterval)
	if err != nil {
		return err
	}
	return nil
}
