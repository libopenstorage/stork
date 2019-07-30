package controllers

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"time"

	storkv1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/libopenstorage/stork/pkg/crypto"
	"github.com/libopenstorage/stork/pkg/log"
	"github.com/libopenstorage/stork/pkg/objectstore"
	"github.com/portworx/sched-ops/k8s"
	"github.com/sirupsen/logrus"
	"gocloud.dev/blob"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/tools/record"
)

// BackupSyncController reconciles applicationbackup objects
type BackupSyncController struct {
	Recorder     record.EventRecorder
	SyncInterval time.Duration
	stopChannel  chan os.Signal
}

// Init Initializes the backup sync controller
func (b *BackupSyncController) Init(stopChannel chan os.Signal) error {
	b.stopChannel = stopChannel
	go b.startBackupSync()
	return nil
}

func (b *BackupSyncController) startBackupSync() {
	for {
		select {
		case <-time.After(b.SyncInterval):
			backupLocations, err := k8s.Instance().ListBackupLocations("")
			if err != nil {
				logrus.Errorf("Error getting backup location to sync: %v", err)
				continue
			}
			for _, backupLocation := range backupLocations.Items {
				err := b.syncBackupsFromLocation(&backupLocation)
				if err != nil {
					log.BackupLocationLog(&backupLocation).Errorf("Error syncing backups from location: %v", err)
					continue
				}
			}

		case <-b.stopChannel:
			return
		}
	}
}

func (b *BackupSyncController) syncBackupsFromLocation(location *storkv1.BackupLocation) error {
	if !location.Location.Sync {
		return nil
	}
	bucket, err := objectstore.GetBucket(location)
	if err != nil {
		return err
	}
	iterator := bucket.List(&blob.ListOptions{
		Prefix:    location.Namespace + "/",
		Delimiter: "/",
	})
	backups := make(map[string]bool)
	for {
		object, err := iterator.Next(context.TODO())
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if object.IsDir {
			backups[object.Key] = true
		}
	}
	for backupName := range backups {
		iterator := bucket.List(&blob.ListOptions{
			Prefix:    backupName,
			Delimiter: "/",
		})
		for {
			object, err := iterator.Next(context.TODO())
			if err == io.EOF {
				break
			}
			if err != nil {
				return err
			}
			if object.IsDir {
				data, err := bucket.ReadAll(context.TODO(), filepath.Join(object.Key, metadataObjectName))
				if err != nil {
					log.BackupLocationLog(location).Errorf("Error syncing backup %v: %v", backupName, err)
					continue
				}
				if location.Location.EncryptionKey != "" {
					if data, err = crypto.Decrypt(data, location.Location.EncryptionKey); err != nil {
						log.BackupLocationLog(location).Errorf("Error decrypting backup %v during sync: %v", backupName, err)
						continue
					}
				}
				backupInfo := storkv1.ApplicationBackup{}
				if err = json.Unmarshal(data, &backupInfo); err != nil {
					log.BackupLocationLog(location).Errorf("Error parsing backup %v during sync: %v", backupName, err)
					continue
				}

				localBackupInfo, err := k8s.Instance().GetApplicationBackup(backupInfo.Name, backupInfo.Namespace)
				if err == nil {
					// The UIDs will match if it was originally created on this
					// cluster. We don't want to sync those backups
					if localBackupInfo.UID == backupInfo.UID {
						continue
					}
				} else if !errors.IsNotFound(err) {
					// Ignore any other error except NotFound
					continue
				}

				// Now check if we've synced this backup to this cluster
				// already using the generated name
				syncedBackupName := b.getSyncedBackupName(&backupInfo)
				_, err = k8s.Instance().GetApplicationBackup(syncedBackupName, backupInfo.Namespace)
				if !errors.IsNotFound(err) {
					// If we get anything other than NotFound ignore it
					continue
				}

				backupInfo.Name = syncedBackupName
				backupInfo.UID = ""
				backupInfo.ResourceVersion = ""
				backupInfo.SelfLink = ""
				backupInfo.Spec.ReclaimPolicy = storkv1.ApplicationBackupReclaimPolicyRetain
				_, err = k8s.Instance().CreateApplicationBackup(&backupInfo)
				if err != nil {
					return err
				}

			}
		}
	}
	return nil
}

func (b *BackupSyncController) getSyncedBackupName(backup *storkv1.ApplicationBackup) string {
	// For scheduled backups use the original name
	if _, ok := backup.Annotations[ApplicationBackupScheduleNameAnnotation]; ok {
		return backup.Name
	}
	return backup.Name + "-" + backup.Status.TriggerTimestamp.Time.Format(nameTimeSuffixFormat)
}
