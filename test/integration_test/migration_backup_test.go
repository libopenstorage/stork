// +build integrationtest

package integrationtest

import (
	"testing"
	"time"

	storkv1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	storkops "github.com/portworx/sched-ops/k8s/stork"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

func TestMigrationBackup(t *testing.T) {
	// reset mock time before running any tests
	err := setMockTime(nil)
	require.NoError(t, err, "Error resetting mock time")

	setDefaultsForBackup(t)

	logrus.Infof("Using stork volume driver: %s", volumeDriverName)
	logrus.Infof("Backup path being used: %s", backupLocationPath)

	t.Run("deploymentMigrationBackupTest", deploymentMigrationBackupTest)
}

func deploymentMigrationBackupTest(t *testing.T) {
	for location, secret := range allConfigMap {
		logrus.Infof("Backing up to cloud: %v using secret %v", location, secret)
		var err error
		// Reset config in case of error
		defer func() {
			err = setRemoteConfig("")
			require.NoError(t, err, "Error resetting remote config")
		}()

		// Trigger migration to backup app from source cluster to destination cluster
		ctxs, preMigrationCtx := triggerMigration(
			t,
			"mysql-migration",
			"mysql-1-pvc",
			nil,
			[]string{"mysql-migration"},
			true,
			false,
			true,
		)

		// Cleanup up source
		validateAndDestroyMigration(t, ctxs, preMigrationCtx, true, true, true, false, true)

		logrus.Infoln("Completed migration of apps from source to destination, will now start backup on second cluster")

		// Change kubeconfig to destination cluster
		err = dumpRemoteKubeConfig(remoteConfig)
		require.NoErrorf(t, err, "Unable to write clusterconfig: %v", err)

		err = setRemoteConfig(remoteFilePath)
		require.NoError(t, err, "Error setting remote config")

		// Backup app from the destination cluster to cloud
		currBackupLocation, err := createBackupLocation(t, appKey+"-backup-location", defaultTestKey+"-mysql-migration", storkv1.BackupLocationType(location), secret)
		require.NoError(t, err, "Error creating backuplocation %s", currBackupLocation.Name)

		currBackup, err := createApplicationBackupWithAnnotation(t, "dest-backup", defaultTestKey+"-mysql-migration", currBackupLocation)
		require.NoError(t, err, "Error creating app backups")

		err = waitForAppBackupCompletion(currBackup.Name, currBackup.Namespace, applicationBackupSyncRetryTimeout)
		require.NoError(t, err, "Application backup %s in namespace %s failed.", currBackup.Name, currBackup.Namespace)

		destroyAndWait(t, []*scheduler.Context{preMigrationCtx})

		logrus.Infoln("Completed backup of apps destination cluster")

		// Switch kubeconfig to source cluster for restore
		err = setRemoteConfig("")
		require.NoError(t, err, "Error resetting remote config")

		srcBackupLocation, err := createBackupLocation(t, appKey+"-backup-location", defaultTestKey+"-mysql-migration", storkv1.BackupLocationType(location), secret)
		require.NoError(t, err, "Error creating backuplocation %s", currBackupLocation.Name)

		// Set sync to true on first cluster so that backup from second cluster is available
		srcBackupLocation.Location.Sync = true
		srcBackupLocation, err = storkops.Instance().UpdateBackupLocation(srcBackupLocation)
		require.NoError(t, err, "Failed to set backup-location sync to true")

		backupToRestore, err := getSyncedBackupWithAnnotation(currBackup, backupSyncAnnotation)
		require.NotNil(t, backupToRestore, "Backup sync failed. Backup not found on the second cluster")

		appRestoreForBackup, err := createApplicationRestore(t, "restore-from-dest-backup", srcBackupLocation.Namespace, backupToRestore, srcBackupLocation)
		require.Nil(t, err, "failure to create restore object in namespace %s", currBackup.Namespace)
		require.NotNil(t, appRestoreForBackup, "failure to restore backup in namespace %s", currBackup.Namespace)

		err = schedulerDriver.WaitForRunning(preMigrationCtx, defaultWaitTimeout, defaultWaitInterval)
		require.NoError(t, err, "Error waiting for app on source cluster post restore from dest backup")

		logrus.Infoln("Completed restore of apps on source cluster")

		// Set sync to false on first cluster so that no more backups are sync'ed
		srcBackupLocation.Location.Sync = false
		srcBackupLocation, err = storkops.Instance().UpdateBackupLocation(srcBackupLocation)
		require.NoError(t, err, "Failed to set backup-location sync to true")

		time.Sleep(time.Second * 30)

		err = deleteAndWaitForBackupDeletion(srcBackupLocation.Namespace)
		require.NoError(t, err, "All backups not deleted on the source cluster post migration-backup-restore: %v.", err)

		time.Sleep(time.Second * 30)

		err = storkops.Instance().DeleteBackupLocation(srcBackupLocation.Name, srcBackupLocation.Namespace)
		require.NoError(t, err, "Failed to delete  backup location %s on source cluster: %v.", srcBackupLocation.Name, err)

		destroyAndWait(t, []*scheduler.Context{preMigrationCtx})

		err = deleteApplicationRestoreList([]*storkv1.ApplicationRestore{appRestoreForBackup})
		require.NoError(t, err, "failure to delete application restore %s: %v", appRestoreForBackup.Name, err)

		logrus.Infoln("Completed cleanup on source cluster")

		// Change kubeconfig to second cluster
		err = dumpRemoteKubeConfig(remoteConfig)
		require.NoErrorf(t, err, "Unable to write clusterconfig: %v", err)

		err = setRemoteConfig(remoteFilePath)
		require.NoError(t, err, "Error setting remote config")
		err = deleteAndWaitForBackupDeletion(currBackup.Namespace)
		require.NoError(t, err, "All backups not deleted on the source cluster post migration-backup-restore: %v.", err)

		err = storkops.Instance().DeleteBackupLocation(currBackupLocation.Name, currBackupLocation.Namespace)
		require.NoError(t, err, "Failed to delete  backup location %s on source cluster: %v.", currBackupLocation.Name, err)

		logrus.Infoln("Completed cleanup on destination cluster")

		err = setRemoteConfig("")
		require.NoError(t, err, "Error resetting remote config")
	}
}
