//go:build integrationtest
// +build integrationtest

package integrationtest

import (
	"fmt"
	"testing"
	"time"

	storkv1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/libopenstorage/stork/pkg/log"
	storkops "github.com/portworx/sched-ops/k8s/stork"
	"github.com/portworx/torpedo/drivers/scheduler"
)

func TestMigrationBackup(t *testing.T) {
	// reset mock time before running any tests
	err := setMockTime(nil)
	log.FailOnError(t, err, "Error resetting mock time")

	setDefaultsForBackup(t)
	currentTestSuite = t.Name()

	log.InfoD("Using stork volume driver: %s", volumeDriverName)
	log.InfoD("Backup path being used: %s", backupLocationPath)

	t.Run("deploymentMigrationBackupTest", deploymentMigrationBackupTest)
}

func deploymentMigrationBackupTest(t *testing.T) {
	var testrailID, testResult = 54209, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	defer updateDashStats(t.Name(), &testResult)

	for location, secret := range allConfigMap {
		log.InfoD("Backing up to cloud: %v using secret %v", location, secret)
		var err error

		err = setSourceKubeConfig()
		log.FailOnError(t, err, "failed to set kubeconfig to source cluster: %v", err)

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
			false,
			"",
			nil,
		)

		// Cleanup up source
		validateAndDestroyMigration(t, ctxs, "mysql-migration", "mysql-1-pvc", preMigrationCtx, true, true, true, false, true, false, nil, nil)

		log.Info("Completed migration of apps from source to destination, will now start backup on second cluster")

		// Change kubeconfig to destination cluster
		err = setDestinationKubeConfig()
		log.FailOnError(t, err, "failed to set kubeconfig to source cluster: %v", err)

		// Backup app from the destination cluster to cloud
		currBackupLocation, err := createBackupLocation(t, appKey+"-backup-location", testKey+"-mysql-migration", storkv1.BackupLocationType(location), secret)
		log.FailOnError(t, err, "Error creating backuplocation %s", currBackupLocation.Name)

		currBackup, err := createApplicationBackupWithAnnotation(t, "dest-backup", testKey+"-mysql-migration", currBackupLocation)
		log.FailOnError(t, err, "Error creating app backups")

		err = waitForAppBackupCompletion(currBackup.Name, currBackup.Namespace, applicationBackupSyncRetryTimeout)
		log.FailOnError(t, err, "Application backup %s in namespace %s failed.", currBackup.Name, currBackup.Namespace)

		destroyAndWait(t, []*scheduler.Context{preMigrationCtx})

		log.Info("Completed backup of apps destination cluster")

		// Switch kubeconfig to source cluster for restore
		err = setSourceKubeConfig()
		log.FailOnError(t, err, "failed to set kubeconfig to source cluster: %v", err)

		srcBackupLocation, err := createBackupLocation(t, appKey+"-backup-location", testKey+"-mysql-migration", storkv1.BackupLocationType(location), secret)
		log.FailOnError(t, err, "Error creating backuplocation %s", currBackupLocation.Name)

		// Set sync to true on first cluster so that backup from second cluster is available
		srcBackupLocation.Location.Sync = true
		srcBackupLocation, err = storkops.Instance().UpdateBackupLocation(srcBackupLocation)
		log.FailOnError(t, err, "Failed to set backup-location sync to true")

		backupToRestore, err := getSyncedBackupWithAnnotation(currBackup, backupSyncAnnotation)
		Dash.VerifyFatal(t, backupToRestore != nil, true, "Backup found on the second cluster")
		log.FailOnError(t, err, "Error getting synced backup-location")

		appRestoreForBackup, err := createApplicationRestore(t, "restore-from-dest-backup", srcBackupLocation.Namespace, backupToRestore, srcBackupLocation)
		Dash.VerifyFatal(t, err, nil, fmt.Sprintf("Create restore object in namespace %s", srcBackupLocation.Namespace))
		Dash.VerifyFatal(t, appRestoreForBackup != nil, true, fmt.Sprintf("Restore backup in namespace %s", srcBackupLocation.Namespace))

		err = schedulerDriver.WaitForRunning(preMigrationCtx, defaultWaitTimeout, defaultWaitInterval)
		log.FailOnError(t, err, "Error waiting for app on source cluster post restore from dest backup")

		log.Info("Completed restore of apps on source cluster")

		// Set sync to false on first cluster so that no more backups are sync'ed
		srcBackupLocation.Location.Sync = false
		srcBackupLocation, err = storkops.Instance().UpdateBackupLocation(srcBackupLocation)
		log.FailOnError(t, err, "Failed to set backup-location sync to true")

		time.Sleep(time.Second * 30)

		err = deleteAndWaitForBackupDeletion(srcBackupLocation.Namespace)
		log.FailOnError(t, err, "All backups not deleted on the source cluster post migration-backup-restore: %v.", err)

		time.Sleep(time.Second * 30)

		err = storkops.Instance().DeleteBackupLocation(srcBackupLocation.Name, srcBackupLocation.Namespace)
		log.FailOnError(t, err, "Failed to delete  backup location %s on source cluster: %v.", srcBackupLocation.Name, err)

		destroyAndWait(t, []*scheduler.Context{preMigrationCtx})

		err = deleteApplicationRestoreList([]*storkv1.ApplicationRestore{appRestoreForBackup})
		log.FailOnError(t, err, "failure to delete application restore %s: %v", appRestoreForBackup.Name, err)

		log.Info("Completed cleanup on source cluster")

		// Change kubeconfig to second cluster
		err = setDestinationKubeConfig()
		log.FailOnError(t, err, "failed to set kubeconfig to source cluster: %v", err)

		err = deleteAndWaitForBackupDeletion(currBackup.Namespace)
		log.FailOnError(t, err, "All backups not deleted on the source cluster post migration-backup-restore: %v.", err)

		err = storkops.Instance().DeleteBackupLocation(currBackupLocation.Name, currBackupLocation.Namespace)
		log.FailOnError(t, err, "Failed to delete  backup location %s on source cluster: %v.", currBackupLocation.Name, err)

		log.Info("Completed cleanup on destination cluster")

		err = setRemoteConfig("")
		log.FailOnError(t, err, "Error resetting remote config")
	}

	// If we are here then the test has passed
	testResult = testResultPass
	log.InfoD("Test status at end of %s test: %s", t.Name(), testResult)
}
