//go:build integrationtest
// +build integrationtest

package integrationtest

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/libopenstorage/stork/pkg/log"
	"github.com/portworx/sched-ops/k8s/core"
	storkops "github.com/portworx/sched-ops/k8s/stork"
	"github.com/portworx/sched-ops/task"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/portworx/torpedo/drivers/scheduler/spec"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"

	storkv1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
)

const (
	testKey              = "mysql-1-pvc"
	appKey               = "mysql"
	restoreName          = "mysql-restore"
	backupSyncAnnotation = "backupsync"
	configMapName        = "secret-config"
	s3SecretName         = "s3secret"
	azureSecretName      = "azuresecret"
	googleSecretName     = "googlesecret"
	prepare              = "prepare"
	fio                  = "fio"
	verify               = "verify"
	secretNameKey        = "secret_name"
	secretNamespaceKey   = "secret_namespace"
	secretName           = "openstorage.io/auth-secret-name"
	secretNamespace      = "openstorage.io/auth-secret-namespace"

	applicationBackupScheduleRetryInterval = 10 * time.Second
	applicationBackupScheduleRetryTimeout  = 5 * time.Minute
	applicationBackupSyncRetryTimeout      = 21 * time.Minute
)

var allConfigMap, defaultConfigMap map[string]string
var defaultBackupLocation storkv1.BackupLocationType
var defaultSecretName string
var defaultsBackupSet bool

func TestApplicationBackup(t *testing.T) {
	setDefaultsForBackup(t)
	currentTestSuite = t.Name()

	log.InfoD("Using stork volume driver: %s", volumeDriverName)
	log.InfoD("Backup path being used: %s", backupLocationPath)

	err := setSourceKubeConfig()
	log.FailOnError(t, err, "failed to set kubeconfig to source cluster: %v", err)

	t.Run("applicationBackupRestoreTest", applicationBackupRestoreTest)
	t.Run("applicationBackupRestoreWithoutNMTest", applicationBackupRestoreWithoutNMTest)
	t.Run("applicationBackupDelBackupLocation", applicationBackupDelBackupLocation)
	t.Run("applicationBackupMultiple", applicationBackupMultiple)
	t.Run("preExecRuleTest", applicationBackupRestorePreExecRuleTest)
	t.Run("postExecRuleTest", applicationBackupRestorePostExecRuleTest)
	t.Run("preExecMissingRuleTest", applicationBackupRestorePreExecMissingRuleTest)
	t.Run("postExecMissingRuleTest", applicationBackupRestorePostExecMissingRuleTest)
	t.Run("preExecFailingRuleTest", applicationBackupRestorePreExecFailingRuleTest)
	t.Run("postExecFailingRuleTest", applicationBackupRestorePostExecFailingRuleTest)
	t.Run("labelSelector", applicationBackupLabelSelectorTest)
	t.Run("scheduleTests", applicationBackupScheduleTests)
	t.Run("backupSyncController", applicationBackupSyncControllerTest)

	err = setRemoteConfig("")
	log.FailOnError(t, err, "setting kubeconfig to default failed")
}

func TestScaleApplicationBackup(t *testing.T) {
	var testrailID, testResult = 50785, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)

	if !defaultsBackupSet {
		setDefaultsForBackup(t)
	}
	err := setSourceKubeConfig()
	log.FailOnError(t, err, "failed to set kubeconfig to source cluster for scale tests: %v", err)

	t.Run("scaleApplicationBackupRestore", scaleApplicationBackupRestore)

	err = setRemoteConfig("")
	log.FailOnError(t, err, "setting kubeconfig to default failed")
}

func triggerBackupRestoreTest(
	t *testing.T,
	appBackupKey []string,
	additionalAppKeys []string,
	appRestoreKey []string,
	configMap map[string]string,
	createBackupLocationFlag bool,
	backupSuccessExpected bool,
	backupAllAppsExpected bool,
) {
	for location, secret := range configMap {
		log.InfoD("Backing up to cloud: %v using secret %v", location, secret)
		var currBackupLocation *storkv1.BackupLocation
		var err error
		var ctxs []*scheduler.Context
		var appBackup *storkv1.ApplicationBackup

		ctx := createApp(t, appBackupKey[0])
		ctxs = append(ctxs, ctx)
		var restoreCtx = &scheduler.Context{
			UID: ctx.UID,
			App: &spec.AppSpec{
				Key:      ctx.App.Key,
				SpecList: []interface{}{},
			}}

		// Track what has been backed up
		preBackupCtx := ctxs[0].DeepCopy()

		// Track what has to be verified post restore (skips apps that will won't be restored due to label selectors)
		postRestoreCtx := preBackupCtx
		postRestoreCtx.ScheduleOptions.Namespace = ctx.ScheduleOptions.Namespace

		if len(additionalAppKeys) > 0 {
			err = schedulerDriver.AddTasks(ctxs[0],
				scheduler.ScheduleOptions{AppKeys: additionalAppKeys, Scheduler: schedulerName})
			log.FailOnError(t, err, "Error scheduling additional apps")
			err = schedulerDriver.WaitForRunning(ctxs[0], defaultWaitTimeout, defaultWaitInterval)
			log.FailOnError(t, err, "Error waiting for additional apps to get to running state")
			if backupAllAppsExpected {
				preBackupCtx = ctxs[0].DeepCopy()
			}
		}

		// Track contexts that will be destroyed before restore
		preRestoreCtx := ctxs[0].DeepCopy()

		// Add preparation pods after app context snapshot is ready
		log.InfoD("Prepare app %s for running", appKey)
		prepareVerifyApp(t, ctxs, appKey, fio)

		log.InfoD("All Apps created %v. Starting backup.", ctx.GetID())

		// Create backuplocation here programatically using config-map that contains name of secrets to be used, passed from the CLI
		if createBackupLocationFlag {
			currBackupLocation, err = createBackupLocation(t, appBackupKey[0]+"-backup-location", ctx.GetID(), storkv1.BackupLocationType(location), secret)
			log.FailOnError(t, err, "Error creating backuplocation")
		}

		// Backup application
		if backupSuccessExpected {
			if backupAllAppsExpected {
				err = schedulerDriver.AddTasks(ctxs[0], scheduler.ScheduleOptions{AppKeys: appBackupKey, Scheduler: schedulerName})
				log.FailOnError(t, err, "Error creating app backups")
				err = schedulerDriver.WaitForRunning(ctxs[0], defaultWaitTimeout, defaultWaitInterval)
			} else {
				err = schedulerDriver.AddTasks(postRestoreCtx, scheduler.ScheduleOptions{AppKeys: appBackupKey, Scheduler: schedulerName})
				log.FailOnError(t, err, "Error creating app backups")
				err = schedulerDriver.WaitForRunning(postRestoreCtx, defaultWaitTimeout, defaultWaitInterval)
			}
			log.FailOnError(t, err, "Error waiting for back-up to complete.")
			log.InfoD("Backup completed.")

			appBackedup, err := storkops.Instance().GetApplicationBackup(appBackupKey[0], ctx.GetID())
			log.FailOnError(t, err, "Failed to get application backup: %s in namespace: %v", appBackupKey[0], ctx.GetID())

			resourcesBackedup := appBackedup.Status.ResourceCount

			// Delete apps so that they can be restored
			destroyAndWait(t, []*scheduler.Context{preRestoreCtx})

			log.InfoD("Starting Restore.")
			// Restore application
			restoreCtx.ScheduleOptions.Namespace = ctx.ScheduleOptions.Namespace
			err = schedulerDriver.AddTasks(restoreCtx,
				scheduler.ScheduleOptions{AppKeys: appRestoreKey})
			log.FailOnError(t, err, "Error restoring apps")

			err = schedulerDriver.WaitForRunning(restoreCtx, defaultWaitTimeout, defaultWaitInterval)
			log.FailOnError(t, err, "Error waiting for restore to complete.")

			log.InfoD("Restore completed.")

			appRestored, err := storkops.Instance().GetApplicationRestore(appRestoreKey[0], ctx.GetID())
			log.FailOnError(t, err, "Failed to get application restore: %s in namespace: %v", appRestoreKey[0], ctx.GetID())

			resourcesRestored := appRestored.Status.RestoredResourceCount
			log.InfoD("resourcesBackedup: %v, resourcesRestored: %v", resourcesBackedup, resourcesRestored)
			Dash.VerifyFatal(t, resourcesBackedup, resourcesRestored, fmt.Sprintf("Restore unsuccessful as backup resources are %v and restore resources are %v", resourcesBackedup, resourcesRestored))

			// Validate that restore results in restoration of correct apps based on whether all apps were expected or not
			err = schedulerDriver.WaitForRunning(preBackupCtx, defaultWaitTimeout, defaultWaitInterval)
			log.FailOnError(t, err, "Error waiting for restore to complete.")

			log.InfoD("App validations after restore completed.")

		} else { // Since backup is expected to fail, reducing the wait time here to catch the error faster
			err = schedulerDriver.AddTasks(ctxs[0], scheduler.ScheduleOptions{AppKeys: appBackupKey, Scheduler: schedulerName})
			log.FailOnError(t, err, "Error backing-up apps")
			err = schedulerDriver.WaitForRunning(ctxs[0], defaultWaitTimeout/5, defaultWaitInterval)
			log.FailOnNoError(t, err, "Backup expected to fail in test: %s.", t.Name())
		}

		if backupSuccessExpected {
			ctxs = append(ctxs, restoreCtx)
			log.InfoD("Verify app %s is running", appKey)
			//TODO: Enable verification of data, sysbench verification fails currently because,
			// after restore sysbench-verify is in 'Error' state
			//prepareVerifyApp(t, ctxs, appKey, verify)
		}

		// Get application backup object to be used for validation of cloud delete later
		appBackup, err = storkops.Instance().GetApplicationBackup(appBackupKey[0], ctx.GetID())
		log.FailOnError(t, err, "Failed to get application backup: %s in namespace: %s, for cloud deletion validation", appBackupKey[0], ctx.GetID())

		if (backupAllAppsExpected && backupSuccessExpected) || !backupSuccessExpected {
			destroyAndWait(t, ctxs)
		} else if !backupAllAppsExpected && backupSuccessExpected {
			// Some apps might have been already destroyed, destroy the remaining
			destroyAndWait(t, []*scheduler.Context{postRestoreCtx})

		}
		if backupSuccessExpected {
			validateBackupDeletionFromObjectstore(t, currBackupLocation, appBackup.Status.BackupPath)
		}

		err = storkops.Instance().DeleteBackupLocation(currBackupLocation.Name, currBackupLocation.Namespace)
		log.FailOnError(t, err, "Failed to delete backuplocation: %s for location %s. Err: %v", currBackupLocation.Name, string(location), err)
	}
}

func prepareVerifyApp(t *testing.T, ctxs []*scheduler.Context, appKey, action string) {
	// Prepare application with data
	err := schedulerDriver.AddTasks(ctxs[0],
		scheduler.ScheduleOptions{AppKeys: []string{fmt.Sprintf("%s-%s", appKey, action)}, Scheduler: schedulerName})
	log.FailOnError(t, err, "Error scheduling %s apps", action)

	err = schedulerDriver.WaitForRunning(ctxs[0], defaultWaitTimeout, defaultWaitInterval)
	log.FailOnError(t, err, "Error waiting for %s apps to get to running state", action)

}

func validateBackupDeletionFromObjectstore(t *testing.T, backupLocation *storkv1.BackupLocation, backupPath string) {
	if !cloudDeletionValidate {
		// validation of deletion of app backups from cloud not requested so returning
		return
	}
	err := objectStoreDriver.ValidateBackupsDeletedFromCloud(backupLocation, backupPath)
	log.FailOnError(t, err, "Failed to validate deletion of backups in bucket for backup location %s in path %s", backupLocation.Name, backupPath)

	log.InfoD("Verified deletion of backup %s from cloud", backupPath)
}

func triggerScaleBackupRestoreTest(
	t *testing.T,
	appKey []string,
	appBackupKey []string,
	appRestoreKey []string,
	backupLocationType storkv1.BackupLocationType,
	createBackupLocationFlag bool,
	backupSuccessExpected bool,
	backupAllAppsExpected bool,
	secretName string,
) {
	var appCtxs []*scheduler.Context
	var appBkps []*storkv1.ApplicationBackup
	var bkpLocations []*storkv1.BackupLocation
	var appRestores []*storkv1.ApplicationRestore

	backuplocationPrefix := "backuplocation-"
	appbackupPrefix := testKey + "-backup-scale-"
	restorePrefix := "mysql-restore-scale-"
	for i := 0; i < backupScaleCount; i++ {
		currCtx, err := schedulerDriver.Schedule("scale-"+strconv.Itoa(i),
			scheduler.ScheduleOptions{AppKeys: appBackupKey, Scheduler: schedulerName})
		log.FailOnError(t, err, "Error scheduling task")
		Dash.VerifyFatal(t, 1, len(currCtx), "Only one task should have started")
		appCtxs = append(appCtxs, currCtx...)

		// Without the sleep, sometimes the mysql pods fail with permission denied error
		time.Sleep(time.Second * 10)
	}

	for i, app := range appCtxs {
		err := schedulerDriver.WaitForRunning(app, defaultWaitTimeout, defaultWaitInterval)
		log.FailOnError(t, err, "Error waiting for app in namespace %s to get to running state", "scale-"+strconv.Itoa(i))
	}
	// Trigger backups for all apps created
	for idx, app := range appCtxs {
		backupLocation, err := createBackupLocation(t, backuplocationPrefix+strconv.Itoa(idx), app.GetID(), backupLocationType, secretName)
		log.FailOnError(t, err, "Error creating backuplocation: %s", "backupLocation-"+strconv.Itoa(idx))
		bkpLocations = append(bkpLocations, backupLocation)

		currBackup, err := createApplicationBackupWithAnnotation(t, appbackupPrefix+strconv.Itoa(idx), app.GetID(), backupLocation)
		log.FailOnError(t, err, "Error creating app backups")

		appBkps = append(appBkps, currBackup)
	}

	// Wait for all backups to be completed
	for _, bkp := range appBkps {
		log.InfoD("Verifying backup in namespace %s", bkp.Namespace)
		err := waitForAppBackupCompletion(bkp.Name, bkp.Namespace, applicationBackupSyncRetryTimeout)
		log.FailOnError(t, err, "Application backup %s in namespace %s failed.", bkp.Name, bkp.Namespace)
	}

	log.InfoD("Deleting all apps before restoring them.")
	destroyAndWait(t, appCtxs)

	// Create restore objects and restore all backups in their namespaces
	for idx, bkp := range appBkps {
		log.InfoD("Creating application restore in namespace %s", bkp.Namespace)
		appRestoreForBackup, err := createApplicationRestore(t, restorePrefix+strconv.Itoa(idx), bkp.Namespace, bkp, bkpLocations[idx])
		Dash.VerifyFatal(t, err == nil, true, fmt.Sprintf("Create restore object in namespace %s", bkp.Namespace))
		Dash.VerifyFatal(t, appRestoreForBackup != nil, true, fmt.Sprintf("restore bkp in namespace %s", "mysql-restore-scale-"+strconv.Itoa(idx)))

		appRestores = append(appRestores, appRestoreForBackup)
	}

	// Wait for all apps to be running
	for i, app := range appCtxs {
		err := schedulerDriver.WaitForRunning(app, defaultWaitTimeout, defaultWaitInterval)
		log.FailOnError(t, err, "Error waiting for app in namespace %s to get to running state", "scale-"+strconv.Itoa(i))
	}
	// Cleanup
	for idx, bkp := range appBkps {
		// Get backuplocation so it can be used in the invocation of the validateBackupDeletionFromObjectstore method
		currBackupLocation, err := storkops.Instance().GetBackupLocation("backuplocation-"+strconv.Itoa(idx), bkp.Namespace)
		log.FailOnError(t, err, "Failed to get backup location %s in namespace %s: %v prior to deletion", "backuplocation-"+strconv.Itoa(idx), bkp.Namespace, err)

		bkpUpdated, err := storkops.Instance().GetApplicationBackup(bkp.Name, bkp.Namespace)
		log.FailOnError(t, err, "Failed to get application backup: %s in namespace: %s, for cloud deletion validation", bkp.Name, bkp.Namespace)

		err = deleteAndWaitForBackupDeletion(bkp.Namespace)
		log.FailOnError(t, err, "All backups not deleted in namespace %s: %v", bkp.Namespace, err)

		validateBackupDeletionFromObjectstore(t, currBackupLocation, bkpUpdated.Status.BackupPath)

		err = storkops.Instance().DeleteBackupLocation("backuplocation-"+strconv.Itoa(idx), bkp.Namespace)
		log.FailOnError(t, err, "Failed to delete  backup location %s in namespace %s: %v", "backuplocation-"+strconv.Itoa(idx), bkp.Namespace, err)

	}

	err := deleteApplicationRestoreList(appRestores)
	Dash.VerifyFatal(t, err == nil, true, "Delete application restore list")

	destroyAndWait(t, appCtxs)
}

func createBackupLocation(
	t *testing.T,
	name string,
	namespace string,
	locationtype storkv1.BackupLocationType,
	secretName string,
) (*storkv1.BackupLocation, error) {

	secretObj, err := core.Instance().GetSecret(secretName, "default")
	log.FailOnError(t, err, "Failed to get secret %s", secretName)

	// copy secret to the app namespace
	newSecretObj := secretObj.DeepCopy()
	newSecretObj.Namespace = namespace
	newSecretObj.ResourceVersion = ""
	_, err = core.Instance().CreateSecret(newSecretObj)
	// Ignore if secret already exists
	if err != nil && !errors.IsAlreadyExists(err) {
		log.FailOnError(t, err, "Failed to copy secret %s  to namespace %s", name, namespace)
	}

	backupType, err := getBackupLocationType(locationtype)
	log.FailOnError(t, err, "Failed to get backuptype for %s - Error: %v", locationtype, err)
	backupLocation := &storkv1.BackupLocation{
		ObjectMeta: meta.ObjectMeta{
			Name:        name,
			Namespace:   namespace,
			Annotations: map[string]string{"stork.libopenstorage.ord/skipresource": "true"},
		},
		Location: storkv1.BackupLocationItem{
			Type:         backupType,
			Path:         backupLocationPath,
			SecretConfig: secretObj.Name,
		},
	}

	if authTokenConfigMap != "" {
		err := addSecurityAnnotation(backupLocation)
		if err != nil {
			return nil, err
		}
	}

	backupLocation, err = storkops.Instance().CreateBackupLocation(backupLocation)
	if err != nil {
		return nil, err
	}

	// Doing a "Get" on the backuplocation created to add any missing info from the secrets,
	// that might be required to later get buckets from the cloud objectstore
	backupLocation, err = storkops.Instance().GetBackupLocation(backupLocation.Name, backupLocation.Namespace)
	if err != nil {
		return nil, err
	}
	return backupLocation, nil
}

func createApplicationRestore(
	t *testing.T,
	name string,
	namespace string,
	backup *storkv1.ApplicationBackup,
	backupLocation *storkv1.BackupLocation,
) (*storkv1.ApplicationRestore, error) {
	namespaceMapping := map[string]string{namespace: namespace}

	appRestore := &storkv1.ApplicationRestore{
		ObjectMeta: meta.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: storkv1.ApplicationRestoreSpec{
			BackupName:       backup.Name,
			BackupLocation:   backupLocation.Name,
			NamespaceMapping: namespaceMapping,
		},
	}

	if authTokenConfigMap != "" {
		err := addSecurityAnnotation(appRestore)
		if err != nil {
			return nil, err
		}
	}

	return storkops.Instance().CreateApplicationRestore(appRestore)
}

func createApplicationBackupWithAnnotation(
	t *testing.T,
	name string,
	namespace string,
	backupLocation *storkv1.BackupLocation,
) (*storkv1.ApplicationBackup, error) {

	appBackup := &storkv1.ApplicationBackup{
		ObjectMeta: meta.ObjectMeta{
			Name:        name,
			Namespace:   namespace,
			Annotations: generateTimestampAnnotationMap(backupSyncAnnotation),
		},
		Spec: storkv1.ApplicationBackupSpec{
			Namespaces:     []string{namespace},
			BackupLocation: backupLocation.Name,
		},
	}

	if authTokenConfigMap != "" {
		err := addSecurityAnnotation(appBackup)
		if err != nil {
			return nil, err
		}
	}

	return storkops.Instance().CreateApplicationBackup(appBackup)
}

func generateTimestampAnnotationMap(annotationKey string) map[string]string {
	t := time.Now()
	val := t.Format(time.RFC1123)
	annotationMap := map[string]string{annotationKey: val}
	log.InfoD("Annotations created to track backup: %v", annotationMap)
	return annotationMap
}

func getBackupFromListWithAnnotations(backupList *storkv1.ApplicationBackupList, annotationValue string) *storkv1.ApplicationBackup {
	for _, backup := range backupList.Items {
		if backup.Annotations != nil {
			for k, v := range backup.Annotations {
				if k == backupSyncAnnotation && v == annotationValue {
					log.InfoD("Backup with annotations found: %s", backup.Name)
					return &backup
				}
			}
		}
	}
	return nil
}

func applicationBackupRestoreTest(t *testing.T) {
	var testrailID, testResult = 50849, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	defer updateDashStats(t.Name(), &testResult)

	triggerBackupRestoreTest(
		t,
		[]string{testKey + "-simple-backup"},
		[]string{},
		[]string{"mysql-simple-restore"},
		allConfigMap,
		true,
		true,
		true,
	)

	// If we are here then the test has passed
	testResult = testResultPass
	log.InfoD("Test status at end of %s test: %s", t.Name(), testResult)
}

func applicationBackupRestoreWithoutNMTest(t *testing.T) {
	var testrailID, testResult = 50849, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	defer updateDashStats(t.Name(), &testResult)

	triggerBackupRestoreTest(
		t,
		[]string{testKey + "-simple-backup"},
		[]string{},
		[]string{"mysql-simple-restore-without-nm"},
		allConfigMap,
		true,
		true,
		true,
	)

	// If we are here then the test has passed
	testResult = testResultPass
	log.InfoD("Test status at end of %s test: %s", t.Name(), testResult)
}

func applicationBackupRestorePreExecRuleTest(t *testing.T) {
	var testrailID, testResult = 50850, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	defer updateDashStats(t.Name(), &testResult)

	triggerBackupRestoreTest(
		t,
		[]string{testKey + "-pre-exec-rule-backup"},
		[]string{},
		[]string{"mysql-pre-exec-rule-restore"},
		defaultConfigMap,
		true,
		true,
		true,
	)

	// If we are here then the test has passed
	testResult = testResultPass
	log.InfoD("Test status at end of %s test: %s", t.Name(), testResult)
}

func applicationBackupRestorePostExecRuleTest(t *testing.T) {
	var testrailID, testResult = 50851, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	defer updateDashStats(t.Name(), &testResult)

	triggerBackupRestoreTest(
		t,
		[]string{testKey + "-post-exec-rule-backup"},
		[]string{},
		[]string{"mysql-post-exec-rule-restore"},
		defaultConfigMap,
		true,
		true,
		true,
	)

	// If we are here then the test has passed
	testResult = testResultPass
	log.InfoD("Test status at end of %s test: %s", t.Name(), testResult)
}

func applicationBackupRestorePreExecMissingRuleTest(t *testing.T) {
	var testrailID, testResult = 50852, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	defer updateDashStats(t.Name(), &testResult)

	triggerBackupRestoreTest(
		t,
		[]string{testKey + "-pre-exec-missing-rule-backup"},
		[]string{},
		[]string{"mysql-pre-exec-missing-rule-restore"},
		defaultConfigMap,
		true,
		false,
		false,
	)

	// If we are here then the test has passed
	testResult = testResultPass
	log.InfoD("Test status at end of %s test: %s", t.Name(), testResult)
}

func applicationBackupRestorePostExecMissingRuleTest(t *testing.T) {
	var testrailID, testResult = 50853, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	defer updateDashStats(t.Name(), &testResult)

	triggerBackupRestoreTest(
		t,
		[]string{testKey + "-post-exec-missing-rule-backup"},
		[]string{},
		[]string{"mysql-post-exec-missing-rule-restore"},
		defaultConfigMap,
		true,
		false,
		false,
	)

	// If we are here then the test has passed
	testResult = testResultPass
	log.InfoD("Test status at end of %s test: %s", t.Name(), testResult)
}

func applicationBackupRestorePreExecFailingRuleTest(t *testing.T) {
	var testrailID, testResult = 50854, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	defer updateDashStats(t.Name(), &testResult)

	triggerBackupRestoreTest(
		t,
		[]string{testKey + "-pre-exec-failing-rule-backup"},
		[]string{},
		[]string{"mysql-pre-exec-failing-rule-restore"},
		defaultConfigMap,
		true,
		false,
		false,
	)

	// If we are here then the test has passed
	testResult = testResultPass
	log.InfoD("Test status at end of %s test: %s", t.Name(), testResult)
}

func applicationBackupRestorePostExecFailingRuleTest(t *testing.T) {
	var testrailID, testResult = 50855, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	defer updateDashStats(t.Name(), &testResult)

	triggerBackupRestoreTest(
		t,
		[]string{testKey + "-post-exec-failing-rule-backup"},
		[]string{},
		[]string{"mysql-post-exec-failing-rule-restore"},
		defaultConfigMap,
		true,
		false,
		false,
	)

	// If we are here then the test has passed
	testResult = testResultPass
	log.InfoD("Test status at end of %s test: %s", t.Name(), testResult)
}

func applicationBackupLabelSelectorTest(t *testing.T) {
	var testrailID, testResult = 50856, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	defer updateDashStats(t.Name(), &testResult)

	triggerBackupRestoreTest(
		t,
		[]string{testKey + "-label-selector-backup"},
		[]string{"cassandra"},
		[]string{"mysql-label-selector-restore"},
		defaultConfigMap,
		true,
		true,
		false,
	)

	// If we are here then the test has passed
	testResult = testResultPass
	log.InfoD("Test status at end of %s test: %s", t.Name(), testResult)
}

func scaleApplicationBackupRestore(t *testing.T) {
	var testrailID, testResult = 50785, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	defer updateDashStats(t.Name(), &testResult)

	triggerScaleBackupRestoreTest(
		t,
		[]string{"mysql-1-pvc"},
		[]string{"mysql-1-pvc"},
		[]string{restoreName},
		defaultBackupLocation,
		true,
		true,
		true,
		defaultSecretName,
	)

	// If we are here then the test has passed
	testResult = testResultPass
	log.InfoD("Test status at end of %s test: %s", t.Name(), testResult)
}

func applicationBackupScheduleTests(t *testing.T) {
	err := setMockTime(nil)
	log.FailOnError(t, err, "Error resetting mock time")
	t.Run("intervalTest", intervalApplicationBackupScheduleTest)
	t.Run("dailyTest", dailyApplicationBackupScheduleTest)
	t.Run("weeklyTest", weeklyApplicationBackupScheduleTest)
	t.Run("monthlyTest", monthlyApplicationBackupScheduleTest)
	t.Run("invalidPolicyTest", invalidPolicyApplicationBackupScheduleTest)
}

func deletePolicyAndApplicationBackupSchedule(t *testing.T, namespace string, policyName string, applicationBackupScheduleName string, expectedBackups int) {
	err := storkops.Instance().DeleteSchedulePolicy(policyName)
	log.FailOnError(t, err, fmt.Sprintf("Error deleting schedule policy %v", policyName))

	err = storkops.Instance().DeleteApplicationBackupSchedule(applicationBackupScheduleName, namespace)
	log.FailOnError(t, err, fmt.Sprintf("Error deleting applicationBackup schedule %v from namespace %v",
		applicationBackupScheduleName, namespace))

	time.Sleep(10 * time.Second)
	applicationBackupList, err := storkops.Instance().ListApplicationBackups(namespace, meta.ListOptions{})
	log.FailOnError(t, err, fmt.Sprintf("Error getting list of applicationBackups for namespace: %v", namespace))
	// sometimes length of backuplist is expected+1 depending on the timing issue
	Dash.VerifyFatal(t, len(applicationBackupList.Items) <= expectedBackups+1, true, fmt.Sprintf("Should have %v ApplicationBackups triggered by schedule in namespace %v", expectedBackups, namespace))
	err = deleteApplicationBackupList(applicationBackupList)
	log.FailOnError(t, err, "failed to delete application backups")
}

func intervalApplicationBackupScheduleTest(t *testing.T) {
	var testrailID, testResult = 86265, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	defer updateDashStats(t.Name(), &testResult)

	backupLocation := "interval-backuplocation"

	ctx := createApp(t, "interval-appbackup-sched-test")

	// Create backuplocation here programatically using config-map that contains name of secrets to be used, passed from the CLI
	_, err := createBackupLocation(t, backupLocation, ctx.GetID(), defaultBackupLocation, defaultSecretName)
	log.FailOnError(t, err, "Error creating backuplocation")

	policyName := "intervalpolicy-appbackup"
	retain := 2
	interval := 2
	schedPolicy := &storkv1.SchedulePolicy{
		ObjectMeta: meta.ObjectMeta{
			Name: policyName,
		},
		Policy: storkv1.SchedulePolicyItem{
			Interval: &storkv1.IntervalPolicy{
				Retain:          storkv1.Retain(retain),
				IntervalMinutes: interval,
			},
		}}

	if authTokenConfigMap != "" {
		err := addSecurityAnnotation(schedPolicy)
		log.FailOnError(t, err, "Error creating interval schedule policy")
	}

	_, err = storkops.Instance().CreateSchedulePolicy(schedPolicy)
	log.FailOnError(t, err, "Error creating interval schedule policy")
	log.InfoD("Created schedulepolicy %v with %v minute interval and retain at %v", policyName, interval, retain)

	scheduleName := "intervalscheduletest"
	namespace := ctx.GetID()
	appBackupSched := &storkv1.ApplicationBackupSchedule{
		ObjectMeta: meta.ObjectMeta{
			Name:      scheduleName,
			Namespace: namespace,
		},
		Spec: storkv1.ApplicationBackupScheduleSpec{
			Template: storkv1.ApplicationBackupTemplateSpec{
				Spec: storkv1.ApplicationBackupSpec{
					Namespaces:     []string{namespace},
					BackupLocation: backupLocation,
					ReclaimPolicy:  storkv1.ApplicationBackupReclaimPolicyDelete,
				},
			},
			SchedulePolicyName: policyName,
		},
	}

	if authTokenConfigMap != "" {
		err := addSecurityAnnotation(appBackupSched)
		log.FailOnError(t, err, "Error creating interval applicationBackup schedule")
	}

	_, err = storkops.Instance().CreateApplicationBackupSchedule(appBackupSched)
	log.FailOnError(t, err, "Error creating interval applicationBackup schedule")
	sleepTime := time.Duration((retain+1)*interval) * time.Minute
	log.InfoD("Created applicationBackupschedule %v in namespace %v, sleeping for %v for schedule to trigger",
		scheduleName, namespace, sleepTime)
	time.Sleep(sleepTime)

	backupStatuses, err := storkops.Instance().ValidateApplicationBackupSchedule("intervalscheduletest",
		namespace,
		retain,
		applicationBackupScheduleRetryTimeout,
		applicationBackupScheduleRetryInterval)
	log.FailOnError(t, err, "Error validating interval applicationBackup schedule")
	Dash.VerifyFatal(t, 1, len(backupStatuses), "Should have applicationBackups for only one policy type")
	Dash.VerifyFatal(t, retain, len(backupStatuses[storkv1.SchedulePolicyTypeInterval]), fmt.Sprintf("Should have only %v applicationBackup for interval policy", retain))
	log.InfoD("Validated applicationBackupschedule %v", scheduleName)

	deletePolicyAndApplicationBackupSchedule(t, namespace, policyName, scheduleName, 2)
	destroyAndWait(t, []*scheduler.Context{ctx})

	// If we are here then the test has passed
	testResult = testResultPass
	log.InfoD("Test status at end of %s test: %s", t.Name(), testResult)
}

func dailyApplicationBackupScheduleTest(t *testing.T) {
	var testrailID, testResult = 86266, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	defer updateDashStats(t.Name(), &testResult)

	backupLocation := "daily-backuplocation"
	ctx := createApp(t, "daily-backup-sched-test")

	// Create backuplocation here programatically using config-map that contains name of secrets to be used, passed from the CLI
	_, err := createBackupLocation(t, backupLocation, ctx.GetID(), defaultBackupLocation, defaultSecretName)
	log.FailOnError(t, err, "Error creating backuplocation")

	policyName := "dailypolicy-appbackup"
	retain := 2
	// Set first trigger 2 minutes from now
	scheduledTime := time.Now().Add(2 * time.Minute)
	nextScheduledTime := scheduledTime.AddDate(0, 0, 1)
	schedPolicy := &storkv1.SchedulePolicy{
		ObjectMeta: meta.ObjectMeta{
			Name: policyName,
		},
		Policy: storkv1.SchedulePolicyItem{
			Daily: &storkv1.DailyPolicy{
				Retain: storkv1.Retain(retain),
				Time:   scheduledTime.Format(time.Kitchen),
			},
		}}

	if authTokenConfigMap != "" {
		err := addSecurityAnnotation(schedPolicy)
		log.FailOnError(t, err, "Error creating daily schedule policy")
	}

	_, err = storkops.Instance().CreateSchedulePolicy(schedPolicy)
	log.FailOnError(t, err, "Error creating daily schedule policy")
	log.InfoD("Created schedulepolicy %v at time %v and retain at %v",
		policyName, scheduledTime.Format(time.Kitchen), retain)

	scheduleName := "dailyscheduletest"
	namespace := ctx.GetID()
	appBackupSched := &storkv1.ApplicationBackupSchedule{
		ObjectMeta: meta.ObjectMeta{
			Name:      scheduleName,
			Namespace: namespace,
		},
		Spec: storkv1.ApplicationBackupScheduleSpec{
			Template: storkv1.ApplicationBackupTemplateSpec{
				Spec: storkv1.ApplicationBackupSpec{
					Namespaces:     []string{namespace},
					BackupLocation: backupLocation,
					ReclaimPolicy:  storkv1.ApplicationBackupReclaimPolicyDelete,
				},
			},
			SchedulePolicyName: policyName,
		},
	}

	if authTokenConfigMap != "" {
		err := addSecurityAnnotation(appBackupSched)
		log.FailOnError(t, err, "Error creating daily applicationBackup schedule")
	}

	_, err = storkops.Instance().CreateApplicationBackupSchedule(appBackupSched)
	log.FailOnError(t, err, "Error creating daily applicationBackup schedule")
	log.InfoD("Created applicationBackupschedule %v in namespace %v",
		scheduleName, namespace)
	commonApplicationBackupScheduleTests(t, scheduleName, policyName, namespace, nextScheduledTime, storkv1.SchedulePolicyTypeDaily)
	destroyAndWait(t, []*scheduler.Context{ctx})

	// If we are here then the test has passed
	testResult = testResultPass
	log.InfoD("Test status at end of %s test: %s", t.Name(), testResult)
}

func weeklyApplicationBackupScheduleTest(t *testing.T) {
	var testrailID, testResult = 86267, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	defer updateDashStats(t.Name(), &testResult)

	backupLocation := "weekly-backuplocation"
	ctx := createApp(t, "weekly-backup-sched-test")

	// Create backuplocation here programatically using config-map that contains name of secrets to be used, passed from the CLI
	_, err := createBackupLocation(t, backupLocation, ctx.GetID(), defaultBackupLocation, defaultSecretName)
	log.FailOnError(t, err, "Error creating backuplocation")

	policyName := "weeklypolicy-appbackup"
	retain := 2
	// Set first trigger 2 minutes from now
	scheduledTime := time.Now().Add(2 * time.Minute)
	nextScheduledTime := scheduledTime.AddDate(0, 0, 7)
	schedPolicy := &storkv1.SchedulePolicy{
		ObjectMeta: meta.ObjectMeta{
			Name: policyName,
		},
		Policy: storkv1.SchedulePolicyItem{
			Weekly: &storkv1.WeeklyPolicy{
				Retain: storkv1.Retain(retain),
				Day:    scheduledTime.Weekday().String(),
				Time:   scheduledTime.Format(time.Kitchen),
			},
		}}

	if authTokenConfigMap != "" {
		err := addSecurityAnnotation(schedPolicy)
		log.FailOnError(t, err, "Error creating weekly schedule policy")
	}

	_, err = storkops.Instance().CreateSchedulePolicy(schedPolicy)
	log.FailOnError(t, err, "Error creating weekly schedule policy")
	log.InfoD("Created schedulepolicy %v at time %v on day %v and retain at %v",
		policyName, scheduledTime.Format(time.Kitchen), scheduledTime.Weekday().String(), retain)

	scheduleName := "weeklyscheduletest"
	namespace := ctx.GetID()
	appBackupSched := &storkv1.ApplicationBackupSchedule{
		ObjectMeta: meta.ObjectMeta{
			Name:      scheduleName,
			Namespace: namespace,
		},
		Spec: storkv1.ApplicationBackupScheduleSpec{
			Template: storkv1.ApplicationBackupTemplateSpec{
				Spec: storkv1.ApplicationBackupSpec{
					Namespaces:     []string{namespace},
					BackupLocation: backupLocation,
					ReclaimPolicy:  storkv1.ApplicationBackupReclaimPolicyDelete,
				},
			},
			SchedulePolicyName: policyName,
		},
	}

	if authTokenConfigMap != "" {
		err := addSecurityAnnotation(appBackupSched)
		log.FailOnError(t, err, "Error creating weekly applicationBackup schedule")
	}

	_, err = storkops.Instance().CreateApplicationBackupSchedule(appBackupSched)
	log.FailOnError(t, err, "Error creating weekly applicationBackup schedule")
	log.InfoD("Created applicationBackupschedule %v in namespace %v",
		scheduleName, namespace)
	commonApplicationBackupScheduleTests(t, scheduleName, policyName, namespace, nextScheduledTime, storkv1.SchedulePolicyTypeWeekly)
	destroyAndWait(t, []*scheduler.Context{ctx})

	// If we are here then the test has passed
	testResult = testResultPass
	log.InfoD("Test status at end of %s test: %s", t.Name(), testResult)
}

func monthlyApplicationBackupScheduleTest(t *testing.T) {
	var testrailID, testResult = 86268, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	defer updateDashStats(t.Name(), &testResult)

	backupLocation := "monthly-backuplocation"
	ctx := createApp(t, "monthly-backup-sched-test")

	// Create backuplocation here programatically using config-map that contains name of secrets to be used, passed from the CLI
	_, err := createBackupLocation(t, backupLocation, ctx.GetID(), defaultBackupLocation, defaultSecretName)
	log.FailOnError(t, err, "Error creating backuplocation")

	policyName := "monthlypolicy-appbackup"
	retain := 2
	// Set first trigger 2 minutes from now
	scheduledTime := time.Now().Add(2 * time.Minute)
	nextScheduledTime := scheduledTime.AddDate(0, 1, 0)
	// Set the time to zero in case the date doesn't exist in the next month
	if nextScheduledTime.Day() != scheduledTime.Day() {
		nextScheduledTime = time.Time{}
	}
	schedPolicy := &storkv1.SchedulePolicy{
		ObjectMeta: meta.ObjectMeta{
			Name: policyName,
		},
		Policy: storkv1.SchedulePolicyItem{
			Monthly: &storkv1.MonthlyPolicy{
				Retain: storkv1.Retain(retain),
				Date:   scheduledTime.Day(),
				Time:   scheduledTime.Format(time.Kitchen),
			},
		}}

	if authTokenConfigMap != "" {
		err := addSecurityAnnotation(schedPolicy)
		log.FailOnError(t, err, "Error creating monthly schedule policy")
	}

	_, err = storkops.Instance().CreateSchedulePolicy(schedPolicy)
	log.FailOnError(t, err, "Error creating monthly schedule policy")
	log.InfoD("Created schedulepolicy %v at time %v on date %v and retain at %v",
		policyName, scheduledTime.Format(time.Kitchen), scheduledTime.Day(), retain)

	scheduleName := "monthlyscheduletest"
	namespace := ctx.GetID()
	appBackupSched := &storkv1.ApplicationBackupSchedule{
		ObjectMeta: meta.ObjectMeta{
			Name:      scheduleName,
			Namespace: namespace,
		},
		Spec: storkv1.ApplicationBackupScheduleSpec{
			Template: storkv1.ApplicationBackupTemplateSpec{
				Spec: storkv1.ApplicationBackupSpec{
					Namespaces:     []string{namespace},
					BackupLocation: backupLocation,
					ReclaimPolicy:  storkv1.ApplicationBackupReclaimPolicyDelete,
				},
			},
			SchedulePolicyName: policyName,
		},
	}

	if authTokenConfigMap != "" {
		err := addSecurityAnnotation(appBackupSched)
		log.FailOnError(t, err, "Error creating monthly applicationBackup schedule")
	}

	_, err = storkops.Instance().CreateApplicationBackupSchedule(appBackupSched)
	log.FailOnError(t, err, "Error creating monthly applicationBackup schedule")
	log.InfoD("Created applicationBackupschedule %v in namespace %v",
		scheduleName, namespace)
	commonApplicationBackupScheduleTests(t, scheduleName, policyName, namespace, nextScheduledTime, storkv1.SchedulePolicyTypeMonthly)
	destroyAndWait(t, []*scheduler.Context{ctx})

	// If we are here then the test has passed
	testResult = testResultPass
	log.InfoD("Test status at end of %s test: %s", t.Name(), testResult)
}

func invalidPolicyApplicationBackupScheduleTest(t *testing.T) {
	var testrailID, testResult = 86269, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	defer updateDashStats(t.Name(), &testResult)

	backupLocation := "invalid-backuplocation"
	ctx := createApp(t, "invalid-backup-sched-test")

	// Create backuplocation here programatically using config-map that contains name of secrets to be used, passed from the CLI
	_, err := createBackupLocation(t, backupLocation, ctx.GetID(), defaultBackupLocation, defaultSecretName)
	log.FailOnError(t, err, "Error creating backuplocation")

	policyName := "invalidpolicy-appbackup"
	scheduledTime := time.Now()
	retain := 2
	_, err = storkops.Instance().CreateSchedulePolicy(&storkv1.SchedulePolicy{
		ObjectMeta: meta.ObjectMeta{
			Name: policyName,
		},
		Policy: storkv1.SchedulePolicyItem{
			Monthly: &storkv1.MonthlyPolicy{
				Retain: storkv1.Retain(retain),
				Date:   scheduledTime.Day(),
				Time:   "13:50PM",
			},
		}})
	log.FailOnError(t, err, "Error creating invalid schedule policy")
	log.InfoD("Created schedulepolicy %v at time %v on date %v and retain at %v",
		policyName, scheduledTime.Format(time.Kitchen), scheduledTime.Day(), retain)

	scheduleName := "invalidpolicyschedule"
	namespace := ctx.GetID()
	_, err = storkops.Instance().CreateApplicationBackupSchedule(&storkv1.ApplicationBackupSchedule{
		ObjectMeta: meta.ObjectMeta{
			Name:      scheduleName,
			Namespace: namespace,
		},
		Spec: storkv1.ApplicationBackupScheduleSpec{
			Template: storkv1.ApplicationBackupTemplateSpec{
				Spec: storkv1.ApplicationBackupSpec{
					Namespaces:     []string{namespace},
					BackupLocation: backupLocation,
					ReclaimPolicy:  storkv1.ApplicationBackupReclaimPolicyDelete,
				},
			},
			SchedulePolicyName: policyName,
		},
	})
	log.FailOnError(t, err, "Error creating applicationBackup schedule with invalid policy")
	log.InfoD("Created applicationBackupschedule %v in namespace %v",
		scheduleName, namespace)
	_, err = storkops.Instance().ValidateApplicationBackupSchedule(scheduleName,
		namespace,
		0,
		3*time.Minute,
		applicationBackupScheduleRetryInterval)
	log.FailOnNoError(t, err, fmt.Sprintf("No applicationBackups should have been created for %v in namespace %v",
		scheduleName, namespace))
	deletePolicyAndApplicationBackupSchedule(t, namespace, policyName, scheduleName, 0)
	destroyAndWait(t, []*scheduler.Context{ctx})

	// If we are here then the test has passed
	testResult = testResultPass
	log.InfoD("Test status at end of %s test: %s", t.Name(), testResult)
}

func commonApplicationBackupScheduleTests(
	t *testing.T,
	scheduleName string,
	policyName string,
	namespace string,
	nextTriggerTime time.Time,
	policyType storkv1.SchedulePolicyType) {
	// Make sure no backup gets created in the next minute
	_, err := storkops.Instance().ValidateApplicationBackupSchedule(scheduleName,
		namespace,
		0,
		1*time.Minute,
		applicationBackupScheduleRetryInterval)
	log.FailOnNoError(t, err, fmt.Sprintf("No backups should have been created for %v in namespace %v",
		scheduleName, namespace))
	sleepTime := time.Duration(1 * time.Minute)
	log.InfoD("Sleeping for %v for schedule to trigger",
		sleepTime)
	time.Sleep(sleepTime)

	backupStatuses, err := storkops.Instance().ValidateApplicationBackupSchedule(scheduleName,
		namespace,
		1,
		applicationBackupScheduleRetryTimeout,
		applicationBackupScheduleRetryInterval)
	log.FailOnError(t, err, "Error validating backup schedule")
	Dash.VerifyFatal(t, 1, len(backupStatuses), "Should have backups for only one policy type")
	Dash.VerifyFatal(t, 1, len(backupStatuses[policyType]), fmt.Sprintf("Should have only one backup for %v schedule", scheduleName))
	log.InfoD("Validated first backupschedule %v", scheduleName)

	// Now advance time to the next trigger if the next trigger is not zero
	if !nextTriggerTime.IsZero() {
		log.InfoD("Updating mock time to %v for next schedule", nextTriggerTime)
		err := setMockTime(&nextTriggerTime)
		log.FailOnError(t, err, "Error setting mock time")
		defer func() {
			err := setMockTime(nil)
			log.FailOnError(t, err, "Error resetting mock time")
		}()
		log.InfoD("Sleeping for 90 seconds for the schedule to get triggered")
		time.Sleep(90 * time.Second)
		backupStatuses, err := storkops.Instance().ValidateApplicationBackupSchedule(scheduleName,
			namespace,
			2,
			applicationBackupScheduleRetryTimeout,
			applicationBackupScheduleRetryInterval)
		log.FailOnError(t, err, "Error validating backup schedule")
		Dash.VerifyFatal(t, 1, len(backupStatuses), "Should have backups for only one policy type")
		Dash.VerifyFatal(t, 2, len(backupStatuses[policyType]), fmt.Sprintf("Should have 2 backups for %v schedule", scheduleName))
		log.InfoD("Validated second backupschedule %v", scheduleName)
	}
	deletePolicyAndApplicationBackupSchedule(t, namespace, policyName, scheduleName, 2)
}

func applicationBackupSyncControllerTest(t *testing.T) {
	var testrailID, testResult = 50858, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	defer updateDashStats(t.Name(), &testResult)

	var err error
	defer func() {
		err = setRemoteConfig("")
		log.FailOnError(t, err, "Error resetting remote config")
	}()
	backupLocationName := appKey + "-backup-location-sync"

	// Create myqsl app deployment
	appCtx := createApp(t, appKey+"-sync")

	// Create backup location on first cluster
	backupLocation, err := createBackupLocation(t, backupLocationName, appCtx.GetID(), defaultBackupLocation, defaultSecretName)
	log.FailOnError(t, err, "Error creating backuplocation")
	log.InfoD("Created backup location:%s sync:%t", backupLocation.Name, backupLocation.Location.Sync)
	backupLocation.Location.Sync = true

	firstBackup, err := createApplicationBackupWithAnnotation(t, appKey+"-backup-sync", appCtx.GetID(), backupLocation)
	log.FailOnError(t, err, "Error creating app backups")

	// Wait for backup completion
	err = waitForAppBackupCompletion(firstBackup.Name, firstBackup.Namespace, applicationBackupSyncRetryTimeout)
	log.FailOnError(t, err, "Application backup failed. Backup name: %s", firstBackup.Name)

	// Create backup location on second cluster
	err = setDestinationKubeConfig()
	log.FailOnError(t, err, "failed to set kubeconfig to source cluster: %v", err)

	// Create namespace for the backuplocation on second cluster
	ns, err := core.Instance().CreateNamespace(&v1.Namespace{
		ObjectMeta: meta.ObjectMeta{
			Name: appCtx.GetID(),
			Labels: map[string]string{
				"creator": "stork-test",
				"app":     appCtx.App.Key,
			},
		},
	})
	log.FailOnError(t, err, "Failed to create namespace %s", appCtx.GetID())

	backupLocation2, err := createBackupLocation(t, backupLocationName, ns.Name, defaultBackupLocation, defaultSecretName)
	log.FailOnError(t, err, "Error creating backuplocation on second cluster")
	log.InfoD("Created backup location on second cluster %s: sync:%t", backupLocation.Name, backupLocation.Location.Sync)

	// Set sync to true on second cluster so that backup location gets synced
	backupLocation2.Location.Sync = true
	_, err = storkops.Instance().UpdateBackupLocation(backupLocation2)
	log.FailOnError(t, err, "Failed to set backup-location sync to true")
	log.InfoD("Updated backup location on 2nd cluster %s: sync:%t", backupLocation2.Name, backupLocation2.Location.Sync)

	backupToRestore, err := getSyncedBackupWithAnnotation(firstBackup, backupSyncAnnotation)
	Dash.VerifyFatal(t, err != nil, true, "Backup found on the second cluster")

	// Create application restore using the backup selected, on second cluster
	log.InfoD("Starting Restore on second cluster.")
	appRestoreForBackup, err := createApplicationRestore(t, "mysql-restore-backup-sync", ns.Name, backupToRestore, backupLocation2)
	Dash.VerifyFatal(t, appRestoreForBackup != nil, true, "Restore on second cluster")
	log.FailOnError(t, err, "Error creating application restore on second cluster")

	log.InfoD("Waiting for apps to come up on the 2nd cluster.")
	err = schedulerDriver.WaitForRunning(appCtx, defaultWaitTimeout, defaultWaitInterval)
	log.FailOnError(t, err, "Error waiting for restore to complete on second cluster.")
	log.InfoD("Restore complete on second cluster.")

	// Delete backup object on second cluster
	err = storkops.Instance().DeleteApplicationBackup(backupToRestore.Name, backupToRestore.Namespace)
	log.FailOnError(t, err, "Failed to delete backup post-restore on second cluster.")

	// Destroy app on first cluster
	log.InfoD("Destroy apps  on first cluster: %v.", appCtx.App.Key)
	err = setSourceKubeConfig()
	log.FailOnError(t, err, "failed to set kubeconfig to source cluster when trying to delete app: %v", err)

	destroyAndWait(t, []*scheduler.Context{appCtx})

	// Restore application on first cluster
	log.InfoD("Starting Restore on first cluster.")
	restoreCtxFirst := &scheduler.Context{
		UID: appCtx.UID,
		App: &spec.AppSpec{
			Key:      appCtx.App.Key,
			SpecList: []interface{}{},
		}}

	restoreCtxFirst.ScheduleOptions.Namespace = appCtx.ScheduleOptions.Namespace
	err = schedulerDriver.AddTasks(restoreCtxFirst,
		scheduler.ScheduleOptions{AppKeys: []string{restoreName + "-backup-sync"}})
	log.FailOnError(t, err, "Error restoring apps")
	err = schedulerDriver.WaitForRunning(restoreCtxFirst, defaultWaitTimeout, defaultWaitInterval)

	log.FailOnError(t, err, "Error waiting for restore to complete on first cluster.")
	log.InfoD("Restore completed on first  cluster.")

	// Check if app is created
	err = schedulerDriver.WaitForRunning(appCtx, defaultWaitTimeout, defaultWaitInterval)
	log.FailOnError(t, err, "App is not running on second cluster post-restore.")

	// Cleanup both clusters
	srcBackupLocation, err := storkops.Instance().GetBackupLocation(backupLocation.Name, backupLocation.Namespace)
	log.FailOnError(t, err, "Failed to get backup-location on first cluster")

	srcBackupLocation.Location.Sync = false
	_, err = storkops.Instance().UpdateBackupLocation(srcBackupLocation)
	log.FailOnError(t, err, "Failed to set backup-location sync to false on first cluster")
	log.InfoD("Updated backup location on first cluster %s: sync:%t", srcBackupLocation.Name, srcBackupLocation.Location.Sync)

	destBackupLocation, err := storkops.Instance().GetBackupLocation(backupLocation2.Name, backupLocation2.Namespace)
	log.FailOnError(t, err, "Failed to get backup-location on second cluster")

	destBackupLocation.Location.Sync = false
	_, err = storkops.Instance().UpdateBackupLocation(destBackupLocation)
	log.FailOnError(t, err, "Failed to set backup-location sync to false on second cluster")
	log.InfoD("Updated backup location on second cluster %s: sync:%t", destBackupLocation.Name, destBackupLocation.Location.Sync)

	time.Sleep(time.Second * 30)

	err = deleteAndWaitForBackupDeletion(ns.Name)
	log.FailOnError(t, err, "All backups not deleted on the first cluster: %v.", err)

	time.Sleep(time.Second * 30)

	err = storkops.Instance().DeleteBackupLocation(srcBackupLocation.Name, ns.Name)
	log.FailOnError(t, err, "Failed to delete  backup location %s on first cluster: %v.", ns.Name, err)

	destroyAndWait(t, []*scheduler.Context{appCtx})

	// Clean up destination cluster
	err = setDestinationKubeConfig()
	log.FailOnError(t, err, "failed to set kubeconfig to source cluster: %v", err)

	err = deleteAndWaitForBackupDeletion(ns.Name)
	log.FailOnError(t, err, "All backups not deleted on the second cluster: %v.", err)

	time.Sleep(time.Second * 30)

	err = storkops.Instance().DeleteBackupLocation(destBackupLocation.Name, ns.Name)
	log.FailOnError(t, err, "Failed to delete  backup location %s on second cluster: %v.", ns.Name, err)

	destroyAndWait(t, []*scheduler.Context{appCtx})
}

func waitForAppBackupCompletion(name, namespace string, timeout time.Duration) error {
	getAppBackup := func() (interface{}, bool, error) {
		appBackup, err := storkops.Instance().GetApplicationBackup(name, namespace)
		if err != nil {
			return "", false, err
		}

		if appBackup.Status.Status != storkv1.ApplicationBackupStatusSuccessful {
			return "", true, fmt.Errorf("App backups %s in %s not complete yet.Retrying", name, namespace)
		}
		return "", false, nil
	}
	_, err := task.DoRetryWithTimeout(getAppBackup, timeout, defaultWaitInterval)
	return err

}

func waitForAppBackupToStart(name, namespace string, timeout time.Duration) error {
	getAppBackup := func() (interface{}, bool, error) {
		appBackup, err := storkops.Instance().GetApplicationBackup(name, namespace)
		if err != nil {
			return "", false, err
		}

		if appBackup.Status.Status != storkv1.ApplicationBackupStatusInProgress {
			return "", true, fmt.Errorf("App backups %s in %s has not started yet.Retrying Status: %s", name, namespace, appBackup.Status.Status)
		}
		return "", false, nil
	}
	_, err := task.DoRetryWithTimeout(getAppBackup, timeout, backupWaitInterval)
	return err

}

func applicationBackupDelBackupLocation(t *testing.T) {
	var testrailID, testResult = 86263, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	defer updateDashStats(t.Name(), &testResult)

	// Create myqsl app deployment
	appCtx := createApp(t, appKey+"-delete-bkp")

	backupLocationName := appKey + "-backup-location"
	// Create backup location on first cluster
	backupLocation, err := createBackupLocation(t, backupLocationName, appCtx.GetID(), defaultBackupLocation, defaultSecretName)
	log.FailOnError(t, err, "Error creating backuplocation")

	appBackup, err := createApplicationBackupWithAnnotation(t, appKey+"-backup", appCtx.GetID(), backupLocation)
	log.FailOnError(t, err, "Error creating app backups")

	// Wait for backup completion
	err = waitForAppBackupToStart(appBackup.Name, appBackup.Namespace, applicationBackupScheduleRetryTimeout)
	log.FailOnError(t, err, "Application backup %s failed to start", appBackup.Name)

	// Application backup started, delete backuplocation
	err = storkops.Instance().DeleteBackupLocation(backupLocation.Name, backupLocation.Namespace)
	log.FailOnError(t, err, "Failed to delete backuplocation: %s", backupLocation.Name)

	err = waitForAppBackupCompletion(appBackup.Name, appBackup.Namespace, applicationBackupScheduleRetryTimeout/time.Duration(5))
	log.FailOnNoError(t, err, "Application backup %s in namespace %s should have failed.", backupLocation.Name, backupLocation.Namespace)

	err = deleteAndWaitForBackupDeletion(appBackup.Namespace)
	log.FailOnError(t, err, "Backups %s not deleted: %s", appBackup.Name, err)

	destroyAndWait(t, []*scheduler.Context{appCtx})
}

func applicationBackupMultiple(t *testing.T) {
	var testrailID, testResult = 86264, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	defer updateDashStats(t.Name(), &testResult)

	// Create myqsl app deployment
	appCtx := createApp(t, appKey+"-multiple-backup")

	backupLocationName := appKey + "-backup-location"
	// Create backup location
	backupLocation, err := createBackupLocation(t, backupLocationName, appCtx.GetID(), defaultBackupLocation, defaultSecretName)
	log.FailOnError(t, err, "Error creating backuplocation")

	// Create first application backup
	appBackup, err := createApplicationBackupWithAnnotation(t, appKey+"-backup", appCtx.GetID(), backupLocation)
	log.FailOnError(t, err, "Error creating app backups")

	// Wait for backup to start
	err = waitForAppBackupToStart(appBackup.Name, appBackup.Namespace, applicationBackupScheduleRetryTimeout)
	log.FailOnError(t, err, "Application backup %s failed to start", appBackup.Name)

	// Create second application backup
	appBackupSecond, err := createApplicationBackupWithAnnotation(t, appKey+"-backup-2", appCtx.GetID(), backupLocation)
	log.FailOnError(t, err, "Error creating app backups")

	err = waitForAppBackupCompletion(appBackup.Name, appBackup.Namespace, applicationBackupScheduleRetryTimeout)
	log.FailOnError(t, err, "Application backup %s in namespace %s failed.", appBackup.Name, appBackup.Namespace)

	err = waitForAppBackupCompletion(appBackupSecond.Name, appBackupSecond.Namespace, (applicationBackupScheduleRetryTimeout)*time.Duration(5))
	log.FailOnError(t, err, "Application backup %s in namespace %s failed.", appBackupSecond.Name, appBackupSecond.Namespace)

	// Get application backup object to be used for validation of cloud delete later
	appBackup, err = storkops.Instance().GetApplicationBackup(appBackup.Name, appBackup.Namespace)
	log.FailOnError(t, err, "Failed to get application backup: %s in namespace: %s, for cloud deletion validation", appBackup.Name, appBackup.Namespace)

	appBackupSecond, err = storkops.Instance().GetApplicationBackup(appBackupSecond.Name, appBackupSecond.Namespace)
	log.FailOnError(t, err, "Failed to get application backup: %s in namespace: %s, for cloud deletion validation", appBackupSecond.Name, appBackupSecond.Namespace)

	firstBackupPath := appBackup.Status.BackupPath
	secondBackupPath := appBackupSecond.Status.BackupPath

	err = deleteAndWaitForBackupDeletion(appBackup.Namespace)
	log.FailOnError(t, err, "Backups not deleted: %v", err)

	err = storkops.Instance().DeleteBackupLocation(backupLocation.Name, backupLocation.Namespace)
	log.FailOnError(t, err, "Failed to delete backuplocation: %s", backupLocation.Name)

	validateBackupDeletionFromObjectstore(t, backupLocation, firstBackupPath)
	validateBackupDeletionFromObjectstore(t, backupLocation, secondBackupPath)

	destroyAndWait(t, []*scheduler.Context{appCtx})
}

func deleteAllBackupsNamespace(namespace string) error {
	log.InfoD("Deleting all backups in namespace: %s", namespace)
	allAppBackups, err := storkops.Instance().ListApplicationBackups(namespace, meta.ListOptions{})
	if err != nil {
		return fmt.Errorf("Failed to list backups before deleting: %v", err)
	}
	for _, bkp := range allAppBackups.Items {
		log.InfoD("Deleting backup: %s", bkp.Name)
		err = storkops.Instance().DeleteApplicationBackup(bkp.Name, namespace)
		if err != nil {
			if errors.IsNotFound(err) {
				continue
			}
			return fmt.Errorf("Failed to delete backup %s: %s", bkp.Name, err)
		}
	}
	return nil
}

func deleteAndWaitForBackupDeletion(namespace string) error {
	listBackupsTask := func() (interface{}, bool, error) {
		err := deleteAllBackupsNamespace(namespace)
		if err != nil {
			return "", false, err
		}

		allAppBackups, err := storkops.Instance().ListApplicationBackups(namespace, meta.ListOptions{})
		if err != nil || len(allAppBackups.Items) != 0 {
			log.InfoD("Failed to delete all app backups in %s. Error: %v. Number of backups: %v", namespace, err, len(allAppBackups.Items))
			return "", true, fmt.Errorf("All backups not deleted yet")
		}
		return "", false, nil
	}
	_, err := task.DoRetryWithTimeout(listBackupsTask, applicationBackupSyncRetryTimeout, defaultWaitInterval)
	return err

}

func getBackupLocationType(locationName storkv1.BackupLocationType) (storkv1.BackupLocationType, error) {
	switch locationName {
	case storkv1.BackupLocationS3:
		return storkv1.BackupLocationS3, nil
	case storkv1.BackupLocationAzure:
		return storkv1.BackupLocationAzure, nil
	case storkv1.BackupLocationGoogle:
		return storkv1.BackupLocationGoogle, nil
	default:
		return storkv1.BackupLocationType(""), fmt.Errorf("Invalid backuplocation type: %s", locationName)
	}
}

// Get config map for only the given type
func getBackupConfigMapForType(allTypes map[string]string, requiredType storkv1.BackupLocationType) map[string]string {
	reqConfigMap := make(map[string]string)
	if key, err := getBackupLocationType(requiredType); err == nil {
		for k, v := range allTypes {
			if k == string(key) {
				reqConfigMap[k] = v
			}
		}
	}
	return reqConfigMap
}

func deleteApplicationRestoreList(appRestoreList []*storkv1.ApplicationRestore) error {
	for _, appRestore := range appRestoreList {
		err := storkops.Instance().DeleteApplicationRestore(appRestore.Name, appRestore.Namespace)
		if err != nil {
			return fmt.Errorf("Error deleting application restore %s in namespace %s, reason: %v", appRestore.Name, appRestore.Namespace, err)
		}
	}
	log.InfoD("Deleted all restores")
	return nil
}

func deleteApplicationBackupList(appBackupList *storkv1.ApplicationBackupList) error {
	for _, appBackup := range appBackupList.Items {
		err := storkops.Instance().DeleteApplicationBackup(appBackup.Name, appBackup.Namespace)
		if err != nil {
			return fmt.Errorf("Error deleting application backup %s in namespace %s: %v", appBackup.Name, appBackup.Namespace, err)
		}
	}
	log.InfoD("Deleted all backups")
	return nil
}

func getBackupLocationForVolumeDriver(volumeDriver string) (storkv1.BackupLocationType, error) {
	switch volumeDriver {
	case "pxd", "aws":
		return storkv1.BackupLocationS3, nil
	case "azure":
		return storkv1.BackupLocationAzure, nil
	case "gce":
		return storkv1.BackupLocationGoogle, nil
	default:
		return storkv1.BackupLocationType(""), fmt.Errorf("Invalid volume driver provided: %s", volumeDriver)
	}
}

func getSecretForVolumeDriver(volumeDriver string) (string, error) {
	switch volumeDriver {
	case "pxd", "aws":
		return s3SecretName, nil
	case "azure":
		return azureSecretName, nil
	case "gce":
		return googleSecretName, nil
	default:
		return "", fmt.Errorf("Invalid volume driver provided: %s", volumeDriver)
	}
}

func setDefaultsForBackup(t *testing.T) {
	// Get location types and secret from config maps
	configMap, err := core.Instance().GetConfigMap(configMapName, "default")
	log.FailOnError(t, err, "Failed to get config map  %s", configMap.Name)

	allConfigMap = configMap.Data

	// Default backup location
	defaultBackupLocation, err = getBackupLocationForVolumeDriver(volumeDriverName)
	log.FailOnError(t, err, "Failed to get default backuplocation for %s: %v", volumeDriverName, err)
	defaultSecretName, err = getSecretForVolumeDriver(volumeDriverName)
	log.FailOnError(t, err, "Failed to get default secret name for %s: %v", volumeDriverName, err)
	log.InfoD("Default backup location set to %v", defaultBackupLocation)
	defaultConfigMap = getBackupConfigMapForType(allConfigMap, defaultBackupLocation)

	// If running pxd driver backup to all locations
	if volumeDriverName != "pxd" {
		allConfigMap = defaultConfigMap
	}
	if !defaultsBackupSet {
		defaultsBackupSet = true
	}

}
func getSyncedBackupWithAnnotation(appBackup *storkv1.ApplicationBackup, lookUpAnnotation string) (*storkv1.ApplicationBackup, error) {
	// Check periodically to see if the backup from this test is synced on second cluster
	var allAppBackups *storkv1.ApplicationBackupList
	var backupToRestore *storkv1.ApplicationBackup
	var err error
	listBackupsTask := func() (interface{}, bool, error) {
		allAppBackups, err = storkops.Instance().ListApplicationBackups(appBackup.Namespace, meta.ListOptions{})
		if err != nil {
			log.InfoD("Failed to list app backups on first cluster post migrate and sync. Error: %v", err)
			return "", true, fmt.Errorf("Failed to list app backups on first cluster")
		} else if allAppBackups != nil && len(allAppBackups.Items) > 0 {
			// backups sync has started, check if current backup has synced
			backupToRestore = getBackupFromListWithAnnotations(allAppBackups, appBackup.Annotations[lookUpAnnotation])
			if backupToRestore != nil {
				return "", false, nil
			}
		}
		return "", true, fmt.Errorf("Failed to list app backups on first cluster")
	}
	_, err = task.DoRetryWithTimeout(listBackupsTask, applicationBackupSyncRetryTimeout, defaultWaitInterval)
	if err != nil {
		return nil, err
	}
	return backupToRestore, nil
}
