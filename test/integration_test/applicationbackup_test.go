// +build integrationtest

package integrationtest

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	storkv1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/portworx/sched-ops/k8s"
	"github.com/portworx/sched-ops/task"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/portworx/torpedo/drivers/scheduler/spec"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	testKey               = "mysql-1-pvc"
	appKey                = "mysql"
	restoreName           = "mysql-restore"
	backupSyncAnnotation  = "backupsync"
	configMapName         = "secret-config"
	defaultBackupLocation = storkv1.BackupLocationS3
	defaultSecretName     = "s3secret"

	applicationBackupScheduleRetryInterval = 10 * time.Second
	applicationBackupScheduleRetryTimeout  = 5 * time.Minute
	applicationBackupSyncRetryTimeout      = 21 * time.Minute
)

var allConfigMap, defaultConfigMap map[string]string

func TestApplicationBackup(t *testing.T) {
	// Get location types and secret from config maps
	configMap, err := k8s.Instance().GetConfigMap(configMapName, "default")
	require.NoError(t, err, "Failed to get config map  %s", configMap.Name)

	allConfigMap = configMap.Data

	// Default backup location
	defaultConfigMap = getBackupConfigMapForType(allConfigMap, defaultBackupLocation)

	t.Run("applicationBackupRestoreTest", applicationBackupRestoreTest)
	t.Run("preExecRuleTest", applicationBackupRestorePreExecRuleTest)
	t.Run("postExecRuleTest", applicationBackupRestorePostExecRuleTest)
	t.Run("preExecMissingRuleTest", applicationBackupRestorePreExecMissingRuleTest)
	t.Run("postExecMissingRuleTest", applicationBackupRestorePostExecMissingRuleTest)
	t.Run("preExecFailingRuleTest", applicationBackupRestorePreExecFailingRuleTest)
	t.Run("postExecFailingRuleTest", applicationBackupRestorePostExecFailingRuleTest)
	t.Run("labelSelector", applicationBackupLabelSelectorTest)
	t.Run("scheduleTests", applicationBackupScheduleTests)
	t.Run("backupSyncController", applicationBackupSyncControllerTest)
}

func TestScaleApplicationBackup(t *testing.T) {
	t.Run("scaleApplicationBackupRestore", scaleApplicationBackupRestore)
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
		logrus.Infof("Backing up to cloud: %v using secret %v", location, secret)
		var currBackupLocation *storkv1.BackupLocation
		var err error
		var ctxs []*scheduler.Context
		ctx := createApp(t, appKey)
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

		if len(additionalAppKeys) > 0 {
			err = schedulerDriver.AddTasks(ctxs[0],
				scheduler.ScheduleOptions{AppKeys: additionalAppKeys})
			require.NoError(t, err, "Error scheduling additional apps")
			err = schedulerDriver.WaitForRunning(ctxs[0], defaultWaitTimeout, defaultWaitInterval)
			require.NoError(t, err, "Error waiting for additional apps to get to running state")
			if backupAllAppsExpected {
				preBackupCtx = ctxs[0].DeepCopy()
			}
		}

		// Track contexts that will be destroyed before restore
		preRestoreCtx := ctxs[0].DeepCopy()

		logrus.Infof("All Apps created %v. Starting backup.", ctx.GetID())

		// Create backuplocation here programatically using config-map that contains name of secrets to be used, passed from the CLI
		if createBackupLocationFlag {
			currBackupLocation, err = createBackupLocation(t, appKey+"-backup-location", ctx.GetID(), storkv1.BackupLocationType(location), secret)
			require.NoError(t, err, "Error creating backuplocation")
		}

		// Backup application
		if backupSuccessExpected {
			if backupAllAppsExpected {
				err = schedulerDriver.AddTasks(ctxs[0], scheduler.ScheduleOptions{AppKeys: appBackupKey})
				require.NoError(t, err, "Error creating app backups")
				err = schedulerDriver.WaitForRunning(ctxs[0], defaultWaitTimeout, defaultWaitInterval)
			} else {
				err = schedulerDriver.AddTasks(postRestoreCtx, scheduler.ScheduleOptions{AppKeys: appBackupKey})
				require.NoError(t, err, "Error creating app backups")
				err = schedulerDriver.WaitForRunning(postRestoreCtx, defaultWaitTimeout, defaultWaitInterval)
			}
			require.NoError(t, err, "Error waiting for back-up to complete.")
			logrus.Infof("Backup completed.")

			// Delete apps so that they can be restored
			destroyAndWait(t, []*scheduler.Context{preRestoreCtx})

			logrus.Infof("Starting Restore.")
			// Restore application
			err = schedulerDriver.AddTasks(restoreCtx,
				scheduler.ScheduleOptions{AppKeys: appRestoreKey})
			require.NoError(t, err, "Error restoring apps")

			err = schedulerDriver.WaitForRunning(restoreCtx, defaultWaitTimeout, defaultWaitInterval)
			require.NoError(t, err, "Error waiting for restore to complete.")

			logrus.Infof("Restore completed.")

			// Validate that restore results in restoration of correct apps based on whether all apps were expected or not
			err = schedulerDriver.WaitForRunning(preBackupCtx, defaultWaitTimeout, defaultWaitInterval)
			require.NoError(t, err, "Error waiting for restore to complete.")

			logrus.Infof("App validations after restore completed.")

		} else { // Since backup is expected to fail, reducing the wait time here to catch the error faster
			err = schedulerDriver.AddTasks(ctxs[0], scheduler.ScheduleOptions{AppKeys: appBackupKey})
			require.NoError(t, err, "Error backing-up apps")
			err = schedulerDriver.WaitForRunning(ctxs[0], defaultWaitTimeout/5, defaultWaitInterval)
			require.Error(t, err, "Backup expected to fail in test: %s.", t.Name())
		}

		ctxs = append(ctxs, restoreCtx)
		if (backupAllAppsExpected && backupSuccessExpected) || !backupSuccessExpected {
			destroyAndWait(t, ctxs)
		} else if !backupAllAppsExpected && backupSuccessExpected {
			// Some apps might have been already destroyed, destroy the remaining
			destroyAndWait(t, []*scheduler.Context{postRestoreCtx})

		}
		err = k8s.Instance().DeleteBackupLocation(currBackupLocation.Name, currBackupLocation.Namespace)
		require.NoError(t, err, "Failed to delete backuplocation: %s for location %s.", currBackupLocation.Name, string(location), err)
	}
}

func triggerScaleBackupRestoreTest(
	t *testing.T,
	appKey []string,
	appBackupKey []string,
	appRestoreKey []string,
	configMap map[string]string,
	createBackupLocationFlag bool,
	backupSuccessExpected bool,
	backupAllAppsExpected bool,
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
			scheduler.ScheduleOptions{AppKeys: appBackupKey})
		require.NoError(t, err, "Error scheduling task")
		require.Equal(t, 1, len(currCtx), "Only one task should have started")
		appCtxs = append(appCtxs, currCtx...)

		// Without the sleep, sometimes the mysql pods fail with permission denied error
		time.Sleep(time.Second * 10)
	}

	for i, app := range appCtxs {
		err := schedulerDriver.WaitForRunning(app, defaultWaitTimeout, defaultWaitInterval)
		require.NoError(t, err, "Error waiting for app in namespace %s to get to running state", "scale-"+strconv.Itoa(i))
	}
	// Trigger backups for all apps created
	for idx, app := range appCtxs {
		backupLocation, err := createBackupLocation(t, backuplocationPrefix+strconv.Itoa(idx), app.GetID(), storkv1.BackupLocationS3, defaultSecretName)
		require.NoError(t, err, "Error creating backuplocation: %s", "backupLocation-"+strconv.Itoa(idx))
		bkpLocations = append(bkpLocations, backupLocation)

		currBackup, err := createApplicationBackupWithAnnotation(t, appbackupPrefix+strconv.Itoa(idx), app.GetID(), backupLocation)
		require.NoError(t, err, "Error creating app backups")

		appBkps = append(appBkps, currBackup)
	}

	// Wait for all backups to be completed
	for _, bkp := range appBkps {
		logrus.Infof("Verifying backup in namespace %s", bkp.Namespace)
		err := waitForAppBackupCompletion(bkp.Name, bkp.Namespace)
		require.NoError(t, err, "Application backup %s in namespace %s failed.", bkp.Name, bkp.Namespace)
	}

	logrus.Info("Deleting all apps before restoring them.")
	destroyAndWait(t, appCtxs)

	// Create restore objects and restore all backups in their namespaces
	for idx, bkp := range appBkps {
		logrus.Infof("Creating application restore in namespace %s", bkp.Namespace)
		appRestoreForBackup, err := createApplicationRestore(t, restorePrefix+strconv.Itoa(idx), bkp.Namespace, bkp, bkpLocations[idx])
		require.Nil(t, err, "failure to create restore object in namespace %s", bkp.Namespace)
		require.NotNil(t, appRestoreForBackup, "failure to restore bkp in namespace %s", "mysql-restore-scale-"+strconv.Itoa(idx))

		appRestores = append(appRestores, appRestoreForBackup)
	}

	// Wait for all apps to be running
	for i, app := range appCtxs {
		err := schedulerDriver.WaitForRunning(app, defaultWaitTimeout, defaultWaitInterval)
		require.NoError(t, err, "Error waiting for app in namespace %s to get to running state", "scale-"+strconv.Itoa(i))
	}
	// Cleanup
	for idx, bkp := range appBkps {
		err := k8s.Instance().DeleteBackupLocation("backuplocation-"+strconv.Itoa(idx), bkp.Namespace)
		require.NoError(t, err, "Failed to delete  backup location %s in namespace %s: %v", "backuplocation-"+strconv.Itoa(idx), bkp.Namespace, err)

		err = deleteAndWaitForBackupDeletion(bkp.Namespace)
		require.NoError(t, err, "All backups not deleted in namespace %s: %v", bkp.Namespace, err)
	}

	err := deleteApplicationRestoreList(appRestores)
	require.Nil(t, err, "failure to delete application restore")

	destroyAndWait(t, appCtxs)
}

func createBackupLocation(
	t *testing.T,
	name string,
	namespace string,
	locationtype storkv1.BackupLocationType,
	secretName string,
) (*storkv1.BackupLocation, error) {

	secretObj, err := k8s.Instance().GetSecret(secretName, "default")
	require.NoError(t, err, "Failed to get secret %s", secretName)

	// copy secret to the app namespace
	newSecretObj := secretObj.DeepCopy()
	newSecretObj.Namespace = namespace
	newSecretObj.ResourceVersion = ""
	_, err = k8s.Instance().CreateSecret(newSecretObj)
	// Ignore if secret already exists
	if err != nil && !errors.IsAlreadyExists(err) {
		require.NoError(t, err, "Failed to copy secret %s  to namespace %s", name, namespace)
	}

	backupType, err := getBackupLocationType(locationtype)
	require.NoError(t, err, "Failed to get backuptype for %s:", locationtype, err)
	backupLocation := &storkv1.BackupLocation{
		ObjectMeta: meta.ObjectMeta{
			Name:        name,
			Namespace:   namespace,
			Annotations: map[string]string{"stork.libopenstorage.ord/skipresource": "true"},
		},
		Location: storkv1.BackupLocationItem{
			Type:         backupType,
			Path:         "test-restore-path",
			SecretConfig: secretObj.Name,
		},
	}
	return k8s.Instance().CreateBackupLocation(backupLocation)
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
	return k8s.Instance().CreateApplicationRestore(appRestore)
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
	return k8s.Instance().CreateApplicationBackup(appBackup)
}

func generateTimestampAnnotationMap(annotationKey string) map[string]string {
	t := time.Now()
	val := t.Format(time.RFC1123)
	annotationMap := map[string]string{annotationKey: val}
	logrus.Infof("Annotations created to track backup: %v", annotationMap)
	return annotationMap
}

func getBackupFromListWithAnnotations(backupList *storkv1.ApplicationBackupList, annotationValue string) *storkv1.ApplicationBackup {
	for _, backup := range backupList.Items {
		if backup.Annotations != nil {
			for k, v := range backup.Annotations {
				if k == backupSyncAnnotation && v == annotationValue {
					logrus.Infof("Backup with annotations found: %s", backup.Name)
					return &backup
				}
			}
		}
	}
	return nil
}

func applicationBackupRestoreTest(t *testing.T) {
	triggerBackupRestoreTest(
		t,
		[]string{testKey + "-backup"},
		[]string{},
		[]string{restoreName},
		allConfigMap,
		true,
		true,
		true,
	)
}

func applicationBackupRestorePreExecRuleTest(t *testing.T) {
	triggerBackupRestoreTest(
		t,
		[]string{testKey + "-pre-exec-rule-backup"},
		[]string{},
		[]string{restoreName},
		defaultConfigMap,
		true,
		true,
		true,
	)
}

func applicationBackupRestorePostExecRuleTest(t *testing.T) {
	triggerBackupRestoreTest(
		t,
		[]string{testKey + "-post-exec-rule-backup"},
		[]string{},
		[]string{restoreName},
		defaultConfigMap,
		true,
		true,
		true,
	)
}

func applicationBackupRestorePreExecMissingRuleTest(t *testing.T) {
	triggerBackupRestoreTest(
		t,
		[]string{testKey + "-pre-exec-missing-rule-backup"},
		[]string{},
		[]string{restoreName},
		defaultConfigMap,
		true,
		false,
		false,
	)
}

func applicationBackupRestorePostExecMissingRuleTest(t *testing.T) {
	triggerBackupRestoreTest(
		t,
		[]string{testKey + "-post-exec-missing-rule-backup"},
		[]string{},
		[]string{restoreName},
		defaultConfigMap,
		true,
		false,
		false,
	)
}

func applicationBackupRestorePreExecFailingRuleTest(t *testing.T) {
	triggerBackupRestoreTest(
		t,
		[]string{testKey + "-pre-exec-failing-rule-backup"},
		[]string{},
		[]string{restoreName},
		defaultConfigMap,
		true,
		false,
		false,
	)
}

func applicationBackupRestorePostExecFailingRuleTest(t *testing.T) {
	triggerBackupRestoreTest(
		t,
		[]string{testKey + "-post-exec-failing-rule-backup"},
		[]string{},
		[]string{restoreName},
		defaultConfigMap,
		true,
		false,
		false,
	)
}

func applicationBackupLabelSelectorTest(t *testing.T) {
	triggerBackupRestoreTest(
		t,
		[]string{testKey + "-label-selector-backup"},
		[]string{"cassandra"},
		[]string{restoreName},
		defaultConfigMap,
		true,
		true,
		false,
	)
}

func scaleApplicationBackupRestore(t *testing.T) {
	triggerScaleBackupRestoreTest(
		t,
		[]string{"mysql-1-pvc"},
		[]string{"mysql-1-pvc"},
		[]string{restoreName},
		defaultConfigMap,
		true,
		true,
		true,
	)
}

func applicationBackupScheduleTests(t *testing.T) {
	err := setMockTime(nil)
	require.NoError(t, err, "Error resetting mock time")
	t.Run("intervalTest", intervalApplicationBackupScheduleTest)
	t.Run("dailyTest", dailyApplicationBackupScheduleTest)
	t.Run("weeklyTest", weeklyApplicationBackupScheduleTest)
	t.Run("monthlyTest", monthlyApplicationBackupScheduleTest)
	t.Run("invalidPolicyTest", invalidPolicyApplicationBackupScheduleTest)
}

func deletePolicyAndApplicationBackupSchedule(t *testing.T, namespace string, policyName string, applicationBackupScheduleName string) {
	err := k8s.Instance().DeleteSchedulePolicy(policyName)
	require.NoError(t, err, fmt.Sprintf("Error deleting schedule policy %v", policyName))

	err = k8s.Instance().DeleteApplicationBackupSchedule(applicationBackupScheduleName, namespace)
	require.NoError(t, err, fmt.Sprintf("Error deleting applicationBackup schedule %v from namespace %v",
		applicationBackupScheduleName, namespace))

	time.Sleep(10 * time.Second)
	applicationBackupList, err := k8s.Instance().ListApplicationBackups(namespace)
	require.NoError(t, err, fmt.Sprintf("Error getting list of applicationBackups for namespace: %v", namespace))
	require.Equal(t, 0, len(applicationBackupList.Items), fmt.Sprintf("All applicationBackups should have been deleted in namespace %v", namespace))
}

func intervalApplicationBackupScheduleTest(t *testing.T) {
	backupLocation := "backuplocation"

	ctx := createApp(t, "interval-appbackup-sched-test")

	// Create backuplocation here programatically using config-map that contains name of secrets to be used, passed from the CLI
	_, err := createBackupLocation(t, backupLocation, ctx.GetID(), storkv1.BackupLocationS3, defaultSecretName)
	require.NoError(t, err, "Error creating backuplocation")

	policyName := "intervalpolicy-appbackup"
	retain := 2
	interval := 2
	_, err = k8s.Instance().CreateSchedulePolicy(&storkv1.SchedulePolicy{
		ObjectMeta: meta.ObjectMeta{
			Name: policyName,
		},
		Policy: storkv1.SchedulePolicyItem{
			Interval: &storkv1.IntervalPolicy{
				Retain:          storkv1.Retain(retain),
				IntervalMinutes: interval,
			},
		}})
	require.NoError(t, err, "Error creating interval schedule policy")
	logrus.Infof("Created schedulepolicy %v with %v minute interval and retain at %v", policyName, interval, retain)

	scheduleName := "intervalscheduletest"
	namespace := ctx.GetID()
	_, err = k8s.Instance().CreateApplicationBackupSchedule(&storkv1.ApplicationBackupSchedule{
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
	require.NoError(t, err, "Error creating interval applicationBackup schedule")
	sleepTime := time.Duration((retain+1)*interval) * time.Minute
	logrus.Infof("Created applicationBackupschedule %v in namespace %v, sleeping for %v for schedule to trigger",
		scheduleName, namespace, sleepTime)
	time.Sleep(sleepTime)

	backupStatuses, err := k8s.Instance().ValidateApplicationBackupSchedule("intervalscheduletest",
		namespace,
		applicationBackupScheduleRetryTimeout,
		applicationBackupScheduleRetryInterval)
	require.NoError(t, err, "Error validating interval applicationBackup schedule")
	require.Equal(t, 1, len(backupStatuses), "Should have applicationBackups for only one policy type")
	require.Equal(t, retain, len(backupStatuses[storkv1.SchedulePolicyTypeInterval]), fmt.Sprintf("Should have only %v applicationBackup for interval policy", retain))
	logrus.Infof("Validated applicationBackupschedule %v", scheduleName)

	deletePolicyAndApplicationBackupSchedule(t, namespace, policyName, scheduleName)
	destroyAndWait(t, []*scheduler.Context{ctx})
}

func dailyApplicationBackupScheduleTest(t *testing.T) {
	backupLocation := "backuplocation"
	ctx := createApp(t, "daily-backup-sched-test")

	// Create backuplocation here programatically using config-map that contains name of secrets to be used, passed from the CLI
	_, err := createBackupLocation(t, backupLocation, ctx.GetID(), storkv1.BackupLocationS3, defaultSecretName)
	require.NoError(t, err, "Error creating backuplocation")

	policyName := "dailypolicy-appbackup"
	retain := 2
	// Set first trigger 2 minutes from now
	scheduledTime := time.Now().Add(2 * time.Minute)
	nextScheduledTime := scheduledTime.AddDate(0, 0, 1)
	_, err = k8s.Instance().CreateSchedulePolicy(&storkv1.SchedulePolicy{
		ObjectMeta: meta.ObjectMeta{
			Name: policyName,
		},
		Policy: storkv1.SchedulePolicyItem{
			Daily: &storkv1.DailyPolicy{
				Retain: storkv1.Retain(retain),
				Time:   scheduledTime.Format(time.Kitchen),
			},
		}})
	require.NoError(t, err, "Error creating daily schedule policy")
	logrus.Infof("Created schedulepolicy %v at time %v and retain at %v",
		policyName, scheduledTime.Format(time.Kitchen), retain)

	scheduleName := "dailyscheduletest"
	namespace := ctx.GetID()
	_, err = k8s.Instance().CreateApplicationBackupSchedule(&storkv1.ApplicationBackupSchedule{
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
	require.NoError(t, err, "Error creating daily applicationBackup schedule")
	logrus.Infof("Created applicationBackupschedule %v in namespace %v",
		scheduleName, namespace)
	commonApplicationBackupScheduleTests(t, scheduleName, policyName, namespace, nextScheduledTime, storkv1.SchedulePolicyTypeDaily)
	destroyAndWait(t, []*scheduler.Context{ctx})
}

func weeklyApplicationBackupScheduleTest(t *testing.T) {
	backupLocation := "backuplocation"
	ctx := createApp(t, "weekly-backup-sched-test")

	// Create backuplocation here programatically using config-map that contains name of secrets to be used, passed from the CLI
	_, err := createBackupLocation(t, backupLocation, ctx.GetID(), storkv1.BackupLocationS3, defaultSecretName)
	require.NoError(t, err, "Error creating backuplocation")

	policyName := "weeklypolicy-appbackup"
	retain := 2
	// Set first trigger 2 minutes from now
	scheduledTime := time.Now().Add(2 * time.Minute)
	nextScheduledTime := scheduledTime.AddDate(0, 0, 7)
	_, err = k8s.Instance().CreateSchedulePolicy(&storkv1.SchedulePolicy{
		ObjectMeta: meta.ObjectMeta{
			Name: policyName,
		},
		Policy: storkv1.SchedulePolicyItem{
			Weekly: &storkv1.WeeklyPolicy{
				Retain: storkv1.Retain(retain),
				Day:    scheduledTime.Weekday().String(),
				Time:   scheduledTime.Format(time.Kitchen),
			},
		}})
	require.NoError(t, err, "Error creating weekly schedule policy")
	logrus.Infof("Created schedulepolicy %v at time %v on day %v and retain at %v",
		policyName, scheduledTime.Format(time.Kitchen), scheduledTime.Weekday().String(), retain)

	scheduleName := "weeklyscheduletest"
	namespace := ctx.GetID()
	_, err = k8s.Instance().CreateApplicationBackupSchedule(&storkv1.ApplicationBackupSchedule{
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
	require.NoError(t, err, "Error creating weekly applicationBackup schedule")
	logrus.Infof("Created applicationBackupschedule %v in namespace %v",
		scheduleName, namespace)
	commonApplicationBackupScheduleTests(t, scheduleName, policyName, namespace, nextScheduledTime, storkv1.SchedulePolicyTypeWeekly)
	destroyAndWait(t, []*scheduler.Context{ctx})
}

func monthlyApplicationBackupScheduleTest(t *testing.T) {
	backupLocation := "backuplocation"
	ctx := createApp(t, "monthly-backup-sched-test")

	// Create backuplocation here programatically using config-map that contains name of secrets to be used, passed from the CLI
	_, err := createBackupLocation(t, backupLocation, ctx.GetID(), storkv1.BackupLocationS3, defaultSecretName)
	require.NoError(t, err, "Error creating backuplocation")

	policyName := "monthlypolicy-appbackup"
	retain := 2
	// Set first trigger 2 minutes from now
	scheduledTime := time.Now().Add(2 * time.Minute)
	nextScheduledTime := scheduledTime.AddDate(0, 1, 0)
	// Set the time to zero in case the date doesn't exist in the next month
	if nextScheduledTime.Day() != scheduledTime.Day() {
		nextScheduledTime = time.Time{}
	}
	_, err = k8s.Instance().CreateSchedulePolicy(&storkv1.SchedulePolicy{
		ObjectMeta: meta.ObjectMeta{
			Name: policyName,
		},
		Policy: storkv1.SchedulePolicyItem{
			Monthly: &storkv1.MonthlyPolicy{
				Retain: storkv1.Retain(retain),
				Date:   scheduledTime.Day(),
				Time:   scheduledTime.Format(time.Kitchen),
			},
		}})
	require.NoError(t, err, "Error creating monthly schedule policy")
	logrus.Infof("Created schedulepolicy %v at time %v on date %v and retain at %v",
		policyName, scheduledTime.Format(time.Kitchen), scheduledTime.Day(), retain)

	scheduleName := "monthlyscheduletest"
	namespace := ctx.GetID()
	_, err = k8s.Instance().CreateApplicationBackupSchedule(&storkv1.ApplicationBackupSchedule{
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
	require.NoError(t, err, "Error creating monthly applicationBackup schedule")
	logrus.Infof("Created applicationBackupschedule %v in namespace %v",
		scheduleName, namespace)
	commonApplicationBackupScheduleTests(t, scheduleName, policyName, namespace, nextScheduledTime, storkv1.SchedulePolicyTypeMonthly)
	destroyAndWait(t, []*scheduler.Context{ctx})
}

func invalidPolicyApplicationBackupScheduleTest(t *testing.T) {
	backupLocation := "backuplocation"
	ctx := createApp(t, "invalid-backup-sched-test")

	// Create backuplocation here programatically using config-map that contains name of secrets to be used, passed from the CLI
	_, err := createBackupLocation(t, backupLocation, ctx.GetID(), storkv1.BackupLocationS3, defaultSecretName)
	require.NoError(t, err, "Error creating backuplocation")

	policyName := "invalidpolicy-appbackup"
	scheduledTime := time.Now()
	retain := 2
	_, err = k8s.Instance().CreateSchedulePolicy(&storkv1.SchedulePolicy{
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
	require.NoError(t, err, "Error creating invalid schedule policy")
	logrus.Infof("Created schedulepolicy %v at time %v on date %v and retain at %v",
		policyName, scheduledTime.Format(time.Kitchen), scheduledTime.Day(), retain)

	scheduleName := "invalidpolicyschedule"
	namespace := ctx.GetID()
	_, err = k8s.Instance().CreateApplicationBackupSchedule(&storkv1.ApplicationBackupSchedule{
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
	require.NoError(t, err, "Error creating applicationBackup schedule with invalid policy")
	logrus.Infof("Created applicationBackupschedule %v in namespace %v",
		scheduleName, namespace)
	_, err = k8s.Instance().ValidateApplicationBackupSchedule(scheduleName,
		namespace,
		3*time.Minute,
		applicationBackupScheduleRetryInterval)
	require.Error(t, err, fmt.Sprintf("No applicationBackups should have been created for %v in namespace %v",
		scheduleName, namespace))
	deletePolicyAndApplicationBackupSchedule(t, namespace, policyName, scheduleName)
	destroyAndWait(t, []*scheduler.Context{ctx})
}

func commonApplicationBackupScheduleTests(
	t *testing.T,
	scheduleName string,
	policyName string,
	namespace string,
	nextTriggerTime time.Time,
	policyType storkv1.SchedulePolicyType) {
	// Make sure no backup gets created in the next minute
	_, err := k8s.Instance().ValidateApplicationBackupSchedule(scheduleName,
		namespace,
		1*time.Minute,
		applicationBackupScheduleRetryInterval)
	require.Error(t, err, fmt.Sprintf("No backups should have been created for %v in namespace %v",
		scheduleName, namespace))
	sleepTime := time.Duration(1 * time.Minute)
	logrus.Infof("Sleeping for %v for schedule to trigger",
		sleepTime)
	time.Sleep(sleepTime)

	backupStatuses, err := k8s.Instance().ValidateApplicationBackupSchedule(scheduleName,
		namespace,
		applicationBackupScheduleRetryTimeout,
		applicationBackupScheduleRetryInterval)
	require.NoError(t, err, "Error validating backup schedule")
	require.Equal(t, 1, len(backupStatuses), "Should have backups for only one policy type")
	require.Equal(t, 1, len(backupStatuses[policyType]), fmt.Sprintf("Should have only one backupshot for %v schedule", scheduleName))
	logrus.Infof("Validated first backupschedule %v", scheduleName)

	// Now advance time to the next trigger if the next trigger is not zero
	if !nextTriggerTime.IsZero() {
		logrus.Infof("Updating mock time to %v for next schedule", nextTriggerTime)
		err := setMockTime(&nextTriggerTime)
		require.NoError(t, err, "Error setting mock time")
		defer func() {
			err := setMockTime(nil)
			require.NoError(t, err, "Error resetting mock time")
		}()
		logrus.Infof("Sleeping for 90 seconds for the schedule to get triggered")
		time.Sleep(90 * time.Second)
		backupStatuses, err := k8s.Instance().ValidateApplicationBackupSchedule(scheduleName,
			namespace,
			applicationBackupScheduleRetryTimeout,
			applicationBackupScheduleRetryInterval)
		require.NoError(t, err, "Error validating backup schedule")
		require.Equal(t, 1, len(backupStatuses), "Should have backups for only one policy type")
		require.Equal(t, 2, len(backupStatuses[policyType]), fmt.Sprintf("Should have 2 backups for %v schedule", scheduleName))
		logrus.Infof("Validated second backupschedule %v", scheduleName)
	}
	deletePolicyAndApplicationBackupSchedule(t, namespace, policyName, scheduleName)
}

func applicationBackupSyncControllerTest(t *testing.T) {
	var err error
	defer func() {
		err = setRemoteConfig("")
		require.NoError(t, err, "Error resetting remote config")
	}()
	backupLocationName := appKey + "-backup-location-sync"

	// Create myqsl app deployment
	appCtx := createApp(t, appKey)

	// Create backup location on first cluster
	backupLocation, err := createBackupLocation(t, backupLocationName, appCtx.GetID(), storkv1.BackupLocationS3, defaultSecretName)
	require.NoError(t, err, "Error creating backuplocation")
	logrus.Infof("Created backup location:%s sync:%t", backupLocation.Name, backupLocation.Location.Sync)
	backupLocation.Location.Sync = true

	firstBackup, err := createApplicationBackupWithAnnotation(t, appKey+"-backup-sync", appCtx.GetID(), backupLocation)
	require.NoError(t, err, "Error creating app backups")

	// Wait for backup completion
	err = waitForAppBackupCompletion(firstBackup.Name, firstBackup.Namespace)
	require.NoError(t, err, "Application backup %s failed.", firstBackup)

	// Create backup location on second cluster
	err = dumpRemoteKubeConfig(remoteConfig)
	require.NoErrorf(t, err, "Unable to write clusterconfig: %v", err)

	err = setRemoteConfig(remoteFilePath)
	require.NoError(t, err, "Error setting remote config")

	// Create namespace for the backuplocation on second cluster
	ns, err := k8s.Instance().CreateNamespace(appCtx.GetID(),
		map[string]string{
			"creator": "stork-test",
			"app":     appCtx.App.Key,
		})
	require.NoError(t, err, "Failed to create namespace %s", appCtx.GetID())

	backupLocation2, err := createBackupLocation(t, backupLocationName, ns.Name, storkv1.BackupLocationS3, defaultSecretName)
	require.NoError(t, err, "Error creating backuplocation on second cluster")
	logrus.Infof("Created backup location on second cluster %s: sync:%t", backupLocation.Name, backupLocation.Location.Sync)

	// Set sync to true on second cluster so that backup location gets synced
	backupLocation2.Location.Sync = true
	_, err = k8s.Instance().UpdateBackupLocation(backupLocation2)
	require.NoError(t, err, "Failed to set backup-location sync to true")
	logrus.Infof("Updated application backup on 2nd cluster %s: sync:%t", backupLocation2.Name, backupLocation2.Location.Sync)

	// Check periodically to see if the backup from this test is synced on second cluster
	var allAppBackups *storkv1.ApplicationBackupList
	listBackupsTask := func() (interface{}, bool, error) {
		allAppBackups, err = k8s.Instance().ListApplicationBackups(ns.Name)
		if err != nil {
			logrus.Infof("Failed to list app backups on second cluster. Error: %v", err)
			return "", true, fmt.Errorf("Failed to list app backups on second cluster")
		} else if allAppBackups != nil && len(allAppBackups.Items) > 0 {
			// backups sync has started, check if current backup has synced
			backupToRestore := getBackupFromListWithAnnotations(allAppBackups, firstBackup.Annotations[backupSyncAnnotation])
			if backupToRestore != nil {
				return "", false, nil
			}
		}
		return "", true, fmt.Errorf("Failed to list app backups on second cluster")
	}
	_, err = task.DoRetryWithTimeout(listBackupsTask, applicationBackupSyncRetryTimeout, defaultWaitInterval)
	require.NoError(t, err, "Error listing application backups")

	backupToRestore := getBackupFromListWithAnnotations(allAppBackups, firstBackup.Annotations[backupSyncAnnotation])
	require.NotNil(t, backupToRestore, "Backup sync failed. Backup not found on the second cluster")

	// Create application restore using the backup selected, on second cluster
	logrus.Infof("Starting Restore on second cluster.")
	appRestoreForBackup, err := createApplicationRestore(t, "mysql-restore-backup-sync", ns.Name, backupToRestore, backupLocation2)
	require.NotNil(t, appRestoreForBackup, "failure to restore on second cluster")
	require.NoError(t, err, "Error creating application restore on second cluster")

	logrus.Infof("Waiting for apps to come up on the 2nd cluster.")
	err = schedulerDriver.WaitForRunning(appCtx, defaultWaitTimeout, defaultWaitInterval)
	require.NoError(t, err, "Error waiting for restore to complete on second cluster.")
	logrus.Infof("Restore complete on second cluster.")

	// Delete backup object on second cluster
	err = k8s.Instance().DeleteApplicationBackup(backupToRestore.Name, backupToRestore.Namespace)
	require.NoError(t, err, "Failed to delete backup post-restore on second cluster.")

	// Destroy app on first cluster
	err = setRemoteConfig("")
	logrus.Infof("Destroy apps  on first cluster: %v.", appCtx.App.Key)
	require.NoError(t, err, "Error resetting remote config")
	destroyAndWait(t, []*scheduler.Context{appCtx})

	// Restore application on first cluster
	logrus.Infof("Starting Restore on first cluster.")
	restoreCtxFirst := &scheduler.Context{
		UID: appCtx.UID,
		App: &spec.AppSpec{
			Key:      appCtx.App.Key,
			SpecList: []interface{}{},
		}}

	err = schedulerDriver.AddTasks(restoreCtxFirst,
		scheduler.ScheduleOptions{AppKeys: []string{restoreName + "-backup-sync"}})
	require.NoError(t, err, "Error restoring apps")
	err = schedulerDriver.WaitForRunning(restoreCtxFirst, defaultWaitTimeout, defaultWaitInterval)

	require.NoError(t, err, "Error waiting for restore to complete on first cluster.")
	logrus.Infof("Restore completed on first  cluster.")

	// Check if app is created
	err = schedulerDriver.WaitForRunning(appCtx, defaultWaitTimeout, defaultWaitInterval)
	require.NoError(t, err, "App is not running on second cluster post-restore.")

	// Cleanup both clusters
	err = k8s.Instance().DeleteBackupLocation(backupLocationName, ns.Name)
	require.NoError(t, err, "Failed to delete  backup location %s on first cluster: %v.", ns.Name, err)

	err = deleteAndWaitForBackupDeletion(ns.Name)
	require.NoError(t, err, "All backups not delete backup: %v.", err)

	destroyAndWait(t, []*scheduler.Context{appCtx})

	err = dumpRemoteKubeConfig(remoteConfig)
	require.NoErrorf(t, err, "Unable to write clusterconfig: %v", err)

	err = setRemoteConfig(remoteFilePath)
	require.NoError(t, err, "Error setting remote config")

	err = k8s.Instance().DeleteBackupLocation(backupLocationName, ns.Name)
	require.NoError(t, err, "Failed to delete  backup location %s on first cluster: %v.", ns.Name, err)

	err = deleteAndWaitForBackupDeletion(ns.Name)
	require.NoError(t, err, "All backups not delete backup: %v.", err)

	destroyAndWait(t, []*scheduler.Context{appCtx})
}

func waitForAppBackupCompletion(name, namespace string) error {
	getAppBackup := func() (interface{}, bool, error) {
		appBackup, err := k8s.Instance().GetApplicationBackup(name, namespace)
		if err != nil {
			return "", false, err
		}

		if appBackup.Status.Status != storkv1.ApplicationBackupStatusSuccessful {
			return "", true, fmt.Errorf("App backups %s in %s not complete yet.Retrying", name, namespace)
		}
		return "", false, nil
	}
	_, err := task.DoRetryWithTimeout(getAppBackup, applicationBackupSyncRetryTimeout, defaultWaitInterval)
	return err

}

func deleteAllBackupsNamespace(namespace string) error {
	allAppBackups, err := k8s.Instance().ListApplicationBackups(namespace)
	if err != nil {
		return fmt.Errorf("Failed to list backups before deleting: %v", err)
	}
	for _, bkp := range allAppBackups.Items {
		err = k8s.Instance().DeleteApplicationBackup(bkp.Name, namespace)
		if err != nil {
			return fmt.Errorf("Failed to delete backup %s", bkp.Name)
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

		allAppBackups, err := k8s.Instance().ListApplicationBackups(namespace)
		if err != nil || len(allAppBackups.Items) != 0 {
			logrus.Infof("Failed to delete all app backups in %s. Error: %v. Number of backups: %v", namespace, err, len(allAppBackups.Items))
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
		err := k8s.Instance().DeleteApplicationRestore(appRestore.Name, appRestore.Namespace)
		if err != nil {
			return fmt.Errorf("Error deleting application restore %s in namespace %s", appRestore.Name, appRestore.Namespace)
		}
	}
	logrus.Info("Deleted all restores")
	return nil
}
