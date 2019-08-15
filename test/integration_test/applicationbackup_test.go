// +build integrationtest

package integrationtest

import (
	"fmt"
	"testing"
	"time"

	storkv1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/portworx/sched-ops/k8s"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	testKey = "mysql-1-pvc"
	appKey  = "mysql"

	applicationBackupScheduleRetryInterval = 10 * time.Second
	applicationBackupScheduleRetryTimeout  = 3 * time.Minute
)

func testApplicationBackup(t *testing.T) {
	t.Run("applicationBackupRestoreTest", applicationBackupRestoreTest)
	t.Run("preExecRuleTest", applicationBackupRestorePreExecRuleTest)
	t.Run("postExecRuleTest", applicationBackupRestorePostExecRuleTest)
	t.Run("preExecMissingRuleTest", applicationBackupRestorePreExecMissingRuleTest)
	t.Run("postExecMissingRuleTest", applicationBackupRestorePostExecMissingRuleTest)
	t.Run("preExecFailingRuleTest", applicationBackupRestorePreExecFailingRuleTest)
	t.Run("postExecFailingRuleTest", applicationBackupRestorePostExecFailingRuleTest)
	t.Run("scheduleTests", applicationBackupScheduleTests)
}

func triggerBackupRestoreTest(
	t *testing.T,
	appBackupKey []string,
	appRestoreKey []string,
	createBackupLocationFlag bool,
	backupSuccessExpected bool,
) {
	var err error
	var ctxs []*scheduler.Context
	ctx := createApp(t, appKey)
	ctxs = append(ctxs, ctx)
	logrus.Infof("App created %v. Starting backup.", ctx.GetID())

	// Create backuplocation here programatically using config-map that contains name of secrets to be used, passed from the CLI
	if createBackupLocationFlag {
		err = createBackupLocation(t, appKey+"-backup-location", ctx.GetID(), storkv1.BackupLocationS3, "secret-config")
		require.NoError(t, err, "Error creating backuplocation")
	}

	// Backup application
	err = schedulerDriver.AddTasks(ctxs[0],
		scheduler.ScheduleOptions{AppKeys: appBackupKey})
	require.NoError(t, err, "Error backing-up apps")
	if backupSuccessExpected {
		err = schedulerDriver.WaitForRunning(ctxs[0], defaultWaitTimeout, defaultWaitInterval)
		require.NoError(t, err, "Error waiting for back-up to complete.")
		logrus.Infof("Backup completed. Starting Restore.")

		// Restore application
		err = schedulerDriver.AddTasks(ctxs[0],
			scheduler.ScheduleOptions{AppKeys: appRestoreKey})
		require.NoError(t, err, "Error restoring apps")

		err = schedulerDriver.WaitForRunning(ctxs[0], defaultWaitTimeout, defaultWaitInterval)
		require.NoError(t, err, "Error waiting for restore to complete.")

		logrus.Infof("Restore completed.")

		// Validate applications after restore
		err = schedulerDriver.WaitForRunning(ctxs[0], defaultWaitTimeout, defaultWaitInterval)
		require.NoError(t, err, "Error waiting for restore to complete.")

		logrus.Infof("App validation after restore completed.")

	} else {
		err = schedulerDriver.WaitForRunning(ctxs[0], defaultWaitTimeout/5, defaultWaitInterval)
		require.Error(t, err, "Backup expected to fail in test: %s.", t.Name())
	}

	destroyAndWait(t, ctxs)
}

func createBackupLocation(
	t *testing.T,
	name string,
	namespace string,
	locationtype storkv1.BackupLocationType,
	configMapName string,
) error {
	configMap, err := k8s.Instance().GetConfigMap(configMapName, "default")
	require.NoError(t, err, "Failed to get config map  %s", configMapName)

	secretName := configMap.Data["secret_name"]

	secretObj, err := k8s.Instance().GetSecret(secretName, "default")
	require.NoError(t, err, "Failed to get secret %s", secretName)

	// copy secret to the app namespace
	newSecretObj := secretObj.DeepCopy()
	newSecretObj.Namespace = namespace
	newSecretObj.ResourceVersion = ""
	newSecret, err := k8s.Instance().CreateSecret(newSecretObj)
	// Ignore if secret already exists
	if err != nil && !errors.IsAlreadyExists(err) {
		require.NoError(t, err, "Failed to copy secret %s  to namespace %s", name, namespace)
	}

	backupLocation := &storkv1.BackupLocation{
		ObjectMeta: meta.ObjectMeta{
			Name:        name,
			Namespace:   namespace,
			Annotations: map[string]string{"stork.libopenstorage.ord/skipresource": "true"},
		},
		Location: storkv1.BackupLocationItem{
			Type:         locationtype,
			Path:         "test-restore-path",
			SecretConfig: newSecret.Name,
		},
	}
	_, err = k8s.Instance().CreateBackupLocation(backupLocation)
	return err
}

func applicationBackupRestoreTest(t *testing.T) {
	triggerBackupRestoreTest(
		t,
		[]string{testKey + "-backup"},
		[]string{"mysql-restore"},
		true,
		true,
	)
}

func applicationBackupRestorePreExecRuleTest(t *testing.T) {
	triggerBackupRestoreTest(
		t,
		[]string{testKey + "-pre-exec-rule-backup"},
		[]string{"mysql-restore"},
		false,
		true,
	)
}

func applicationBackupRestorePostExecRuleTest(t *testing.T) {
	triggerBackupRestoreTest(
		t,
		[]string{testKey + "-post-exec-rule-backup"},
		[]string{"mysql-restore"},
		false,
		true,
	)
}

func applicationBackupRestorePreExecMissingRuleTest(t *testing.T) {
	triggerBackupRestoreTest(
		t,
		[]string{testKey + "-pre-exec-missing-rule-backup"},
		[]string{"mysql-restore"},
		false,
		false,
	)
}

func applicationBackupRestorePostExecMissingRuleTest(t *testing.T) {
	triggerBackupRestoreTest(
		t,
		[]string{testKey + "-post-exec-missing-rule-backup"},
		[]string{"mysql-restore"},
		false,
		false,
	)
}

func applicationBackupRestorePreExecFailingRuleTest(t *testing.T) {
	triggerBackupRestoreTest(
		t,
		[]string{testKey + "-pre-exec-failing-rule-backup"},
		[]string{"mysql-restore"},
		false,
		false,
	)
}

func applicationBackupRestorePostExecFailingRuleTest(t *testing.T) {
	triggerBackupRestoreTest(
		t,
		[]string{testKey + "-post-exec-failing-rule-backup"},
		[]string{"mysql-restore"},
		false,
		false,
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
	err := createBackupLocation(t, backupLocation, ctx.GetID(), storkv1.BackupLocationS3, "secret-config")
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
	err := createBackupLocation(t, backupLocation, ctx.GetID(), storkv1.BackupLocationS3, "secret-config")
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
	err := createBackupLocation(t, backupLocation, ctx.GetID(), storkv1.BackupLocationS3, "secret-config")
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
	err := createBackupLocation(t, backupLocation, ctx.GetID(), storkv1.BackupLocationS3, "secret-config")
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
	err := createBackupLocation(t, backupLocation, ctx.GetID(), storkv1.BackupLocationS3, "secret-config")
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
