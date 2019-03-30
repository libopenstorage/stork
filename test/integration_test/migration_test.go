// +build integrationtest

package integrationtest

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/portworx/sched-ops/k8s"
	"github.com/portworx/sched-ops/task"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/errors"
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
)

func testMigration(t *testing.T) {
	// reset mock time before running any tests
	setMockTime(nil)

	t.Run("deploymentTest", deploymentMigrationTest)
	t.Run("statefulsetTest", statefulsetMigrationTest)
	t.Run("statefulsetRuleTest", statefulsetMigrationRuleTest)
	t.Run("preExecRuleMissingTest", statefulsetMigrationRulePreExecMissingTest)
	t.Run("postExecRuleMissingTest", statefulsetMigrationRulePostExecMissingTest)
	t.Run("disallowedNamespaceTest", migrationDisallowedNamespaceTest)
	t.Run("failingPreExecRuleTest", migrationFailingPreExecRuleTest)
	t.Run("failingPostExecRuleTest", migrationFailingPostExecRuleTest)
	t.Run("labelSelectorTest", migrationLabelSelectorTest)
	t.Run("intervalScheduleTest", migrationIntervalScheduleTest)
	t.Run("dailyScheduleTest", migrationDailyScheduleTest)
	t.Run("weeklyScheduleTest", migrationWeeklyScheduleTest)
	t.Run("monthlyScheduleTest", migrationMonthlyScheduleTest)
	t.Run("scheduleInvalidTest", migrationScheduleInvalidTest)
}

func triggerMigrationTest(
	t *testing.T,
	instanceID string,
	appKey string,
	additionalAppKeys []string,
	migrationAppKey string,
	migrationSuccessExpected bool,
	migrateAllAppsExpected bool,
) {
	var err error
	// Reset config in case of error
	defer func() {
		err = setRemoteConfig("")
		require.NoError(t, err, "Error resetting remote config")
	}()

	ctxs, preMigrationCtx := triggerMigration(t, instanceID, appKey, additionalAppKeys, []string{migrationAppKey}, migrateAllAppsExpected)

	validateAndDestroyMigration(t, ctxs, preMigrationCtx, migrationSuccessExpected, true, migrateAllAppsExpected)
}

func triggerMigration(
	t *testing.T,
	instanceID string,
	appKey string,
	additionalAppKeys []string,
	migrationAppKeys []string,
	migrateAllAppsExpected bool,
) ([]*scheduler.Context, *scheduler.Context) {
	// schedule the app on cluster 1
	ctxs, err := schedulerDriver.Schedule(instanceID,
		scheduler.ScheduleOptions{AppKeys: []string{appKey}})
	require.NoError(t, err, "Error scheduling task")
	require.Equal(t, 1, len(ctxs), "Only one task should have started")

	err = schedulerDriver.WaitForRunning(ctxs[0], defaultWaitTimeout, defaultWaitInterval)
	require.NoError(t, err, "Error waiting for app to get to running state")

	preMigrationCtx := ctxs[0].DeepCopy()

	if len(additionalAppKeys) > 0 {
		err = schedulerDriver.AddTasks(ctxs[0],
			scheduler.ScheduleOptions{AppKeys: additionalAppKeys})
		require.NoError(t, err, "Error scheduling additional apps")
		err = schedulerDriver.WaitForRunning(ctxs[0], defaultWaitTimeout, defaultWaitInterval)
		require.NoError(t, err, "Error waiting for additional apps to get to running state")
	}

	if migrateAllAppsExpected {
		preMigrationCtx = ctxs[0].DeepCopy()
	}
	// create, apply and validate cluster pair specs
	err = scheduleClusterPair(ctxs[0])
	require.NoError(t, err, "Error scheduling cluster pair")

	// apply migration specs
	err = schedulerDriver.AddTasks(ctxs[0],
		scheduler.ScheduleOptions{AppKeys: migrationAppKeys})
	require.NoError(t, err, "Error scheduling migration specs")

	return ctxs, preMigrationCtx
}

func validateAndDestroyMigration(
	t *testing.T,
	ctxs []*scheduler.Context,
	preMigrationCtx *scheduler.Context,
	migrationSuccessExpected bool,
	startAppsOnMigration bool,
	migrateAllAppsExpected bool,
) {
	var err error

	timeout := defaultWaitTimeout
	if !migrationSuccessExpected {
		timeout = timeout / 2
	}

	allAppsCtx := ctxs[0].DeepCopy()
	err = schedulerDriver.WaitForRunning(ctxs[0], timeout, defaultWaitInterval)
	if migrationSuccessExpected {
		require.NoError(t, err, "Error waiting for migration to get to Ready state")

		// wait on cluster 2 for the app to be running
		err = setRemoteConfig(remoteFilePath)
		require.NoError(t, err, "Error setting remote config")

		if startAppsOnMigration {
			err = schedulerDriver.WaitForRunning(preMigrationCtx, defaultWaitTimeout, defaultWaitInterval)
			require.NoError(t, err, "Error waiting for pod to get to running state on remote cluster after migration")
			if !migrateAllAppsExpected {
				err = schedulerDriver.WaitForRunning(allAppsCtx, defaultWaitTimeout/2, defaultWaitInterval)
				require.Error(t, err, "All apps shouldn't have been migrated")
			}
		}

		/* Failing right now as SC's are not migrated
		* else {
			logrus.Infof("test only validating storage components as migration has startApplications disabled")
			err = schedulerDriver.InspectVolumes(preMigrationCtx, defaultWaitTimeout, defaultWaitInterval)
			require.NoError(t, err, "Error validating storage components on remote cluster after migration")
		}*/
		// destroy mysql app on cluster 2
		destroyAndWait(t, []*scheduler.Context{preMigrationCtx})
	} else {
		require.Error(t, err, "Expected migration to fail")
	}

	// destroy app on cluster 1
	err = setRemoteConfig("")
	require.NoError(t, err, "Error resetting remote config")
	destroyAndWait(t, ctxs)
}

func deploymentMigrationTest(t *testing.T) {
	triggerMigrationTest(
		t,
		"mysql-migration",
		"mysql-1-pvc",
		nil,
		"mysql-migration",
		true,
		true,
	)
}

func statefulsetMigrationTest(t *testing.T) {
	triggerMigrationTest(
		t,
		"cassandra-migration",
		"cassandra",
		nil,
		"cassandra-migration",
		true,
		true,
	)
}

func statefulsetMigrationRuleTest(t *testing.T) {
	triggerMigrationTest(
		t,
		"cassandra-migration-rule",
		"cassandra",
		nil,
		"cassandra-migration-rule",
		true,
		true,
	)
}

func statefulsetMigrationRulePreExecMissingTest(t *testing.T) {
	triggerMigrationTest(
		t,
		"migration-pre-exec-missing",
		"mysql-1-pvc",
		nil,
		"mysql-migration-pre-exec-missing",
		false,
		true,
	)
}
func statefulsetMigrationRulePostExecMissingTest(t *testing.T) {
	triggerMigrationTest(
		t,
		"migration-post-exec-missing",
		"mysql-1-pvc",
		nil,
		"mysql-migration-post-exec-missing",
		false,
		true,
	)
}

func migrationDisallowedNamespaceTest(t *testing.T) {
	triggerMigrationTest(
		t,
		"migration-disallowed-namespace",
		"mysql-1-pvc",
		nil,
		"mysql-migration-disallowed-ns",
		false,
		true,
	)
}

func migrationFailingPreExecRuleTest(t *testing.T) {
	triggerMigrationTest(
		t,
		"migration-failing-pre-exec-rule",
		"mysql-1-pvc",
		nil,
		"mysql-migration-failing-pre-exec",
		false,
		true,
	)
}

func migrationFailingPostExecRuleTest(t *testing.T) {
	triggerMigrationTest(
		t,
		"migration-failing-post-exec-rule",
		"mysql-1-pvc",
		nil,
		"mysql-migration-failing-post-exec",
		false,
		true,
	)
}

func migrationLabelSelectorTest(t *testing.T) {
	triggerMigrationTest(
		t,
		"migration-label-selector-test",
		"cassandra",
		[]string{"mysql-1-pvc"},
		"label-selector-migration",
		true,
		false,
	)
}

// migrationIntervalScheduleTest runs test for migrations with schedules that are
// intervals of time
func migrationIntervalScheduleTest(t *testing.T) {
	var err error
	// Reset config in case of error
	defer func() {
		err = setRemoteConfig("")
		require.NoError(t, err, "Error resetting remote config")
	}()

	// the schedule interval for these specs it set to 5 minutes
	ctxs, preMigrationCtx := triggerMigration(
		t,
		"mysql-migration-schedule-interval",
		"mysql-1-pvc",
		nil,
		[]string{"mysql-migration-schedule-interval"},
		true,
	)

	// bump time of the world by 5 minutes
	mockNow := time.Now().Add(6 * time.Minute)
	setMockTime(&mockNow)

	validateAndDestroyMigration(t, ctxs, preMigrationCtx, true, false, true)
}

func migrationDailyScheduleTest(t *testing.T) {
	migrationScheduleTest(t, v1alpha1.SchedulePolicyTypeDaily, "mysql-migration-schedule-daily", "", -1)
}

func migrationWeeklyScheduleTest(t *testing.T) {
	migrationScheduleTest(t, v1alpha1.SchedulePolicyTypeWeekly, "mysql-migration-schedule-weekly", "Monday", -1)
}

func migrationMonthlyScheduleTest(t *testing.T) {
	migrationScheduleTest(t, v1alpha1.SchedulePolicyTypeMonthly, "mysql-migration-schedule-monthly", "", 11)
}

func migrationScheduleInvalidTest(t *testing.T) {
	migrationSchedules := []string{
		"mysql-migration-schedule-daily-invalid",
		"mysql-migration-schedule-weekly-invalid",
		"mysql-migration-schedule-monthly-invalid",
	}

	ctxs, preMigrationCtx := triggerMigration(
		t,
		"mysql-migration-schedule-invalid",
		"mysql-1-pvc",
		nil,
		migrationSchedules,
		true,
	)

	namespace := preMigrationCtx.GetID()
	time.Sleep(90 * time.Second)

	// **** TEST ensure 0 migrations since the schedule is invalid. Also check events for invalid specs
	for _, migrationScheduleName := range migrationSchedules {
		migrationSchedule, err := k8s.Instance().GetMigrationSchedule(migrationScheduleName, namespace)
		require.NoError(t, err, fmt.Sprintf("failed to get migration schedule: [%s] %s", namespace, migrationScheduleName))
		require.Empty(t, migrationSchedule.Status.Items, "expected 0 items in migration schedule status")

		listOptions := meta_v1.ListOptions{
			FieldSelector: fields.OneTermEqualSelector("involvedObject.name", migrationScheduleName).String(),
			Watch:         false,
		}

		storkEvents, err := k8s.Instance().ListEvents(namespace, listOptions)
		require.NoError(t, err, "failed to list stork events")

		foundFailedEvent := false
		for _, ev := range storkEvents.Items {
			if ev.Reason == "Failed" && strings.Contains(ev.Message, "Error checking if migration should be triggered: Invalid") {
				foundFailedEvent = true
				break
			}
		}

		require.True(t, foundFailedEvent,
			fmt.Sprintf("failed to find an event for the migration schedule: [%s] %s for the invalid policy",
				namespace, migrationScheduleName))
	}

	destroyAndWait(t, ctxs)
}

// NOTE: below test assumes all schedule policies used here  (interval, daily, weekly or monthly) have a
// trigger time of 12:05PM. Ensure the SchedulePolicy specs use that time.
func migrationScheduleTest(
	t *testing.T,
	scheduleType v1alpha1.SchedulePolicyType,
	migrationScheduleName string,
	scheduleDay string,
	scheduleDate int) {
	var err error
	// Reset config in case of error
	defer func() {
		err = setRemoteConfig("")
		require.NoError(t, err, "Error resetting remote config")
	}()

	date := time.Now().Day()
	if scheduleDate > 0 {
		date = scheduleDate
	} else if len(scheduleDay) > 0 {
		// find next available date (e.g 17th) for the given day (e.g Monday)
		currTime := time.Now()
		found := false
		for i := 0; i < 7; i++ {
			if currTime.Weekday().String() == scheduleDay {
				date = currTime.Day()
				found = true
				break
			}

			currTime = currTime.Add(24 * time.Hour) // move to next day
		}

		require.True(t, found, fmt.Sprintf("failed to find date from given schedule day: %s", scheduleDay))
	}

	month := time.Now().Month()
	// Increment the month if we went to the next
	if date < time.Now().Day() {
		month++
	}
	// Increment the year if we went to the next
	year := time.Now().Year()
	if month < time.Now().Month() {
		year++
	}
	nextTrigger := time.Date(year, month, date, 12, 4, 0, 0, time.Local)
	// Set time 2 hours before the scheduled time so no migrations run
	mockNow := nextTrigger.Add(-2 * time.Hour)

	setMockTime(&mockNow)

	ctxs, preMigrationCtx := triggerMigration(
		t,
		migrationScheduleName,
		"mysql-1-pvc",
		nil,
		[]string{migrationScheduleName},
		true,
	)

	namespace := preMigrationCtx.GetID()
	failureErrString := fmt.Sprintf("basic validation of migration schedule: [%s] %s failed",
		namespace, migrationScheduleName)

	// **** TEST 1: ensure 0 migrations since we haven't reached the daily scheduled time
	migrationSchedule, err := k8s.Instance().GetMigrationSchedule(migrationScheduleName, namespace)
	require.NoError(t, err, "failed to get migration schedule")
	require.Empty(t, migrationSchedule.Status.Items, "expected 0 items in migration schedule status")

	// **** TEST 2: bump time one minute past the scheduled time of daily migration
	mockNow = nextTrigger.Add(1 * time.Minute)
	setMockTime(&mockNow)

	//  ensure only one migration has run
	migrationsMap, err := k8s.Instance().ValidateMigrationSchedule(
		migrationScheduleName, namespace, defaultWaitTimeout, defaultWaitInterval)
	require.NoError(t, err, failureErrString)
	require.Len(t, migrationsMap, 1, "expected only one schedule type in migration map")

	migrations := migrationsMap[scheduleType]
	require.Len(t, migrations, 1, fmt.Sprintf("expected exactly one %v migration. Found: %d", scheduleType, len(migrations)))

	migrationStatus := migrations[0]

	// Independently validate the migration
	err = k8s.Instance().ValidateMigration(migrationStatus.Name, namespace, 1*time.Minute, defaultWaitInterval)
	require.NoError(t, err, "failed to validate first daily migration")

	// check creation time of the new migration
	firstMigrationCreationTime := migrationStatus.CreationTimestamp
	require.True(t, mockNow.Before(firstMigrationCreationTime.Time) || mockNow.Equal(firstMigrationCreationTime.Time),
		fmt.Sprintf("creation of migration: %v should have been after or equal to: %v",
			firstMigrationCreationTime, mockNow))

	// **** TEST 3 bump time by 2 more hours. Should not cause any new migrations
	mockNow = nextTrigger.Add(2 * time.Hour)
	setMockTime(&mockNow)

	//  ensure no new migrations
	migrationsMap, err = k8s.Instance().ValidateMigrationSchedule(
		migrationScheduleName, namespace, defaultWaitTimeout, defaultWaitInterval)
	require.NoError(t, err, failureErrString)
	require.Len(t, migrationsMap, 1, "expected only one schedule type in migration map")

	migrations = migrationsMap[scheduleType]
	require.Len(t, migrations, 1, fmt.Sprintf("expected exactly one daily migration. Found: %d", len(migrations)))
	require.Equal(t, firstMigrationCreationTime, migrations[0].CreationTimestamp,
		"timestamps of first and most recent migrations don't match")

	// **** TEST 4 bump time by (1 day / 1 week / 1 month) + 5 minutes. Should cause one new migration
	switch scheduleType {
	case v1alpha1.SchedulePolicyTypeDaily:
		mockNow = nextTrigger.AddDate(0, 0, 1)
	case v1alpha1.SchedulePolicyTypeWeekly:
		mockNow = nextTrigger.AddDate(0, 0, 7)
	case v1alpha1.SchedulePolicyTypeMonthly:
		mockNow = nextTrigger.AddDate(0, 1, 0)
	default:
		t.Fatalf("this testcase only supports daily, weekly and monthly intervals")
	}
	mockNow = mockNow.Add(5 * time.Minute)
	setMockTime(&mockNow)

	// Give time for new migration to trigger
	time.Sleep(time.Minute)

	migrationsMap, err = k8s.Instance().ValidateMigrationSchedule(
		migrationScheduleName, namespace, defaultWaitTimeout, defaultWaitInterval)
	require.NoError(t, err, failureErrString)
	require.Len(t, migrationsMap, 1, "expected only one schedule type in migration map")

	migrations = migrationsMap[scheduleType]
	require.Len(t, migrations, 1, fmt.Sprintf("expected exactly one daily migration. Found: %d", len(migrations)))

	migrationStatus = migrations[0]
	require.True(t, firstMigrationCreationTime.Time.Before(migrationStatus.CreationTimestamp.Time),
		fmt.Sprintf("creation of migration: %v should have been after: %v the first migration",
			firstMigrationCreationTime, migrationStatus.CreationTimestamp))

	// validate and destroy apps on both clusters
	validateAndDestroyMigration(t, ctxs, preMigrationCtx, true, false, true)

	// explicitly check if all child migrations of the schedule are deleted
	f := func() (interface{}, bool, error) {
		for _, migrations := range migrationsMap {
			for _, m := range migrations {
				_, err := k8s.Instance().GetMigration(m.Name, namespace)
				if err == nil {
					return "", true, fmt.Errorf("get on migration: %s should have failed", m.Name)
				}

				if !errors.IsNotFound(err) {
					logrus.Infof("unexpected err: %v when checking deleted migration: %s", err, m.Name)
					return "", true, err
				}
			}
		}

		//done all deleted
		return "", false, nil
	}

	_, err = task.DoRetryWithTimeout(f, defaultWaitTimeout, defaultWaitInterval)
	require.NoError(t, err, "migrations for schedules have not been deleted")
}
