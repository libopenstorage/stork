//go:build integrationtest
// +build integrationtest

package integrationtest

import (
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/portworx/sched-ops/k8s/apps"
	"github.com/portworx/sched-ops/k8s/core"
	storkops "github.com/portworx/sched-ops/k8s/stork"
	"github.com/portworx/sched-ops/task"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/portworx/torpedo/drivers/scheduler/spec"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	apps_api "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
)

const (
	migrationRetryInterval = 10 * time.Second
	migrationRetryTimeout  = 5 * time.Minute
	rabbitmqNamespace      = "rabbitmq-operator-migration"
)

func TestMigration(t *testing.T) {
	// reset mock time before running any tests
	err := setMockTime(nil)
	require.NoError(t, err, "Error resetting mock time")

	logrus.Infof("Using stork volume driver: %s", volumeDriverName)
	logrus.Infof("Backup path being used: %s", backupLocationPath)

	t.Run("testMigration", testMigration)
	t.Run("testMigrationFailoverFailback", testMigrationFailoverFailback)
	t.Run("deleteStorkPodsSourceDuringMigrationTest", deleteStorkPodsSourceDuringMigrationTest)
	t.Run("deleteStorkPodsDestDuringMigrationTest", deleteStorkPodsDestDuringMigrationTest)
}

func testMigration(t *testing.T) {
	// reset mock time before running any tests
	err := setMockTime(nil)
	require.NoError(t, err, "Error resetting mock time")

	err = setSourceKubeConfig()
	require.NoError(t, err, "failed to set kubeconfig to source cluster: %v", err)

	t.Run("deploymentTest", deploymentMigrationTest)
	t.Run("deploymentMigrationReverseTest", deploymentMigrationReverseTest)
	t.Run("statefulsetTest", statefulsetMigrationTest)
	t.Run("statefulsetStartAppFalseTest", statefulsetMigrationStartAppFalseTest)
	t.Run("statefulsetRuleTest", statefulsetMigrationRuleTest)
	t.Run("preExecRuleMissingTest", statefulsetMigrationRulePreExecMissingTest)
	t.Run("postExecRuleMissingTest", statefulsetMigrationRulePostExecMissingTest)
	t.Run("disallowedNamespaceTest", migrationDisallowedNamespaceTest)
	t.Run("failingPreExecRuleTest", migrationFailingPreExecRuleTest)
	t.Run("failingPostExecRuleTest", migrationFailingPostExecRuleTest)
	// TODO: waiting for https://portworx.atlassian.net/browse/STOR-281 to be resolved
	if authTokenConfigMap == "" {
		t.Run("labelSelectorTest", migrationLabelSelectorTest)
		t.Run("intervalScheduleTest", migrationIntervalScheduleTest)
		t.Run("dailyScheduleTest", migrationDailyScheduleTest)
		t.Run("weeklyScheduleTest", migrationWeeklyScheduleTest)
		t.Run("monthlyScheduleTest", migrationMonthlyScheduleTest)
		t.Run("scheduleInvalidTest", migrationScheduleInvalidTest)
		t.Run("intervalScheduleCleanupTest", intervalScheduleCleanupTest)
	}
	t.Run("networkpolicyTest", networkPolicyMigrationTest)
	t.Run("endpointTest", endpointMigrationTest)
	t.Run("clusterPairFailuresTest", clusterPairFailuresTest)
	t.Run("scaleTest", migrationScaleTest)
	t.Run("pvcResizeTest", pvcResizeMigrationTest)
	t.Run("transformResourceTest", transformResourceTest)
	t.Run("suspendMigrationTest", suspendMigrationTest)
	t.Run("operatorMigrationMongoTest", operatorMigrationMongoTest)
	t.Run("operatorMigrationRabbitmqTest", operatorMigrationRabbitmqTest)
	t.Run("bidirectionalClusterPairTest", bidirectionalClusterPairTest)

	err = setRemoteConfig("")
	require.NoError(t, err, "setting kubeconfig to default failed")
}

func triggerMigrationTest(
	t *testing.T,
	instanceID string,
	appKey string,
	additionalAppKeys []string,
	migrationAppKey string,
	migrationSuccessExpected bool,
	migrateAllAppsExpected bool,
	startAppsOnMigration bool,
) {
	var err error
	// Reset config in case of error
	defer func() {
		err = setSourceKubeConfig()
		require.NoError(t, err, "Error resetting source config")
	}()

	ctxs, preMigrationCtx := triggerMigration(t, instanceID, appKey, additionalAppKeys, []string{migrationAppKey}, migrateAllAppsExpected, false, startAppsOnMigration, false, "", nil)

	validateAndDestroyMigration(t, ctxs, preMigrationCtx, migrationSuccessExpected, startAppsOnMigration, migrateAllAppsExpected, false, false)
}

func triggerMigration(
	t *testing.T,
	instanceID string,
	appKey string,
	additionalAppKeys []string,
	migrationAppKeys []string,
	migrateAllAppsExpected bool,
	skipStoragePair bool,
	startAppsOnMigration bool,
	pairReverse bool,
	projectIDMappings string,
	namespaceLabels map[string]string,
) ([]*scheduler.Context, *scheduler.Context) {
	ctxs, err := schedulerDriver.Schedule(instanceID,
		scheduler.ScheduleOptions{
			AppKeys: []string{appKey},
			Labels:  namespaceLabels,
		})
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
	err = scheduleClusterPair(ctxs[0], skipStoragePair, true, defaultClusterPairDir, projectIDMappings, pairReverse)
	require.NoError(t, err, "Error scheduling cluster pair")

	// apply migration specs
	err = schedulerDriver.AddTasks(ctxs[0],
		scheduler.ScheduleOptions{AppKeys: migrationAppKeys})
	require.NoError(t, err, "Error scheduling migration specs")

	return ctxs, preMigrationCtx
}

// validateMigrationSummary validats the migration summary
// currently we don't have an automated way to find out how many resources got deployed
// through torpedo specs. For ex. a statefulset can have an inline PVC and that should
// get counted as a resource in Migration, but torpedo won't count it as a separate resource
// in its context. The caller is expected provide the counts
func validateMigrationSummary(
	t *testing.T,
	preMigrationCtx *scheduler.Context,
	expectedResources uint64,
	expectedVolumes uint64,
	migrationName, namespace string,
) {
	if preMigrationCtx == nil {
		return
	}
	if preMigrationCtx.App == nil {
		return
	}
	migObj, err := storkops.Instance().GetMigration(migrationName, namespace)
	require.NoError(t, err, "get migration failed")
	require.NotNil(t, migObj.Status.Summary, "migration summary is nil")
	require.Equal(t, migObj.Status.Summary.NumberOfMigratedResources, expectedResources, "unexpected number of resources migrated")
	require.Equal(t, migObj.Status.Summary.NumberOfMigratedVolumes, expectedVolumes, "unexpected number of volumes migrated")
	require.Equal(t, migObj.Status.Summary.TotalNumberOfResources, expectedResources, "unexpected number of total resources")
	require.Equal(t, migObj.Status.Summary.TotalNumberOfVolumes, expectedVolumes, "unexpected number of total volumes")
	if expectedVolumes > 0 {
		require.True(t, migObj.Status.Summary.TotalBytesMigrated > 0, "expected bytes total to be non-zero")
	} else {
		require.True(t, migObj.Status.Summary.TotalBytesMigrated == 0, "expected bytes total to be zero")
	}
}

func validateAndDestroyMigration(
	t *testing.T,
	ctxs []*scheduler.Context,
	preMigrationCtx *scheduler.Context,
	migrationSuccessExpected bool,
	startAppsOnMigration bool,
	migrateAllAppsExpected bool,
	skipAppDeletion bool,
	skipDestDeletion bool,
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
		funcWaitAndDelete := func() {
			if startAppsOnMigration {
				err = schedulerDriver.WaitForRunning(preMigrationCtx, defaultWaitTimeout, defaultWaitInterval)
				require.NoError(t, err, "Error waiting for pod to get to running state on remote cluster after migration")
				if !migrateAllAppsExpected {
					err = schedulerDriver.WaitForRunning(allAppsCtx, defaultWaitTimeout/2, defaultWaitInterval)
					require.Error(t, err, "All apps shouldn't have been migrated")
				}
			} else {
				err = schedulerDriver.WaitForRunning(preMigrationCtx, defaultWaitTimeout/4, defaultWaitInterval)
				require.Error(t, err, "Expected pods to NOT get to running state on remote cluster after migration")
			}

			/* Failing right now as SC's are not migrated
			* else {
				logrus.Infof("test only validating storage components as migration has startApplications disabled")
				err = schedulerDriver.InspectVolumes(preMigrationCtx, defaultWaitTimeout, defaultWaitInterval)
				require.NoError(t, err, "Error validating storage components on remote cluster after migration")
			}*/

			// Validate the migration summary
			// destroy mysql app on cluster 2
			if !skipAppDeletion && !skipDestDeletion {
				destroyAndWait(t, []*scheduler.Context{preMigrationCtx})
			}
		}
		executeOnDestination(t, funcWaitAndDelete)
	} else {
		require.Error(t, err, "Expected migration to fail")
	}

	// destroy app on cluster 1
	if !skipAppDeletion {
		destroyAndWait(t, ctxs)
	}
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
		true,
	)
}

func deploymentMigrationReverseTest(t *testing.T) {
	var err error

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
	validateAndDestroyMigration(t, ctxs, preMigrationCtx, true, true, true, false, false)

	// Change kubeconfig to destination
	err = setDestinationKubeConfig()
	require.NoError(t, err, "failed to set kubeconfig to source cluster: %v", err)

	ctxsReverse, err := schedulerDriver.Schedule("mysql-migration",
		scheduler.ScheduleOptions{AppKeys: []string{"mysql-1-pvc"}})
	require.NoError(t, err, "Error scheduling task")
	require.Equal(t, 1, len(ctxsReverse), "Only one task should have started")

	err = schedulerDriver.WaitForRunning(ctxsReverse[0], defaultWaitTimeout, defaultWaitInterval)
	require.NoError(t, err, "Error waiting for app to get to running state")

	postMigrationCtx := ctxsReverse[0].DeepCopy()

	// create, apply and validate cluster pair specs
	err = scheduleClusterPair(ctxsReverse[0], false, false, "cluster-pair-reverse", "", true)
	require.NoError(t, err, "Error scheduling cluster pair")

	// apply migration specs
	err = schedulerDriver.AddTasks(ctxsReverse[0],
		scheduler.ScheduleOptions{AppKeys: []string{"mysql-migration"}})
	require.NoError(t, err, "Error scheduling migration specs")

	err = schedulerDriver.WaitForRunning(ctxsReverse[0], defaultWaitTimeout, defaultWaitInterval)
	require.NoError(t, err, "Error waiting for migration to complete")

	destroyAndWait(t, ctxsReverse)

	// Cleanup up source
	err = setSourceKubeConfig()
	require.NoError(t, err, "failed to set kubeconfig to source cluster: %v", err)

	err = schedulerDriver.WaitForRunning(postMigrationCtx, defaultWaitTimeout, defaultWaitInterval)
	require.NoError(t, err, "Error waiting for migration to complete")

	//validateAndDestroyMigration(t, []*scheduler.Context{preMigrationCtx}, preMigrationCtx, true, true, true, true, false)

	destroyAndWait(t, []*scheduler.Context{postMigrationCtx})
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
		true,
	)
}

func statefulsetMigrationStartAppFalseTest(t *testing.T) {
	triggerMigrationTest(
		t,
		"cassandra-migration",
		"cassandra",
		nil,
		"cassandra-migration-startapps-false",
		true,
		true,
		false,
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
		true,
	)
}

// migrationIntervalScheduleTest runs test for migrations with schedules that are
// intervals of time
func migrationIntervalScheduleTest(t *testing.T) {
	var err error
	// Reset config in case of error
	defer func() {
		err = setSourceKubeConfig()
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
		false,
		false,
		false,
		"",
		nil,
	)

	// bump time of the world by 5 minutes
	mockNow := time.Now().Add(6 * time.Minute)
	err = setMockTime(&mockNow)
	require.NoError(t, err, "Error setting mock time")

	validateAndDestroyMigration(t, ctxs, preMigrationCtx, true, false, true, false, false)
}

// intervalScheduleCleanupTest runs test for migrations with schedules that are
// intervals of time, will try to perform cleanup of k8s resources which are deleted
// on source cluster
func intervalScheduleCleanupTest(t *testing.T) {
	var err error
	var name, namespace string
	var pvcs *v1.PersistentVolumeClaimList
	// Reset config in case of error
	defer func() {
		err = setSourceKubeConfig()
		require.NoError(t, err, "Error resetting remote config")
	}()
	err = setMockTime(nil)
	require.NoError(t, err, "Error resetting mock time")
	// the schedule interval for these specs it set to 5 minutes
	ctxs, preMigrationCtx := triggerMigration(
		t,
		"mysql-migration-schedule-interval",
		"mysql-1-pvc",
		[]string{"cassandra"},
		[]string{"mysql-migration-schedule-interval"},
		true,
		false,
		false,
		false,
		"",
		nil,
	)

	validateMigration(t, "mysql-migration-schedule-interval", preMigrationCtx.GetID())

	// delete statefulset from source cluster
	for i, spec := range ctxs[0].App.SpecList {
		if obj, ok := spec.(*apps_api.StatefulSet); ok {
			name = obj.GetName()
			namespace = obj.GetNamespace()
			pvcs, err = apps.Instance().GetPVCsForStatefulSet(obj)
			require.NoError(t, err, "error getting pvcs for ss")
			err = apps.Instance().DeleteStatefulSet(name, namespace)
			require.NoError(t, err, "error deleting cassandra statefulset")
			err = apps.Instance().ValidateStatefulSet(obj, 1*time.Minute)
			require.NoError(t, err, "error deleting cassandra statefulset")
			ctxs[0].App.SpecList = append(ctxs[0].App.SpecList[:i], ctxs[0].App.SpecList[i+1:]...)
			break
		}
	}
	// remove statefulset from preMigrationCtx as well
	for i, spec := range preMigrationCtx.App.SpecList {
		if _, ok := spec.(*apps_api.StatefulSet); ok {
			preMigrationCtx.App.SpecList = append(preMigrationCtx.App.SpecList[:i],
				preMigrationCtx.App.SpecList[i+1:]...)
			break
		}
	}

	// delete pvcs
	for _, pvc := range pvcs.Items {
		err := core.Instance().DeletePersistentVolumeClaim(pvc.Name, pvc.Namespace)
		require.NoError(t, err, "Error deleting pvc")
	}

	// bump time of the world by 5 minutes
	mockNow := time.Now().Add(6 * time.Minute)
	err = setMockTime(&mockNow)
	require.NoError(t, err, "Error setting mock time")

	// verify app deleted on source cluster with second migration
	time.Sleep(1 * time.Minute)
	validateMigration(t, "mysql-migration-schedule-interval", preMigrationCtx.GetID())
	validateMigrationCleanup(t, name, namespace, pvcs)

	validateAndDestroyMigration(t, ctxs, preMigrationCtx, true, false, true, false, false)
}

func validateMigrationCleanup(t *testing.T, name, namespace string, pvcs *v1.PersistentVolumeClaimList) {
	// validate if statefulset got deleted on cluster2
	err := setDestinationKubeConfig()
	require.NoError(t, err, "failed to set kubeconfig to destination cluster: %v", err)

	// Verify if statefulset get delete
	_, err = apps.Instance().GetStatefulSet(name, namespace)
	require.Error(t, err, "expected ss:%v error not found", name)

	for _, pvc := range pvcs.Items {
		resp, err := core.Instance().GetPersistentVolumeClaim(pvc.Name, pvc.Namespace)
		if err == nil {
			require.NotNil(t, resp.DeletionTimestamp)
		} else {
			require.Error(t, err, "expected pvc to be deleted:%v", resp.Name)
		}
	}

	err = setSourceKubeConfig()
	require.NoError(t, err, "Error resetting remote config")
}

func validateMigration(t *testing.T, name, namespace string) {
	//  ensure only one migration has run
	migrationsMap, err := storkops.Instance().ValidateMigrationSchedule(
		name, namespace, defaultWaitTimeout, defaultWaitInterval)
	require.NoError(t, err, "error getting migration schedule")
	require.Len(t, migrationsMap, 1, "expected only one schedule type in migration map")

	migrationStatus := migrationsMap[v1alpha1.SchedulePolicyTypeInterval][0]
	// Independently validate the migration
	err = storkops.Instance().ValidateMigration(
		migrationStatus.Name, namespace, defaultWaitTimeout, defaultWaitInterval)
	require.NoError(t, err, "failed to validate migration")
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
		false,
		false,
		false,
		"",
		nil,
	)

	namespace := preMigrationCtx.GetID()
	time.Sleep(90 * time.Second)

	// **** TEST ensure 0 migrations since the schedule is invalid. Also check events for invalid specs
	for _, migrationScheduleName := range migrationSchedules {
		migrationSchedule, err := storkops.Instance().GetMigrationSchedule(migrationScheduleName, namespace)
		require.NoError(t, err, fmt.Sprintf("failed to get migration schedule: [%s] %s", namespace, migrationScheduleName))
		require.Empty(t, migrationSchedule.Status.Items, "expected 0 items in migration schedule status")

		listOptions := meta_v1.ListOptions{
			FieldSelector: fields.OneTermEqualSelector("involvedObject.name", migrationScheduleName).String(),
			Watch:         false,
		}

		storkEvents, err := core.Instance().ListEvents(namespace, listOptions)
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
		err = setSourceKubeConfig()
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

	err = setMockTime(&mockNow)
	require.NoError(t, err, "Error setting mock time")

	ctxs, preMigrationCtx := triggerMigration(
		t,
		migrationScheduleName,
		"mysql-1-pvc",
		nil,
		[]string{migrationScheduleName},
		true,
		false,
		false,
		false,
		"",
		nil,
	)

	namespace := preMigrationCtx.GetID()
	failureErrString := fmt.Sprintf("basic validation of migration schedule: [%s] %s failed",
		namespace, migrationScheduleName)

	// **** TEST 1: ensure 0 migrations since we haven't reached the daily scheduled time
	migrationSchedule, err := storkops.Instance().GetMigrationSchedule(migrationScheduleName, namespace)
	require.NoError(t, err, "failed to get migration schedule")
	require.Empty(t, migrationSchedule.Status.Items, "expected 0 items in migration schedule status")

	// **** TEST 2: bump time one minute past the scheduled time of daily migration
	mockNow = nextTrigger.Add(1 * time.Minute)
	err = setMockTime(&mockNow)
	require.NoError(t, err, "Error setting mock time")

	//  ensure only one migration has run
	migrationsMap, err := storkops.Instance().ValidateMigrationSchedule(
		migrationScheduleName, namespace, defaultWaitTimeout, defaultWaitInterval)
	require.NoError(t, err, failureErrString)
	require.Len(t, migrationsMap, 1, "expected only one schedule type in migration map")

	migrations := migrationsMap[scheduleType]
	require.Len(t, migrations, 1, fmt.Sprintf("expected exactly one %v migration. Found: %d", scheduleType, len(migrations)))

	migrationStatus := migrations[0]

	// Independently validate the migration
	err = storkops.Instance().ValidateMigration(migrationStatus.Name, namespace, 1*time.Minute, defaultWaitInterval)
	require.NoError(t, err, "failed to validate first daily migration")

	// check creation time of the new migration
	firstMigrationCreationTime := migrationStatus.CreationTimestamp
	require.True(t, mockNow.Before(firstMigrationCreationTime.Time) || mockNow.Equal(firstMigrationCreationTime.Time),
		fmt.Sprintf("creation of migration: %v should have been after or equal to: %v",
			firstMigrationCreationTime, mockNow))

	// **** TEST 3 bump time by 2 more hours. Should not cause any new migrations
	mockNow = nextTrigger.Add(2 * time.Hour)
	err = setMockTime(&mockNow)
	require.NoError(t, err, "Error setting mock time")

	//  ensure no new migrations
	migrationsMap, err = storkops.Instance().ValidateMigrationSchedule(
		migrationScheduleName, namespace, defaultWaitTimeout, defaultWaitInterval)
	require.NoError(t, err, failureErrString)
	require.Len(t, migrationsMap, 1, "expected only one schedule type in migration map")

	migrations = migrationsMap[scheduleType]
	require.Len(t, migrations, 1, fmt.Sprintf("expected exactly one %v migration. Found: %d",
		scheduleType, len(migrations)))
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
	err = setMockTime(&mockNow)
	require.NoError(t, err, "Error setting mock time")

	// Give time for new migration to trigger
	time.Sleep(time.Minute)

	for i := 0; i < 10; i++ {
		migrationsMap, err = storkops.Instance().ValidateMigrationSchedule(
			migrationScheduleName, namespace, defaultWaitTimeout, defaultWaitInterval)
		require.NoError(t, err, failureErrString)
		require.Len(t, migrationsMap, 1, "expected only one schedule type in migration map")

		migrations = migrationsMap[scheduleType]
		// If there are more than 1 migrations, the prune might still be in
		// progress, so retry after a short sleep
		if len(migrations) == 1 {
			break
		}
		time.Sleep(10 * time.Second)
	}
	require.Len(t, migrations, 1, fmt.Sprintf("expected exactly one %v migration. Found: %d", scheduleType, len(migrations)))

	migrationStatus = migrations[0]
	require.True(t, firstMigrationCreationTime.Time.Before(migrationStatus.CreationTimestamp.Time),
		fmt.Sprintf("creation of migration: %v should have been after: %v the first migration",
			firstMigrationCreationTime, migrationStatus.CreationTimestamp))

	// validate and destroy apps on both clusters
	validateAndDestroyMigration(t, ctxs, preMigrationCtx, true, false, true, false, false)

	// explicitly check if all child migrations of the schedule are deleted
	f := func() (interface{}, bool, error) {
		for _, migrations := range migrationsMap {
			for _, m := range migrations {
				_, err := storkops.Instance().GetMigration(m.Name, namespace)
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

func migrationScaleTest(t *testing.T) {
	triggerMigrationScaleTest(
		t,
		"mysql-migration",
		"mysql-1-pvc",
		true,
		true,
		true,
	)
}

func triggerMigrationScaleTest(t *testing.T, migrationKey, migrationAppKey string, includeResourcesFlag, includeVolumesFlag, startApplicationsFlag bool) {
	var appCtxs []*scheduler.Context
	var ctxs []*scheduler.Context
	var allMigrations []*v1alpha1.Migration
	var err error

	// Reset config in case of error
	defer func() {
		err = setRemoteConfig("")
		require.NoError(t, err, "Error resetting remote config")
	}()

	for i := 1; i <= migrationScaleCount; i++ {
		currCtxs, err := schedulerDriver.Schedule(migrationKey+"-"+strconv.Itoa(i),
			scheduler.ScheduleOptions{AppKeys: []string{migrationAppKey}})
		require.NoError(t, err, "Error scheduling task")
		require.Equal(t, 1, len(currCtxs), "Only one task should have started")

		err = schedulerDriver.WaitForRunning(currCtxs[0], defaultWaitTimeout, defaultWaitInterval)
		require.NoError(t, err, "Error waiting for app to get to running state")

		// Save context without the clusterpair object
		preMigrationCtx := currCtxs[0].DeepCopy()
		appCtxs = append(appCtxs, preMigrationCtx)

		// create, apply and validate cluster pair specs
		err = scheduleClusterPair(currCtxs[0], false, true, defaultClusterPairDir, "", false)
		require.NoError(t, err, "Error scheduling cluster pair")
		ctxs = append(ctxs, currCtxs...)

		currMigNamespace := migrationAppKey + "-" + migrationKey + "-" + strconv.Itoa(i)
		currMig, err := createMigration(t, migrationKey, currMigNamespace, "remoteclusterpair", currMigNamespace, &includeResourcesFlag, &includeVolumesFlag, &startApplicationsFlag)
		require.NoError(t, err, "failed to create migration: %s in namespace %s", migrationKey, currMigNamespace)
		allMigrations = append(allMigrations, currMig)

	}
	err = WaitForMigration(allMigrations)
	require.NoError(t, err, "Error in scaled migrations")

	// Validate apps on destination
	if startApplicationsFlag {
		// wait on cluster 2 for the app to be running
		err = setDestinationKubeConfig()
		require.NoError(t, err, "failed to set kubeconfig to destination cluster: %v", err)

		for _, ctx := range appCtxs {
			err = schedulerDriver.WaitForRunning(ctx, defaultWaitTimeout, defaultWaitInterval)
			require.NoError(t, err, "Error waiting for app to get to running state on destination cluster")
		}
		// Delete all apps on destination cluster
		destroyAndWait(t, appCtxs)
	}

	err = setSourceKubeConfig()
	require.NoError(t, err, "failed to set kubeconfig to source cluster: %v", err)

	// Delete all apps and cluster pairs on source cluster
	destroyAndWait(t, ctxs)

	// Delete migrations
	err = deleteMigrations(allMigrations)
	require.NoError(t, err, "error in deleting migrations.")
}

func clusterPairFailuresTest(t *testing.T) {
	ctxs, err := schedulerDriver.Schedule("cluster-pair-failures",
		scheduler.ScheduleOptions{AppKeys: []string{testKey}})
	require.NoError(t, err, "Error scheduling task")
	require.Equal(t, 1, len(ctxs), "Only one task should have started")

	err = schedulerDriver.WaitForRunning(ctxs[0], defaultWaitTimeout, defaultWaitInterval)
	require.NoError(t, err, "Error waiting for app to get to running state")

	var clusterPairCtx = &scheduler.Context{
		UID:             ctxs[0].UID,
		ScheduleOptions: scheduler.ScheduleOptions{Namespace: "cp-failure"},
		App: &spec.AppSpec{
			Key:      ctxs[0].App.Key,
			SpecList: []interface{}{},
		}}

	badTokenInfo, errPairing := volumeDriver.GetClusterPairingInfo(remoteFilePath, "", IsEks(), false)
	require.NoError(t, errPairing, "Error writing to clusterpair.yml: %v")

	// Change token value to an incorrect token
	badTokenInfo[tokenKey] = "randomtoken"
	err = createClusterPair(badTokenInfo, false, true, defaultClusterPairDir, "")
	require.NoError(t, err, "Error creating cluster Spec: %v")

	err = schedulerDriver.RescanSpecs(specDir, volumeDriverName)
	require.NoError(t, err, "Unable to parse spec dir: %v")

	err = schedulerDriver.AddTasks(clusterPairCtx,
		scheduler.ScheduleOptions{AppKeys: []string{defaultClusterPairDir}})
	require.NoError(t, err, "Failed to schedule Cluster Pair Specs: %v")

	// We would like to exit early in case of negative tests
	err = schedulerDriver.WaitForRunning(clusterPairCtx, defaultWaitTimeout/10, defaultWaitInterval)
	require.Error(t, err, "Cluster pairing should have failed due to incorrect token")

	destroyAndWait(t, []*scheduler.Context{clusterPairCtx})

	badIPInfo, errPairing := volumeDriver.GetClusterPairingInfo(remoteFilePath, "", IsEks(), false)
	require.NoError(t, errPairing, "Error writing to clusterpair.yml: %v")

	badIPInfo[clusterIP] = "0.0.0.0"

	err = createClusterPair(badIPInfo, false, true, defaultClusterPairDir, "")
	require.NoError(t, err, "Error creating cluster Spec: %v")

	err = schedulerDriver.RescanSpecs(specDir, volumeDriverName)
	require.NoError(t, err, "Unable to parse spec dir: %v")

	err = schedulerDriver.AddTasks(clusterPairCtx,
		scheduler.ScheduleOptions{AppKeys: []string{defaultClusterPairDir}})
	require.NoError(t, err, "Failed to schedule Cluster Pair Specs: %v")

	// We would like to exit early in case of negative tests
	err = schedulerDriver.WaitForRunning(clusterPairCtx, defaultWaitTimeout/10, defaultWaitInterval)
	require.Error(t, err, "Cluster pairing should have failed due to incorrect IP")

	destroyAndWait(t, []*scheduler.Context{clusterPairCtx})

	badPortInfo, errPairing := volumeDriver.GetClusterPairingInfo(remoteFilePath, "", IsEks(), false)
	require.NoError(t, errPairing, "Error writing to clusterpair.yml: %v")

	badPortInfo[clusterPort] = "0000"

	err = createClusterPair(badPortInfo, false, true, defaultClusterPairDir, "")
	require.NoError(t, err, "Error creating cluster Spec: %v")

	err = schedulerDriver.RescanSpecs(specDir, volumeDriverName)
	require.NoError(t, err, "Unable to parse spec dir: %v")

	err = schedulerDriver.AddTasks(clusterPairCtx,
		scheduler.ScheduleOptions{AppKeys: []string{defaultClusterPairDir}})
	require.NoError(t, err, "Failed to schedule Cluster Pair Specs")

	// We would like to exit early in case of negative tests
	err = schedulerDriver.WaitForRunning(clusterPairCtx, defaultWaitTimeout/10, defaultWaitInterval)
	require.Error(t, err, "Cluster pairing should have failed due to incorrect port")

	destroyAndWait(t, []*scheduler.Context{clusterPairCtx})
	destroyAndWait(t, ctxs)

}

func bidirectionalClusterPairTest(t *testing.T) {
	clusterPairName := "birectional-cluster-pair"
	clusterPairNamespace := "bidirectional-clusterpair-ns"

	// Scheduler cluster pairs: source cluster --> destination cluster and destination cluster --> source cluster
	err := scheduleBidirectionalClusterPair(clusterPairName, clusterPairNamespace, "")
	require.NoError(t, err, "failed to set bidirectional cluster pair: %v", err)

	err = setSourceKubeConfig()
	require.NoError(t, err, "failed to set kubeconfig to source cluster: %v", err)

	_, err = storkops.Instance().GetClusterPair(clusterPairName, clusterPairNamespace)
	require.NoError(t, err, "failed to get bidirectional cluster pair on source: %v", err)

	err = storkops.Instance().ValidateClusterPair(clusterPairName, clusterPairNamespace, defaultWaitTimeout, defaultWaitInterval)
	require.NoError(t, err, "failed to validate bidirectional cluster pair on source: %v", err)

	logrus.Infof("Successfully validated cluster pair %s in namespace %s on source cluster", clusterPairName, clusterPairNamespace)

	// Clean up on source cluster
	err = storkops.Instance().DeleteClusterPair(clusterPairName, clusterPairNamespace)
	require.NoError(t, err, "Error deleting clusterpair on source cluster")

	logrus.Infof("Successfully deleted cluster pair %s in namespace %s on source cluster", clusterPairName, clusterPairNamespace)

	err = setDestinationKubeConfig()
	require.NoError(t, err, "failed to set kubeconfig to destination cluster: %v", err)

	_, err = storkops.Instance().GetClusterPair(clusterPairName, clusterPairNamespace)
	require.NoError(t, err, "failed to get bidirectional cluster pair on destination: %v", err)

	err = storkops.Instance().ValidateClusterPair(clusterPairName, clusterPairNamespace, defaultWaitTimeout, defaultWaitInterval)
	require.NoError(t, err, "failed to validate bidirectional cluster pair on destination: %v", err)

	logrus.Infof("Successfully validated cluster pair %s in namespace %s on destination cluster", clusterPairName, clusterPairNamespace)

	// Clean up on destination cluster
	err = storkops.Instance().DeleteClusterPair(clusterPairName, clusterPairNamespace)
	require.NoError(t, err, "Error deleting clusterpair on destination cluster")

	logrus.Infof("Successfully deleted cluster pair %s in namespace %s on destination cluster", clusterPairName, clusterPairNamespace)

	err = setSourceKubeConfig()
	require.NoError(t, err, "failed to set kubeconfig to source cluster: %v", err)
}

func operatorMigrationMongoTest(t *testing.T) {
	triggerMigrationTest(
		t,
		"migration",
		"mongo-operator",
		nil,
		"mongo-op-migration",
		true,
		true,
		true,
	)
}

func operatorMigrationRabbitmqTest(t *testing.T) {
	_, err := core.Instance().CreateNamespace(&v1.Namespace{
		ObjectMeta: meta_v1.ObjectMeta{
			Name: rabbitmqNamespace,
			Labels: map[string]string{
				"app.kubernetes.io/component": "rabbitmq-operator",
				"app.kubernetes.io/name":      "rabbitmq-system",
				"app.kubernetes.io/part-of":   "rabbitmq",
			},
		},
	})
	if !errors.IsAlreadyExists(err) {
		require.NoError(t, err, "failed to create namespace %s for rabbitmq", rabbitmqNamespace)
	}

	triggerMigrationTest(
		t,
		"migration",
		"rabbitmq-operator",
		nil,
		"rabbitmq-migration",
		true,
		true,
		true,
	)
}

func createMigration(
	t *testing.T,
	name string,
	namespace string,
	clusterPair string,
	migrationNamespace string,
	includeResources *bool,
	includeVolumes *bool,
	startApplications *bool,
) (*v1alpha1.Migration, error) {

	migration := &v1alpha1.Migration{
		ObjectMeta: meta_v1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: v1alpha1.MigrationSpec{
			ClusterPair:       clusterPair,
			IncludeResources:  includeResources,
			IncludeVolumes:    includeVolumes,
			StartApplications: startApplications,
			Namespaces:        []string{migrationNamespace},
		},
	}
	if authTokenConfigMap != "" {
		err := addSecurityAnnotation(migration)
		if err != nil {
			return nil, err
		}
	}

	mig, err := storkops.Instance().CreateMigration(migration)
	return mig, err
}

func deleteMigrations(migrations []*v1alpha1.Migration) error {
	for _, mig := range migrations {
		err := storkops.Instance().DeleteMigration(mig.Name, mig.Namespace)
		if err != nil {
			return fmt.Errorf("Failed to delete migration %s in namespace %s. Error: %v", mig.Name, mig.Namespace, err)
		}
	}
	return nil
}

func WaitForMigration(migrationList []*v1alpha1.Migration) error {
	checkMigrations := func() (interface{}, bool, error) {
		isComplete := true
		for _, m := range migrationList {
			mig, err := storkops.Instance().GetMigration(m.Name, m.Namespace)
			if err != nil {
				return "", false, err
			}
			if mig.Status.Status != v1alpha1.MigrationStatusSuccessful {
				logrus.Infof("Migration %s in namespace %s is pending", m.Name, m.Namespace)
				isComplete = false
			}
		}
		if isComplete {
			return "", false, nil
		}
		return "", true, fmt.Errorf("some migrations are still pending")
	}
	_, err := task.DoRetryWithTimeout(checkMigrations, migrationRetryTimeout, migrationRetryInterval)
	return err
}

// pvcResizeMigrationTest validate migrated pvcs size get
// reflected correctly after change on source pvc
// it also make sure that sc param is kept for migrated pvcs
func pvcResizeMigrationTest(t *testing.T) {
	var err error
	var namespace string
	var pvcs *v1.PersistentVolumeClaimList
	// Reset config in case of error
	defer func() {
		err = setSourceKubeConfig()
		require.NoError(t, err, "Error resetting remote config")
	}()
	err = setMockTime(nil)
	require.NoError(t, err, "Error resetting mock time")
	// the schedule interval for these specs it set to 5 minutes
	ctxs, preMigrationCtx := triggerMigration(
		t,
		"mysql-migration-schedule-interval",
		"mysql-1-pvc",
		[]string{"cassandra"},
		[]string{"mysql-migration-schedule-interval"},
		true,
		false,
		false,
		false,
		"",
		nil,
	)

	validateMigration(t, "mysql-migration-schedule-interval", preMigrationCtx.GetID())

	// Get pvc lists from source cluster
	for _, spec := range ctxs[0].App.SpecList {
		if obj, ok := spec.(*apps_api.StatefulSet); ok {
			namespace = obj.GetNamespace()
			pvcs, err = core.Instance().GetPersistentVolumeClaims(namespace, nil)
			require.NoError(t, err, "error retriving pvc list from %s namespace", namespace)
			break
		}
	}

	// resize pvcs
	for _, pvc := range pvcs.Items {
		cap := pvc.Status.Capacity[v1.ResourceName(v1.ResourceStorage)]
		currSize := cap.Value()
		pvc.Spec.Resources.Requests[v1.ResourceStorage] = *resource.NewQuantity(int64(currSize*2), resource.BinarySI)
		_, err := core.Instance().UpdatePersistentVolumeClaim(&pvc)
		require.NoError(t, err, "Error updating pvc: %s/%s", pvc.GetNamespace(), pvc.GetName())
	}
	logrus.Infof("Resized PVCs on source cluster")
	// bump time of the world by 5 minutes
	mockNow := time.Now().Add(6 * time.Minute)
	err = setMockTime(&mockNow)
	require.NoError(t, err, "Error setting mock time")

	time.Sleep(10 * time.Second)
	logrus.Infof("Trigger second migration")
	validateMigration(t, "mysql-migration-schedule-interval", preMigrationCtx.GetID())

	// Change kubeconfig to destination
	err = setDestinationKubeConfig()
	require.NoError(t, err, "failed to set kubeconfig to source cluster: %v", err)

	// verify resized pvcs
	for _, pvc := range pvcs.Items {
		cap := pvc.Status.Capacity[v1.ResourceName(v1.ResourceStorage)]
		pvcSize := cap.Value()

		migratedPvc, err := core.Instance().GetPersistentVolumeClaim(pvc.GetName(), pvc.GetNamespace())
		require.NoError(t, err, "error retriving pvc %s/%s", pvc.GetNamespace(), pvc.GetName())

		cap = migratedPvc.Status.Capacity[v1.ResourceName(v1.ResourceStorage)]
		migrSize := cap.Value()

		if migrSize != pvcSize*2 {
			resizeErr := fmt.Errorf("pvc %s/%s is not resized. Expected: %v, Current: %v", pvc.GetNamespace(), pvc.GetName(), pvcSize*2, migrSize)
			require.NoError(t, resizeErr, resizeErr)
		}
		srcSC, err := getStorageClassNameForPVC(&pvc)
		require.NoError(t, err, "error retriving sc for %s/%s", pvc.GetNamespace(), pvc.GetName())

		destSC, err := getStorageClassNameForPVC(migratedPvc)
		require.NoError(t, err, "error retriving sc for %s/%s", migratedPvc.GetNamespace(), migratedPvc.GetName())

		if srcSC != destSC {
			scErr := fmt.Errorf("migrated pvc storage class does not match")
			require.NoError(t, scErr, "SC Expected: %v, Current: %v", srcSC, destSC)
		}
	}

	logrus.Infof("Successfully verified migrated pvcs on destination cluster")

	err = setSourceKubeConfig()
	require.NoError(t, err, "failed to set kubeconfig to source cluster: %v", err)
	validateAndDestroyMigration(t, ctxs, preMigrationCtx, true, false, true, false, false)
}

func getStorageClassNameForPVC(pvc *v1.PersistentVolumeClaim) (string, error) {
	var scName string
	if pvc.Spec.StorageClassName != nil && len(*pvc.Spec.StorageClassName) > 0 {
		scName = *pvc.Spec.StorageClassName
	} else {
		scName = pvc.Annotations[v1.BetaStorageClassAnnotation]
	}

	if len(scName) == 0 {
		return "", fmt.Errorf("PVC: %s does not have a storage class", pvc.Name)
	}
	return scName, nil
}

func suspendMigrationTest(t *testing.T) {
	var err error
	var namespace string
	// Reset config in case of error
	defer func() {
		err = setSourceKubeConfig()
		require.NoError(t, err, "Error resetting remote config")
	}()
	err = setMockTime(nil)
	require.NoError(t, err, "Error resetting mock time")
	// the schedule interval for these specs it set to 5 minutes
	ctxs, preMigrationCtx := triggerMigration(
		t,
		"mysql-migration-schedule-interval-autosuspend",
		"mysql-1-pvc",
		[]string{"cassandra"},
		[]string{"mysql-migration-schedule-interval-autosuspend"},
		true,
		false,
		false,
		false,
		"",
		nil,
	)

	validateMigration(t, "mysql-migration-schedule-interval-autosuspend", preMigrationCtx.GetID())

	// Change kubeconfig to destination
	err = setDestinationKubeConfig()
	require.NoError(t, err, "failed to set kubeconfig to source cluster: %v", err)

	// start sts on dr cluster
	for _, spec := range ctxs[0].App.SpecList {
		if obj, ok := spec.(*apps_api.StatefulSet); ok {
			scale := int32(1)
			sts, err := apps.Instance().GetStatefulSet(obj.GetName(), obj.GetNamespace())
			require.NoError(t, err, "Error retriving sts: %s/%s", obj.GetNamespace(), obj.GetName())
			sts.Spec.Replicas = &scale
			namespace = obj.GetNamespace()
			_, err = apps.Instance().UpdateStatefulSet(sts)
			require.NoError(t, err, "Error updating sts: %s/%s", obj.GetNamespace(), obj.GetName())
			break
		}
	}

	// verify migration status on DR cluster
	migrSched, err := storkops.Instance().GetMigrationSchedule("mysql-migration-schedule-interval-autosuspend", namespace)
	require.NoError(t, err, "failed to retrive migration schedule: %v", err)

	if migrSched.Spec.Suspend != nil && !(*migrSched.Spec.Suspend) {
		// fail test
		suspendErr := fmt.Errorf("migrationschedule is not in suspended state on DR cluster: %s/%s", migrSched.GetName(), migrSched.GetNamespace())
		require.NoError(t, err, "Failed: %v", suspendErr)
	}

	// validate if migrationschedule is suspended on source cluster
	err = setSourceKubeConfig()
	require.NoError(t, err, "failed to set kubeconfig to source cluster: %v", err)

	migrSched, err = storkops.Instance().GetMigrationSchedule("mysql-migration-schedule-interval-autosuspend", namespace)
	require.NoError(t, err, "failed to retrive migration schedule: %v", err)

	if migrSched.Spec.Suspend != nil && !(*migrSched.Spec.Suspend) {
		// fail test
		suspendErr := fmt.Errorf("migrationschedule is not suspended on source cluster : %s/%s", migrSched.GetName(), migrSched.GetNamespace())
		require.NoError(t, err, "Failed: %v", suspendErr)
	}

	logrus.Infof("Successfully verified suspend migration case")

	validateAndDestroyMigration(t, ctxs, preMigrationCtx, true, false, true, false, false)
}

func endpointMigrationTest(t *testing.T) {
	var err error
	namespace := "endpoint-migration-schedule-interval"
	// Reset config in case of error
	defer func() {
		err = setSourceKubeConfig()
		require.NoError(t, err, "Error resetting remote config")
	}()
	err = setMockTime(nil)
	require.NoError(t, err, "Error resetting mock time")
	// the schedule interval for these specs it set to 5 minutes
	ctxs, preMigrationCtx := triggerMigration(
		t,
		"endpoint-migration-schedule-interval",
		"endpoint",
		[]string{},
		[]string{"endpoint-migration-schedule-interval"},
		true,
		false,
		false,
		false,
		"",
		nil,
	)

	validateMigration(t, "endpoint-migration-schedule-interval", preMigrationCtx.GetID())

	srcEndpoints, err := core.Instance().ListEndpoints(namespace, meta_v1.ListOptions{})
	require.NoError(t, err, "error retriving endpoints list from %s namespace", namespace)
	cnt := 0
	for _, endpoint := range srcEndpoints.Items {
		collect := false
		for _, subset := range endpoint.Subsets {
			for _, addr := range subset.Addresses {
				if addr.TargetRef != nil {
					collect = true
				}
			}
		}
		if !collect {
			cnt++
		}
	}
	logrus.Infof("endpoint on source cluster: %+v", srcEndpoints)

	// Change kubeconfig to destination
	err = setDestinationKubeConfig()
	require.NoError(t, err, "failed to set kubeconfig to source cluster: %v", err)

	destEndpoints, err := core.Instance().ListEndpoints(namespace, meta_v1.ListOptions{})
	require.NoError(t, err, "error retriving endpoints list from %s namespace", namespace)

	if len(destEndpoints.Items) != cnt {
		matchErr := fmt.Errorf("migrated endpoints does not match")
		require.NoError(t, matchErr, "Endpoints Expected: %v, Current: %v", srcEndpoints, destEndpoints)
	}

	logrus.Infof("Successfully verified migrated endpoints on destination cluster")

	err = setSourceKubeConfig()
	require.NoError(t, err, "failed to set kubeconfig to source cluster: %v", err)
	validateAndDestroyMigration(t, ctxs, preMigrationCtx, true, false, true, false, false)
}

// networkPolicyMigrationTest validate migrated network policy
func networkPolicyMigrationTest(t *testing.T) {
	// validate default behaviour of network policy migration where policy which has CIDR set
	// will not be migrated
	validateNetworkPolicyMigration(t, false)
	logrus.Infof("Validating migration of all network policy with IncludeNetworkPolicyWithCIDR set to true")
	// validate behaviour of network policy migration where policy which has CIDR set
	// will be migrated using IncludeNetworkPolicyWithCIDR option in migration schedule
	validateNetworkPolicyMigration(t, true)
}
func validateNetworkPolicyMigration(t *testing.T, all bool) {
	var err error
	namespace := "networkpolicy-networkpolicy-migration-schedule-interval"
	scheduleName := "networkpolicy-migration-schedule-interval"
	if all {
		namespace = "networkpolicy-networkpolicy-all-migration-schedule-interval"
		scheduleName = "networkpolicy-all-migration-schedule-interval"
	}
	// Reset config in case of error
	defer func() {
		err = setSourceKubeConfig()
		require.NoError(t, err, "Error resetting remote config")
	}()
	err = setMockTime(nil)
	require.NoError(t, err, "Error resetting mock time")
	// the schedule interval for these specs it set to 5 minutes
	ctxs, preMigrationCtx := triggerMigration(
		t,
		scheduleName,
		"networkpolicy",
		[]string{},
		[]string{scheduleName},
		true,
		false,
		false,
		false,
		"",
		nil,
	)

	validateMigration(t, scheduleName, preMigrationCtx.GetID())

	networkPolicies, err := core.Instance().ListNetworkPolicy(namespace, meta_v1.ListOptions{})
	require.NoError(t, err, "error retriving network policy list from %s namespace", namespace)
	cnt := 0
	for _, networkPolicy := range networkPolicies.Items {
		collect := true
		ingressRule := networkPolicy.Spec.Ingress
		for _, ingress := range ingressRule {
			for _, fromPolicyPeer := range ingress.From {
				ipBlock := fromPolicyPeer.IPBlock
				if ipBlock != nil && len(ipBlock.CIDR) != 0 {
					collect = false
				}
			}
		}
		egreeRule := networkPolicy.Spec.Egress
		for _, egress := range egreeRule {
			for _, networkPolicyPeer := range egress.To {
				ipBlock := networkPolicyPeer.IPBlock
				if ipBlock != nil && len(ipBlock.CIDR) != 0 {
					collect = false
				}
			}
		}
		if collect {
			cnt++
		}
	}
	if all {
		cnt = len(networkPolicies.Items)
	}
	logrus.Infof("No. of network policies which does not have CIDR set: %d", cnt)

	// Change kubeconfig to destination
	err = setDestinationKubeConfig()
	require.NoError(t, err, "failed to set kubeconfig to source cluster: %v", err)

	destNetworkPolicies, err := core.Instance().ListNetworkPolicy(namespace, meta_v1.ListOptions{})
	require.NoError(t, err, "error retriving network policy list from %s namespace", namespace)

	if len(destNetworkPolicies.Items) != cnt {
		matchErr := fmt.Errorf("migrated network poilcy does not match")
		logrus.Infof("Policy found on source cluster")
		for _, networkPolicy := range networkPolicies.Items {
			logrus.Infof("Network Policy: %s/%s", networkPolicy.Namespace, networkPolicy.Name)
		}
		logrus.Infof("Policy found on DR cluster")
		for _, networkPolicy := range destNetworkPolicies.Items {
			logrus.Infof("Network Policy: %s/%s", networkPolicy.Namespace, networkPolicy.Name)
		}
		require.NoError(t, matchErr, "NetworkPolicy Expected: %v, Actual: %v", cnt, len(destNetworkPolicies.Items))
	}

	logrus.Infof("Successfully verified migrated network policies on destination cluster")

	err = setSourceKubeConfig()
	require.NoError(t, err, "failed to set kubeconfig to source cluster: %v", err)
	validateAndDestroyMigration(t, ctxs, preMigrationCtx, true, false, true, false, false)
}

func transformResourceTest(t *testing.T) {
	var err error
	// Reset config in case of error
	defer func() {
		err = setSourceKubeConfig()
		require.NoError(t, err, "Error resetting remote config")
	}()
	err = setMockTime(nil)
	require.NoError(t, err, "Error resetting mock time")
	// the schedule interval for these specs it set to 5 minutes
	ctxs, preMigrationCtx := triggerMigration(
		t,
		"mysql-migration-transform-interval",
		"mysql-1-pvc",
		[]string{"transform-service"},
		[]string{"mysql-migration-transform-interval"},
		true,
		false,
		false,
		false,
		"",
		nil,
	)
	namespace := "mysql-1-pvc-mysql-migration-transform-interval"
	validateMigration(t, "mysql-migration-transform-interval", preMigrationCtx.GetID())

	// expected service changes after transformation
	expectedServiceType := "LoadBalancer"
	expectedLabels := make(map[string]string)
	expectedLabels["handler"] = "project"
	expectedLabels["app"] = "mysql-2"

	// Change kubeconfig to destination
	err = setDestinationKubeConfig()
	require.NoError(t, err, "failed to set kubeconfig to source cluster: %v", err)

	// verify service resource migration on destination cluster
	// Verify if statefulset get delete
	svcs, err := core.Instance().ListServices(namespace, meta_v1.ListOptions{})
	require.NoError(t, err, "unable to query services in namespace : %s", namespace)

	for _, svc := range svcs.Items {
		if string(svc.Spec.Type) != expectedServiceType {
			matchErr := fmt.Errorf("transformed service type does not match")
			require.NoError(t, matchErr, "actual: %s, expected: %s", svc.Spec.Type, expectedServiceType)
		}
		labels := svc.GetLabels()
		for k, v := range expectedLabels {
			if val, ok := labels[k]; !ok || val != v {
				matchErr := fmt.Errorf("transformed service labels does not match")
				require.NoError(t, matchErr, "actual: %s, expected: %s", labels, expectedLabels)
			}
		}
	}
	err = setSourceKubeConfig()
	require.NoError(t, err, "failed to set kubeconfig to source cluster: %v", err)
	validateAndDestroyMigration(t, ctxs, preMigrationCtx, true, false, true, false, true)
}
