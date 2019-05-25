// +build unittest

package storkctl

import (
	"strconv"
	"strings"
	"testing"
	"time"

	storkv1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/portworx/sched-ops/k8s"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestGetMigrationSchedulesNoMigrationSchedule(t *testing.T) {
	cmdArgs := []string{"get", "migrationschedules"}

	expected := "No resources found.\n"
	testCommon(t, cmdArgs, nil, expected, false)
}

func createMigrationScheduleAndVerify(
	t *testing.T,
	name string,
	schedulePolicyName string,
	namespace string,
	clusterpair string,
	namespaces []string,
	preExecRule string,
	postExecRule string,
	suspend bool,
) {
	cmdArgs := []string{"create", "migrationschedules", "-s", schedulePolicyName, "-n", namespace, "-c", clusterpair, "--namespaces", strings.Join(namespaces, ","), name, "--suspend=" + strconv.FormatBool(suspend)}
	if preExecRule != "" {
		cmdArgs = append(cmdArgs, "--preExecRule", preExecRule)
	}
	if postExecRule != "" {
		cmdArgs = append(cmdArgs, "--postExecRule", postExecRule)
	}

	_, err := k8s.Instance().CreateSchedulePolicy(&storkv1.SchedulePolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name: schedulePolicyName,
		},
		Policy: storkv1.SchedulePolicyItem{
			Interval: &storkv1.IntervalPolicy{
				IntervalMinutes: 1,
			}},
	})
	require.True(t, err == nil || errors.IsAlreadyExists(err), "Error creating schedulepolicy")

	expected := "MigrationSchedule " + name + " created successfully\n"
	testCommon(t, cmdArgs, nil, expected, false)

	// Make sure it was created correctly
	migration, err := k8s.Instance().GetMigrationSchedule(name, namespace)
	require.NoError(t, err, "Error getting migration schedule")
	require.Equal(t, name, migration.Name, "MigrationSchedule name mismatch")
	require.Equal(t, namespace, migration.Namespace, "MigrationSchedule namespace mismatch")
	require.Equal(t, clusterpair, migration.Spec.Template.Spec.ClusterPair, "MigrationSchedule clusterpair mismatch")
	require.Equal(t, namespaces, migration.Spec.Template.Spec.Namespaces, "MigrationSchedule namespace mismatch")
	require.Equal(t, preExecRule, migration.Spec.Template.Spec.PreExecRule, "MigrationSchedule preExecRule mismatch")
	require.Equal(t, postExecRule, migration.Spec.Template.Spec.PostExecRule, "MigrationSchedule postExecRule mismatch")
}

func TestGetMigrationSchedulesOneMigrationSchedule(t *testing.T) {
	defer resetTest()
	createMigrationScheduleAndVerify(t, "getmigrationscheduletest", "testpolicy", "test", "clusterpair1", []string{"namespace1"}, "preExec", "postExec", true)

	expected := "NAME                       POLICYNAME   CLUSTERPAIR    SUSPEND   LAST-SUCCESS-TIME   LAST-SUCCESS-DURATION\n" +
		"getmigrationscheduletest   testpolicy   clusterpair1   true                          \n"

	cmdArgs := []string{"get", "migrationschedules", "-n", "test"}
	testCommon(t, cmdArgs, nil, expected, false)
}

func TestGetMigrationSchedulesMultiple(t *testing.T) {
	defer resetTest()
	createMigrationScheduleAndVerify(t, "getmigrationscheduletest1", "testpolicy", "default", "clusterpair1", []string{"namespace1"}, "", "", true)
	createMigrationScheduleAndVerify(t, "getmigrationscheduletest2", "testpolicy", "default", "clusterpair2", []string{"namespace1"}, "", "", true)

	expected := "NAME                        POLICYNAME   CLUSTERPAIR    SUSPEND   LAST-SUCCESS-TIME   LAST-SUCCESS-DURATION\n" +
		"getmigrationscheduletest1   testpolicy   clusterpair1   true                          \n" +
		"getmigrationscheduletest2   testpolicy   clusterpair2   true                          \n"

	cmdArgs := []string{"get", "migrationschedules", "getmigrationscheduletest1", "getmigrationscheduletest2"}
	testCommon(t, cmdArgs, nil, expected, false)

	// Should get all migrationschedules if no name given
	cmdArgs = []string{"get", "migrationschedules"}
	testCommon(t, cmdArgs, nil, expected, false)

	expected = "NAME                        POLICYNAME   CLUSTERPAIR    SUSPEND   LAST-SUCCESS-TIME   LAST-SUCCESS-DURATION\n" +
		"getmigrationscheduletest1   testpolicy   clusterpair1   true                          \n"
	// Should get only one migration if name given
	cmdArgs = []string{"get", "migrationschedules", "getmigrationscheduletest1"}
	testCommon(t, cmdArgs, nil, expected, false)
}

func TestGetMigrationSchedulesMultipleNamespaces(t *testing.T) {
	defer resetTest()
	_, err := k8s.Instance().CreateNamespace("test1", nil)
	require.NoError(t, err, "Error creating test1 namespace")
	_, err = k8s.Instance().CreateNamespace("test2", nil)
	require.NoError(t, err, "Error creating test2 namespace")

	createMigrationScheduleAndVerify(t, "getmigrationscheduletest1", "testpolicy", "test1", "clusterpair1", []string{"namespace1"}, "", "", true)
	createMigrationScheduleAndVerify(t, "getmigrationscheduletest2", "testpolicy", "test2", "clusterpair2", []string{"namespace1"}, "", "", true)

	expected := "NAME                        POLICYNAME   CLUSTERPAIR    SUSPEND   LAST-SUCCESS-TIME   LAST-SUCCESS-DURATION\n" +
		"getmigrationscheduletest1   testpolicy   clusterpair1   true                          \n"

	cmdArgs := []string{"get", "migrationschedules", "-n", "test1"}
	testCommon(t, cmdArgs, nil, expected, false)

	// Should get all migrationschedules
	cmdArgs = []string{"get", "migrationschedules", "--all-namespaces"}
	expected = "NAMESPACE   NAME                        POLICYNAME   CLUSTERPAIR    SUSPEND   LAST-SUCCESS-TIME   LAST-SUCCESS-DURATION\n" +
		"test1       getmigrationscheduletest1   testpolicy   clusterpair1   true                          \n" +
		"test2       getmigrationscheduletest2   testpolicy   clusterpair2   true                          \n"
	testCommon(t, cmdArgs, nil, expected, false)
}

func TestGetMigrationSchedulesWithClusterPair(t *testing.T) {
	defer resetTest()
	createMigrationScheduleAndVerify(t, "getmigrationscheduletest1", "testpolicy", "default", "clusterpair1", []string{"namespace1"}, "", "", true)
	createMigrationScheduleAndVerify(t, "getmigrationscheduletest2", "testpolicy", "default", "clusterpair2", []string{"namespace1"}, "", "", true)

	expected := "NAME                        POLICYNAME   CLUSTERPAIR    SUSPEND   LAST-SUCCESS-TIME   LAST-SUCCESS-DURATION\n" +
		"getmigrationscheduletest1   testpolicy   clusterpair1   true                          \n"

	cmdArgs := []string{"get", "migrationschedules", "-c", "clusterpair1"}
	testCommon(t, cmdArgs, nil, expected, false)
}

func TestGetMigrationSchedulesWithStatus(t *testing.T) {
	defer resetTest()
	createMigrationScheduleAndVerify(t, "getmigrationschedulestatustest", "testpolicy", "default", "clusterpair1", []string{"namespace1"}, "", "", true)
	migrationSchedule, err := k8s.Instance().GetMigrationSchedule("getmigrationschedulestatustest", "default")
	require.NoError(t, err, "Error getting migration")

	// Update the status of the daily migration
	migrationSchedule.Status.Items = make(map[storkv1.SchedulePolicyType][]*storkv1.ScheduledMigrationStatus)
	migrationSchedule.Status.Items[storkv1.SchedulePolicyTypeDaily] = make([]*storkv1.ScheduledMigrationStatus, 0)
	now := metav1.Now()
	finishTimestamp := metav1.NewTime(now.Add(5 * time.Minute))
	migrationSchedule.Status.Items[storkv1.SchedulePolicyTypeDaily] = append(migrationSchedule.Status.Items[storkv1.SchedulePolicyTypeDaily],
		&storkv1.ScheduledMigrationStatus{
			Name:              "dailymigration",
			CreationTimestamp: now,
			FinishTimestamp:   finishTimestamp,
			Status:            storkv1.MigrationStatusSuccessful,
		},
	)
	migrationSchedule, err = k8s.Instance().UpdateMigrationSchedule(migrationSchedule)

	expected := "NAME                             POLICYNAME   CLUSTERPAIR    SUSPEND   LAST-SUCCESS-TIME     LAST-SUCCESS-DURATION\n" +
		"getmigrationschedulestatustest   testpolicy   clusterpair1   true      " + toTimeString(finishTimestamp.Time) + "   5m0s\n"
	cmdArgs := []string{"get", "migrationschedules", "getmigrationschedulestatustest"}
	testCommon(t, cmdArgs, nil, expected, false)

	now = metav1.Now()
	finishTimestamp = metav1.NewTime(now.Add(5 * time.Minute))
	migrationSchedule.Status.Items[storkv1.SchedulePolicyTypeWeekly] = append(migrationSchedule.Status.Items[storkv1.SchedulePolicyTypeWeekly],
		&storkv1.ScheduledMigrationStatus{
			Name:              "weeklymigration",
			CreationTimestamp: now,
			FinishTimestamp:   finishTimestamp,
			Status:            storkv1.MigrationStatusSuccessful,
		},
	)
	migrationSchedule, err = k8s.Instance().UpdateMigrationSchedule(migrationSchedule)

	expected = "NAME                             POLICYNAME   CLUSTERPAIR    SUSPEND   LAST-SUCCESS-TIME     LAST-SUCCESS-DURATION\n" +
		"getmigrationschedulestatustest   testpolicy   clusterpair1   true      " + toTimeString(finishTimestamp.Time) + "   5m0s\n"
	cmdArgs = []string{"get", "migrationschedules", "getmigrationschedulestatustest"}
	testCommon(t, cmdArgs, nil, expected, false)

	now = metav1.Now()
	finishTimestamp = metav1.NewTime(now.Add(5 * time.Minute))
	migrationSchedule.Status.Items[storkv1.SchedulePolicyTypeMonthly] = append(migrationSchedule.Status.Items[storkv1.SchedulePolicyTypeMonthly],
		&storkv1.ScheduledMigrationStatus{
			Name:              "monthlymigration",
			CreationTimestamp: now,
			FinishTimestamp:   finishTimestamp,
			Status:            storkv1.MigrationStatusSuccessful,
		},
	)
	migrationSchedule, err = k8s.Instance().UpdateMigrationSchedule(migrationSchedule)

	expected = "NAME                             POLICYNAME   CLUSTERPAIR    SUSPEND   LAST-SUCCESS-TIME     LAST-SUCCESS-DURATION\n" +
		"getmigrationschedulestatustest   testpolicy   clusterpair1   true      " + toTimeString(finishTimestamp.Time) + "   5m0s\n"
	cmdArgs = []string{"get", "migrationschedules", "getmigrationschedulestatustest"}
	testCommon(t, cmdArgs, nil, expected, false)
}

func TestCreateMigrationSchedulesNoNamespace(t *testing.T) {
	cmdArgs := []string{"create", "migrationschedules", "-c", "clusterPair1", "migration1"}

	expected := "error: Need to provide atleast one namespace to migrate"
	testCommon(t, cmdArgs, nil, expected, true)
}

func TestCreateMigrationSchedulesNoClusterPair(t *testing.T) {
	cmdArgs := []string{"create", "migrationschedules", "migration1"}

	expected := "error: ClusterPair name needs to be provided for migration schedule"
	testCommon(t, cmdArgs, nil, expected, true)
}

func TestCreateMigrationSchedulesNoName(t *testing.T) {
	cmdArgs := []string{"create", "migrationschedules"}

	expected := "error: Exactly one name needs to be provided for migration schedule name"
	testCommon(t, cmdArgs, nil, expected, true)
}

func TestCreateMigrationSchedules(t *testing.T) {
	defer resetTest()
	createMigrationScheduleAndVerify(t, "createmigration", "testpolicy", "default", "clusterpair1", []string{"namespace1"}, "", "", true)
}

func TestCreateDuplicateMigrationSchedules(t *testing.T) {
	defer resetTest()
	createMigrationScheduleAndVerify(t, "createmigrationschedule", "testpolicy", "default", "clusterpair1", []string{"namespace1"}, "", "", true)
	cmdArgs := []string{"create", "migrationschedules", "-s", "testpolicy", "-c", "clusterpair1", "--namespaces", "namespace1", "createmigrationschedule"}

	expected := "Error from server (AlreadyExists): migrationschedules.stork.libopenstorage.org \"createmigrationschedule\" already exists"
	testCommon(t, cmdArgs, nil, expected, true)
}

func TestDeleteMigrationSchedulesNoMigrationName(t *testing.T) {
	cmdArgs := []string{"delete", "migrationschedules"}

	var migrationList storkv1.MigrationList
	expected := "error: At least one argument needs to be provided for migration schedule name if cluster pair isn't provided"
	testCommon(t, cmdArgs, &migrationList, expected, true)
}

func TestDeleteMigrationSchedulesNoMigration(t *testing.T) {
	cmdArgs := []string{"delete", "migrationschedules", "-c", "migration1"}

	expected := "No resources found.\n"
	testCommon(t, cmdArgs, nil, expected, false)
}

func TestDeleteMigrationSchedules(t *testing.T) {
	defer resetTest()
	createMigrationScheduleAndVerify(t, "deletemigration", "testpolicy", "default", "clusterpair1", []string{"namespace1"}, "", "", false)

	cmdArgs := []string{"delete", "migrationschedules", "deletemigration"}
	expected := "MigrationSchedule deletemigration deleted successfully\n"
	testCommon(t, cmdArgs, nil, expected, false)

	cmdArgs = []string{"delete", "migrationschedules", "deletemigration"}
	expected = "Error from server (NotFound): migrationschedules.stork.libopenstorage.org \"deletemigration\" not found"
	testCommon(t, cmdArgs, nil, expected, true)

	createMigrationScheduleAndVerify(t, "deletemigration1", "testpolicy", "default", "clusterpair1", []string{"namespace1"}, "", "", true)
	createMigrationScheduleAndVerify(t, "deletemigration2", "testpolicy", "default", "clusterpair2", []string{"namespace1"}, "", "", true)

	cmdArgs = []string{"delete", "migrationschedules", "deletemigration1", "deletemigration2"}
	expected = "MigrationSchedule deletemigration1 deleted successfully\n"
	expected += "MigrationSchedule deletemigration2 deleted successfully\n"
	testCommon(t, cmdArgs, nil, expected, false)

	createMigrationScheduleAndVerify(t, "deletemigration1", "testpolicy", "default", "clusterpair1", []string{"namespace1"}, "", "", true)
	createMigrationScheduleAndVerify(t, "deletemigration2", "testpolicy", "default", "clusterpair1", []string{"namespace1"}, "", "", true)

	cmdArgs = []string{"delete", "migrationschedules", "-c", "clusterpair1"}
	expected = "MigrationSchedule deletemigration1 deleted successfully\n"
	expected += "MigrationSchedule deletemigration2 deleted successfully\n"
	testCommon(t, cmdArgs, nil, expected, false)
}

func TestDefaultMigrationSchedulePolicy(t *testing.T) {
	defer resetTest()
	createMigrationScheduleAndVerify(t, "deletemigration", "testpolicy", "default", "clusterpair1", []string{"namespace1"}, "", "", false)

	// Create schedule without the default policy present
	cmdArgs := []string{"create", "migrationschedules", "defaultpolicy", "-n", "test", "-c", "clusterpair", "--namespaces", "test"}
	expected := "error: error getting schedulepolicy default-migration-policy: schedulepolicies.stork.libopenstorage.org \"default-migration-policy\" not found"
	testCommon(t, cmdArgs, nil, expected, true)

	// Create again adding default policy
	_, err := k8s.Instance().CreateSchedulePolicy(&storkv1.SchedulePolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name: "default-migration-policy",
		},
		Policy: storkv1.SchedulePolicyItem{
			Interval: &storkv1.IntervalPolicy{
				IntervalMinutes: 1,
			}},
	})
	require.NoError(t, err, "Error creating schedulepolicy")
	expected = "MigrationSchedule defaultpolicy created successfully\n"
	testCommon(t, cmdArgs, nil, expected, false)
}
