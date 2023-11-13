//go:build unittest
// +build unittest

package storkctl

import (
	"strconv"
	"strings"
	"testing"
	"time"

	storkv1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/portworx/sched-ops/k8s/core"
	storkops "github.com/portworx/sched-ops/k8s/stork"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
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
		cmdArgs = append(cmdArgs, "--pre-exec-rule", preExecRule)
	}
	if postExecRule != "" {
		cmdArgs = append(cmdArgs, "--post-exec-rule", postExecRule)
	}

	_, err := storkops.Instance().CreateSchedulePolicy(&storkv1.SchedulePolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name: schedulePolicyName,
		},
		Policy: storkv1.SchedulePolicyItem{
			Interval: &storkv1.IntervalPolicy{
				IntervalMinutes: 1,
			}},
	})
	require.True(t, err == nil || errors.IsAlreadyExists(err), "Error creating schedulepolicy")

	createClusterPair(t, clusterpair, namespace, "async-dr")

	expected := "MigrationSchedule " + name + " created successfully\n"
	testCommon(t, cmdArgs, nil, expected, false)

	// Make sure it was created correctly
	migration, err := storkops.Instance().GetMigrationSchedule(name, namespace)
	require.NoError(t, err, "Error getting migration schedule")
	require.Equal(t, name, migration.Name, "MigrationSchedule name mismatch")
	require.Equal(t, namespace, migration.Namespace, "MigrationSchedule namespace mismatch")
	require.Equal(t, clusterpair, migration.Spec.Template.Spec.ClusterPair, "MigrationSchedule clusterpair mismatch")
	require.Equal(t, namespaces, migration.Spec.Template.Spec.Namespaces, "MigrationSchedule namespace mismatch")
	require.Equal(t, preExecRule, migration.Spec.Template.Spec.PreExecRule, "MigrationSchedule preExecRule mismatch")
	require.Equal(t, postExecRule, migration.Spec.Template.Spec.PostExecRule, "MigrationSchedule postExecRule mismatch")
	require.Equal(t, true, *migration.Spec.Template.Spec.IncludeVolumes, "MigrationSchedule includeVolumes mismatch")
}

func TestGetMigrationSchedulesOneMigrationSchedule(t *testing.T) {
	defer resetTest()
	createMigrationScheduleAndVerify(t, "getmigrationscheduletest", "testpolicy", "test", "clusterpair1", []string{"test"}, "preExec", "postExec", true)

	expected := "NAME                       POLICYNAME   CLUSTERPAIR    SUSPEND   LAST-SUCCESS-TIME   LAST-SUCCESS-DURATION\n" +
		"getmigrationscheduletest   testpolicy   clusterpair1   true                          \n"

	cmdArgs := []string{"get", "migrationschedules", "-n", "test"}
	testCommon(t, cmdArgs, nil, expected, false)
}

func TestGetMigrationSchedulesMultiple(t *testing.T) {
	defer resetTest()
	createMigrationScheduleAndVerify(t, "getmigrationscheduletest1", "testpolicy", "default", "clusterpair1", []string{"default"}, "", "", true)
	createMigrationScheduleAndVerify(t, "getmigrationscheduletest2", "testpolicy", "default", "clusterpair2", []string{"default"}, "", "", true)

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
	_, err := core.Instance().CreateNamespace(&v1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "test1"}})
	require.NoError(t, err, "Error creating test1 namespace")
	_, err = core.Instance().CreateNamespace(&v1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "test2"}})
	require.NoError(t, err, "Error creating test2 namespace")

	createMigrationScheduleAndVerify(t, "getmigrationscheduletest1", "testpolicy", "test1", "clusterpair1", []string{"test1"}, "", "", true)
	createMigrationScheduleAndVerify(t, "getmigrationscheduletest2", "testpolicy", "test2", "clusterpair2", []string{"test2"}, "", "", true)

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
	createMigrationScheduleAndVerify(t, "getmigrationscheduletest1", "testpolicy", "default", "clusterpair1", []string{"default"}, "", "", true)
	createMigrationScheduleAndVerify(t, "getmigrationscheduletest2", "testpolicy", "default", "clusterpair2", []string{"default"}, "", "", true)

	expected := "NAME                        POLICYNAME   CLUSTERPAIR    SUSPEND   LAST-SUCCESS-TIME   LAST-SUCCESS-DURATION\n" +
		"getmigrationscheduletest1   testpolicy   clusterpair1   true                          \n"

	cmdArgs := []string{"get", "migrationschedules", "-c", "clusterpair1"}
	testCommon(t, cmdArgs, nil, expected, false)
}

func TestGetMigrationSchedulesWithStatus(t *testing.T) {
	defer resetTest()
	createMigrationScheduleAndVerify(t, "getmigrationschedulestatustest", "testpolicy", "default", "clusterpair1", []string{"default"}, "", "", true)
	migrationSchedule, err := storkops.Instance().GetMigrationSchedule("getmigrationschedulestatustest", "default")
	require.NoError(t, err, "Error getting migration schedule")

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
	migrationSchedule, err = storkops.Instance().UpdateMigrationSchedule(migrationSchedule)
	require.NoError(t, err, "Error updating migration schedule")

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
	migrationSchedule, err = storkops.Instance().UpdateMigrationSchedule(migrationSchedule)
	require.NoError(t, err, "Error updating migration schedule")

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
	_, err = storkops.Instance().UpdateMigrationSchedule(migrationSchedule)
	require.NoError(t, err, "Error updating migration schedule")

	expected = "NAME                             POLICYNAME   CLUSTERPAIR    SUSPEND   LAST-SUCCESS-TIME     LAST-SUCCESS-DURATION\n" +
		"getmigrationschedulestatustest   testpolicy   clusterpair1   true      " + toTimeString(finishTimestamp.Time) + "   5m0s\n"
	cmdArgs = []string{"get", "migrationschedules", "getmigrationschedulestatustest"}
	testCommon(t, cmdArgs, nil, expected, false)
}

func TestCreateMigrationSchedulesNoNamespace(t *testing.T) {
	defer resetTest()
	clusterPairName := "clusterPair1"
	createClusterPair(t, clusterPairName, "default", "async-dr")
	cmdArgs := []string{"create", "migrationschedules", "-c", clusterPairName, "migration1"}

	expected := "error: no valid namespace found based on the provided --namespaces and --namespace-selectors"
	testCommon(t, cmdArgs, nil, expected, true)
}

func TestCreateMigrationSchedulesNoClusterPair(t *testing.T) {
	defer resetTest()
	cmdArgs := []string{"create", "migrationschedules", "migration1"}

	expected := "error: ClusterPair name needs to be provided for migration schedule"
	testCommon(t, cmdArgs, nil, expected, true)
}

func TestCreateMigrationSchedulesInvalidClusterPair(t *testing.T) {
	defer resetTest()
	clusterPairName := "clusterPair1"
	cmdArgs := []string{"create", "migrationschedule", "-c", clusterPairName, "migration1"}

	expected := "error: unable to find the cluster pair in the given namespace"
	testCommon(t, cmdArgs, nil, expected, true)
}

func TestCreateMigrationSchedulesInvalidAdminClusterPair(t *testing.T) {
	defer resetTest()
	clusterPair := "clusterpair1"
	namespace := "namespace1"
	name := "createmigrationschedule"
	createClusterPair(t, clusterPair, "namespace1", "sync-dr")
	cmdArgs := []string{"create", "migrationschedules", "-c", clusterPair, "--admin-cluster-pair", "adminclusterpair",
		"--namespaces", namespace, name}
	expected := "error: unable to find the admin cluster pair in the admin namespace"
	testCommon(t, cmdArgs, nil, expected, true)
}

func TestCreateMigrationSchedulesNoName(t *testing.T) {
	defer resetTest()
	cmdArgs := []string{"create", "migrationschedules"}

	expected := "error: exactly one name needs to be provided for migration schedule name"
	testCommon(t, cmdArgs, nil, expected, true)
}

func TestCreateMigrationSchedules(t *testing.T) {
	defer resetTest()
	createMigrationScheduleAndVerify(t, "createmigration", "testpolicy", "default", "clusterpair1", []string{"default"}, "", "", true)
}

func TestCreateMigrationScheduleWithNamespaceSelector(t *testing.T) {
	defer resetTest()
	clusterPair := "clusterpair1"
	namespace := "test-ns"
	_, err := core.Instance().CreateNamespace(&v1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name:   namespace,
			Labels: map[string]string{"key": "value"},
		}})
	require.NoError(t, err, "Error creating test-ns namespace")
	createClusterPair(t, clusterPair, "test-ns", "async-dr")
	cmdArgs := []string{"create", "migrationschedules", "-c", clusterPair, "-i", "15",
		"--namespace-selectors", "key=value", "-n", namespace, "ms"}
	expected := "MigrationSchedule ms created successfully\n"
	testCommon(t, cmdArgs, nil, expected, false)
}

func TestCreateMigrationScheduleSyncDrExcludeVolumesFalse(t *testing.T) {
	defer resetTest()
	clusterPair := "clusterpair1"
	namespace := "namespace1"
	name := "createmigrationschedule"
	createClusterPair(t, clusterPair, "namespace1", "sync-dr")
	cmdArgs := []string{"create", "migrationschedules", "-i", "15", "-c", clusterPair,
		"--namespaces", namespace, "--annotations", "key1=value1", name, "-n", namespace, "--exclude-volumes=" + strconv.FormatBool(false)}
	expected := "error: --exclude-volumes can only be set to true if it is a sync-dr use case or storage options are not provided in the cluster pair"
	testCommon(t, cmdArgs, nil, expected, true)
}

func TestCreateMigrationScheduleWithIntervalAndVerify(t *testing.T) {
	defer resetTest()
	clusterPair := "clusterpair1"
	adminClusterPair := "adminClusterPair"
	namespace := "namespace1"
	name := "createmigrationschedule"
	createClusterPair(t, clusterPair, "namespace1", "sync-dr")
	//create admin cluster pair in default admin namespace
	createClusterPair(t, adminClusterPair, "kube-system", "async-dr")
	cmdArgs := []string{"create", "migrationschedules", "-i", "15", "-c", clusterPair, "--admin-cluster-pair", adminClusterPair,
		"--namespaces", namespace, "--annotations", "key1=value1", name, "-n", namespace}
	expected := "MigrationSchedule createmigrationschedule created successfully\n"
	testCommon(t, cmdArgs, nil, expected, false)

	// Make sure it was created correctly
	migration, err := storkops.Instance().GetMigrationSchedule(name, namespace)
	require.NoError(t, err, "Error getting migration schedule")
	schedulePolicy, err := storkops.Instance().GetSchedulePolicy(name)
	require.NoError(t, err, "Error getting schedule policy")
	require.Equal(t, name, migration.Name, "MigrationSchedule name mismatch")
	require.Equal(t, namespace, migration.Namespace, "MigrationSchedule namespace mismatch")
	require.Equal(t, clusterPair, migration.Spec.Template.Spec.ClusterPair, "MigrationSchedule clusterpair mismatch")
	require.Equal(t, []string{namespace}, migration.Spec.Template.Spec.Namespaces, "MigrationSchedule namespace mismatch")
	//verifying includeVolumes default for syncDR usecase is false
	require.Equal(t, false, *migration.Spec.Template.Spec.IncludeVolumes, "MigrationSchedule includeVolumes mismatch")
	require.Equal(t, 15, schedulePolicy.Policy.Interval.IntervalMinutes, "MigrationSchedule schedulePolicy interval mismatch")
	require.Equal(t, map[string]string{"key1": "value1"}, migration.Annotations, "MigrationSchedule annotations mismatch")
	require.Equal(t, true, migration.Spec.AutoSuspend, "MigrationSchedule autoSuspend mismatch")
}

func TestCreateMigrationScheduleWithBothIntervalAndPolicyName(t *testing.T) {
	defer resetTest()
	createClusterPair(t, "clusterPair1", "namespace1", "async-dr")
	cmdArgs := []string{"create", "migrationschedules", "-i", "15", "-s", "test-policy", "-c", "clusterPair1",
		"--namespaces", "namespace1", "migrationschedule", "-n", "namespace1"}
	expected := "error: must provide only one of schedule-policy-name or interval values"
	testCommon(t, cmdArgs, nil, expected, true)
}

func TestCreateMigrationScheduleWithInvalidNamespaces(t *testing.T) {
	defer resetTest()
	namespace := "test-ns"
	_, err := core.Instance().CreateNamespace(&v1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name:   namespace,
			Labels: map[string]string{"key": "value"},
		}})
	require.NoError(t, err, "Error creating test-ns namespace")
	createClusterPair(t, "clusterPair1", "namespace1", "async-dr")
	cmdArgs := []string{"create", "migrationschedules", "-i", "15", "-c", "clusterPair1",
		"--namespaces", "namespace1", "--namespace-selectors", "key=value", "migrationschedule", "-n", "namespace1"}
	expected := "error: migration namespaces should only contain the current namespace"
	testCommon(t, cmdArgs, nil, expected, true)
}

func TestCreateMigrationScheduleInAdminNamespace(t *testing.T) {
	defer resetTest()
	namespace := "test-ns"
	adminNamespace := "kube-system"
	_, err := core.Instance().CreateNamespace(&v1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name:   namespace,
			Labels: map[string]string{"key": "value"},
		}})
	require.NoError(t, err, "Error creating test-ns namespace")
	createClusterPair(t, "clusterPair1", adminNamespace, "async-dr")
	cmdArgs := []string{"create", "migrationschedules", "-i", "15", "-c", "clusterPair1",
		"--namespaces", "namespace1", "--namespace-selectors", "key=value", "migrationschedule", "-n", adminNamespace}
	expected := "MigrationSchedule migrationschedule created successfully\n"
	testCommon(t, cmdArgs, nil, expected, false)
}

func TestCreateMigrationScheduleWithMissingTransformSpec(t *testing.T) {
	defer resetTest()
	createClusterPair(t, "clusterPair1", "namespace1", "async-dr")
	cmdArgs := []string{"create", "migrationschedule", "-i", "15", "-c", "clusterPair1",
		"--namespaces", "namespace1", "migrationschedule", "-n", "namespace1", "--transform-spec", "test-rt"}
	expected := "error: unable to retrieve transformation namespace1/test-rt, err: resourcetransformations.stork.libopenstorage.org \"test-rt\" not found"
	testCommon(t, cmdArgs, nil, expected, true)
}

func TestCreateMigrationScheduleWithInvalidTransformSpec(t *testing.T) {
	defer resetTest()
	createClusterPair(t, "clusterPair1", "default", "async-dr")
	cmdArgs := []string{"create", "migrationschedule", "-i", "15", "-c", "clusterPair1",
		"--namespaces", "default", "migrationschedule", "--transform-spec", "test-rt", "-n", "default"}
	createResourceTransformation(t, "test-rt", "default", storkv1.ResourceTransformationStatusFailed)
	expected := "error: transformation default/test-rt is not in ready state, state: Failed"
	testCommon(t, cmdArgs, nil, expected, true)
}

func TestCreateMigrationScheduleWithValidTransformSpec(t *testing.T) {
	defer resetTest()
	createClusterPair(t, "clusterPair1", "default", "async-dr")
	cmdArgs := []string{"create", "migrationschedule", "-i", "15", "-c", "clusterPair1",
		"--namespaces", "default", "migrationschedule", "--transform-spec", "test-rt", "-n", "default"}
	createResourceTransformation(t, "test-rt", "default", storkv1.ResourceTransformationStatusReady)
	expected := "MigrationSchedule migrationschedule created successfully\n"
	testCommon(t, cmdArgs, nil, expected, false)
}

func TestCreateMigrationScheduleWithInvalidInterval(t *testing.T) {
	defer resetTest()
	createClusterPair(t, "clusterPair1", "namespace1", "async-dr")
	cmdArgs := []string{"create", "migrationschedules", "-i", "-15", "-c", "clusterPair1",
		"--namespaces", "namespace1", "migrationschedule", "-n", "namespace1"}
	expected := "error: could not create a schedule policy with specified interval: Invalid intervalMinutes (-15) in Interval policy"
	testCommon(t, cmdArgs, nil, expected, true)
}

func TestCreateDuplicateMigrationSchedules(t *testing.T) {
	defer resetTest()
	createMigrationScheduleAndVerify(t, "createmigrationschedule", "testpolicy", "default", "clusterpair1", []string{"default"}, "", "", true)
	cmdArgs := []string{"create", "migrationschedules", "-s", "testpolicy", "-c", "clusterpair1", "--namespaces", "default", "createmigrationschedule"}

	expected := "Error from server (AlreadyExists): migrationschedules.stork.libopenstorage.org \"createmigrationschedule\" already exists"
	testCommon(t, cmdArgs, nil, expected, true)
}

func TestDefaultMigrationSchedulePolicy(t *testing.T) {
	defer resetTest()
	// Create schedule without the default policy present
	createClusterPair(t, "clusterpair1", "test", "async-dr")
	cmdArgs := []string{"create", "migrationschedules", "defaultmigrationschedule", "-c", "clusterpair1", "--namespaces", "test", "-n", "test"}
	expected := "error: unable to get schedulepolicy default-migration-policy: schedulepolicies.stork.libopenstorage.org \"default-migration-policy\" not found"
	testCommon(t, cmdArgs, nil, expected, true)

	// Create again adding default policy
	_, err := storkops.Instance().CreateSchedulePolicy(&storkv1.SchedulePolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name: "default-migration-policy",
		},
		Policy: storkv1.SchedulePolicyItem{
			Interval: &storkv1.IntervalPolicy{
				IntervalMinutes: 1,
			}},
	})
	require.NoError(t, err, "Error creating schedulepolicy")
	expected = "MigrationSchedule defaultmigrationschedule created successfully\n"
	testCommon(t, cmdArgs, nil, expected, false)
}
func TestDeleteMigrationSchedulesNoMigrationName(t *testing.T) {
	cmdArgs := []string{"delete", "migrationschedules"}

	var migrationList storkv1.MigrationList
	expected := "error: at least one argument needs to be provided for migration schedule name if cluster pair isn't provided"
	testCommon(t, cmdArgs, &migrationList, expected, true)
}

func TestDeleteMigrationSchedulesNoMigration(t *testing.T) {
	cmdArgs := []string{"delete", "migrationschedules", "-c", "migration1"}

	expected := "No resources found.\n"
	testCommon(t, cmdArgs, nil, expected, false)
}

func TestDeleteMigrationSchedules(t *testing.T) {
	defer resetTest()
	createMigrationScheduleAndVerify(t, "deletemigration", "testpolicy", "default", "clusterpair1", []string{"default"}, "", "", false)

	cmdArgs := []string{"delete", "migrationschedules", "deletemigration"}
	expected := "MigrationSchedule deletemigration deleted successfully\n"
	testCommon(t, cmdArgs, nil, expected, false)

	cmdArgs = []string{"delete", "migrationschedules", "deletemigration"}
	expected = "Error from server (NotFound): migrationschedules.stork.libopenstorage.org \"deletemigration\" not found"
	testCommon(t, cmdArgs, nil, expected, true)

	createMigrationScheduleAndVerify(t, "deletemigration1", "testpolicy", "default", "clusterpair1", []string{"default"}, "", "", true)
	createMigrationScheduleAndVerify(t, "deletemigration2", "testpolicy", "default", "clusterpair2", []string{"default"}, "", "", true)

	cmdArgs = []string{"delete", "migrationschedules", "deletemigration1", "deletemigration2"}
	expected = "MigrationSchedule deletemigration1 deleted successfully\n"
	expected += "MigrationSchedule deletemigration2 deleted successfully\n"
	testCommon(t, cmdArgs, nil, expected, false)

	createMigrationScheduleAndVerify(t, "deletemigration1", "testpolicy", "default", "clusterpair1", []string{"default"}, "", "", true)
	createMigrationScheduleAndVerify(t, "deletemigration2", "testpolicy", "default", "clusterpair1", []string{"default"}, "", "", true)

	cmdArgs = []string{"delete", "migrationschedules", "-c", "clusterpair1"}
	expected = "MigrationSchedule deletemigration1 deleted successfully\n"
	expected += "MigrationSchedule deletemigration2 deleted successfully\n"
	testCommon(t, cmdArgs, nil, expected, false)
}

func TestSuspendResumeMigrationSchedule(t *testing.T) {
	name := "testmigrationschedule"
	name1 := "testmigrationschedule-2"
	namespace := "default"
	defer resetTest()
	createMigrationScheduleAndVerify(t, name, "testpolicy", namespace, "clusterpair1", []string{namespace}, "", "", false)

	cmdArgs := []string{"suspend", "migrationschedules", name}
	expected := "MigrationSchedule " + name + " suspended successfully\n"
	testCommon(t, cmdArgs, nil, expected, false)

	migrationSchedule, err := storkops.Instance().GetMigrationSchedule(name, namespace)
	require.NoError(t, err, "Error getting migrationschedule")
	require.True(t, *migrationSchedule.Spec.Suspend, "migration schedule not suspended")

	cmdArgs = []string{"resume", "migrationschedules", name}
	expected = "MigrationSchedule " + name + " resumed successfully\n"
	testCommon(t, cmdArgs, nil, expected, false)

	migrationSchedule, err = storkops.Instance().GetMigrationSchedule(name, namespace)
	require.NoError(t, err, "Error getting migrationschedule")
	require.False(t, *migrationSchedule.Spec.Suspend, "migration schedule suspended")

	cmdArgs = []string{"suspend", "migrationschedules", "-c", "clusterpair1"}
	expected = "MigrationSchedule " + name + " suspended successfully\n"
	testCommon(t, cmdArgs, nil, expected, false)

	migrationSchedule, err = storkops.Instance().GetMigrationSchedule(name, namespace)
	require.NoError(t, err, "Error getting migrationschedule")
	require.True(t, *migrationSchedule.Spec.Suspend, "migration schedule not suspended")

	cmdArgs = []string{"resume", "migrationschedules", "-c", "clusterpair1"}
	expected = "MigrationSchedule " + name + " resumed successfully\n"
	testCommon(t, cmdArgs, nil, expected, false)

	migrationSchedule, err = storkops.Instance().GetMigrationSchedule(name, namespace)
	require.NoError(t, err, "Error getting migrationschedule")
	require.False(t, *migrationSchedule.Spec.Suspend, "migration schedule suspended")

	cmdArgs = []string{"suspend", "migrationschedules", "invalidschedule"}
	expected = "Error from server (NotFound): migrationschedules.stork.libopenstorage.org \"invalidschedule\" not found"
	testCommon(t, cmdArgs, nil, expected, true)

	cmdArgs = []string{"resume", "migrationschedules", "invalidschedule"}
	testCommon(t, cmdArgs, nil, expected, true)

	// test multiple suspend/resume using same clusterpair
	createMigrationScheduleAndVerify(t, name1, "testpolicy", namespace, "clusterpair1", []string{namespace}, "", "", false)
	cmdArgs = []string{"suspend", "migrationschedules", "-c", "clusterpair1"}
	expected = "MigrationSchedule " + name + " suspended successfully\nMigrationSchedule " + name1 + " suspended successfully\n"
	testCommon(t, cmdArgs, nil, expected, false)

	cmdArgs = []string{"resume", "migrationschedules", "-c", "clusterpair1"}
	expected = "MigrationSchedule " + name + " resumed successfully\nMigrationSchedule " + name1 + " resumed successfully\n"
	testCommon(t, cmdArgs, nil, expected, false)

}

func createClusterPair(t *testing.T, clusterPairName string, namespace string, mode string) {
	options := make(map[string]string)
	storageStatus := storkv1.ClusterPairStatusNotProvided
	if mode == "async-dr" {
		options["backuplocation"] = "value1"
		options["ip"] = "value2"
		options["port"] = "value3"
		options["token"] = "value4"
		storageStatus = storkv1.ClusterPairStatusReady
	}
	clusterPair := &storkv1.ClusterPair{
		ObjectMeta: metav1.ObjectMeta{
			Name:      clusterPairName,
			Namespace: namespace,
		},

		Spec: storkv1.ClusterPairSpec{
			Options: options,
		},

		Status: storkv1.ClusterPairStatus{
			SchedulerStatus: storkv1.ClusterPairStatusReady,
			StorageStatus:   storageStatus,
		},
	}
	_, err := storkops.Instance().CreateClusterPair(clusterPair)
	require.True(t, err == nil || errors.IsAlreadyExists(err), "Error creating cluster pair")
}

func createResourceTransformation(t *testing.T, resourceTransformName string, namespace string, status storkv1.ResourceTransformationStatusType) {
	_, err := storkops.Instance().CreateResourceTransformation(&storkv1.ResourceTransformation{
		ObjectMeta: metav1.ObjectMeta{
			Name:      resourceTransformName,
			Namespace: namespace,
		},
		Spec: storkv1.ResourceTransformationSpec{
			Objects: []storkv1.TransformSpecs{
				{
					Resource: "/v1/Service",
					Paths: []storkv1.ResourcePaths{
						{
							Path:  "spec.type",
							Value: "LoadBalancer",
							Type:  "string",
						},
					},
				}},
		},
		Status: storkv1.ResourceTransformationStatus{
			Status: status,
		},
	})
	require.NoError(t, err, "Error creating Resource Transformation")
}
