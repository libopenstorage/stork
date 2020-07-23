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

func TestGetApplicationBackupSchedulesNoApplicationBackupSchedule(t *testing.T) {
	cmdArgs := []string{"get", "applicationbackupschedules"}

	expected := "No resources found.\n"
	testCommon(t, cmdArgs, nil, expected, false)
}

func createApplicationBackupScheduleAndVerify(
	t *testing.T,
	name string,
	schedulePolicyName string,
	namespace string,
	backupLocation string,
	namespaces []string,
	preExecRule string,
	postExecRule string,
	suspend bool,
) {
	cmdArgs := []string{"create", "applicationbackupschedules",
		name,
		"-s", schedulePolicyName,
		"-n", namespace,
		"-b", backupLocation,
		"--namespaces", strings.Join(namespaces, ","),
		"--suspend=" + strconv.FormatBool(suspend)}
	if preExecRule != "" {
		cmdArgs = append(cmdArgs, "--preExecRule", preExecRule)
	}
	if postExecRule != "" {
		cmdArgs = append(cmdArgs, "--postExecRule", postExecRule)
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

	expected := "ApplicationBackupSchedule " + name + " created successfully\n"
	testCommon(t, cmdArgs, nil, expected, false)

	// Make sure it was created correctly
	applicationbackup, err := storkops.Instance().GetApplicationBackupSchedule(name, namespace)
	require.NoError(t, err, "Error getting applicationbackup schedule")
	require.Equal(t, name, applicationbackup.Name, "ApplicationBackupSchedule name mismatch")
	require.Equal(t, namespace, applicationbackup.Namespace, "ApplicationBackupSchedule namespace mismatch")
	require.Equal(t, backupLocation, applicationbackup.Spec.Template.Spec.BackupLocation, "ApplicationBackupSchedule backupLocation mismatch")
	require.Equal(t, namespaces, applicationbackup.Spec.Template.Spec.Namespaces, "ApplicationBackupSchedule namespace mismatch")
	require.Equal(t, preExecRule, applicationbackup.Spec.Template.Spec.PreExecRule, "ApplicationBackupSchedule preExecRule mismatch")
	require.Equal(t, postExecRule, applicationbackup.Spec.Template.Spec.PostExecRule, "ApplicationBackupSchedule postExecRule mismatch")
}

func TestGetApplicationBackupSchedulesOneApplicationBackupSchedule(t *testing.T) {
	defer resetTest()
	createApplicationBackupScheduleAndVerify(t, "getapplicationbackupscheduletest", "testpolicy", "test", "backuplocation1", []string{"namespace1"}, "preExec", "postExec", true)

	expected := "NAME                               POLICY-NAME   BACKUP-LOCATION   SUSPEND   LAST-SUCCESS-TIME   LAST-SUCCESS-DURATION\n" +
		"getapplicationbackupscheduletest   testpolicy    backuplocation1   true                          \n"

	cmdArgs := []string{"get", "applicationbackupschedules", "-n", "test"}
	testCommon(t, cmdArgs, nil, expected, false)
}

func TestGetApplicationBackupSchedulesMultiple(t *testing.T) {
	defer resetTest()
	createApplicationBackupScheduleAndVerify(t, "getapplicationbackupscheduletest1", "testpolicy", "default", "backuplocation1", []string{"namespace1"}, "", "", true)
	createApplicationBackupScheduleAndVerify(t, "getapplicationbackupscheduletest2", "testpolicy", "default", "backuplocation2", []string{"namespace1"}, "", "", true)

	expected := "NAME                                POLICY-NAME   BACKUP-LOCATION   SUSPEND   LAST-SUCCESS-TIME   LAST-SUCCESS-DURATION\n" +
		"getapplicationbackupscheduletest1   testpolicy    backuplocation1   true                          \n" +
		"getapplicationbackupscheduletest2   testpolicy    backuplocation2   true                          \n"

	cmdArgs := []string{"get", "applicationbackupschedules", "getapplicationbackupscheduletest1", "getapplicationbackupscheduletest2"}
	testCommon(t, cmdArgs, nil, expected, false)

	// Should get all applicationbackupschedules if no name given
	cmdArgs = []string{"get", "applicationbackupschedules"}
	testCommon(t, cmdArgs, nil, expected, false)

	expected = "NAME                                POLICY-NAME   BACKUP-LOCATION   SUSPEND   LAST-SUCCESS-TIME   LAST-SUCCESS-DURATION\n" +
		"getapplicationbackupscheduletest1   testpolicy    backuplocation1   true                          \n"
	// Should get only one applicationbackup if name given
	cmdArgs = []string{"get", "applicationbackupschedules", "getapplicationbackupscheduletest1"}
	testCommon(t, cmdArgs, nil, expected, false)
}

func TestGetApplicationBackupSchedulesMultipleNamespaces(t *testing.T) {
	defer resetTest()
	_, err := core.Instance().CreateNamespace(&v1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "test1"}})
	require.NoError(t, err, "Error creating test1 namespace")
	_, err = core.Instance().CreateNamespace(&v1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "test2"}})
	require.NoError(t, err, "Error creating test2 namespace")

	createApplicationBackupScheduleAndVerify(t, "getapplicationbackupscheduletest1", "testpolicy", "test1", "backuplocation1", []string{"namespace1"}, "", "", true)
	createApplicationBackupScheduleAndVerify(t, "getapplicationbackupscheduletest2", "testpolicy", "test2", "backuplocation2", []string{"namespace1"}, "", "", true)

	expected := "NAME                                POLICY-NAME   BACKUP-LOCATION   SUSPEND   LAST-SUCCESS-TIME   LAST-SUCCESS-DURATION\n" +
		"getapplicationbackupscheduletest1   testpolicy    backuplocation1   true                          \n"

	cmdArgs := []string{"get", "applicationbackupschedules", "-n", "test1"}
	testCommon(t, cmdArgs, nil, expected, false)

	// Should get all applicationbackupschedules
	cmdArgs = []string{"get", "applicationbackupschedules", "--all-namespaces"}
	expected = "NAMESPACE   NAME                                POLICY-NAME   BACKUP-LOCATION   SUSPEND   LAST-SUCCESS-TIME   LAST-SUCCESS-DURATION\n" +
		"test1       getapplicationbackupscheduletest1   testpolicy    backuplocation1   true                          \n" +
		"test2       getapplicationbackupscheduletest2   testpolicy    backuplocation2   true                          \n"
	testCommon(t, cmdArgs, nil, expected, false)
}

func TestGetApplicationBackupSchedulesWithBackupLocation(t *testing.T) {
	defer resetTest()
	createApplicationBackupScheduleAndVerify(t, "getapplicationbackupscheduletest1", "testpolicy", "default", "backuplocation1", []string{"namespace1"}, "", "", true)
	createApplicationBackupScheduleAndVerify(t, "getapplicationbackupscheduletest2", "testpolicy", "default", "backuplocation2", []string{"namespace1"}, "", "", true)

	expected := "NAME                                POLICY-NAME   BACKUP-LOCATION   SUSPEND   LAST-SUCCESS-TIME   LAST-SUCCESS-DURATION\n" +
		"getapplicationbackupscheduletest1   testpolicy    backuplocation1   true                          \n"

	cmdArgs := []string{"get", "applicationbackupschedules", "-b", "backuplocation1"}
	testCommon(t, cmdArgs, nil, expected, false)
}

func TestGetApplicationBackupSchedulesWithStatus(t *testing.T) {
	defer resetTest()
	createApplicationBackupScheduleAndVerify(t, "getapplicationbackupschedulestatustest", "testpolicy", "default", "backuplocation1", []string{"namespace1"}, "", "", true)
	applicationBackupSchedule, err := storkops.Instance().GetApplicationBackupSchedule("getapplicationbackupschedulestatustest", "default")
	require.NoError(t, err, "Error getting applicationbackup schedule")

	// Update the status of the daily applicationbackup
	applicationBackupSchedule.Status.Items = make(map[storkv1.SchedulePolicyType][]*storkv1.ScheduledApplicationBackupStatus)
	applicationBackupSchedule.Status.Items[storkv1.SchedulePolicyTypeDaily] = make([]*storkv1.ScheduledApplicationBackupStatus, 0)
	now := metav1.Now()
	finishTimestamp := metav1.NewTime(now.Add(5 * time.Minute))
	applicationBackupSchedule.Status.Items[storkv1.SchedulePolicyTypeDaily] = append(applicationBackupSchedule.Status.Items[storkv1.SchedulePolicyTypeDaily],
		&storkv1.ScheduledApplicationBackupStatus{
			Name:              "dailyapplicationbackup",
			CreationTimestamp: now,
			FinishTimestamp:   finishTimestamp,
			Status:            storkv1.ApplicationBackupStatusSuccessful,
		},
	)
	applicationBackupSchedule, err = storkops.Instance().UpdateApplicationBackupSchedule(applicationBackupSchedule)
	require.NoError(t, err, "Error updating applicationbackup schedule")

	expected := "NAME                                     POLICY-NAME   BACKUP-LOCATION   SUSPEND   LAST-SUCCESS-TIME     LAST-SUCCESS-DURATION\n" +
		"getapplicationbackupschedulestatustest   testpolicy    backuplocation1   true      " + toTimeString(finishTimestamp.Time) + "   5m0s\n"
	cmdArgs := []string{"get", "applicationbackupschedules", "getapplicationbackupschedulestatustest"}
	testCommon(t, cmdArgs, nil, expected, false)

	now = metav1.Now()
	finishTimestamp = metav1.NewTime(now.Add(5 * time.Minute))
	applicationBackupSchedule.Status.Items[storkv1.SchedulePolicyTypeWeekly] = append(applicationBackupSchedule.Status.Items[storkv1.SchedulePolicyTypeWeekly],
		&storkv1.ScheduledApplicationBackupStatus{
			Name:              "weeklyapplicationbackup",
			CreationTimestamp: now,
			FinishTimestamp:   finishTimestamp,
			Status:            storkv1.ApplicationBackupStatusSuccessful,
		},
	)
	applicationBackupSchedule, err = storkops.Instance().UpdateApplicationBackupSchedule(applicationBackupSchedule)
	require.NoError(t, err, "Error updating applicationbackup schedule")

	expected = "NAME                                     POLICY-NAME   BACKUP-LOCATION   SUSPEND   LAST-SUCCESS-TIME     LAST-SUCCESS-DURATION\n" +
		"getapplicationbackupschedulestatustest   testpolicy    backuplocation1   true      " + toTimeString(finishTimestamp.Time) + "   5m0s\n"
	cmdArgs = []string{"get", "applicationbackupschedules", "getapplicationbackupschedulestatustest"}
	testCommon(t, cmdArgs, nil, expected, false)

	now = metav1.Now()
	finishTimestamp = metav1.NewTime(now.Add(5 * time.Minute))
	applicationBackupSchedule.Status.Items[storkv1.SchedulePolicyTypeMonthly] = append(applicationBackupSchedule.Status.Items[storkv1.SchedulePolicyTypeMonthly],
		&storkv1.ScheduledApplicationBackupStatus{
			Name:              "monthlyapplicationbackup",
			CreationTimestamp: now,
			FinishTimestamp:   finishTimestamp,
			Status:            storkv1.ApplicationBackupStatusSuccessful,
		},
	)
	_, err = storkops.Instance().UpdateApplicationBackupSchedule(applicationBackupSchedule)
	require.NoError(t, err, "Error updating applicationbackup schedule")

	expected = "NAME                                     POLICY-NAME   BACKUP-LOCATION   SUSPEND   LAST-SUCCESS-TIME     LAST-SUCCESS-DURATION\n" +
		"getapplicationbackupschedulestatustest   testpolicy    backuplocation1   true      " + toTimeString(finishTimestamp.Time) + "   5m0s\n"
	cmdArgs = []string{"get", "applicationbackupschedules", "getapplicationbackupschedulestatustest"}
	testCommon(t, cmdArgs, nil, expected, false)
}

func TestCreateApplicationBackupSchedulesNoNamespace(t *testing.T) {
	cmdArgs := []string{"create", "applicationbackupschedules", "-b", "backuplocation1", "applicationbackup1"}

	expected := "error: need to provide atleast one namespace to migrate"
	testCommon(t, cmdArgs, nil, expected, true)
}

func TestCreateApplicationBackupSchedulesNoBackupLocation(t *testing.T) {
	cmdArgs := []string{"create", "applicationbackupschedules", "applicationbackup1"}

	expected := "error: BackupLocation name needs to be provided for applicationbackup schedule"
	testCommon(t, cmdArgs, nil, expected, true)
}

func TestCreateApplicationBackupSchedulesNoName(t *testing.T) {
	cmdArgs := []string{"create", "applicationbackupschedules"}

	expected := "error: exactly one name needs to be provided for applicationbackup schedule name"
	testCommon(t, cmdArgs, nil, expected, true)
}

func TestCreateApplicationBackupSchedules(t *testing.T) {
	defer resetTest()
	createApplicationBackupScheduleAndVerify(t, "createapplicationbackup", "testpolicy", "default", "backuplocation1", []string{"namespace1"}, "", "", true)
}

func TestCreateDuplicateApplicationBackupSchedules(t *testing.T) {
	defer resetTest()
	createApplicationBackupScheduleAndVerify(t, "createapplicationbackupschedule", "testpolicy", "default", "backuplocation1", []string{"namespace1"}, "", "", true)
	cmdArgs := []string{"create", "applicationbackupschedules", "-s", "testpolicy", "-b", "backuplocation1", "--namespaces", "namespace1", "createapplicationbackupschedule"}

	expected := "Error from server (AlreadyExists): applicationbackupschedules.stork.libopenstorage.org \"createapplicationbackupschedule\" already exists"
	testCommon(t, cmdArgs, nil, expected, true)
}

func TestDeleteApplicationBackupSchedulesNoApplicationBackupName(t *testing.T) {
	cmdArgs := []string{"delete", "applicationbackupschedules"}

	var applicationbackupList storkv1.ApplicationBackupList
	expected := "error: at least one argument needs to be provided for applicationbackup schedule name if backupLocation isn't provided"
	testCommon(t, cmdArgs, &applicationbackupList, expected, true)
}

func TestDeleteApplicationBackupSchedulesNoApplicationBackup(t *testing.T) {
	cmdArgs := []string{"delete", "applicationbackupschedules", "-b", "backuplocation1"}

	expected := "No resources found.\n"
	testCommon(t, cmdArgs, nil, expected, false)
}

func TestDeleteApplicationBackupSchedules(t *testing.T) {
	defer resetTest()
	createApplicationBackupScheduleAndVerify(t, "deleteapplicationbackup", "testpolicy", "default", "backuplocation1", []string{"namespace1"}, "", "", false)

	cmdArgs := []string{"delete", "applicationbackupschedules", "deleteapplicationbackup"}
	expected := "ApplicationBackupSchedule deleteapplicationbackup deleted successfully\n"
	testCommon(t, cmdArgs, nil, expected, false)

	cmdArgs = []string{"delete", "applicationbackupschedules", "deleteapplicationbackup"}
	expected = "Error from server (NotFound): applicationbackupschedules.stork.libopenstorage.org \"deleteapplicationbackup\" not found"
	testCommon(t, cmdArgs, nil, expected, true)

	createApplicationBackupScheduleAndVerify(t, "deleteapplicationbackup1", "testpolicy", "default", "backuplocation1", []string{"namespace1"}, "", "", true)
	createApplicationBackupScheduleAndVerify(t, "deleteapplicationbackup2", "testpolicy", "default", "backuplocation2", []string{"namespace1"}, "", "", true)

	cmdArgs = []string{"delete", "applicationbackupschedules", "deleteapplicationbackup1", "deleteapplicationbackup2"}
	expected = "ApplicationBackupSchedule deleteapplicationbackup1 deleted successfully\n"
	expected += "ApplicationBackupSchedule deleteapplicationbackup2 deleted successfully\n"
	testCommon(t, cmdArgs, nil, expected, false)

	createApplicationBackupScheduleAndVerify(t, "deleteapplicationbackup1", "testpolicy", "default", "backuplocation1", []string{"namespace1"}, "", "", true)
	createApplicationBackupScheduleAndVerify(t, "deleteapplicationbackup2", "testpolicy", "default", "backuplocation1", []string{"namespace1"}, "", "", true)

	cmdArgs = []string{"delete", "applicationbackupschedules", "-b", "backuplocation1"}
	expected = "ApplicationBackupSchedule deleteapplicationbackup1 deleted successfully\n"
	expected += "ApplicationBackupSchedule deleteapplicationbackup2 deleted successfully\n"
	testCommon(t, cmdArgs, nil, expected, false)
}

func TestDefaultApplicationBackupSchedulePolicy(t *testing.T) {
	defer resetTest()
	createApplicationBackupScheduleAndVerify(t, "deleteapplicationbackup", "testpolicy", "default", "backuplocation1", []string{"namespace1"}, "", "", false)

	// Create schedule without the default policy present
	cmdArgs := []string{"create", "applicationbackupschedules", "defaultpolicy", "-n", "test", "-b", "backuplocation", "--namespaces", "test"}
	expected := "error: error getting schedulepolicy default-applicationbackup-policy: schedulepolicies.stork.libopenstorage.org \"default-applicationbackup-policy\" not found"
	testCommon(t, cmdArgs, nil, expected, true)

	// Create again adding default policy
	_, err := storkops.Instance().CreateSchedulePolicy(&storkv1.SchedulePolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name: "default-applicationbackup-policy",
		},
		Policy: storkv1.SchedulePolicyItem{
			Interval: &storkv1.IntervalPolicy{
				IntervalMinutes: 15,
			}},
	})
	require.NoError(t, err, "Error creating schedulepolicy")
	expected = "ApplicationBackupSchedule defaultpolicy created successfully\n"
	testCommon(t, cmdArgs, nil, expected, false)
}

func TestSuspendResumeApplicationBackupSchedule(t *testing.T) {
	name := "testapplicationbackupschedule"
	namespace := "default"
	defer resetTest()
	createApplicationBackupScheduleAndVerify(t, name, "testpolicy", namespace, "backuplocation1", []string{"namespace1"}, "", "", false)

	cmdArgs := []string{"suspend", "applicationbackupschedules", name}
	expected := "ApplicationBackupSchedule " + name + " suspended successfully\n"
	testCommon(t, cmdArgs, nil, expected, false)

	applicationBackupSchedule, err := storkops.Instance().GetApplicationBackupSchedule(name, namespace)
	require.NoError(t, err, "Error getting applicationbackupschedule")
	require.True(t, *applicationBackupSchedule.Spec.Suspend, "applicationbackup schedule not suspended")

	cmdArgs = []string{"resume", "applicationbackupschedules", name}
	expected = "ApplicationBackupSchedule " + name + " resumed successfully\n"
	testCommon(t, cmdArgs, nil, expected, false)

	applicationBackupSchedule, err = storkops.Instance().GetApplicationBackupSchedule(name, namespace)
	require.NoError(t, err, "Error getting applicationbackupschedule")
	require.False(t, *applicationBackupSchedule.Spec.Suspend, "applicationbackup schedule suspended")

	cmdArgs = []string{"suspend", "applicationbackupschedules", "-b", "backuplocation1"}
	expected = "ApplicationBackupSchedule " + name + " suspended successfully\n"
	testCommon(t, cmdArgs, nil, expected, false)

	applicationBackupSchedule, err = storkops.Instance().GetApplicationBackupSchedule(name, namespace)
	require.NoError(t, err, "Error getting applicationbackupschedule")
	require.True(t, *applicationBackupSchedule.Spec.Suspend, "applicationbackup schedule not suspended")

	cmdArgs = []string{"resume", "applicationbackupschedules", "-b", "backuplocation1"}
	expected = "ApplicationBackupSchedule " + name + " resumed successfully\n"
	testCommon(t, cmdArgs, nil, expected, false)

	applicationBackupSchedule, err = storkops.Instance().GetApplicationBackupSchedule(name, namespace)
	require.NoError(t, err, "Error getting applicationbackupschedule")
	require.False(t, *applicationBackupSchedule.Spec.Suspend, "applicationbackup schedule suspended")

	cmdArgs = []string{"suspend", "applicationbackupschedules", "invalidschedule"}
	expected = "Error from server (NotFound): applicationbackupschedules.stork.libopenstorage.org \"invalidschedule\" not found"
	testCommon(t, cmdArgs, nil, expected, true)

	cmdArgs = []string{"resume", "applicationbackupschedules", "invalidschedule"}
	testCommon(t, cmdArgs, nil, expected, true)

}
