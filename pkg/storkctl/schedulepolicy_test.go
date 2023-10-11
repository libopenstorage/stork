//go:build unittest
// +build unittest

package storkctl

import (
	"github.com/portworx/sched-ops/k8s/core"
	"testing"

	storkv1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	storkops "github.com/portworx/sched-ops/k8s/stork"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestNoSchedulePolicy(t *testing.T) {
	cmdArgs := []string{"get", "schedulepolicy"}

	expected := "No resources found.\n"
	testCommon(t, cmdArgs, nil, expected, false)
}

func TestSchedulePolicyNotFound(t *testing.T) {
	defer resetTest()
	cmdArgs := []string{"get", "schedulepolicy", "testpolicy"}
	expected := `Error from server (NotFound): schedulepolicies.stork.libopenstorage.org "testpolicy" not found`
	testCommon(t, cmdArgs, nil, expected, true)

	schedulePolicy := &storkv1.SchedulePolicy{
		ObjectMeta: meta.ObjectMeta{
			Name: "testpolicy1",
		},
	}
	_, err := storkops.Instance().CreateSchedulePolicy(schedulePolicy)
	require.NoError(t, err, "Error creating schedulepolicy")

	expected = `Error from server (NotFound): schedulepolicies.stork.libopenstorage.org "testpolicy" not found`
	testCommon(t, cmdArgs, nil, expected, true)

	expected = "NAME          INTERVAL-MINUTES   DAILY   WEEKLY   MONTHLY\n" +
		"testpolicy1   N/A                N/A     N/A      N/A\n"
	cmdArgs = []string{"get", "schedulepolicy", "testpolicy1"}
	testCommon(t, cmdArgs, nil, expected, false)
}

func TestIntervalSchedulePolicy(t *testing.T) {
	defer resetTest()

	schedulePolicy := &storkv1.SchedulePolicy{
		ObjectMeta: meta.ObjectMeta{
			Name: "intervalpolicy",
		},
		Policy: storkv1.SchedulePolicyItem{
			Interval: &storkv1.IntervalPolicy{
				//Invalid interval
				IntervalMinutes: -1,
			},
		},
	}
	_, err := storkops.Instance().CreateSchedulePolicy(schedulePolicy)
	require.NoError(t, err, "Error creating schedulepolicy")

	expected := "NAME             INTERVAL-MINUTES   DAILY   WEEKLY   MONTHLY\n" +
		"intervalpolicy   Invalid            N/A     N/A      N/A\n"
	cmdArgs := []string{"get", "schedulepolicy", "intervalpolicy"}
	testCommon(t, cmdArgs, nil, expected, false)

	// Update with valid interval
	schedulePolicy.Policy.Interval.IntervalMinutes = 60
	_, err = storkops.Instance().UpdateSchedulePolicy(schedulePolicy)
	require.NoError(t, err, "Error creating schedulepolicy")

	expected = "NAME             INTERVAL-MINUTES   DAILY   WEEKLY   MONTHLY\n" +
		"intervalpolicy   60                 N/A     N/A      N/A\n"
	testCommon(t, cmdArgs, nil, expected, false)
}

func TestDailySchedulePolicy(t *testing.T) {
	defer resetTest()

	schedulePolicy := &storkv1.SchedulePolicy{
		ObjectMeta: meta.ObjectMeta{
			Name: "dailypolicy",
		},
		Policy: storkv1.SchedulePolicyItem{
			Daily: &storkv1.DailyPolicy{
				//Invalid time
				Time: "13:15pm",
			},
		},
	}
	_, err := storkops.Instance().CreateSchedulePolicy(schedulePolicy)
	require.NoError(t, err, "Error creating schedulepolicy")

	expected := "NAME          INTERVAL-MINUTES   DAILY     WEEKLY   MONTHLY\n" +
		"dailypolicy   N/A                Invalid   N/A      N/A\n"
	cmdArgs := []string{"get", "schedulepolicy", "dailypolicy"}
	testCommon(t, cmdArgs, nil, expected, false)

	// 24 hour notation is also invalid
	schedulePolicy.Policy.Daily.Time = "1215"
	testCommon(t, cmdArgs, nil, expected, false)

	// Update with valid time
	schedulePolicy.Policy.Daily.Time = "12:15pm"
	_, err = storkops.Instance().UpdateSchedulePolicy(schedulePolicy)
	require.NoError(t, err, "Error creating schedulepolicy")

	expected = "NAME          INTERVAL-MINUTES   DAILY     WEEKLY   MONTHLY\n" +
		"dailypolicy   N/A                12:15pm   N/A      N/A\n"
	testCommon(t, cmdArgs, nil, expected, false)
}

func TestWeeklySchedulePolicy(t *testing.T) {
	defer resetTest()

	schedulePolicy := &storkv1.SchedulePolicy{
		ObjectMeta: meta.ObjectMeta{
			Name: "weeklypolicy",
		},
		Policy: storkv1.SchedulePolicyItem{
			Weekly: &storkv1.WeeklyPolicy{
				//Invalid day
				Day:  "Weekday",
				Time: "12:15pm",
			},
		},
	}
	_, err := storkops.Instance().CreateSchedulePolicy(schedulePolicy)
	require.NoError(t, err, "Error creating schedulepolicy")

	expected := "NAME           INTERVAL-MINUTES   DAILY   WEEKLY    MONTHLY\n" +
		"weeklypolicy   N/A                N/A     Invalid   N/A\n"
	cmdArgs := []string{"get", "schedulepolicy", "weeklypolicy"}
	testCommon(t, cmdArgs, nil, expected, false)

	// Update with valid day but invalid time
	schedulePolicy.Policy.Weekly.Day = "Sun"
	schedulePolicy.Policy.Weekly.Time = "1215"
	_, err = storkops.Instance().UpdateSchedulePolicy(schedulePolicy)
	require.NoError(t, err, "Error creating schedulepolicy")
	testCommon(t, cmdArgs, nil, expected, false)

	// Update with valid time
	schedulePolicy.Policy.Weekly.Time = "12:15pm"
	_, err = storkops.Instance().UpdateSchedulePolicy(schedulePolicy)
	require.NoError(t, err, "Error creating schedulepolicy")
	expected = "NAME           INTERVAL-MINUTES   DAILY   WEEKLY        MONTHLY\n" +
		"weeklypolicy   N/A                N/A     Sun@12:15pm   N/A\n"
	testCommon(t, cmdArgs, nil, expected, false)
}

func TestMonthlySchedulePolicy(t *testing.T) {
	defer resetTest()

	schedulePolicy := &storkv1.SchedulePolicy{
		ObjectMeta: meta.ObjectMeta{
			Name: "monthlypolicy",
		},
		Policy: storkv1.SchedulePolicyItem{
			Monthly: &storkv1.MonthlyPolicy{
				//Invalid date
				Date: 35,
				Time: "12:15pm",
			},
		},
	}
	_, err := storkops.Instance().CreateSchedulePolicy(schedulePolicy)
	require.NoError(t, err, "Error creating schedulepolicy")

	expected := "NAME            INTERVAL-MINUTES   DAILY   WEEKLY   MONTHLY\n" +
		"monthlypolicy   N/A                N/A     N/A      Invalid\n"
	cmdArgs := []string{"get", "schedulepolicy", "monthlypolicy"}
	testCommon(t, cmdArgs, nil, expected, false)

	// Update with valid date but invalid time
	schedulePolicy.Policy.Monthly.Date = 15
	schedulePolicy.Policy.Monthly.Time = "1215"
	_, err = storkops.Instance().UpdateSchedulePolicy(schedulePolicy)
	require.NoError(t, err, "Error creating schedulepolicy")
	testCommon(t, cmdArgs, nil, expected, false)

	// Update with valid time
	schedulePolicy.Policy.Monthly.Time = "12:15pm"
	_, err = storkops.Instance().UpdateSchedulePolicy(schedulePolicy)
	require.NoError(t, err, "Error creating schedulepolicy")
	expected = "NAME            INTERVAL-MINUTES   DAILY   WEEKLY   MONTHLY\n" +
		"monthlypolicy   N/A                N/A     N/A      15@12:15pm\n"
	testCommon(t, cmdArgs, nil, expected, false)
}

func TestCreateSchedulePolicyFailureCases(t *testing.T) {
	defer resetTest()
	cmdArgs := []string{"create", "schedulepolicy"}
	expected := "error: exactly one name needs to be provided for schedule policy name"
	testCommon(t, cmdArgs, nil, expected, true)

	//Invalid Schedule Policy Type
	cmdArgs = []string{"create", "schedulepolicy", "test-policy", "-t", "Invalid"}
	expected = "error: need to provide a valid schedule policy type. Valid schedule types are Interval, Daily, Weekly and Monthly"
	testCommon(t, cmdArgs, nil, expected, true)

	//Invalid IntervalMinutes value for Interval Policy
	cmdArgs = []string{"create", "schedulepolicy", "test-policy", "-t", "Interval", "--interval-minutes", "0"}
	expected = "error: Invalid intervalMinutes (0) in Interval policy"
	testCommon(t, cmdArgs, nil, expected, true)

	//Invalid Time value for Daily Policy
	cmdArgs = []string{"create", "schedulepolicy", "test-policy", "-t", "Daily", "--time", "12:75PM"}
	expected = "error: Invalid time (12:75PM) in Daily policy: parsing time \"12:75PM\": minute out of range"
	testCommon(t, cmdArgs, nil, expected, true)

	//Invalid Day value for Weekly Policy
	cmdArgs = []string{"create", "schedulepolicy", "test-policy", "-t", "Weekly", "--time", "12:05PM", "--day-of-week", "SUNDAY"}
	expected = "error: Invalid day of the week (SUNDAY) in Weekly policy"
	testCommon(t, cmdArgs, nil, expected, true)

	//Invalid Date value for Monthly Policy
	cmdArgs = []string{"create", "schedulepolicy", "test-policy", "-t", "Monthly", "--date-of-month", "32"}
	expected = "error: Invalid date of the month (32) in Monthly policy"
	testCommon(t, cmdArgs, nil, expected, true)
}

func TestCreateIntervalPolicy(t *testing.T) {
	defer resetTest()
	policyName := "test-policy"
	cmdArgs := []string{"create", "schedulepolicy", policyName, "--policy-type", "Interval", "--interval-minutes", "90", "--retain", "2"}
	expected := "schedule policy test-policy created successfully\n"
	testCommon(t, cmdArgs, nil, expected, false)

	// Make sure it was created correctly
	actualPolicy, err := storkops.Instance().GetSchedulePolicy(policyName)
	require.NoError(t, err, "Error getting SchedulePolicy")

	expectedPolicy := storkv1.SchedulePolicy{
		ObjectMeta: meta.ObjectMeta{
			Name: policyName,
		},
		Policy: storkv1.SchedulePolicyItem{
			Interval: &storkv1.IntervalPolicy{
				IntervalMinutes: 90,
				Retain:          2,
			}},
	}
	require.Equal(t, expectedPolicy, *actualPolicy, "Schedule Interval Policy mismatch")
}

func TestCreateDailyPolicy(t *testing.T) {
	defer resetTest()
	policyName := "test-policy"
	cmdArgs := []string{"create", "schedulepolicy", policyName, "--policy-type", "Daily", "--time", "12:30PM", "--retain", "2"}
	expected := "schedule policy test-policy created successfully\n"
	testCommon(t, cmdArgs, nil, expected, false)

	// Make sure it was created correctly
	actualPolicy, err := storkops.Instance().GetSchedulePolicy(policyName)
	require.NoError(t, err, "Error getting SchedulePolicy")

	expectedPolicy := storkv1.SchedulePolicy{
		ObjectMeta: meta.ObjectMeta{
			Name: policyName,
		},
		Policy: storkv1.SchedulePolicyItem{
			Daily: &storkv1.DailyPolicy{
				Time:                 "12:30PM",
				Retain:               2,
				ForceFullSnapshotDay: "MONDAY", //default value of policy.daily.forceFullSnapshotDay
			}},
	}
	require.Equal(t, expectedPolicy, *actualPolicy, "Schedule Daily Policy mismatch")
}

func TestCreateWeeklyPolicy(t *testing.T) {
	defer resetTest()
	policyName := "test-policy"
	cmdArgs := []string{"create", "schedulepolicy", policyName, "--policy-type", "Weekly", "--time", "1:30PM", "--day-of-week", "Friday", "--retain", "4"}
	expected := "schedule policy test-policy created successfully\n"
	testCommon(t, cmdArgs, nil, expected, false)

	// Make sure it was created correctly
	actualPolicy, err := storkops.Instance().GetSchedulePolicy(policyName)
	require.NoError(t, err, "Error getting SchedulePolicy")

	expectedPolicy := storkv1.SchedulePolicy{
		ObjectMeta: meta.ObjectMeta{
			Name: policyName,
		},
		Policy: storkv1.SchedulePolicyItem{
			Weekly: &storkv1.WeeklyPolicy{
				Day:    "Friday",
				Time:   "1:30PM",
				Retain: 4,
			}},
	}
	require.Equal(t, expectedPolicy, *actualPolicy, "Schedule Monthly Policy mismatch")
}

func TestCreateMonthlyPolicy(t *testing.T) {
	defer resetTest()
	policyName := "test-policy"
	cmdArgs := []string{"create", "schedulepolicy", policyName, "--policy-type", "Monthly", "--time", "12:30PM", "--date-of-month", "12", "--retain", "2"}
	expected := "schedule policy test-policy created successfully\n"
	testCommon(t, cmdArgs, nil, expected, false)

	// Make sure it was created correctly
	actualPolicy, err := storkops.Instance().GetSchedulePolicy(policyName)
	require.NoError(t, err, "Error getting SchedulePolicy")

	expectedPolicy := storkv1.SchedulePolicy{
		ObjectMeta: meta.ObjectMeta{
			Name: policyName,
		},
		Policy: storkv1.SchedulePolicyItem{
			Monthly: &storkv1.MonthlyPolicy{
				Date:   12,
				Time:   "12:30PM",
				Retain: 2,
			}},
	}
	require.Equal(t, expectedPolicy, *actualPolicy, "Schedule Monthly Policy mismatch")
}

func TestDeleteSchedulePolicyFailureCases(t *testing.T) {
	defer resetTest()
	cmdArgs := []string{"delete", "schedulepolicy"}
	expected := "error: at least one argument needs to be provided for schedule policy name"
	testCommon(t, cmdArgs, nil, expected, true)

	cmdArgs = []string{"delete", "schedulepolicy", "test-delete-schedule-policy"}
	expected = "Error from server (NotFound): schedulepolicies.stork.libopenstorage.org \"test-delete-schedule-policy\" not found"
	testCommon(t, cmdArgs, nil, expected, true)
}

func TestDeleteSchedulePolicyBeingUsedErrorCase(t *testing.T) {
	defer resetTest()
	schedulePolicy := &storkv1.SchedulePolicy{}
	schedulePolicy.Name = "test-policy"
	_, err := storkops.Instance().CreateSchedulePolicy(schedulePolicy)
	require.NoError(t, err, "Error creating schedulepolicy")

	_, err = core.Instance().CreateNamespace(&v1.Namespace{ObjectMeta: meta.ObjectMeta{Name: "test-ns"}})
	require.NoError(t, err, "Error creating namespace")

	mSchedule := &storkv1.MigrationSchedule{ObjectMeta: meta.ObjectMeta{Name: "test-migration-schedule"}}
	mSchedule.Spec.SchedulePolicyName = "test-policy"
	mSchedule.Namespace = "test-ns"
	_, err = storkops.Instance().CreateMigrationSchedule(mSchedule)
	require.NoError(t, err, "Error creating migration schedule")

	abSchedule := &storkv1.ApplicationBackupSchedule{ObjectMeta: meta.ObjectMeta{Name: "test-applicationBackup-schedule"}}
	abSchedule.Spec.SchedulePolicyName = "test-policy"
	abSchedule.Namespace = "test-ns"
	_, err = storkops.Instance().CreateApplicationBackupSchedule(abSchedule)
	require.NoError(t, err, "Error creating Application Backup schedule")

	vsSchedule := &storkv1.VolumeSnapshotSchedule{ObjectMeta: meta.ObjectMeta{Name: "test-volumeSnapshot-schedule"}}
	vsSchedule.Spec.SchedulePolicyName = "test-policy"
	vsSchedule.Namespace = "test-ns"
	_, err = storkops.Instance().CreateSnapshotSchedule(vsSchedule)
	require.NoError(t, err, "Error creating Volume Snapshot schedule")

	cmdArgs := []string{"delete", "schedulepolicy", "test-policy"}
	expected := "error: cannot delete the schedule policy test-policy.\n" +
		"The resource is linked to -> Migration Schedules : test-ns/test-migration-schedule\n" +
		"Application Backup Schedules : test-ns/test-applicationBackup-schedule\n" +
		"Volume Snapshot Schedules : test-ns/test-volumeSnapshot-schedule\n"
	testCommon(t, cmdArgs, nil, expected, true)

}

func TestDeleteSchedulePolicy(t *testing.T) {
	defer resetTest()
	schedulePolicy1 := &storkv1.SchedulePolicy{
		ObjectMeta: meta.ObjectMeta{
			Name: "monthly-policy",
		},
		Policy: storkv1.SchedulePolicyItem{
			Monthly: &storkv1.MonthlyPolicy{
				Date: 30,
				Time: "12:15pm",
			},
		},
	}
	schedulePolicy2 := &storkv1.SchedulePolicy{
		ObjectMeta: meta.ObjectMeta{
			Name: "weekly-policy",
		},
		Policy: storkv1.SchedulePolicyItem{
			Weekly: &storkv1.WeeklyPolicy{
				Day:  "Monday",
				Time: "12:15pm",
			},
		},
	}

	_, err := storkops.Instance().CreateSchedulePolicy(schedulePolicy1)
	require.NoError(t, err, "Error creating schedulepolicy")

	_, err = storkops.Instance().CreateSchedulePolicy(schedulePolicy2)
	require.NoError(t, err, "Error creating schedulepolicy")

	cmdArgs := []string{"delete", "schedulepolicy", "monthly-policy", "weekly-policy"}
	expected := "schedule policy monthly-policy deleted successfully\nschedule policy weekly-policy deleted successfully\n"
	testCommon(t, cmdArgs, nil, expected, false)
}
