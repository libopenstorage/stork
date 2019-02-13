// +build unittest

package storkctl

import (
	"testing"

	storkv1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/portworx/sched-ops/k8s"
	"github.com/stretchr/testify/require"
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
	_, err := k8s.Instance().CreateSchedulePolicy(schedulePolicy)
	require.NoError(t, err, "Error creating schedulepolicy")

	expected = `Error from server (NotFound): schedulepolicies.stork.libopenstorage.org "testpolicy" not found`
	testCommon(t, cmdArgs, nil, expected, true)

	expected = "NAME          INTERVAL-MINUTES   DAILY     WEEKLY    MONTHLY\n" +
		"testpolicy1   N/A                N/A       N/A       N/A\n"
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
	_, err := k8s.Instance().CreateSchedulePolicy(schedulePolicy)
	require.NoError(t, err, "Error creating schedulepolicy")

	expected := "NAME             INTERVAL-MINUTES   DAILY     WEEKLY    MONTHLY\n" +
		"intervalpolicy   Invalid            N/A       N/A       N/A\n"
	cmdArgs := []string{"get", "schedulepolicy", "intervalpolicy"}
	testCommon(t, cmdArgs, nil, expected, false)

	// Update with valid interval
	schedulePolicy.Policy.Interval.IntervalMinutes = 60
	_, err = k8s.Instance().UpdateSchedulePolicy(schedulePolicy)
	require.NoError(t, err, "Error creating schedulepolicy")

	expected = "NAME             INTERVAL-MINUTES   DAILY     WEEKLY    MONTHLY\n" +
		"intervalpolicy   60                 N/A       N/A       N/A\n"
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
	_, err := k8s.Instance().CreateSchedulePolicy(schedulePolicy)
	require.NoError(t, err, "Error creating schedulepolicy")

	expected := "NAME          INTERVAL-MINUTES   DAILY     WEEKLY    MONTHLY\n" +
		"dailypolicy   N/A                Invalid   N/A       N/A\n"
	cmdArgs := []string{"get", "schedulepolicy", "dailypolicy"}
	testCommon(t, cmdArgs, nil, expected, false)

	// 24 hour notation is also invalid
	schedulePolicy.Policy.Daily.Time = "1215"
	testCommon(t, cmdArgs, nil, expected, false)

	// Update with valid time
	schedulePolicy.Policy.Daily.Time = "12:15pm"
	_, err = k8s.Instance().UpdateSchedulePolicy(schedulePolicy)
	require.NoError(t, err, "Error creating schedulepolicy")

	expected = "NAME          INTERVAL-MINUTES   DAILY     WEEKLY    MONTHLY\n" +
		"dailypolicy   N/A                12:15pm   N/A       N/A\n"
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
	_, err := k8s.Instance().CreateSchedulePolicy(schedulePolicy)
	require.NoError(t, err, "Error creating schedulepolicy")

	expected := "NAME           INTERVAL-MINUTES   DAILY     WEEKLY    MONTHLY\n" +
		"weeklypolicy   N/A                N/A       Invalid   N/A\n"
	cmdArgs := []string{"get", "schedulepolicy", "weeklypolicy"}
	testCommon(t, cmdArgs, nil, expected, false)

	// Update with valid day but invalid time
	schedulePolicy.Policy.Weekly.Day = "Sun"
	schedulePolicy.Policy.Weekly.Time = "1215"
	_, err = k8s.Instance().UpdateSchedulePolicy(schedulePolicy)
	require.NoError(t, err, "Error creating schedulepolicy")
	testCommon(t, cmdArgs, nil, expected, false)

	// Update with valid time
	schedulePolicy.Policy.Weekly.Time = "12:15pm"
	_, err = k8s.Instance().UpdateSchedulePolicy(schedulePolicy)
	require.NoError(t, err, "Error creating schedulepolicy")
	expected = "NAME           INTERVAL-MINUTES   DAILY     WEEKLY        MONTHLY\n" +
		"weeklypolicy   N/A                N/A       Sun@12:15pm   N/A\n"
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
	_, err := k8s.Instance().CreateSchedulePolicy(schedulePolicy)
	require.NoError(t, err, "Error creating schedulepolicy")

	expected := "NAME            INTERVAL-MINUTES   DAILY     WEEKLY    MONTHLY\n" +
		"monthlypolicy   N/A                N/A       N/A       Invalid\n"
	cmdArgs := []string{"get", "schedulepolicy", "monthlypolicy"}
	testCommon(t, cmdArgs, nil, expected, false)

	// Update with valid date but invalid time
	schedulePolicy.Policy.Monthly.Date = 15
	schedulePolicy.Policy.Monthly.Time = "1215"
	_, err = k8s.Instance().UpdateSchedulePolicy(schedulePolicy)
	require.NoError(t, err, "Error creating schedulepolicy")
	testCommon(t, cmdArgs, nil, expected, false)

	// Update with valid time
	schedulePolicy.Policy.Monthly.Time = "12:15pm"
	_, err = k8s.Instance().UpdateSchedulePolicy(schedulePolicy)
	require.NoError(t, err, "Error creating schedulepolicy")
	expected = "NAME            INTERVAL-MINUTES   DAILY     WEEKLY    MONTHLY\n" +
		"monthlypolicy   N/A                N/A       N/A       15@12:15pm\n"
	testCommon(t, cmdArgs, nil, expected, false)
}
