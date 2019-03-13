// +build unittest

package schedule

import (
	"testing"
	"time"

	stork_api "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	fakeclient "github.com/libopenstorage/stork/pkg/client/clientset/versioned/fake"
	"github.com/portworx/sched-ops/k8s"
	"github.com/stretchr/testify/require"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	kubernetes "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest/fake"
)

var fakeStorkClient *fakeclient.Clientset
var fakeRestClient *fake.RESTClient

func init() {
	resetTest()
}
func TestSchedule(t *testing.T) {
	t.Run("triggerIntervalRequiredTest", triggerIntervalRequiredTest)
	t.Run("triggerDailyRequiredTest", triggerDailyRequiredTest)
	t.Run("triggerWeeklyRequiredTest", triggerWeeklyRequiredTest)
	t.Run("triggerMonthlyRequiredTest", triggerMonthlyRequiredTest)
	t.Run("validateSchedulePolicyTest", validateSchedulePolicyTest)
	t.Run("policyRetainTest", policyRetainTest)
}

func resetTest() {
	scheme := runtime.NewScheme()
	stork_api.AddToScheme(scheme)
	fakeStorkClient = fakeclient.NewSimpleClientset()
	fakeKubeClient := kubernetes.NewSimpleClientset()

	k8s.Instance().SetClient(fakeKubeClient, nil, fakeStorkClient, nil, nil)
}

func triggerIntervalRequiredTest(t *testing.T) {
	defer k8s.Instance().DeleteSchedulePolicy("intervalpolicy")
	_, err := k8s.Instance().CreateSchedulePolicy(&stork_api.SchedulePolicy{
		ObjectMeta: meta.ObjectMeta{
			Name: "intervalpolicy",
		},
		Policy: stork_api.SchedulePolicyItem{
			Interval: &stork_api.IntervalPolicy{
				IntervalMinutes: 60,
			},
		},
	})
	require.NoError(t, err, "Error creating policy")

	required, err := TriggerRequired("missingpolicy", stork_api.SchedulePolicyTypeInterval, meta.Date(2019, time.February, 7, 23, 14, 0, 0, time.Local))
	require.Error(t, err, "Should return error for missing policy")

	setMockTime(time.Date(2019, time.February, 7, 23, 16, 0, 0, time.Local))
	// Last triggered 2 mins ago
	required, err = TriggerRequired("intervalpolicy", stork_api.SchedulePolicyTypeInterval, meta.Date(2019, time.February, 7, 23, 14, 0, 0, time.Local))
	require.NoError(t, err, "Error checking if trigger required")
	require.False(t, required, "Trigger should not have been required")
	// Last triggered 59 mins ago
	required, err = TriggerRequired("intervalpolicy", stork_api.SchedulePolicyTypeInterval, meta.Date(2019, time.February, 7, 22, 16, 0, 0, time.Local))
	require.NoError(t, err, "Error checking if trigger required")
	require.False(t, required, "Trigger should not have been required")
	// Last triggered 61 mins ago
	required, err = TriggerRequired("intervalpolicy", stork_api.SchedulePolicyTypeInterval, meta.Date(2019, time.February, 7, 22, 14, 0, 0, time.Local))
	require.NoError(t, err, "Error checking if trigger required")
	require.True(t, required, "Trigger should have been required")
}

func triggerDailyRequiredTest(t *testing.T) {
	defer k8s.Instance().DeleteSchedulePolicy("dailypolicy")
	_, err := k8s.Instance().CreateSchedulePolicy(&stork_api.SchedulePolicy{
		ObjectMeta: meta.ObjectMeta{
			Name: "dailypolicy",
		},
		Policy: stork_api.SchedulePolicyItem{
			Daily: &stork_api.DailyPolicy{
				Time: "11:15PM",
			},
		},
	})
	require.NoError(t, err, "Error creating policy")

	required, err := TriggerRequired("missingpolicy", stork_api.SchedulePolicyTypeDaily, meta.Date(2019, time.February, 7, 23, 14, 0, 0, time.Local))
	require.Error(t, err, "Should return error for missing policy")

	setMockTime(time.Date(2019, time.February, 7, 23, 16, 0, 0, time.Local))
	// Last triggered before schedule
	required, err = TriggerRequired("dailypolicy", stork_api.SchedulePolicyTypeDaily, meta.Date(2019, time.February, 7, 23, 14, 0, 0, time.Local))
	require.NoError(t, err, "Error checking if trigger required")
	require.True(t, required, "Trigger should have been required")

	// Last triggered at schedule
	required, err = TriggerRequired("dailypolicy", stork_api.SchedulePolicyTypeDaily, meta.Date(2019, time.February, 7, 23, 15, 0, 0, time.Local))
	require.NoError(t, err, "Error checking if trigger required")
	require.False(t, required, "Trigger should not have been required")

	// Last triggered one day ago before schedule
	required, err = TriggerRequired("dailypolicy", stork_api.SchedulePolicyTypeDaily, meta.Date(2019, time.February, 6, 23, 14, 0, 0, time.Local))
	require.NoError(t, err, "Error checking if trigger required")
	require.True(t, required, "Trigger should have been required")

	// Last triggered one day ago after schedule
	required, err = TriggerRequired("dailypolicy", stork_api.SchedulePolicyTypeDaily, meta.Date(2019, time.February, 6, 23, 16, 0, 0, time.Local))
	require.NoError(t, err, "Error checking if trigger required")
	require.True(t, required, "Trigger should have been required")
}

func triggerWeeklyRequiredTest(t *testing.T) {
	defer k8s.Instance().DeleteSchedulePolicy("weeklypolicy")
	_, err := k8s.Instance().CreateSchedulePolicy(&stork_api.SchedulePolicy{
		ObjectMeta: meta.ObjectMeta{
			Name: "weeklypolicy",
		},
		Policy: stork_api.SchedulePolicyItem{
			Weekly: &stork_api.WeeklyPolicy{
				Day:  "Sunday",
				Time: "11:15pm",
			},
		},
	})
	require.NoError(t, err, "Error creating policy")

	required, err := TriggerRequired("missingpolicy", stork_api.SchedulePolicyTypeWeekly, meta.Date(2019, time.February, 7, 23, 14, 0, 0, time.Local))
	require.Error(t, err, "Should return error for missing policy")

	setMockTime(time.Date(2019, time.February, 7, 23, 16, 0, 0, time.Local)) // Current day: Thursday
	// LastTriggered one week before on Saturday at 11:15pm
	required, err = TriggerRequired("weeklypolicy", stork_api.SchedulePolicyTypeWeekly, meta.Date(2019, time.February, 2, 23, 16, 0, 0, time.Local))
	require.NoError(t, err, "Error checking if trigger required")
	require.False(t, required, "Trigger should not have been required")

	// LastTriggered one week before on Sunday at 11:15pm
	required, err = TriggerRequired("weeklypolicy", stork_api.SchedulePolicyTypeWeekly, meta.Date(2019, time.February, 3, 23, 15, 0, 0, time.Local))
	require.NoError(t, err, "Error checking if trigger required")
	require.False(t, required, "Trigger should not have been required")

	setMockTime(time.Date(2019, time.February, 10, 23, 16, 0, 0, time.Local)) // Current date: Sunday 11:16pm
	// LastTriggered last Wednesday at 11:16pm
	required, err = TriggerRequired("weeklypolicy", stork_api.SchedulePolicyTypeWeekly, meta.Date(2019, time.February, 6, 23, 16, 0, 0, time.Local))
	require.NoError(t, err, "Error checking if trigger required")
	require.True(t, required, "Trigger should have been required")
}

func triggerMonthlyRequiredTest(t *testing.T) {
	_, err := k8s.Instance().CreateSchedulePolicy(&stork_api.SchedulePolicy{
		ObjectMeta: meta.ObjectMeta{
			Name: "monthlypolicy",
		},
		Policy: stork_api.SchedulePolicyItem{
			Monthly: &stork_api.MonthlyPolicy{
				Date: 28,
				Time: "11:15pm",
			},
		},
	})
	require.NoError(t, err, "Error creating policy")

	required, err := TriggerRequired("missingpolicy", stork_api.SchedulePolicyTypeMonthly, meta.Date(2019, time.February, 7, 23, 14, 0, 0, time.Local))
	require.Error(t, err, "Should return error for missing policy")

	setMockTime(time.Date(2019, time.February, 28, 23, 16, 0, 0, time.Local))
	// Last triggered before schedule
	required, err = TriggerRequired("monthlypolicy", stork_api.SchedulePolicyTypeMonthly, meta.Date(2019, time.February, 2, 23, 16, 0, 0, time.Local))
	require.NoError(t, err, "Error checking if trigger required")
	require.True(t, required, "Trigger should have been required")

	// Last triggered one minute after schedule
	required, err = TriggerRequired("monthlypolicy", stork_api.SchedulePolicyTypeMonthly, meta.Date(2019, time.February, 28, 23, 16, 0, 0, time.Local))
	require.NoError(t, err, "Error checking if trigger required")
	require.False(t, required, "Trigger should not have been required")
}

func validateSchedulePolicyTest(t *testing.T) {
	policy := &stork_api.SchedulePolicy{
		ObjectMeta: meta.ObjectMeta{
			Name: "validpolicy",
		},
		Policy: stork_api.SchedulePolicyItem{
			Daily: &stork_api.DailyPolicy{
				Time: "01:15am",
			},
			Weekly: &stork_api.WeeklyPolicy{
				Day:  "Sunday",
				Time: "11:15pm",
			},
			Monthly: &stork_api.MonthlyPolicy{
				Date: 15,
				Time: "12:15pm",
			},
		},
	}
	err := ValidateSchedulePolicy(policy)
	require.NoError(t, err, "Valid policy shouldn't return error")

	policy = &stork_api.SchedulePolicy{
		ObjectMeta: meta.ObjectMeta{
			Name: "invaliddailypolicy",
		},
		Policy: stork_api.SchedulePolicyItem{
			Daily: &stork_api.DailyPolicy{
				Time: "25:15am",
			},
		},
	}
	err = ValidateSchedulePolicy(policy)
	require.Error(t, err, "Invalid daily policy should return error")

	policy = &stork_api.SchedulePolicy{
		ObjectMeta: meta.ObjectMeta{
			Name: "invalidweeklypolicy",
		},
		Policy: stork_api.SchedulePolicyItem{
			Weekly: &stork_api.WeeklyPolicy{
				Day:  "T",
				Time: "11:15pm",
			},
		},
	}
	err = ValidateSchedulePolicy(policy)
	require.Error(t, err, "Invalid weekly policy should return error")

	policy = &stork_api.SchedulePolicy{
		ObjectMeta: meta.ObjectMeta{
			Name: "invalidweeklypolicy",
		},
		Policy: stork_api.SchedulePolicyItem{
			Weekly: &stork_api.WeeklyPolicy{
				Day:  "Tue",
				Time: "13:15pm",
			},
		},
	}
	err = ValidateSchedulePolicy(policy)
	require.Error(t, err, "Invalid weekly policy should return error")

	policy = &stork_api.SchedulePolicy{
		ObjectMeta: meta.ObjectMeta{
			Name: "invalidMonthlypolicy",
		},
		Policy: stork_api.SchedulePolicyItem{
			Monthly: &stork_api.MonthlyPolicy{
				Date: 32,
				Time: "11:15pm",
			},
		},
	}
	err = ValidateSchedulePolicy(policy)
	require.Error(t, err, "Invalid monthly policy should return error")

	policy = &stork_api.SchedulePolicy{
		ObjectMeta: meta.ObjectMeta{
			Name: "invalidMonthlypolicy",
		},
		Policy: stork_api.SchedulePolicyItem{
			Monthly: &stork_api.MonthlyPolicy{
				Date: 12,
				Time: "13:15pm",
			},
		},
	}
	err = ValidateSchedulePolicy(policy)
	require.Error(t, err, "Invalid monthly policy should return error")
}

func policyRetainTest(t *testing.T) {
	policyName := "policy"
	policy, err := k8s.Instance().CreateSchedulePolicy(&stork_api.SchedulePolicy{
		ObjectMeta: meta.ObjectMeta{
			Name: policyName,
		},
		Policy: stork_api.SchedulePolicyItem{
			Interval: &stork_api.IntervalPolicy{
				IntervalMinutes: 60,
			},
			Daily: &stork_api.DailyPolicy{
				Time: "10:40PM",
			},
			Weekly: &stork_api.WeeklyPolicy{
				Time: "10:40PM",
				Day:  "Thur",
			},
			Monthly: &stork_api.MonthlyPolicy{
				Time: "10:40PM",
				Date: 25,
			},
		},
	})
	require.NoError(t, err, "Error creating schedule policy")

	retain, err := GetRetain(policyName, stork_api.SchedulePolicyTypeInterval)
	require.NoError(t, err, "Error getting retain")
	require.Equal(t, stork_api.DefaultIntervalPolicyRetain, retain, "Wrong default retain for interval policy")
	policy.Policy.Interval.Retain = 0
	_, err = k8s.Instance().UpdateSchedulePolicy(policy)
	require.NoError(t, err, "Error updating schedule policy")
	retain, err = GetRetain(policyName, stork_api.SchedulePolicyTypeInterval)
	require.NoError(t, err, "Error getting retain")
	require.Equal(t, stork_api.DefaultIntervalPolicyRetain, retain, "Wrong default retain for interval policy")

	policy.Policy.Interval.Retain = 5
	_, err = k8s.Instance().UpdateSchedulePolicy(policy)
	require.NoError(t, err, "Error updating schedule policy")

	retain, err = GetRetain(policyName, stork_api.SchedulePolicyTypeInterval)
	require.NoError(t, err, "Error getting retain")
	require.Equal(t, policy.Policy.Interval.Retain, retain, "Wrong retain for interval policy")

	retain, err = GetRetain(policyName, stork_api.SchedulePolicyTypeDaily)
	require.NoError(t, err, "Error getting retain")
	require.Equal(t, stork_api.DefaultDailyPolicyRetain, retain, "Wrong default retain for daily policy")
	policy.Policy.Daily.Retain = 0
	_, err = k8s.Instance().UpdateSchedulePolicy(policy)
	require.NoError(t, err, "Error updating schedule policy")
	retain, err = GetRetain(policyName, stork_api.SchedulePolicyTypeDaily)
	require.NoError(t, err, "Error getting retain")
	require.Equal(t, stork_api.DefaultDailyPolicyRetain, retain, "Wrong default retain for daily policy")

	policy.Policy.Daily.Retain = 10
	_, err = k8s.Instance().UpdateSchedulePolicy(policy)
	require.NoError(t, err, "Error updating schedule policy")
	retain, err = GetRetain(policyName, stork_api.SchedulePolicyTypeDaily)
	require.NoError(t, err, "Error getting retain")
	require.Equal(t, policy.Policy.Daily.Retain, retain, "Wrong default retain for daily policy")

	retain, err = GetRetain(policyName, stork_api.SchedulePolicyTypeWeekly)
	require.NoError(t, err, "Error getting retain")
	require.Equal(t, stork_api.DefaultWeeklyPolicyRetain, retain, "Wrong default retain for weekly policy")
	policy.Policy.Weekly.Retain = 0
	_, err = k8s.Instance().UpdateSchedulePolicy(policy)
	require.NoError(t, err, "Error updating schedule policy")
	retain, err = GetRetain(policyName, stork_api.SchedulePolicyTypeWeekly)
	require.NoError(t, err, "Error getting retain")
	require.Equal(t, stork_api.DefaultWeeklyPolicyRetain, retain, "Wrong default retain for weekly policy")

	policy.Policy.Weekly.Retain = 20
	_, err = k8s.Instance().UpdateSchedulePolicy(policy)
	require.NoError(t, err, "Error updating schedule policy")
	retain, err = GetRetain(policyName, stork_api.SchedulePolicyTypeWeekly)
	require.NoError(t, err, "Error getting retain")
	require.Equal(t, policy.Policy.Weekly.Retain, retain, "Wrong default retain for weekly policy")

	retain, err = GetRetain(policyName, stork_api.SchedulePolicyTypeMonthly)
	require.NoError(t, err, "Error getting retain")
	require.Equal(t, stork_api.DefaultMonthlyPolicyRetain, retain, "Wrong default retain for monthly policy")
	policy.Policy.Monthly.Retain = 0
	_, err = k8s.Instance().UpdateSchedulePolicy(policy)
	require.NoError(t, err, "Error updating schedule policy")
	retain, err = GetRetain(policyName, stork_api.SchedulePolicyTypeMonthly)
	require.NoError(t, err, "Error getting retain")
	require.Equal(t, stork_api.DefaultMonthlyPolicyRetain, retain, "Wrong default retain for monthly policy")

	policy.Policy.Monthly.Retain = 30
	_, err = k8s.Instance().UpdateSchedulePolicy(policy)
	require.NoError(t, err, "Error updating schedule policy")
	retain, err = GetRetain(policyName, stork_api.SchedulePolicyTypeMonthly)
	require.NoError(t, err, "Error getting retain")
	require.Equal(t, policy.Policy.Monthly.Retain, retain, "Wrong default retain for monthly policy")
}
