package schedule

import (
	"fmt"
	"os"
	"reflect"
	"time"

	stork_api "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/libopenstorage/stork/pkg/k8sutils"
	"github.com/libopenstorage/stork/pkg/utils"
	"github.com/libopenstorage/stork/pkg/version"
	"github.com/portworx/sched-ops/k8s/apiextensions"
	"github.com/portworx/sched-ops/k8s/core"
	storkops "github.com/portworx/sched-ops/k8s/stork"
	"github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	apiextensionsv1beta1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1beta1"
	"k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

const (
	validateCRDInterval time.Duration = 5 * time.Second
	validateCRDTimeout  time.Duration = 1 * time.Minute
	// storkTestModeEnvVariable is env variable to enable test mode features in stork
	storkTestModeEnvVariable = "TEST_MODE"
	// MockTimeConfigMapName is the name of the config map used to mock times
	MockTimeConfigMapName = "stork-mock-time"
	// MockTimeConfigMapNamespace is the namespace of the config map used to mock times
	MockTimeConfigMapNamespace = "kube-system"
	// MockTimeConfigMapKey is the key name in the config map data that contains the time
	MockTimeConfigMapKey = "time"
)

var mockTime *time.Time

// setMockTime is used in tests to update the time
func setMockTime(mt *time.Time) {
	mockTime = mt
}

// GetCurrentTime returns the current time as per the scheduler
func GetCurrentTime() time.Time {
	if mockTime != nil {
		return *mockTime
	}
	return time.Now()
}

// TriggerRequired Check if a trigger is required for a policy given the last
// trigger time
func TriggerRequired(
	policyName string,
	namespace string,
	policyType stork_api.SchedulePolicyType,
	lastTrigger meta.Time,
) (bool, error) {
	schedulePolicy, err := getSchedulePolicy(policyName, namespace)
	if err != nil {
		return false, err
	}

	if err := ValidateSchedulePolicy(schedulePolicy); err != nil {
		return false, err
	}

	now := GetCurrentTime()
	switch policyType {
	case stork_api.SchedulePolicyTypeInterval:
		if schedulePolicy.Policy.Interval == nil {
			return false, nil
		}
		duration := time.Duration(schedulePolicy.Policy.Interval.IntervalMinutes) * time.Minute
		// Trigger if more than intervalMinutes has passed since
		// last trigger
		if lastTrigger.Add(duration).Before(now) {
			return true, nil
		}
		return false, nil

	case stork_api.SchedulePolicyTypeDaily:
		if schedulePolicy.Policy.Daily == nil {
			return false, nil
		}

		policyHour, policyMinute, err := schedulePolicy.Policy.Daily.GetHourMinute()
		if err != nil {
			return false, err
		}

		nextTrigger := time.Date(now.Year(), now.Month(), now.Day(), policyHour, policyMinute, 0, 0, time.Local)

		return checkTrigger(lastTrigger.Time, nextTrigger, now)

	case stork_api.SchedulePolicyTypeWeekly:
		if schedulePolicy.Policy.Weekly == nil {
			return false, nil
		}
		currentDay := now.Weekday()
		scheduledDay := stork_api.Days[schedulePolicy.Policy.Weekly.Day]
		policyHour, policyMinute, err := schedulePolicy.Policy.Weekly.GetHourMinute()
		if err != nil {
			return false, err
		}
		nextTrigger := time.Date(now.Year(), now.Month(), now.Day(), policyHour, policyMinute, 0, 0, time.Local)
		// Figure out how many days to add to get to the next
		// trigger week day
		if currentDay < scheduledDay {
			nextTrigger = nextTrigger.Add(time.Hour * time.Duration((scheduledDay-currentDay)*24))
		} else if currentDay > scheduledDay {
			nextTrigger = nextTrigger.Add(time.Duration((7-(currentDay-scheduledDay))*24) * time.Hour)
		}

		return checkTrigger(lastTrigger.Time, nextTrigger, now)
	case stork_api.SchedulePolicyTypeMonthly:
		if schedulePolicy.Policy.Monthly == nil {
			return false, nil
		}
		policyHour, policyMinute, err := schedulePolicy.Policy.Monthly.GetHourMinute()
		if err != nil {
			return false, err
		}
		nextTrigger := time.Date(now.Year(), now.Month(), schedulePolicy.Policy.Monthly.Date, policyHour, policyMinute, 0, 0, time.Local)

		return checkTrigger(lastTrigger.Time, nextTrigger, now)
	}
	return false, nil
}

func checkTrigger(
	lastTrigger time.Time,
	nextTrigger time.Time,
	now time.Time,
) (bool, error) {
	// If we had triggered after the scheduled time this month, don't
	// triggered again
	if lastTrigger.After(nextTrigger) || lastTrigger.Equal(nextTrigger) {
		return false, nil
	}

	// If we are within one hour after/at the next trigger time, trigger a new
	// schedule
	if now.Equal(nextTrigger) || (now.After(nextTrigger) && now.Sub(nextTrigger).Hours() < 1) {
		return true, nil
	}
	return false, nil
}

func hasTriggered(
	lastTrigger time.Time,
	lastTriggerEarliest time.Time,
	now time.Time,
) (bool, error) {
	// Check if the last trigger falls between the allowed earliest trigger time and now
	if lastTrigger.Before(lastTriggerEarliest) || lastTrigger.After(now) {
		return false, nil
	}
	return true, nil
}

func AlreadyTriggered(
	policyName string,
	namespace string,
	policyType stork_api.SchedulePolicyType,
	lastTrigger meta.Time,
) (bool, error) {
	schedulePolicy, err := getSchedulePolicy(policyName, namespace)
	if err != nil {
		return false, err
	}

	if err := ValidateSchedulePolicy(schedulePolicy); err != nil {
		return false, err
	}

	now := GetCurrentTime()
	switch policyType {
	case stork_api.SchedulePolicyTypeInterval:
		if schedulePolicy.Policy.Interval == nil {
			return false, nil
		}
		duration := time.Duration(schedulePolicy.Policy.Interval.IntervalMinutes) * time.Minute
		// Triggerred if last trigger is within the duration from now
		earliestTrigger := now.Add(-duration)
		return hasTriggered(lastTrigger.Time, earliestTrigger, now)

	case stork_api.SchedulePolicyTypeDaily:
		if schedulePolicy.Policy.Daily == nil {
			return false, nil
		}

		policyHour, policyMinute, err := schedulePolicy.Policy.Daily.GetHourMinute()
		if err != nil {
			return false, err
		}

		nextTrigger := time.Date(now.Year(), now.Month(), now.Day(), policyHour, policyMinute, 0, 0, time.Local)
		earliestTrigger := nextTrigger.AddDate(0, 0, -1)
		return hasTriggered(lastTrigger.Time, earliestTrigger, now)

	case stork_api.SchedulePolicyTypeWeekly:
		if schedulePolicy.Policy.Weekly == nil {
			return false, nil
		}
		policyHour, policyMinute, err := schedulePolicy.Policy.Weekly.GetHourMinute()
		if err != nil {
			return false, err
		}
		nextTrigger := time.Date(now.Year(), now.Month(), now.Day(), policyHour, policyMinute, 0, 0, time.Local)
		earliestTrigger := nextTrigger.AddDate(0, 0, -7)

		return hasTriggered(lastTrigger.Time, earliestTrigger, now)

	case stork_api.SchedulePolicyTypeMonthly:
		if schedulePolicy.Policy.Monthly == nil {
			return false, nil
		}
		policyHour, policyMinute, err := schedulePolicy.Policy.Monthly.GetHourMinute()
		if err != nil {
			return false, err
		}
		nextTrigger := time.Date(now.Year(), now.Month(), schedulePolicy.Policy.Monthly.Date, policyHour, policyMinute, 0, 0, time.Local)
		earliestTrigger := nextTrigger.AddDate(0, -1, 0)
		return hasTriggered(lastTrigger.Time, earliestTrigger, now)

	}
	return false, nil
}

// ValidateSchedulePolicy Validate if a given schedule policy is valid
func ValidateSchedulePolicy(policy *stork_api.SchedulePolicy) error {
	if policy == nil {
		return nil
	}

	if policy.Policy.Interval != nil {
		if err := policy.Policy.Interval.Validate(); err != nil {
			return err
		}
	}
	if policy.Policy.Daily != nil {
		if err := policy.Policy.Daily.Validate(); err != nil {
			return err
		}
	}
	if policy.Policy.Weekly != nil {
		if err := policy.Policy.Weekly.Validate(); err != nil {
			return err
		}
	}
	if policy.Policy.Monthly != nil {
		if err := policy.Policy.Monthly.Validate(); err != nil {
			return err
		}
	}
	return nil
}

// GetRetain Returns the retain value for the specified policy. Returns the
// default for the policy if none is specified
func GetRetain(policyName string, namespace string, policyType stork_api.SchedulePolicyType) (stork_api.Retain, error) {
	schedulePolicy, err := getSchedulePolicy(policyName, namespace)
	if err != nil {
		return 0, err
	}
	switch policyType {
	case stork_api.SchedulePolicyTypeInterval:
		if schedulePolicy.Policy.Interval != nil {
			if schedulePolicy.Policy.Interval.Retain == 0 {
				return stork_api.DefaultIntervalPolicyRetain, nil
			}
			return schedulePolicy.Policy.Interval.Retain, nil
		}
	case stork_api.SchedulePolicyTypeDaily:
		if schedulePolicy.Policy.Daily != nil {
			if schedulePolicy.Policy.Daily.Retain == 0 {
				return stork_api.DefaultDailyPolicyRetain, nil
			}
			return schedulePolicy.Policy.Daily.Retain, nil
		}
	case stork_api.SchedulePolicyTypeWeekly:
		if schedulePolicy.Policy.Weekly != nil {
			if schedulePolicy.Policy.Weekly.Retain == 0 {
				return stork_api.DefaultWeeklyPolicyRetain, nil
			}
			return schedulePolicy.Policy.Weekly.Retain, nil
		}
	case stork_api.SchedulePolicyTypeMonthly:
		if schedulePolicy.Policy.Monthly != nil {
			if schedulePolicy.Policy.Monthly.Retain == 0 {
				return stork_api.DefaultMonthlyPolicyRetain, nil
			}
			return schedulePolicy.Policy.Monthly.Retain, nil
		}
	default:
		return 0, fmt.Errorf("invalid policy type: %v", policyType)
	}

	return 1, nil
}

// GetOptions Returns the options set for a policy type
func GetOptions(policyName string, namespace string, policyType stork_api.SchedulePolicyType) (map[string]string, error) {
	schedulePolicy, err := getSchedulePolicy(policyName, namespace)
	if err != nil {
		return nil, err
	}
	switch policyType {
	case stork_api.SchedulePolicyTypeInterval:
		return schedulePolicy.Policy.Interval.Options, nil
	case stork_api.SchedulePolicyTypeDaily:
		options := schedulePolicy.Policy.Daily.Options
		if len(options) == 0 {
			options = make(map[string]string)
		}
		scheduledDay, ok := stork_api.Days[schedulePolicy.Policy.Daily.ForceFullSnapshotDay]
		if ok {
			currentDay := GetCurrentTime().Weekday()
			// force full backup on specified day
			if currentDay == scheduledDay {
				options[utils.PXIncrementalCountAnnotation] = "0"
			}
			logrus.Infof("Forcing full-snapshot for daily snapshotschedule policy on the day %s", schedulePolicy.Policy.Daily.ForceFullSnapshotDay)
		}
		return options, nil
	case stork_api.SchedulePolicyTypeWeekly:
		return schedulePolicy.Policy.Weekly.Options, nil
	case stork_api.SchedulePolicyTypeMonthly:
		return schedulePolicy.Policy.Monthly.Options, nil
	default:
		return nil, fmt.Errorf("invalid policy type: %v", policyType)
	}
}

// Init initializes the schedule module
func Init() error {
	err := createCRD()
	if err != nil {
		return err
	}

	testMode := os.Getenv(storkTestModeEnvVariable)
	if testMode == "true" {

		fn := func(object runtime.Object) error {
			cm, ok := object.(*v1.ConfigMap)
			if !ok {
				err := fmt.Errorf("invalid object type on configmap watch: %v", object)
				return err
			}

			if len(cm.Data) > 0 {
				timeString := cm.Data[MockTimeConfigMapKey]
				if len(timeString) > 0 {
					t, err := time.Parse(time.RFC1123, timeString)
					if err != nil {
						err = fmt.Errorf("failed to parse time in mock config map due to: %v", err)
						logrus.Errorf(err.Error())
						return err
					}

					logrus.Infof("Setting mock time to: %v current time: %v", t, GetCurrentTime())
					setMockTime(&t)
				} else {
					logrus.Infof("Time string is empty. Resetting mock time")
					setMockTime(nil)
				}
			}

			return nil
		}

		logrus.Infof("Stork test mode enabled. Watching for config map: %s for mock times", MockTimeConfigMapName)
		cm, err := core.Instance().GetConfigMap(MockTimeConfigMapName, MockTimeConfigMapNamespace)
		if err != nil {
			if errors.IsNotFound(err) {
				logrus.Infof("Stork in test mode, however no config map present to mock time. Creating it.")
				// create new config map
				data := map[string]string{
					MockTimeConfigMapKey: "",
				}

				cm = &v1.ConfigMap{
					ObjectMeta: meta.ObjectMeta{
						Name:      MockTimeConfigMapName,
						Namespace: MockTimeConfigMapNamespace,
					},
					Data: data,
				}

				cm, err = core.Instance().CreateConfigMap(cm)
				if err != nil {
					return err
				}
			} else {
				logrus.Errorf("Failed to get config map: %s due to: %v", MockTimeConfigMapName, err)
				return err
			}
		}

		cm = cm.DeepCopy()

		err = core.Instance().WatchConfigMap(cm, fn)
		if err != nil {
			logrus.Errorf("Failed to watch on config map: %s due to: %v", MockTimeConfigMapName, err)
			return err
		}
	}

	if err := startSchedulePolicyCache(); err != nil {
		return err
	}
	return createDefaultPolicy()
}

func createDefaultPolicy() error {
	_, err := storkops.Instance().CreateSchedulePolicy(&stork_api.SchedulePolicy{
		ObjectMeta: meta.ObjectMeta{
			Name: "default-migration-policy",
		},
		Policy: stork_api.SchedulePolicyItem{
			Interval: &stork_api.IntervalPolicy{
				IntervalMinutes: 30,
			}},
	})
	if err != nil {
		if errors.IsAlreadyExists(err) {
			// Modify IntervalMinutes to 30
			schedulePolicy, err := storkops.Instance().GetSchedulePolicy("default-migration-policy")
			if err != nil {
				return err
			}
			// Only if the interval minutes is 1 (means no update has happened to the default policy by user), update it to 30.
			if schedulePolicy.Policy.Interval.IntervalMinutes == 1 {
				schedulePolicy.Policy.Interval.IntervalMinutes = 30
				_, err := storkops.Instance().UpdateSchedulePolicy(schedulePolicy)
				if err != nil {
					return err
				}
			}
		} else {
			return err
		}
	}
	_, err = storkops.Instance().CreateSchedulePolicy(&stork_api.SchedulePolicy{
		ObjectMeta: meta.ObjectMeta{
			Name: "default-interval-policy",
		},
		Policy: stork_api.SchedulePolicyItem{
			Interval: &stork_api.IntervalPolicy{
				IntervalMinutes: 15,
				Retain:          10,
			}},
	})
	if err != nil && !errors.IsAlreadyExists(err) {
		return err
	}
	_, err = storkops.Instance().CreateSchedulePolicy(&stork_api.SchedulePolicy{
		ObjectMeta: meta.ObjectMeta{
			Name: "default-daily-policy",
		},
		Policy: stork_api.SchedulePolicyItem{
			Daily: &stork_api.DailyPolicy{
				Time:   "12:00am",
				Retain: 7,
			}},
	})
	if err != nil && !errors.IsAlreadyExists(err) {
		return err
	}
	_, err = storkops.Instance().CreateSchedulePolicy(&stork_api.SchedulePolicy{
		ObjectMeta: meta.ObjectMeta{
			Name: "default-weekly-policy",
		},
		Policy: stork_api.SchedulePolicyItem{
			Weekly: &stork_api.WeeklyPolicy{
				Day:    "Sunday",
				Time:   "12:00am",
				Retain: 4,
			}},
	})
	if err != nil && !errors.IsAlreadyExists(err) {
		return err
	}

	_, err = storkops.Instance().CreateSchedulePolicy(&stork_api.SchedulePolicy{
		ObjectMeta: meta.ObjectMeta{
			Name: "default-monthly-policy",
		},
		Policy: stork_api.SchedulePolicyItem{
			Monthly: &stork_api.MonthlyPolicy{
				Date:   15,
				Time:   "12:00am",
				Retain: 12,
			}},
	})
	if err != nil && !errors.IsAlreadyExists(err) {
		return err
	}
	return nil
}

func createCRD() error {
	resource := apiextensions.CustomResource{
		Name:    stork_api.SchedulePolicyResourceName,
		Plural:  stork_api.SchedulePolicyResourcePlural,
		Group:   stork_api.SchemeGroupVersion.Group,
		Version: stork_api.SchemeGroupVersion.Version,
		Scope:   apiextensionsv1beta1.ClusterScoped,
		Kind:    reflect.TypeOf(stork_api.SchedulePolicy{}).Name(),
	}
	policy := apiextensions.CustomResource{
		Name:    stork_api.NamespacedSchedulePolicyResourceName,
		Plural:  stork_api.NamespacedSchedulePolicyResourcePlural,
		Group:   stork_api.SchemeGroupVersion.Group,
		Version: stork_api.SchemeGroupVersion.Version,
		Scope:   apiextensionsv1beta1.NamespaceScoped,
		Kind:    reflect.TypeOf(stork_api.NamespacedSchedulePolicy{}).Name(),
	}

	ok, err := version.RequiresV1Registration()
	if err != nil {
		return err
	}
	if ok {
		err := k8sutils.CreateCRDV1(resource)
		if err != nil && !errors.IsAlreadyExists(err) {
			return err
		}
		if err := apiextensions.Instance().ValidateCRD(resource.Plural+"."+resource.Group, validateCRDTimeout, validateCRDInterval); err != nil {
			return err
		}
		err = k8sutils.CreateCRDV1(policy)
		if err != nil && !errors.IsAlreadyExists(err) {
			return err
		}
		return apiextensions.Instance().ValidateCRD(policy.Plural+"."+policy.Group, validateCRDTimeout, validateCRDInterval)
	}
	err = apiextensions.Instance().CreateCRDV1beta1(resource)
	if err != nil && !errors.IsAlreadyExists(err) {
		return err
	}
	if err := apiextensions.Instance().ValidateCRDV1beta1(resource, validateCRDTimeout, validateCRDInterval); err != nil {
		return err
	}
	err = apiextensions.Instance().CreateCRDV1beta1(policy)
	if err != nil && !errors.IsAlreadyExists(err) {
		return err
	}
	return apiextensions.Instance().ValidateCRDV1beta1(policy, validateCRDTimeout, validateCRDInterval)
}
