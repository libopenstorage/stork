package v1alpha1

import (
	"fmt"
	"strings"
	"time"

	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	// SchedulePolicyResourceName is name for "schedulepolicy" resource
	SchedulePolicyResourceName = "schedulepolicy"
	// SchedulePolicyResourcePlural is plural for "schedulepolicy" resource
	SchedulePolicyResourcePlural = "schedulepolicies"
)

// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// SchedulePolicy represents a policy for executing actions on a schedule
type SchedulePolicy struct {
	meta.TypeMeta   `json:",inline"`
	meta.ObjectMeta `json:"metadata,omitempty"`
	// Policy
	Policy SchedulePolicyItem `json:"policy"`
}

// SchedulePolicyType is the type of schedule policy
type SchedulePolicyType string

const (
	// SchedulePolicyTypeInvalid is an invalid schedule policy
	SchedulePolicyTypeInvalid SchedulePolicyType = "Invalid"
	// SchedulePolicyTypeInterval is the type for an interval schedule policy
	SchedulePolicyTypeInterval SchedulePolicyType = "Interval"
	// SchedulePolicyTypeDaily is the type for a daily schedule policy
	SchedulePolicyTypeDaily SchedulePolicyType = "Daily"
	// SchedulePolicyTypeWeekly is the type for a weekly schedule policy
	SchedulePolicyTypeWeekly SchedulePolicyType = "Weekly"
	// SchedulePolicyTypeMonthly is the type for a monthly schedule policy
	SchedulePolicyTypeMonthly SchedulePolicyType = "Monthly"
)

// GetValidSchedulePolicyTypes returns the valid types of schedule policies that
// can be configured
func GetValidSchedulePolicyTypes() []SchedulePolicyType {
	return []SchedulePolicyType{SchedulePolicyTypeInterval, SchedulePolicyTypeDaily, SchedulePolicyTypeWeekly, SchedulePolicyTypeMonthly}
}

// Days is a map of valid Day strings
var Days = map[string]time.Weekday{
	"Sunday":    time.Sunday,
	"Sun":       time.Sunday,
	"Monday":    time.Monday,
	"Mon":       time.Monday,
	"Tuesday":   time.Tuesday,
	"Tue":       time.Tuesday,
	"Wednesday": time.Wednesday,
	"Wed":       time.Wednesday,
	"Thursday":  time.Thursday,
	"Thu":       time.Thursday,
	"Thur":      time.Thursday,
	"Thurs":     time.Thursday,
	"Friday":    time.Friday,
	"Fri":       time.Friday,
	"Saturday":  time.Saturday,
	"Sat":       time.Saturday,
}

// SchedulePolicyItem represents the schedule for executing an action
type SchedulePolicyItem struct {
	// Interval policy that will be triggered at the specified interval
	Interval *IntervalPolicy `json:"interval"`
	// Daily policy that will be triggered daily at a specified time
	Daily *DailyPolicy `json:"daily"`
	// Weekly policy that will be triggered on the specified day of the week at
	// the specified time
	Weekly *WeeklyPolicy `json:"weekly"`
	// Monthly policy that will be triggered on the specified date of the month
	// at the specified time
	Monthly *MonthlyPolicy `json:"monthly"`
}

// Retain Type to specify how many objects should be retained for a policy
type Retain int

// DefaultIntervalPolicyRetain Default for objects to be retained for the
// interval policy
const DefaultIntervalPolicyRetain = Retain(10)

// IntervalPolicy contains the interval at which an action should be triggered
type IntervalPolicy struct {
	IntervalMinutes int `json:"intervalMinutes"`
	// Retain Number of objects to retain for interval policy. Defaults to
	// @DefaultIntervalPolicyRetain
	Retain Retain `json:"retain"`
}

// Validate validates an IntervalPolicy
func (i *IntervalPolicy) Validate() error {
	if i.IntervalMinutes < 1 {
		return fmt.Errorf("Invalid intervalMinutes (%v) in Interval policy", i.IntervalMinutes)
	}
	return nil
}

// DefaultDailyPolicyRetain Default for objects to be retained for the daily
// policy
const DefaultDailyPolicyRetain = Retain(30)

// DailyPolicy contains the time in the day where an action should be executed
type DailyPolicy struct {
	// Time when the policy should be triggered. Expected format is
	// time.Kitchen eg 12:04PM or 12:04pm
	Time string `json:"time"`
	// Retain Number of objects to retain for daily policy. Defaults to
	// @DefaultDailyPolicyRetain
	Retain Retain `json:"retain"`
}

// GetHourMinute parses and return the hour and minute specified in the policy
func (d *DailyPolicy) GetHourMinute() (int, int, error) {
	return getHourMinute(d.Time)
}

// Validate validates a DailyPolicy
func (d *DailyPolicy) Validate() error {
	if _, _, err := d.GetHourMinute(); err != nil {
		return fmt.Errorf("Invalid time (%v) in Daily policy: %v", d.Time, err)
	}
	return nil
}

func getHourMinute(policyTime string) (int, int, error) {
	parsedTime, err := time.Parse(time.Kitchen, policyTime)
	if err != nil {
		parsedTime, err = time.Parse(strings.ToLower(time.Kitchen), policyTime)
		if err != nil {
			return 0, 0, err
		}
	}
	return parsedTime.Hour(), parsedTime.Minute(), nil
}

// DefaultWeeklyPolicyRetain Default for objects to be retained for the
// weekly policy
const DefaultWeeklyPolicyRetain = Retain(7)

// WeeklyPolicy contains the day and time in a week when an action should be
// executed
type WeeklyPolicy struct {
	// Day of the week when the policy should be triggered. Valid format are
	// specified in `Days` above
	Day string `json:"day"`
	// Time when the policy should be triggered. Expected format is
	// time.Kitchen eg 12:04PM or 12:04pm
	Time string `json:"time"`
	// Retain Number of objects to retain for weekly policy. Defaults to
	// @DefaultWeeklyPolicyRetain
	Retain Retain `json:"retain"`
}

// GetHourMinute parses and return the hour and minute specified in the policy
func (w *WeeklyPolicy) GetHourMinute() (int, int, error) {
	return getHourMinute(w.Time)
}

// Validate validates a WeeklyPolicy
func (w *WeeklyPolicy) Validate() error {
	if _, _, err := w.GetHourMinute(); err != nil {
		return fmt.Errorf("Invalid time (%v) in Weekly policy: %v", w.Time, err)
	}
	if _, present := Days[w.Day]; !present {
		return fmt.Errorf("Invalid day of the week (%v) in Weekly policy", w.Day)
	}
	return nil
}

// DefaultMonthlyPolicyRetain Default for objects to be retained for the
// monthly policy
const DefaultMonthlyPolicyRetain = Retain(12)

// MonthlyPolicy contains the date and time in a month when an action should be
// executed
type MonthlyPolicy struct {
	// Date of the month when the policy should be triggered. If a given date
	// doesn't exist in a month it'll rollover to the next date of the month.
	// For example if 31 is specified, it'll trigger on either 1st or 2nd March
	// depending on if it is a leap year.
	Date int `json:"date"`
	// Time when the policy should be triggered. Expected format is
	// time.Kitchen eg 12:04PM or 12:04pm
	Time string `json:"time"`
	// Retain Number of objects to retain for monthly policy. Defaults to
	// @DefaultMonthlyPolicyRetain
	Retain Retain `json:"retain"`
}

// GetHourMinute parses and return the hour and minute specified in the policy
func (m *MonthlyPolicy) GetHourMinute() (int, int, error) {
	return getHourMinute(m.Time)
}

// Validate validates a MonthlyPolicy
func (m *MonthlyPolicy) Validate() error {
	if _, _, err := m.GetHourMinute(); err != nil {
		return fmt.Errorf("Invalid time (%v) in Monthly policy: %v", m.Time, err)
	}
	if m.Date < 1 || m.Date > 31 {
		return fmt.Errorf("Invalid date of the month (%v) in Monthly policy", m.Date)
	}
	return nil
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// SchedulePolicyList is a list of schedule policies
type SchedulePolicyList struct {
	meta.TypeMeta `json:",inline"`
	meta.ListMeta `json:"metadata,omitempty"`

	Items []SchedulePolicy `json:"items"`
}
