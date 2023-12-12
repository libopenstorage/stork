package storkctl

import (
	"fmt"
	storkv1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/portworx/sched-ops/k8s/core"
	storkops "github.com/portworx/sched-ops/k8s/stork"
	"github.com/spf13/cobra"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	metav1beta1 "k8s.io/apimachinery/pkg/apis/meta/v1beta1"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/kubectl/pkg/cmd/util"
	"k8s.io/kubernetes/pkg/printers"
	"strings"
)

const (
	schedulePolicySubcommand  = "schedulepolicy"
	migrationSchedule         = "Migration Schedules"
	applicationBackupSchedule = "Application Backup Schedules"
	volumeSnapshotSchedule    = "Volume Snapshot Schedules"
)

var schedulePolicyColumns = []string{"NAME", "INTERVAL-MINUTES", "DAILY", "WEEKLY", "MONTHLY"}
var schedulePolicyAliases = []string{"sp"}

func newGetSchedulePolicyCommand(cmdFactory Factory, ioStreams genericclioptions.IOStreams) *cobra.Command {
	var err error
	getSchedulePolicyCommand := &cobra.Command{
		Use:     schedulePolicySubcommand,
		Aliases: schedulePolicyAliases,
		Short:   "Get schedule policies",
		Run: func(c *cobra.Command, args []string) {
			var schedulePolicies *storkv1.SchedulePolicyList
			if len(args) > 0 {
				schedulePolicies = new(storkv1.SchedulePolicyList)
				for _, policyName := range args {
					policy, err := storkops.Instance().GetSchedulePolicy(policyName)
					if err == nil {
						schedulePolicies.Items = append(schedulePolicies.Items, *policy)
					} else {
						util.CheckErr(err)
						return
					}
				}
			} else {
				schedulePolicies, err = storkops.Instance().ListSchedulePolicies()
				if err != nil {
					util.CheckErr(err)
					return
				}
			}

			if len(schedulePolicies.Items) == 0 {
				handleEmptyList(ioStreams.Out)
				return
			}

			if err := printObjects(c, schedulePolicies, cmdFactory, schedulePolicyColumns, schedulePolicyPrinter, ioStreams.Out); err != nil {
				util.CheckErr(err)
				return
			}
		},
	}

	return getSchedulePolicyCommand
}

func newCreateSchedulePolicyCommand(cmdFactory Factory, ioStreams genericclioptions.IOStreams) *cobra.Command {
	var schedulePolicyType string
	var intervalMinutes int
	var retain int
	var time string
	var dailyForceFullSnapshotDay string
	var dayOfWeek string
	var dateOfMonth int

	const (
		forceFullSnapshotDayFlag = "force-full-snapshot-day"
		intervalMinutesFlag      = "interval-minutes"
		timeFlag                 = "time"
		dayOfWeekFlag            = "day-of-week"
		dateOfMonthFlag          = "date-of-month"
	)

	createSchedulePolicyCommand := &cobra.Command{
		Use:     schedulePolicySubcommand,
		Aliases: schedulePolicyAliases,
		Short:   "Create schedule policy",
		Run: func(c *cobra.Command, args []string) {
			if len(args) != 1 {
				util.CheckErr(fmt.Errorf("exactly one name needs to be provided for schedule policy name"))
				return
			}
			schedulePolicyName := args[0]
			var policyItem storkv1.SchedulePolicyItem
			schedulePolicyType = strings.ToLower(schedulePolicyType)
			dayOfWeek = strings.ToLower(dayOfWeek)
			//Validate user input for retain
			if c.Flags().Changed("retain") && retain <= 0 {
				util.CheckErr(fmt.Errorf("need to provide a valid value for retain. It should be a positive integer"))
				return
			}
			switch schedulePolicyType {
			case "interval":
				if c.Flags().Changed(timeFlag) || c.Flags().Changed(dayOfWeekFlag) || c.Flags().Changed(dateOfMonthFlag) || c.Flags().Changed(forceFullSnapshotDayFlag) {
					util.CheckErr(fmt.Errorf("for an Interval SchedulePolicy you can only provide values for --interval-minutes and --retain flags"))
					return
				}
				var intervalPolicy storkv1.IntervalPolicy
				intervalPolicy.IntervalMinutes = intervalMinutes
				if retain == 0 {
					intervalPolicy.Retain = storkv1.DefaultIntervalPolicyRetain
				} else {
					intervalPolicy.Retain = storkv1.Retain(retain)
				}
				// Validate the user input
				err := intervalPolicy.Validate()
				if err != nil {
					util.CheckErr(err)
					return
				}
				policyItem.Interval = &intervalPolicy
			case "daily":
				if c.Flags().Changed(intervalMinutesFlag) || c.Flags().Changed(dayOfWeekFlag) || c.Flags().Changed(dateOfMonthFlag) {
					util.CheckErr(fmt.Errorf("for a Daily SchedulePolicy you can only provide values for --time, --retain and --force-full-snapshot-day flags"))
					return
				}
				var dailyPolicy storkv1.DailyPolicy
				dailyPolicy.Time = time
				dailyPolicy.ForceFullSnapshotDay = strings.ToLower(dailyForceFullSnapshotDay)
				//Validate ForceFullSnapshotDay is a valid weekday
				if _, present := storkv1.Days[dailyPolicy.ForceFullSnapshotDay]; !present {
					util.CheckErr(fmt.Errorf("invalid day of the week (%v) in policy.daily.forceFullSnapshotDay", dailyPolicy.ForceFullSnapshotDay))
					return
				}
				if retain == 0 {
					dailyPolicy.Retain = storkv1.DefaultDailyPolicyRetain
				} else {
					dailyPolicy.Retain = storkv1.Retain(retain)
				}
				err := dailyPolicy.Validate()
				if err != nil {
					util.CheckErr(err)
					return
				}
				policyItem.Daily = &dailyPolicy
			case "weekly":
				if c.Flags().Changed(intervalMinutesFlag) || c.Flags().Changed(forceFullSnapshotDayFlag) || c.Flags().Changed(dateOfMonthFlag) {
					util.CheckErr(fmt.Errorf("for a Weekly SchedulePolicy you can only provide values for --time, --day-of-week and --retain flags"))
					return
				}
				var weeklyPolicy storkv1.WeeklyPolicy
				weeklyPolicy.Time = time
				weeklyPolicy.Day = dayOfWeek
				if retain == 0 {
					weeklyPolicy.Retain = storkv1.DefaultWeeklyPolicyRetain
				} else {
					weeklyPolicy.Retain = storkv1.Retain(retain)
				}
				err := weeklyPolicy.Validate()
				if err != nil {
					util.CheckErr(err)
					return
				}
				policyItem.Weekly = &weeklyPolicy
			case "monthly":
				if c.Flags().Changed(intervalMinutesFlag) || c.Flags().Changed(forceFullSnapshotDayFlag) || c.Flags().Changed(dayOfWeekFlag) {
					util.CheckErr(fmt.Errorf("for a Monthly SchedulePolicy you can only provide values for --time, --date-of-month and --retain flags"))
					return
				}
				var monthlyPolicy storkv1.MonthlyPolicy
				monthlyPolicy.Time = time
				monthlyPolicy.Date = dateOfMonth
				if retain == 0 {
					monthlyPolicy.Retain = storkv1.DefaultMonthlyPolicyRetain
				} else {
					monthlyPolicy.Retain = storkv1.Retain(retain)
				}
				err := monthlyPolicy.Validate()
				if err != nil {
					util.CheckErr(err)
					return
				}
				policyItem.Monthly = &monthlyPolicy
			default:
				util.CheckErr(fmt.Errorf("need to provide a valid schedule policy type. Valid schedule types are Interval, Daily, Weekly and Monthly"))
				return
			}
			var schedulePolicy storkv1.SchedulePolicy
			schedulePolicy.Name = schedulePolicyName
			schedulePolicy.Policy = policyItem
			_, err := storkops.Instance().CreateSchedulePolicy(&schedulePolicy)
			if err != nil {
				util.CheckErr(err)
				return
			}
			msg := fmt.Sprintf("Schedule policy %v created successfully", schedulePolicy.Name)
			printMsg(msg, ioStreams.Out)
		},
	}
	// Picking up user inputs and setting the defaults for flags
	createSchedulePolicyCommand.Flags().StringVarP(&schedulePolicyType, "policy-type", "t", "Interval", "Select Type of schedule policy to apply. Interval / Daily / Weekly / Monthly.")
	createSchedulePolicyCommand.Flags().IntVarP(&intervalMinutes, intervalMinutesFlag, "i", 30, "Specify the interval, in minutes, after which Stork should trigger the operation.")
	createSchedulePolicyCommand.Flags().IntVarP(&retain, "retain", "", 0, "For backup operations,specify how many backups triggered as part of this schedule should be retained.")
	createSchedulePolicyCommand.Flags().StringVarP(&time, timeFlag, "", "12:00AM", "Specify the time of the day in the 12 hour AM/PM format, when Stork should trigger the operation.")
	createSchedulePolicyCommand.Flags().StringVarP(&dailyForceFullSnapshotDay, forceFullSnapshotDayFlag, "", "Monday", "For daily scheduled backup operations, specify on which day to trigger a full backup.")
	createSchedulePolicyCommand.Flags().StringVarP(&dayOfWeek, dayOfWeekFlag, "", "Sunday", "Specify the day of the week when Stork should trigger the operation. You can use both the abbreviated or the full name of the day of the week.")
	createSchedulePolicyCommand.Flags().IntVarP(&dateOfMonth, dateOfMonthFlag, "", 1, "Specify the date of the month when Stork should trigger the operation.")
	return createSchedulePolicyCommand
}

func newDeleteSchedulePolicyCommand(cmdFactory Factory, ioStreams genericclioptions.IOStreams) *cobra.Command {
	deleteSchedulePolicyCommand := &cobra.Command{
		Use:     schedulePolicySubcommand,
		Aliases: schedulePolicyAliases,
		Short:   "Delete schedule policies",
		Run: func(c *cobra.Command, args []string) {
			if len(args) == 0 {
				util.CheckErr(fmt.Errorf("at least one argument needs to be provided for schedule policy name"))
				return
			}

			namespaces, err := core.Instance().ListNamespaces(nil)
			if err != nil {
				util.CheckErr(err)
				return
			}
			for i := 0; i < len(args); i++ {
				// ensure that the schedule policy is not being used by any stork resource
				scheduleTypes := []string{migrationSchedule, applicationBackupSchedule, volumeSnapshotSchedule}
				policyLinkedTo, err := isSchedulePolicyBeingUsed(scheduleTypes, args[i], namespaces)
				if err != nil {
					util.CheckErr(fmt.Errorf("cannot delete the schedule policy %s. Unable to verify if the schedule policy is linked to other resources. %w", args[i], err))
					return
				}
				if len(policyLinkedTo[migrationSchedule])+
					len(policyLinkedTo[applicationBackupSchedule])+
					len(policyLinkedTo[volumeSnapshotSchedule]) != 0 {
					// Print the names of resources linked to the given schedule policy
					message := "\nThe resource is linked to -> "
					for _, scheduleType := range scheduleTypes {
						schedules := policyLinkedTo[scheduleType]
						if len(schedules) > 0 {
							message += scheduleType + " : "
							message += strings.Join(schedules, " , ")
							message += "\n"
						}
					}
					util.CheckErr(fmt.Errorf("cannot delete the schedule policy %s.%s", args[i], message))
					return
				}
				// Now we can try to delete the schedule policy since it's not linked to any other resource
				err = storkops.Instance().DeleteSchedulePolicy(args[i])
				if err != nil {
					util.CheckErr(err)
					return
				}
				msg := fmt.Sprintf("Schedule policy %v deleted successfully", args[i])
				printMsg(msg, ioStreams.Out)
			}
		},
	}
	return deleteSchedulePolicyCommand
}

func isSchedulePolicyBeingUsed(scheduleTypes []string, policyName string, namespaces *v1.NamespaceList) (map[string][]string, error) {
	policyInUseBy := make(map[string][]string)
	for _, scheduleType := range scheduleTypes {
		policyInUseBy[scheduleType] = []string{}
	}

	for _, ns := range namespaces.Items {
		// Checking all the migration schedules for schedulePolicyName
		migrationSchedules, err := storkops.Instance().ListMigrationSchedules(ns.Name)
		if err != nil {
			return nil, err
		}
		for _, schedule := range migrationSchedules.Items {
			if schedule.Spec.SchedulePolicyName == policyName {
				scheduleName := ns.Name + "/" + schedule.Name
				policyInUseBy[migrationSchedule] = append(policyInUseBy[migrationSchedule], scheduleName)
			}
		}

		// Checking all applicationBackupSchedules for schedulePolicyName
		applicationBackupSchedules, err := storkops.Instance().ListApplicationBackupSchedules(ns.Name, metav1.ListOptions{})
		if err != nil {
			return nil, err
		}
		for _, schedule := range applicationBackupSchedules.Items {
			if schedule.Spec.SchedulePolicyName == policyName {
				scheduleName := ns.Name + "/" + schedule.Name
				policyInUseBy[applicationBackupSchedule] = append(policyInUseBy[applicationBackupSchedule], scheduleName)
			}
		}

		// Checking all VolumeSnapshotSchedules for schedulePolicyName
		snapshotSchedules, err := storkops.Instance().ListSnapshotSchedules(ns.Name)
		if err != nil {
			return nil, err
		}
		for _, schedule := range snapshotSchedules.Items {
			if schedule.Spec.SchedulePolicyName == policyName {
				scheduleName := ns.Name + "/" + schedule.Name
				policyInUseBy[volumeSnapshotSchedule] = append(policyInUseBy[volumeSnapshotSchedule], scheduleName)
			}
		}
	}
	return policyInUseBy, nil
}

func schedulePolicyPrinter(
	schedulePolicyList *storkv1.SchedulePolicyList,
	options printers.GenerateOptions,
) ([]metav1beta1.TableRow, error) {
	const notConfiguredString = "N/A"
	const invalidString = "Invalid"

	if schedulePolicyList == nil {
		return nil, nil
	}

	rows := make([]metav1beta1.TableRow, 0)
	for _, schedulePolicy := range schedulePolicyList.Items {
		interval := notConfiguredString
		daily := notConfiguredString
		weekly := notConfiguredString
		monthly := notConfiguredString
		if schedulePolicy.Policy.Interval != nil {
			if schedulePolicy.Policy.Interval.Validate() == nil {
				interval = fmt.Sprintf("%v", schedulePolicy.Policy.Interval.IntervalMinutes)
			} else {
				interval = invalidString
			}
		}
		if schedulePolicy.Policy.Daily != nil {
			if schedulePolicy.Policy.Daily.Validate() == nil {
				daily = schedulePolicy.Policy.Daily.Time
			} else {
				daily = invalidString
			}
		}

		if schedulePolicy.Policy.Weekly != nil {
			if schedulePolicy.Policy.Weekly.Validate() == nil {
				weekly = fmt.Sprintf("%v@%v", schedulePolicy.Policy.Weekly.Day, schedulePolicy.Policy.Weekly.Time)
			} else {
				weekly = invalidString
			}
		}

		if schedulePolicy.Policy.Monthly != nil {
			if schedulePolicy.Policy.Monthly.Validate() == nil {
				monthly = fmt.Sprintf("%v@%v", schedulePolicy.Policy.Monthly.Date, schedulePolicy.Policy.Monthly.Time)
			} else {
				monthly = "Invalid"
			}
		}

		row := getRow(&schedulePolicy,
			[]interface{}{schedulePolicy.Name,
				interval,
				daily,
				weekly,
				monthly},
		)
		rows = append(rows, row)
	}
	return rows, nil
}
