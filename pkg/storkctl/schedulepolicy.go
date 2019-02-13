package storkctl

import (
	"fmt"
	"io"

	storkv1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/portworx/sched-ops/k8s"
	"github.com/spf13/cobra"
	"k8s.io/kubernetes/pkg/kubectl/cmd/util"
	"k8s.io/kubernetes/pkg/kubectl/genericclioptions"
	"k8s.io/kubernetes/pkg/printers"
)

const (
	schedulePolicySubcommand = "schedulepolicy"
)

var schedulePolicyColumns = []string{"NAME", "INTERVAL-MINUTES", "DAILY", "WEEKLY", "MONTHLY"}

func newGetSchedulePolicyCommand(cmdFactory Factory, ioStreams genericclioptions.IOStreams) *cobra.Command {
	var err error
	getSchedulePolicyCommand := &cobra.Command{
		Use:     schedulePolicySubcommand,
		Aliases: []string{"sp"},
		Short:   "Get schedule policies",
		Run: func(c *cobra.Command, args []string) {
			var schedulePolicies *storkv1.SchedulePolicyList
			if len(args) > 0 {
				schedulePolicies = new(storkv1.SchedulePolicyList)
				for _, pairName := range args {
					pair, err := k8s.Instance().GetSchedulePolicy(pairName)
					if err == nil {
						schedulePolicies.Items = append(schedulePolicies.Items, *pair)
					} else {
						util.CheckErr(err)
						return
					}
				}
			} else {
				schedulePolicies, err = k8s.Instance().ListSchedulePolicies()
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

func schedulePolicyPrinter(schedulePolicyList *storkv1.SchedulePolicyList, writer io.Writer, options printers.PrintOptions) error {
	const notConfiguredString = "N/A"
	const invalidString = "Invalid"
	if schedulePolicyList == nil {
		return nil
	}
	for _, schedulePolicy := range schedulePolicyList.Items {
		name := printers.FormatResourceName(options.Kind, schedulePolicy.Name, options.WithKind)

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

		if _, err := fmt.Fprintf(writer, "%v\t%v\t%v\t%v\t%v\n",
			name,
			interval,
			daily,
			weekly,
			monthly,
		); err != nil {
			return err
		}
	}
	return nil
}
