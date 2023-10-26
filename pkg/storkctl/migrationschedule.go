package storkctl

import (
	"fmt"
	"time"

	storkv1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	storkops "github.com/portworx/sched-ops/k8s/stork"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	metav1beta1 "k8s.io/apimachinery/pkg/apis/meta/v1beta1"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/kubectl/pkg/cmd/util"
	"k8s.io/kubernetes/pkg/printers"
)

var migrationScheduleColumns = []string{"NAME", "POLICYNAME", "CLUSTERPAIR", "SUSPEND", "LAST-SUCCESS-TIME", "LAST-SUCCESS-DURATION"}
var migrationScheduleSubcommand = "migrationschedules"
var migrationScheduleAliases = []string{"migrationschedule"}

func newCreateMigrationScheduleCommand(cmdFactory Factory, ioStreams genericclioptions.IOStreams) *cobra.Command {
	var migrationScheduleName string
	var clusterPair string
	var namespaceList []string
	var excludeResources bool
	var excludeVolumes bool
	var startApplications bool
	var preExecRule string
	var postExecRule string
	var schedulePolicyName string
	var suspend bool
	var disableAutoSuspend bool
	var intervalMinutes int
	var annotations map[string]string

	createMigrationScheduleCommand := &cobra.Command{
		Use:     migrationScheduleSubcommand,
		Aliases: migrationScheduleAliases,
		Short:   "Create a migration schedule",
		Run: func(c *cobra.Command, args []string) {
			// Since we store the opposite of the boolean flag's captured values in our CR definition.
			var autoSuspend = !disableAutoSuspend
			var includeResources = !excludeResources
			var includeVolumes = !excludeVolumes

			if len(args) != 1 {
				util.CheckErr(fmt.Errorf("exactly one name needs to be provided for migration schedule name"))
				return
			}
			migrationScheduleName = args[0]
			if len(clusterPair) == 0 {
				util.CheckErr(fmt.Errorf("ClusterPair name needs to be provided for migration schedule"))
				return
			}

			//Identify whether it is a Sync-Dr or Async-Dr use-case from the ClusterPair spec
			migrationScheduleNs := cmdFactory.GetNamespace()
			clusterPairObj, err := storkops.Instance().GetClusterPair(clusterPair, migrationScheduleNs)
			if err != nil {
				util.CheckErr(fmt.Errorf("unable to find the cluster pair in the given namespace"))
				return
			}
			isSyncDr := false
			//clusterPairObj.Spec.Options map is empty for syncDr and non-empty for asyncDR
			if len(clusterPairObj.Spec.Options) == 0 {
				isSyncDr = true
			}

			// Default value of includeVolumes for syncDr is false and for async DR is true
			if isSyncDr {
				if c.Flags().Changed("exclude-volumes") && includeVolumes {
					util.CheckErr(fmt.Errorf("the flag exclude-volumes can only be set to true in case of a sync-dr usecase as there is a single stretched cluster from storage perspective"))
					return
				}
				includeVolumes = false
			}

			if len(namespaceList) == 0 {
				util.CheckErr(fmt.Errorf("need to provide atleast one namespace to migrate"))
				return
			}

			//There are 4 possible cases for schedulePolicyName and interval flags
			//user provides both schedulePolicyName and interval value -> we throw an error saying you can only fill one of the values
			//user provides interval value only -> we will create a new schedule policy and use that in the migrationSchedule
			//user provides schedulePolicyName only -> we will check if such a schedule policy exists and if yes use that schedulePolicy.
			//user doesn't provide schedulePolicyName nor interval value -> we go ahead with the "default-migration-policy"

			if c.Flags().Changed("schedule-policy-name") && c.Flags().Changed("interval") {
				util.CheckErr(fmt.Errorf("must provide only one of schedule-policy-name or interval values"))
				return
			}

			if c.Flags().Changed("interval") {
				var policyItem storkv1.SchedulePolicyItem
				var intervalPolicy storkv1.IntervalPolicy
				intervalPolicy.IntervalMinutes = intervalMinutes
				intervalPolicy.Retain = storkv1.DefaultIntervalPolicyRetain
				// Validate the user input
				err := intervalPolicy.Validate()
				if err != nil {
					util.CheckErr(fmt.Errorf("could not create a schedule policy with specified interval. %v", err))
					return
				}
				policyItem.Interval = &intervalPolicy
				//name of the schedule policy created will be same as the migration schedule name provided in the input.
				schedulePolicyName = migrationScheduleName
				var schedulePolicy storkv1.SchedulePolicy
				schedulePolicy.Name = schedulePolicyName
				schedulePolicy.Policy = policyItem
				_, err = storkops.Instance().CreateSchedulePolicy(&schedulePolicy)
				if err != nil {
					util.CheckErr(fmt.Errorf("could not create a schedule policy with specified interval. %v", err))
					return
				}
			} else {
				// case 3 and 4 are covered in this branch
				logrus.Infof("schedulePolicy name is %s", schedulePolicyName)
				_, err := storkops.Instance().GetSchedulePolicy(schedulePolicyName)
				if err != nil {
					util.CheckErr(fmt.Errorf("unable to get schedulepolicy %v: %v", schedulePolicyName, err))
					return
				}
			}

			migrationSchedule := &storkv1.MigrationSchedule{
				ObjectMeta: meta.ObjectMeta{Annotations: annotations},
				Spec: storkv1.MigrationScheduleSpec{
					Template: storkv1.MigrationTemplateSpec{
						Spec: storkv1.MigrationSpec{
							ClusterPair:       clusterPair,
							Namespaces:        namespaceList,
							IncludeResources:  &includeResources,
							IncludeVolumes:    &includeVolumes,
							StartApplications: &startApplications,
							PreExecRule:       preExecRule,
							PostExecRule:      postExecRule,
						},
					},
					SchedulePolicyName: schedulePolicyName,
					Suspend:            &suspend,
					AutoSuspend:        autoSuspend,
				},
			}
			migrationSchedule.Name = migrationScheduleName
			migrationSchedule.Namespace = cmdFactory.GetNamespace()
			_, err = storkops.Instance().CreateMigrationSchedule(migrationSchedule)
			if err != nil {
				util.CheckErr(err)
				return
			}
			msg := fmt.Sprintf("MigrationSchedule %v created successfully", migrationSchedule.Name)
			printMsg(msg, ioStreams.Out)
		},
	}
	createMigrationScheduleCommand.Flags().StringSliceVarP(&namespaceList, "namespaces", "", nil, "Specify the comma-separated list of namespaces to be included in the migration")
	createMigrationScheduleCommand.Flags().StringVarP(&clusterPair, "cluster-pair", "c", "", "Specify the name of the ClusterPair in the same namespace to be used for the migration")
	createMigrationScheduleCommand.Flags().BoolVarP(&excludeResources, "exclude-resources", "", false, "If present, Kubernetes resources will not be migrated")
	createMigrationScheduleCommand.Flags().BoolVarP(&excludeVolumes, "exclude-volumes", "", false, "If present, the underlying Portworx volumes will not be migrated. This is the only allowed and default behaviour in sync-dr use cases.")
	createMigrationScheduleCommand.Flags().BoolVarP(&startApplications, "start-applications", "a", false, "If present, the applications will be scaled up on the target cluster after a successful migration")
	createMigrationScheduleCommand.Flags().StringVarP(&preExecRule, "pre-exec-rule", "", "", "Specify the name of the rule to be executed before every migration is triggered")
	createMigrationScheduleCommand.Flags().StringVarP(&postExecRule, "post-exec-rule", "", "", "Specify the name of the rule to be executed after every migration is triggered")
	createMigrationScheduleCommand.Flags().StringVarP(&schedulePolicyName, "schedule-policy-name", "s", "default-migration-policy", "Name of the schedule policy to use. If you want to create a new interval policy, use the interval flag instead")
	createMigrationScheduleCommand.Flags().BoolVar(&suspend, "suspend", false, "Flag to denote whether schedule should be suspended on creation")
	createMigrationScheduleCommand.Flags().BoolVar(&disableAutoSuspend, "disable-auto-suspend", false, "Prevent automatic suspension of DR migration schedules on the source cluster in case of a disaster")
	createMigrationScheduleCommand.Flags().IntVarP(&intervalMinutes, "interval", "i", 30, "Specify the time interval, in minutes, at which Stork should trigger migrations")
	createMigrationScheduleCommand.Flags().StringToStringVar(&annotations, "annotations", map[string]string{}, "Add required annotations to the resource in comma-separated key value pairs. key1=value1,key2=value2,... ")
	return createMigrationScheduleCommand
}

func newGetMigrationScheduleCommand(cmdFactory Factory, ioStreams genericclioptions.IOStreams) *cobra.Command {
	var clusterPair string
	getMigrationScheduleCommand := &cobra.Command{
		Use:     migrationScheduleSubcommand,
		Aliases: migrationScheduleAliases,
		Short:   "Get migration schedules",
		Run: func(c *cobra.Command, args []string) {
			var migrationSchedules *storkv1.MigrationScheduleList
			var err error

			namespaces, err := cmdFactory.GetAllNamespaces()
			if err != nil {
				util.CheckErr(err)
				return
			}
			if len(args) > 0 {
				migrationSchedules = new(storkv1.MigrationScheduleList)
				for _, migrationScheduleName := range args {
					for _, ns := range namespaces {
						migrationSchedule, err := storkops.Instance().GetMigrationSchedule(migrationScheduleName, ns)
						if err != nil {
							util.CheckErr(err)
							return
						}
						migrationSchedules.Items = append(migrationSchedules.Items, *migrationSchedule)
					}
				}
			} else {
				var tempMigrationSchedules storkv1.MigrationScheduleList
				for _, ns := range namespaces {
					migrationSchedules, err = storkops.Instance().ListMigrationSchedules(ns)
					if err != nil {
						util.CheckErr(err)
						return
					}
					tempMigrationSchedules.Items = append(tempMigrationSchedules.Items, migrationSchedules.Items...)
				}
				migrationSchedules = &tempMigrationSchedules
			}

			if len(clusterPair) != 0 {
				var tempMigrationSchedules storkv1.MigrationScheduleList

				for _, migrationSchedule := range migrationSchedules.Items {
					if migrationSchedule.Spec.Template.Spec.ClusterPair == clusterPair {
						tempMigrationSchedules.Items = append(tempMigrationSchedules.Items, migrationSchedule)
						continue
					}
				}
				migrationSchedules = &tempMigrationSchedules
			}

			if len(migrationSchedules.Items) == 0 {
				handleEmptyList(ioStreams.Out)
				return
			}
			if cmdFactory.IsWatchSet() {
				if err := printObjectsWithWatch(c, migrationSchedules, cmdFactory, migrationScheduleColumns, migrationSchedulePrinter, ioStreams.Out); err != nil {
					util.CheckErr(err)
					return
				}
				return
			}
			if err := printObjects(c, migrationSchedules, cmdFactory, migrationScheduleColumns, migrationSchedulePrinter, ioStreams.Out); err != nil {
				util.CheckErr(err)
				return
			}
		},
	}
	getMigrationScheduleCommand.Flags().StringVarP(&clusterPair, "clusterpair", "c", "", "Name of the cluster pair for which to list migration schedules")
	cmdFactory.BindGetFlags(getMigrationScheduleCommand.Flags())

	return getMigrationScheduleCommand
}

func newDeleteMigrationScheduleCommand(cmdFactory Factory, ioStreams genericclioptions.IOStreams) *cobra.Command {
	var clusterPair string
	deleteMigrationScheduleCommand := &cobra.Command{
		Use:     migrationScheduleSubcommand,
		Aliases: migrationScheduleAliases,
		Short:   "Delete migration schedules",
		Run: func(c *cobra.Command, args []string) {
			var migrationSchedules []string

			if len(clusterPair) == 0 {
				if len(args) == 0 {
					util.CheckErr(fmt.Errorf("at least one argument needs to be provided for migration schedule name if cluster pair isn't provided"))
					return
				}
				migrationSchedules = args
			} else {
				migrationScheduleList, err := storkops.Instance().ListMigrationSchedules(cmdFactory.GetNamespace())
				if err != nil {
					util.CheckErr(err)
					return
				}
				for _, migrationSchedule := range migrationScheduleList.Items {
					if migrationSchedule.Spec.Template.Spec.ClusterPair == clusterPair {
						migrationSchedules = append(migrationSchedules, migrationSchedule.Name)
					}
				}
			}

			if len(migrationSchedules) == 0 {
				handleEmptyList(ioStreams.Out)
				return
			}

			deleteMigrationSchedules(migrationSchedules, cmdFactory.GetNamespace(), ioStreams)
		},
	}
	deleteMigrationScheduleCommand.Flags().StringVarP(&clusterPair, "clusterPair", "c", "", "Name of the ClusterPair for which to delete ALL migration schedules")

	return deleteMigrationScheduleCommand
}

func deleteMigrationSchedules(migrationSchedules []string, namespace string, ioStreams genericclioptions.IOStreams) {
	for _, migrationSchedule := range migrationSchedules {
		err := storkops.Instance().DeleteMigrationSchedule(migrationSchedule, namespace)
		if err != nil {
			util.CheckErr(err)
			return
		}
		msg := fmt.Sprintf("MigrationSchedule %v deleted successfully", migrationSchedule)
		printMsg(msg, ioStreams.Out)
	}
}

func getMigrationSchedules(clusterPair string, args []string, namespace string) ([]*storkv1.MigrationSchedule, error) {
	var migrationSchedules []*storkv1.MigrationSchedule
	if len(clusterPair) == 0 {
		if len(args) == 0 {
			return nil, fmt.Errorf("at least one argument needs to be provided for migration schedule name if cluster pair isn't provided")
		}
		migrationSchedule, err := storkops.Instance().GetMigrationSchedule(args[0], namespace)
		if err != nil {
			return nil, err
		}
		migrationSchedules = append(migrationSchedules, migrationSchedule)
	} else {
		migrationScheduleList, err := storkops.Instance().ListMigrationSchedules(namespace)
		if err != nil {
			return nil, err
		}
		for _, migrationSchedule := range migrationScheduleList.Items {
			if migrationSchedule.Spec.Template.Spec.ClusterPair == clusterPair {
				migrSched := migrationSchedule
				migrationSchedules = append(migrationSchedules, &migrSched)
			}
		}
	}
	return migrationSchedules, nil
}

func newSuspendMigrationSchedulesCommand(cmdFactory Factory, ioStreams genericclioptions.IOStreams) *cobra.Command {
	var clusterPair string
	suspendMigrationScheduleCommand := &cobra.Command{
		Use:     migrationScheduleSubcommand,
		Aliases: migrationScheduleAliases,
		Short:   "Suspend migration schedules",
		Run: func(c *cobra.Command, args []string) {
			migrationSchedules, err := getMigrationSchedules(clusterPair, args, cmdFactory.GetNamespace())
			if err != nil {
				util.CheckErr(err)
				return
			}

			if len(migrationSchedules) == 0 {
				handleEmptyList(ioStreams.Out)
				return
			}
			updateMigrationSchedules(migrationSchedules, cmdFactory.GetNamespace(), ioStreams, true)
		},
	}
	suspendMigrationScheduleCommand.Flags().StringVarP(&clusterPair, "clusterPair", "c", "", "Name of the ClusterPair for which to suspend ALL migration schedules")

	return suspendMigrationScheduleCommand
}

func newResumeMigrationSchedulesCommand(cmdFactory Factory, ioStreams genericclioptions.IOStreams) *cobra.Command {
	var clusterPair string
	resumeMigrationScheduleCommand := &cobra.Command{
		Use:     migrationScheduleSubcommand,
		Aliases: migrationScheduleAliases,
		Short:   "Resume migration schedules",
		Run: func(c *cobra.Command, args []string) {
			migrationSchedules, err := getMigrationSchedules(clusterPair, args, cmdFactory.GetNamespace())
			if err != nil {
				util.CheckErr(err)
				return
			}

			if len(migrationSchedules) == 0 {
				handleEmptyList(ioStreams.Out)
				return
			}
			updateMigrationSchedules(migrationSchedules, cmdFactory.GetNamespace(), ioStreams, false)
		},
	}
	resumeMigrationScheduleCommand.Flags().StringVarP(&clusterPair, "clusterPair", "c", "", "Name of the ClusterPair for which to resume ALL migration schedules")

	return resumeMigrationScheduleCommand
}

func updateMigrationSchedules(migrationSchedules []*storkv1.MigrationSchedule, namespace string, ioStreams genericclioptions.IOStreams, suspend bool) {
	var action string
	if suspend {
		action = "suspended"
	} else {
		action = "resumed"
	}
	for _, migrationSchedule := range migrationSchedules {
		migrationSchedule.Spec.Suspend = &suspend
		_, err := storkops.Instance().UpdateMigrationSchedule(migrationSchedule)
		if err != nil {
			util.CheckErr(err)
			return
		}
		msg := fmt.Sprintf("MigrationSchedule %v %v successfully", migrationSchedule.Name, action)
		printMsg(msg, ioStreams.Out)
	}
}

func migrationSchedulePrinter(
	migrationScheduleList *storkv1.MigrationScheduleList,
	options printers.GenerateOptions,
) ([]metav1beta1.TableRow, error) {
	if migrationScheduleList == nil {
		return nil, nil
	}

	rows := make([]metav1beta1.TableRow, 0)
	for _, migrationSchedule := range migrationScheduleList.Items {
		lastSuccessTime := time.Time{}
		lastSuccessDuration := ""
		for _, policyType := range storkv1.GetValidSchedulePolicyTypes() {
			if len(migrationSchedule.Status.Items[policyType]) == 0 {
				continue
			}
			for _, migrationStatus := range migrationSchedule.Status.Items[policyType] {
				if migrationStatus.Status == storkv1.MigrationStatusSuccessful && migrationStatus.FinishTimestamp.Time.After(lastSuccessTime) {
					lastSuccessTime = migrationStatus.FinishTimestamp.Time
					lastSuccessDuration = migrationStatus.FinishTimestamp.Time.Sub(migrationStatus.CreationTimestamp.Time).String()
				}
			}
		}

		var suspend bool
		if migrationSchedule.Spec.Suspend == nil {
			suspend = false
		} else {
			suspend = *migrationSchedule.Spec.Suspend
		}

		row := getRow(&migrationSchedule,
			[]interface{}{migrationSchedule.Name,
				migrationSchedule.Spec.SchedulePolicyName,
				migrationSchedule.Spec.Template.Spec.ClusterPair,
				suspend,
				toTimeString(lastSuccessTime),
				lastSuccessDuration},
		)
		rows = append(rows, row)
	}
	return rows, nil
}
