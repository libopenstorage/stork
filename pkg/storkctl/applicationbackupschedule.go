package storkctl

import (
	"fmt"
	"time"

	storkv1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	storkops "github.com/portworx/sched-ops/k8s/stork"
	"github.com/spf13/cobra"
	metav1beta1 "k8s.io/apimachinery/pkg/apis/meta/v1beta1"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/kubectl/pkg/cmd/util"
	"k8s.io/kubernetes/pkg/printers"
)

var applicationBackupScheduleColumns = []string{"NAME", "POLICY-NAME", "BACKUP-LOCATION", "SUSPEND", "LAST-SUCCESS-TIME", "LAST-SUCCESS-DURATION"}
var applicationBackupScheduleSubcommand = "applicationbackupschedules"
var applicationBackupScheduleAliases = []string{"applicationbackupschedule"}

func newCreateApplicationBackupScheduleCommand(cmdFactory Factory, ioStreams genericclioptions.IOStreams) *cobra.Command {
	var applicationBackupScheduleName string
	var backupLocation string
	var namespaceList []string
	var preExecRule string
	var postExecRule string
	var schedulePolicyName string
	var suspend bool

	createApplicationBackupScheduleCommand := &cobra.Command{
		Use:     applicationBackupScheduleSubcommand,
		Aliases: applicationBackupScheduleAliases,
		Short:   "Create a applicationBackup schedule",
		Run: func(c *cobra.Command, args []string) {
			if len(args) != 1 {
				util.CheckErr(fmt.Errorf("exactly one name needs to be provided for applicationbackup schedule name"))
				return
			}
			applicationBackupScheduleName = args[0]
			if len(backupLocation) == 0 {
				util.CheckErr(fmt.Errorf("BackupLocation name needs to be provided for applicationbackup schedule"))
				return
			}
			if len(namespaceList) == 0 {
				util.CheckErr(fmt.Errorf("need to provide atleast one namespace to migrate"))
				return
			}
			if len(schedulePolicyName) == 0 {
				util.CheckErr(fmt.Errorf("need to provide schedulePolicyName"))
				return
			}

			_, err := storkops.Instance().GetSchedulePolicy(schedulePolicyName)
			if err != nil {
				util.CheckErr(fmt.Errorf("error getting schedulepolicy %v: %v", schedulePolicyName, err))
				return
			}

			applicationBackupSchedule := &storkv1.ApplicationBackupSchedule{
				Spec: storkv1.ApplicationBackupScheduleSpec{
					Template: storkv1.ApplicationBackupTemplateSpec{
						Spec: storkv1.ApplicationBackupSpec{
							BackupLocation: backupLocation,
							Namespaces:     namespaceList,
							PreExecRule:    preExecRule,
							PostExecRule:   postExecRule,
						},
					},
					SchedulePolicyName: schedulePolicyName,
					Suspend:            &suspend,
				},
			}
			applicationBackupSchedule.Name = applicationBackupScheduleName
			applicationBackupSchedule.Namespace = cmdFactory.GetNamespace()
			_, err = storkops.Instance().CreateApplicationBackupSchedule(applicationBackupSchedule)
			if err != nil {
				util.CheckErr(err)
				return
			}
			msg := fmt.Sprintf("ApplicationBackupSchedule %v created successfully", applicationBackupSchedule.Name)
			printMsg(msg, ioStreams.Out)
		},
	}
	createApplicationBackupScheduleCommand.Flags().StringSliceVarP(&namespaceList, "namespaces", "", nil, "Comma separated list of namespaces to migrate")
	createApplicationBackupScheduleCommand.Flags().StringVarP(&backupLocation, "backupLocation", "b", "", "BackupLocation to use for the backup")
	createApplicationBackupScheduleCommand.Flags().StringVarP(&preExecRule, "preExecRule", "", "", "Rule to run before executing applicationBackup")
	createApplicationBackupScheduleCommand.Flags().StringVarP(&postExecRule, "postExecRule", "", "", "Rule to run after executing applicationBackup")
	createApplicationBackupScheduleCommand.Flags().StringVarP(&schedulePolicyName, "schedulePolicyName", "s", "default-applicationbackup-policy", "Name of the schedule policy to use")
	createApplicationBackupScheduleCommand.Flags().BoolVar(&suspend, "suspend", false, "Flag to denote whether schedule should be suspended on creation")

	return createApplicationBackupScheduleCommand
}

func newGetApplicationBackupScheduleCommand(cmdFactory Factory, ioStreams genericclioptions.IOStreams) *cobra.Command {
	var backupLocation string
	getApplicationBackupScheduleCommand := &cobra.Command{
		Use:     applicationBackupScheduleSubcommand,
		Aliases: applicationBackupScheduleAliases,
		Short:   "Get applicationBackup schedules",
		Run: func(c *cobra.Command, args []string) {
			var applicationBackupSchedules *storkv1.ApplicationBackupScheduleList
			var err error

			namespaces, err := cmdFactory.GetAllNamespaces()
			if err != nil {
				util.CheckErr(err)
				return
			}
			if len(args) > 0 {
				applicationBackupSchedules = new(storkv1.ApplicationBackupScheduleList)
				for _, applicationBackupScheduleName := range args {
					for _, ns := range namespaces {
						applicationBackupSchedule, err := storkops.Instance().GetApplicationBackupSchedule(applicationBackupScheduleName, ns)
						if err != nil {
							util.CheckErr(err)
							return
						}
						applicationBackupSchedules.Items = append(applicationBackupSchedules.Items, *applicationBackupSchedule)
					}
				}
			} else {
				var tempApplicationBackupSchedules storkv1.ApplicationBackupScheduleList
				for _, ns := range namespaces {
					applicationBackupSchedules, err = storkops.Instance().ListApplicationBackupSchedules(ns)
					if err != nil {
						util.CheckErr(err)
						return
					}
					tempApplicationBackupSchedules.Items = append(tempApplicationBackupSchedules.Items, applicationBackupSchedules.Items...)
				}
				applicationBackupSchedules = &tempApplicationBackupSchedules
			}

			if len(backupLocation) != 0 {
				var tempApplicationBackupSchedules storkv1.ApplicationBackupScheduleList

				for _, applicationBackupSchedule := range applicationBackupSchedules.Items {
					if applicationBackupSchedule.Spec.Template.Spec.BackupLocation == backupLocation {
						tempApplicationBackupSchedules.Items = append(tempApplicationBackupSchedules.Items, applicationBackupSchedule)
						continue
					}
				}
				applicationBackupSchedules = &tempApplicationBackupSchedules
			}

			if len(applicationBackupSchedules.Items) == 0 {
				handleEmptyList(ioStreams.Out)
				return
			}
			if cmdFactory.IsWatchSet() {
				if err := printObjectsWithWatch(c, applicationBackupSchedules, cmdFactory, applicationBackupScheduleColumns, applicationBackupSchedulePrinter, ioStreams.Out); err != nil {
					util.CheckErr(err)
					return
				}
				return
			}
			if err := printObjects(c, applicationBackupSchedules, cmdFactory, applicationBackupScheduleColumns, applicationBackupSchedulePrinter, ioStreams.Out); err != nil {
				util.CheckErr(err)
				return
			}
		},
	}
	getApplicationBackupScheduleCommand.Flags().StringVarP(&backupLocation, "backupLocation", "b", "", "Name of the BackupLocation for which to list applicationBackup schedules")
	cmdFactory.BindGetFlags(getApplicationBackupScheduleCommand.Flags())

	return getApplicationBackupScheduleCommand
}

func newDeleteApplicationBackupScheduleCommand(cmdFactory Factory, ioStreams genericclioptions.IOStreams) *cobra.Command {
	var backupLocation string
	deleteApplicationBackupScheduleCommand := &cobra.Command{
		Use:     applicationBackupScheduleSubcommand,
		Aliases: applicationBackupScheduleAliases,
		Short:   "Delete applicationBackup schedules",
		Run: func(c *cobra.Command, args []string) {
			var applicationBackupSchedules []string

			if len(backupLocation) == 0 {
				if len(args) == 0 {
					util.CheckErr(fmt.Errorf("at least one argument needs to be provided for applicationbackup schedule name if backupLocation isn't provided"))
					return
				}
				applicationBackupSchedules = args
			} else {
				applicationBackupScheduleList, err := storkops.Instance().ListApplicationBackupSchedules(cmdFactory.GetNamespace())
				if err != nil {
					util.CheckErr(err)
					return
				}
				for _, applicationBackupSchedule := range applicationBackupScheduleList.Items {
					if applicationBackupSchedule.Spec.Template.Spec.BackupLocation == backupLocation {
						applicationBackupSchedules = append(applicationBackupSchedules, applicationBackupSchedule.Name)
					}
				}
			}

			if len(applicationBackupSchedules) == 0 {
				handleEmptyList(ioStreams.Out)
				return
			}

			deleteApplicationBackupSchedules(applicationBackupSchedules, cmdFactory.GetNamespace(), ioStreams)
		},
	}
	deleteApplicationBackupScheduleCommand.Flags().StringVarP(&backupLocation, "backupLocation", "b", "", "Name of the BackupLocation for which to delete ALL applicationBackup schedules")

	return deleteApplicationBackupScheduleCommand
}

func deleteApplicationBackupSchedules(applicationBackupSchedules []string, namespace string, ioStreams genericclioptions.IOStreams) {
	for _, applicationBackupSchedule := range applicationBackupSchedules {
		err := storkops.Instance().DeleteApplicationBackupSchedule(applicationBackupSchedule, namespace)
		if err != nil {
			util.CheckErr(err)
			return
		}
		msg := fmt.Sprintf("ApplicationBackupSchedule %v deleted successfully", applicationBackupSchedule)
		printMsg(msg, ioStreams.Out)
	}
}

func getApplicationBackupSchedules(backupLocation string, args []string, namespace string) ([]*storkv1.ApplicationBackupSchedule, error) {
	var applicationBackupSchedules []*storkv1.ApplicationBackupSchedule
	if len(backupLocation) == 0 {
		if len(args) == 0 {
			return nil, fmt.Errorf("at least one argument needs to be provided for applicationBackup schedule name if backupLocation isn't provided")
		}
		applicationBackupSchedule, err := storkops.Instance().GetApplicationBackupSchedule(args[0], namespace)
		if err != nil {
			return nil, err
		}
		applicationBackupSchedules = append(applicationBackupSchedules, applicationBackupSchedule)
	} else {
		applicationBackupScheduleList, err := storkops.Instance().ListApplicationBackupSchedules(namespace)
		if err != nil {
			return nil, err
		}
		for _, applicationBackupSchedule := range applicationBackupScheduleList.Items {
			if applicationBackupSchedule.Spec.Template.Spec.BackupLocation == backupLocation {
				applicationBackupSchedules = append(applicationBackupSchedules, &applicationBackupSchedule)
			}
		}
	}
	return applicationBackupSchedules, nil
}

func newSuspendApplicationBackupSchedulesCommand(cmdFactory Factory, ioStreams genericclioptions.IOStreams) *cobra.Command {
	var backupLocation string
	suspendApplicationBackupScheduleCommand := &cobra.Command{
		Use:     applicationBackupScheduleSubcommand,
		Aliases: applicationBackupScheduleAliases,
		Short:   "Suspend applicationBackup schedules",
		Run: func(c *cobra.Command, args []string) {
			applicationBackupSchedules, err := getApplicationBackupSchedules(backupLocation, args, cmdFactory.GetNamespace())
			if err != nil {
				util.CheckErr(err)
				return
			}

			if len(applicationBackupSchedules) == 0 {
				handleEmptyList(ioStreams.Out)
				return
			}
			updateApplicationBackupSchedules(applicationBackupSchedules, cmdFactory.GetNamespace(), ioStreams, true)
		},
	}
	suspendApplicationBackupScheduleCommand.Flags().StringVarP(&backupLocation, "backupLocation", "b", "", "Name of the BackupLocation for which to suspend ALL applicationBackup schedules")

	return suspendApplicationBackupScheduleCommand
}

func newResumeApplicationBackupSchedulesCommand(cmdFactory Factory, ioStreams genericclioptions.IOStreams) *cobra.Command {
	var backupLocation string
	resumeApplicationBackupScheduleCommand := &cobra.Command{
		Use:     applicationBackupScheduleSubcommand,
		Aliases: applicationBackupScheduleAliases,
		Short:   "Resume applicationBackup schedules",
		Run: func(c *cobra.Command, args []string) {
			applicationBackupSchedules, err := getApplicationBackupSchedules(backupLocation, args, cmdFactory.GetNamespace())
			if err != nil {
				util.CheckErr(err)
				return
			}

			if len(applicationBackupSchedules) == 0 {
				handleEmptyList(ioStreams.Out)
				return
			}
			updateApplicationBackupSchedules(applicationBackupSchedules, cmdFactory.GetNamespace(), ioStreams, false)
		},
	}
	resumeApplicationBackupScheduleCommand.Flags().StringVarP(&backupLocation, "backupLocation", "b", "", "Name of the BackupLocation for which to resume ALL applicationBackup schedules")

	return resumeApplicationBackupScheduleCommand
}

func updateApplicationBackupSchedules(applicationBackupSchedules []*storkv1.ApplicationBackupSchedule, namespace string, ioStreams genericclioptions.IOStreams, suspend bool) {
	var action string
	if suspend {
		action = "suspended"
	} else {
		action = "resumed"
	}
	for _, applicationBackupSchedule := range applicationBackupSchedules {
		applicationBackupSchedule.Spec.Suspend = &suspend
		_, err := storkops.Instance().UpdateApplicationBackupSchedule(applicationBackupSchedule)
		if err != nil {
			util.CheckErr(err)
			return
		}
		msg := fmt.Sprintf("ApplicationBackupSchedule %v %v successfully", applicationBackupSchedule.Name, action)
		printMsg(msg, ioStreams.Out)
	}
}

func applicationBackupSchedulePrinter(
	applicationBackupScheduleList *storkv1.ApplicationBackupScheduleList,
	options printers.GenerateOptions,
) ([]metav1beta1.TableRow, error) {
	if applicationBackupScheduleList == nil {
		return nil, nil
	}
	rows := make([]metav1beta1.TableRow, 0)
	for _, applicationBackupSchedule := range applicationBackupScheduleList.Items {
		name := applicationBackupSchedule.Name

		lastSuccessTime := time.Time{}
		lastSuccessDuration := ""
		for _, policyType := range storkv1.GetValidSchedulePolicyTypes() {
			if len(applicationBackupSchedule.Status.Items[policyType]) == 0 {
				continue
			}
			for _, applicationBackupStatus := range applicationBackupSchedule.Status.Items[policyType] {
				if applicationBackupStatus.Status == storkv1.ApplicationBackupStatusSuccessful && applicationBackupStatus.FinishTimestamp.Time.After(lastSuccessTime) {
					lastSuccessTime = applicationBackupStatus.FinishTimestamp.Time
					lastSuccessDuration = applicationBackupStatus.FinishTimestamp.Time.Sub(applicationBackupStatus.CreationTimestamp.Time).String()
				}
			}
		}

		var suspend bool
		if applicationBackupSchedule.Spec.Suspend == nil {
			suspend = false
		} else {
			suspend = *applicationBackupSchedule.Spec.Suspend
		}
		row := getRow(&applicationBackupSchedule,
			[]interface{}{name,
				applicationBackupSchedule.Spec.SchedulePolicyName,
				applicationBackupSchedule.Spec.Template.Spec.BackupLocation,
				suspend,
				toTimeString(lastSuccessTime),
				lastSuccessDuration},
		)
		rows = append(rows, row)
	}
	return rows, nil
}
