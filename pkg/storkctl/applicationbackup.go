package storkctl

import (
	"fmt"
	"io/ioutil"
	"log"
	"time"

	storkv1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	storkops "github.com/portworx/sched-ops/k8s/stork"
	"github.com/portworx/sched-ops/task"
	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	metav1beta1 "k8s.io/apimachinery/pkg/apis/meta/v1beta1"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/kubectl/pkg/cmd/util"
	"k8s.io/kubernetes/pkg/printers"
)

var (
	backupStatusRetryInterval = 30 * time.Second
	backupStatusRetryTimeout  = 6 * time.Hour
)

var applicationBackupColumns = []string{"NAME", "STAGE", "STATUS", "VOLUMES", "RESOURCES", "CREATED", "ELAPSED"}
var applicationBackupSubcommand = "applicationbackups"
var applicationBackupAliases = []string{"applicationbackup", "backup", "backups"}

func newCreateApplicationBackupCommand(cmdFactory Factory, ioStreams genericclioptions.IOStreams) *cobra.Command {
	var applicationBackupName string
	var namespaceList []string
	var preExecRule string
	var postExecRule string
	var waitForCompletion bool
	var backupLocation string

	createApplicationBackupCommand := &cobra.Command{
		Use:     applicationBackupSubcommand,
		Aliases: applicationBackupAliases,
		Short:   "Start an applicationBackup",
		Run: func(c *cobra.Command, args []string) {
			if len(args) != 1 {
				util.CheckErr(fmt.Errorf("exactly one name needs to be provided for applicationbackup name"))
				return
			}
			applicationBackupName = args[0]
			if len(namespaceList) == 0 {
				util.CheckErr(fmt.Errorf("need to provide atleast one namespace to backup"))
				return
			}
			if backupLocation == "" {
				util.CheckErr(fmt.Errorf("need to provide BackupLocation to use for backup"))
				return
			}
			applicationBackup := &storkv1.ApplicationBackup{
				Spec: storkv1.ApplicationBackupSpec{
					Namespaces:     namespaceList,
					PreExecRule:    preExecRule,
					PostExecRule:   postExecRule,
					BackupLocation: backupLocation,
				},
			}
			applicationBackup.Name = applicationBackupName
			applicationBackup.Namespace = cmdFactory.GetNamespace()
			_, err := storkops.Instance().CreateApplicationBackup(applicationBackup)
			if err != nil {
				util.CheckErr(err)
				return
			}

			msg := "ApplicationBackup " + applicationBackupName + " started successfully"
			printMsg(msg, ioStreams.Out)

			if waitForCompletion {
				msg, err := waitForApplicationBackup(applicationBackup.Name, applicationBackup.Namespace, ioStreams)
				if err != nil {
					util.CheckErr(err)
					return
				}
				printMsg(msg, ioStreams.Out)
			}
		},
	}
	createApplicationBackupCommand.Flags().StringSliceVarP(&namespaceList, "namespaces", "", nil, "Comma separated list of namespaces to backup")
	createApplicationBackupCommand.Flags().BoolVarP(&waitForCompletion, "wait", "", false, "Wait for applicationbackup to complete")
	createApplicationBackupCommand.Flags().StringVarP(&preExecRule, "preExecRule", "", "", "Rule to run before executing applicationbackup")
	createApplicationBackupCommand.Flags().StringVarP(&postExecRule, "postExecRule", "", "", "Rule to run after executing applicationbackup")
	createApplicationBackupCommand.Flags().StringVarP(&backupLocation, "backupLocation", "b", "", "BackupLocation to use for the backup")

	return createApplicationBackupCommand
}

func newGetApplicationBackupCommand(cmdFactory Factory, ioStreams genericclioptions.IOStreams) *cobra.Command {
	getApplicationBackupCommand := &cobra.Command{
		Use:     applicationBackupSubcommand,
		Aliases: applicationBackupAliases,
		Short:   "Get applicationbackup resources",
		Run: func(c *cobra.Command, args []string) {
			var applicationBackups *storkv1.ApplicationBackupList
			var err error

			namespaces, err := cmdFactory.GetAllNamespaces()
			if err != nil {
				util.CheckErr(err)
				return
			}
			if len(args) > 0 {
				applicationBackups = new(storkv1.ApplicationBackupList)
				for _, applicationBackupName := range args {
					for _, ns := range namespaces {
						applicationBackup, err := storkops.Instance().GetApplicationBackup(applicationBackupName, ns)
						if err != nil {
							util.CheckErr(err)
							return
						}
						applicationBackups.Items = append(applicationBackups.Items, *applicationBackup)
					}
				}
			} else {
				var tempApplicationBackups storkv1.ApplicationBackupList
				listOptions := metav1.ListOptions{}
				for _, ns := range namespaces {
					applicationBackups, err = storkops.Instance().ListApplicationBackups(ns, listOptions)
					if err != nil {
						util.CheckErr(err)
						return
					}
					tempApplicationBackups.Items = append(tempApplicationBackups.Items, applicationBackups.Items...)
				}
				applicationBackups = &tempApplicationBackups
			}

			if len(applicationBackups.Items) == 0 {
				handleEmptyList(ioStreams.Out)
				return
			}
			if cmdFactory.IsWatchSet() {
				if err := printObjectsWithWatch(c, applicationBackups, cmdFactory, applicationBackupColumns, applicationBackupPrinter, ioStreams.Out); err != nil {
					util.CheckErr(err)
					return
				}
				return
			}

			if err := printObjects(c, applicationBackups, cmdFactory, applicationBackupColumns, applicationBackupPrinter, ioStreams.Out); err != nil {
				util.CheckErr(err)
				return
			}
		},
	}
	cmdFactory.BindGetFlags(getApplicationBackupCommand.Flags())

	return getApplicationBackupCommand
}

func newDeleteApplicationBackupCommand(cmdFactory Factory, ioStreams genericclioptions.IOStreams) *cobra.Command {
	deleteApplicationBackupCommand := &cobra.Command{
		Use:     applicationBackupSubcommand,
		Aliases: applicationBackupAliases,
		Short:   "Delete applicationbackup resources",
		Run: func(c *cobra.Command, args []string) {
			var applicationBackups []string

			if len(args) == 0 {
				util.CheckErr(fmt.Errorf("at least one argument needs to be provided for applicationbackup name"))
				return
			}
			applicationBackups = args

			deleteApplicationBackups(applicationBackups, cmdFactory.GetNamespace(), ioStreams)
		},
	}

	return deleteApplicationBackupCommand
}

func deleteApplicationBackups(applicationBackups []string, namespace string, ioStreams genericclioptions.IOStreams) {
	for _, applicationBackup := range applicationBackups {
		err := storkops.Instance().DeleteApplicationBackup(applicationBackup, namespace)
		if err != nil {
			util.CheckErr(err)
			return
		}
		msg := fmt.Sprintf("ApplicationBackup %v deleted successfully", applicationBackup)
		printMsg(msg, ioStreams.Out)
	}
}

func applicationBackupPrinter(
	applicationBackupList *storkv1.ApplicationBackupList,
	options printers.GenerateOptions,
) ([]metav1beta1.TableRow, error) {
	if applicationBackupList == nil {
		return nil, nil
	}

	rows := make([]metav1beta1.TableRow, 0)
	for _, applicationBackup := range applicationBackupList.Items {
		totalVolumes := len(applicationBackup.Status.Volumes)
		doneVolumes := 0
		for _, volume := range applicationBackup.Status.Volumes {
			if volume.Status == storkv1.ApplicationBackupStatusSuccessful {
				doneVolumes++
			}
		}
		volumeStatus := fmt.Sprintf("%v/%v", doneVolumes, totalVolumes)

		elapsed := ""
		if !applicationBackup.CreationTimestamp.IsZero() {
			if applicationBackup.Status.Stage == storkv1.ApplicationBackupStageFinal {
				if applicationBackup.Status.TriggerTimestamp.Time.IsZero() {
					elapsed = "N/A"
				} else if !applicationBackup.Status.FinishTimestamp.IsZero() {
					elapsed = applicationBackup.Status.FinishTimestamp.Sub(applicationBackup.Status.TriggerTimestamp.Time).String()
				}
			} else {
				if applicationBackup.Status.TriggerTimestamp.Time.IsZero() {
					elapsed = "N/A"
				} else {
					elapsed = time.Since(applicationBackup.Status.TriggerTimestamp.Time).String()
				}
			}
		}

		creationTime := toTimeString(applicationBackup.Status.TriggerTimestamp.Time)
		row := getRow(&applicationBackup,
			[]interface{}{applicationBackup.Name,
				applicationBackup.Status.Stage,
				applicationBackup.Status.Status,
				volumeStatus,
				len(applicationBackup.Status.Resources),
				creationTime,
				elapsed},
		)
		rows = append(rows, row)
	}
	return rows, nil
}

func waitForApplicationBackup(name, namespace string, ioStreams genericclioptions.IOStreams) (string, error) {
	var msg string
	var err error

	log.SetFlags(0)
	log.SetOutput(ioutil.Discard)
	heading := fmt.Sprintf("%s\t\t%-20s", stage, status)
	printMsg(heading, ioStreams.Out)
	t := func() (interface{}, bool, error) {
		backup, err := storkops.Instance().GetApplicationBackup(name, namespace)
		if err != nil {
			util.CheckErr(err)
			return "", false, err
		}
		stat := fmt.Sprintf("%s\t\t%-20s", backup.Status.Stage, backup.Status.Status)
		printMsg(stat, ioStreams.Out)
		if backup.Status.Status == storkv1.ApplicationBackupStatusSuccessful ||
			backup.Status.Status == storkv1.ApplicationBackupStatusPartialSuccess {
			msg = fmt.Sprintf("ApplicationBackup %v completed successfully", name)
			return "", false, nil
		}
		if backup.Status.Status == storkv1.ApplicationBackupStatusFailed {
			msg = fmt.Sprintf("ApplicationBackup %v failed", name)
			return "", false, nil
		}
		return "", true, fmt.Errorf("%v", backup.Status.Status)
	}
	// sleep just so that instead of blank initial stage/status,
	// we have something at start
	time.Sleep(5 * time.Second)
	if _, err = task.DoRetryWithTimeout(t, backupStatusRetryTimeout, backupStatusRetryInterval); err != nil {
		msg = "Timed out performing task"
	}

	return msg, err
}
