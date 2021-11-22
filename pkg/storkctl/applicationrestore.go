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
	restoreStatusRetryInterval = 30 * time.Second
	restoreStatusRetryTimeout  = 6 * time.Hour
)

var applicationRestoreColumns = []string{"NAME", "STAGE", "STATUS", "VOLUMES", "RESOURCES", "CREATED", "ELAPSED"}
var applicationRestoreSubcommand = "applicationrestores"
var applicationRestoreAliases = []string{"applicationrestore", "apprestore", "apprestores"}

func newCreateApplicationRestoreCommand(cmdFactory Factory, ioStreams genericclioptions.IOStreams) *cobra.Command {
	var applicationRestoreName string
	var backupLocation string
	var waitForCompletion bool
	var backupName string
	var replacePolicy string

	createApplicationRestoreCommand := &cobra.Command{
		Use:     applicationRestoreSubcommand,
		Aliases: applicationRestoreAliases,
		Short:   "Start an applicationRestore",
		Run: func(c *cobra.Command, args []string) {
			if len(args) != 1 {
				util.CheckErr(fmt.Errorf("exactly one name needs to be provided for applicationrestore name"))
				return
			}
			if backupLocation == "" {
				util.CheckErr(fmt.Errorf("need to provide BackupLocation to use for restore"))
				return
			}
			if backupName == "" {
				util.CheckErr(fmt.Errorf("need to provide BackupName to restore"))
				return
			}

			applicationRestoreName = args[0]
			applicationRestore := &storkv1.ApplicationRestore{
				Spec: storkv1.ApplicationRestoreSpec{
					BackupLocation: backupLocation,
					BackupName:     backupName,
					ReplacePolicy:  storkv1.ApplicationRestoreReplacePolicyType(replacePolicy),
				},
			}
			applicationRestore.Name = applicationRestoreName
			applicationRestore.Namespace = cmdFactory.GetNamespace()
			_, err := storkops.Instance().CreateApplicationRestore(applicationRestore)
			if err != nil {
				util.CheckErr(err)
				return
			}

			msg := "ApplicationRestore " + applicationRestoreName + " started successfully"
			printMsg(msg, ioStreams.Out)

			if waitForCompletion {
				msg, err := waitForApplicationRestore(applicationRestore.Name, applicationRestore.Namespace, ioStreams)
				if err != nil {
					util.CheckErr(err)
					return
				}
				printMsg(msg, ioStreams.Out)
			}
		},
	}
	createApplicationRestoreCommand.Flags().BoolVarP(&waitForCompletion, "wait", "", false, "Wait for applicationrestore to complete")
	createApplicationRestoreCommand.Flags().StringVarP(&backupLocation, "backupLocation", "l", "", "BackupLocation to use for the restore")
	createApplicationRestoreCommand.Flags().StringVarP(&backupName, "backupName", "b", "", "Backup to restore from")
	createApplicationRestoreCommand.Flags().StringVarP(&replacePolicy, "replacePolicy", "r", "Retain", "Policy to use if resources being restored already exist (Retain or Delete).")

	return createApplicationRestoreCommand
}

func newGetApplicationRestoreCommand(cmdFactory Factory, ioStreams genericclioptions.IOStreams) *cobra.Command {
	getApplicationRestoreCommand := &cobra.Command{
		Use:     applicationRestoreSubcommand,
		Aliases: applicationRestoreAliases,
		Short:   "Get applicationrestore resources",
		Run: func(c *cobra.Command, args []string) {
			var applicationRestores *storkv1.ApplicationRestoreList
			var err error

			namespaces, err := cmdFactory.GetAllNamespaces()
			if err != nil {
				util.CheckErr(err)
				return
			}
			if len(args) > 0 {
				applicationRestores = new(storkv1.ApplicationRestoreList)
				for _, applicationRestoreName := range args {
					for _, ns := range namespaces {
						applicationRestore, err := storkops.Instance().GetApplicationRestore(applicationRestoreName, ns)
						if err != nil {
							util.CheckErr(err)
							return
						}
						applicationRestores.Items = append(applicationRestores.Items, *applicationRestore)
					}
				}
			} else {
				var tempApplicationRestores storkv1.ApplicationRestoreList
				listOptions := metav1.ListOptions{}
				for _, ns := range namespaces {
					applicationRestores, err = storkops.Instance().ListApplicationRestores(ns, listOptions)
					if err != nil {
						util.CheckErr(err)
						return
					}
					tempApplicationRestores.Items = append(tempApplicationRestores.Items, applicationRestores.Items...)
				}
				applicationRestores = &tempApplicationRestores
			}

			if len(applicationRestores.Items) == 0 {
				handleEmptyList(ioStreams.Out)
				return
			}
			if cmdFactory.IsWatchSet() {
				if err := printObjectsWithWatch(c, applicationRestores, cmdFactory, applicationRestoreColumns, applicationRestorePrinter, ioStreams.Out); err != nil {
					util.CheckErr(err)
					return
				}
				return
			}
			if err := printObjects(c, applicationRestores, cmdFactory, applicationRestoreColumns, applicationRestorePrinter, ioStreams.Out); err != nil {
				util.CheckErr(err)
				return
			}
		},
	}
	cmdFactory.BindGetFlags(getApplicationRestoreCommand.Flags())

	return getApplicationRestoreCommand
}

func newDeleteApplicationRestoreCommand(cmdFactory Factory, ioStreams genericclioptions.IOStreams) *cobra.Command {
	deleteApplicationRestoreCommand := &cobra.Command{
		Use:     applicationRestoreSubcommand,
		Aliases: applicationRestoreAliases,
		Short:   "Delete applicationrestore resources",
		Run: func(c *cobra.Command, args []string) {
			var applicationRestores []string

			if len(args) == 0 {
				util.CheckErr(fmt.Errorf("at least one argument needs to be provided for applicationrestore name"))
				return
			}
			applicationRestores = args

			deleteApplicationRestores(applicationRestores, cmdFactory.GetNamespace(), ioStreams)
		},
	}

	return deleteApplicationRestoreCommand
}

func deleteApplicationRestores(applicationRestores []string, namespace string, ioStreams genericclioptions.IOStreams) {
	for _, applicationRestore := range applicationRestores {
		err := storkops.Instance().DeleteApplicationRestore(applicationRestore, namespace)
		if err != nil {
			util.CheckErr(err)
			return
		}
		msg := fmt.Sprintf("ApplicationRestore %v deleted successfully", applicationRestore)
		printMsg(msg, ioStreams.Out)
	}
}

func applicationRestorePrinter(
	applicationRestoreList *storkv1.ApplicationRestoreList,
	options printers.GenerateOptions,
) ([]metav1beta1.TableRow, error) {
	if applicationRestoreList == nil {
		return nil, nil
	}
	rows := make([]metav1beta1.TableRow, 0)
	for _, applicationRestore := range applicationRestoreList.Items {
		name := applicationRestore.Name

		totalVolumes := len(applicationRestore.Status.Volumes)
		doneVolumes := 0
		for _, volume := range applicationRestore.Status.Volumes {
			if volume.Status == storkv1.ApplicationRestoreStatusSuccessful {
				doneVolumes++
			}
		}
		volumeStatus := fmt.Sprintf("%v/%v", doneVolumes, totalVolumes)

		elapsed := ""
		if !applicationRestore.CreationTimestamp.IsZero() {
			if applicationRestore.Status.Stage == storkv1.ApplicationRestoreStageFinal {
				if !applicationRestore.Status.FinishTimestamp.IsZero() {
					elapsed = applicationRestore.Status.FinishTimestamp.Sub(applicationRestore.CreationTimestamp.Time).String()
				}
			} else {
				elapsed = time.Since(applicationRestore.CreationTimestamp.Time).String()
			}
		}

		creationTime := toTimeString(applicationRestore.CreationTimestamp.Time)
		row := getRow(&applicationRestore,
			[]interface{}{name,
				applicationRestore.Status.Stage,
				applicationRestore.Status.Status,
				volumeStatus,
				len(applicationRestore.Status.Resources),
				creationTime,
				elapsed},
		)
		rows = append(rows, row)
	}
	return rows, nil
}

func waitForApplicationRestore(name, namespace string, ioStreams genericclioptions.IOStreams) (string, error) {
	var msg string
	var err error

	log.SetFlags(0)
	log.SetOutput(ioutil.Discard)
	heading := fmt.Sprintf("%s\t\t%-20s", stage, status)
	printMsg(heading, ioStreams.Out)
	t := func() (interface{}, bool, error) {
		restore, err := storkops.Instance().GetApplicationRestore(name, namespace)
		if err != nil {
			util.CheckErr(err)
			return "", false, err
		}
		stat := fmt.Sprintf("%s\t\t%-20s", restore.Status.Stage, restore.Status.Status)
		printMsg(stat, ioStreams.Out)
		if restore.Status.Status == storkv1.ApplicationRestoreStatusSuccessful ||
			restore.Status.Status == storkv1.ApplicationRestoreStatusPartialSuccess {
			msg = fmt.Sprintf("ApplicationRestore %v completed successfully", name)
			return "", false, nil
		}
		if restore.Status.Status == storkv1.ApplicationRestoreStatusFailed {
			msg = fmt.Sprintf("ApplicationRestore %v failed", name)
			return "", false, nil
		}
		return "", true, fmt.Errorf("%v", restore.Status.Status)
	}
	// sleep just so that instead of blank initial stage/status,
	// we have something at start
	time.Sleep(5 * time.Second)
	if _, err = task.DoRetryWithTimeout(t, restoreStatusRetryTimeout, restoreStatusRetryInterval); err != nil {
		msg = "Timed out performing task"
	}

	return msg, err
}
