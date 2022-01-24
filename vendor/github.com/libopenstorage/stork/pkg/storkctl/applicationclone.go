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
	metav1beta1 "k8s.io/apimachinery/pkg/apis/meta/v1beta1"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/kubectl/pkg/cmd/util"
	"k8s.io/kubernetes/pkg/printers"
)

var (
	cloneStatusRetryInterval = 30 * time.Second
	cloneStatusRetryTimeout  = 6 * time.Hour
)

var applicationCloneColumns = []string{"NAME", "SOURCE", "DESTINATION", "STAGE", "STATUS", "VOLUMES", "RESOURCES", "CREATED", "ELAPSED"}
var applicationCloneSubcommand = "applicationclones"
var applicationCloneAliases = []string{"applicationclone", "clone", "clones"}

func newCreateApplicationCloneCommand(cmdFactory Factory, ioStreams genericclioptions.IOStreams) *cobra.Command {
	var applicationCloneName string
	var sourceNamespace string
	var destinationNamespace string
	var preExecRule string
	var postExecRule string
	var waitForCompletion bool
	var replacePolicy string

	createApplicationCloneCommand := &cobra.Command{
		Use:     applicationCloneSubcommand,
		Aliases: applicationCloneAliases,
		Short:   "Start an applicationClone",
		Run: func(c *cobra.Command, args []string) {
			if len(args) != 1 {
				util.CheckErr(fmt.Errorf("exactly one name needs to be provided for applicationclone name"))
				return
			}
			applicationCloneName = args[0]
			applicationClone := &storkv1.ApplicationClone{
				Spec: storkv1.ApplicationCloneSpec{
					SourceNamespace:      sourceNamespace,
					DestinationNamespace: destinationNamespace,
					PreExecRule:          preExecRule,
					PostExecRule:         postExecRule,
					ReplacePolicy:        storkv1.ApplicationCloneReplacePolicyType(replacePolicy),
				},
			}
			applicationClone.Name = applicationCloneName
			applicationClone.Namespace = cmdFactory.GetNamespace()
			_, err := storkops.Instance().CreateApplicationClone(applicationClone)
			if err != nil {
				util.CheckErr(err)
				return
			}

			msg := "ApplicationClone " + applicationCloneName + " started successfully"
			printMsg(msg, ioStreams.Out)

			if waitForCompletion {
				msg, err := waitForApplicationClone(applicationClone.Name, applicationClone.Namespace, ioStreams)
				if err != nil {
					util.CheckErr(err)
					return
				}
				printMsg(msg, ioStreams.Out)
			}
		},
	}
	createApplicationCloneCommand.Flags().BoolVarP(&waitForCompletion, "wait", "", false, "Wait for applicationclone to complete")
	createApplicationCloneCommand.Flags().StringVarP(&preExecRule, "preExecRule", "", "", "Rule to run before executing applicationclone")
	createApplicationCloneCommand.Flags().StringVarP(&postExecRule, "postExecRule", "", "", "Rule to run after executing applicationclone")
	createApplicationCloneCommand.Flags().StringVarP(&sourceNamespace, "sourceNamespace", "", "", "The namespace from where applications should be cloned")
	createApplicationCloneCommand.Flags().StringVarP(&destinationNamespace, "destinationNamespace", "", "", "The namespace to where the applications should be cloned")
	createApplicationCloneCommand.Flags().StringVarP(&replacePolicy, "replacePolicy", "r", "Retain", "Policy to use if resources being cloned already exist in destination namespace (Retain or Delete).")

	return createApplicationCloneCommand
}

func newGetApplicationCloneCommand(cmdFactory Factory, ioStreams genericclioptions.IOStreams) *cobra.Command {
	getApplicationCloneCommand := &cobra.Command{
		Use:     applicationCloneSubcommand,
		Aliases: applicationCloneAliases,
		Short:   "Get applicationclone resources",
		Run: func(c *cobra.Command, args []string) {
			var applicationClones *storkv1.ApplicationCloneList
			var err error

			namespaces, err := cmdFactory.GetAllNamespaces()
			if err != nil {
				util.CheckErr(err)
				return
			}
			if len(args) > 0 {
				applicationClones = new(storkv1.ApplicationCloneList)
				for _, applicationCloneName := range args {
					for _, ns := range namespaces {
						applicationClone, err := storkops.Instance().GetApplicationClone(applicationCloneName, ns)
						if err != nil {
							util.CheckErr(err)
							return
						}
						applicationClones.Items = append(applicationClones.Items, *applicationClone)
					}
				}
			} else {
				var tempApplicationClones storkv1.ApplicationCloneList
				for _, ns := range namespaces {
					applicationClones, err = storkops.Instance().ListApplicationClones(ns)
					if err != nil {
						util.CheckErr(err)
						return
					}
					tempApplicationClones.Items = append(tempApplicationClones.Items, applicationClones.Items...)
				}
				applicationClones = &tempApplicationClones
			}

			if len(applicationClones.Items) == 0 {
				handleEmptyList(ioStreams.Out)
				return
			}
			if cmdFactory.IsWatchSet() {
				if err := printObjectsWithWatch(c, applicationClones, cmdFactory, applicationCloneColumns, applicationClonePrinter, ioStreams.Out); err != nil {
					util.CheckErr(err)
					return
				}
				return
			}
			if err := printObjects(c, applicationClones, cmdFactory, applicationCloneColumns, applicationClonePrinter, ioStreams.Out); err != nil {
				util.CheckErr(err)
				return
			}
		},
	}
	cmdFactory.BindGetFlags(getApplicationCloneCommand.Flags())

	return getApplicationCloneCommand
}

func newDeleteApplicationCloneCommand(cmdFactory Factory, ioStreams genericclioptions.IOStreams) *cobra.Command {
	deleteApplicationCloneCommand := &cobra.Command{
		Use:     applicationCloneSubcommand,
		Aliases: applicationCloneAliases,
		Short:   "Delete applicationclone resources",
		Run: func(c *cobra.Command, args []string) {
			var applicationClones []string

			if len(args) == 0 {
				util.CheckErr(fmt.Errorf("at least one argument needs to be provided for applicationclone name"))
				return
			}
			applicationClones = args

			deleteApplicationClones(applicationClones, cmdFactory.GetNamespace(), ioStreams)
		},
	}

	return deleteApplicationCloneCommand
}

func deleteApplicationClones(applicationClones []string, namespace string, ioStreams genericclioptions.IOStreams) {
	for _, applicationClone := range applicationClones {
		err := storkops.Instance().DeleteApplicationClone(applicationClone, namespace)
		if err != nil {
			util.CheckErr(err)
			return
		}
		msg := fmt.Sprintf("ApplicationClone %v deleted successfully", applicationClone)
		printMsg(msg, ioStreams.Out)
	}
}

func applicationClonePrinter(
	applicationCloneList *storkv1.ApplicationCloneList,
	options printers.GenerateOptions,
) ([]metav1beta1.TableRow, error) {

	if applicationCloneList == nil {
		return nil, nil
	}
	rows := make([]metav1beta1.TableRow, 0)

	for _, applicationClone := range applicationCloneList.Items {
		name := applicationClone.Name

		totalVolumes := len(applicationClone.Status.Volumes)
		doneVolumes := 0
		for _, volume := range applicationClone.Status.Volumes {
			if volume.Status == storkv1.ApplicationCloneStatusSuccessful {
				doneVolumes++
			}
		}
		volumeStatus := fmt.Sprintf("%v/%v", doneVolumes, totalVolumes)

		elapsed := ""
		if !applicationClone.CreationTimestamp.IsZero() {
			if applicationClone.Status.Stage == storkv1.ApplicationCloneStageFinal {
				if !applicationClone.Status.FinishTimestamp.IsZero() {
					elapsed = applicationClone.Status.FinishTimestamp.Sub(applicationClone.CreationTimestamp.Time).String()
				}
			} else {
				elapsed = time.Since(applicationClone.CreationTimestamp.Time).String()
			}
		}

		creationTime := toTimeString(applicationClone.CreationTimestamp.Time)
		row := getRow(&applicationClone,
			[]interface{}{name,
				applicationClone.Spec.SourceNamespace,
				applicationClone.Spec.DestinationNamespace,
				applicationClone.Status.Stage,
				applicationClone.Status.Status,
				volumeStatus,
				len(applicationClone.Status.Resources),
				creationTime,
				elapsed},
		)
		rows = append(rows, row)
	}
	return rows, nil
}

func waitForApplicationClone(name, namespace string, ioStreams genericclioptions.IOStreams) (string, error) {
	var msg string
	var err error

	log.SetFlags(0)
	log.SetOutput(ioutil.Discard)
	heading := fmt.Sprintf("%s\t\t%-20s", stage, status)
	printMsg(heading, ioStreams.Out)
	t := func() (interface{}, bool, error) {
		clone, err := storkops.Instance().GetApplicationClone(name, namespace)
		if err != nil {
			util.CheckErr(err)
			return "", false, err
		}
		stat := fmt.Sprintf("%s\t\t%-20s", clone.Status.Stage, clone.Status.Status)
		printMsg(stat, ioStreams.Out)
		if clone.Status.Status == storkv1.ApplicationCloneStatusSuccessful ||
			clone.Status.Status == storkv1.ApplicationCloneStatusPartialSuccess {
			msg = fmt.Sprintf("ApplicationClone %v completed successfully", name)
			return "", false, nil
		}
		if clone.Status.Status == storkv1.ApplicationCloneStatusFailed {
			msg = fmt.Sprintf("ApplicationClone %v failed", name)
			return "", false, nil
		}
		return "", true, fmt.Errorf("%v", clone.Status.Status)
	}
	// sleep just so that instead of blank initial stage/status,
	// we have something at start
	time.Sleep(5 * time.Second)
	if _, err = task.DoRetryWithTimeout(t, cloneStatusRetryTimeout, cloneStatusRetryInterval); err != nil {
		msg = "Timed out performing task"
	}

	return msg, err
}
