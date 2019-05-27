package storkctl

import (
	"fmt"
	"io"
	"time"

	snapv1 "github.com/kubernetes-incubator/external-storage/snapshot/pkg/apis/crd/v1"
	storkv1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/portworx/sched-ops/k8s"
	"github.com/spf13/cobra"
	"k8s.io/kubernetes/pkg/kubectl/cmd/util"
	"k8s.io/kubernetes/pkg/kubectl/genericclioptions"
	"k8s.io/kubernetes/pkg/printers"
)

var snapshotScheduleColumns = []string{"NAME", "PVC", "POLICYNAME", "PRE-EXEC-RULE", "POST-EXEC-RULE", "RECLAIM-POLICY", "SUSPEND", "LAST-SUCCESS-TIME"}
var snapshotScheduleSubcommand = "volumesnapshotschedules"
var snapshotScheduleAliases = []string{"volumesnapshotschedule", "snapshotschedule", "snapshotschedules"}

func newCreateSnapshotScheduleCommand(cmdFactory Factory, ioStreams genericclioptions.IOStreams) *cobra.Command {
	var snapshotScheduleName string
	var preExecRule string
	var postExecRule string
	var schedulePolicyName string
	var reclaimPolicy string
	var suspend bool
	var pvc string

	createSnapshotScheduleCommand := &cobra.Command{
		Use:     snapshotScheduleSubcommand,
		Aliases: snapshotScheduleAliases,
		Short:   "Create a snapshot schedule",
		Run: func(c *cobra.Command, args []string) {
			if len(args) != 1 {
				util.CheckErr(fmt.Errorf("Exactly one name needs to be provided for volume snapshot schedule name"))
				return
			}
			snapshotScheduleName = args[0]
			if len(schedulePolicyName) == 0 {
				util.CheckErr(fmt.Errorf("Need to provide schedulePolicyName"))
				return
			}

			snapshotSchedule := &storkv1.VolumeSnapshotSchedule{
				Spec: storkv1.VolumeSnapshotScheduleSpec{
					Template: storkv1.VolumeSnapshotTemplateSpec{
						Spec: snapv1.VolumeSnapshotSpec{
							PersistentVolumeClaimName: pvc,
						},
					},
					PreExecRule:        preExecRule,
					PostExecRule:       postExecRule,
					SchedulePolicyName: schedulePolicyName,
					Suspend:            &suspend,
					ReclaimPolicy:      storkv1.ReclaimPolicyType(reclaimPolicy),
				},
			}
			snapshotSchedule.Name = snapshotScheduleName
			snapshotSchedule.Namespace = cmdFactory.GetNamespace()
			_, err := k8s.Instance().CreateSnapshotSchedule(snapshotSchedule)
			if err != nil {
				util.CheckErr(err)
				return
			}
			msg := fmt.Sprintf("VolumeSnapshotSchedule %v created successfully", snapshotSchedule.Name)
			printMsg(msg, ioStreams.Out)
		},
	}
	createSnapshotScheduleCommand.Flags().StringVarP(&pvc, "pvc", "p", "", "Name of the PVC for which to create a snapshot schedule")
	createSnapshotScheduleCommand.Flags().StringVarP(&preExecRule, "preExecRule", "", "", "Rule to run before executing snapshot")
	createSnapshotScheduleCommand.Flags().StringVarP(&postExecRule, "postExecRule", "", "", "Rule to run after executing snapshot")
	createSnapshotScheduleCommand.Flags().StringVarP(&schedulePolicyName, "schedulePolicyName", "s", "", "Name of the schedule policy to use")
	createSnapshotScheduleCommand.Flags().StringVarP(&reclaimPolicy, "reclaimPolicy", "", "Retain", "Reclaim policy for the created snapshots (Retain or Delete)")
	createSnapshotScheduleCommand.Flags().BoolVar(&suspend, "suspend", false, "Flag to denote whether schedule should be suspended on creation")

	return createSnapshotScheduleCommand
}

func newGetSnapshotScheduleCommand(cmdFactory Factory, ioStreams genericclioptions.IOStreams) *cobra.Command {
	var pvc string
	getSnapshotScheduleCommand := &cobra.Command{
		Use:     snapshotScheduleSubcommand,
		Aliases: snapshotScheduleAliases,
		Short:   "Get volume snapshot schedules",
		Run: func(c *cobra.Command, args []string) {
			var snapshotSchedules *storkv1.VolumeSnapshotScheduleList
			var err error

			namespaces, err := cmdFactory.GetAllNamespaces()
			if err != nil {
				util.CheckErr(err)
				return
			}
			if len(args) > 0 {
				snapshotSchedules = new(storkv1.VolumeSnapshotScheduleList)
				for _, snapshotScheduleName := range args {
					for _, ns := range namespaces {
						snapshotSchedule, err := k8s.Instance().GetSnapshotSchedule(snapshotScheduleName, ns)
						if err != nil {
							util.CheckErr(err)
							return
						}
						snapshotSchedules.Items = append(snapshotSchedules.Items, *snapshotSchedule)
					}
				}
			} else {
				var tempVolumeSnapshotSchedules storkv1.VolumeSnapshotScheduleList
				for _, ns := range namespaces {
					snapshotSchedules, err = k8s.Instance().ListSnapshotSchedules(ns)
					if err != nil {
						util.CheckErr(err)
						return
					}
					tempVolumeSnapshotSchedules.Items = append(tempVolumeSnapshotSchedules.Items, snapshotSchedules.Items...)
				}
				snapshotSchedules = &tempVolumeSnapshotSchedules
			}

			if len(pvc) != 0 {
				var tempVolumeSnapshotSchedules storkv1.VolumeSnapshotScheduleList

				for _, snapshotSchedule := range snapshotSchedules.Items {
					if snapshotSchedule.Spec.Template.Spec.PersistentVolumeClaimName == pvc {
						tempVolumeSnapshotSchedules.Items = append(tempVolumeSnapshotSchedules.Items, snapshotSchedule)
						continue
					}
				}
				snapshotSchedules = &tempVolumeSnapshotSchedules
			}

			if len(snapshotSchedules.Items) == 0 {
				handleEmptyList(ioStreams.Out)
				return
			}

			if err := printObjects(c, snapshotSchedules, cmdFactory, snapshotScheduleColumns, snapshotSchedulePrinter, ioStreams.Out); err != nil {
				util.CheckErr(err)
				return
			}
		},
	}
	getSnapshotScheduleCommand.Flags().StringVarP(&pvc, "pvc", "p", "", "Name of the PVC for which to list snapshot schedules")
	cmdFactory.BindGetFlags(getSnapshotScheduleCommand.Flags())

	return getSnapshotScheduleCommand
}

func newDeleteSnapshotScheduleCommand(cmdFactory Factory, ioStreams genericclioptions.IOStreams) *cobra.Command {
	var pvc string
	deleteSnapshotScheduleCommand := &cobra.Command{
		Use:     snapshotScheduleSubcommand,
		Aliases: snapshotScheduleAliases,
		Short:   "Delete snapshot schedules",
		Run: func(c *cobra.Command, args []string) {
			var snapshotSchedules []string

			if len(pvc) == 0 {
				if len(args) == 0 {
					util.CheckErr(fmt.Errorf("At least one argument needs to be provided for snapshot schedule name if pvc isn't provided"))
					return
				}
				snapshotSchedules = args
			} else {
				snapshotScheduleList, err := k8s.Instance().ListSnapshotSchedules(cmdFactory.GetNamespace())
				if err != nil {
					util.CheckErr(err)
					return
				}
				for _, snapshotSchedule := range snapshotScheduleList.Items {
					if snapshotSchedule.Spec.Template.Spec.PersistentVolumeClaimName == pvc {
						snapshotSchedules = append(snapshotSchedules, snapshotSchedule.Name)
					}
				}
			}

			if len(snapshotSchedules) == 0 {
				handleEmptyList(ioStreams.Out)
				return
			}

			deleteSnapshotSchedules(snapshotSchedules, cmdFactory.GetNamespace(), ioStreams)
		},
	}
	deleteSnapshotScheduleCommand.Flags().StringVarP(&pvc, "pvc", "p", "", "Name of the PVC for which to delete snapshot schedules")

	return deleteSnapshotScheduleCommand
}

func deleteSnapshotSchedules(snapshotSchedules []string, namespace string, ioStreams genericclioptions.IOStreams) {
	for _, snapshotSchedule := range snapshotSchedules {
		err := k8s.Instance().DeleteSnapshotSchedule(snapshotSchedule, namespace)
		if err != nil {
			util.CheckErr(err)
			return
		}
		msg := fmt.Sprintf("VolumeSnapshotSchedule %v deleted successfully", snapshotSchedule)
		printMsg(msg, ioStreams.Out)
	}
}

func getSnapshotSchedules(args []string, namespace string) ([]*storkv1.VolumeSnapshotSchedule, error) {
	var snapshotSchedules []*storkv1.VolumeSnapshotSchedule
	if len(args) == 0 {
		return nil, fmt.Errorf("at least one argument needs to be provided for volumesnapshot schedule name")
	}
	snapshotSchedule, err := k8s.Instance().GetSnapshotSchedule(args[0], namespace)
	if err != nil {
		return nil, err
	}
	snapshotSchedules = append(snapshotSchedules, snapshotSchedule)
	return snapshotSchedules, nil
}

func newSuspendSnapshotSchedulesCommand(cmdFactory Factory, ioStreams genericclioptions.IOStreams) *cobra.Command {
	suspendSnapshotScheduleCommand := &cobra.Command{
		Use:     snapshotScheduleSubcommand,
		Aliases: snapshotScheduleAliases,
		Short:   "Suspend snapshot schedules",
		Run: func(c *cobra.Command, args []string) {
			snapshotSchedules, err := getSnapshotSchedules(args, cmdFactory.GetNamespace())
			if err != nil {
				util.CheckErr(err)
				return
			}

			if len(snapshotSchedules) == 0 {
				handleEmptyList(ioStreams.Out)
				return
			}
			updateSnapshotSchedules(snapshotSchedules, cmdFactory.GetNamespace(), ioStreams, true)
		},
	}

	return suspendSnapshotScheduleCommand
}

func newResumeSnapshotSchedulesCommand(cmdFactory Factory, ioStreams genericclioptions.IOStreams) *cobra.Command {
	resumeSnapshotScheduleCommand := &cobra.Command{
		Use:     snapshotScheduleSubcommand,
		Aliases: snapshotScheduleAliases,
		Short:   "Resume snapshot schedules",
		Run: func(c *cobra.Command, args []string) {
			snapshotSchedules, err := getSnapshotSchedules(args, cmdFactory.GetNamespace())
			if err != nil {
				util.CheckErr(err)
				return
			}

			if len(snapshotSchedules) == 0 {
				handleEmptyList(ioStreams.Out)
				return
			}
			updateSnapshotSchedules(snapshotSchedules, cmdFactory.GetNamespace(), ioStreams, false)
		},
	}

	return resumeSnapshotScheduleCommand
}

func updateSnapshotSchedules(snapshotSchedules []*storkv1.VolumeSnapshotSchedule, namespace string, ioStreams genericclioptions.IOStreams, suspend bool) {
	var action string
	if suspend {
		action = "suspended"
	} else {
		action = "resumed"
	}
	for _, snapshotSchedule := range snapshotSchedules {
		snapshotSchedule.Spec.Suspend = &suspend
		_, err := k8s.Instance().UpdateSnapshotSchedule(snapshotSchedule)
		if err != nil {
			util.CheckErr(err)
			return
		}
		msg := fmt.Sprintf("VolumeSnapshotSchedule %v %v successfully", snapshotSchedule.Name, action)
		printMsg(msg, ioStreams.Out)
	}
}

func snapshotSchedulePrinter(snapshotScheduleList *storkv1.VolumeSnapshotScheduleList, writer io.Writer, options printers.PrintOptions) error {
	if snapshotScheduleList == nil {
		return nil
	}
	for _, snapshotSchedule := range snapshotScheduleList.Items {
		name := printers.FormatResourceName(options.Kind, snapshotSchedule.Name, options.WithKind)

		if options.WithNamespace {
			if _, err := fmt.Fprintf(writer, "%v\t", snapshotSchedule.Namespace); err != nil {
				return err
			}
		}

		lastSuccessTime := time.Time{}
		for _, policyType := range storkv1.GetValidSchedulePolicyTypes() {
			if len(snapshotSchedule.Status.Items[policyType]) == 0 {
				continue
			}
			for _, snapshotStatus := range snapshotSchedule.Status.Items[policyType] {
				if snapshotStatus.Status == snapv1.VolumeSnapshotConditionReady && snapshotStatus.FinishTimestamp.Time.After(lastSuccessTime) {
					lastSuccessTime = snapshotStatus.FinishTimestamp.Time
				}
			}
		}

		var suspend bool
		if snapshotSchedule.Spec.Suspend == nil {
			suspend = false
		} else {
			suspend = *snapshotSchedule.Spec.Suspend
		}
		if _, err := fmt.Fprintf(writer, "%v\t%v\t%v\t%v\t%v\t%v\t%v\t%v\n",
			name,
			snapshotSchedule.Spec.Template.Spec.PersistentVolumeClaimName,
			snapshotSchedule.Spec.SchedulePolicyName,
			snapshotSchedule.Spec.PreExecRule,
			snapshotSchedule.Spec.PostExecRule,
			snapshotSchedule.Spec.ReclaimPolicy,
			suspend,
			toTimeString(lastSuccessTime),
		); err != nil {
			return err
		}
	}
	return nil
}
