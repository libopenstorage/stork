package storkctl

import (
	"fmt"
	"io"
	"time"

	snapv1 "github.com/kubernetes-incubator/external-storage/snapshot/pkg/apis/crd/v1"
	"github.com/libopenstorage/stork/drivers/volume"
	"github.com/portworx/sched-ops/k8s"
	"github.com/spf13/cobra"
	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/kubernetes/pkg/kubectl/cmd/util"
	"k8s.io/kubernetes/pkg/kubectl/genericclioptions"
	"k8s.io/kubernetes/pkg/printers"
)

var snapshotColumns = []string{"NAME", "PVC", "STATUS", "CREATED", "COMPLETED", "TYPE"}
var snapSubcommand = "volumesnapshots"
var snapAliases = []string{"volumesnapshot", "snapshots", "snapshot", "snap"}

func newCreateSnapshotCommand(cmdFactory Factory, ioStreams genericclioptions.IOStreams) *cobra.Command {
	var snapName string
	var pvcName string
	createSnapshotCommand := &cobra.Command{
		Use:     snapSubcommand,
		Aliases: snapAliases,
		Short:   "Create snapshot resources",
		Run: func(c *cobra.Command, args []string) {
			if len(args) != 1 {
				util.CheckErr(fmt.Errorf("Exactly one argument needs to be provided for snapshot name"))
				return
			}
			snapName = args[0]
			if len(pvcName) == 0 {
				util.CheckErr(fmt.Errorf("PVC name needs to be given"))
				return
			}

			namespace := cmdFactory.GetNamespace()

			snapshot := &snapv1.VolumeSnapshot{
				Metadata: metav1.ObjectMeta{
					Name:      snapName,
					Namespace: namespace,
				},
				Spec: snapv1.VolumeSnapshotSpec{
					PersistentVolumeClaimName: pvcName,
				},
			}
			_, err := k8s.Instance().CreateSnapshot(snapshot)
			if err != nil {
				util.CheckErr(err)
				return
			}
			msg := fmt.Sprintf("Snapshot %v created successfully\n", snapshot.Metadata.Name)
			printMsg(msg, ioStreams.Out)
		},
	}
	createSnapshotCommand.Flags().StringVarP(&pvcName, "pvc", "p", "", "Name of the PVC which should be used to create a snapshot")

	return createSnapshotCommand
}

func newGetSnapshotCommand(cmdFactory Factory, ioStreams genericclioptions.IOStreams) *cobra.Command {
	var pvcName string
	getSnapshotCommand := &cobra.Command{
		Use:     snapSubcommand,
		Aliases: snapAliases,
		Short:   "Get volume snapshot resources",
		Run: func(c *cobra.Command, args []string) {
			var snapshots *snapv1.VolumeSnapshotList
			var err error

			namespaces, err := cmdFactory.GetAllNamespaces()
			if err != nil {
				util.CheckErr(err)
				return
			}
			if len(args) > 0 {
				snapshots = new(snapv1.VolumeSnapshotList)
				for _, snapName := range args {
					for _, ns := range namespaces {
						snapshot, err := k8s.Instance().GetSnapshot(snapName, ns)
						if err != nil {
							util.CheckErr(err)
							return
						}
						snapshots.Items = append(snapshots.Items, *snapshot)
					}
				}
			} else {
				var tempSnapshots snapv1.VolumeSnapshotList
				for _, ns := range namespaces {
					snapshots, err = k8s.Instance().ListSnapshots(ns)
					if err != nil {
						util.CheckErr(err)
						return
					}
					tempSnapshots.Items = append(tempSnapshots.Items, snapshots.Items...)
				}
				snapshots = &tempSnapshots
			}

			if len(pvcName) != 0 {
				var tempSnapshots snapv1.VolumeSnapshotList

				for _, snap := range snapshots.Items {
					if snap.Spec.PersistentVolumeClaimName == pvcName {
						tempSnapshots.Items = append(tempSnapshots.Items, snap)
						continue
					}
				}
				snapshots = &tempSnapshots
			}

			if len(snapshots.Items) == 0 {
				handleEmptyList(ioStreams.Out)
				return
			}

			if err := printObjects(c, snapshots, cmdFactory, snapshotColumns, snapshotPrinter, ioStreams.Out); err != nil {
				util.CheckErr(err)
				return
			}
		},
	}
	getSnapshotCommand.Flags().StringVarP(&pvcName, "pvc", "p", "", "Name of the PVC for which to list snapshots")
	cmdFactory.BindGetFlags(getSnapshotCommand.Flags())

	return getSnapshotCommand
}

func newDeleteSnapshotCommand(cmdFactory Factory, ioStreams genericclioptions.IOStreams) *cobra.Command {
	var pvcName string
	deleteSnapshotCommand := &cobra.Command{
		Use:     snapSubcommand,
		Aliases: snapAliases,
		Short:   "Delete volume snapshot resources",
		Run: func(c *cobra.Command, args []string) {
			var snaps []string
			namespace := cmdFactory.GetNamespace()

			if len(pvcName) == 0 {
				if len(args) == 0 {
					util.CheckErr(fmt.Errorf("At least one argument needs to be provided for snapshot name"))
					return
				}
				snaps = args
			} else {
				snapshots, err := k8s.Instance().ListSnapshots(namespace)
				if err != nil {
					util.CheckErr(err)
					return
				}
				for _, snap := range snapshots.Items {
					if snap.Spec.PersistentVolumeClaimName == pvcName {
						snaps = append(snaps, snap.Metadata.Name)
					}
				}
			}

			if len(snaps) == 0 {
				handleEmptyList(ioStreams.Out)
				return
			}

			deleteSnapshots(snaps, namespace, ioStreams)
		},
	}
	deleteSnapshotCommand.Flags().StringVarP(&pvcName, "pvc", "p", "", "Name of the PVC for which to delete ALL snapshots")

	return deleteSnapshotCommand
}

func deleteSnapshots(snaps []string, namespace string, ioStreams genericclioptions.IOStreams) {
	for _, snap := range snaps {
		err := k8s.Instance().DeleteSnapshot(snap, namespace)
		if err != nil {
			util.CheckErr(err)
			return
		}
		msg := fmt.Sprintf("Snapshot %v deleted successfully", snap)
		printMsg(msg, ioStreams.Out)
	}
}

func snapshotPrinter(snapList *snapv1.VolumeSnapshotList, writer io.Writer, options printers.PrintOptions) error {
	if snapList == nil {
		return nil
	}
	for _, snap := range snapList.Items {
		name := printers.FormatResourceName(options.Kind, snap.Metadata.Name, options.WithKind)

		if options.WithNamespace {
			if _, err := fmt.Fprintf(writer, "%v\t", snap.Metadata.Namespace); err != nil {
				return err
			}
		}

		status, completedTime := getSnapshotStatusAndTime(&snap)
		snapType := volume.GetSnapshotType(&snap)
		creationTime := toTimeString(snap.Metadata.CreationTimestamp.Time)
		if _, err := fmt.Fprintf(writer, "%v\t%v\t%v\t%v\t%v\t%v\n", name, snap.Spec.PersistentVolumeClaimName, status, creationTime, completedTime, snapType); err != nil {
			return err
		}
	}
	return nil
}

func getSnapshotStatusAndTime(snap *snapv1.VolumeSnapshot) (string, string) {
	for _, condition := range snap.Status.Conditions {
		if condition.Type == snapv1.VolumeSnapshotConditionReady {
			if condition.Status == v1.ConditionTrue {
				return string(snapv1.VolumeSnapshotConditionReady), toTimeString(condition.LastTransitionTime.Time)
			}
			return string(snapv1.VolumeSnapshotConditionPending), toTimeString(time.Time{})
		}
	}
	return string(snapv1.VolumeSnapshotConditionPending), toTimeString(time.Time{})
}
