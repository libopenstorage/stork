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
	"k8s.io/kubernetes/pkg/printers"
)

var snapshotColumns = []string{"NAME", "PVC", "STATUS", "CREATED", "COMPLETED", "TYPE"}
var snapSubcommand = "volumesnapshots"
var snapAliases = []string{"volumesnapshot", "snapshots", "snapshot", "snap"}

func newCreateSnapshotCommand(cmdFactory Factory) *cobra.Command {
	var snapName string
	var pvcName string
	createSnapshotCommand := &cobra.Command{
		Use:     snapSubcommand,
		Aliases: snapAliases,
		Short:   "Create snapshot resources",
		Run: func(c *cobra.Command, args []string) {
			if len(args) != 1 {
				handleError(fmt.Errorf("Exactly one argument needs to be provided for snapshot name"))
			} else {
				snapName = args[0]
			}
			if len(pvcName) == 0 {
				handleError(fmt.Errorf("PVC name needs to be given"))
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
				handleError(err)
			}
			fmt.Printf("Snapshot %v created successfully\n", snapshot.Metadata.Name)
		},
	}
	createSnapshotCommand.Flags().StringVarP(&pvcName, "pvc", "p", "", "Name of the PVC which should be used to create a snapshot")

	return createSnapshotCommand
}

func newGetSnapshotCommand(cmdFactory Factory) *cobra.Command {
	var pvcName string
	getSnapshotCommand := &cobra.Command{
		Use:     snapSubcommand,
		Aliases: snapAliases,
		Short:   "Get volume snapshot resources",
		Run: func(c *cobra.Command, args []string) {
			var snapshots *snapv1.VolumeSnapshotList
			var err error

			namespace := cmdFactory.GetNamespace()

			if len(args) > 0 {
				snapshots = new(snapv1.VolumeSnapshotList)
				for _, snapName := range args {
					snapshot, err := k8s.Instance().GetSnapshot(snapName, namespace)
					if err != nil {
						handleError(err)
					}
					snapshots.Items = append(snapshots.Items, *snapshot)
				}
			} else {
				snapshots, err = k8s.Instance().ListSnapshots(namespace)
				if err != nil {
					handleError(err)
				}
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
				handleEmptyList()
				return
			}

			outputFormat, err := cmdFactory.GetOutputFormat()
			if err != nil {
				handleError(err)
			}

			if err := printObjects(c, snapshots, outputFormat, snapshotColumns, snapshotPrinter); err != nil {
				handleError(err)
			}
		},
	}
	getSnapshotCommand.Flags().StringVarP(&pvcName, "pvc", "p", "", "Name of the PVC for which to list snapshots")

	return getSnapshotCommand
}

func newDeleteSnapshotCommand(cmdFactory Factory) *cobra.Command {
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
					handleError(fmt.Errorf("Atleast one argument needs to be provided for snapshot name"))
				}
				snaps = args
			} else {
				snapshots, err := k8s.Instance().ListSnapshots(namespace)
				if err != nil {
					handleError(err)
				}
				for _, snap := range snapshots.Items {
					if snap.Spec.PersistentVolumeClaimName == pvcName {
						snaps = append(snaps, snap.Metadata.Name)
					}
				}
			}

			if len(snaps) == 0 {
				handleEmptyList()
			}

			deleteSnapshots(snaps, namespace)
		},
	}
	deleteSnapshotCommand.Flags().StringVarP(&pvcName, "pvc", "p", "", "Name of the PVC for which to delete ALL snapshots")

	return deleteSnapshotCommand
}

func deleteSnapshots(snaps []string, namespace string) {
	for _, snap := range snaps {
		err := k8s.Instance().DeleteSnapshot(snap, namespace)
		if err != nil {
			handleError(err)
		} else {
			fmt.Printf("Snapshot %v deleted successfully\n", snap)
		}
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
		creationTime := toTimeString(snap.Metadata.CreationTimestamp)
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
				return string(snapv1.VolumeSnapshotConditionReady), toTimeString(condition.LastTransitionTime)
			}
			return string(snapv1.VolumeSnapshotConditionPending), toTimeString(metav1.NewTime(time.Time{}))
		}
	}
	return string(snapv1.VolumeSnapshotConditionPending), toTimeString(metav1.NewTime(time.Time{}))
}
