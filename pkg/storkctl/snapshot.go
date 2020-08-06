package storkctl

import (
	"fmt"
	"time"

	snapv1 "github.com/kubernetes-incubator/external-storage/snapshot/pkg/apis/crd/v1"
	"github.com/libopenstorage/stork/drivers/volume"
	storkv1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	k8sextops "github.com/portworx/sched-ops/k8s/externalstorage"
	storkops "github.com/portworx/sched-ops/k8s/stork"
	"github.com/spf13/cobra"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	metav1beta1 "k8s.io/apimachinery/pkg/apis/meta/v1beta1"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/kubectl/pkg/cmd/util"
	"k8s.io/kubernetes/pkg/printers"
)

var snapshotColumns = []string{"NAME", "PVC", "STATUS", "CREATED", "COMPLETED", "TYPE"}
var snapSubcommand = "volumesnapshots"
var snapAliases = []string{"volumesnapshot", "snapshots", "snapshot", "snap"}

var snapRestoreSubCommand = "volumesnapshotrestore"
var snapRestoreAliases = []string{"volumesnapshotrestores", "snapshotrestore", "snapshotrestore", "snaprestore", "snapsrestores"}
var snapRestoreColumns = []string{"NAME", "SOURCE-SNAPSHOT", "SOURCE-SNAPSHOT-NAMESPACE", "STATUS", "VOLUMES", "CREATED"}

func newCreateSnapshotCommand(cmdFactory Factory, ioStreams genericclioptions.IOStreams) *cobra.Command {
	var snapName string
	var pvcName string
	createSnapshotCommand := &cobra.Command{
		Use:     snapSubcommand,
		Aliases: snapAliases,
		Short:   "Create snapshot resources",
		Run: func(c *cobra.Command, args []string) {
			if len(args) != 1 {
				util.CheckErr(fmt.Errorf("exactly one argument needs to be provided for snapshot name"))
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
			_, err := k8sextops.Instance().CreateSnapshot(snapshot)
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
						snapshot, err := k8sextops.Instance().GetSnapshot(snapName, ns)
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
					snapshots, err = k8sextops.Instance().ListSnapshots(ns)
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
					util.CheckErr(fmt.Errorf("at least one argument needs to be provided for snapshot name"))
					return
				}
				snaps = args
			} else {
				snapshots, err := k8sextops.Instance().ListSnapshots(namespace)
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
		err := k8sextops.Instance().DeleteSnapshot(snap, namespace)
		if err != nil {
			util.CheckErr(err)
			return
		}
		msg := fmt.Sprintf("Snapshot %v deleted successfully", snap)
		printMsg(msg, ioStreams.Out)
	}
}

func snapshotPrinter(
	snapList *snapv1.VolumeSnapshotList,
	options printers.GenerateOptions,
) ([]metav1beta1.TableRow, error) {
	if snapList == nil {
		return nil, nil
	}

	rows := make([]metav1beta1.TableRow, 0)
	for _, snap := range snapList.Items {
		status, completedTime := getSnapshotStatusAndTime(&snap)
		snapType := volume.GetSnapshotType(&snap)
		creationTime := toTimeString(snap.Metadata.CreationTimestamp.Time)
		row := getRow(&snap,
			[]interface{}{snap.Metadata.Name,
				snap.Spec.PersistentVolumeClaimName,
				status,
				creationTime,
				completedTime,
				snapType},
		)
		rows = append(rows, row)
	}
	return rows, nil
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

func newCreateVolumeSnapshotRestoreCommand(cmdFactory Factory, ioStreams genericclioptions.IOStreams) *cobra.Command {
	var snapName string
	var snapNamespace string
	var snapGroup bool

	restoreSnapshotCommand := &cobra.Command{
		Use:     snapRestoreSubCommand,
		Aliases: snapRestoreAliases,
		Short:   "Restore snapshot to source PVC",
		Run: func(c *cobra.Command, args []string) {
			if len(args) != 1 {
				util.CheckErr(fmt.Errorf("exactly one argument needs to be provided for volumesnapshotrestore name"))
				return
			}
			restoreCRDName := args[0]
			snapRestore := &storkv1.VolumeSnapshotRestore{
				Spec: storkv1.VolumeSnapshotRestoreSpec{
					SourceName:      snapName,
					SourceNamespace: snapNamespace,
					GroupSnapshot:   snapGroup,
				},
			}
			snapRestore.Name = restoreCRDName
			snapRestore.Namespace = cmdFactory.GetNamespace()
			_, err := storkops.Instance().CreateVolumeSnapshotRestore(snapRestore)
			if err != nil {
				util.CheckErr(err)
				return
			}

			msg := fmt.Sprintf("Snapshot restore %v started successfully", restoreCRDName)
			printMsg(msg, ioStreams.Out)
		},
	}
	restoreSnapshotCommand.Flags().StringVarP(&snapName, "snapname", "", "", "Snapshot name to be restored")
	restoreSnapshotCommand.Flags().StringVarP(&snapNamespace, "sourcenamepace", "", "default", "Namespace of snapshot")
	restoreSnapshotCommand.Flags().BoolVarP(&snapGroup, "groupsnapshot", "g", false, "True if snapshot is group, default false")
	return restoreSnapshotCommand
}

func newGetVolumeSnapshotRestoreCommand(cmdFactory Factory, ioStreams genericclioptions.IOStreams) *cobra.Command {
	restoreSnapshotCommand := &cobra.Command{
		Use:     snapRestoreSubCommand,
		Aliases: snapRestoreAliases,
		Short:   "Get snapshot restore status",
		Run: func(c *cobra.Command, args []string) {
			var snapRestoreList *storkv1.VolumeSnapshotRestoreList
			var err error

			namespaces, err := cmdFactory.GetAllNamespaces()
			if err != nil {
				util.CheckErr(err)
				return
			}

			if len(args) > 0 {
				snapRestoreList = new(storkv1.VolumeSnapshotRestoreList)
				for _, restoreName := range args {
					for _, ns := range namespaces {
						snapRestore, err := storkops.Instance().GetVolumeSnapshotRestore(restoreName, ns)
						if err != nil {
							util.CheckErr(err)
							return
						}
						snapRestoreList.Items = append(snapRestoreList.Items, *snapRestore)
					}
				}
			} else {
				var tempRestoreList storkv1.VolumeSnapshotRestoreList
				for _, ns := range namespaces {
					snapRestoreList, err = storkops.Instance().ListVolumeSnapshotRestore(ns)
					if err != nil {
						util.CheckErr(err)
						return
					}
					tempRestoreList.Items = append(tempRestoreList.Items, snapRestoreList.Items...)
				}
				snapRestoreList = &tempRestoreList
			}

			if len(snapRestoreList.Items) == 0 {
				handleEmptyList(ioStreams.Out)
				return
			}
			if cmdFactory.IsWatchSet() {
				if err := printObjectsWithWatch(c, snapRestoreList, cmdFactory, snapRestoreColumns, snapshotRestorePrinter, ioStreams.Out); err != nil {
					util.CheckErr(err)
					return
				}
				return
			}
			if err := printObjects(c, snapRestoreList, cmdFactory, snapRestoreColumns, snapshotRestorePrinter, ioStreams.Out); err != nil {
				util.CheckErr(err)
				return
			}
		},
	}

	cmdFactory.BindGetFlags(restoreSnapshotCommand.Flags())
	return restoreSnapshotCommand
}

func newDeleteVolumeSnapshotRestoreCommand(cmdFactory Factory, ioStreams genericclioptions.IOStreams) *cobra.Command {
	restoreSnapshotCommand := &cobra.Command{
		Use:     snapRestoreSubCommand,
		Aliases: snapRestoreAliases,
		Short:   "Delete Volume snapshot restore",
		Run: func(c *cobra.Command, args []string) {
			if len(args) == 0 {
				util.CheckErr(fmt.Errorf("at least one argument needs to be provided for volumesnapshotrestore name"))
				return
			}
			name := args[0]
			err := storkops.Instance().DeleteVolumeSnapshotRestore(name, cmdFactory.GetNamespace())
			if err != nil {
				util.CheckErr(err)
				return
			}

			msg := fmt.Sprintf("Volume snapshot restore %v deleted successfully", name)
			printMsg(msg, ioStreams.Out)
		},
	}

	return restoreSnapshotCommand
}

func snapshotRestorePrinter(
	snapRestoreList *storkv1.VolumeSnapshotRestoreList,
	options printers.GenerateOptions,
) ([]metav1beta1.TableRow, error) {
	if snapRestoreList == nil {
		return nil, nil
	}

	rows := make([]metav1beta1.TableRow, 0)
	for _, snapRestore := range snapRestoreList.Items {
		creationTime := toTimeString(snapRestore.CreationTimestamp.Time)
		row := getRow(&snapRestore,
			[]interface{}{snapRestore.Name,
				snapRestore.Spec.SourceName,
				snapRestore.Spec.SourceNamespace,
				snapRestore.Status.Status,
				len(snapRestore.Status.Volumes),
				creationTime},
		)
		rows = append(rows, row)
	}
	return rows, nil
}
