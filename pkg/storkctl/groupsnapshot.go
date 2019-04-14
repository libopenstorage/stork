package storkctl

import (
	"fmt"
	"io"
	"strings"

	storkv1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/portworx/sched-ops/k8s"
	"github.com/spf13/cobra"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/kubernetes/pkg/kubectl/cmd/util"
	"k8s.io/kubernetes/pkg/kubectl/genericclioptions"
	"k8s.io/kubernetes/pkg/printers"
)

var groupSnapshotColumns = []string{"NAME", "STATUS", "STAGE", "SNAPSHOTS", "CREATED"}
var groupSnapshotSubcommand = "groupsnapshots"
var groupSnapshotAliases = []string{"groupsnapshot"}

func newCreateGroupSnapshotCommand(cmdFactory Factory, ioStreams genericclioptions.IOStreams) *cobra.Command {
	var groupSnapshotName string
	var restoreNamespaces []string
	var opts []string
	var pvcSelectors []string
	var preExecRule string
	var postExecRule string
	var maxRetries int

	createGroupVolumeSnapshotCommand := &cobra.Command{
		Use:     groupSnapshotSubcommand,
		Aliases: groupSnapshotAliases,
		Short:   "Create a group volume snapshot",
		Run: func(c *cobra.Command, args []string) {
			if len(args) != 1 {
				util.CheckErr(fmt.Errorf("exactly one name needs to be provided for groupsnapshot name"))
				return
			}
			groupSnapshotName = args[0]
			if len(pvcSelectors) == 0 {
				util.CheckErr(fmt.Errorf("PVC label selectors must be provided"))
				return
			}

			labelSelector, err := parseKeyValueList(pvcSelectors)
			if err != nil {
				util.CheckErr(err)
				return
			}

			pvcSelectorSpec := storkv1.PVCSelectorSpec{
				LabelSelector: meta.LabelSelector{
					MatchLabels: labelSelector,
				},
			}

			var optsMap map[string]string
			if len(opts) > 0 {
				optsMap, err = parseKeyValueList(opts)
				if err != nil {
					util.CheckErr(err)
					return
				}
			}

			groupSnapshot := &storkv1.GroupVolumeSnapshot{
				Spec: storkv1.GroupVolumeSnapshotSpec{
					PreExecRule:       preExecRule,
					PostExecRule:      postExecRule,
					PVCSelector:       pvcSelectorSpec,
					RestoreNamespaces: restoreNamespaces,
					MaxRetries:        maxRetries,
					Options:           optsMap,
				},
			}
			groupSnapshot.Name = groupSnapshotName
			groupSnapshot.Namespace = cmdFactory.GetNamespace()
			_, err = k8s.Instance().CreateGroupSnapshot(groupSnapshot)
			if err != nil {
				util.CheckErr(err)
				return
			}
			msg := fmt.Sprintf("GroupVolumeSnapshot %v created successfully", groupSnapshot.Name)
			printMsg(msg, ioStreams.Out)
		},
	}

	createGroupVolumeSnapshotCommand.Flags().StringSliceVarP(
		&pvcSelectors, "pvcSelectors", "", nil,
		"Comma-separated list of PVC selectors in the format key1=value1,key2=value2. "+" e.g app=mysql,tier=db")

	createGroupVolumeSnapshotCommand.Flags().StringVarP(
		&preExecRule, "preExecRule", "", "", "Rule to run before triggering group volume snapshot")

	createGroupVolumeSnapshotCommand.Flags().StringVarP(
		&postExecRule, "postExecRule", "", "", "Rule to run after triggering group volume snapshot")

	createGroupVolumeSnapshotCommand.Flags().StringSliceVarP(
		&restoreNamespaces, "restoreNamespaces", "", nil,
		"List of namespaces to which the snapshots can be restored to")

	createGroupVolumeSnapshotCommand.Flags().IntVarP(
		&maxRetries, "maxRetries", "", 0, "Number of times to retry the groupvolumesnapshot on failure")

	createGroupVolumeSnapshotCommand.Flags().StringSliceVarP(
		&opts, "opts", "", nil,
		"Comma-separated list of options to provide to the storage driver. These "+
			"are in the format key1=value1,key2=value2. e.g portworx/snapshot-type=cloud")

	return createGroupVolumeSnapshotCommand
}

func newGetGroupVolumeSnapshotCommand(cmdFactory Factory, ioStreams genericclioptions.IOStreams) *cobra.Command {
	getGroupVolumeSnapshotCommand := &cobra.Command{
		Use:     groupSnapshotSubcommand,
		Aliases: groupSnapshotAliases,
		Short:   "Get group volume snapshots",
		Run: func(c *cobra.Command, args []string) {
			var groupSnapshots *storkv1.GroupVolumeSnapshotList
			var err error

			namespaces, err := cmdFactory.GetAllNamespaces()
			if err != nil {
				util.CheckErr(err)
				return
			}

			groupSnapshots = new(storkv1.GroupVolumeSnapshotList)

			if len(args) > 0 {
				for _, groupSnapshotName := range args {
					for _, ns := range namespaces {
						groupSnapshot, err := k8s.Instance().GetGroupSnapshot(groupSnapshotName, ns)
						if err != nil {
							util.CheckErr(err)
							return
						}
						groupSnapshots.Items = append(groupSnapshots.Items, *groupSnapshot)
					}
				}
			} else {
				// Get all
				for _, ns := range namespaces {
					groupSnapshotsInNamespace, err := k8s.Instance().ListGroupSnapshots(ns)
					if err != nil {
						util.CheckErr(err)
						return
					}
					groupSnapshots.Items = append(groupSnapshots.Items, groupSnapshotsInNamespace.Items...)
				}
			}

			if len(groupSnapshots.Items) == 0 {
				handleEmptyList(ioStreams.Out)
				return
			}

			if err := printObjects(c, groupSnapshots, cmdFactory, groupSnapshotColumns, groupSnapshotPrinter, ioStreams.Out); err != nil {
				util.CheckErr(err)
				return
			}
		},
	}

	cmdFactory.BindGetFlags(getGroupVolumeSnapshotCommand.Flags())
	return getGroupVolumeSnapshotCommand
}

func newDeleteGroupVolumeSnapshotCommand(cmdFactory Factory, ioStreams genericclioptions.IOStreams) *cobra.Command {
	deleteGroupVolumeSnapshotCommand := &cobra.Command{
		Use:     groupSnapshotSubcommand,
		Aliases: groupSnapshotAliases,
		Short:   "Delete group volume snapshots",
		Run: func(c *cobra.Command, args []string) {
			if len(args) == 0 {
				util.CheckErr(fmt.Errorf("at least one argument needs to be provided for groupsnapshot name"))
				return
			}

			deleteGroupVolumeSnapshots(args, cmdFactory.GetNamespace(), ioStreams)
		},
	}

	return deleteGroupVolumeSnapshotCommand
}

func deleteGroupVolumeSnapshots(groupSnapshots []string, namespace string, ioStreams genericclioptions.IOStreams) {
	for _, groupSnapshot := range groupSnapshots {
		err := k8s.Instance().DeleteGroupSnapshot(groupSnapshot, namespace)
		if err != nil {
			util.CheckErr(err)
			return
		}
		msg := fmt.Sprintf("GroupVolumeSnapshot %v deleted successfully", groupSnapshot)
		printMsg(msg, ioStreams.Out)
	}
}

func groupSnapshotPrinter(groupSnapshotList *storkv1.GroupVolumeSnapshotList, writer io.Writer, options printers.PrintOptions) error {
	if groupSnapshotList == nil {
		return nil
	}

	for _, groupSnapshot := range groupSnapshotList.Items {
		name := printers.FormatResourceName(options.Kind, groupSnapshot.Name, options.WithKind)

		if options.WithNamespace {
			if _, err := fmt.Fprintf(writer, "%v\t", groupSnapshot.Namespace); err != nil {
				return err
			}
		}

		creationTime := toTimeString(groupSnapshot.CreationTimestamp.Time)
		if _, err := fmt.Fprintf(writer, "%v\t%v\t%v\t%d\t%v\n",
			name,
			groupSnapshot.Status.Status,
			groupSnapshot.Status.Stage,
			len(groupSnapshot.Status.VolumeSnapshots),
			creationTime,
		); err != nil {
			return err
		}
	}
	return nil
}

// parseKeyValueList parses a list of key values into a map
func parseKeyValueList(expressions []string) (map[string]string, error) {
	matchLabels := make(map[string]string)
	for _, e := range expressions {
		entry := strings.SplitN(e, "=", 2)
		if len(entry) != 2 {
			return nil, fmt.Errorf("invalid key value: %s provided. "+
				"Example format: app=mysql", e)
		}

		matchLabels[entry[0]] = entry[1]
	}

	return matchLabels, nil
}
