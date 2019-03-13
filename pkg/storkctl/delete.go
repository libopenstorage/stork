package storkctl

import (
	"github.com/spf13/cobra"
	"k8s.io/kubernetes/pkg/kubectl/genericclioptions"
)

func newDeleteCommand(cmdFactory Factory, ioStreams genericclioptions.IOStreams) *cobra.Command {
	deleteCommands := &cobra.Command{
		Use:   "delete",
		Short: "Delete stork resources",
	}

	deleteCommands.AddCommand(
		newDeleteSnapshotCommand(cmdFactory, ioStreams),
		newDeleteMigrationCommand(cmdFactory, ioStreams),
		newDeleteMigrationScheduleCommand(cmdFactory, ioStreams),
		newDeleteSnapshotScheduleCommand(cmdFactory, ioStreams),
	)
	return deleteCommands
}
