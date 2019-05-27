package storkctl

import (
	"github.com/spf13/cobra"
	"k8s.io/kubernetes/pkg/kubectl/genericclioptions"
)

func newSuspendCommand(cmdFactory Factory, ioStreams genericclioptions.IOStreams) *cobra.Command {
	suspendCommands := &cobra.Command{
		Use:   "suspend",
		Short: "Suspend schedules",
	}

	suspendCommands.AddCommand(
		newSuspendMigrationSchedulesCommand(cmdFactory, ioStreams),
		newSuspendSnapshotSchedulesCommand(cmdFactory, ioStreams),
	)

	return suspendCommands
}
