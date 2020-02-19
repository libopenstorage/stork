package storkctl

import (
	"github.com/spf13/cobra"
	"k8s.io/cli-runtime/pkg/genericclioptions"
)

func newSuspendCommand(cmdFactory Factory, ioStreams genericclioptions.IOStreams) *cobra.Command {
	suspendCommands := &cobra.Command{
		Use:   "suspend",
		Short: "Suspend schedules",
	}

	suspendCommands.AddCommand(
		newSuspendMigrationSchedulesCommand(cmdFactory, ioStreams),
		newSuspendSnapshotSchedulesCommand(cmdFactory, ioStreams),
		newSuspendApplicationBackupSchedulesCommand(cmdFactory, ioStreams),
	)

	return suspendCommands
}
